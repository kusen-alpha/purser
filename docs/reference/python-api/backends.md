# StorageBackend / Coordination

Purser 把状态拆成两个独立平面：

- **storage（存储层）**：资源主数据、状态、槽位、版本与密文。丢失即业务事故。
- **coordination（协调层）**：在途租约、到期时间与唤醒。单实例丢失后可对账重建；多实例下丢失可能让同一资源被重复并发使用，Redis 必须持久化部署。

两层的丢失分级与对账边界见[多实例部署](../../guides/multi-instance.md)。

## 导入与选择后端

内置实现可从顶层 `purser` 导入；自定义后端的抽象契约与辅助类型从 `purser.backends.base` 导入：

```python
from purser import (
    MemoryStorage, SqliteStorage, MySqlStorage,
    MemoryCoordination, RedisCoordination,
)
from purser.backends.base import (
    StorageBackend, Coordination, ResourcePatch, TransitionResult,
)
```

`StorageBackend`、`Coordination`、`ResourcePatch`、`TransitionResult` 不在顶层 `purser.__all__`，不能写成 `from purser import StorageBackend`。

后端按**池**配置，而不是全局唯一：

```python
ps = Purser(
    storage="sqlite", sqlite_path="purser.db",
    coordination="redis", redis_url="redis://127.0.0.1:6379/0",
)
pool = ps.create_pool(name="proxies", kind="proxy")  # 继承 Purser 默认值
pool2 = ps.create_pool(name="accounts", kind="account", storage="memory")
pool3 = ps.create_pool(
    name="other", kind="proxy",
    config={"storage": "mysql", "coordination": "redis"},
)
```

- `Purser(storage=..., coordination=...)` 只是建池时的默认值。
- 建池按“实例默认值 → `profile` → `config` → 关键字参数”合并；传 `PoolConfig` 实例时不能再传 `profile`，但 `**overrides` 仍会覆盖实例字段。
- `storage` 可选 `memory` / `sqlite` / `mysql`；`coordination` 可选 `memory` / `redis`。
- `storage="sqlite"` 必须提供 `sqlite_path`，否则抛 `ConfigError`，不会静默退回内存。
- MySQL 需要 `pip install "purser[mysql]"`；aiomysql 懒加载，不装不影响其他后端。

| 实现 | 平面 | 要点 |
| --- | --- | --- |
| `MemoryStorage` | storage | 单进程字典 + 状态/标签倒排索引；读改写天然原子；进程退出即丢失 |
| `SqliteStorage` | storage | stdlib `sqlite3`，文件库启用 WAL；`resources` + `resource_tags` 两表；版本 CAS；密文字段 AES-GCM 落库 |
| `MySqlStorage` | storage | `aiomysql` 懒连接；utf8mb4；`ON DUPLICATE KEY UPDATE` 版本自增；连接失败抛 `BackendUnavailable`；id 超过 255 字符需要投影 |
| `MemoryCoordination` | coordination | 单进程租约表；无跨实例可见性 |
| `RedisCoordination` | coordination | `redis.asyncio`；每资源租约哈希 + token 索引；注销用 Lua 原子完成；不使用阻塞式 `KEYS`；支持 `key_prefix` |

`Purser.create_pool()` 目前只按配置字符串构造内置后端，没有公开的自定义后端注册表；自定义实现可以实现下面的 ABC，但接入 facade 需要自行组装池或扩展内部工厂。

## `StorageBackend` 契约

`StorageBackend` 是资源主数据的权威平面，不保存租约。除特别说明外，下列方法均为抽象方法，自定义后端必须实现。

### `get()` / `put()` / `delete()` {#storage-crud}

```python
async def get(self, pool: str, rid: str) -> Resource | None: ...
async def put(
    self,
    pool: str,
    resource: Resource,
    *,
    expected_version: int | None = None,
) -> Resource: ...
async def delete(self, pool: str, rid: str) -> bool: ...
```

| 方法 | 返回 | 关键语义 |
| --- | --- | --- |
| `get` | `Resource` 或 `None` | 读取主数据；适配器类型化由池层 `hydrate()` 负责 |
| `put` | 持久化后的 `Resource` | `expected_version=None` 为 upsert；`0` 仅创建；`N` 仅当存储版本恰为 `N` 时更新；冲突抛 `VersionConflict` |
| `delete` | `bool` | 删除是否实际发生 |

### `query()` / `counts()` / `health_scores()` {#storage-query}

```python
async def query(
    self,
    pool: str,
    *,
    states: set[ResourceState] | None = None,
    tags: frozenset[str] | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[Resource]: ...
async def counts(self, pool: str) -> dict[ResourceState, int]: ...
async def health_scores(self, pool: str) -> list[float]: ...
```

- `query()` 返回匹配资源列表；`limit` / `offset` 语义与池层一致。
- `counts()` 返回状态直方图。
- `health_scores()` 是可选覆盖的 concrete 方法：默认遍历 `scan()`，只返回有验活样本资源的分数；SQL 后端应覆盖为只读健康列，避免为了统计解密 payload。

### 槽位迁移

```python
async def transition_acquire(
    self, pool: str, rid: str, slots: int
) -> TransitionResult: ...
async def transition_release(
    self, pool: str, rid: str, slots: int
) -> TransitionResult: ...
```

**用途**：槽位感知的原子迁移。获取时增加 `used_slots` 并进入 `ACTIVE` / `FULL`；归还时减少槽位。

**返回**：`TransitionResult(ok, state, resource)`。并发下失败返回 `ok=False`，由池层重试或转成冲突；`transition_release()` 归还最后一个槽位时，`DRAINING` 资源自动归档。

### `reset_usage()` {#storage-reset-usage}

**签名**：`async def reset_usage(pool: str, rid: str, slots: int = 0) -> bool`

**用途**：租约对账入口，把存储侧 `used_slots` 对齐到存活租约之和。

- `slots=0`：清掉孤儿占用，回到 `IDLE`；无在途租约的 `DRAINING` 资源归档。
- `slots>0`：对齐到存活租约总槽位，不动主数据。

**返回**：是否实际更新。

### `set_state()` {#storage-set-state}

```python
async def set_state(
    self,
    pool: str,
    rid: str,
    expected_version: int,
    *,
    expect_state: ResourceState | None = None,
    patch: ResourcePatch,
) -> bool: ...
```

**用途**：以版本号和可选状态做 CAS，并原子应用主数据补丁。

**参数**：`expected_version` 是写入前版本；`expect_state` 额外约束当前状态；`patch` 为 `ResourcePatch`。

**返回**：更新成功返回 `True`，版本或状态冲突返回 `False`。

**注意**：`ResourcePatch` 中字段为 `None` 通常表示“不修改”；需要清空 `usage_quota`、`expires_at`、`next_available_at`、`mutex_group`、`schedule_window` 或 `quota` 时，使用 `clear_fields`。

### `scan()` {#storage-scan}

**签名**

```python
def scan(
    self,
    pool: str,
    *,
    states: set[ResourceState] | None = None,
    batch: int = 50,
) -> AsyncIterator[Resource]: ...
```

**用途**：异步分批遍历资源。内置实现按 id 顺序产出；SQL 后端通常按批读取。

### `sample_allocatable()` {#storage-sample-allocatable}

**签名**

```python
async def sample_allocatable(
    self,
    pool: str,
    *,
    states: set[ResourceState],
    slots: int = 1,
    tags: frozenset[str] | None = None,
    required_tag: str | None = None,
    limit: int = 256,
    rng: random.Random | None = None,
) -> list[Resource]: ...
```

**用途**：为获取热路径返回至多 `limit` 个候选。它是可选覆盖的 concrete 方法，默认遍历 `scan()` 并提前停止。

**返回**：候选列表。结果达到 `limit` 时只表示“样本已满”，不能据此判定池耗尽；调用方应重试或回退。状态、空闲槽位和标签是硬约束，调度类过滤留在租约服务层。

### `flush()` {#storage-flush}

**签名**：`async def flush(pool: str) -> None`

**用途**：删除该池的全部存储数据。`delete_pool(force=True)` 会调用它。自定义实现应把它视为危险操作，不自动备份。

## `Coordination` 契约

`Coordination` 是瞬态租约与唤醒平面，内容可重建。共享实例必须保持时钟同步；租约是否有效以 `expires_at` 和墙钟判断，Redis key TTL 只是清理宽限期。

### `register_lease()` / `get_lease()` / `leases_for()` / `active()` {#coordination-read}

```python
async def register_lease(self, pool: str, lease: Lease) -> None: ...
async def get_lease(self, pool: str, token: str) -> Lease | None: ...
async def leases_for(self, pool: str, rid: str) -> list[Lease]: ...
async def active(self, pool: str) -> list[Lease]: ...
```

| 方法 | 返回 | 关键语义 |
| --- | --- | --- |
| `register_lease` | `None` | 共享后端必须让其他实例立即可见 |
| `get_lease` | `Lease` 或 `None` | 按 token 读取 |
| `leases_for` | `list[Lease]` | 某资源的全部活跃租约 |
| `active` | `list[Lease]` | 池内全部活跃租约，供对账、报表和删除保护使用 |

租约的 `resource` / `payload` 是本进程快照，不进入协调层；跨实例读取时这些字段可能为 `None`。

### `renew_lease()` {#coordination-renew}

**签名**

```python
async def renew_lease(
    self, pool: str, rid: str, token: str, expires_at: float
) -> bool: ...
```

**用途**：只在 token 仍存在时延长到期时间。

**返回**：成功返回 `True`；租约已回收或过期返回 `False`，池层会转换为 `LeaseLost`。

### `unregister_lease()` {#coordination-unregister}

**签名**：`async def unregister_lease(pool: str, token: str) -> Lease | None`

**用途**：原子移除租约。

**返回**：被移除的租约；token 不存在时返回 `None`。这个返回值是归还路径判断“是否还应扣减存储槽位”的依据，不能随意改成固定 `True`。

### `flush()` / `close()` {#coordination-close}

```python
async def flush(self, pool: str) -> None: ...
async def close(self) -> None: ...
```

- `flush(pool)` 清空该池协调数据，属于危险操作。
- `close()` 关闭后端连接或释放资源，不保存租约。

### `try_leader()` {#coordination-try-leader}

**签名**：`async def try_leader(pool: str, name: str, ttl: float) -> bool`

**用途**：共享周期任务的领导者选举，例如 `reconcile`。

**返回**：允许执行返回 `True`，否则 `False`。这是可选 concrete hook，默认返回 `True`；内存协调层没有对等实例，共享后端应覆盖为基于租约的选举，保证同一时刻只有一个实例执行跨资源扫描。

## 故障与对账语义

- 默认 **fail-closed**：依赖后端的获取/归还/迁移在后端不可用时抛 `BackendUnavailable`，不会静默退回内存。
- 配置 `fail_open=True` 后，部分续约/归还路径会吸收协调层故障，按本地降级状态继续，并记录 `coordination_degraded` 事件与 `fail_open_total` 指标。它不等于后端故障消失，也不适用于所有操作。
- `Pool.start()` 会执行租约对账：以协调层存活租约为准，把存储侧 `used_slots` 对齐到存活租约之和；没有存活租约的占用回 `IDLE`，`DRAINING` 且无租约的资源直接归档。
- 存储层是权威主数据，协调层是运行期权威；单实例异常重启后主数据不丢，但槽位可能需要校正。
- **多实例下协调层丢失不是“只影响调度准确性”**：其他实例看不到在途租约，可能立即把同一资源再次分配。Redis 必须开启持久化（至少 AOF），并按共享部署对待。
