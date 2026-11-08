# StorageBackend / Coordination

purser 把状态拆成两个独立平面：

- **storage（存储层）**：资源主数据、状态、槽位、版本与密文。丢失即业务事故。
- **coordination（协调层）**：在途租约、到期时间与唤醒。单实例丢失后可对账重建；多实例下丢失可能让同一资源被重复并发使用，Redis 必须持久化部署。

两层的丢失分级、对账边界与多实例风险见[数据平面与丢失语义](../../design/data-planes.md)。

## 选择后端

后端按**池**配置，而不是全局唯一：

```python
ps = Purser(
    storage="sqlite", sqlite_path="purser.db",
    coordination="redis", redis_url="redis://127.0.0.1:6379/0",
)
pool = ps.create_pool("proxies", "proxy")               # 继承 Purser 默认值
pool2 = ps.create_pool("accounts", "account", storage="memory")
pool3 = ps.create_pool("other", "proxy",
                       config={"storage": "mysql", "coordination": "redis"})
```

- `Purser(storage=..., coordination=...)` 只是建池时的默认值。
- 传 `PoolConfig` 实例时以实例字段为准；传字典时字典覆盖默认值。
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

内置实现都从顶层 `purser` 导出，也可直接构造：

```python
SqliteStorage(path="purser.db", codec=codec, secret_fields=("api_key",))
MySqlStorage(url="mysql://user:pass@127.0.0.1:3306/purser", secret_fields=("api_key",))
RedisCoordination(url="redis://127.0.0.1:6379/0", key_prefix="purser")
```

`Purser.create_pool()` 目前只按配置字符串构造内置后端，没有公开的自定义后端注册表；自定义实现可以直接实现下面的 ABC，但接入 facade 需要自行组装池或扩展 `_make_storage` / `_make_coordination`。

## `StorageBackend` 契约

```python
class StorageBackend(ABC):
    async def get(pool, rid) -> Resource | None
    async def put(pool, resource, *, expected_version=None) -> Resource
    async def delete(pool, rid) -> bool
    async def query(pool, *, states=None, tags=None, limit=100, offset=0) -> list[Resource]
    async def counts(pool) -> dict[ResourceState, int]
    async def health_scores(pool) -> list[float]          # 有样本资源的分数
    async def transition_acquire(pool, rid, slots) -> TransitionResult
    async def transition_release(pool, rid, slots) -> TransitionResult
    async def reset_usage(pool, rid, slots=0) -> bool
    async def set_state(pool, rid, expected_version, *, expect_state=None, patch) -> bool
    def scan(pool, *, states=None, batch=50) -> AsyncIterator[Resource]
    async def flush(pool) -> None
```

- `put(expected_version)`：`None` 维持 upsert；`0` 仅创建；`N` 仅当存储版本恰为 `N` 时更新。冲突抛 `VersionConflict`，用于跨实例防丢更新。
- `transition_acquire` / `transition_release` 是**槽位感知**的原子迁移，返回 `TransitionResult(ok, state, resource)`；并发下失败返回 `ok=False`，由调用方决定重试或抛冲突。
- `transition_release` 归还最后一个槽位时，`DRAINING` 资源会自动归档。
- `reset_usage(pool, rid, slots=0)`：对账入口。`slots=0` 把孤儿占用清回 `IDLE`（或归档已无在途租约的 `DRAINING`）；`slots>0` 把 `used_slots` 对齐到存活租约之和，不动主数据。
- `set_state` 以 `expected_version` + 可选 `expect_state` 做 CAS；`ResourcePatch` 中的字段为 `None` 表示“不修改”，需要清空时使用 `clear_fields`。
- `scan()` 是异步迭代器，内置实现按 id 顺序产出；SQL 后端通常按批读取。
- `health_scores()` 默认遍历 `scan()`，SQL 后端覆盖为只读健康列，避免为统计解密 payload。
- `flush(pool)` 删除该池的全部存储数据；`delete_pool(force=True)` 会调用它。

## `Coordination` 契约

```python
class Coordination(ABC):
    async def register_lease(pool, lease) -> None
    async def unregister_lease(pool, token) -> Lease | None
    async def get_lease(pool, token) -> Lease | None
    async def leases_for(pool, rid) -> list[Lease]
    async def renew_lease(pool, rid, token, expires_at) -> bool
    async def active(pool) -> list[Lease]
    async def flush(pool) -> None
    async def close() -> None
```

- 共享后端（Redis）下 `register_lease()` 必须让其他实例可见；租约的 `resource` / `payload` 是本进程快照，不进入协调层。
- `unregister_lease()` 返回被移除的租约；token 已不存在时返回 `None`。这个返回值是归还路径判断“是否还应扣减存储槽位”的依据。
- `renew_lease()` 只在 token 仍存在时延长到期时间；返回 `False` 表示租约已被回收或过期。
- Redis 实现用每资源哈希 + token 索引，跨实例强制撤销、列表与续约都不需要扫描全库。
- Redis / MySQL 连接失败统一抛 `BackendUnavailable`；调用方采取 fail-closed，不会静默退回内存。

## 对账与丢失

`Pool.start()` 会执行租约对账：

- 以协调层的存活租约为准，把存储侧的 `used_slots` 对齐到存活租约之和；没有存活租约的占用回 `IDLE`，`DRAINING` 且无租约的资源直接归档。
- 存储层是权威数据，协调层只是运行期权威；因此单实例异常重启后资源主数据不会丢，只会短暂校正槽位。
- **多实例下协调层丢失不是“只影响调度准确性”**：其他实例看不到在途租约，可能立即把同一资源再次分配。Redis 必须开启持久化（至少 AOF），并按共享部署对待。
- 任一后端不可用时，依赖它的获取/归还/迁移操作以 `BackendUnavailable` 失败，而不是降级到本地状态。

MySQL / Redis 的真实服务集成测试由 `PURSER_TEST_MYSQL_URL` / `PURSER_TEST_REDIS_URL` 门控，见[测试与工程规范](../../contributing/testing.md)。
