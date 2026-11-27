# Purser

`Purser` 是进程级入口：持有池注册表、资源适配器注册表、事件总线、审计、指标与用量账本。一个进程通常只创建一个实例；需要同机多进程共享时使用 [LocalScheduler](../local-scheduler.md)。

本页签名以嵌入式异步 `Purser` 为准。同步、线程和远程门面的能力差异见[同步 Purser](../../guides/sync-facade.md)。

## 构造

```python
Purser(
    *, maintenance_enabled=True, event_queue_size=10_000,
    usage_ledger_size=50_000, storage="memory", coordination="memory",
    mysql_url=None, mysql_connection=None, redis_url=None, redis_client=None,
    sqlite_path=None, audit_path=None, secret_key=None, secret_key_id="1",
    previous_secret_keys=None, default_adapters="global", bundle_mode="rollback",
    bundle_fairness=None, reservation_ttl=30.0, bundle_gc_interval=30.0,
    fairness_ttl=300.0,
)
```

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `maintenance_enabled` | `bool` | `True` | 是否启动周期维护任务（配额重置、自动解封、陈旧资源回收等） |
| `event_queue_size` | `int` | `10_000` | 事件分发队列与历史环形缓冲的容量 |
| `usage_ledger_size` | `int` | `50_000` | 内存用量明细保留条数，超出后淘汰最旧记录；进程重启清空 |
| `storage` | `"memory"` / `"sqlite"` / `"mysql"` | `"memory"` | 池的默认存储层 |
| `coordination` | `"memory"` / `"redis"` | `"memory"` | 池的默认协调层 |
| `mysql_url` / `mysql_connection` | <code>str &#124; None</code> / 连接或连接池 | `None` | MySQL 连接串，或直接注入 `aiomysql` 连接/连接池 |
| `redis_url` / `redis_client` | <code>str &#124; None</code> / `Redis` | `None` | Redis 连接串，或注入 `redis.asyncio.Redis` 实例 |
| `sqlite_path` | <code>str &#124; Path &#124; None</code> | `None` | SQLite 文件路径；`storage="sqlite"` 时必填，不会静默退回内存 |
| `audit_path` | <code>str &#124; Path &#124; None</code> | `None` | 非空时审计哈希链同时写入该 SQLite 文件，重启后恢复并继续校验 |
| `secret_key` | <code>str &#124; bytes &#124; None</code> | `None` | payload 加密主密钥；缺省读取 `PURSER_SECRET_KEY`，sqlite 下可从 key 文件加载 |
| `secret_key_id` | `str` | `"1"` | 当前密文标识 |
| `previous_secret_keys` | 映射或 `None` | `None` | 轮换期旧密钥表 |
| `default_adapters` | `"global"` / `"builtin"` / `"none"` | `"global"` | `global` = 内置 `proxy` / `account` + 全局注册适配器；`builtin` = 只装内置；`none` = 空注册表 |
| `bundle_mode` | `"rollback"` / `"two_phase"` | `"rollback"` | 组合租约编排模式 |
| `bundle_fairness` | <code>BundleFairness &#124; None</code> | `None` | 非空时开启跨池准入配额 |
| `reservation_ttl` | `float` | `30.0` | 两阶段拼装期成员书签 TTL（秒） |
| `bundle_gc_interval` | `float` | `30.0` | `bundle_gc` 崩溃回收扫批周期（秒） |
| `fairness_ttl` | `float` | `300.0` | 公平计数/组合保活标记 TTL（秒）；开启 `bundle_fairness` 后接管标记 TTL |

`storage` / `coordination` 只是**默认值**：建池时按“实例默认值 → `profile` → `config` → 关键字参数”合并，后出现的来源覆盖先出现的来源。传入完整 `PoolConfig` 实例时不能再同时指定 `profile`，但后续 `**overrides` 仍会覆盖实例字段。SQLite 场景下未显式给 `secret_key` 时会从 `sqlite_path` 派生或加载 key 文件；MySQL + `secret_fields` 若只有临时密钥会拒绝启动，避免其他实例无法解密。字段、预设与合并规则见[配置](../configuration.md)。

## 生命周期

### `start()` {#purser-start}

**用途**：提前启动事件总线与所有已注册池，避免首次获取时承担启动成本。

**签名**

```python
async def start(self) -> None: ...
```

**返回与异常**：返回 `None`。重复调用幂等；进程隔离不满足时抛 `IsolationConflict`，后端启动失败时向上传播。

**示例**

```python
ps = Purser(storage="sqlite", sqlite_path="purser.db")
await ps.start()
```

**注意**：每个池启动 reaper、等待服务循环、后台验活与周期维护，最后执行一次租约对账，把槽位收敛到协调端的真实存活租约。启动后创建的池会在后台自动启动；首次获取会等待该池的启动对账完成。

### `close()` {#purser-close}

**用途**：关闭所有池、事件总线与审计，并释放进程隔离占用。

**签名**

```python
async def close(self) -> None: ...
```

**返回与异常**：返回 `None`。会先关闭每个池，用 `PoolExhausted` 唤醒全部在途等待者，再停止事件总线并关闭审计。

**示例**

```python
await ps.close()
```

**注意**：推荐 `async with Purser() as ps:`，异常路径也会关闭。关闭后不支持隐式自动重启；显式 `start()` 对内存后端可重新启动同一实例，SQLite 等已关闭连接的后端不能直接重开。`ps.started` 只读，`start()` 后尚未 `close()` 时为 `True`。

```python
async with Purser() as ps:
    pool = ps.create_pool(name="demo", kind="proxy")
```

## 池注册表

### `create_pool()` {#create-pool}

**用途**：严格创建一个新池；同名池已存在时失败。

**签名**

```python
def create_pool(
    self,
    name: str,
    kind: str,
    *,
    config: PoolConfig | Mapping[str, Any] | None = None,
    profile: str | Mapping[str, Any] | None = None,
    **overrides: Any,
) -> ResourcePool: ...
```

**参数**

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `name` | `str` | 必填 | 池名 |
| `kind` | `str` | 必填 | 资源类型，必须已注册对应适配器 |
| `config` | <code>PoolConfig &#124; Mapping &#124; None</code> | `None` | 完整配置或配置字典 |
| `profile` | <code>str &#124; Mapping &#124; None</code> | `None` | 场景预设；不能与 `PoolConfig` 实例同时使用 |
| `overrides` | `Any` | — | 最后覆盖的池配置字段 |

**返回与异常**：返回 `ResourcePool`。同名池抛 `PoolExists`；未知适配器、非法配置或后端不可用会抛出对应异常。

**示例**

```python
pool = ps.create_pool(name="proxies", kind="proxy", lease_ttl=120)
pool = ps.create_pool(name="accounts", kind="account", profile="validated")
```

**注意**：`max_slots` 未指定时取适配器的 `default_slots`；`secret_fields` 为空时继承适配器声明。两者都经 `adapter.defaults()` 合并。

### `ensure_pool()` {#ensure-pool}

**用途**：幂等地获取或创建池，适合重复执行初始化逻辑。

**签名**

```python
def ensure_pool(
    self,
    name: str,
    kind: str,
    *,
    config: PoolConfig | Mapping[str, Any] | None = None,
    profile: str | Mapping[str, Any] | None = None,
    **overrides: Any,
) -> ResourcePool: ...
```

**参数**：与 `create_pool()` 相同。

**返回与异常**：返回已有或新建的 `ResourcePool`。已存在时直接返回，忽略本次 `config` / `profile` / `overrides`，也不校验 `kind`；不存在时才按参数创建。

**示例**

```python
pool = ps.ensure_pool(name="proxies", kind="proxy", lease_ttl=120)
```

### `get_pool()` {#get-pool}

**用途**：只读取已注册池，不创建新池。

**签名**

```python
def get_pool(
    self, name: str, *, default: Any = ...
) -> ResourcePool | Any: ...
```

**参数**

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `name` | `str` | 必填 | 池名 |
| `default` | `Any` | 省略 | 不存在时的返回值；省略则抛 `PoolNotFound` |

**返回与异常**：存在时返回池；不存在且传了 `default` 时返回它，否则抛 `PoolNotFound`。

**示例**

```python
pool = ps.get_pool("proxies")
maybe_pool = ps.get_pool("missing", default=None)
```

### `delete_pool()` {#delete-pool}

**用途**：从注册表删除池，并可选择清空仍有资源的池。

**签名**

```python
async def delete_pool(
    self, name: str, *, force: bool = False, operator: str = "system"
) -> dict[str, Any]: ...
```

**参数**

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `name` | `str` | 必填 | 池名 |
| `force` | `bool` | `False` | 是否允许删除仍有资源的池；会先 flush 存储与协调层 |
| `operator` | `str` | `"system"` | 审计操作者 |

**返回与异常**：返回 `{"pool", "removed", "forced"}`。池不存在抛 `PoolNotFound`；有活跃租约时始终拒绝，即使 `force=True`；仍有资源但未传 `force` 时抛 `BadState`。

**注意**：`force` 只绕过“仍有资源”的限制，不能绕过活跃租约限制。

### 池枚举

| 成员 | 签名 | 说明 |
| --- | --- | --- |
| `pools` | `@property -> Mapping[str, ResourcePool]` | 本地已注册池的只读映射快照；之后的增删不会反映到已取得的对象上 |
| `get_pool_names()` | `def get_pool_names() -> list[str]` | 返回已注册池名列表 |

## 注册资源适配器

### `register_adapter()` {#register-adapter}

**用途**：注册一类资源的生成、验活和生命周期插件。

**签名**

```python
def register_adapter(self, adapter: ResourceAdapter) -> None: ...
```

**参数**：`adapter` 为实现了 `ResourceAdapter` 契约的实例。

**返回与异常**：返回 `None`。`kind` 必须非空；同一 `kind` 再次注册会覆盖先前实例；进程隔离冲突抛 `IsolationConflict`。

**示例**

```python
ps.register_adapter(adapter=MyAdapter())
```

**注意**：内置 `proxy` / `account` 已在构造时注册。已存在的池继续持有创建时的适配器实例，不会被后续同 `kind` 注册替换。插件契约见[Resource / ResourceAdapter](resources.md)。

## 组合租约（bundle）

跨池一次获取多类资源的组合，成员都是普通池租约，各自在所属池内续约；失败时已获取成员按模式回滚或部分返回。完整流程见[组合租约指南](../../guides/bundle-leases.md)。

### 请求对象

| 成员 | 签名 | 说明 |
| --- | --- | --- |
| `BundleSlot` | `BundleSlot(pool, rid=None, req=None, match=None)` | 一个成员槽：按池过滤、定向绑定或从锚点派生约束 |
| `BundleRequest` | `BundleRequest(slots, consumer=None, mode="atomic", order="scarce_first", timeout=None, bundle_id=None, max_retries=0, strict=None, priority=0)` | 组合请求；`strict=None` 跟随 `Purser(bundle_mode=...)`，`priority` 用于公平准入队列 |
| `MatchRule` | `MatchRule(anchor_pool, from_field, to_tag)` | 从锚点成员取字段值，目标成员以 `required_tag=f"{to_tag}:{value}"` 过滤 |
| `BundleLease` | `bundle_id` / `consumer` / `members` | `member(pool)` 取成员，`ids()` 返回资源 id 映射 |

- `mode="atomic"`：任一槽失败抛 `BundleAcquireFailed` 并回滚；`best_effort` 返回已获取部分，一个都没获取到仍抛错。
- `bundle_id` 幂等：同一 `consumer` 重复获取同一 `bundle_id` 返回已持有组合。
- `scarce_first` 优先执行更稀缺的池，但 `match` 槽始终排在其锚点之后。
- `two_phase` 与 `best_effort` 互斥；`strict` 可按请求覆盖实例默认模式。

### `bundle()` {#purser-bundle}

**用途**：组合租约的上下文管理器，退出时自动释放全部成员。

**签名**

```python
async def bundle(
    self, request: BundleRequest | dict[str, Any], **overrides: Any
) -> AsyncIterator[BundleLease]: ...
```

**参数**：`request` 为 `BundleRequest` 或等价字典；`overrides` 覆盖请求字段。

**返回与异常**：进入上下文时产出 `BundleLease`；退出时调用 `release_bundle()`。获取阶段的异常与 `acquire_bundle()` 一致。

**示例**

```python
from purser import AcquireRequest, BundleRequest, BundleSlot

async with ps.bundle(
    BundleRequest(
        slots=(
            BundleSlot(pool="accounts", rid="a1"),
            BundleSlot(pool="proxies", req=AcquireRequest(tags=frozenset({"us"}))),
        ),
        consumer="worker-1",
        timeout=10.0,
    )
) as bundle:
    account = bundle.member(pool="accounts")
    proxy = bundle.member(pool="proxies")
```

### `acquire_bundle()` {#acquire-bundle}

**签名**

```python
async def acquire_bundle(
    self, request: BundleRequest | dict[str, Any], **overrides: Any
) -> BundleLease: ...
```

**用途**：手动获取跨池组合；需要跨函数持有时使用。

**返回与异常**：返回 `BundleLease`。原子模式失败抛 `BundleAcquireFailed`；公平准入超限抛 `BundleQuotaExceeded`；其他成员池异常按组合回滚语义处理。

### `release_bundle()` {#release-bundle}

**签名**

```python
async def release_bundle(
    self,
    bundle: BundleLease,
    *,
    outcome_by_pool: dict[str, UsageOutcome] | None = None,
    used_by_pool: dict[str, int] | None = None,
) -> None: ...
```

**参数**

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `bundle` | `BundleLease` | 必填 | 要释放的组合 |
| `outcome_by_pool` | <code>dict[str, UsageOutcome] &#124; None</code> | `None` | 按池指定成员归还结果 |
| `used_by_pool` | <code>dict[str, int] &#124; None</code> | `None` | 按池指定成员用量 |

**返回**：`None`。成员按获取的逆序归还。

### `bundle_gc()` {#bundle-gc}

**签名**：`async def bundle_gc() -> dict[str, int]`

**用途**：刷新仍存活组合的标记，回收崩溃中的两阶段预留并清理公平计数。返回各回收任务的计数字典。

## 用量报表

### `report_usage()` {#report-usage}

**用途**：汇总已结束租约的历史用量，并叠加当前活跃租约与池负载快照。

**签名**

```python
async def report_usage(
    self,
    *,
    since: float | None = None,
    until: float | None = None,
    group_by: str = "consumer",
    pool: str | None = None,
    limit: int = 20,
) -> UsageReport: ...
```

**参数**

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `since` | <code>float &#124; None</code> | `None` | 时间窗口起点（epoch 秒），必须有限 |
| `until` | <code>float &#124; None</code> | `None` | 时间窗口终点（epoch 秒），必须有限且不早于 `since` |
| `group_by` | `"consumer"` / `"target"` / `"pool"` / `"kind"` | `"consumer"` | 分组维度 |
| `pool` | <code>str &#124; None</code> | `None` | 限定单个池；缺省汇总全部池 |
| `limit` | `int` | `20` | 分组返回上限，必须 `>= 1` |

**返回与异常**：返回 `UsageReport`，运行时是普通 `dict`，可直接 JSON 序列化。非法窗口、分组或 `limit` 抛 `ConfigError`；未知池抛 `PoolNotFound`。

| 字段 | 内容 |
| --- | --- |
| `window` | `since` / `until` / `oldest` / `newest` / `events` / `retained` / `capacity` |
| `group_by` | 实际分组维度 |
| `totals` | `releases` / `used` / `slots` / `outcomes` |
| `groups` | `list[UsageGroup]`：`key` / `releases` / `used` / `slots` / `outcomes` |
| `groups_total` / `truncated` | 分组总数与是否被 `limit` 截断 |
| `active` | 当前活跃租约按同一维度分组 |
| `pools` | 各池当前负载快照 |

**示例**

```python
report = await ps.report_usage(
    since=None, until=None, group_by="consumer", pool=None, limit=20
)
```

**注意**：历史来自有界内存账本，每次归还或强制撤销记一行，进程重启后清空；`active` 与 `pools` 是实时快照，不受账本容量影响。

## 备份与恢复

### `backup()` {#purser-backup}

**用途**：导出池配置、资源主数据和活跃租约快照。

**签名**

```python
async def backup(self, *, operator: str = "system") -> dict[str, Any]: ...
```

**参数**：`operator` 为审计操作者，默认 `"system"`。

**返回**：包含 `format` / `version` / `schema_version` / `created_at` / `pools` 的字典；每个池含 `name`、`kind`、`config`、`resources` 与 `leases` 快照。

**注意**：备份文档包含明文 payload（含密文字段），只应交给可信调用方。租约快照仅用于审计/展示，恢复时不会迁移租约。

### `restore_backup()` {#purser-restore-backup}

**用途**：按备份文档重建池与资源，租约不恢复。

**签名**

```python
async def restore_backup(
    self,
    document: dict[str, Any],
    *,
    conflict: str = "skip",
    operator: str = "system",
) -> dict[str, Any]: ...
```

**参数**

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `document` | `dict` | 必填 | `backup()` 返回的文档 |
| `conflict` | `"skip"` / `"overwrite"` | `"skip"` | 已存在资源时跳过或覆盖 |
| `operator` | `str` | `"system"` | 审计操作者 |

**返回与异常**：返回 `{"ok", "format", "conflict", "created_pools", "reused_pools", "imported", "updated", "skipped", "normalized", "leases_ignored", "errors"}`。文档格式、版本或冲突策略非法时抛 `ConfigError`；单池或单资源失败会收集到 `errors`，不会让整个返回值变为失败。

**注意**：即使存在条目错误，`ok` 仍固定为 `True`；调用方必须检查 `errors` 判断实际恢复是否完整。占用态资源（`new` / `validating` / `active` / `full` / `draining` / `lost`）回到 `IDLE`；`banned` / `quarantined` / `suspended` / `quota_exhausted` / `archived` 保留。池的 `storage` / `coordination` 始终取当前服务配置，不取备份文档。

**示例**

```python
document = await ps.backup(operator="backup-job")
result = await ps.restore_backup(
    document, conflict="skip", operator="restore-job"
)
if result["errors"]:
    ...
```

## 运行时属性

| 属性 | 类型 | 用途 |
| --- | --- | --- |
| `started` | `bool` | `start()` 后尚未 `close()` 时为 `True` |
| `bus` | `EventBus` | `subscribe(async_listener) -> unsubscribe`；`history(after_seq=0, limit=500)` 回放；`publish()` 同步入队 |
| `audit` | `AuditLog` | `entries(limit=None, after_seq=0)`、`verify()`、`len(audit)` |
| `metrics` | `Metrics` | `inc(name, delta=1)`、`observe(name, value)`、`snapshot()`、`render_prometheus()` |
| `usage` | `UsageLedger` | 用量账本，`report_usage()` 的历史来源 |
| `idempotency` | `IdempotencyStore` | 进程内幂等结果；配置 Redis 时自动换成共享实现 |
| `probe_guard` | `ProbeGuard` | 探针自检：单探针失败计数、冷却与集体下线暂停 |
| `adapters` | `ResourceAdapterRegistry` | `register()` / `unregister(kind)` / `get(kind)` / `names()` |
| `storage_kind` / `coordination_kind` | `str` | 构造时给出的默认平面 |

事件对象的字段与事件类型见 [Events](../rest-api/events.md)；Python 侧订阅到的是 `Event` 对象，可调用 `to_dict()` 转成与 REST 一致的 JSON。

## 同步与多进程

- 同步代码使用 [同步 Purser](../../guides/sync-facade.md) 的 `SyncPurser`：它在专属线程里运行同一个内核，方法阻塞并在超时时抛 `TimeoutError`。
- 同机多进程共享一套池使用 [LocalScheduler](../local-scheduler.md)：调度进程独占 `Purser`，worker 通过 `multiprocessing.connection` 调用。
- 两者都不可跨进程传递内核实例；跨进程共享存储与协调后端时，各进程仍应各自对账。
- 门面签名并非完全一致，例如 `SyncPool.release()` 支持 `latency_ms`，`ThreadPool.release()` 与 `RemotePool.release()` 不支持。
