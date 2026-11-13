# Purser

`Purser` 是进程级入口：持有池注册表、资源适配器注册表、事件总线、审计、指标与用量账本。一个进程通常只创建一个实例；需要同机多进程共享时使用 [LocalScheduler](../local-scheduler.md)。

## 构造

```python
Purser(
    *, maintenance_enabled=True, event_queue_size=10_000,
    usage_ledger_size=50_000, storage="memory", coordination="memory",
    mysql_url=None, mysql_connection=None, redis_url=None, redis_client=None,
    sqlite_path=None, audit_path=None, secret_key=None, secret_key_id="1",
    previous_secret_keys=None, use_globals=True, bundle_mode="rollback",
    bundle_fairness=None, reservation_ttl=30.0, bundle_gc_interval=30.0,
    fairness_ttl=300.0,
)
```

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `maintenance_enabled` | `True` | 是否启动周期维护任务（配额重置、自动解封、陈旧资源回收等） |
| `event_queue_size` | `10_000` | 事件分发队列与历史环形缓冲的容量 |
| `usage_ledger_size` | `50_000` | 内存用量明细保留条数，超出后淘汰最旧记录；进程重启清空 |
| `storage` | `"memory"` | 池的默认存储层：`memory` / `sqlite` / `mysql` |
| `coordination` | `"memory"` | 池的默认协调层：`memory` / `redis` |
| `mysql_url` / `mysql_connection` | `None` | MySQL 连接串，或直接注入 `aiomysql` 连接/连接池 |
| `redis_url` / `redis_client` | `None` | Redis 连接串，或注入 `redis.asyncio.Redis` 实例 |
| `sqlite_path` | `None` | SQLite 文件路径；`storage="sqlite"` 时必填，不会静默退回内存 |
| `audit_path` | `None` | 非空时审计哈希链同时写入该 SQLite 文件，重启后恢复并继续校验 |
| `secret_key` / `secret_key_id` / `previous_secret_keys` | `None` / `"1"` / `None` | payload 加密主密钥、当前密文标识与轮换期旧密钥表；缺省读取 `PURSER_SECRET_KEY`，sqlite 下可从 key 文件加载 |
| `use_globals` | `True` | 是否装载 `@register` 声明的全局默认适配器；kind 与内置 `proxy` / `account` 冲突时报错 |
| `bundle_mode` | `"rollback"` | 组合租约编排模式：`rollback`（默认）/ `two_phase`（跨实例严格两阶段） |
| `bundle_fairness` | `None` | `BundleFairness` 配置；非空时开启跨池准入配额 |
| `reservation_ttl` | `30.0` | 两阶段拼装期成员书签 TTL（秒） |
| `bundle_gc_interval` | `30.0` | `bundle_gc` 崩溃回收扫批周期（秒） |
| `fairness_ttl` | `300.0` | 公平计数/组合保活标记 TTL（秒）；开启 `bundle_fairness` 后接管标记 TTL |

`storage` / `coordination` 只是**默认值**：建池时传字典配置会合并这两个默认值，而传入完整的 `PoolConfig` 实例时以实例自身字段为准。SQLite 场景下未显式给 `secret_key` 时会从 `sqlite_path` 派生或加载 key 文件；MySQL + `secret_fields` 若只有临时密钥会拒绝启动，避免其他实例无法解密。

## 生命周期

```python
ps = Purser(storage="sqlite", sqlite_path="purser.db")
await ps.start()      # 幂等
...
await ps.close()      # 幂等；关闭池、事件总线与审计
```

- `start()` 启动事件总线与所有已注册池；每个池在启动时执行租约对账，随后启动 reaper、后台验活与周期维护。
- `close()` 先关闭每个池（用 `PoolExhausted` 唤醒全部在途等待者），再停止事件总线并关闭审计。
- 推荐 `async with Purser() as ps:`，异常路径也会关闭。
- `ps.started` 只读，`start()` 后尚未 `close()` 时为 `True`。
- 启动后创建的池会在后台自动启动；首次获取会等待该池的启动对账完成，而不是与对账竞争。
- **启动前提**：`acquire()` / `lease()` / `acquire_many()` 要求池已启动，否则抛 `NotStarted`。导入、查询、状态变更等管理面操作在 `start()` 之前仍可用。

## 池注册表

| 方法 | 行为 |
| --- | --- |
| `create_pool(name, kind, *, config=None, **overrides)` | 新建池；同名已存在抛 `PoolExists`。`config` 可为 `PoolConfig` 或字典，`overrides` 再覆盖 |
| `pool(name, kind=None, *, config=None, **overrides)` | 已存在则返回；否则用给定 `kind` 创建。缺 `kind` 且不存在时抛 `PoolNotFound` |
| `get_pool(name)` | 不存在返回 `None` |
| `require_pool(name)` | 不存在抛 `PoolNotFound` |
| `pool_names()` | 返回已注册池名列表 |
| `delete_pool(name, *, force=False, operator="system")` | 删除池；有活跃租约时始终拒绝。仅当 `force=True` 才允许删除仍有资源的池，并先 flush 两个后端。返回 `{"pool", "removed", "forced"}` |

建池时 `max_slots` 未指定则取资源适配器的 `default_concurrency`；`secret_fields` 为空则继承资源适配器的声明。两者都经 `adapter.defaults()` 这一单一来源合并（`PoolConfig.default_for(kind, adapter=...)` 与之同源）。存储与协调后端的组装规则见[StorageBackend / Coordination](backends.md)。

## 注册资源适配器

```python
ps.register_adapter(MyAdapter())
```

- 内置 `proxy` / `account` 已在构造时注册。
- `kind` 必须非空；同一 `kind` 再次注册会覆盖先前实例。
- 插件契约见[Resource / ResourceAdapter](resources.md)。

## 组合租约（bundle）

跨池一次拿取多类资源的组合（O8）：例如 `{account, proxy}` 一整套。成员都是普通池租约，各自在所属池内续期；失败时已拿到的成员立即回滚释放（`best_effort` 则返回已拿到的部分）。

完整使用流程见[组合租约指南](../../guides/bundle-leases.md)。

```python
from purser import BundleRequest, BundleSlot, MatchRule

bundle = await ps.acquire_bundle(
    BundleRequest(
        slots=(
            BundleSlot(pool="accounts", resource_id="a1"),
            # 按账号 region 派生代理约束：required_tag = "region:us"
            BundleSlot(
                pool="proxies",
                match=MatchRule(
                    anchor_pool="accounts", from_field="region", to_tag="region"
                ),
            ),
        ),
        consumer="worker-1",
        order="scarce_first",
        timeout=10.0,
    )
)
try:
    account_lease = bundle.member("accounts")
    proxy_lease = bundle.member("proxies")
finally:
    await ps.release_bundle(bundle)
```

| 成员 | 说明 |
| --- | --- |
| `BundleSlot(pool, resource_id=None, req=None, match=None)` | 一个成员槽：按池过滤（`req`）、定向绑定（`resource_id`，O12）或从锚点派生约束（`match`） |
| `BundleRequest(slots, consumer=None, mode="atomic", order="scarce_first", timeout=None, bundle_id=None, max_retries=0)` | 组合请求；`mode` 取 `atomic` / `best_effort`，`order` 取 `scarce_first` / `declared`，`max_retries` 为瞬态失败（超时/资源暂缺）的抖动重试次数 |
| `MatchRule(anchor_pool, from_field, to_tag)` | 从先拿到的锚点成员取字段值，目标成员以 `required_tag=f"{to_tag}:{value}"` 过滤 |
| `BundleLease` | `bundle_id` / `consumer` / `members`（池名 -> `Lease`）；`member(pool)`、`ids()` |

- `bundle_id` 幂等：同一 `consumer` 重复申请同一 `bundle_id` 返回已持有的组合。
- 失败语义：`atomic` 任一槽失败抛 `BundleAcquireFailed` 并回滚已拿成员；`best_effort` 失败时返回已拿到的部分（一个都没拿到同样抛错）。
- 排序正确性：`scarce_first` 会优先执行资源更稀缺的池，但 `match` 槽始终排在其 `anchor_pool` 之后，锚点依赖不会被稀缺排序破坏。
- 死锁缓解：两个组合以相反顺序互相抢占成员时会循环等待；`max_retries` 让组合在瞬态失败后回滚并以随机退避重试，打破循环而不引入全局锁。
- 同步门面：`SyncPurser.acquire_bundle(...)` / `release_bundle(...)` 同名。
- REST：`POST /pools/{pool}/leases` 带 `bundle` 字段即可组合获取（锚点 `{pool}` 必须出现在 `slots` 里）。
- 固定 1:1 绑定（账号↔代理不可换）建议直接用"组合资源"建模：把成对数据建成单池一条资源，独占/租约/验活/封禁全部复用，无需跨池编排。

#### bundle 高级开关（默认关闭）

- `Purser(bundle_mode="two_phase")`：跨实例严格拼装。成员按 `(pool, resource_id)` 确定性全序获取（消除 ABBA 死锁）、书签写入 `bundle_store`、全部到位后 `confirm`；编排进程崩溃由 `bundle_gc` 按 `reservation_ttl` 回收 `assembling` 残留。逐请求可用 `BundleRequest(strict=True/False)` 覆盖；`two_phase` 与 `best_effort` 互斥。
- `Purser(bundle_fairness=BundleFairness(...))`：跨池准入。`max_per_consumer` / `max_total` / `per_pool_headroom`（每池为纯资源请求保留空位）/ `wait`+`timeout`；`BundleRequest.priority` 决定释放时先唤醒谁的准入队列。超额时抛或等待 `BundleQuotaExceeded`。
- `bundle_gc()`（同步侧 `SyncPurser.bundle_gc()`）：为本进程仍持有、或成员租约仍存活的已提交组合保活刷新标记，再回滚崩溃中的两阶段成员并清理公平计数。
- 分布式等待：公平或两阶段开启后，成员书签与公平标记都写到共享 store；`bundle_fairness` 的等待在共享 store 上以 `0.1s` 轮询（与池层等待一致），另一实例释放组合后本实例等待者会在一个轮询周期内被唤醒（跨实例优先级为尽力而为，实例内仍严格按 `priority` 排队）。

## 用量报表

```python
report = await ps.report_usage(
    since=None, until=None, group_by="consumer", pool=None, limit=20
)
```

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `since` / `until` | `None` | 时间窗口（epoch 秒）；必须有限且 `since <= until` |
| `group_by` | `"consumer"` | `consumer` / `target` / `pool` / `kind` |
| `pool` | `None` | 限定单个池；缺省汇总全部池 |
| `limit` | `20` | 分组返回上限，必须 `>= 1` |

返回 `UsageReport`：

| 字段 | 内容 |
| --- | --- |
| `window` | `UsageWindow`：`since` / `until` / `oldest` / `newest` / `events` / `retained` / `capacity` |
| `group_by` | 实际分组维度 |
| `totals` | `UsageTotals`：`releases` / `used` / `slots` / `outcomes`（按结果计数） |
| `groups` | `list[UsageGroup]`：`key` / `releases` / `used` / `slots` / `outcomes` |
| `groups_total` / `truncated` | 分组总数与是否被 `limit` 截断 |
| `active` | `ActiveUsage`：当前活跃租约按同一维度分组 |
| `pools` | `list[PoolUsage]`：各池当前负载快照 |

历史来自有界内存账本（每次归还或强制撤销记一行，按容量淘汰最旧），因此**进程重启后历史清空**；`active` 与 `pools` 是实时读取的协调层与存储层快照，不受账本容量影响。

## 备份与恢复

```python
document = await ps.backup(operator="backup-job")
result = await ps.restore(document, conflict="skip", operator="restore-job")
```

- `backup()` 返回 `format` / `version` / `schema_version` / `created_at` / `pools`；每个池含 `config`、`resources` 与**租约快照**。文档包含明文 payload（含密文字段），只应交给可信调用方。
- `restore()` 校验 `format` / `version`；`conflict` 取 `skip`（已存在资源不动）或 `overwrite`。
- 恢复不迁移租约：`new` / `validating` / `active` / `full` / `draining` / `lost` 占用态一律回到 `IDLE`；`banned` / `quarantined` / `suspended` / `quota_exhausted` / `archived` 保留。备份中的租约只计入返回值的 `leases_ignored`。
- 池的 `storage` / `coordination` 是部署选择，始终取当前服务配置，不取备份文档。
- 返回 `{"ok", "format", "conflict", "created_pools", "reused_pools", "imported", "updated", "skipped", "normalized", "leases_ignored", "errors"}`。

## 运行时属性

| 属性 | 类型 | 用途 |
| --- | --- | --- |
| `bus` | `EventBus` | `subscribe(async_listener) -> unsubscribe`；`history(after_seq=0, limit=500)` 回放；`publish()` 同步入队 |
| `audit` | `AuditLog` | `entries(limit=None, after_seq=0)`、`verify()`、`len(audit)` |
| `metrics` | `Metrics` | `inc(name, delta=1)`、`observe(name, value)`、`snapshot()`、`render_prometheus()` |
| `usage` | `UsageLedger` | 用量账本，`report_usage()` 的历史来源 |
| `idempotency` | `IdempotencyStore` | 进程内幂等结果；配置 Redis 时自动换成共享实现 |
| `probe_guard` | `ProbeGuard` | 代理验活的并发与限速保护 |
| `adapters` | `ResourceAdapterRegistry` | `register()` / `unregister(kind)` / `get(kind)` / `names()` |
| `storage_kind` / `coordination_kind` | `str` | 构造时给出的默认平面 |

事件对象的字段与事件类型见 [REST 事件](../rest-api/events.md)；Python 侧订阅到的是 `Event` 对象，可调用 `to_dict()` 转成与 REST 一致的 JSON。

## 同步与多进程

- 同步代码使用 [同步 Purser](../../guides/sync-facade.md) 的 `SyncPurser`：它在专属线程里运行同一个内核，方法阻塞并在超时时抛 `TimeoutError`。
- 同机多进程共享一套池使用 [LocalScheduler](../local-scheduler.md)：调度进程独占 `Purser`，worker 通过 `multiprocessing.connection` 调用。
- 两者都不可跨进程传递内核实例；跨进程共享存储与协调后端时，各进程仍应各自对账。
