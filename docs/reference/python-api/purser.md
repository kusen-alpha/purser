# Purser

`Purser` 是进程级入口：持有池注册表、资源类型注册表、事件总线、审计、指标与用量账本。一个进程通常只创建一个实例；需要同机多进程共享时使用 [LocalScheduler](../local-scheduler.md)。

## 构造

```python
Purser(
    *, maintenance_enabled=True, event_queue_size=10_000,
    usage_ledger_size=50_000, storage="memory", coordination="memory",
    mysql_url=None, mysql_connection=None, redis_url=None, redis_client=None,
    sqlite_path=None, audit_path=None, secret_key=None, secret_key_id="1",
    previous_secret_keys=None,
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

建池时 `max_slots` 未指定则取资源类型的 `default_concurrency`；`secret_fields` 为空则继承资源类型的声明。存储与协调后端的组装规则见[StorageBackend / Coordination](backends.md)。

## 注册资源类型

```python
ps.register_type(MyType())
```

- 内置 `proxy` / `account` 已在构造时注册。
- `kind` 必须非空；同一 `kind` 再次注册会覆盖先前实例。
- 插件契约见[Resource / ResourceType](resources.md)。

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
| `types` | `ResourceTypeRegistry` | `register()` / `unregister(kind)` / `get(kind)` / `names()` |
| `storage_kind` / `coordination_kind` | `str` | 构造时给出的默认平面 |

事件对象的字段与事件类型见 [REST 事件](../rest-api/events.md)；Python 侧订阅到的是 `Event` 对象，可调用 `to_dict()` 转成与 REST 一致的 JSON。

## 同步与多进程

- 同步代码使用 [同步 Purser](../../guides/sync-facade.md) 的 `SyncPurser`：它在专属线程里运行同一个内核，方法阻塞并在超时时抛 `TimeoutError`。
- 同机多进程共享一套池使用 [LocalScheduler](../local-scheduler.md)：调度进程独占 `Purser`，worker 通过 `multiprocessing.connection` 调用。
- 两者都不可跨进程传递内核实例；跨进程共享存储与协调后端时，各进程仍应各自对账。
