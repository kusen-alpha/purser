# purSer API 契约（A2）

> 对应设计文档 4.4 / 4.5 的编码契约。所有写操作在 REST 层要求 `X-Operator`
> 头（缺省 `api`）；`force_revoke` / `admin-state` 要求 `reason`。

## `Purser`

```python
Purser(*, maintenance_enabled=True, event_queue_size=10_000,
       storage="memory", coordination="memory",
       redis_url=None, redis_client=None, sqlite_path=None,
       audit_path=None, secret_key=None)
```

- `storage`：系统记录层，`memory | sqlite | mysql`。资源主数据、重要状态、槽位与版本 CAS
  在此持久化，丢失即业务事故；sqlite 单机重启不丢，mysql 需 `pip install "purser[mysql]"`
  （aiomysql 懒加载，不装不影响导入）。
- `coordination`：调度记录层，`memory | redis`。在途租约/心跳/唤醒/缓存，丢失后由
  池在 `start()` 对账重建，只影响调度准确性，不影响资源基本信息。
- `redis_url`：传连接串时自动创建 `redis.asyncio.Redis` 客户端；`redis_client`
  注入时必须传 `redis.asyncio.Redis` 实例（异步客户端），供 coordination=redis 使用。
- `sqlite_path`：SQLite 数据库文件（缺省 `:memory:`），供 storage=sqlite 使用。
- `audit_path`：非空时审计哈希链同时写入 SQLite，重启后从库恢复并继续校验。
- `register_type(rtype)`：运行时注册资源类型插件（X7）。
- `create_pool(name, kind, *, config=None, **overrides) -> ResourcePool`
- `pool(name, kind=None, **overrides) -> ResourcePool`：已存在则返回，否则创建。
- `get_pool(name)` / `require_pool(name)` / `pool_names()`
- `start()` / `close()`：启动/停止事件总线、各池 reaper、后台验活与周期任务；支持 `async with Purser() as ps`。
- 属性：`bus`（EventBus）、`audit`（AuditLog）、`metrics`、`idempotency`、`probe_guard`、`types`。

## `ResourcePool`

```python
async def import_resources(records, *,
    dedup_key="id", source="manual", operator="system",
    fail_fast=False, import_validate=None, batch_id=None) -> ImportReport
async def acquire(req: AcquireRequest | None = None, **kwargs) -> Lease
async def acquire_many(n, *, req=None, mode="atomic", n_min=None, timeout=None) -> BatchLease
async def lease(req=None, **kwargs)          # async context manager
async def release(lease, *, outcome=UsageOutcome.OK, used=None) -> bool
async def renew_manual(lease) -> bool
async def force_revoke(rid, *, operator, reason, token=None) -> Lease
async def validate_now(rid, *, operator="system") -> HealthReport
async def ban / unban / suspend / resume / top_up / drain / restore / admin_set_state
async def update_resource(rid, *, payload=None, tags=None, max_slots=None, operator, reason)
async def move_resource(rid, target_pool, *, operator, reason)
async def recall_batch(batch_id, *, action="requalify", operator, reason)
async def run_maintenance(name)   # quota_reset|periodic_unban|lost_gc|validating_gc|quarantine_gc|archive_gc
async def get(rid) -> Resource | None
async def query(*, states=None, tags=None, limit=100, offset=0) -> list[Resource]
async def counts() -> dict[ResourceState, int]
async def stats() -> dict
```

### 获取语义

- 流程固定：**硬约束过滤 -> 分配器打分（Top-K）-> 加权随机首选 + 按分回退 -> 请求级校验 -> 首个成功原子占用**；候选全部被校验拒绝抛 `NoSuitableResource`。
- 空池（没有任何资源）立即抛 `PoolEmpty`；资源忙则进入公平等待队列，超时抛 `PoolExhausted`。
- `resource_id` 定向获取：被占用按 timeout 等待，超时抛 `ResourceBusy`。
- `acquire_many`：`atomic` 任一项不足则整体回滚；`best_effort` 拿多少算多少，低于 `n_min` 抛 `PoolExhausted`。
- 同 `consumer` 重复持有同一资源默认返回已有租约（`allow_duplicate=True` 可放开）。
- 全部租约带 `token`；归还必须 token 匹配，不匹配抛 `LeaseConflict`。

## `AcquireRequest`

```python
AcquireRequest(
    consumer=None, target=None, priority=0, slots=1,
    tags=frozenset(), timeout=None, probe=None, trace_id=None,
    use_limit=None, allow_duplicate=False, ignore_cooldown=False,
    resource_id=None, required_tag=None, affinity=None,
)
```

## `Lease`

```python
Lease(token, pool, resource_id, consumer=None, slots=1, started_at, expires_at,
      heartbeat_at, priority=0, use_limit=None, used=0, trace_id=None,
      outcome=UsageOutcome.OK, target=None, payload=None)
```

`lease()` 上下文管理器退出时自动以 `lease.outcome` 归还；心跳由 `LeaseManager`
自动续约（默认 TTL/3），后端拒绝续约立即触发 `lease_lost` 事件。

## `ResourceType` 插件

```python
class ResourceType(ABC):
    kind: str
    default_concurrency: int = 1
    secret_fields: tuple[str, ...] = ()

    async def generate(self, spec) -> Resource                 # 必实现
    async def validate(self, resource) -> HealthReport          # 必实现
    async def on_request_check(self, resource, req) -> CheckVerdict   # 默认 PASS
    async def on_release(self, resource, outcome) -> None             # 默认无操作
    async def on_state_changed(self, resource, old, new, reason) -> None
    async def on_error(self, context, error) -> None
```

## `StorageBackend` × `Coordination`

```python
class StorageBackend(ABC):          # 系统记录层：memory / sqlite
    async def get(pool, rid) -> Resource | None
    async def put(pool, resource) -> Resource            # upsert，version +1
    async def delete(pool, rid) -> bool
    async def query(pool, *, states=None, tags=None, limit=100, offset=0)
    async def counts(pool) -> dict[ResourceState, int]
    async def transition_acquire(pool, rid, slots) -> TransitionResult   # 槽位 CAS
    async def transition_release(pool, rid, slots) -> TransitionResult
    async def reset_usage(pool, rid) -> bool             # 对账：孤儿占用回 IDLE
    async def set_state(pool, rid, expected_version, *, expect_state=None, patch)
    async def scan(pool, *, states=None, batch=50)
    async def flush(pool)

class Coordination(ABC):            # 调度记录层：memory / redis
    async def register_lease(pool, lease)
    async def unregister_lease(pool, token) -> Lease | None
    async def get_lease(pool, token) -> Lease | None
    async def leases_for(pool, rid) -> list[Lease]
    async def renew_lease(pool, rid, token, expires_at) -> bool
    async def active(pool) -> list[Lease]
    async def flush(pool)
```

两个平面独立取值：`storage ∈ {memory, sqlite, mysql}`、`coordination ∈ {memory, redis}`，
池把二者组合使用。`sqlite` 用 stdlib `sqlite3` + 版本号 CAS，`resources` /
`resource_tags` 两表持久化主数据，payload 敏感字段 AES-GCM 落库，单机重启不丢；
`mysql` 用 `aiomysql` 实现同一契约（utf8mb4、`ON DUPLICATE KEY UPDATE` 版本自增、
连接懒建立，断连抛 `BackendUnavailable`）；
`meta_patch` 与内存存储同为合并语义。`redis` 协调用 `redis.asyncio`，每资源
租约哈希 + token 索引（不用阻塞式 `KEYS`），断连抛 `BackendUnavailable`。
异常重启后 `Pool.start()` 执行对账：协调端没有活租约的占用槽位自动回 `IDLE`，
资源基本信息保持准确。MySQL 集成测试由 `PURSER_TEST_MYSQL_URL` 环境变量门控。

## REST API（速览）

```text
GET  /healthz | /readyz | /metrics
GET  /pools                       # 池概览
POST /pools                       # 创建池 {name, kind, config?}
GET  /pools/{pool}                # 池统计
PATCH /pools/{pool}               # 热更新配置 {config: {...}}，即时重建 allocator/校验门/心跳/调度
GET  /pools/{pool}/resources      # ?state=&tag=&q=&limit=&offset=
POST /pools/{pool}/resources      # 导入 {records, import_validate?}，支持 Idempotency-Key
GET  /pools/{pool}/resources/{rid}   # ?include_secrets=true（需 admin）
PUT  /pools/{pool}/resources/{rid}   # 更新 payload/tags/max_slots
POST /pools/{pool}/resources/{rid}/{validate|ban|unban|suspend|resume|top-up|drain|restore}
POST /pools/{pool}/resources/{rid}/revoke      # force_revoke，需 reason
POST /pools/{pool}/resources/{rid}/admin-state # {target, reason}
DELETE /pools/{pool}/resources/{rid}           # 有租约->drain，否则归档
POST /pools/{pool}/leases       # 获取 {consumer?, target?, slots?, resource_id?, count?, timeout?}
GET  /pools/{pool}/leases       # 活跃租约
DELETE /pools/{pool}/leases/{token}  # {outcome?, used?}
GET  /pools/{pool}/events?after_seq=0   # SSE 事件流（游标续传）
POST /pools/{pool}/drain | /recall
```

鉴权：配置 `PURSER_API_KEY`（admin）与可选 `PURSER_READONLY_KEY`（readonly）
后，`Authorization: Bearer <key>`；写操作需要 admin。未配置密钥则开放访问。
