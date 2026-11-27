# ResourcePool

`ResourcePool` 由 [`Purser`](purser.md) 创建，承载资源主数据、槽位与租约。池配置字段见[配置](../configuration.md)；本页只描述方法契约。签名以嵌入式异步 `ResourcePool` 为准。

## 生命周期

### `start()` / `started` {#pool-start}

**用途**：启动池的 reaper、后台验活、唤醒与周期维护；通常由 `Purser.start()` 或首次获取自动触发。

**签名**

```python
async def start(self) -> None: ...
@property
def started(self) -> bool: ...
```

**返回与异常**：`start()` 返回 `None`；`started` 返回池运行时是否已启动且尚未关闭。启动会执行一次租约对账，把存储侧槽位对齐到协调端真实存活租约。

### `close()` {#pool-close}

**用途**：停止池的后台任务与等待队列，并唤醒在途等待者。

**签名**：`async def close() -> None`

**返回与异常**：返回 `None`。用 `PoolExhausted` 唤醒全部在途等待者；关闭后池不能再分配资源。`Purser.close()` 会依次关闭其所有池。

## 导入与生成

### `import_resources()` {#import-resources}

**用途**：批量导入字典规格或已构造的 `Resource`，支持去重更新、失败收集和可选验活。

**签名**

```python
async def import_resources(
    self,
    records: Sequence[dict[str, Any] | Resource],
    *,
    dedup_key: str | None = "id",
    source: str = "manual",
    operator: str = "system",
    fail_fast: bool = False,
    import_validate: ImportValidate | str | None = None,
    batch_id: str | None = None,
) -> ImportReport: ...
```

**参数**

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `records` | `Sequence[dict 或 Resource]` | 必填 | 资源规格或已构造资源序列 |
| `dedup_key` | <code>str &#124; None</code> | `"id"` | 用记录中的哪个键作为资源 id；`None` 表示不覆盖生成结果。`Resource` 记录先查 `payload`，再查 `meta` |
| `source` | `str` | `"manual"` | 写入 `meta["source"]`，用于批次与报表 |
| `operator` | `str` | `"system"` | 审计操作者 |
| `fail_fast` | `bool` | `False` | 第一条生成错误后停止；默认收集到 `errors` 后继续 |
| `import_validate` | `"auto"` / `"none"` / `"on_import"` / `"batch"` / `None` | 池配置（默认 `auto`） | 导入验活策略，见[导入与验活](../../guides/import-and-validate.md) |
| `batch_id` | <code>str &#124; None</code> | 自动生成 | 批次标识；同时写入每个资源的 `meta["batch_id"]` |

**返回**：`ImportReport`。

| 字段 | 说明 |
| --- | --- |
| `imported` / `updated` / `skipped` | 新建、更新、跳过数量 |
| `errors` | `{"line", "error"}` 列表；`line` 是输入下标 |
| `batch_id` | 本次批次标识 |
| `validation` | `on_import` 模式下逐条结果：`id` / `alive` / `latency_ms` / `error` / `action` |
| `total` | 属性，`imported + updated + skipped + len(errors)` |
| `to_dict()` | 返回包含以上字段和 `total` 的 JSON 友好字典 |

**示例**

```python
report = await pool.import_resources(
    records=[{"id": "r1", "tags": ["us"]}],
    import_validate="on_import",
)
payload = report.to_dict()
```

**注意**

- 字典记录中与 payload 字段并列的保留键会被拆出并应用到调度字段：`max_slots`、`usage_quota`、`quota_limit`、`expires_at`、`next_available_at`、`mutex_group`、`schedule_window`、`priority`、`preference`、`source`。
- 重复导入同一 id 是**更新**而不是替换：保留已有状态、槽位、健康度、配额、失败标记、已用计数与 `next_available_at`；合并 `meta`，刷新 payload 与标签；`ARCHIVED` 资源计入 `skipped`，不会复活。
- 显式 `batch`（以及默认 `auto` 下超过 10 条自动转后台）的验活结果不在 `validation` 中，需按 `batch_id` 查询验活任务。
- 导入边界会执行 `ResourceAdapter.adopt()` 并强制必填字段；运行期 `update_resource(payload=...)` 不走这条严格路径。

### `generate()` {#pool-generate}

**用途**：按适配器规则从规格生成资源，并立即导入验活。

**签名**

```python
async def generate(
    self,
    specs: Sequence[dict[str, Any] | Resource],
    *,
    source: str = "builtin",
    operator: str = "system",
) -> ImportReport: ...
```

**参数**

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `specs` | `Sequence[dict 或 Resource]` | 必填 | 适配器输入规格 |
| `source` | `str` | `"builtin"` | 写入资源来源 |
| `operator` | `str` | `"system"` | 审计操作者 |

**返回**：`ImportReport`。等价于 `import_resources(..., import_validate="on_import")` 的生成路径。

## 查询与遍历

### `get()` / `require()` {#pool-get}

**用途**：读取单个资源的类型化快照。

**签名**

```python
async def get(self, rid: str) -> Resource | None: ...
async def require(self, rid: str) -> Resource: ...
```

**参数**：`rid` 为资源 id。

**返回与异常**：`get()` 不存在时返回 `None`；`require()` 不存在时抛 `ResourceNotFound`。两者都执行 `ResourceAdapter.hydrate()`。

### `query()` {#pool-query}

**用途**：按状态、标签和关键字筛选资源，并支持分页与排序。

**签名**

```python
async def query(
    self,
    *,
    states: set[ResourceState] | None = None,
    tags: frozenset[str] | None = None,
    limit: int = 100,
    offset: int = 0,
    search: str | None = None,
    sort: str | None = None,
    order: str | None = None,
) -> list[Resource]: ...
```

**参数**

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `states` | <code>set[ResourceState] &#124; None</code> | `None` | 状态集合 |
| `tags` | <code>frozenset[str] &#124; None</code> | `None` | 资源必须包含全部标签 |
| `limit` | `int` | `100` | 返回上限，必须 `>= 0`；`0` 返回空列表 |
| `offset` | `int` | `0` | 分页偏移，必须 `>= 0` |
| `search` | <code>str &#124; None</code> | `None` | 匹配 id、kind、标签及 `meta` 中的 `batch_id` / `source` |
| `sort` | <code>str &#124; None</code> | `None` | `id` / `health` / `recent`；默认 `id` 升序，其余默认降序 |
| `order` | `"asc"` / `"desc"` / `None` | `None` | 排序方向 |

**返回与异常**：返回 `list[Resource]`。非法分页、排序字段或方向会抛 `ConfigError`。

**示例**

```python
from purser import ResourceState

resources = await pool.query(
    states={ResourceState.IDLE},
    tags=frozenset({"us"}),
    limit=100,
    offset=0,
    search="batch-1",
    sort="health",
    order="desc",
)
```

### `iter_resources()` {#pool-iter-resources}

**用途**：按 id 顺序流式遍历匹配资源，避免构造完整结果列表。

**签名**

```python
def iter_resources(
    self,
    *,
    states: set[ResourceState] | None = None,
    tags: Iterable[str] | None = None,
    search: str | None = None,
    batch_size: int = 100,
) -> AsyncGenerator[Resource, None]: ...
```

**参数**：过滤语义与 `query()` 一致；`batch_size` 控制存储批次大小。

**返回**：异步生成器。遍历期间的新增/删除不会造成 offset 窗口偏移；SQL 后端仍可能在内部一次性读取匹配行，内存优势来自不构造完整列表。

### `count_matching()` / `counts()` / `stats()` {#pool-stats}

| 方法 | 签名 | 返回与说明 |
| --- | --- | --- |
| `count_matching` | `async def count_matching(*, states=None, tags=None, search=None) -> int` | 忽略分页的匹配数量 |
| `counts` | `async def counts(*, by_name: bool = False) -> dict` | 状态分布；默认枚举键，`by_name=True` 返回字符串键 |
| `stats` | `async def stats() -> PoolStats` | 池负载与健康快照 |

`PoolStats` 字段：`name` / `kind` / `profile` / `total` / `counts` / `allocatable` / `active_leases` / `waiting` / `avg_health` / `config`。`avg_health` 只统计有验活样本的资源，没有样本时为 `None`。

### `leases()` / `lease_by_token()` {#pool-leases}

**用途**：读取协调层活跃租约，支持按资源或 token 查询。

**签名**

```python
async def leases(self, rid: str | None = None) -> list[Lease]: ...
async def lease_by_token(self, token: str) -> Lease | None: ...
```

**返回**：活跃租约列表或单份租约；不存在时 `lease_by_token()` 返回 `None`。`leases()` 包含其他实例签发的租约，其 `resource` 快照可能为 `None`。`lease_by_token()` 优先返回本地对象，便于 `renew()` / `release()` 原地更新。

### `batches()` / `describe()` {#pool-batches-describe}

| 方法 | 签名 | 说明 |
| --- | --- | --- |
| `batches` | `async def batches() -> list[dict[str, Any]]` | 按 `meta["batch_id"]` 聚合的批次列表 |
| `describe` | `def describe(resource: Resource, *, secret: bool = False) -> dict[str, Any]` | 面向 UI/日志的摘要；默认把整个 payload 掩码为 `***` |

获取、续约与归还的完整契约见[AcquireRequest / Lease / BatchLease](acquisition.md)。

## `pool.admin` 管理操作

管理操作按用途分为“常用资源维护”和“修复/批量处置/运维”。数据面方法仍在池顶层。`SyncPool.admin` 提供全部 18 项；`ThreadPool.admin` / `RemotePool.admin` 目前只覆盖 `ban` / `unban`，其余操作可经 `client.pool_call` 路由。

### 验活

#### `validate_now()` {#admin-validate-now}

**签名**：`async def validate_now(rid: str, *, operator: str = "system") -> HealthReport`

**用途**：立即验活并应用结果。`NEW` / `IDLE` 先进入 `VALIDATING`；`ACTIVE` 原地校验。`BANNED`、`DRAINING`、`ARCHIVED`、`SUSPENDED`、`QUOTA_EXHAUSTED` 拒绝。

#### `validate_check()` {#admin-validate-check}

**签名**：`async def validate_check(rid: str, check: str) -> HealthReport`

**用途**：按名称运行单个适配器校验项，不改变资源状态。未知检查项抛 `ConfigError`。

### 常用资源维护

| 方法 | 签名 | 关键语义 |
| --- | --- | --- |
| `ban` | `async def ban(rid, *, reason, operator="system") -> None` | 先回收该资源全部活跃租约，再置 `BANNED`；非 `BANNED` / `ARCHIVED` 可用 |
| `unban` | `async def unban(rid, *, operator="system", reason="manual unban", revalidate=True) -> None` | 必须 `BANNED`；置 `VALIDATING`，`revalidate=True` 立即验活 |
| `suspend` | `async def suspend(rid, *, operator="system", reason) -> None` | 必须 `IDLE`；置 `SUSPENDED` |
| `resume` | `async def resume(rid, *, operator="system", reason="manual resume", revalidate=False) -> None` | 必须 `SUSPENDED`；默认回 `IDLE`，`revalidate=True` 先验活 |
| `top_up` | `async def top_up(rid, *, operator="system", amount=None, reason="manual top_up") -> None` | 见下方参数说明 |
| `drain` | `async def drain(rid, *, operator="system", reason) -> None` | 停止新获取；最后一租约归还后归档；已 `DRAINING` 时幂等 |
| `cancel_drain` | `async def cancel_drain(rid, *, operator="system", reason="cancel drain") -> None` | 必须 `DRAINING`；回到 `IDLE` / `ACTIVE` / `FULL` |
| `update_resource` | `async def update_resource(rid, *, payload=None, tags=None, max_slots=None, usage_quota=省略, quota_limit=省略, expires_at=省略, next_available_at=省略, mutex_group=省略, schedule_window=省略, priority=省略, preference=省略, operator="system", reason) -> Resource` | 有活跃租约时拒绝；省略保持，传 `None` 清空调度字段 |

#### `top_up()` {#admin-top-up}

**用途**：补充资源配额，使 `QUOTA_EXHAUSTED` 资源回到 `IDLE`。

**参数**

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `rid` | `str` | 必填 | 资源 id |
| `operator` | `str` | `"system"` | 审计操作者 |
| `amount` | <code>int &#124; None</code> | `None` | 显式传正数时补充周期配额；`None` 把 `quota.used` 清零 |
| `reason` | `str` | `"manual top_up"` | 操作原因 |

**返回与异常**：返回 `None`。没有 `quota` 和 `usage_quota` 时抛 `BadState`；`amount <= 0` 抛 `ConfigError`。只有同时存在 `usage_quota` 且显式传 `amount` 时，才减少 `used_count`。

#### `update_resource()` {#admin-update-resource}

**用途**：修改 payload、标签和调度字段，不替换资源主键。

**签名**

```python
async def update_resource(
    self,
    rid: str,
    *,
    payload: dict[str, Any] | None = None,
    tags: frozenset[str] | None = None,
    max_slots: int | None = None,
    usage_quota: Any = ...,
    quota_limit: Any = ...,
    expires_at: Any = ...,
    next_available_at: Any = ...,
    mutex_group: Any = ...,
    schedule_window: Any = ...,
    priority: Any = ...,
    preference: Any = ...,
    operator: str = "system",
    reason: str,
) -> Resource: ...
```

**参数**

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `rid` | 必填 | 资源 id |
| `payload` / `tags` / `max_slots` | `None` | 整体替换；未传则保持 |
| `usage_quota` / `quota_limit` / `expires_at` / `next_available_at` / `mutex_group` / `schedule_window` / `priority` / `preference` | 省略 | 省略保持，传 `None` 清空 |
| `operator` | `"system"` | 审计操作者 |
| `reason` | 必填 | 操作原因 |

**返回与异常**：返回更新后的 `Resource`。有活跃租约时抛 `BadState`；非法值抛 `ConfigError`。

**注意**：`payload` 通过 `ResourcePatch` / payload setter 更新，不走导入时的 `adopt(require=True)`，不要假设必填字段会在这里重新严格校验。`schedule_window` 必须是两个 `HH:MM` 字符串，支持跨夜。

**示例**

```python
updated = await pool.admin.update_resource(
    rid=rid,
    payload={"host": "10.0.0.2"},
    tags=frozenset({"us"}),
    max_slots=4,
    usage_quota=1000,
    operator="alice",
    reason="rotate endpoint",
)
```

### 修复、批量处置与运维

| 方法 | 签名 | 关键语义 |
| --- | --- | --- |
| `admin_set_state` | `async def admin_set_state(rid, *, target, operator="system", reason) -> None` | 目标限 `IDLE` / `SUSPENDED` / `QUOTA_EXHAUSTED` / `VALIDATING` / `ARCHIVED`，且资源不忙 |
| `move_resource` | `async def move_resource(rid, target_pool, *, operator="system", reason) -> Resource` | 资源必须空闲、目标 kind 相同且无同 id；先写目标再删源，非原子 |
| `recall_batch` | `async def recall_batch(batch_id, *, action="requalify", operator="system", reason) -> dict` | `requalify` / `isolate` / `retire`；返回 `action` / `batch_id` / `processed` / `errors` |
| `run_maintenance` | `async def run_maintenance(name) -> dict[str, int]` | 立即执行一项维护扫描，未知名称抛 `ConfigError` |
| `force_revoke` | `async def force_revoke(rid, *, operator="system", reason, token=None) -> Lease` | 完整参数与失败语义见[强制撤销](acquisition.md#force-revoke) |

`admin.resume`、`admin.top_up`、`admin.cancel_drain`、`admin.admin_set_state`、`admin.force_revoke` 会立即唤醒等待队列；`drain` 与 `restore_resource` 不主动唤醒。

以下修复操作保留汇总，需要时查源码契约：

| 方法 | 效果 |
| --- | --- |
| `apply_defaults(*, operator="system", reason="apply pool defaults")` | 给缺失字段的资源回填池默认值 |
| `switch_resource(rid, *, operator="system", reason="manual switch")` | 调用适配器 `switch()`，同 id 原地更新，新 id 导入并归档旧行 |
| `restore_resource(rid, *, operator="system", reason="manual restore")` | 必须 `ARCHIVED`；置 `VALIDATING` 并立即验活 |

### 维护任务

| 名称 | 作用 |
| --- | --- |
| `quota_reset` | 日配额到期的 `QUOTA_EXHAUSTED` 回 `IDLE` 并清零用量 |
| `periodic_unban` | `unban_mode` 允许时，封禁超过 `unban_after` 的资源自动解封 |
| `lost_gc` | `LOST` 超过 `lost_retention` 后归档 |
| `validating_gc` | `VALIDATING` 超过 `validate_timeout` 按验活失败处理 |
| `quarantine_gc` | 隔离到期的资源回 `IDLE`（或按策略回 `VALIDATING`） |
| `archive_gc` | 归档超过 `archive_retention` 的资源安全擦除 payload 后删除 |
| `expire_gc` | 过期超过 `expired_retention` 的 `SUSPENDED` 资源归档；默认关闭 |
| `reconcile` | 领导者选举后执行一次槽位对账 |

`maintenance_enabled=True` 时这些扫描由后台周期任务自动执行；`admin.run_maintenance()` 用于手动触发或测试。

## 验活任务

| 方法 | 签名 | 返回与说明 |
| --- | --- | --- |
| `validate_jobs` | `def validate_jobs() -> list[ValidateJobView]` | 按创建时间倒序返回后台验活任务 |
| `validate_job` | <code>def validate_job(job_id: str) -> ValidateJobView&#124;None</code> | 不存在时返回 `None` |
| `cancel_validate_job` | <code>def cancel_validate_job(job_id: str) -> ValidateJobCancelView&#124;None</code> | 停止未开始条目；在途探测允许完成；未知 job 返回 `None`，已结束 job 返回 `cancelled=False` |

显式 `import_validate="batch"`（或默认 `auto` 超过 10 条）创建的任务以 `batch_id` 为 job id；状态为 `running` / `cancelling` / `completed` / `cancelled`。终态任务只保留最近 100 个。

`ValidateJobView` 字段：`id` / `pool` / `status` / `total` / `done` / `alive` / `failed` / `skipped` / `errors` / `created_at` / `updated_at` / `operator` / `pending`；取消视图额外带 `cancelled`。

## 重配置

### `reconfigure()` {#pool-reconfigure}

**用途**：实时更新池参数，并重建分配器、验活门限、心跳节奏与调度间隔。

**签名**：`def reconfigure(self, **updates: Any) -> PoolConfig`

**参数**：`updates` 为可运行时修改的池配置字段。

**返回与异常**：返回新的 `PoolConfig`。`storage` / `coordination` / `secret_fields` / `event_queue_size` / `schema_version` 等不可变字段传入时抛 `ConfigError`；非法值同样抛 `ConfigError`。
