# ResourcePool

`ResourcePool` 由 [`Purser`](purser.md) 创建，承载资源主数据、槽位与租约。池配置字段见[配置](../configuration.md)；本页只描述方法契约。

## 导入与生成

```python
report = await pool.import_resources(
    records, *,
    dedup_key="id", source="manual", operator="system",
    fail_fast=False, import_validate=None, batch_id=None,
)
```

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `records` | — | `dict` 规格或已构造的 `Resource` 序列 |
| `dedup_key` | `"id"` | 用记录中的哪个键作为资源 id；`None` 表示不覆盖生成结果。`Resource` 记录先查 `payload`，再查 `meta` |
| `source` | `"manual"` | 写入 `meta["source"]`，用于批次与报表 |
| `operator` | `"system"` | 审计操作者 |
| `fail_fast` | `False` | 第一条生成错误后停止；默认收集到 `errors` 继续 |
| `import_validate` | 池配置 | `none` / `on_import` / `batch`，见[导入与验活](../../guides/import-and-validate.md) |
| `batch_id` | 自动生成 | 批次标识；同时写入每个资源的 `meta["batch_id"]` |

`dict` 记录中与 payload 字段并列的保留键会被拆出并应用到调度字段：`max_slots`、`usage_quota`、`expires_at`、`mutex_group`、`schedule_window`、`source`。未显式给 `max_slots` 时使用池配置值；`expires_in`、`usage_quota`、`total_quota` 等池默认值只在记录未提供时补齐。

重复导入同一 id 是**更新**而不是替换：

- 保留已有 `state`、槽位、健康度、配额、站点失败、已用计数与 `next_available_at`；
- 合并 `meta`，刷新 payload 与标签；
- 已是 `ARCHIVED` 的资源计入 `skipped`，不会被复活。

`ImportReport` 字段：

| 字段 | 说明 |
| --- | --- |
| `imported` / `updated` / `skipped` | 新建、更新、跳过数量 |
| `errors` | `{"line", "error"}` 列表；`line` 是输入下标 |
| `batch_id` | 本次批次标识 |
| `validation` | `on_import` 模式下逐条结果：`id` / `alive` / `latency_ms` / `error` / `action` |
| `total` | 属性，`imported + updated + skipped + len(errors)` |

`batch` 模式的验活结果不在 `validation` 中，需按 `batch_id` 查询验活任务。

```python
report = await pool.generate(specs, source="builtin", operator="system")
```

`generate()` 等价于 `import_resources(..., import_validate="on_import")`，适合先同步验活再返回的生成路径。

## 查询与遍历

```python
resource = await pool.get(rid)        # None when missing
resource = await pool.require(rid)    # raises ResourceNotFound
resources = await pool.query(
    states={ResourceState.IDLE}, tags=frozenset({"us"}),
    limit=100, offset=0, search="batch-1", sort="health", order="desc",
)
async for resource in pool.iter_resources(states=None, tags=None, search=None, batch_size=100):
    ...
```

- `query()` 的 `limit` / `offset` 必须 `>= 0`；`limit=0` 返回空列表。
- `tags` 是“全部包含”语义；`search` 匹配 id、kind、标签以及 `meta` 中的 `batch_id` / `source`。
- `sort` 仅支持 `id` / `health` / `recent`；默认 `id` 升序，其余默认降序。`order` 只能是 `asc` / `desc`。
- `iter_resources()` 过滤语义与 `query()` 一致，但按 id 顺序逐条 hydrate，不使用 offset，遍历期间的新增/删除不会造成窗口偏移。SQL 后端仍会在内部一次性读取匹配行，内存优势来自不构造完整结果列表。
- 读取边界统一执行 `ResourceType.hydrate()`，返回声明了 `payload_fields` 的类型化子类。

| 方法 | 说明 |
| --- | --- |
| `counts(by_name=False)` | 状态分布；默认枚举键，`by_name=True` 返回字符串键 |
| `count_matching(states=None, tags=None, search=None)` | 忽略分页的匹配数量 |
| `batches()` | 按 `meta["batch_id"]` 聚合的批次列表 |
| `leases(resource_id=None)` | 协调层活跃租约；包含其他实例签发者，其 `resource` 快照为 `None` |
| `stats()` | `PoolStats` 快照（见下表） |
| `describe(resource, secret=False)` | 面向 UI/日志的摘要；默认把整个 payload 掩码为 `***` |

`PoolStats` 字段：`name` / `kind` / `total` / `counts` / `allocatable` / `active_leases` / `waiting` / `avg_health` / `config`。`avg_health` 只统计有验活样本的资源，不受分页上限影响；没有样本时为 `None`。

## 运维动作

```python
await pool.ban(rid, reason="credential leak")
await pool.unban(rid, reason="rotated", revalidate=True)
await pool.suspend(rid, reason="vendor maintenance")
await pool.resume(rid, reason="back", revalidate=False)
await pool.top_up(rid, amount=100, reason="paid")
await pool.drain(rid, reason="retire")
await pool.admin_set_state(rid, target=ResourceState.SUSPENDED, reason="manual")
```

| 方法 | 前置状态 / 约束 | 效果 |
| --- | --- | --- |
| `ban` | 非 `BANNED` / `ARCHIVED` | 先回收该资源全部活跃租约（含其他实例），再置 `BANNED` |
| `unban` | 必须 `BANNED` | 置 `VALIDATING`；`revalidate=True` 立即验活，否则停在 `VALIDATING` 等待后续验活 |
| `suspend` | 必须 `IDLE` | 置 `SUSPENDED`，不可被获取 |
| `resume` | 必须 `SUSPENDED` | 默认回 `IDLE`；`revalidate=True` 置 `VALIDATING` 等待验活 |
| `top_up` | 资源必须有 `quota` 或 `usage_quota` | `amount` 必须 `> 0`；`QUOTA_EXHAUSTED` 回 `IDLE`，`usage_quota` 同步减少 `used_count` |
| `drain` | 已 `DRAINING` 时幂等 | 置 `DRAINING`，不再签发新租约；最后一个租约归还后归档 |
| `cancel_drain` | 必须 `DRAINING` | 经状态机 `UNDRAIN` 回到 `IDLE` / `ACTIVE` |
| `restore` | 必须 `ARCHIVED` | 置 `VALIDATING` 并立即验活 |
| `admin_set_state` | 目标 ∈ `IDLE` / `SUSPENDED` / `QUOTA_EXHAUSTED` / `VALIDATING` / `ARCHIVED`，且资源不忙 | 按状态机允许的迁移强制置状态；非法迁移抛 `InvalidTransition` / `BadState` |
| `run_maintenance(name)` | `name` 见下 | 立即执行一次维护扫描，返回 `{任务名: 处理数}` |

`top_up`、`drain`、`cancel_drain`、`restore`、`admin_set_state` 都会唤醒等待队列，让刚恢复可用的槽位被立即分配。

### 修改资源

```python
updated = await pool.update_resource(
    rid,
    payload={"host": "10.0.0.2"},      # 整体替换 payload
    tags=frozenset({"us"}),           # 整体替换标签
    max_slots=4,
    usage_quota=1000,                 # 省略则保持；None 清空
    expires_at=None,                  # None 清空
    mutex_group="account-group",
    schedule_window=["08:00", "20:00"],
    operator="alice", reason="rotate endpoint",
)
```

- 资源有活跃租约时拒绝修改（`BadState`）。
- `payload` / `tags` / `max_slots` 是整体替换；未传则保持。
- `usage_quota` / `expires_at` / `mutex_group` / `schedule_window` 支持“省略保持、传 `None` 清空”。
- `schedule_window` 必须是两个 `HH:MM` 字符串，支持跨夜窗口。
- 迁移资源用 `move_resource(rid, target_pool, reason=...)`：资源必须空闲、目标池 kind 相同、目标池不能已有同 id。

### 批次召回

```python
result = await pool.recall_batch(batch_id, action="retire", reason="vendor batch bad")
```

`action` 取 `requalify`（逐条 `validate_now`）、`isolate`（按验活失败路径隔离）、`retire`（逐条 `drain`）。返回 `{"action", "batch_id", "processed", "errors"}`。

### 维护任务

| 名称 | 作用 |
| --- | --- |
| `quota_reset` | 日配额到期的 `QUOTA_EXHAUSTED` 回 `IDLE` 并清零用量 |
| `periodic_unban` | `unban_mode` 允许时，封禁超过 `unban_after` 的资源自动解封 |
| `lost_gc` | `LOST` 超过 `lost_retention` 后归档 |
| `validating_gc` | `VALIDATING` 超过 `validate_timeout` 按验活失败处理 |
| `quarantine_gc` | 隔离到期的资源回 `IDLE`（或按策略回 `VALIDATING`） |
| `archive_gc` | 归档超过 `archive_retention` 的资源安全擦除 payload 后删除 |

`maintenance_enabled=True` 时这些扫描由后台周期任务自动执行；`run_maintenance()` 用于手动触发或测试。

## 验活

```python
verdict = await pool.validate_now(rid, operator="alice")

jobs = pool.validate_jobs()
job = pool.validate_job(job_id)
cancelled = pool.cancel_validate_job(job_id)
```

- `validate_now()` 对 `NEW` / `IDLE` 先进入 `VALIDATING` 再执行类型校验；`ACTIVE` 原地校验，不改变状态。`BANNED`、`DRAINING`、`ARCHIVED`、`SUSPENDED`、`QUOTA_EXHAUSTED` 拒绝。
- `import_validate="batch"` 创建的后台任务以 `batch_id` 为 job id；任务状态为 `running` / `cancelling` / `completed` / `cancelled`。
- `validate_jobs()` 按创建时间倒序返回；`validate_job()` 不存在时返回 `None`。
- `cancel_validate_job()` 停止未开始的条目，在途探测允许完成；未知 job 返回 `None`，已结束 job 返回 `cancelled=False`。
- `ValidateJobView` 字段：`id` / `pool` / `status` / `total` / `done` / `alive` / `failed` / `skipped` / `errors` / `created_at` / `updated_at` / `operator` / `pending`；取消视图额外带 `cancelled`。
- 终态任务只保留最近 100 个，更旧的自动清理。

逐条验活的判定与状态副作用见[公平性与健康度](../../guides/fairness-and-health.md)。

## 重配置与关闭

- `reconfigure(**updates)` 实时更新池参数并重建分配器、验活门限、心跳节奏与调度间隔，返回新的 `PoolConfig`。`storage` / `coordination` 不可在运行期修改，传入会抛 `ConfigError`。
- `close()` 停止该池的 reaper、后台任务与等待队列，并以 `PoolExhausted` 唤醒所有在途等待者；关闭后池不能再分配资源。
- `Purser.close()` 会依次关闭其所有池。
