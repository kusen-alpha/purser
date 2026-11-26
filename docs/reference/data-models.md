# 数据模型

本页把跨 API 出现的结构收敛到一张地图：字段的完整语义在各自页面，这里只回答“它长什么样、在哪一层”。

## 核心对象

| 对象 | 所在层 | 说明 | 详细 |
| --- | --- | --- | --- |
| `Resource` | 存储层 | 一条资源的主数据与调度字段 | [Resource](python-api/resources.md) |
| `Lease` / `LeaseView` | 协调层 + 返回视图 | 一次占用的凭据与快照 | [AcquireRequest / Lease](python-api/acquisition.md) |
| `PoolStats` | 返回视图 | 池的瞬时容量与负载 | [ResourcePool](python-api/pools.md) |
| `ImportReport` | 返回视图 | 导入结果与批次 | [ResourcePool](python-api/pools.md) |
| `Event` | 事件总线 | 状态/租约通知 | [事件](events.md) |
| `AuditEntry` | 审计 | 追加写的操作记录 | [审计](audit.md) |
| `UsageReport` | 返回视图 | 内存用量账本聚合 | [Reports](rest-api/reports.md) |
| `ValidateJobView` | 返回视图 | 后台验活任务 | [ResourcePool](python-api/pools.md) |
| `BundleLease` | 返回视图 | 跨池组合租约 | [组合租约](../guides/bundle-leases.md) |

## `Resource`

存储形态由 `Resource.to_dict()` 给出；`state` 是字符串，`tags` 排序后输出：

```json
{
  "id": "proxy://1.2.3.4:8080",
  "kind": "proxy",
  "state": "idle",
  "version": 3,
  "tags": ["http", "region:us"],
  "concurrency": {"max_slots": 1, "used_slots": 0},
  "health": {
    "score": 0.82,
    "samples": 12,
    "last_latency_ms": 90.2,
    "last_checked_at": 1764000000.0,
    "last_error": null,
    "consecutive_failures": 0
  },
  "site_failures": {},
  "quota": {"limit": 100, "used": 12, "reset_at": 1764057600.0},
  "usage_quota": 1000,
  "used_count": 57,
  "expires_at": null,
  "next_available_at": null,
  "mutex_group": null,
  "schedule_window": null,
  "meta": {"source": "manual", "batch_id": "..."},
  "payload": {"host": "1.2.3.4", "port": 8080, "protocol": "http", "country": "US"}
}
```

要点：

- **payload 是合并视图**：类型化字段（`host` / `port` / …）在 `to_dict()` 时折回 `payload`；未声明的键也保留在 `payload`。
- **`used_slots` 与状态联动**：`max_slots=1` 时占用即 `full`；大于 1 时占用中为 `active`。`allocatable` 只在 `idle` / `active` 且有剩余槽位时为真。
- **`health.score`** 是 0–1 的 EWMA；冷启动为 `0.5`，不是“已验证通过”。
- **`meta`** 包含框架写入的审计信息（`source` / `batch_id` / `banned_at` / `quarantine_until` 等）。`user_meta` 只保留用户键，`system_meta` 反之。
- **敏感字段**：密码、cookie、token 在存储中以加密形式存在；`describe(secret=False)` 默认把整个 payload 掩码。备份包含明文，见[安全模型](../guides/security.md)。

## `Lease`

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `token` | `str` | 唯一凭据 |
| `pool` / `resource_id` | `str` | 所属池与资源 |
| `consumer` / `target` | `str \| None` | 请求上下文 |
| `slots` | `int` | 占用槽位 |
| `started_at` / `expires_at` / `heartbeat_at` | `float` | epoch 秒 |
| `priority` | `int` | 获取优先级 |
| `use_limit` / `used` | `int \| None` / `int` | 次数上限与已用 |
| `trace_id` | `str \| None` | 追踪 id |
| `outcome` | `UsageOutcome` | 归还结果 |
| `payload` | `dict \| None` | 获取时 payload 副本 |
| `resource` | `Resource \| None` | 本进程类型化快照；跨进程可能为 `None` |

REST 的租约视图只包含上表除 `pool`、`heartbeat_at`、`resource` 之外的字段；`replayed` 只出现在幂等重放的响应信封上，不在租约对象里。以 OpenAPI 为准。

## `PoolStats`

```python
{
    "name": "proxies", "kind": "proxy", "profile": None,
    "total": 100, "counts": {"idle": 80, "active": 10, "quarantined": 5, ...},
    "allocatable": 90, "active_leases": 12, "waiting": 3,
    "avg_health": 0.78, "config": {...},
}
```

`avg_health` 只统计有验活样本的资源；没有样本时为 `None`。`profile` 只记录创建时选择的具名预设，实际生效值看 `config`。

## `ImportReport`

```python
{
    "imported": 10, "updated": 2, "skipped": 1,
    "errors": [{"line": 3, "error": "port is required"}],
    "batch_id": "b-...",
    "validation": [{"id": "r1", "alive": true, "latency_ms": 12.3,
                    "error": None, "action": "ok"}],
    "total": 13,
}
```

`total = imported + updated + skipped + len(errors)`。后台验活结果不在 `validation` 里，需按 `batch_id` 查任务。

## `UsageReport`

| 字段 | 说明 |
| --- | --- |
| `window` | `since` / `until` / `oldest` / `newest` / `events` / `retained` / `capacity` |
| `group_by` | `consumer` / `target` / `pool` / `kind` |
| `totals` | `releases` / `used` / `slots` / `outcomes` |
| `groups` | 每组 `key` 与同样四项 |
| `groups_total` / `truncated` | 分组是否被 `limit` 截断 |
| `active` | 当前活跃租约的分组快照 |
| `pools` | 每池瞬时容量（`PoolUsage`） |

历史来自有界内存账本，重启清空；`active` 与 `pools` 是实时读取。见[数据保留](../guides/data-retention.md)。

## `ValidateJobView`

```python
{
    "id": "b-...", "pool": "proxies", "status": "running",
    "total": 100, "done": 42, "alive": 40, "failed": 2, "skipped": 0,
    "errors": [{"rid": "r9", "error": "..."}],
    "created_at": 1764000000.0, "updated_at": 1764000030.0,
    "operator": "alice", "pending": 58,
}
```

`status`：`running` / `cancelling` / `completed` / `cancelled`。`errors` 最多保留 100 条。取消视图额外带 `cancelled`。

## `Event` / `AuditEntry`

两者的字段与序列化见[事件](events.md)与[审计](audit.md)。区分：

- `Event` 是**瞬时通知**，`seq` 进程内递增、重启归零，不保证投递。
- `AuditEntry` 是**持久记录**，`seq` 在链内连续，配置 `audit_path` 后跨重启延续。

## 版本与兼容

这些结构是 Python API 的返回契约，字段只增不减（major 版本除外）。REST 视图是独立的序列化层，可能包含派生字段或省略内部字段；以 `/docs` 的 OpenAPI 为准。见[版本与弃用政策](stability.md)。