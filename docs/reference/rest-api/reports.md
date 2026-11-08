# 报表与验活任务

## 用量报表

`GET /reports/usage` 把**已归还租约的历史用量**与**当前在租/池负载快照**合并成一个响应，只读密钥可用。

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `group_by` | `consumer` | `consumer` / `target` / `pool` / `kind` |
| `since` / `until` | 不限 | 闭合区间，Unix epoch 秒；`since > until` 返回 `422` |
| `pool` | 全部池 | 限定单池，不存在返回 `404` |
| `limit` | `20` | 分组行数上限，`1..500` |

```json
{
  "window": {"since": null, "until": null, "oldest": 1731000000.0,
             "newest": 1731000100.0, "events": 42, "retained": 42, "capacity": 50000},
  "group_by": "consumer",
  "totals": {"releases": 42, "used": 57, "slots": 42,
             "outcomes": {"ok": 40, "captcha": 2}},
  "groups": [
    {"key": "c1", "releases": 30, "used": 45, "slots": 30,
     "outcomes": {"ok": 29, "captcha": 1}}
  ],
  "groups_total": 3,
  "truncated": false,
  "active": {"group_by": "consumer",
             "groups": [{"key": "c2", "leases": 2, "slots": 2}]},
  "pools": [
    {"name": "proxy-a", "kind": "proxy", "total": 10,
     "counts": {"idle": 8, "active": 2}, "allocatable": 8,
     "active_leases": 2, "waiting": 0, "avg_health": 0.83}
  ]
}
```

约定：

- 数据源是进程内有界账本（`Purser(usage_ledger_size=...)`），满后淘汰最旧记录，进程重启清空。
- `used` 取归还时的 `used` 参数；未显式传值时按租约的 `used` 或 1 计。`force_revoke` 记 `outcome="preempted"`、`used=0`。
- 空分组键使用 `(anonymous)` / `(no target)`。
- `active` 与 `pools` 是实时快照，不受 `since` / `until` 影响，也不计入 `totals`。
- `truncated=true` 表示分组行被 `limit` 截断，`groups_total` 是完整分组数。

## 批量验活任务

`import_resources(import_validate="batch")` 会把本次导入交给后台任务，任务 id 就是导入报告里的 `batch_id`。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/pools/{pool}/validate-jobs` | 任务列表，按创建时间倒序 |
| `GET` | `/pools/{pool}/validate-jobs/{id}` | 单任务进度；不存在返回 404 |
| `POST` | `/pools/{pool}/validate-jobs/{id}/cancel` | 协作式取消；需写权限 |

```json
{
  "id": "8b1f...", "pool": "proxy-a", "status": "running",
  "total": 120, "done": 30, "pending": 90,
  "alive": 28, "failed": 2, "skipped": 0,
  "errors": [{"rid": "r7", "error": "probe timeout"}],
  "created_at": 1731000000.0, "updated_at": 1731000012.0, "operator": "alice"
}
```

- 生命周期：`running → cancelling → cancelled`，或 `running → completed`。
- 取消是协作式的：队列中尚未开始的资源计 `skipped`，已在探测中的执行完成后才收尾。
- 取消已完成/已取消的任务是幂等空操作，返回当前任务并带 `"cancelled": false`。
- 并发度沿用池的 `validate_concurrency`；`errors` 最多保留 100 条。
- 任务表在内存中，最多保留 100 个终态任务，重启后清空。
- 进度通过 SSE 发布 `validate_job_started` / `validate_job_finished`。

另可查询导入批次：`GET /pools/{pool}/batches`。
