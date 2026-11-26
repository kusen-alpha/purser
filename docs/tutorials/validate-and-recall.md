# 教程：验活、召回与封禁

目标：把“导入成功”变成“可放心分配”，并在批次变坏时能整批处置、在个别资源失败时能自动隔离与恢复。

## 0. 先选导入策略

| 策略 | 导入何时返回 | 适合 |
| --- | --- | --- |
| `auto`（默认） | ≤10 条同步验活；更多立即返回，后台验活 | 大多数场景 |
| `on_import` | 全部同步验活完 | 小批量、必须“返回即可用” |
| `batch` | 立即返回 | 大批量、可接受稍后可用 |
| `none` | 立即返回，资源停在 `NEW` | 离线预装，稍后手动验活 |

`auto` 的阈值是 10 条。首次 `acquire()` 会等待 `NEW` / `VALIDATING` 的资源完成验活，直到请求超时；但 `none` 且没有验活任务时，`acquire()` 会立即抛 `PoolExhausted` 并提示如何推进，不会空等。

```python
report = await pool.import_resources(records, import_validate="auto")
print(report.imported, report.updated, report.skipped, report.batch_id)
print(report.validation)   # 同步部分的逐条结果
```

## 1. 看后台验活任务

超过 10 条（或显式 `batch`）时，`report.batch_id` 就是任务 id：

```python
job = pool.validate_job(report.batch_id)
while job is not None and job.status in ("running", "cancelling"):
    await asyncio.sleep(1)
    job = pool.validate_job(report.batch_id)
print(job.status, job.done, job.alive, job.failed, job.skipped)
```

`ValidateJobView` 字段：`id` / `pool` / `status` / `total` / `done` / `alive` / `failed` / `skipped` / `errors` / `pending` / `created_at` / `updated_at` / `operator`。终态任务只保留最近 100 个。

REST 等价入口：

```bash
curl "http://127.0.0.1:8000/pools/proxies/validate-jobs"
curl "http://127.0.0.1:8000/pools/proxies/validate-jobs/<job_id>"
curl -X POST "http://127.0.0.1:8000/pools/proxies/validate-jobs/<job_id>/cancel"
```

取消只停止尚未开始的条目，在途探测允许完成。

## 2. 单条验活与命名检查

```python
await pool.admin.validate_now("proxy://1.2.3.4:8080", operator="alice")

# 只跑某个命名检查项，不改变状态/健康
result = await pool.admin.validate_check("proxy://1.2.3.4:8080", "check_expiry")
```

`validate_now()` 的边界：

- `NEW` / `IDLE` 先进入 `VALIDATING`，再执行适配器校验；
- `ACTIVE` 原地校验，不改变状态；
- `BANNED` / `DRAINING` / `ARCHIVED` / `SUSPENDED` / `QUOTA_EXHAUSTED` 拒绝。

命名检查项用 `@validator` 声明；`both=True` 的检查会随生命周期验活一起执行，失败进入隔离流程；`both=False` 只能按名手动触发，不改状态。见[自定义资源适配器](../guides/custom-resource-adapter.md)。

## 3. 整批召回

当某个导入批次整体变坏（供应商换池、账号批量失效）时：

```python
result = await pool.admin.recall_batch(
    report.batch_id, action="retire", reason="vendor batch bad"
)
print(result)   # {"action", "batch_id", "processed", "errors"}
```

| `action` | 行为 |
| --- | --- |
| `requalify` | 逐条 `validate_now`，重新判定 |
| `isolate` | 按验活失败路径进入隔离，保留资源等待恢复 |
| `retire` | 逐条 `drain`，停止新分配，存量租约归还后归档 |

召回是逐条执行的，部分失败会记入 `errors`，不会因为一条失败而中止整批。

## 4. 隔离、封禁与恢复

失败不是立刻“删除”，而是按退避升级：

```text
validate 失败 → QUARANTINED（指数退避）→ 连续失败达阈值 → BANNED
```

关键参数（建池时传或 `PATCH /pools/{pool}` 热更新）：

| 参数 | 默认 | 作用 |
| --- | --- | --- |
| `quarantine.max_failures` | `3` | 连续失败达到多少次升级封禁 |
| `quarantine.base` / `cap` / `jitter` | `30` / `1800` / `10` 秒 | 隔离退避 |
| `quarantine.trial_hours` | `24` | 新资源试用期内阈值减半 |
| `unban_mode` | `mixed` | `manual_only` / `auto_after_ttl` / `mixed` |
| `unban_after` | `86400` 秒 | 自动解封等待时长 |
| `requalify_on_release` | `False` | 隔离到期后先验活再回 `IDLE` |

人工处置：

```python
await pool.admin.ban(rid, reason="credential leak")
await pool.admin.unban(rid, reason="rotated", revalidate=True)

await pool.admin.suspend(rid, reason="vendor maintenance")
await pool.admin.resume(rid, reason="back", revalidate=False)

await pool.admin.drain(rid, reason="retire")
await pool.admin.cancel_drain(rid, reason="keep")

await pool.admin.restore_resource(rid)   # ARCHIVED → VALIDATING → 立即验活
```

- `ban` 会先回收该资源的全部活跃租约（包括其他实例签发的），再置 `BANNED`。
- `unban` 要求资源处于 `BANNED`，默认进入 `VALIDATING`；`revalidate=True` 立即探测。
- `drain` 只停止新分配，不会立刻打断在途租约。

## 5. 用事件驱动处置

订阅事件总线，把“验活任务结束”“资源被封”接进告警或工单：

```python
from purser import Purser

ps = Purser()

async def on_event(event):
    if event.type == "validate_job_finished":
        print(event.pool, event.data["job_id"], event.data["status"],
              event.data["alive"], event.data["failed"])
    elif event.type == "quarantined":
        print("quarantined", event.pool, event.rid, event.data)
    elif event.type == "banned":
        print("banned", event.pool, event.rid, event.data.get("reason"))

unsubscribe = ps.bus.subscribe(on_event)
# ... 运行 ...
unsubscribe()
```

事件字段全集见[事件目录](../reference/events.md)。监听器是异步回调，单个监听器超时会被暂停而不是拖垮发布方。

## 6. 维护任务

`maintenance_enabled=True`（默认）时以下扫描自动周期执行；也可以手动触发：

```python
print(await pool.admin.run_maintenance("quarantine_gc"))
print(await pool.admin.run_maintenance("validating_gc"))
print(await pool.admin.run_maintenance("periodic_unban"))
```

| 任务 | 作用 |
| --- | --- |
| `quota_reset` | 周期配额到期复位 |
| `periodic_unban` | 按 `unban_mode` 自动解封 |
| `lost_gc` | `LOST` 超过 `lost_retention` 后归档 |
| `validating_gc` | `VALIDATING` 超时按失败处理 |
| `quarantine_gc` | 隔离到期恢复 |
| `archive_gc` | 归档超过 `archive_retention` 后擦除并删除 |
| `expire_gc` | 过期资源归档（需 `expired_retention`） |
| `reconcile` | 与协调层对账租约 |

## 检查单

- [ ] 导入后确认 `NEW` 资源已进入 `IDLE`，而不是一直不可分配。
- [ ] 大批量导入使用 `batch_id` 跟踪任务，而不是轮询 `acquire()`。
- [ ] 失败上报了正确的 `outcome`，否则隔离与健康度不会生效。
- [ ] 自动解封策略与供应商的实际恢复时间匹配，避免“刚解封又被封”。
- [ ] 召回/封禁动作写入了审计（操作者与原因），见[审计目录](../reference/audit.md)。