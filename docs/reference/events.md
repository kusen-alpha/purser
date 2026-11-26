# 事件目录

事件是池内状态变化与租约动作的**通知**，不承担持久化。发布方同步入队、立即返回；订阅方异步消费，单个监听器超时或异常不会阻塞内核。

## 事件信封

```python
@dataclass
class Event:
    type: str
    pool: str
    rid: str | None
    data: dict
    seq: int
    ts: float
    trace_id: str | None
```

- `seq` 在进程内单调递增，**重启后从 0 重新开始**；不要把它当全局唯一 id。
- `ts` 是 epoch 秒。
- `trace_id` 只在部分租约事件上出现，用于关联一次获取/归还。
- REST 与 SSE 返回的字段与 `Event.to_dict()` 一致。

## Python 订阅

```python
async def listener(event):
    print(event.seq, event.type, event.pool, event.rid, event.data)

unsubscribe = ps.bus.subscribe(listener)
...
unsubscribe()

# 回放已分发的事件（不包含尚未消费的队列）
for event in ps.bus.history(after_seq=100, limit=50):
    ...
```

- 监听器必须是异步函数或返回 awaitable；同步回调会抛异常。
- 单个监听器超时（默认 5 秒）或连续失败会被暂停 10 秒；`bus.dropped` 记录丢弃数。
- 事件队列与历史是两个独立的有界环形缓冲，容量来自 `event_queue_size`（默认 10000）。队列满时最旧的事件被淘汰。

## 事件全集

队列/历史只保留 `event_queue_size` 条；REST 历史接口的 `limit` 上限是 1000。

<!-- catalog:events:start -->
| 事件 | 资源组 | `data` 字段 | 说明 |
| --- | --- | --- | --- |
| `banned` | 资源 | `reason` | 资源被人工封禁或验活连续失败 |
| `coordination_degraded` | 协调 | `action` | `fail_open=True` 时协调层不可用，降级为本地模式 |
| `expired` | 资源 | — | 资源到期（`expires_in` / `expires_at`） |
| `health_reported` | 资源 | `alive` / `latency_ms` / `error` | 每次验活产生健康样本 |
| `import_finished` | 资源 | `report` | 导入结束，`report` 是 `ImportReport.to_dict()` |
| `lease_acquired` | 租约 | `consumer` / `slots` / `target`；stateless 额外带 `tracked: false` | 租约签发成功 |
| `lease_expired` | 租约 | `consumer` | reaper 回收过期租约 |
| `lease_lost` | 租约 | `reason`（可选） | 协调端 token 消失或心跳被拒 |
| `lease_preempted` | 租约 | `by` / `priority` / `victim_priority` | 高优先级请求抢占低优先级租约 |
| `lease_reconciled` | 租约 | `reason` / `used_slots` | 启动对账重置孤儿租约 |
| `lease_released` | 租约 | `outcome` / `consumer`；stateless 额外带 `tracked: false` | 租约归还成功 |
| `lease_revoked` | 租约 | `reason` | 管理员强制撤销租约 |
| `pool_created` | 池 | — | 池创建成功 |
| `pool_deleted` | 池 | `removed` / `forced` | 池删除完成 |
| `pool_draining` | 池 | `timeout` | 优雅停机开始等待在途租约 |
| `quarantine_expired` | 资源 | — | 隔离到期恢复 |
| `quarantined` | 资源 | `reason` | 验活失败进入隔离 |
| `quota_exhausted` | 资源 | — | 用量或周期配额耗尽 |
| `quota_reset` | 资源 | — | 周期配额复位 |
| `resource_restored` | 资源 | `state` / `conflict` | 备份恢复写入资源 |
| `resource_switched` | 资源 | `old_id` / `new_id` / `reason` | 适配器 `switch()` 产生替换资源 |
| `state_changed` | 资源 | `old` / `new` / `reason` | 任意状态迁移；最通用的事件 |
| `unbanned` | 资源 | `reason` | 解封（自动或人工） |
| `validate_job_finished` | 验活 | `job_id` / `status` / `alive` / `failed` / `skipped` | 后台验活任务结束 |
| `validate_job_started` | 验活 | `job_id` / `total` | 后台验活任务开始 |
| `validate_ok` | 验活 | — | 一次验活通过并完成状态迁移 |
<!-- catalog:events:end -->

## 常见消费方式

### 等待批次验活结束

```python
done = asyncio.Event()

async def wait_job(event):
    if event.type == "validate_job_finished" and event.data["job_id"] == batch_id:
        done.set()

unsubscribe = ps.bus.subscribe(wait_job)
await pool.import_resources(records, import_validate="batch")
await asyncio.wait_for(done.wait(), timeout=300)
unsubscribe()
```

### 审计之外的告警

```python
ALERT = {"banned", "lease_lost", "coordination_degraded", "pool_draining"}

async def alert(event):
    if event.type in ALERT:
        await send_to_alerting(event.to_dict())
```

`banned` / `quarantined` 由验活失败自动产生，也可能是人工操作；需要区分时看同一 `rid` 的审计记录。

## REST 与 SSE

```bash
# 实时事件流（SSE）
curl -N "http://127.0.0.1:8000/pools/proxies/events"

# 历史回放
curl "http://127.0.0.1:8000/pools/proxies/events/history?after_seq=0&limit=200"
```

`/events/history` 支持 `after_seq`、`type`（逗号分隔）、`rid` 与 `limit`（1–1000）。SSE 在断线后可以用最后收到的 `seq` 继续。字段与端点细节见 [Events](rest-api/events.md)。

## 与指标、审计的分工

| 机制 | 回答的问题 | 是否持久 |
| --- | --- | --- |
| 事件 | 刚刚发生了什么 | 否（有界环形缓冲） |
| [指标](metrics.md) | 累计发生了多少次 | 否（进程内计数） |
| [审计](audit.md) | 谁在什么时候对哪条资源做了什么 | 配置 `audit_path` 时持久 |