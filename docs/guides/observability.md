# 可观测性

Purser 提供三层信号，职责不同：

| 信号 | 回答 | 保留 |
| --- | --- | --- |
| [事件](../reference/events.md) | 刚刚发生了什么、影响哪条资源 | 进程内环形缓冲 |
| [指标](../reference/metrics.md) | 累计发生多少次、趋势如何 | 进程内计数 |
| [审计](../reference/audit.md) | 谁在什么时候改了什么 | 配置 `audit_path` 时持久 |
| 用量报表 | 每个消费者/目标用了多少 | 进程内有界账本 |

日志不属于内核契约：库使用标准 `logging`（`purser`、`purser.events`、`purser.lease` 等 logger），格式与采集由部署层决定。

## 健康与就绪

```bash
curl http://127.0.0.1:8000/healthz    # {"status":"ok"}
curl http://127.0.0.1:8000/readyz     # {"status":"ready","pools":[...]}
curl -H "Authorization: Bearer $PURSER_API_KEY" \
  http://127.0.0.1:8000/service
```

- `/healthz` 只证明事件循环能响应。
- `/readyz` 只证明进程已启动并返回当前池列表；**不探测** MySQL / Redis。后端不可用时它仍可能返回 ready。
- `/service` 返回版本、`uptime_s`、`storage`、`coordination`、池、适配器与维护开关，适合确认滚动发布后每个实例接的是同一套后端。
- 以上端点均不鉴权（`/service` 需要读权限），不要把内部拓扑直接暴露到公网。

真正的后端健康要看业务请求的错误率与 `BackendUnavailable` 日志；多实例部署建议额外探测 Redis/MySQL 本身。

## 指标

```bash
curl http://127.0.0.1:8000/metrics           # Prometheus 文本，不鉴权
curl -H "Authorization: Bearer $PURSER_API_KEY" \
  http://127.0.0.1:8000/metrics/snapshot     # JSON，需要读权限
```

关键计数器（完整列表见[指标目录](../reference/metrics.md)）：

| 指标 | 含义 | 何时告警 |
| --- | --- | --- |
| `acquire_total - lease_total - lease_untracked_total` | 获取失败量（近似） | 持续为正且上升 |
| `lease_lost_total` | 租约丢失 | 任何非零增量 |
| `fail_open_total` | 协调层降级运行 | 任何非零增量 |
| `feedback_dropped_total` | 用量反馈被丢弃 | 任何非零增量 |
| `acquire_replayed` | 幂等重放 | 突增说明客户端重试异常 |
| `preempt_total` | 抢占 | 突增说明优先级配置或容量不足 |

注意：

- 指标名包含池名（`purser_pool_<pool>_...`），池名应只含字母、数字、下划线。
- 每个实例单独累计，重启归零。Prometheus 侧用 `sum by (__name__)` 或 `sum(rate(...))` 聚合，不要跨实例直接取 `max` 当总量。
- 直方图只输出 `_count` / `_sum`，没有 bucket；当前也没有内置的获取/验活延迟直方图。需要时在业务侧 `observe()` 自定义指标。

## 事件

```python
async def on_event(event):
    if event.type in {"coordination_degraded", "lease_lost", "pool_draining"}:
        await alert(event.to_dict())

unsubscribe = ps.bus.subscribe(on_event)
```

- 监听器必须异步；单个监听器超时（5 秒）或连续失败会被暂停，`bus.dropped` 记录队列溢出。
- 事件是**通知**，不是可靠消息队列：进程重启、队列溢出、订阅方掉线都可能丢。需要“不可丢”的记录用审计。
- SSE 断线可用最后收到的 `seq` 从 `/pools/{pool}/events/history` 续读；历史也只保留 `event_queue_size` 条。
- `seq` 是进程内序号，重启归零，不可作为全局事件 id。

## 审计与用量

- `GET /audit?tail=true&limit=50` 查看最近操作；`/audit/verify` 校验哈希链。
- 强制操作（`force_revoke`、`ban`、`drain`、`admin-state`、`backup`、`restore` 等）都会留痕；`reason` 是排障时最有价值的字段。
- `GET /reports/usage?group_by=consumer|target|pool|kind` 合并历史用量与当前负载快照；数据源是内存账本，重启清空。
- 长期报表应定时拉取并落到外部时序库或数仓，见[数据保留](data-retention.md)。

## 日志与追踪

- 建议统一采集服务进程的 stderr/stdout；库不写业务日志文件。
- `X-Trace-Id` 会写入租约与审计，可把一次业务请求串到事件和审计；没有内置的分布式追踪导出。
- 排障时按“`trace_id` → 审计 → 同 `rid` 的事件 → 指标变化”顺序收集证据。
- 不要打印完整资源 payload；需要定位时用 `include_secrets=false` 的视图或字段名。

## 仪表盘建议

至少放四组图：

1. **容量**：每池 `total` / `allocatable` / `active_leases` / `waiting` / `avg_health`（来自 `/reports/usage` 的 `pools` 或 `stats()`）。
2. **流量**：`acquire_total`、`lease_total`、`release_total` 的速率与差值。
3. **故障**：`lease_lost_total`、`fail_open_total`、`feedback_dropped_total`、`preempt_total`。
4. **状态**：按状态计数的资源数（`idle` / `quarantined` / `banned` / `suspended` 等）。

## 常见故障的定位顺序

| 现象 | 先看 | 再看 |
| --- | --- | --- |
| 获取失败率高 | `acquire_total` 与 `lease_total` 的差、`PoolExhausted` 日志 | 池状态计数、等待者、上游占用时长 |
| 租约莫名丢失 | `lease_lost_total`、`lease_lost` 事件 | 心跳间隔、事件循环是否被同步代码阻塞、Redis 延迟 |
| 降级运行 | `fail_open_total`、`coordination_degraded` | Redis 连接、持久化、网络 |
| 资源大面积不可用 | `avg_health`、`banned`/`quarantined` 事件 | 探活目标、`site_failure_*`、封禁阈值 |
| 审计校验失败 | `/audit/verify`、审计文件权限 | 是否有进程绕过 append 写库、是否手工删行 |
| 事件缺口 | `bus.dropped`、慢消费者日志 | SSE 续读位置、历史容量、是否重启 |

相关页面：[指标目录](../reference/metrics.md)、[事件目录](../reference/events.md)、[审计目录](../reference/audit.md)、[生产检查清单](production-checklist.md)、[排障](troubleshooting.md)。