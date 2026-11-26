# 指标目录

Purser 的指标是**进程内、无标签**的计数器与直方图。它不依赖 Prometheus 客户端，也不会主动推送；服务模式通过 `/metrics` 暴露文本，通过 `/metrics/snapshot` 返回 JSON 快照。

## 访问方式

```bash
curl http://127.0.0.1:8000/metrics
curl http://127.0.0.1:8000/metrics/snapshot
```

嵌入式：

```python
print(ps.metrics.snapshot())
print(ps.metrics.render_prometheus())
```

`/metrics` 不需要鉴权；`/metrics/snapshot` 需要读权限（配置密钥时）。

## 命名规则

- 代码中的指标名形如 `pool.<池名>.<指标>`；池名进入指标名，因此**池名应只使用字母、数字、下划线**，避免产生非法 Prometheus 名称。
- 渲染时统一加前缀 `purser_`，点号与连字符替换为下划线：`pool.proxies.acquire_total` → `purser_pool_proxies_acquire_total`。
- 所有指标都是进程内累计值；重启归零，多实例需要分别抓取或由 Prometheus 聚合。

## 内置计数器

<!-- catalog:metrics:start -->
| 指标（`pool.<name>.` 之后） | 递增时机 | 用途 |
| --- | --- | --- |
| `acquire_total` | 每次调用 `acquire()`（含参数校验失败） | 获取请求总量，与成功量对比得到失败率 |
| `lease_total` | 签发一条占用槽位的租约 | 成功获取量 |
| `lease_untracked_total` | `acquire_mode="stateless"` 签发取样租约 | 无占用取样量 |
| `release_total` | 成功归还一条租约 | 归还量 |
| `lease_lost_total` | 协调端 token 消失或心跳被拒 | 租约丢失量 |
| `acquire_replayed` | 幂等键命中已存活租约 | 重放量，过高说明客户端重试频繁 |
| `preempt_total` | 高优先级抢占成功 | 抢占次数 |
| `fail_open_total` | 协调层不可用且 `fail_open=True` | 降级次数，非零应立即告警 |
| `feedback_dropped_total` | 用量反馈多次 CAS 失败被丢弃 | 数据一致性风险，非零应排查 |
<!-- catalog:metrics:end -->

这些计数器覆盖内核自身写入的指标。`Metrics.observe()` / `histograms` 是公开能力，但当前版本没有内置直方图观测点；自定义代码可以写入并在 `/metrics/snapshot` 中看到 `count` / `sum` / `min` / `max` / `avg`。

## 快照格式

```json
{
  "counters": {
    "pool.proxies.acquire_total": 120,
    "pool.proxies.lease_total": 118
  },
  "histograms": {
    "custom.acquire_ms": {"count": 3, "sum": 45.2, "min": 10.1, "max": 20.0, "avg": 15.07}
  }
}
```

直方图每个指标最多保留 10000 个观测值，超出后丢弃最旧的；没有分位数计算。

## Prometheus 抓取

```yaml title="prometheus.yml"
scrape_configs:
  - job_name: purser
    metrics_path: /metrics
    static_configs:
      - targets: ["purser-1:8000", "purser-2:8000"]
```

服务暴露的文本以 `purser_` 开头。直方图输出 `<name>_count` 与 `<name>_sum`，不输出 bucket。

## 建议的告警

没有绝对阈值，下面按“变化率”而非绝对值设计：

| 现象 | 表达式思路 | 处理 |
| --- | --- | --- |
| 获取失败率高 | `rate(purser_pool_*_acquire_total[5m]) - rate(purser_pool_*_lease_total[5m])` 持续为正 | 看 `PoolExhausted` 过滤原因、扩容或缩短占用 |
| 租约大量丢失 | `rate(purser_pool_*_lease_lost_total[5m]) > 0` | 检查心跳、事件循环阻塞、Redis 稳定性 |
| 降级运行 | `increase(purser_pool_*_fail_open_total[5m]) > 0` | 协调层故障，立即处理 |
| 反馈被丢弃 | `increase(purser_pool_*_feedback_dropped_total[5m]) > 0` | 存储 CAS 冲突或热点资源竞争 |
| 重放异常多 | `rate(purser_pool_*_acquire_replayed[5m])` 突增 | 客户端超时重试或网络抖动 |

指标只回答“发生了多少次”，不回答“哪条资源、为什么”。定位个例要结合[事件](events.md)、`/metrics/snapshot` 与[审计](audit.md)。

## 缺失的观测

当前没有内置的获取延迟直方图、验活延迟直方图与池内等待时长。需要这些数据时：

- 在业务侧包一层计时并写 `ps.metrics.observe("custom.acquire_ms", value)`；
- 或从[事件](events.md)的 `ts` / `trace_id` 计算端到端耗时；
- 或使用 `PoolStats`（`waiting` / `active_leases` / `allocatable`）做瞬时容量监控。

容量规划见[性能与容量](../guides/performance.md)。