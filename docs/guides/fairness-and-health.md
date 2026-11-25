# 公平性与健康度

分配的目标不是“随机选择一条”，而是在硬约束内选出**最可能成功、最不挤占他人**的资源。

## 分配流程

```text
硬约束过滤 → 打分(Top-K) → 加权随机首选 + 按分回退 → 请求级校验 → 原子占用
```

硬约束包括：可分配状态、槽位、`required_tag`、冷却、过期、配额。任何一条不满足直接淘汰，不参与打分。

## 打分

默认分配器 `health_weighted`：

```text
score = health*w_health + age*w_age - load*w_load + preference + affinity + jitter
```

| 权重 | 默认 | 含义 |
| --- | --- | --- |
| `health` | `1.0` | EWMA 健康分，越高越好 |
| `age` | `0.15` | 空闲越久越优先，做 LRU 轮换 |
| `load` | `0.30` | 已占槽位越多越降权 |
| `preference` | `0.0` | 业务偏好 |
| `affinity` | `0.0` | 由 `affinity_weight` 注入 |
| `jitter` | `0.05` | 抗惊群随机项 |

取 `top_k=8` 个候选后按分数加权随机，而非永远选第一名。其他分配器：`random`（均匀随机）、`round_robin`、`least_loaded`、`static_priority`。自定义分配器继承 `Allocator`，用 `register_allocator` 注册后即可作为 `allocator=` 的取值：

```python
from purser import Allocator, register_allocator

@register_allocator
class CheapestFirst(Allocator):
    name = "cheapest_first"

    def rank(self, candidates, ctx):
        return sorted(
            ((res, -float(res.meta.get("price", 0))) for res in candidates),
            key=lambda item: item[1],
            reverse=True,
        )
```

## 公平队列

默认调度器 `priority_quota`：

- 高 `priority` 优先调度；同优先级 FIFO。
- 等待者超时抛 `PoolExhausted`，消息携带各状态数量。
- 池内完全没有资源时立即 `PoolEmpty`，不等待。
- 池关闭会用 `PoolExhausted` 唤醒所有在途等待者。
- 等待者被取消后由队列清扫，不会把租约交给已取消的等待者。

`PoolConfig(scheduler=...)` 切换队列语义：

| 取值 | 行为 |
| --- | --- |
| `priority_quota` | 默认；`priority` 高者先出队，同优先级按到达顺序 |
| `fifo` | 完全忽略 `priority`，严格按到达顺序出队 |
| `token_bucket` | 在 `priority_quota` 基础上，按消费者做 `rate_limit` req/s、`rate_burst` 突发的准入限速；超时抛 `PoolExhausted` 并给出建议重试等待 |

## 抢占

`preemptible=True` 时，获取失败会尝试踢出一个低优先级持有者：

- 条件：`请求 priority - 持有者 priority >= preempt_threshold`（默认 10）。
- 被抢占方进入 10 秒消费者冷却（`cooldown:<consumer>`），记录一条 `PREEMPTED` 用量事件，并发布 `lease_preempted`。
- 抢占成功后重试一次分配；仍失败则退回等待队列。`stateless` 模式不参与抢占。

## 容量与槽位

- `max_slots` 控制单资源并发槽位；`slots` 控制单次租约占用。
- 并发资源在剩余槽位时保持 `ACTIVE`，占满转 `FULL`。
- 已占槽位永不超过 `max_slots`，也永不为负；这是跨后端、跨实例的硬不变量。

## 健康分

- EWMA：首次 `alpha=0.6`，之后 `alpha=0.3`，冷启动 `0.5`。
- 健康分只影响排序；连续失败由隔离策略处理。
- `stats().avg_health` 覆盖全部有样本资源。

## 隔离与封禁

- 默认 `base=30s`、`cap=1800s`、`jitter=10s` 指数退避。
- `max_failures=3`；24 小时试用期内阈值减半，首次失败不直接封禁。
- `unban_mode=mixed`：`unban_after=86400s` 后自动转 `VALIDATING`，也支持人工 `unban`。
- 风控类结果按策略处理，不等同于“直接封禁”：`CAPTCHA` 计入站点失败并触发冷却，`BANNED` 结果才走失败计数/阈值升级。

## 站点失败索引

- 请求级校验按 `target` 记录失败，`site_failure_ttl=300s` 自动清理。
- `site_failure_max=16` 限制条目，`check_cache_ttl=2s` 缓存最近校验。
- 只降权/拒绝本轮候选，不直接改变资源状态。

## 配额、冷却与过期

| 能力 | 配置 | 行为 |
| --- | --- | --- |
| 单资源用量配额 | `usage_quota` | 用尽进入 `QUOTA_EXHAUSTED`，`top_up` 恢复 |
| 周期配额 | `quota_limit` | 每资源周期配额，周期任务重置 |
| 释放后冷却 | `cooldown_after_release` | 冷却期内不分配；`cooldown_consumer_only` 只冷却原消费者 |
| 绝对过期 | `expires_in` / `expires_at` | 到期后不可获取，默认转 `SUSPENDED`；`expire_policy="quota"` 时转 `QUOTA_EXHAUSTED` |
| 亲和性 | `affinity_weight` | 同一 consumer 优先复用资源 |

> 自动抢占（`preemptible` / `preempt_threshold`）尚未实现，配置为非默认值会抛 `ConfigError`；当前被占用资源只能等待、强制撤销或换资源。

调参建议：先调 `top_k` 与 `weights`，再调整隔离阈值；`max_slots` 与 `lease_ttl` 是控制容量与泄漏风险的主要参数。完整的“为什么这个资源现在不可用”判定顺序见[资源可用性](availability.md)。
