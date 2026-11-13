# 资源可用性判定

“为什么这个资源当前获取不到？”答案不是单一状态，而是一条按顺序执行的过滤链。任何一步失败，资源都不会进入候选集，也就不会参与打分。

## 判定顺序

`acquire()` 的硬约束按以下顺序执行（与 `facade._hard_filtered_candidates` 一致）：

| # | 检查 | 失败时的表现 |
| --- | --- | --- |
| 1 | 状态必须是 `IDLE` 或 `ACTIVE` | 其余 10 个状态一律跳过 |
| 2 | 剩余槽位 `used_slots + req.slots <= max_slots` | 槽位不足，跳过 |
| 3 | `req.tags` 必须是资源标签的子集 | 标签不匹配，跳过 |
| 4 | `req.required_tag` 必须存在 | 缺少硬性标签，跳过 |
| 5 | `schedule_window` 当前必须开放 | 不在时段内，跳过 |
| 6 | `expires_at` 必须大于当前时间 | 到期：触发一次到期处理（默认转 `SUSPENDED`），跳过 |
| 7 | 资源/隔离/消费者冷却必须已到期 | 未到期且未设置 `ignore_cooldown`，跳过 |
| 8 | `req.target` 在该资源上没有活跃站点失败 | 该目标最近失败过，跳过 |
| 9 | `usage_quota` 未用尽 | `used_count >= usage_quota`，跳过 |
| 10 | 周期配额 `quota` 未耗尽 | 配额耗尽，跳过 |
| 11 | `mutex_group` 不与同 consumer 已持有的资源冲突 | 互斥冲突，跳过 |
| 12 | 同 consumer 未持有该资源（除非 `allow_duplicate=True`） | 已持有，跳过 |

通过硬过滤后，才进入分配器打分、Top-K 加权随机、请求级校验（`on_request_check`，默认按 `target` 缓存 2 秒）和原子占用。

## 各检查的语义

### 状态与槽位

只有 `IDLE` 和 `ACTIVE` 可分配。`FULL` 表示槽位已占满，不能通过“等待一次 acquire”自然恢复；必须等 release、reaper 或强制撤销唤醒等待者。`NEW` 必须先验活，`QUARANTINED` 要等隔离期，`SUSPENDED` / `BANNED` / `QUOTA_EXHAUSTED` 要显式恢复，`DRAINING` 不再分配。

槽位是单资源并发度：`max_slots=1` 为独占资源，大于 1 为共享资源。一次租约可请求多个 `slots`。

### 到期与 `expires_in`

`expires_at` 是**截止时间**，不是“最早可用时间”。资源一旦到期：

- 默认转 `SUSPENDED`；
- 池配置 `expire_policy="quota"` 时转 `QUOTA_EXHAUSTED`，可用 `top_up` 恢复；
- 到期处理由获取路径触发，不依赖单独的定时器。

通过 `PoolConfig(expires_in=N)` 导入的资源会自动设置 `expires_at = now + N`；已有 `expires_at` 的资源以自身值为准。

!!! warning "到期不是定时推进"

    `expires_at` 是截止时间，不是“最早可用时间”，也不会自动延期。到期处理由获取路径触发，没有单独的定时器逐个扫描；如果资源长时间无人获取，它会保持原状态，直到下一次 `acquire()` 触碰后才转入 `SUSPENDED` / `QUOTA_EXHAUSTED`。

### 冷却、隔离与消费者级暂停

`Resource.next_usable_at()` 取以下三者的最大值：

- `next_available_at`（释放后冷却、外部暂停）；
- `meta["quarantine_until"]`（隔离期截止时间）；
- `meta["cooldown:<consumer>"]`（被强制撤销的消费者在**该资源**上的 10 秒冷却）。

`ignore_cooldown=True` 会跳过整个冷却检查，只建议在运维验证或补偿任务中使用。隔离状态本身不属于可分配状态，所以即使冷却时间到了，也要先完成 `QUARANTINED → IDLE` 的迁移。

### 站点失败

请求级校验按 `target` 记录失败，`site_failure_ttl=300s` 内该目标在这条资源上直接失败，不参与本轮候选。它不会改变资源状态，也不会影响其他 target。

### 配额的两层含义

| 概念 | 字段 | 作用范围 | 耗尽后 |
| --- | --- | --- | --- |
| 单资源累计用量 | `usage_quota` / `used_count` | 一条资源 | 过滤跳过；`top_up` 或重置 |
| 周期配额 | `quota_limit` / `quota` | 一条资源的周期计划 | 状态转 `QUOTA_EXHAUSTED`，周期任务重置或 `top_up` |

真正的按站限流由请求级校验与站点失败索引承担；`quota_limit` 是每资源的周期配额，不按 target 分别生效。

### 互斥与重复持有

- `mutex_group`：同一 consumer 在同一组内最多持有一条资源，用于“相同账号不能同时开两个会话”这类约束。
- 同一 consumer 默认不能重复持有同一资源；`allow_duplicate=True` 放开，但会分别计槽位。

## 常见失败结果

| 现象 | 含义 | 排查 |
| --- | --- | --- |
| `PoolEmpty` | 池里没有任何资源 | 先导入 |
| `PoolExhausted`（立即） | 资源全部为 `NEW`，或 `slots` 超过池容量 | `validate_now` / 重新导入；检查 `max_slots` |
| `PoolExhausted`（等待后） | 有候选但一直无槽位/冷却未到 | 看状态计数、租约、`next_available_at` |
| `ResourceBusy` | 定向获取的资源在 timeout 内不可用 | 换资源或增大 timeout |
| `NoSuitableResource` | 有候选，但全被请求级校验拒绝 | 检查 `target`、站点失败与探针配置 |

## 排查一条具体资源

```python
resource = await pool.require("r42")
print(resource.state)                 # 是否可分配
print(resource.concurrency.available) # 剩余槽位
print(resource.expires_at)            # 绝对截止时间
print(resource.next_available_at)     # 冷却
print(resource.meta.get("quarantine_until"))
print(resource.meta.get("cooldown:crawler-a"))
print(resource.usage_quota, resource.used_count)
print(resource.quota)
print(resource.schedule_window, resource.mutex_group)
print(resource.site_failures)
```

REST 侧可用 `GET /pools/{pool}/resources/{rid}` 获取同样的字段。相关页面：[公平性与健康度](fairness-and-health.md)、[租约生命周期](lease-lifecycle.md)、[状态机](../reference/state-machine.md)。
