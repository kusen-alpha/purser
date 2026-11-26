# 分配器

分配器只做一件事：在**已通过硬约束过滤**的候选资源中排序并选一个。它不检查状态、标签、配额或冷却——那些在过滤链里完成，见[资源可用性判定](../guides/availability.md)。

## 选择流程

```text
硬约束过滤 → Allocator.rank(candidates) → Allocator.pick_ranked(ranked)
          → 请求级校验 → 原子占用（失败则按排名回退）
```

默认分配器对排名前 `top_k`（默认 8）做 softmax 加权随机，而不是永远选第一名；这样避免所有消费者同时撞向同一条“最优”资源。

## 内置分配器

| `allocator` | 排序依据 | 适用 |
| --- | --- | --- |
| `health_weighted`（默认） | 健康分、空闲时长、负载、偏好、亲和与抖动 | 通用 |
| `least_loaded` | 已用槽位少优先，健康分只做极小权重 | 共享账号池 |
| `round_robin` | 游标轮转，均匀使用 | 需要严格轮换 |
| `static_priority` | 声明的 `priority`（或 `group` 优先级）高者优先，不跨层 | 分级资源 |
| `random` | 完全随机，忽略分数 | 压测、无偏好场景 |

### `health_weighted`

```text
score = health * health.score
      + age   * clamp((now - last_used_at) / 3600, 0, 1)
      - load  * used_slots / max_slots
      + preference * meta.preference
      + affinity   * affinity_score
      + jitter     * random()
```

默认权重：`health=1.0`、`age=0.15`、`load=0.30`、`preference=0.0`、`affinity=0.0`、`jitter=0.05`。`affinity` 只有 `affinity_weight > 0` 时生效；`preference` 需要资源显式设置。

### `least_loaded`

按 `-used_slots + health.score * 0.01` 排序：先看负载，健康分只在同负载时打破平局。适合 `max_slots > 1` 的账号池。

### `round_robin`

按候选列表顺序轮转，游标保存在分配器实例内。候选集合变化时会从当前游标取模，因此不是严格的全局公平队列；需要公平性时用调度器（`fifo` / `token_bucket`）而不是分配器。

### `static_priority`

资源 `meta.priority` 高者优先；如果配置了 `group_priority` 且资源有 `meta.group`，按组优先级。**不会跨优先级层**：只在最高层内做加权随机，避免高优先级资源被低优先级挤占。

### `random`

先打乱候选，再取第一个；忽略 `top_k` 与权重。适合不需要健康度、只想均匀取样的场景。

## 自定义分配器

```python
from purser import Allocator, register_allocator

@register_allocator
class LatencyFirst(Allocator):
    name = "latency_first"

    def rank(self, candidates, ctx):
        return sorted(
            ((res, -(res.health.last_latency_ms or 1e9)) for res in candidates),
            key=lambda item: item[1],
            reverse=True,
        )
```

然后在建池时使用：

```python
pool = ps.create_pool("proxies", "proxy", allocator="latency_first")
```

契约：

- `rank(candidates, ctx) -> list[(Resource, float)]` 必须返回**全部**候选的排序结果；请求级校验失败时内核会按排名回退。
- 可以只实现 `rank()`，复用基类的 `pick_ranked()`（Top-K softmax）。
- 需要自定义选择逻辑时覆盖 `pick()` 或 `pick_ranked()`；`pick_ranked()` 是热路径，推荐覆盖它以避免重复打分。
- `name` 必须非空且不能与已注册名称重复；重复注册抛 `ConfigError`。
- `AllocationContext` 提供 `now` / `request` / `weights` / `affinity_scores` / `group_priority` / `top_k` / `rng`。

注册是**进程级全局**的；服务模式下需要在启动进程注册，REST 建池时才能识别该名称。`GET /config/schema` 的 allocator 选项会同步包含自定义名称。

## 参数与排障

| 现象 | 可能原因 |
| --- | --- |
| 总是同一条资源 | `top_k=1`、`random` 之外的分配器权重过于极端，或候选只有一条 |
| 资源使用不均 | `round_robin` 的候选集合在变化；或调度器 FIFO 让同一消费者反复命中 |
| 高优先级资源不被使用 | `static_priority` 只在最高层内选；低层资源不会“补位” |
| 亲和完全不生效 | `affinity_weight=0`（默认），或 `AcquireRequest.affinity` 为空 |
| 健康分低的资源仍被选中 | 健康权重不是唯一项；检查 `load` / `age` / `preference` 与 Top-K 随机 |

权重与字段定义见[配置](configuration.md)。公平性、隔离与健康度的完整规则见[公平性与健康度](../guides/fairness-and-health.md)。