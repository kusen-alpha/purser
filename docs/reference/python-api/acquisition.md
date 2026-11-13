# AcquireRequest / Lease / BatchLease

获取流程固定为：硬约束过滤 → 分配器打分（Top-K）→ 加权随机首选 + 按分回退 → 请求级校验 → 首个成功原子占用。完整过滤链见[资源可用性判定](../../guides/availability.md)，租约状态与等待语义见[租约生命周期](../../guides/lease-lifecycle.md)。

## `AcquireRequest`

```python
AcquireRequest(
    consumer=None, target=None, priority=0, slots=1,
    tags=frozenset(), timeout=None, probe=None, trace_id=None,
    use_limit=None, allow_duplicate=False, ignore_cooldown=False,
    resource_id=None, required_tag=None, affinity=None,
)
```

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `consumer` | `None` | 消费者标识，用于公平队列、去重、亲和与消费者级冷却 |
| `target` | `None` | 目标站点，用于请求级校验、站点失败索引与报表 |
| `priority` | `0` | 越高越优先；高优先级等待者可越过低优先级 FIFO |
| `slots` | `1` | 单个租约占用的槽位数，必须为正整数 |
| `tags` | 空 | 资源必须包含全部标签 |
| `timeout` | `None` | 等待秒数；缺省用池 `wait_timeout` |
| `probe` | `None` | 按目标实时验活（opt-in）：以该 URL 为目标，在取得候选前调用 `on_request_check` 实测；结果按 `(consumer, target, rid)` 缓存 `check_cache_ttl` 秒。`None` 时维持原行为（默认 `PASS` / 自定义钩子照旧） |
| `trace_id` | `None` | 缺省按调用生成；不会写回传入对象 |
| `use_limit` | `None` | 本租约允许的使用次数，必须 `>= 0`；达到后心跳不再续约 |
| `allow_duplicate` | `False` | 允许同一 consumer 重复持有同一资源 |
| `ignore_cooldown` | `False` | 忽略资源级/消费者级冷却（隔离与站点失败仍然生效） |
| `resource_id` | `None` | 定向获取；被占用时按 timeout 等待 |
| `required_tag` | `None` | 硬约束标签，与 `tags` 的“全部包含”独立 |
| `affinity` | `None` | **保留字段**：非 `None` 立即抛 `ConfigError`；亲和度通过 `PoolConfig(affinity_weight=...)` 开启 |

## 单个获取

```python
lease = await pool.acquire(
    AcquireRequest(consumer="crawler-a", tags=frozenset({"us"}), timeout=10)
)
# 等价写法：await pool.acquire(consumer="crawler-a", timeout=10)
```

- `acquire()` 复制传入的 `AcquireRequest`，`**kwargs` 只作用于本次调用；同一个请求对象可以安全复用。
- 未知 `kwargs` 抛 `TypeError`；`slots <= 0`、`use_limit < 0` 抛 `ConfigError`。
- 同一 consumer 重复获取同一资源时，默认返回已有租约；`allow_duplicate=True` 才允许第二份租约。
- 返回的 `Lease` 带唯一 `token`；归还必须 token 匹配。

失败矩阵：

| 情况 | 异常 |
| --- | --- |
| 池内没有任何资源 | `PoolEmpty` |
| 池内资源全部为 `NEW` | `PoolExhausted`（提示导入时验活或调用 `validate_now()`） |
| 资源忙且等待超时 | `PoolExhausted` |
| `timeout <= 0` 且无法立即满足 | `PoolExhausted` |
| 定向获取的资源不存在 | `ResourceNotFound` |
| 定向获取的资源处于不可分配状态 | `BadState` |
| 定向获取被占用且等待超时 | `ResourceBusy` |
| 候选全部被请求级校验拒绝 | `NoSuitableResource` |
| 池未启动 | `NotStarted` |

定向获取（`resource_id`）不走分配器，但仍检查状态、槽位与重复持有；等待期间每 0.1 秒复查一次，直到 timeout。

## 批量获取

```python
batch = await pool.acquire_many(
    4, req=AcquireRequest(consumer="crawler-a", slots=2), mode="atomic", timeout=10
)
```

- `n` 是**租约条数**，必须 `> 0`；每条租约按 `req.slots` 占用槽位。
- `timeout` 参数优先于 `req.timeout`；两者都为空时用池 `wait_timeout`。
- `mode="atomic"`：任一条获取失败则整体回滚并抛出。
- `mode="best_effort"`：按实际可用数量返回；低于 `n_min`（缺省等于 `n`）时释放已获取的租约并抛 `PoolExhausted`。
- 返回 `BatchLease`，可迭代、可 `len()`；`await batch.release_all(pool, outcome=UsageOutcome.OK)` 逐条归还并清空列表。

## 上下文管理器

```python
async with pool.lease(consumer="worker", tags=frozenset({"us"})) as lease:
    ...
    lease.outcome = UsageOutcome.OK
    lease.used = 1
```

`lease()` 先执行 `acquire()`，退出时以 `lease.outcome` / `lease.used` 归还，异常路径同样归还。推荐把它作为默认路径；手动 `acquire()` 适用于需要跨函数持有租约的场景。

## `Lease`

```python
Lease(
    token, pool, resource_id, consumer=None, slots=1, started_at, expires_at,
    heartbeat_at, priority=0, use_limit=None, used=0, trace_id=None,
    outcome=UsageOutcome.OK, target=None, payload=None, resource=None,
)
```

| 字段 | 说明 |
| --- | --- |
| `token` | 唯一租约凭据；归还、续约、定向强制撤销都使用它 |
| `pool` / `resource_id` / `slots` | 所属池、资源与占用槽位 |
| `consumer` / `target` / `priority` / `trace_id` | 获取时的请求上下文 |
| `started_at` / `expires_at` / `heartbeat_at` | 起始、到期与最近心跳时间（epoch 秒） |
| `use_limit` / `used` | 使用次数上限与已用次数 |
| `outcome` | 归还时上报的结果，默认 `UsageOutcome.OK` |
| `payload` | 获取时资源 payload 的副本，便于持有者直接使用 |
| `resource` | 本进程加载的类型化资源快照；跨进程重建的租约为 `None`，需要最新状态时用 `await pool.require(lease.resource_id)` |

- `lease.expired(now=None)` 判断是否到期。
- 心跳由 `LeaseManager` 按 `lease_ttl / 3` 自动续约；`max_lease_duration` 会给续约到期时间封顶，总寿命用尽后停止续约并由 reaper 回收。
- `use_limit` 达到后 `renew_manual()` 与心跳续约都返回/视为失败，租约到期后回收。

## 续约与归还

```python
ok = await pool.renew_manual(lease)     # False: 协调端拒绝、use_limit 用尽或总寿命已到
ok = await pool.release(lease, outcome=UsageOutcome.RATE_LIMITED, used=1)
```

- `renew_manual()` 用于长任务显式保活；常规场景交给心跳。
- `release()` 的 `outcome` 省略时使用 `lease.outcome`；`used` 省略时按 `lease.used or 1` 计量。`used < 0` 或非 `UsageOutcome` 值抛 `ConfigError`。
- 归还成功后返回 `True`。如果 token 已不在协调端（被强制撤销、过期、其他实例先归还或协调层被清空），归还**幂等返回 `True`**，且不会扣减存储槽位，避免影响新持有者。
- token 仍在协调端但存储侧槽位对不上时抛 `LeaseConflict`。
- `await pool.release_consumer(consumer, outcome=UsageOutcome.OK)` 归还该 consumer 在当前池的全部活跃租约，返回归还列表。

### `UsageOutcome` 副作用

| 结果 | 主要副作用 |
| --- | --- |
| `ok` | 正常计量 |
| `rate_limited` | 有 `target` 时记录站点失败；未配置 `cooldown_after_release` 时至少冷却 5 秒，配置后以该值为准 |
| `captcha` | 有 `target` 时记录站点失败 |
| `timeout` / `network_error` / `banned` | 降低健康度并标记 `needs_validate`；网络错误同时记录站点失败 |
| `quota_exhausted` | 正常计量；是否进入 `QUOTA_EXHAUSTED` 由配额判定 |
| `preempted` | 该 consumer 在资源上冷却 10 秒（强制撤销会自动产生此结果） |

状态与健康度的完整规则见[公平性与健康度](../../guides/fairness-and-health.md)。

## 强制撤销

```python
revoked = await pool.force_revoke(
    "r42", operator="alice", reason="credential leak", token=lease.token
)
```

- 不带 `token`：只撤销该资源的**任意一条**活跃租约，并发资源可能仍有其他租约。
- 带 `token`：精确撤销指定租约；token 不存在抛 `LeaseConflict`。
- 资源没有任何活跃租约抛 `BadState`。
- 强制撤销会释放槽位、给被撤销租约的 consumer 写入 10 秒资源级冷却、记录 `preempted` 用量并发布 `lease_revoked` 事件。
- 需要撤销该资源的全部租约时，由调用方遍历当前租约并容忍并发归还：

```python
for lease in list(await pool.leases("r42")):
    try:
        await pool.force_revoke(
            "r42", operator="alice", reason="credential leak", token=lease.token
        )
    except LeaseConflict:
        pass
```

## 相关事件

获取、归还、过期、失联、强制撤销分别发布 `lease_acquired` / `lease_released` / `lease_expired` / `lease_lost` / `lease_revoked`；事件字段与回放接口见 [REST 事件](../rest-api/events.md)。
