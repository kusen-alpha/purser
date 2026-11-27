# AcquireRequest / Lease / BatchLease

获取流程固定为：硬约束过滤 → 分配器打分（Top-K）→ 加权随机首选 + 按分回退 → 请求级校验 → 首个成功原子占用。完整过滤链见[资源可用性判定](../../guides/availability.md)，租约状态与等待语义见[租约生命周期](../../guides/lease-lifecycle.md)。

本页签名以嵌入式异步 `ResourcePool` 为准。同步、线程和远程门面的差异见[同步 Purser](../../guides/sync-facade.md)与 [LocalScheduler](../local-scheduler.md)。

## `AcquireRequest`

```python
AcquireRequest(
    consumer=None, target=None, priority=0, slots=1,
    tags=frozenset(), timeout=None, probe=None, trace_id=None,
    use_limit=None, allow_duplicate=False, ignore_cooldown=False,
    rid=None, required_tag=None, affinity=None,
    idempotency_key=None,
)
```

| 字段 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `consumer` | <code>str &#124; None</code> | `None` | 消费者标识，用于公平队列、去重、亲和与消费者级冷却 |
| `target` | <code>str &#124; None</code> | `None` | 目标站点，用于请求级校验、站点失败索引与报表 |
| `priority` | `int` | `0` | 越高越优先；高优先级等待者可越过低优先级 FIFO |
| `slots` | `int` | `1` | 单个租约占用的槽位数，必须为正整数 |
| `tags` | `frozenset[str]` | 空集合 | 资源必须包含全部标签；传字符串会抛 `TypeError` |
| `timeout` | <code>float &#124; None</code> | `None` | 等待秒数；缺省用池 `wait_timeout` |
| `probe` | <code>str &#124; None</code> | `None` | 按目标实时验活（opt-in）：以该 URL 为目标，在取得候选前调用 `on_request_check` 实测；结果按 `(consumer, target, rid)` 缓存 `check_cache_ttl` 秒。`None` 时维持默认 `PASS` / 自定义钩子行为 |
| `trace_id` | <code>str &#124; None</code> | `None` | 缺省按调用生成；不会写回传入对象 |
| `use_limit` | <code>int &#124; None</code> | `None` | 本租约允许的使用次数，必须 `>= 0`；达到后心跳不再续约 |
| `allow_duplicate` | `bool` | `False` | 允许同一 consumer 重复持有同一资源 |
| `ignore_cooldown` | `bool` | `False` | 忽略资源级/消费者级冷却；隔离与站点失败仍然生效 |
| `rid` | <code>str &#124; None</code> | `None` | 定向获取；被占用时按 timeout 等待 |
| `required_tag` | <code>str &#124; None</code> | `None` | 硬约束标签，与 `tags` 的“全部包含”独立 |
| `affinity` | <code>str &#124; None</code> | `None` | 亲和历史键，缺省等于 `consumer`；同一键越常用越容易被分配器加权（需 `affinity_weight > 0`）。空字符串抛 `ValueError` |
| `idempotency_key` | <code>str &#124; None</code> | `None` | 同一池内重复获取返回同一份仍存活的租约；并发同 key 请求共享一次获取结果。空字符串抛 `ValueError` |

`AcquireRequest` 是获取请求的描述对象：`acquire()` 不修改传入对象，需要覆盖字段时会通过构造器创建副本，因此 `__post_init__` 的校验在对象构造和关键字参数两条路径上都生效。

## 单个获取

### `pool.acquire()` {#pool-acquire}

**用途**：获取一份资源租约；需要自动归还时优先使用 [`pool.lease()`](#pool-lease)。

**签名**

```python
async def acquire(
    req: AcquireRequest | None = None, **kwargs: Any
) -> Lease: ...
```

**参数**

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `req` | <code>AcquireRequest &#124; None</code> | `None` | 可复用的请求对象，不会被修改 |
| `kwargs` | `Any` | — | `AcquireRequest` 字段；未知字段抛 `TypeError` |

**返回与异常**：返回带唯一 `token` 的 `Lease`。归还、续约和定向强制撤销都必须使用该 token。常见异常见下表。

**示例**

```python
from purser import AcquireRequest

lease = await pool.acquire(
    AcquireRequest(consumer="crawler-a", tags=frozenset({"us"}), timeout=10)
)
# 等价写法：await pool.acquire(consumer="crawler-a", timeout=10)
```

**注意**

- 首次 `acquire()` / `lease()` / `acquire_many()` 会自动启动运行时，`Purser.start()` 是可选的提前启动。
- 关闭后不支持隐式自动重启；显式 `Purser.start()` 是否能重启取决于后端。
- `slots <= 0`、`use_limit < 0` 抛 `ConfigError`。
- 同一 consumer 重复获取同一资源时，默认返回已有租约；`allow_duplicate=True` 才允许第二份租约。
- 传入 `idempotency_key` 时，同池同 key 的重复调用返回同一份仍存活的租约；并发同 key 只执行一次获取。
- `acquire_mode="stateless"` 时返回的 `Lease` 不占槽位、不需要归还；`release()` 只反馈用量与健康。

失败矩阵：

| 情况 | 异常 |
| --- | --- |
| 池内没有任何资源 | `PoolEmpty` |
| 资源停在 `NEW` 且没有进行中的验活任务（显式 `import_validate="none"`） | `PoolExhausted`（提示调用 `pool.admin.validate_now()` 或改用 `batch`） |
| 资源正在验活（`VALIDATING` / 后台任务运行中） | 等待至 timeout；超时抛 `PoolExhausted` 并带任务进度 |
| 资源忙且等待超时 | `PoolExhausted`（消息附带被跳过的过滤原因，如 `tags=1`、`cooldown=2`） |
| `timeout <= 0` 且无法立即满足 | `PoolExhausted` |
| 消费者已达 `max_leases_per_consumer` | `PoolExhausted`（过滤原因中出现 `consumer_limit`） |
| `scheduler="token_bucket"` 且等待令牌超时 | `PoolExhausted`（提示建议重试等待秒数） |
| 定向获取的资源不存在 | `ResourceNotFound` |
| 定向获取的资源处于不可分配状态（如 `QUARANTINED`） | `BadState` |
| 定向获取的目标在 `NEW` / `VALIDATING` | 先等待验活；`NEW` 且无验活任务时立即抛 `PoolExhausted`（提示 `pool.admin.validate_now()`） |
| 定向获取被占用且等待超时 | `ResourceBusy` |
| 候选全部被请求级校验拒绝 | `NoSuitableResource` |
| 运行时已关闭且不支持当前重启方式 | `NotStarted` |

定向获取（`rid`）不走分配器，但仍检查状态、槽位与重复持有；等待期间每 0.1 秒复查一次，直到 timeout。

### `pool.lease()` {#pool-lease}

**用途**：作为上下文管理器获取租约，并在退出时自动归还，适合绝大多数短生命周期任务。

**签名**

```python
async def lease(
    req: AcquireRequest | None = None, **kwargs: Any
) -> AsyncIterator[Lease]: ...
```

**参数**

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `req` | <code>AcquireRequest &#124; None</code> | `None` | 与 `acquire()` 相同的请求对象 |
| `kwargs` | `Any` | — | 与 `acquire()` 相同的请求字段 |

**返回与异常**：进入上下文时产出 `Lease`；获取阶段的异常与 `acquire()` 一致。退出时调用 `release(lease, outcome=lease.outcome, used=lease.used)`，异常路径同样归还。

**示例**

```python
from purser import UsageOutcome

async with pool.lease(consumer="worker", tags=frozenset({"us"})) as lease:
    lease.outcome = UsageOutcome.OK
    lease.used = 1
```

**注意**：上下文管理器默认把 `lease.used`（初始为 `0`）作为用量上报。要统计真实使用次数，需要在块内设置 `lease.used`。手动 `acquire()` 适合需要跨函数持有租约的场景。

## 批量获取 {#batch-acquire}

### `pool.acquire_many()` {#pool-acquire-many}

**用途**：一次获取多份租约，可选择整体成功或保留部分成功结果。

**签名**

```python
async def acquire_many(
    n: int,
    *,
    req: AcquireRequest | None = None,
    mode: Literal["atomic", "best_effort"] = "atomic",
    n_min: int | None = None,
    timeout: float | None = None,
) -> BatchLease: ...
```

**参数**

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `n` | `int` | 必填 | 租约条数，必须 `> 0`；每条租约按 `req.slots` 占用槽位 |
| `req` | <code>AcquireRequest &#124; None</code> | `None` | 每份租约共用的请求模板，不会被修改 |
| `mode` | `"atomic"` / `"best_effort"` | `"atomic"` | `atomic` 任一条失败则整体回滚；`best_effort` 保留实际获取结果 |
| `n_min` | <code>int &#124; None</code> | `None` | `best_effort` 最少可接受条数；缺省等于 `n`，必须 `> 0` 且不能大于 `n` |
| `timeout` | <code>float &#124; None</code> | `None` | 覆盖 `req.timeout`；两者都为空时用池 `wait_timeout` |

**返回与异常**：返回可迭代、可 `len()` 的 `BatchLease`。`atomic` 失败会释放已获取成员并抛出原异常；`best_effort` 低于 `n_min` 时释放已获取成员并抛 `PoolExhausted`。

**示例**

```python
from purser import AcquireRequest, UsageOutcome

batch = await pool.acquire_many(
    4,
    req=AcquireRequest(consumer="crawler-a", slots=2),
    mode="atomic",
    timeout=10,
)
try:
    for lease in batch:
        ...
finally:
    await batch.release_all(pool=pool, outcome=UsageOutcome.OK)
```

**注意**：`n` 是租约条数，不是槽位数。`mode` 非法、`n <= 0`、`n_min <= 0` 或 `n_min > n` 都会抛 `ConfigError`。

### `batch.release_all()` {#batch-release-all}

**用途**：按列表顺序逐条归还批量租约，全部成功后清空列表。

**签名**

```python
async def release_all(
    pool: ResourcePool,
    outcome: UsageOutcome | str = UsageOutcome.OK,
) -> None: ...
```

**参数**

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `pool` | `ResourcePool` | 必填 | 签发这些租约的池 |
| `outcome` | <code>UsageOutcome &#124; str</code> | `UsageOutcome.OK` | 每份租约统一使用的归还结果；合法字符串会被转换为枚举 |

**返回与异常**：返回 `None`。中途某条归还抛错时，列表不会清空；已成功项仍保留在列表中，可依赖 `release()` 的幂等语义重试。

**示例**

```python
await batch.release_all(pool=pool, outcome="ok")
```

## `Lease`

```python
Lease(
    token, pool, rid, consumer=None, slots=1,
    started_at=<now>, expires_at=<now + 60>, heartbeat_at=<now>,
    priority=0, use_limit=None, used=0, trace_id=None,
    outcome=UsageOutcome.OK, target=None, payload=None, resource=None,
)
```

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `token` | `str` | 唯一租约凭据；归还、续约、定向强制撤销都使用它 |
| `pool` / `rid` / `slots` | `str` / `str` / `int` | 所属池、资源与占用槽位 |
| `consumer` / `target` / `priority` / `trace_id` | 请求上下文 | 获取时的消费者、目标、优先级与追踪标识 |
| `started_at` / `expires_at` / `heartbeat_at` | `float` | 起始、到期与最近心跳时间（epoch 秒） |
| `use_limit` / `used` | <code>int &#124; None</code> / `int` | 使用次数上限与已用次数 |
| `outcome` | `UsageOutcome` | 归还时上报的结果，默认 `UsageOutcome.OK` |
| `payload` | <code>dict &#124; None</code> | 获取时资源 payload 的副本，便于持有者直接使用 |
| `resource` | <code>Resource &#124; None</code> | 本进程加载的类型化资源快照；跨进程重建的租约为 `None`，需要最新状态时用 `await pool.require(rid=lease.rid)` |

直接构造 `Lease` 适合反序列化或测试；正常运行时应通过 `acquire()` / `lease()` 获取已登记的租约。

### `Lease.new()` {#lease-new}

**用途**：创建一个带新 token 的本地 `Lease` 对象，便于测试、手工构造或反序列化场景。

**签名**

```python
@staticmethod
def new(
    pool: str,
    rid: str,
    *,
    consumer: str | None = None,
    slots: int = 1,
    ttl: float = 60.0,
    priority: int = 0,
    use_limit: int | None = None,
    trace_id: str | None = None,
    now: float | None = None,
) -> Lease: ...
```

**参数**

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `pool` | `str` | 必填 | 池名 |
| `rid` | `str` | 必填 | 资源 id |
| `consumer` | <code>str &#124; None</code> | `None` | 消费者标识 |
| `slots` | `int` | `1` | 占用槽位数 |
| `ttl` | `float` | `60.0` | 有效秒数，用于计算 `expires_at` |
| `priority` | `int` | `0` | 请求优先级 |
| `use_limit` | <code>int &#124; None</code> | `None` | 使用次数上限 |
| `trace_id` | <code>str &#124; None</code> | `None` | 追踪标识 |
| `now` | <code>float &#124; None</code> | `None` | 起始时间；缺省取当前时间 |

**返回**：新的 `Lease`。它只构造本地对象，不会向池或协调层注册；要取得可续约、可归还的有效租约，请使用 `acquire()`。

### `lease.expired()` {#lease-expired}

**用途**：判断租约是否已经到期。

**签名**

```python
def expired(now: float | None = None) -> bool: ...
```

**参数**：`now` 为可选 epoch 秒，缺省取当前时间。

**返回**：`now >= expires_at` 时返回 `True`，否则返回 `False`。

## 续约与归还

### `pool.renew()` {#pool-renew}

**用途**：为长任务显式续约；常规场景交给心跳自动保活。

**签名**

```python
async def renew(lease: Lease, *, used: int | None = None) -> float: ...
```

**参数**

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `lease` | `Lease` | 必填 | 要续约的租约 |
| `used` | <code>int &#124; None</code> | `None` | 上报当前使用次数；租约保留两次上报中的较大值，重试或乱序上报不会降低计数 |

**返回与异常**：返回新的 `expires_at`。`use_limit` 或 `max_lease_duration` 用尽时抛 `LeaseLimitReached`；协调层已回收或撤销 token 时抛 `LeaseLost`；默认 fail-closed 下协调层不可用抛 `BackendUnavailable`。

**示例**

```python
try:
    expires_at = await pool.renew(lease, used=lease.used)
except LeaseLimitReached:
    ...
except LeaseLost:
    ...
```

**注意**：心跳由 `LeaseManager` 按 `lease_ttl / 3` 自动续约；`max_lease_duration` 会给续约到期时间封顶，总寿命用尽后停止续约并由 reaper 回收。跨进程调用时可先更新本地 `lease.used` 再续约。

### `pool.release()` {#pool-release}

**用途**：归还租约，并可选地上报结果、用量和业务延迟。

**签名**

```python
async def release(
    lease: Lease,
    *,
    outcome: UsageOutcome | str | None = None,
    used: int | None = None,
    latency_ms: float | None = None,
) -> bool: ...
```

**参数**

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `lease` | `Lease` | 必填 | 要归还的租约 |
| `outcome` | <code>UsageOutcome &#124; str &#124; None</code> | `None` | 省略时使用 `lease.outcome`；合法字符串会转换为枚举，未知字符串抛 `ConfigError` |
| `used` | <code>int &#124; None</code> | `None` | 省略时按 `lease.used or 1` 计量；负值抛 `ConfigError` |
| `latency_ms` | <code>float &#124; None</code> | `None` | 业务延迟；负值抛 `ConfigError`。`outcome=OK` 时用于更新资源健康 EWMA |

**返回与异常**：归还成功后返回 `True`。token 已不在协调端时幂等返回 `True`，不扣减存储槽位；token 仍在但存储槽位对不上时抛 `LeaseConflict`。默认 fail-closed 下协调层不可用抛 `BackendUnavailable`；配置 `fail_open=True` 时部分路径会按本地降级状态继续。

**示例**

```python
from purser import UsageOutcome

await pool.release(
    lease,
    outcome=UsageOutcome.RATE_LIMITED,
    used=1,
    latency_ms=120.0,
)
```

**注意**：`pool.lease()` 上下文管理器固定传 `used=lease.used`（默认 0），所以该路径默认记 0；要统计真实用量，需在块内设置 `lease.used`。`SyncPool.release()` 支持 `latency_ms`，`ThreadPool.release()` 和 `RemotePool.release()` 不支持该参数。

### `pool.release_consumer()` {#pool-release-consumer}

**用途**：归还某个 consumer 在当前池持有的全部活跃租约。

**签名**

```python
async def release_consumer(
    consumer: str,
    *,
    outcome: UsageOutcome | str = UsageOutcome.OK,
    operator: str = "system",
) -> list[Lease]: ...
```

**参数**

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `consumer` | `str` | 必填 | 消费者标识 |
| `outcome` | <code>UsageOutcome &#124; str</code> | `UsageOutcome.OK` | 所有租约统一使用的归还结果 |
| `operator` | `str` | `"system"` | 审计操作者 |

**返回**：归还的租约列表；没有匹配租约时返回空列表。

**示例**

```python
from purser import UsageOutcome

released = await pool.release_consumer(
    "crawler-a", outcome=UsageOutcome.OK, operator="scheduler"
)
```

### `UsageOutcome` 副作用 {#usageoutcome}

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

### `pool.admin.force_revoke()` {#force-revoke}

**用途**：立即撤销资源上的任意一条或指定一条活跃租约，并释放槽位、记录结果与事件。

**签名**

```python
async def force_revoke(
    rid: str,
    *,
    operator: str = "system",
    reason: str,
    token: str | None = None,
) -> Lease: ...
```

**参数**

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `rid` | `str` | 必填 | 资源 id |
| `operator` | `str` | `"system"` | 审计操作者 |
| `reason` | `str` | 必填 | 撤销原因 |
| `token` | <code>str &#124; None</code> | `None` | 指定租约 token；省略时只撤销该资源的任意一条活跃租约 |

**返回与异常**：返回被撤销租约的快照。指定 token 不存在时抛 `LeaseConflict`；资源没有任何活跃租约时抛 `BadState`。

**示例**

```python
revoked = await pool.admin.force_revoke(
    rid="r42",
    operator="alice",
    reason="credential leak",
    token=lease.token,
)
```

**注意**：强制撤销会给被撤销租约的 consumer 写入 10 秒资源级冷却、记录 `preempted` 用量并发布 `lease_revoked` 事件。需要撤销该资源的全部租约时，由调用方遍历当前租约并容忍并发归还：

```python
for lease in list(await pool.leases(rid="r42")):
    try:
        await pool.admin.force_revoke(
            rid="r42",
            operator="alice",
            reason="credential leak",
            token=lease.token,
        )
    except LeaseConflict:
        pass
```

## 相关事件

获取、归还、过期、失联、强制撤销分别发布 `lease_acquired` / `lease_released` / `lease_expired` / `lease_lost` / `lease_revoked`；事件字段与回放接口见 [Events](../rest-api/events.md)。
