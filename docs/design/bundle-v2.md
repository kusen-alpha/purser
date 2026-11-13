# Bundle v2：跨实例严格原子与整体公平（配置化设计）

> 状态：M1 / M2 / M3 已实现（`purser/core/bundle_store.py` + `Purser` 开关），全部默认关闭；本文保留为设计依据。
> 原则：所有高级能力 **默认关闭**；不启用时 `acquire_bundle` / `release_bundle` 走现有 `rollback` 路径，行为与 v1 完全一致，内核其余路径零改动。

## 0. 背景与边界

`acquire_bundle` v1 提供跨池组合获取（O8）：单调度进程内原子（逐槽 acquire + 失败回滚）、`MatchRule` 约束派生、`max_retries` 抖动重试、REST `bundle` 字段。它解决的是"一个进程编排组合"。

v2 要补两件 v1 覆盖不到的事，都只在特定部署下才有意义：

| 能力 | 解决的痛点 | 何时需要 |
| --- | --- | --- |
| 跨实例严格两阶段（reserve → confirm） | 多进程/多机共享 Redis 时，组合拼装存在"半占用窗口"与崩溃残留；靠超时回滚收敛但会震荡、且崩溃后要等心跳回收 | 两个及以上 Purser 实例共享同一协调平面 |
| 跨池整体公平配额 | bundle 是"整套"单位，现有每池各自排队无法限住并发组合、无法给整套整体优先级、组合会把稀缺池挤爆 | 资源紧张 / 需要按 consumer 控制并发套数 |

两者都通过配置开启；未开启时，下面所有新对象、协调层方法和计数都不参与执行。

## 1. 现状路径（改动基线）

`Purser.acquire_bundle` 当前流程（`bundle_mode="rollback"`）：

1. 校验 `BundleRequest`（槽池唯一、锚点在槽内、`max_retries >= 0`）。
2. `bundle_id` 幂等查重（进程内 `self._bundles`）。
3. 排序：`scarce_first`（带锚点依赖约束）或 `declared`。
4. 逐槽 `pool.acquire`（真实租约：storage.transition_acquire + coordination.register_lease + LeaseManager 心跳）。
5. 任一槽失败：已拿成员逆序 `release`，瞬态失败按 `max_retries` 抖动重试；`best_effort` 返回部分。
6. 全部到位：注册 `BundleLease` 到 `self._bundles`。

这条路径的"原子性"建立在**编排者单进程**这个前提上。两阶段与公平都是在这个流程外层加配置门控的阶段，不替换内核。

## 2. 严格两阶段（reserve → confirm）

### 2.1 开关

```python
Purser(..., bundle_mode: Literal["rollback", "two_phase"] = "rollback")
BundleRequest(..., strict: bool | None = None)  # None = 跟随 Purser 默认
```

- 默认 `"rollback"` → 完全走第 1 节流程，无任何新调用。
- `"two_phase"`（或逐请求 `strict=True`）→ 组合走下面的协议。
- 协调平面只支持 memory/redis；memory 下两阶段仍正确（等价 rollback 加书签），价值只在跨实例，文档注明。

### 2.2 协议

目标：两个实例同时拼装组合时，**不出现 ABBA 死锁窗口（用全局确定性序消除，而非仅靠超时收敛）**，且编排进程崩溃不留下半套占用。

1. `intent`：进入两阶段时，在协调平面申请全局单调序号（Redis：`INCR bundle:seq`；Memory：进程内计数器）绑定 `bundle_id`。该序号只用于排序，不落资源。
2. 全局序：槽位按**确定性全序**执行（默认 `(pool, resource_id)`；`match` 槽仍排在其 `anchor_pool` 之后）。所有实例按同一全序抢资源，环不存在，ABBA 死锁从结构上消除；只会出现"资源缺失 → 等待/超时"。
3. `reserve`：每拿到一个成员（真实租约），写 `reserve:{bundle_id}` 成员书签 `{pool, rid, token, expires_at}`，TTL = `reservation_ttl`（默认 30s）。成员本身仍是正式租约，槽位计数不变。
4. `confirm`：全部成员到位后，单条 Lua（Redis）或多对象原子提交（Memory）把 `reserve:{bundle_id}` 置为 `committed`，随后返回 `BundleLease`。
5. `abort`：任一步失败，显式 `release` 已拿成员并删除书签；同 v1 的 `max_retries` 退避语义保留，但重试前必须 `abort` 干净。
6. 崩溃兜底：编排进程死亡时，书签 TTL 过期；成员租约按自身心跳过期回收。新增维护任务 `bundle_gc`（`bundle_gc_interval`，默认 30s）扫描过期书签并 `force_revoke` 其残留成员，避免"书签没了但租约还活着"的空窗。`bundle_gc` 同时为两类活组合保活刷新标记：本进程仍持有的组合、以及成员租约仍存活的已提交组合（跨实例），避免公平计数欠账；已提交且成员已归还的过期标记只清理不误踢。

### 2.3 协调层新增（仅两阶段启用时调用）

实现落点：新增独立的跨池 `BundleStore`（`core/bundle_store.py`），而不是改 per-pool coordination 协议。`MemoryBundleStore` / `RedisBundleStore` 各一份，key 空间为 `purser:bundle:v2:*`，普通租约路径完全感知不到。接口即本节列出的原语：

```python
reserve_member(bundle_id, pool, rid, token, ttl) -> bool     # 写书签，CAS
confirm_reservation(bundle_id) -> bool                      # Lua/内存原子置 committed
abort_reservation(bundle_id) -> None
list_reservations(bundle_id) -> list[ReservationMember]
expired_reservations(now) -> list[str]                      # 供 bundle_gc 扫描
next_bundle_sequence() -> int
```

Memory 与 Redis 各一份实现；Redis 的 `confirm_reservation` 与 `next_bundle_sequence` 用 Lua 保证原子。这些键独立命名空间（`bundle:*`），普通租约路径完全感知不到。

> 实现备注：Redis 侧不用 Lua——确认是单条 `HSET`（天然原子），序号用 `INCR`，到期扫描用 sorted set（`ZRANGEBYSCORE`），避免 `KEYS`。

> 实现备注：只要启用了两阶段或公平（store 非空），组合成员（`pool, rid, token`）就写入书签并在成功时 `confirm`——因此 rollback + 公平模式的标记同样携带成员租约，`bundle_gc` 可跨实例验证活性；默认纯 rollback（无高级开关）完全不建 store，行为与 v1 一致。

### 2.4 语义与失败

- 资源不够：成员等待/超时按池 `wait_timeout` 走，超时整体 `abort` + `BundleAcquireFailed`。
- 并发同序：两个 bundle 拿到不同成员不会互相持锁等待，只有最后一个稀缺成员上的竞争，输家 `abort` 后按退避重试。
- `best_effort`：两阶段模式下一部分成功后即 `confirm` 该子集？否——两阶段的语义是"要么一套要么没有"；`best_effort` 与 `two_phase` 互斥（配置校验报错），避免语义混淆。
- 分析：单实例 `two_phase` 退化为「书签版 rollback」，正确但无额外价值，不建议开。

## 3. 跨池整体公平配额

### 3.1 开关

```python
@dataclass(frozen=True)
class BundleFairness:
    max_per_consumer: int | None = None          # 同一 consumer 并发组合上限
    max_total: int | None = None                 # 全局限并发组合上限
    per_pool_headroom: int = 1                   # 每个池为“非组合获取”保留的空位
    wait: bool = True                            # True=排队等待；False=立即抛 BundleQuotaExceeded
    timeout: float | None = None                 # 配额排队超时

Purser(..., bundle_fairness: BundleFairness | None = None)  # 默认 None = 关闭
```

`None` 时 `acquire_bundle` 完全不含准入逻辑。

### 3.2 模型

- 公平计数器挂在 **Purser 编排层**（不在任何单个池上）：`active_bundles`（全局）、`active_by_consumer`、`bundle_members_per_pool`。
- 单机：进程内计数 + `asyncio` FIFO 准入队列。
- 多实例：计数放协调平面（Redis `hincrby bundle:fair:*` + Lua），与两阶段的全局序同一套设施。
- 准入在 `acquire_bundle` **取成员之前**执行；`release_bundle`（含 abort、回滚、`bundle_gc` 兜底）释放计数。

### 3.3 每池空位（防止组合挤爆稀缺池）

- 判别"稀缺"：组合成员进入某池前，检查该池当前 `bundle_members_per_pool >= IDLE - per_pool_headroom` 则组合整体进入该池的等待/拒绝（按 `wait`）。
- 效果：账号多、代理少时，组合最多占满 `IDLE - 1` 个代理，始终给纯代理请求留空位；这正是"账号多代理少，bundle 堵死代理"场景的解法。
- 空位计算用 `pool.counts()`（现成 API），仅准入阶段调用，不改分配器。

### 3.4 优先级

- v1：组合成员的优先级分开传，整体等待由最慢的池决定。
- v2（可选字段）：`BundleRequest.priority` 作用于准入队列（FIFO 内按优先级取）；成员槽仍用各自池语义。跨池整体优先级仅对准入生效，不做跨池工作保存式的全序调度。

## 4. 配置汇总

| 配置 | 默认 | 启用后的影响 |
| --- | --- | --- |
| `Purser(bundle_mode=...)` | `"rollback"` | 两阶段协议、书签、`bundle_gc` |
| `BundleRequest.strict` | `None`（跟随） | 逐请求覆盖两阶段 |
| `Purser(bundle_fairness=...)` | `None` | 准入队列、全局/按 consumer 上限、每池空位 |
| `BundleRequest.priority` | `0` | 准入队列内优先级（仅 `bundle_fairness` 开启时生效） |
| `reservation_ttl` / `bundle_gc_interval` | `30s` / `30s` | 两阶段书签与回收 |

所有默认值不启用任何高级流程；`PoolConfig` 无需新增字段，避免与现有每个池的配置语义纠缠（组合是跨池对象，配置挂 Purser 层）。

## 5. 不回归保证

1. 默认 `bundle_mode="rollback"` + `bundle_fairness=None`：`acquire_bundle` 的调用面、排序、幂等、重试、REST 行为与 v1 完全一致（用 v1 测试集直接回归）。
2. 协调层只新增方法；普通 `register_lease` / `transition_acquire` 路径不读 `bundle:*` 键。
3. 公平计数只作用在编排层，池的分配器、等待队列、配额语义不动。
4. 新增错误 `BundleQuotaExceeded` 与既有错误同族（`PurserError`），REST 映射沿用 409/422 规则。
5. 配置非法互斥（如 `best_effort + two_phase`）在构造/请求校验期直接报错，不进入执行。

## 6. 里程碑

- M1：协调层 reservation 原语（Memory/Redis + Lua）+ `bundle_mode` 开关 + `bundle_gc` + 两阶段测试（含模拟崩溃、双实例同序竞争）。
- M2：`BundleFairness` 准入 + 计数（内存，单机）+ 每池空位 + 测试。
- M3：公平计数上 Redis（跨实例） + `BundleRequest.priority` 准入排序。

每个里程碑独立可评审、可合入；未完成的里程碑不阻塞其余默认功能。

> 已实现并回测：M1（两阶段 confirm/回滚/崩溃 gc/保活刷新/best_effort 互斥/重试/确定性排序/Redis 协调端到端）；M2（每消费者、全局、每池空位、等待释放、回滚释放配额、超时、默认不建 store）；M3（`BundleRequest.priority` 每次释放唤醒最高优先级等待者、Redis 分布式等待用 `WAITER_POLL_INTERVAL=0.1s` 轮询共享计数实现跨实例级联唤醒（跨实例优先级为尽力而为）、rollback+公平标记携带成员租约可跨实例验活、Sync `bundle_gc`）。边界用例见 `tests/test_bundle_v2.py`。
