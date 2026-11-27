# 术语表

按主题分组，英文是代码与 API 中的实际标识。术语的完整契约见对应参考页。

## 资源

**资源（Resource）** — 一条可被租用的对象，如代理、账号或任意自定义类型。由 `id`、`kind`、`payload`、`tags`、`state`、槽位、健康样本、配额等字段组成。

**资源池（Pool / ResourcePool）** — 同 `kind` 资源与一套调度配置的集合。池名在 `Purser` 实例内唯一。

**适配器（Adapter / ResourceAdapter）** — 定义一种资源如何生成、验活、类型化装载（hydrate）与处置的插件。内置 `proxy` / `account`；自定义类型通过 `register_adapter()` 接入。

**类型化 payload** — 适配器声明的字段（如 `host` / `port`）在读取时成为类型化属性；未声明的键进入 `Resource.extra`，不会丢弃。

**标签（tags）** — 用于调度筛选的集合；获取时要求“全部包含”。不是业务字段，也不参与健康度。

**必需标签（required_tag）** — 单个硬约束标签，组合租约用它把账号的 `region` 绑定到代理的 `region:us`。

**目标（target）** — 本次使用要访问的站点标识，用于请求级校验、站点失败记忆与报表分组；不是资源字段。

**槽位（slot）** — 一条租约占用的并发单位。`max_slots=1` 为独占，大于 1 为共享；资源在 `ACTIVE` 与 `FULL` 之间随占用变化。

**消费者（consumer）** — 调用方标识，用于公平队列、重复持有判定、消费者级冷却、租约上限与报表分组。跨进程/实例必须唯一。

## 状态与健康

**状态（state）** — 12 个生命周期状态：`new` / `validating` / `idle` / `active` / `full` / `quarantined` / `suspended` / `quota_exhausted` / `banned` / `lost` / `draining` / `archived`。只有 `idle` 与 `active` 可分配。见[状态机](../reference/state-machine.md)。

**验活（validate）** — 调用适配器的 `validate()` 判断资源是否可用，产生健康报告并驱动状态迁移。

**健康样本（health sample）** — 一次验活或使用反馈产生的存活、延迟与错误记录。分配器按健康分排序，`stale_after` 后样本过期。

**隔离（quarantine）** — 验活失败后的退避状态；到期可回 `idle` 或先重新验活。参数在 `quarantine.*`。

**封禁（ban）** — 隔离连续失败达到阈值后的状态；`unban_mode` 决定是否以及何时自动解封。

**暂停（suspend）** — 人工下线，资源保留但不可分配；`resume` 恢复。

**排空（drain）** — 停止签发新租约，等存量租约归还后归档；可 `cancel_drain`。

**召回（recall）** — 按导入批次批量执行 `requalify` / `isolate` / `retire`。

**失联（lost）** — 租约或资源在协调层消失、无法对账；`lost_gc` 在 `lost_retention` 后归档。

**归档（archive）** — 资源退出调度；`archive_gc` 在 `archive_retention` 后擦除 payload 并删除。

## 租约

**租约（lease）** — 一段时间内对槽位的占用凭据，带唯一 `token`、TTL、心跳与使用上限。

**token** — 租约的唯一句柄；归还、续约、定向强制撤销都用它匹配。

**TTL** — 租约寿命（`lease_ttl`）。心跳按 `lease_ttl/3` 续约；过期后由 reaper 回收。

**心跳（heartbeat）** — 后台续约机制；`max_lease_duration` 是总寿命封顶。

**回收器（reaper）** — 扫描过期租约并释放槽位的后台任务。

**续约（renew）** — 显式延长租约；达到 `use_limit` 或总寿命时抛 `LeaseLimitReached`。

**强制撤销（force revoke）** — 管理员提前终止租约；会释放槽位、给消费者写冷却并发布 `lease_revoked`。

**用量结果（UsageOutcome）** — 归还时上报的 `ok` / `captcha` / `rate_limited` / `banned` / `timeout` / `network_error` / `quota_exhausted` / `preempted`，直接影响健康、隔离与配额。

**幂等键（idempotency key）** — 获取租约时同一键重复请求返回同一份仍存活的租约；REST 也可用于创建池、导入与恢复。

**组合租约（bundle）** — 跨池一次获取多个成员（如账号 + 代理），失败时回滚已获取成员。`atomic` 全有或全无，`best_effort` 返回部分。

**锚点（anchor）** — 组合中先获取、供其他槽派生约束的成员；`MatchRule` 从锚点读取字段并生成 `required_tag`。

**两阶段（two-phase / strict）** — 组合租约的严格模式：成员在全局顺序下拼装，确认时原子生效；默认关闭。

## 调度

**分配器（allocator）** — 对候选资源打分的策略：`health_weighted`（默认）/ `random` / `round_robin` / `least_loaded` / `static_priority`，也可注册自定义实现。

**过滤器（filter）** — 获取前的硬约束检查（状态、标签、槽位、冷却、配额、时间窗等）；被跳过的原因会出现在 `PoolExhausted` 消息中。

**Top-K** — 分配器只对前 K 个候选做加权随机，避免惊群；`top_k` 默认 8。

**亲和（affinity）** — 按历史使用给同一消费者/键的资源加权；需要 `affinity_weight > 0`。Python `AcquireRequest.affinity` 支持，REST 请求体不接收该字段。

**优先级（priority）** — 等待队列排序依据；开启 `preemptible` 后高优先级还可抢占低优先级租约。

**抢占（preemption）** — 高优先级请求强制回收低优先级租约；需要 `preemptible=True` 且优先级差达到 `preempt_threshold`。

**调度器（scheduler）** — 等待队列的排队策略：默认 `priority_quota`（按优先级与配额排队），也可选 `fifo` 或 `token_bucket`（配合 `rate_limit` / `rate_burst` 做每消费者限速）。

**准入（admission）** — 组合租约在获取成员前的配额与余量检查；由 `BundleFairness` 控制。

## 后端与部署

**存储层（storage）** — 资源主数据、状态与审计的持久化平面：`memory` / `sqlite` / `mysql`。

**协调层（coordination）** — 运行期租约、槽位、心跳与唤醒的共享平面：`memory` / `redis`。多实例必须共享 Redis。

**对账（reconcile）** — 启动或维护时用存储状态与协调层租约对齐，收敛崩溃残留。

**fail-closed / fail-open** — 协调层不可用时的策略：默认 fail-closed（拒绝并抛 `BackendUnavailable`）；`fail_open=True` 降级为本地模式继续数据面。

**门面（facade）** — 同一内核的不同调用形态：`Purser`（异步）、`SyncPurser`（阻塞）、`ThreadPurser`（线程）、`ProcessPurser`（进程）、`RemotePool`（IPC）。

**预设（profile）** — 建池时的参数起点：`basic` / `validated` / `shared` / `long_running` / `quota` / `random` / `one_shot`；显式字段始终覆盖预设。

**维护任务（maintenance）** — 周期扫描：`quota_reset` / `periodic_unban` / `lost_gc` / `validating_gc` / `quarantine_gc` / `archive_gc` / `expire_gc` / `reconcile`。

**审计链（audit chain）** — 追加写的 SHA-256 哈希链；`verify()` 能发现条目被修改。它不是访问控制，也不阻止整文件删除。

**指标（metrics）** — 进程内计数器与直方图；Prometheus 文本通过 `/metrics` 暴露，前缀 `purser_`。

**事件（event）** — 状态与租约变化的通知，带 `seq`、`type`、`pool`、`rid`、`data`、`ts`、`trace_id`；通过 `ps.bus.subscribe()` 或 SSE 消费。

**trace_id** — 一次获取/管理请求的关联 id，出现在事件、审计与响应头中，用于跨日志追踪。

相关页面：[状态机](../reference/state-machine.md)、[数据模型](../reference/data-models.md)、[配置参考](../reference/configuration.md)、[指南概览](index.md)。