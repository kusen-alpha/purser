# 算法与不变量

这一页提炼内核里“不应随意修改”的规则：每个算法先写问题与不变量，再写可调参数与失败路径。完整推导见仓库中的设计文档。

## 1. 分配：硬约束先淘汰，软偏好只打分

流程固定：

```text
硬约束过滤 → 分配器打分(Top-K) → 加权随机首选 + 按分回退 → 请求级校验 → 首个成功原子占用
```

- **硬约束**：`required_tag`、状态是否可分配、槽位是否足够、冷却/过期/配额。不满足直接排除，不参与打分。
- **软偏好**：`health*w1 + age*w2 - load*w3 + preference + jitter`，只在候选中排序，不能把不可用资源重新纳入候选。
- **Top-K + 加权随机**：默认 `top_k=8`，抗惊群；`jitter` 默认 `0.05`。
- **请求级校验**：按 `target` 做站点探测，候选全部被拒抛 `NoSuitableResource`。

默认权重见 `Weights`：`health=1.0`、`age=0.15`、`load=0.30`、`preference=0.0`、`affinity=0.0`、`jitter=0.05`。

失败路径：空池立即 `PoolEmpty`；有资源但忙则进公平队列，超时 `PoolExhausted`（消息携带各状态数量）。

## 2. 租约：token + 心跳 + 回收器

- 每次获取生成唯一 `token`；归还必须 token 匹配（`LeaseConflict`）。
- 心跳默认按 `lease_ttl/3` 续约；后端拒绝续约立即发布 `lease_lost`。
- reaper（租约回收器）回收过期租约；`max_lease_duration` 到达后拒续，心跳停跳后由 reaper 回收。
- `force_revoke` 带 `operator` + `reason`，写审计并发事件。
- 迟到 release（token 已不在协调端）幂等，绝不触碰新持有者的槽位。

不变量：**任何时刻资源已占槽位 ≤ `max_slots`，且 ≥ 0**。归还、强制撤销、过期回收都必须走同一条槽位回退路径。

## 3. 公平性：优先级配额 + FIFO

- 默认调度器 `priority_quota`：高优先级可优先调度，同级 FIFO。
- 等待者被取消、池关闭或资源状态变化时必须被清扫，不能留下“残留等待者”。
- `PoolEmpty`（完全没有资源）与 `PoolExhausted`（暂时不可满足）语义分开：前者立即失败，后者才等待。

不变量：**等待者不会因并发取消而泄漏槽位**；`_commit` 在任意点被取消都会回滚已占槽位并注销协调租约。

## 4. 隔离与封禁：先隔离，最后才封

- 退避时长：`min(cap, base*2^(n-1)) + jitter`；默认 `base=30s`、`cap=1800s`、`jitter=10s`。
- 默认 `max_failures=3`；24 小时试用期内阈值减半，且**首次失败绝不直接封禁**。
- `unban_mode=mixed`：`unban_after=86400s` 后自动转 `VALIDATING`，也可人工 `unban`。
- 使用中被风控（如 CAPTCHA）可直达 `BANNED`，与“验活失败”区分。

不变量：**处罚性状态（BANNED）必须有显式原因与审计**；隔离是默认路径，封禁是升级路径。

## 5. 健康分：EWMA

- 首次验活 `alpha=0.6`，之后 `alpha=0.3`；冷启动默认 `0.5`。
- 健康分只影响排序，不改变状态；连续失败由隔离策略处理。
- `stats().avg_health` 覆盖池内全部有样本资源，不受分页上限影响。

## 6. 请求级校验与站点失败索引

- 按目标站记录失败，`site_failure_ttl=300s` 到期自动清理，`site_failure_max=16` 限制条目。
- 校验结果短缓存 `check_cache_ttl=2s`，避免同一请求窗口内重复探测。
- 站点失败只降权/拒绝本轮候选，不直接封禁资源。

失败路径：目标站持续失败会返回 `NoSuitableResource`；资源本身状态不变。

## 7. 一致性：CAS、Lua 与重启对账

- **存储写**：`version` 乐观锁；`expected_version=0` 仅创建，冲突抛 `VersionConflict`。
- **槽位变更**：`transition_acquire` / `transition_release` 原子 CAS，跨实例不超卖。
- **Redis 协调**：每资源哈希 + token 索引，Lua 保证注册/续约/注销原子性；强制 RESP2 兼容 Redis < 6。
- **fail-closed**：协调端连接/操作失败抛 `BackendUnavailable`，不降级为内存状态。
- **重启对账**：`start()` 按协调端当前存活租约之和重设槽位；资源主数据不受影响。

不变量：

1. 槽位计数永不为负、永不超 `max_slots`。
2. 迟到 release 不改变当前持有者的槽位。
3. 对账完成后的稳态下，“协调端存活租约之和”是占用槽位的权威值；对账前两者可能短暂不一致。
4. 审计链 append-only，可校验。

## 可调参数

| 参数 | 默认 | 影响 |
| --- | --- | --- |
| `allocator` | `health_weighted` | `round_robin` / `least_loaded` / `static_priority` |
| `top_k` | `8` | 随机候选数 |
| `weights` | 见上 | 健康/空闲/负载/偏好/亲和/抖动 |
| `lease_ttl` | `60s` | 租约寿命 |
| `heartbeat_interval` | `ttl/3` | 心跳周期 |
| `max_lease_duration` | `null` | 单租约总寿命上限 |
| `wait_timeout` | `30s` | 获取等待上限 |
| `quarantine.*` | `30s / 1800s / 10s / 3` | 退避与封禁阈值 |
| `site_failure_ttl` | `300s` | 站点失败记忆时长 |
| `check_cache_ttl` | `2s` | 请求级校验缓存 |
| `archive_retention` | `30d` | 归档保留 |
| `lost_retention` | `4h` | 失联判定失效时长 |

完整字段见 [配置参考](../reference/configuration.md)。
