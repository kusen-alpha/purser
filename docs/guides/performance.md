# 性能与容量

Purser 的延迟主要来自后端与网络，而不是调度算法本身。先定位“慢在哪一层”，再调参数；盲目加大 `top_k` 或线程数通常只会放大争用。

## 延迟构成

一次 `acquire()` 大致经过：

```text
参数校验 → 候选采样（存储层）→ 硬约束过滤 → 分配器打分 Top-K
→ 请求级校验（可选，可能访问目标站）→ 原子占用（协调层 + 存储层）
```

| 阶段 | 成本来源 | 典型优化 |
| --- | --- | --- |
| 候选采样 | SQLite/MySQL 查询、Redis 往返 | 缩小池、加索引型标签、避免超大 `tags` 组合 |
| 分配打分 | CPU，候选数 × 打分 | 控制 `top_k`；池内资源多时优先 `health_weighted` 的采样路径 |
| 请求级校验 | 目标站网络 RTT | 配 `check_cache_ttl`；不要在热路径做全量验活 |
| 原子占用 | 存储 CAS + 协调 Lua | 多实例用 MySQL + Redis；SQLite 单写者 |
| 等待 | 池满/资源忙 | 扩容、缩短占用、调 `wait_timeout`、按业务拆池 |

验活是网络密集型操作，通常比调度本身慢几个数量级。导入时的大批量验活应走后台任务。

## 后端成本

| 组合 | 写入 | 读/采样 | 适用 |
| --- | --- | --- | --- |
| `memory` + `memory` | 进程内，最快 | 进程内 | 单进程、测试、低延迟 |
| `sqlite` + `memory` | 单写者，WAL，锁冲突短重试 | 本地查询，随机窗口采样 | 单机持久化 |
| `sqlite` / `mysql` + `redis` | 存储 CAS + Redis Lua | 存储查询 + Redis 往返 | 多实例 |
| `mysql` + `memory` | 网络 + 事务 | 网络查询 | 单实例、复用 MySQL |

- SQLite 的并发写是串行的；多个进程写同一文件会频繁 `database is locked`。需要横向写扩展时换 MySQL。
- Redis 协调层每次获取/续约/归还需要网络往返；把 Redis 放在应用同机房，避免跨地域调用。
- 后端每次操作都有超时（Redis 连接/操作 5 秒）；超时会 fail-closed 为 `BackendUnavailable`，不会无限挂起。

## 关键参数

| 参数 | 默认 | 调大的影响 | 调小的影响 |
| --- | --- | --- | --- |
| `top_k` | `8` | 候选更多、更抗惊群，打分 CPU 上升 | 更快，但更容易集中到少数资源 |
| `wait_timeout` | `30s` | 等待更久，占用连接/请求 | 更快失败，调用方重试压力上升 |
| `lease_ttl` | `60s` | 崩溃后回收更慢，误回收更少 | 回收更快，但心跳/时钟抖动更敏感 |
| `heartbeat_interval` | `ttl/3` | 协调层写压力下降 | 租约更稳，但 Redis QPS 上升 |
| `max_slots` | 适配器默认 | 单资源并发更高，隔离性下降 | 争用更多，但单资源更安全 |
| `max_leases_per_consumer` | `None` | 允许单消费者占更多 | 防止单消费者打满池 |
| `validate_concurrency` | `8` | 验活吞吐上升，目标站压力上升 | 更温和，但积压更久 |
| `background_validate_sample` | `1.0` | 更多后台验活 | 更少探活，健康分更陈旧 |
| `check_cache_ttl` | `2s` | 减少重复站点探测 | 结果更实时 |
| `import_validate` | `auto` | 小批量同步、大批量后台 | 显式 `none` 可跳过首次验活 |
| `event_queue_size` | `10000` | 抗突发、内存上升 | 更省内存，更早丢事件 |
| `usage_ledger_size` | `50000` | 报表窗口更长、内存上升 | 更省内存，报表更短 |

`scheduler="token_bucket"` + `rate_limit` / `rate_burst` 可以限制**每个消费者**的获取速率，适合保护上游；它不是全局限流，也不能替代网关。

## 容量估算

### 租约心跳

活动租约约 `N` 条、心跳周期 `H` 秒时，协调层每秒约承受 `N / H` 次续约，外加获取/归还需要。把 `heartbeat_interval` 显式设为 `lease_ttl/3` 左右，不要为了“省 Redis”设得接近 TTL，否则一次抖动就会丢租约。

### 等待队列

等待者数量受 `wait_timeout` 和上游并发控制。`max_leases_per_consumer` 与 `max_slots` 决定单个消费者能占多少槽位；容量不足时优先按业务拆池，而不是无限加大 `wait_timeout`。

### 内存

粗算：`资源数 × 单条 payload 大小` + 事件环形缓冲 + 用量账本 + 审计（配置 `audit_path` 时全部历史在内存）+ Python 运行时。资源 payload 很大时，先降低 `event_queue_size` / `usage_ledger_size`，并评估审计归档。

## 基准测试

仓库自带两个脚本：

```bash
.venv/Scripts/python.exe benchmarks/import_time.py

# 可选外部后端
export PURSER_TEST_REDIS_URL=redis://127.0.0.1:6379/15
export PURSER_TEST_MYSQL_URL=mysql://root:@127.0.0.1:3306/purser_bench
.venv/Scripts/python.exe benchmarks/sync_facade_bench.py --threads 8 --ops 200
```

`sync_facade_bench.py` 会对可用的 storage × coordination 组合跑同一套获取/归还负载，输出吞吐与延迟分位。用自己的负载复测时至少记录：

- 后端组合、池大小、`max_slots`、`top_k`；
- 并发消费者数与目标 QPS；
- p50 / p95 / p99 获取延迟与失败率；
- Redis/MySQL 的 CPU、连接数与网络 RTT；
- 事件丢弃数、`lease_lost_total`、`feedback_dropped_total`。

只看平均值会掩盖长尾；容量按 p95/p99 与失败率定，不按均值定。

## 常见瓶颈

| 现象 | 更可能的原因 | 处理 |
| --- | --- | --- |
| 接入层 CPU 高、p99 抖动 | 事件循环被同步代码阻塞 | 把阻塞调用移入线程；不要在心跳/监听器里做同步 IO |
| SQLite `database is locked` | 多进程/多线程写同一文件 | 单写者，或换 MySQL |
| 获取延迟随池规模线性上升 | 候选采样/打分成本 | 控制 `top_k`；按标签/地区拆池 |
| 租约频繁丢失 | 心跳间隔过大、事件循环卡顿、Redis 超时 | 调小 `heartbeat_interval`、排查阻塞、检查 Redis |
| 验活拖慢导入 | `validate_concurrency` 过高或目标站限流 | 降低并发、改后台验活、加探活目标 |
| 等待超时多但池看似有空闲 | 标签/`required_tag`/目标校验过滤掉了候选 | 看 `NoSuitableResource` 与站点失败索引 |
| 内存持续上涨 | 资源多、payload 大、审计无归档 | 调整环形缓冲、归档审计、精简 payload |

## 不要做的事

- 不要用 `stateless` 模式去模拟互斥；它不占槽位。
- 不要用超长 `lease_ttl` 代替心跳；崩溃后回收会非常慢。
- 不要在多实例下把 Redis 当纯缓存；丢租约会导致重复占用。
- 不要为了“更快”关闭 `fail_open` 以外的安全检查；`PURSER_ALLOW_SHARED_BACKENDS` 不是性能开关。
- 不要在热路径里全量 `query()` 或逐条 `stats()`；用分页与报表接口。

相关页面：[指标目录](../reference/metrics.md)、[可观测性](observability.md)、[多实例部署](multi-instance.md)、[后端配置示例](backend-configuration.md)、[生产检查清单](production-checklist.md)。