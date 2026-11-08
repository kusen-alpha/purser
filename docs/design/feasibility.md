# purser 同步版 / 多线程 / 多进程支持可行性分析

> 分析对象：当前工作树中的异步内核及其后端、公开 API 和测试，不以 Git HEAD 或历史测试总数代替现状。
> 初始分析及主要探针验证：2026-11-07；文档修订：2026-11-08。
> 状态：本文是可行性分析与实施建议。**同步门面已在 `purser/sync.py` 落地；单实例六组合已在真实 MySQL 8.4 与 Redis 7.2.4 上通过验收矩阵；V25/V26 及跨实例 V29/V32 竞态已修复；同进程/跨进程隔离守卫与存储↔协调双向绑定已落地；同步故障注入、长时压力与性能基准已补测；其余内核 P0 未修复。** 下文明确区分当前行为、实测反例和契约要求。

## 一、结论与 24 种部署组合

### 1.1 判定口径

四种运行形态分别是：

1. **同步版**：单进程、单实例，通过同步门面调用异步内核。
2. **多线程**：多个调用线程共享一个同步门面，门面拥有一个长期运行的事件循环线程；另一种变体是每个线程（或线程组）各持有一个门面，见 §1.4，仍属于本形态，不额外增加组合数。
3. **共享后端多进程**：每进程一个实例，共享同一资源集合的存储及协调状态。
4. **独立后端多进程**：每进程管理独立分片，不提供跨分片共享调度；也称 silo / 静态分片。

每种形态展开为三种存储 × 两种协调，共 24 个组合。下表中的“可设计”不是“当前已实现”，更不是生产验收通过。

必须分开考察三个维度：

| 维度 | 判定内容 | 不能替代它的证据 |
| --- | --- | --- |
| 功能覆盖 | 公开方法、公开对象、参数、返回值、生命周期、事件、迭代和插件契约是否覆盖 | 方法能调用、正常获取归还成功 |
| 正确性 | 容量、去重、取消、过期、恢复及故障路径是否满足约定 | 单次 CAS 成功、计数始终不超过上限 |
| 性能与运行质量 | 吞吐、延迟、锁等待、资源清理、故障恢复时间是否达到目标 | 没有抛异常、一次压力测试通过 |

本文不再把“100%”作为当前状态标签。若未来使用该措辞，应以**公开契约逐项验收**为准：同步版还需明确安全快照、实时租约代理等与原对象访问方式的差异；共享后端还需明确哪些语义是全局的；独立后端只验收每个分片内部语义，不能据此宣称全局公平、互斥或配额成立。

**结论：**

- 同步版与共享门面多线程的六种组合，均具有工程可行性；门面已实现。同步单实例六个组合已在真实 MySQL 8.4 与 Redis 7.2.4 上通过验收矩阵；V25/V26/V29/V32 竞态已修复并回归。同步门面的跨实例共享后端（同进程双实例 + 双 OS 进程，SQLite/MySQL + Redis）已完成核心租约语义验收；通用等待者已通过 100 ms 存储轮询实现跨实例唤醒（V03 修复），其余内核 P0（取消补偿、管理并发、失联清理等）仍阻断生产就绪。
- 单 loop 表示 Python 回调在一个线程执行，**不表示整次 API 调用串行**。协程会在 `await` 处交错。
- 共享后端多进程仍没有任何组合达到完整全局语义（0/6）；但 SQLite/MySQL + Redis 的核心租约语义（全局容量、可见性、撤销、陈旧归还、启动对账、TTL 回收、双进程竞争、通用/定向等待唤醒）已通过真进程验收，剩余缺口集中在推送通知（当前为轮询）、去重、维护接管与报表/事件作用域。
- 中心调度服务（`purser serve`）单副本时，六种存储/协调组合从客户端视角均可共享，已用真实 uvicorn × 六组合验收（6/6）；共享边界移至服务进程。该结论不覆盖多副本、HA/leader election 或生产级运维，详见 §1.5.1。
- 同机多进程/进程池另提供本地 IPC 调度（`purser.local`）：一个调度进程持有唯一 `SyncPurser`，worker 通过 `multiprocessing.connection`（Windows 命名管道 / POSIX UDS）以 JSON 协议接入，不经过 HTTP；数据面不按存储/协调后端分支，已用 `spawn` Process、`Pool`、`ProcessPoolExecutor`、worker 硬退出回收验收，并实测 memory/memory、SQLite + Redis、MySQL + Redis 三种组合，另三种组合未单独跑（§1.5.2）。该形态同样是单副本，多副本语义仍需共享后端。
- 各进程独立创建 `Purser` 时，只有 SQLite/MySQL + Redis 能同时共享存储与协调；memory + Redis、SQLite/MySQL + memory 的同名池现由运行时守卫在第二个进程 `start()` 时抛 `IsolationConflict`，memory + memory 则本就是互不相交的独立集合（§1.5.3）。同一 durable 存储上的协调端身份另由存储内的 `pool_bindings` 持久固定，换协调类型或命名空间会在 `start()` 被拒绝；反方向（同一 Redis 命名空间被不同存储复用，含 memory 与 durable 的混合对）由 Redis 内的实时标记在重叠运行期间拒绝（§1.5.4）。
- 独立后端六种组合均可设计，但只消除跨实例共享造成的部分问题。取消回滚、Redis 租约协议、同实例并发去重、生命周期等缺陷仍可能适用。
- 推荐方向仍是**异步内核 + 显式同步门面**。先建立可信测试和基础租约协议，再验收同步完整适配及共享后端扩展。

### 1.2 同步版：单进程、单实例

以下六行均以驱动可用、所需服务可达为前提；“单实例验收”仅指第三章门面契约与第七章验收矩阵，不包含第五章内核 P0 故障路径。

| 存储 | 协调 | 门面可行性 | 当前完整性 | 关键条件 |
| --- | --- | --- | --- | --- |
| memory | memory | 可设计 | 单实例验收通过（8 项） | 最轻；资源不持久化；不能直接跨线程暴露内部可变对象 |
| memory | Redis | 可设计 | 单实例验收通过（8 项，Redis 7.2.4 实连） | 资源仍不共享、不持久化；注意残留租约及 Redis 协议缺陷 |
| SQLite | memory | 可设计；优先考虑 | 单实例验收通过（8 项） | 必须提供 `sqlite_path`；适合单实例持久化，无外部服务 |
| SQLite | Redis | 可设计 | 单实例验收通过（8 项，Redis 7.2.4 实连） | 增加协调依赖，但不会自动增强单实例全部语义 |
| MySQL | memory | 可设计 | 单实例验收通过（8 项，MySQL 8.4 实连） | 连接在所属 loop 初始化；需要秘密字段的池须稳定密钥 |
| MySQL | Redis | 可设计 | 单实例验收通过（8 项，MySQL 8.4 + Redis 7.2.4 实连） | 两类客户端统一所有权；共享后端扩展前仍需修基础协议 |

验收矩阵见 [tests/test_sync_acceptance.py](tests/test_sync_acceptance.py)，覆盖公开方法面、生命周期与 CRUD、租约回写、迭代、事件、备份恢复、重启持久化、6 线程争用、等待与超时、关闭幂等及关闭竞争、管理操作。六个组合共 49 项通过（含 1 项公共 API 面检查）；MySQL 连接内网测试主机的 MySQL 8.4.0（root、空密码、专用库 `purser_sync_acceptance`），Redis 连接同一主机的 Redis 7.2.4（专用 DB 15）。既有 `tests/test_mysql.py` 5 项、真实 Redis 专项 9 项通过；[tests/test_sync_fault_stress.py](tests/test_sync_fault_stress.py) 6 项覆盖协调写失败回滚、协调删除失败保留租约、存储归还失败恢复 token、撤销/归还 60 轮竞争、8 线程 × 40 次长时压力与跨线程心跳；[tests/test_sync_cross_instance.py](tests/test_sync_cross_instance.py) 每个共享后端组合 7 项覆盖跨实例容量、可见性、撤销、陈旧归还、启动对账、定向等待、通用等待轮询唤醒、硬崩溃 TTL 回收与双进程竞争；[tests/test_sync_static_shards.py](tests/test_sync_static_shards.py) 以双 OS 进程覆盖五种静态分片形态的分片内单槽容量、跨分片隔离与重启语义（含 1 项重叠实例 xfail、1 项共享 Redis 池名反例断言）；[tests/test_service_central_matrix.py](tests/test_service_central_matrix.py) 以真实 uvicorn 子进程覆盖六组合的中心调度服务，6 项全通过（§1.5.1）；[tests/test_local_scheduler.py](tests/test_local_scheduler.py) 覆盖同机 IPC 调度，11 项全通过（§1.5.2）；[tests/test_cross_process_memory_guard.py](tests/test_cross_process_memory_guard.py) 覆盖 memory 半共享组合的跨进程拒绝与释放，9 项全通过（§1.5.3）；[tests/test_pool_binding.py](tests/test_pool_binding.py) 覆盖存储端与调度端的双向绑定，15 项全通过（§1.5.4）。异步共享后端双实例用例在配置真实 Redis 时直连网络，不再以 fakeredis 结论代替。启用真实服务后的全量为 1042 passed / 40 skipped / 1 xfailed。该矩阵不覆盖第五章仍存在的其他 P0、跨实例推送通知/去重等全局语义与生产容量；跨实例核心租约语义见 §1.5，静态分片见 §1.6，性能结果见 §7.4。

### 1.3 多线程：共享一个同步门面

| 存储 | 协调 | 门面可行性 | 当前完整性 | 关键条件 |
| --- | --- | --- | --- | --- |
| memory | memory | 可设计 | 门面已落地；争用/故障/长时压力通过；跨实例不适用（无共享平面） | 所有访问进入所属 loop；输入隔离、输出快照及租约代理必需 |
| memory | Redis | 可设计 | 门面已落地；争用/故障/长时压力通过；跨实例受主数据不共享限制 | Redis 客户端归所属 loop；不改变主数据在内存的事实 |
| SQLite | memory | 可设计；优先考虑 | 门面已落地；争用/故障/长时压力通过；跨实例受协调层不共享限制 | 单 loop 可并发等待 I/O；同步审计写仍可能阻塞该 loop |
| SQLite | Redis | 可设计 | 门面已落地；争用/故障/长时压力通过；跨实例核心租约语义已验收（§1.5） | 需要线程安全的提交、关闭与包装缓存协议 |
| MySQL | memory | 可设计 | 门面已落地；争用/故障/长时压力通过；跨实例受协调层不共享限制 | 优先传连接参数或所属 loop 内工厂，而不是外部已使用的连接 |
| MySQL | Redis | 可设计 | 门面已落地；争用/故障/长时压力通过；跨实例核心租约语义已验收（§1.5） | I/O 可重叠；吞吐同时受 loop、连接池、后端及插件影响 |

每线程各建一个 `Purser` 属于**多实例**，不是此表的共享门面模式。若这些实例共享后端，第五章问题即使在同一进程内也适用；当前实现直接拒绝这种半共享（§1.4.1）。

§1.2 的验收矩阵与故障/压力专项已覆盖 6 线程争用、关闭竞争、后端故障注入、撤销/归还竞争、跨线程心跳及 8 线程 × 40 次长时压力（MySQL 8.4、Redis 7.2.4 实连）；跨实例部分另见 §1.5 与 [tests/test_sync_cross_instance.py](tests/test_sync_cross_instance.py)。审计写阻塞、跨实例通知/等待与目标环境容量仍未验收，不能把上述结果等同于生产级多线程验收。

### 1.4 多线程：不共享一个同步门面

每个调用线程（或线程组）各自创建并持有一个 `SyncPurser`，每个门面拥有自己的事件循环线程；线程之间不共享门面对象。这属于**同进程多实例**，不是 §1.3 的共享门面模式；两个调度平面是否共享决定其正确性，而当前实现只允许两平面都全新、互不相交的并存（§1.4.1）。

| 存储 | 协调 | 共享基础 | 当前完整性 | 关键条件 |
| --- | --- | --- | --- | --- |
| memory | memory | 两平面均私有 | 不成立 | 各门面有独立资源集合与计数；复制资源也不会让占用计数原子共享 |
| memory | Redis | 仅租约记录可见，主数据与计数私有 | 不成立 | 与多进程同名组合同一问题；容量判断仍基于各自内存 |
| SQLite | memory | 存储共享、协调各自独立 | 不成立 | P0-2/V02：B 启动对账只看到自己的空租约表，会把 A 的占用清零；V05 同类交错可产生 1 槽多租约 |
| SQLite | Redis | 两平面共享 | 核心租约语义成立；完整全局语义不成立 | 同进程双门面已实测（§1.5）：容量、可见性、撤销、陈旧归还、启动对账、TTL 回收通过；等待通知、事件、去重、维护仍为实例本地 |
| MySQL | memory | 存储共享、协调各自独立 | 不成立 | 同 SQLite + memory；换数据库不能补上协调缺失 |
| MySQL | Redis | 两平面共享 | 核心租约语义成立；完整全局语义不成立 | 已实测（含 V29 CAS 重试修复）；剩余缺口与 SQLite + Redis 相同 |

证据边界：跨实例专项 [tests/test_sync_cross_instance.py](tests/test_sync_cross_instance.py) 正是本形态——同进程两个 `SyncPurser`、各自事件循环线程、共享 SQLite/MySQL + Redis，因此两平面共享的两行有直接实测。这类用例通过 `PURSER_ALLOW_SHARED_BACKENDS=1` 夹具模拟两台主机；生产代码中的同进程半共享已由 §1.4.1 的守卫拒绝。memory 协调两行由“协调层实例私有”推断，并有异步内核同进程探针 V02/V05 支持；未作为同步门面专项单列通过。

不共享门面不会自动带来隔离或正确性：

- **租约对象不可跨门面传递**：`SyncLease` 绑定所属门面与池，交给另一门面的 `release` 会抛 `ValueError("lease belongs to a different SyncPool")`（[purser/sync.py:447](purser/sync.py#L447)）。跨门面管理只能通过共享后端提供的查询与撤销接口。
- **命名空间按池名划分**：Redis 键为 `purser:{pool}:...`，同一 Redis DB 下同名池即同一租约域；不同资源集合必须使用不同池名或不同 DB。`Purser` 当前未暴露 `key_prefix`，幂等存储在整库范围共享（V20），不能假设不同池完全隔离。
- **实例内状态不合并**：等待队列、consumer 去重辅助状态、亲和性、验活任务、事件、用量、审计内存链、指标都属于各自门面；共享后端不会自动汇总（§2.4、§5.2）。
- **生命周期各自负责**：关闭一个门面不影响其他门面，但它持有的租约要等 TTL 或对账回收；每个实例都必须在自己的 `finally` 中关闭。

成本与选型：

- 每个门面是 1 个 loop 线程 + 默认 4 个工作线程的回调池 + 各自的后端连接、心跳任务与 Redis 客户端。N 个线程各建门面不是零成本并行；SQLite 文件写锁与 MySQL 行级 CAS 仍是共同瓶颈，§7.4 的单实例 MySQL 约 60 ops/s 不会随门面数线性放大。
- 需要跨线程共享容量与租约时，优先 §1.3 的共享门面；需要线程级隔离且能接受实例本地控制面时，SQLite/MySQL + Redis 可用本形态，但只承诺核心租约语义；memory 存储或 memory 协调只能限定单实例独占同一资源集合；需要完全独立的资源集合时按 §1.6 静态分片处理，而不是同池多实例。
- 上述取舍现在由库内守卫强制（§1.4.1）：同一进程内的多个 `Purser` 只能全独立，不能半共享。

#### 1.4.1 同进程多实例的运行时隔离约束

同一进程内允许多个 `Purser` 并存，但每个实例必须拥有全新的调度平面，不得与另一个存活实例交叉共享其中任何一部分。该约束在库内识别，并在接线点抛出 `IsolationConflict`（`ConfigError` 子类），而不是只靠文档约定。实现见 [purser/isolation.py](purser/isolation.py)：

| 共享对象 | 识别点 | 键（已去敏） |
| --- | --- | --- |
| 同一 SQLite 文件（池名不同也不行） | `create_pool` 构造 sqlite 存储时 | `realpath + normcase`；`:memory:` 与 `file:...?mode=memory` 是实例私有内存库，不参与 |
| 同一 MySQL 库（池名不同也不行） | `create_pool` 构造 mysql 存储时 | 规范化 `host:port/db`，忽略用户名/密码 |
| 同一 Redis DB（协调层是 memory 也禁止） | `Purser.__init__`（幂等存储）与 `create_pool`（Redis 协调） | 规范化 `host:port/db`，忽略凭据与 `redis`/`rediss` 差异；幂等键是整库范围，故初始化即认领 |
| 同一个 `mysql_connection` / `redis_client` 对象 | 对应后端构造时 | 对象身份（外部客户端跨 loop 共享本身不安全） |
| 同一个 `ResourceType` 实例 | `register_type` 与 `create_pool` | 对象身份（插件可能带可变状态） |

语义细节：

- 冲突只在**不同存活实例**之间判定；同一实例的多个池共用同一后端是正常形态，按实例维度计数，删池逐个释放认领。
- 认领随 `Purser.close()` / `SyncPurser.close()` 释放，`delete_pool` 只释放该池的认领；实例被回收后由弱引用清理，残留认领不会永久阻塞后续实例。
- `create_pool` 在构造后端之前原子认领，任一冲突即整体拒绝；后端构造失败会释放本次认领。
- `audit_path` 是显式例外：审计是追加日志，多实例共写不构成调度平面共享。
- 后端身份识别是进程内的；不同 OS 进程各自创建实例后共享同一文件/库属于 §1.5 的跨实例形态，不受此表约束。实例对象本身跨进程另由下面的 pickle/PID 检查拒绝。
- 同一 Redis/MySQL 端点以“URL”和“外部客户端对象”两种形式混用时，库无法反查对象指向，只按各自形式识别；此时需调用方保证二者不指向同一 DB。否则仍可能在对象层面漏报。
- `PURSER_ALLOW_SHARED_BACKENDS=1` 仅供“在单进程内模拟多主机”的验收/压测夹具使用；正常业务不得开启。
- 专项 [tests/test_isolation_guard.py](tests/test_isolation_guard.py) 14 项覆盖五类禁止、完全独立允许、同实例多池、删池释放认领、构造失败回滚、凭据去敏匹配、豁免开关及 SQLite 硬链接身份；`SyncPurser.close()` 与 `Purser.close()` 均释放认领。
- **实例本身不能跨进程。** `Purser` / `ResourcePool` / `SyncPurser` / `SyncPool` 的 `__getstate__` 直接抛 `IsolationConflict`，spawn、进程池或任何 pickle 传参在序列化阶段即失败；`fork` 继承的实例带创建时 PID，子进程首次调用 `start` / `create_pool` / `acquire` / `backup` / `close` 等入口即抛错，不会继续使用已停止的线程、已关闭的事件循环或复制来的连接。
- `fork` 子进程的 `atfork` 钩子会清空进程内后端身份登记表，因此子进程新建的 `Purser` 与父进程实例互不认领；这既符合“每个进程各自创建实例”，也让 `start_method="fork"` 的 `LocalScheduler` 不受影响。豁免开关只关闭后端身份判定，不关闭上述 PID 检查与 pickle 拒绝。
- 专项 [tests/test_isolation_process.py](tests/test_isolation_process.py) 覆盖四类对象的 pickle 拒绝、PID 不一致拒绝与子进程重建；POSIX `fork` 用例 3 项（继承实例拒绝、继承池拒绝、子进程自建成功）在 Windows 上跳过，由 PID 模拟用例补齐。

选择：需要跨线程共享一个资源集合时，共享一个 `Purser`/`SyncPurser`（§1.3）；需要独立集合时使用不同文件/库（或 §1.6 静态分片）；同机多进程使用 §1.5.2 的 IPC 调度。跨进程绝不能共享同一个实例对象：要么每个进程各自创建（§1.5、§1.6），要么由单一调度进程持有唯一实例（§1.5.1、§1.5.2）。

### 1.5 多进程：共享同一资源集合

下表的“共享”均指**各进程独立创建自己的 `Purser`**，再让两个调度平面指向同一后端；跨进程共享同一个 `Purser` 实例已被 §1.4.1 的 pickle/PID 检查禁止。

| 存储 | 协调 | 当前共享基础 | 当前完整全局语义 | 判断 |
| --- | --- | --- | --- | --- |
| memory | memory | 两平面均不共享 | 不成立 | 各进程是独立资源集合；同名池也不共享，无需检测 |
| memory | Redis | 仅租约共享，主数据和计数各自独立 | 不成立；同名池第二进程启动即拒绝 | 第二进程 `start()` 抛 `IsolationConflict`（§1.5.3）；手动复制资源也不会使占用计数原子共享 |
| SQLite | memory | 主数据共享、租约各自独立 | 不成立；同名池第二进程启动即拒绝 | 守卫阻止重叠实例（§1.5.3）；无守卫时新实例对账会误清旧实例占用 |
| SQLite | Redis | 两平面均有共享载体 | 核心租约语义成立；完整全局语义不成立 | 新增专项实测：容量/可见性/撤销/陈旧归还/启动对账/TTL 回收/双进程竞争/通用等待轮询唤醒通过；无推送通知（V03 已修复） |
| MySQL | memory | 主数据共享、租约各自独立 | 不成立；同名池第二进程启动即拒绝 | 守卫阻止重叠实例（§1.5.3）；换数据库不能补足共享租约与协调协议 |
| MySQL | Redis | 两平面均有共享载体 | 核心租约语义成立；完整全局语义不成立 | 可作为共享后端工程化候选；MySQL 8.4 + Redis 7.2.4 专项通过（含 V29 修复）；通用等待者已可通过轮询唤醒 |

§1.5 的三个不成立组合（memory + Redis、SQLite/MySQL + memory）现在由运行时守卫识别：同一后端命名空间下的同名池，第二个进程在 `start()` 时抛 `IsolationConflict`（§1.5.3）。守卫只针对跨进程半共享；SQLite/MySQL + Redis 的跨进程共享不受影响。同一 durable 存储上的协调端身份另由存储内的 `pool_bindings` 持久固定，串行切换协调类型或命名空间同样会在 `start()` 被拒绝；同一 Redis 命名空间被不同存储复用（含 memory 与 durable 的混合对）则由 Redis 实时标记在重叠运行期间拒绝（§1.5.4）。

“当前实现不是共享方案”限定于现有后端职责；并非断言未来不能增加数据库协调、外部调度服务等新架构。中心调度服务已在单副本形态落地并实测（§1.5.1）；同机多进程/进程池可使用本地 IPC 调度（§1.5.2），其数据面不按后端分支，已抽测 memory/memory、SQLite/Redis、MySQL/Redis 三种，其余三种为同一数据面的推断。数据库协调与其他协调后端仍未实现。

跨实例专项 [tests/test_sync_cross_instance.py](tests/test_sync_cross_instance.py) 以两个独立 `SyncPurser`（各自事件循环线程）和两个 `spawn` OS 进程实连 SQLite/MySQL + Redis 7.2.4，覆盖：全局容量与无超卖、跨实例租约可见、`force_revoke`、陈旧归还 no-op、启动对账保留活跃租约、定向获取轮询唤醒、通用等待者轮询唤醒、硬崩溃后 TTL 回收与重启对账、双进程 12 轮竞争。两个共享后端组合均为 7 项全通过、无 xfail；通用等待者的最坏唤醒延迟由 100 ms 轮询间隔决定，不是推送。异步内核的同 loop 双实例用例（`test_edge_cases` / `test_chaos_storm`）现已直连真实 Redis，但仍是同 loop 双实例，只能佐证真实网络协议层行为，不能当作多进程结论；多进程结论以本专项与静态分片专项为准。该验收**不代表**跨实例事件、幂等去重、维护接管、配额/报表作用域等全局能力已实现。

即使业务不需要全局事件、等待或去重，也仍需先解决容量、取消、过期、撤销等基础问题；不能以“只用简单 acquire/release”为由跳过正确性验收。

### 1.5.1 中心调度服务（E）：单副本让六种组合对客户端可用

现有 `purser serve` 与 REST API 已提供中心调度服务的骨架（[purser/ui/server.py](purser/ui/server.py)）：服务进程持有唯一的 `Purser`，客户端只能通过 HTTP 获取/归还租约、查询资源与事件；服务端可用 `--storage/--coordination` 选择六种组合之一。实测见 [tests/test_service_central_matrix.py](tests/test_service_central_matrix.py)：每个组合启动真实 `uvicorn` 进程，客户端仅经网络调用，覆盖组合回显、导入、获取、幂等重放、容量耗尽、跨客户端等待唤醒、事件历史与释放归零，六组合全部通过。

关键结论：**单副本时六种组合从客户端视角都成立**，包括 memory/memory——共享边界从“客户端进程”移到了“服务进程”。选择哪种组合不再决定客户端能否共享，而决定服务端的持久化与故障切换能力：

| 服务端组合 | 单副本可用 | 重启/切换后果 |
| --- | --- | --- |
| memory / memory | 是 | 重启即空；无持久化，不能做主备切换 |
| memory / Redis | 是 | 主数据仍在服务内存；备副本接管拿不到资源，租约共享也补不回来 |
| SQLite / memory | 是 | 资源持久化；但备副本内存协调看不到在租租约，接管会按 V02 误清占用 |
| SQLite / Redis | 是 | 可做共享后端的 active-passive；在飞等待者、本地去重与维护状态随活动进程丢失 |
| MySQL / memory | 是 | 同 SQLite / memory |
| MySQL / Redis | 是 | 可做共享后端的 active-passive；L4 仍留在活动进程 |

边界必须写清：

- **只验证了单副本。** 一旦运行第二个服务进程，就回到 §1.5 的共享后端语义：memory 组合不成立，SQLite/MySQL + Redis 仅核心租约语义成立；memory + Redis 与 SQLite/MySQL + memory 的第二个实例会在 `start()` 时被 §1.5.3 拒绝。HA/leader election、跨副本等待与去重不在本结论内。
- **“API 可用”不等于生产就绪。** 服务级 TLS、限流、鉴权轮换、客户端断线重试与对账、故障切换演练仍未实现或未验收；当前幂等存储随 `redis_url` 落在 Redis，纯 memory/sqlite 组合下随进程丢失。
- **客户端拿到的是网络租约。** 客户端崩溃后由服务端 TTL 回收，不依赖客户端心跳；但响应丢失后的重试必须带 `Idempotency-Key`，否则可能重复获取。

### 1.5.2 本地 IPC 调度：同机多进程与进程池

HTTP 只用于 UI/远程管理，不应成为同机 worker 的数据面。同机多进程的落地方案是 [purser/local](purser/local/scheduler.py)：`LocalScheduler` 启动一个独立调度进程，进程内持有唯一的 `SyncPurser`；worker 进程通过标准库 `multiprocessing.connection`（Windows 命名管道 / POSIX Unix domain socket）接入，消息为长度前缀 JSON（协议 v1），并带 32 字节随机 `authkey`。调度进程内不启动 HTTP 服务。

| worker 接入方式 | 用法 |
| --- | --- |
| `multiprocessing.Process` | 把 `sched.address`（可 pickle 的 `SchedulerAddress`）传给子进程，子进程调用 `init_worker(address)` |
| `multiprocessing.Pool` / `ProcessPoolExecutor` | `initializer=init_worker, initargs=(sched.address,)`；任务内用 `get_client()` |
| joblib / loky 等无 initializer 的库 | 在父进程 `with sched.env():`，子进程继承 `PURSER_LOCAL_SCHEDULER` 后调用 `get_client()` |
| 主进程自身 | 直接使用 `sched.client` / `sched.acquire(...)`，不要另建 `SyncPurser`，否则同一资源集合出现第二个权威 |

数据面语义与同步门面一致，并针对 IPC 做了闭合：

- 获取、容量、等待、续租、释放全部在调度进程内完成；数据面不按后端分支，已实测 memory/memory、SQLite + Redis、MySQL + Redis 三种组合，另三种组合未单独跑。
- 幂等获取：同一 `idempotency_key` 返回同一 token 并置 `replayed=True`；进行中的同 key 请求合流，不重复占槽。
- 幂等释放：token 墓碑（600 s）+ 服务端“token 已不存在即成功”的语义，与进程内 `release` 契约一致；`renew` 对失效 token 仍报冲突。
- 断连回收：租约按连接登记所有权；worker 进程崩溃或连接关闭后，调度进程立即归还其租约，不等待 TTL，也不会被调度进程心跳永久续租。
- 错误映射：服务端的结构化错误码在客户端重建为对应的 `PurserError` 子类（`PoolExhausted`、`PoolNotFound`、`LeaseConflict` 等）；调度进程不可达或不响应分别抛 `SchedulerUnavailable` / `SchedulerTimeout`。
- 连接是线程本地、进程本地的：客户端对象可以跨线程使用；`fork` 后通过 PID 检测惰性重连，连接对象从不跨进程 pickle。

验收见 [tests/test_local_scheduler.py](tests/test_local_scheduler.py)（11 项全通过）：协议字节往返、同进程数据面、阻塞获取被释放唤醒、`spawn` 单进程、`Pool` initializer 共享容量、`ProcessPoolExecutor`、环境变量发现、worker `os._exit` 后租约回收、调度进程被杀后的错误上报，以及真实 SQLite + Redis、MySQL + Redis 两种共享后端下 worker 获取/归还。全量（含本专项）为 1042 passed / 40 skipped / 1 xfailed。

边界与 §1.5.1 相同，另加三点：

- **一个资源集合仍只能有一个调度进程。** 启动第二个 `LocalScheduler` 或另行启动 `purser serve` 指向同一存储/协调时，就回到 §1.5 的共享后端语义，而不是自动获得全局一致性；若后端是 memory 半共享组合，第二个实例会在 `start()` 时被 §1.5.3 拒绝。
- **跨主机不适用。** 命名管道/UDS 只在本机；远程或多副本需要 HTTP/服务化，并另行解决鉴权、重试与对账。
- **消息是 JSON，不是 pickle。** 自定义 `ResourceType` 需在创建 `LocalScheduler(types=[...])` 时传入，且类在 `spawn` 子进程中可导入；资源 `payload` 中的 `bytes` 以 base64 哨兵传输，其他不可 JSON 化的对象会被拒绝。

### 1.5.3 跨进程 memory 组合的运行时拒绝

§1.5 的三个不成立组合不再只靠文档约定。两个 OS 进程各自创建 `Purser`、指向同一后端命名空间和同一池名时，后启动者在 `ResourcePool.start()` 阶段、任何租约写入或启动对账之前抛出 `IsolationConflict`（`ConfigError` 子类）。实现见 [purser/authority.py](purser/authority.py)：

| 组合 | 识别载体 | 冲突范围 | 硬崩溃后的释放 |
| --- | --- | --- | --- |
| memory + Redis | Redis 键 `purser:authority:{pool}:owner`，`SET NX PX` 认领，20 s 心跳续 60 s TTL | 同一 Redis DB + 同一池名 | 心跳停止后 TTL 过期 |
| SQLite + memory | SQLite 文件所在目录的 `.purser-authority-<dev>-<ino>-<hash>.lock` 旁路文件（按文件 inode 命名；文件尚未创建时退回路径哈希命名），`fcntl.lockf` / `msvcrt.locking` 非阻塞独占 | 同一 SQLite 文件（同一 inode）+ 同一池名；同目录硬链接别名同样命中 | OS 关闭 fd 即释放（`os._exit` 实测） |
| MySQL + memory | 存储连接上的 `GET_LOCK('purser-auth-<sha1(db,pool)>')` | 同一 MySQL 服务器 + 同一库 + 同一池名 | 会话结束时由服务器释放 |
| memory + memory | 无共享平面 | 不检测；各进程本就是独立资源集合 | — |

语义细节：

- 只对“同一池名 + 同一后端命名空间”判冲突；不同池名是不同资源集合，允许并存。
- 混合配置由 §1.5.4 的反向标记补上：同一池若一边用 SQLite/MySQL + Redis、另一边用 memory + Redis，先启动者的实时标记记录其存储身份，后启动者在 `start()` 被拒绝；两个不同 SQLite 文件共用同一 Redis 命名空间同理。durable 对 durable 的协调端漂移（同一 SQLite/MySQL 存储改用 memory 或另一个 Redis 命名空间）由 §1.5.4 的持久钉拒绝。
- 守卫只在这三种 memory 半共享组合上生效；SQLite/MySQL + Redis（§1.5 支持的两平面共享组合）不做认领，指向**同一存储**的第二、第三个进程仍可正常启动（不同存储的同名池由 §1.5.4 反向标记拒绝）。
- 认领发生在 `ResourcePool.start()`、`_reconcile_leases()` 之前；`ResourcePool.close()` 在关闭存储/协调之前释放。启动中途失败会回滚认领。
- 硬崩溃由载体自行回收：SQLite/MySQL 立即释放（OS/服务器），Redis 在 TTL 后释放。Redis 心跳丢失且无法重领时记录 error 日志，下一次 `start()` 才重新判定，不会在操作路径上中途报错。
- 同一进程内若先前 `close()` 未能删掉 Redis 标记，且标记的主机/PID 与当前进程一致，视为陈旧标记并接管；正常的同进程半共享仍由 §1.4.1 的守卫拒绝。
- `PURSER_ALLOW_SHARED_BACKENDS=1` 跳过本守卫（夹具模拟多主机用），不会放宽 §1.4.1 的 PID/pickle 检查。
- SQLite 旁路锁文件在释放后保留，避免“解锁后删除文件”与另一进程打开同名文件的竞态。
- 旁路文件按 `(st_dev, st_ino)` 命名并放在数据库所在目录：同目录下的硬链接别名落到同一个锁文件，不能靠换路径各拿一把锁绕过守卫。跨目录硬链接因旁路文件本就在各自目录，仍不共用锁；跨主机网络文件系统的 `st_dev/st_ino` 也不保证一致——这两种情况仍是已知缺口。反向标记的存储身份同样取 inode，跨目录硬链接别名在 §1.5.4 会被拒绝；§1.4.1 的同进程身份仍按规范化路径比较，跨目录硬链接别名不在其覆盖内。
- 专项 [tests/test_cross_process_memory_guard.py](tests/test_cross_process_memory_guard.py) 共 9 项：真实 Redis 下同名池第二进程拒绝、关闭后允许、不同池名允许；SQLite 同名池拒绝/关闭后允许/不同池名允许/硬崩溃自动释放/豁免开关/同目录硬链接别名拒绝与关闭后允许；memory + memory 同名池互不影响；真实 MySQL 同名池拒绝与关闭后允许。无真实服务时后 3 项跳过。全量（含本专项）为 1042 passed / 40 skipped / 1 xfailed。

### 1.5.4 存储端与调度端的双向绑定

§1.5.3 只解决“重叠运行”的 memory 半共享，挡不住配置漂移：第一个实例以 SQLite/MySQL + Redis 启动并关闭后，第二个实例改用同一存储 + memory（或另一个 Redis 命名空间）仍会被放行，启动对账会按自己为空的租约视图清零前者的占用；反方向上，两个不同存储（或 memory 与 durable）复用同一个 Redis 池命名空间，也会互相清空对方租约。实现见 [purser/binding.py](purser/binding.py)，由正向持久钉与反向实时标记共同覆盖。

**正向：存储 → 协调（持久钉）**

- 每个池首次 `start()` 时，把协调端身份写入**存储库内**的 `pool_bindings(pool, storage_kind, coordination_kind, coordination_id, claim_token, updated_at)` 行。身份取值为 `memory` 字面量，或 Redis 规范化 `host:port/db#key_prefix`（unix socket 为 `unix:<path>/<db>#<prefix>`）。fakeredis 仅用于测试，按 `redis:fake/<db>#<prefix>` 处理，因为其主机名每个实例随机。
- 后续 `start()` 先读该行：协调类型或命名空间不一致即抛 `IsolationConflict`，发生在任何租约写入与启动对账之前。键为池名；同库不同池名是不同资源集合，互不绑定。
- 绑定是**部署不变量，不随 `close()` 释放**；显式改绑通过 `delete_pool` 删除该池的绑定行（池内仍有资源时需 `force=True`）。启动在写入新绑定后失败时，用 `claim_token` 只回滚本实例刚插入的行，不误删既有绑定。
- 只覆盖 durable 存储；memory 存储没有共享平面可写。

**反向：协调 → 存储（实时标记）**

- 每个 Redis 协调池在 `start()` 时额外写 `{key_prefix}:binding:{pool}:storage` 键，值为 JSON `{storage_kind, storage_id, token, pid, host, started_at}`。Lua 原子认领：键为空或 `storage_id` 相同则写入并 `PX` 续 60 s TTL，否则抛 `IsolationConflict`；20 s 心跳续期，心跳丢失后尝试重领，确认被他人占用则记录 error。
- 存储身份：memory → `memory`；SQLite → 优先 `sqlite:d<st_dev>-i<st_ino>`（同一文件的硬链接别名报告同一身份），文件尚不存在时退回 `sqlite:<规范化绝对路径>`（`:memory:` 或 `mode=memory` → `memory`）；MySQL → `mysql:<@@server_uuid>/<DATABASE()>`，MariaDB 无 `@@server_uuid` 时退到 `@@hostname`，查询失败再用配置 URL 推导。无法确定身份时跳过反向标记并记录 warning。inode 身份适合同主机共享，跨主机网络文件系统的 `st_dev/st_ino` 可能不一致，属已知限制。
- 覆盖“同一 Redis 命名空间 + 同一池名 + 不同存储”的重叠运行：两个不同 SQLite 文件、SQLite/MySQL + Redis 与 memory + Redis 的混合对（两个方向），以及 `sqlite :memory:` + Redis 与 durable 存储的同名池。
- 与持久钉相反，标记随 `close()` 释放：Lua 先校验 token 再删除，非持有者不会误删；硬崩溃由 60 s TTL 回收。释放后不同存储可立即复用该池名。
- 同一存储身份的重启/重连允许重领（替换记录并接管心跳）；两个进程指向同一存储 + 同一命名空间是本项目支持的两平面共享组合，不受影响。
- memory 与 `sqlite :memory:` 都报告 `memory` 身份，二者彼此重叠仍由 §1.5.3 的 `RedisAuthority` 拒绝；为此 `sqlite :memory:` + Redis 已并入该守卫（与 memory + Redis 同处理）。
- 代价：SQLite/MySQL + Redis 的受支持组合在 `start()` 时新增一次 Redis 写（与既有租约注册同等级）；Redis 不可用时以 `BackendUnavailable` 失败，而不是带着未绑定的状态启动。
- `PURSER_ALLOW_SHARED_BACKENDS=1` 同时跳过正向与反向认领，仅供模拟多主机的夹具使用。
- 专项 [tests/test_pool_binding.py](tests/test_pool_binding.py) 共 15 项：正向覆盖 SQLite 池 memory→Redis 与 Redis→memory 的跨进程拒绝及同配置重启放行、MySQL 池 Redis→memory 拒绝、Redis `key_prefix` 命名空间不一致拒绝、同库不同池互不绑定、`delete_pool` 后改绑、启动失败回滚；反向覆盖真实 Redis 下不同 SQLite 文件同名池拒绝（同文件放行、关闭后另一文件放行）、durable 与 memory 双向拒绝及关闭后放行、`sqlite :memory:` + Redis 第二进程拒绝，以及 fakeredis 下的认领/拒绝/同存储重领/token 隔离/豁免开关/`close()` 即删标记/同文件硬链接别名共享标记。无真实服务时跨进程项跳过。全量（含本专项）为 1042 passed / 40 skipped / 1 xfailed。

### 1.6 多进程：各自独立后端 / 静态分片

| 存储 | 协调 | 分片内可行性 | 当前完整性 | 关键条件 |
| --- | --- | --- | --- | --- |
| memory | memory | 可设计 | 分片内继承单实例；双进程独立分片实测通过 | 每分片独立资源；重启需重建（实测新进程为空池） |
| memory | Redis | 可设计 | 分片内继承单实例；独立池名双进程实测通过 | 租约命名空间按池名隔离；同名池第二进程启动即被拒绝（§1.5.3），不再进入可见/撤销阶段 |
| SQLite | memory | 可设计；优先考虑 | 分片内继承单实例；独立文件双进程实测通过 | 同一分片新旧实例不能重叠运行；底层重复占槽缺陷仍有 xfail 复现（用豁免开关），生产路径由 §1.5.3 拒绝 |
| SQLite | Redis | 可设计 | 分片内继承单实例；独立文件 + 独立池名双进程实测通过 | 独立存储不等于独立协调：同名池在重叠运行期间由 §1.5.4 反向标记拒绝（豁免开关下仍可复现启动对账污染）；幂等仍整库共享（V20） |
| MySQL | memory | 可设计 | 分片内继承单实例；同库不同池名双进程实测通过 | 逻辑分片依赖池名隔离；同库同名池重叠启动由 §1.5.3 拒绝；仍共享 MySQL 实例与容量 |
| MySQL | Redis | 可设计 | 分片内继承单实例；同库不同池名 + Redis 双进程实测通过 | 存储、租约、幂等、审计及控制面分别定义隔离范围 |

静态分片专项 [tests/test_sync_static_shards.py](tests/test_sync_static_shards.py) 以两个 `spawn` OS 进程、每进程一个 `SyncPurser` 和独立分片实连服务：memory/memory、SQLite/memory、SQLite/Redis、MySQL/memory、MySQL/Redis 五形态均验证同一资源 id（`r0`、`r1`）在两个分片同时各持 1 槽、跨分片资源与租约不可见、释放后归零；SQLite/MySQL 分片重启后资源仍在且占用为 0，memory 分片重启后为空。MySQL 形态为同库不同池名的逻辑分片，SQLite 形态为独立文件。

两条部署边界已固化为测试：同一 SQLite 分片上 memory 协调的两个实例重叠时，后启动实例的启动对账会把前者的活跃占用清零，从而可重复占槽（xfail，对应 V02/P0-2）；该 xfail 用例通过豁免开关保留，用于复现底层缺陷，生产路径已被 §1.5.3 拒绝。私有存储分片共用 Redis 池名时，后启动分片的启动对账会把另一分片的租约计为自身占用（该专项同样通过豁免开关断言当前行为；不使用开关时，§1.5.4 的反向标记会拒绝后启动者。生产必须使用不同池名或不同 Redis DB）；memory 存储 + Redis 协调的同名池现由 §1.5.3 在启动时直接拒绝。

本验收只覆盖“分片内继承单实例 + 分片间不共享”，不代表跨分片配额、公平、事件、去重或维护接管；分片内多线程/跨进程竞争分别由单实例与跨实例专项覆盖。六行的跨分片共享语义均为**不提供**。分片可以减少跨实例竞态，但不能替代单实例修复与验收，不能直接得出“同步门面完成即可上线”。具体部署前提见第六章。

“完全独立”在本节按严格定义使用：两个平面都不共享，而不只是存储路径不同。按此定义：

- 每进程 `memory` / `memory`，或“每进程独立 SQLite 文件 + `memory` 协调”，是最彻底的两平面私有形态；后者仍受单机文件锁与运维边界限制。
- 协调端选 Redis 时，同一 Redis DB 即使池名不同也不是完全独立：租约域按池名隔离，但幂等键在整库范围共享（V20），同名池则直接是同一租约域。要完全独立需使用不同 Redis DB，或退回每进程 `memory` 协调。
- MySQL 同库不同池名只是逻辑分片，不是完全独立：仍共享实例、连接与容量边界。
- 表中未单列 `memory` / Redis 的独立形态；按构造它成立（主数据私有、租约按池名隔离），但没有像其余五形态那样做双进程实测。
- 每进程都必须创建自己的 `Purser`；同一个实例跨进程传递已被 §1.4.1 拒绝。需要共享而不是独立时，走 §1.5 的共享后端或 §1.5.2 的单一调度进程。

## 二、架构事实与正确性边界

### 2.1 两平面已共享，不等于一次操作已原子化

| 平面 | 当前职责 | 实现 |
| --- | --- | --- |
| 存储 | 资源主数据、状态、槽位计数、版本控制 | memory / SQLite / MySQL |
| 协调 | 租约注册、查询、续租、删除及相关索引 | memory / Redis |
| 实例内运行时 | 等待队列、心跳任务、去重辅助状态、亲和性、事件、用量、验活任务、调度器 | Python 对象和 asyncio 任务 |

Redis 协调层当前**没有跨实例推送通知**；同步门面的定向与通用等待者改为每 100 ms 轮询共享存储，另一实例释放后可在约 100 ms 内唤醒（V03 修复）。异步内核的通用等待者仍由实例本地 `_serve_waiters` 服务，不能套用同步门面的结论；把“轮询”写成“协调层已具备推送能力”同样不成立。

典型操作顺序：

```text
acquire：存储占用增加 → 状态通知 → 协调注册 → 本地心跳与跟踪
release：协调删除 → 本地清理 → 存储占用减少 → 用量和事件
reconcile：扫描存储 → 读取协调租约 → 按总槽位重置存储
```

证据：[获取提交](purser/facade.py#L1624)、[归还](purser/facade.py#L1736)、[启动对账](purser/facade.py#L486)。

SQLite 的 `reset_usage` **已经有内部版本 CAS**，MySQL 也有事务与版本检查：[SQLite](purser/backends/sqlite.py#L458)、[MySQL](purser/backends/mysql.py#L498)。问题不是“重置完全没有 CAS”，而是 CAS 无法证明其 `target` 来自一个跨两平面一致的租约快照。

### 2.2 应验收的不变量

对每个资源，在获取、归还、过期回收和补偿已经完成，或处于协议定义的一致性检查点时，应满足：

```text
0 <= used_slots <= max_slots
used_slots == sum(slots of authoritative live leases)
每个已提交 token 最多归还一次对应占用
一次获取没有成功交付给调用者时，最终不留下无主占用或持续心跳
```

使用 `sum(lease.slots)` 而不是 `len(leases)`；一个租约可能占多个槽位。“权威 live lease”不能简单等同于 Redis `active()` 当前返回的所有记录，因为后者尚不逐 token 过滤过期项。

还应在故障和恢复过程中验证：有效持有者不会因误减计数而与新持有者共同突破容量；失效持有者在有界时间内得到通知。对于外部副作用，即使租约记录过期，也不能物理阻止旧进程继续使用外部资源；若要求强排他，需业务合作或外部系统校验 fencing token，不能只依靠 TTL。

当前两平面操作存在合法中间态，因此任意时刻先后读取两个后端所得的数值不一定可直接比较。测试需使用屏障、一致性快照或操作完成后的稳定检查点；不能把非原子采样中的瞬时差异一概判成超卖，也不能因此忽略稳定状态下的反例。

### 2.3 loop 所有权

已创建的 Task、Future、正在使用的异步连接和被绑定的同步原语具有 loop 亲和性。后台心跳、事件分发与维护任务必须由所属 loop 管理：[租约心跳](purser/core/lease.py#L38)、[事件分发](purser/core/events.py#L123)。

本轮 Python 3.11.9 实测：主线程没有 running loop 时构造 `Purser` 和未启动的池可以成功；随后在另一线程统一启动、关闭也能成功。故不能写成“只要主线程构造就必报错”。但把**已启动**实例交给另一 loop，或在没有 running loop 的线程为运行中实例建池，会遇到任务创建或跨 loop 关闭错误。

Python 3.8/3.9 与新版本的锁、信号量绑定时机不同；本轮没有完成所有受支持版本的实机验证。统一在所属 loop 线程内构造、使用和关闭仍是合理的门面约束，它降低兼容风险，而不是依靠一个不成立的绝对报错论证。

### 2.4 生命周期及实例内状态

当前获取租约要求已启动；导入、查询等部分管理操作允许启动前使用。运行中的 `Purser.create_pool()` 会安排异步启动任务，而非同步等待全部初始化完成：[启动检查](purser/facade.py#L407)、[建池](purser/facade.py#L3009)。

以下作用域目前主要是实例内，而非全局：

| 状态 | 影响 |
| --- | --- |
| `_waiters`、`_consumer_holds`、`_affinity` | 通知、去重辅助信息和亲和性不自动跨实例 |
| `_check_cache`、`probe_guard` | 校验缓存及熔断范围按实例；需要明确是否接受此作用域 |
| `_validate_jobs`、维护调度器 | 任务查询、取消与维护执行没有统一跨实例协调 |
| `bus`、`usage`、`metrics` | 事件和用量、指标需额外汇总；单实例结果不能冒充全局结果 |
| `_pools`、类型注册表 | 其他实例不会自动获得新建池、类型插件或配置变更 |
| 审计内存视图 | 可以共享磁盘审计库，但当前写者内存视图存在并发缺漏问题 |

## 三、同步门面：可行，但不能只做方法转发

### 3.1 推荐结构与职责

```text
调用线程 / 线程池
    │ 同步提交，等待结果
    ▼
SyncPurser / SyncPool
    │ run_coroutine_threadsafe：异步方法
    │ call_soon_threadsafe + concurrent.futures.Future：普通方法
    ▼
专用 loop 线程：Purser、池、连接、任务、内部可变状态
```

一个门面持有一个 loop 线程；不要每次调用新建 loop，也不要继承 `Purser` 后混合暴露异步方法。门面负责调用桥接、数据所有权和生命周期，不另写一套调度器、缓存或业务重试。

当前实现位于 [purser/sync.py](purser/sync.py)，导出 `SyncPurser`、`SyncPool`、`SyncLease`、`SyncBatchLease`、`SyncResourceIterator` 和 `SyncSubscription`。异步方法经 `run_coroutine_threadsafe`，普通方法经 `call_soon_threadsafe`；输入在提交前深拷贝，普通输出在所属 loop 内构造快照，租约使用实时代理。`audit`、`metrics`、`idempotency`、`probe_guard` 和 `usage` 通过同步代理暴露；`bus` 不直接暴露，事件由 `subscribe()` / `history()` 提供；`types` 按注册契约返回真实插件实例。基础测试见 [tests/test_sync.py](tests/test_sync.py)，单实例组合验收见 [tests/test_sync_acceptance.py](tests/test_sync_acceptance.py)。

下面的约束是实现与验收标准，而不是“已全部通过”的声明；内核层面的 P0 结论仍以第五章为准。

### 3.2 输入与输出所有权

| 对象 | 推荐契约 | 当前风险 / 注意事项 |
| --- | --- | --- |
| 请求、配置、导入记录、嵌套 dict/list | 明确支持的数据类型；在交付所有权前完成隔离，调用过程中不得由调用方并发修改输入 | 实测调用方修改 `PoolConfig.meta` 嵌套列表会影响内部配置 |
| Resource、报告、统计、事件历史等普通数据 | 在所属 loop 内构造独立快照，再交给调用线程 | 先返回内部对象、再在调用线程 `deepcopy` 仍有竞态 |
| 租约 | 实时字段采用受控代理；只读数据可为快照 | 整体复制会切断心跳更新和 `used` 回写，见 3.3 |
| 插件实例、连接、锁、文件句柄 | 不盲目深拷贝；规定所属 loop、工厂创建或独占使用契约 | 外部已绑定连接不能简单搬入新 loop；插件本身也可能有可变状态 |
| 普通同步属性与方法 | 经所属 loop 读取 / 调用，返回快照或门面 | 普通 `def` 并不代表可从调用线程直接访问 |

`memory.get()` 返回内部对象，扫描使用浅拷贝：[get](purser/backends/memory.py#L80)、[scan](purser/backends/memory.py#L282)。实测修改返回资源的 `meta` 或扫描结果的嵌套 `concurrency` 能改变存储。门面必须阻断这些跨线程别名，但不应把“保留这种偶然可变别名”列作功能等价目标。

拷贝失败也必须定义清楚。当前 `import_resources(Resource)` 有深拷贝，但 dict 经插件 `generate()` 生成的对象不受同一入口约束：[导入路径](purser/facade.py#L968)。因此不能说“不可复制对象在导入时一定已经被拒绝”。

### 3.3 Lease 不能简单整体 deepcopy

`Lease` 是可变对象，包含 `used`、`outcome`、`expires_at`、`heartbeat_at`、`slots` 等字段：[定义](purser/core/resource.py#L183)。获取提交已经启动心跳，**原草案不是缺少心跳**；问题是调用方副本与心跳使用的原对象失联。

实测原草案，`lease_ttl=0.15`、`heartbeat_interval=0.03`、`use_limit=1`，调用方设置 `lease.used=1` 后等待：

```text
copy_expired=True, core_expired=False
copy_used=1, core_used=0
```

内核按原对象的 `used` 决定能否续租，续租后也只更新原对象期限：[续租](purser/facade.py#L1695)。因此“字段都复制出来”不等价于“使用行为不变”。

建议全量门面使用受控的 `SyncLease`（已实现）：

- `used`、`outcome` 的赋值通过所属 loop 更新权威对象；期限和心跳字段读取反映当前状态，`expired()` 不能使用永久滞后的副本。
- token 作为受控租约标识；资源数据按快照契约读取，禁止调用方随意改变归还所依据的资源 ID、槽位数等字段。
- `release` 的资源、槽位和所有权必须来自权威租约记录，而不是信任调用方副本。
- 同步 `lease()` 上下文退出时保留当前异步实现的用量反馈语义。当前异步上下文也不会自动把任意业务异常分类成失败，不应把此类新功能混入适配。
- 批量租约同样需要管理代理、部分失败补偿和统一归还，不能只复制 `BatchLease` 后暴露底层异步方法。

若选择“不可变快照 + 显式反馈/刷新方法”，也可以形成清晰 API，但这是另一种契约，必须明示差异，不能宣称与当前可变 `Lease` 完全等价。

**交付失败补偿是必需项。** 探针让插件生成带 `threading.Lock` 的资源元数据：导入成功，随后同步 `acquire` 的 deepcopy 抛 `TypeError`，内核却仍有 `active=1 / used=1 / heartbeat=1`。获取成功到返回代理/快照之间的失败，必须归还已获得的租约或进入可观测、可重试的补偿流程；仅向调用方抛异常不够。

### 3.4 启动、关闭与取消契约

推荐保留显式 `start()`：构造门面可以建立 loop 和内核对象，但不自动改变“启动前可做管理操作”的原生命周期；`with SyncPurser(...)` 再负责启动与退出关闭。若另选自动启动构造，须列为有意的行为变化。

| 场景 | 必须满足的条件 |
| --- | --- |
| 初始化失败 | 关闭已创建的池、审计库和自有客户端，再停止 loop；不能只停线程 |
| 启动失败 | 回滚启动状态或保持可查询的失败状态；失败不能被 `started=True` 掩盖 |
| 运行中建池 | 等待真实启动任务的成功或失败；已结束且失败的任务也必须检查；失败注册需回滚 |
| 提交与关闭并发 | 状态检查和提交有一致协议；关闭后拒绝调用，包括包装缓存命中路径 |
| 删除池失败 | 不能无条件清除仍有效的包装缓存；删除成功才失效 |
| 业务超时 | 保留 `PoolExhausted`、`ResourceBusy` 等内核语义，不误报为门面等待超时 |
| 外层等待超时 | 取消只是请求，不是“保证没有提交”；须处理完成与取消竞态及交付失败补偿 |
| 并发关闭 | 新调用被拒绝；在途调用最终成功、失败或明确取消，不能留下永远无人完成的 Future |
| 部分关闭失败 | 继续尝试关闭其余对象，汇总错误；定义注入客户端与自建客户端的关闭责任 |
| 关闭期限耗尽 | 检查线程实际是否退出；不能把 `join(timeout)` 返回当作已关闭 |
| loop 线程内调用同步门面 | 在修改关闭状态、创建待提交协程等副作用之前拒绝，避免自等待和半关闭 |

当前 `_ensure_started()` 只等待尚未结束的 `_start_task`，无法据此保证已失败任务会被重新抛出；`_started` 和 `_running` 又先于成功完成被置真：[池生命周期](purser/facade.py#L383)、[Purser 生命周期](purser/facade.py#L3424)。实测失败任务已经带有 `ValueError` 时，原草案仍能成功返回包装对象。

原草案还有启动失败未调用内核 close、loop 内误调 close 先污染标志、executor 未结束时 close 返回但线程仍存活等问题。另一方面，当前池关闭会主动令普通 waiter 收到 `PoolExhausted`：[关闭](purser/facade.py#L424)。所以也不能反向夸大为“所有在途调用遇到 close 都必然永久阻塞”。

正常关闭应先在仍运行的 loop 上排空 / 取消业务任务并关闭资源，再完成异步生成器和 executor 清理、停止并关闭 loop，调用线程最终确认线程退出。具体超时预算、强制退出不可实现的边界及异常映射应写入 API 契约。仅把外层超时换成 `asyncio.wait_for` 不能修复内核的双回滚或遗留占用。

### 3.5 完整公开 API 面

按当前 `facade.py` 的 AST 去除 overload、property、dunder，按 `owner.method` 去重计数：**ResourcePool 39 个、Purser 12 个，合计 51 个**。这是所属类与方法的组合数，不是不同方法名数量；原表并非漏掉十余个方法，而是数量和覆盖口径不准确。

| 所属对象 | 分组 | 必须纳入适配清单的方法 |
| --- | --- | --- |
| Purser | 生命周期 | `start`、`close` |
| Purser | 类型 / 池 | `register_type`、`create_pool`、`pool`、`get_pool`、`require_pool`、`pool_names`、`delete_pool` |
| Purser | 运维 | `report_usage`、`backup`、`restore` |
| ResourcePool | 生命周期 | `start`、`close`、`reconfigure` |
| ResourcePool | 读取 | `get`、`require`、`query`、`iter_resources`、`count_matching`、`batches`、`counts`、`stats`、`describe`、`leases` |
| ResourcePool | 租约 | `acquire`、`lease`、`release`、`release_consumer`、`acquire_many`、`renew_manual` |
| ResourcePool | 导入 / 验活 | `import_resources`、`generate`、`validate_now`、`validate_jobs`、`validate_job`、`cancel_validate_job` |
| ResourcePool | 状态 / 资源管理 | `ban`、`unban`、`suspend`、`resume`、`top_up`、`drain`、`cancel_drain`、`restore`、`admin_set_state`、`force_revoke`、`update_resource`、`move_resource`、`recall_batch` |
| ResourcePool | 维护 | `run_maintenance` |

方法之外，还包括 `started` 等属性、上下文管理器、批量归还接口，以及 `bus`、`audit`、`metrics`、`idempotency`、`probe_guard`、`types`、`usage` 等公开对象。需要为其定义同步代理、只读快照或明确的不支持范围，不能直接暴露原对象。只要排除其中部分公开能力，就应据实标注覆盖范围，而不是继续使用全量等价标签。

当前实现的 51 个 `owner.method` 均已纳入同步门面。`audit`、`metrics`、`idempotency`、`probe_guard`、`usage` 以同步代理暴露，`types` 返回真实插件实例；`bus` 不直接暴露，改由 `subscribe()`、`history()` 和 `SyncSubscription` 提供同步事件接口。包装缓存按池名复用，删除成功后才失效；`close()` 拒绝来自事件回调线程的关闭调用。

原接口包含普通 `def`、协程、异步生成器和异步上下文管理器；应根据实际调用形式适配，不能全部传给 `run_coroutine_threadsafe`。完整签名以源码为准：[Purser 池访问方法](purser/facade.py#L3009)、[ResourcePool 查询](purser/facade.py#L772)。

### 3.6 迭代、事件与插件

**迭代：** `iter_resources` 应保留底层迭代行为，而不是一次 `list()` 消耗全部资源。可选择逐次提交迭代器的 `__anext__()`，或后台 pump + 有界交接缓冲区。后者必须保证满队列不会通过阻塞式 `queue.put()` 卡住所属 loop，并处理取消、背压、异常传递及关闭。同步迭代器要提供显式关闭方式；不能假设调用方 `break` 后生成器一定立刻关闭。`query(limit, offset)` 在并发写入下可能漏项或重复，不是等价替代，也不应额外承诺原迭代器不具备的事务快照。当前实现采用逐次提交 `__anext__()`，`SyncResourceIterator` 同时支持 `close()` 与上下文管理。

**事件：** 当前 `EventBus` 接受普通同步 listener，也会等待返回的协程；同步 listener 直接在 dispatch 所属 loop 执行，阻塞函数会卡住该 loop：[实现](purser/core/events.py#L149)。门面可把同步回调移到 executor，但应定义顺序、超时、丢弃策略、取消、取消订阅与重入规则。当前实现把回调交给固定大小 executor，事件快照在 loop 内构造，沿用总线的监听器超时与失败降级；`SyncSubscription.close()` 可重复调用，回调线程内的 `close()` 会在产生副作用前被拒绝。

“loop await executor，executor 再调用门面”**不必然死锁**：await 会让出 loop，本轮 `listener → executor → SyncPool.stats()` 正常完成。真正危险的是 loop 线程直接调用阻塞门面，或形成闭合依赖环，例如回调等待某操作，而该操作又必须等待当前回调返回。实现可保守禁止某些回调重入，但要区分产品约束与普遍机制。

**插件：** 不仅 `generate`、`validate`、`on_request_check`，还包括 `on_release`、`on_state_changed`、`on_error` 等钩子。异步钩子留在所属 loop；阻塞 I/O 应显式经 executor / `to_thread` 处理，不得在工作线程直接触碰 loop 内部状态。同步插件适配器是额外 API 设计，不是全量转发自然得到的能力。

### 3.7 替代方案与事实边界

| 方案 | 实际机制 | 对 purser 的判断 |
| --- | --- | --- |
| 显式门面 + 长期后台 loop | 一个所属 loop 执行异步内核，同步调用跨线程等待 | 推荐；便于表达生命周期与对象所有权，但仍需完整适配 |
| 每次调用 `asyncio.run()` | 每次创建并关闭一个 loop | 不适合复用带长期任务和连接的实例；一次性完整异步程序仍可这样运行 |
| 自适应调用，如 universalasync | 依据 running-loop 上下文返回值或 coroutine | 并非语法上不可能；但同步 helper、类型推断和多 loop 对象所有权更难约束 |
| synchronicity | 在独立线程创建 loop，提供同步包装及 `.aio` 入口 | 不是 greenlet 方案；可评估自动包装，但对象身份、类型、生命周期和依赖兼容仍需验证 |
| Playwright Python sync API | 用 greenlet 协作切换，并限制在已有 running loop 中使用同步入口 | 可借鉴 sync/async 入口隔离，不能把内部机制写成后台线程 |
| unasync | 从异步源代码在构建期生成同步代码 | 不是手工维护两套源码；但转换规则、生成验证及两种运行时语义仍有成本 |
| 另写同步内核 | 独立维护状态机和后端行为 | 不推荐；无必要扩大正确性验证面 |
| 同类增加 `_sync` 方法 | 显式平行方法入口 | 可行，但本项目更适合用独立门面类型表达 loop 所有权 |

本轮核对 synchronicity 主分支声明 Python >= 3.10，而 purser 声明 >= 3.8：[项目要求](pyproject.toml#L10)。若考虑引入，需要按实际选择的发布版本重新核对，不能用库名推断兼容性。Python 3.8 没有 `asyncio.to_thread` 与 `loop.shutdown_default_executor`，应设计相应执行器所有权及关闭路径；能力检测跳过某 API 不等于清理已经完成。

核对来源：[synchronicity](https://github.com/modal-labs/synchronicity)、[Playwright 同步上下文](https://github.com/microsoft/playwright-python/blob/main/playwright/sync_api/_context_manager.py)、[universalasync](https://github.com/bitcart/universalasync)、[unasync](https://github.com/python-trio/unasync)。这些对比用于说明机制，不作为 purser 正确性的证明。

## 四、多线程的适用范围

| 编号 | 使用方式 | 结论 |
| --- | --- | --- |
| T1 | 多线程共享一个 SyncPurser | 已实现并通过基础并发测试；契约逐项验收仍需继续 |
| T2 | 多线程各自 `asyncio.run()`，共用已启动 Purser | 不支持；跨 loop 任务和连接所有权不成立 |
| T3 | 每线程自己的实例和 loop | 属于多实例。共享同一资源集合见 §1.4/§1.5；独立后端按 §1.6 静态分片处理：分片内继承单实例，跨分片不提供共享调度 |
| T4 | 异步应用把阻塞插件逻辑移入工作线程 | 可行；工作线程不直接操作内部对象，不能推断为 CPU 调度线性加速 |
| T5 | 同步代码直接调用异步 `pool.acquire()` | 返回 coroutine，不会自动完成获取 |

正常路径漏斗测试中，同步门面完成了 6 线程 × 40 次获取归还，共 240 次成功、最终占用为 0、正常关闭线程退出。这只证明桥接方向可运行，不等同于完整多线程验收。

另一个探针中，两个钩子各等待约 120ms，总耗时约 125ms，钩子进入峰值为 2，直接说明单 loop 内请求可交错。对账、取消、读取后提交等竞态不会因为调用都进入同一 loop 就消失。

单线程 Python 执行、同步审计写、CPU 密集钩子可能成为瓶颈；I/O 等待则能重叠。性能必须按资源数、连接池、后端延迟、钩子耗时及审计频率测试，不能写成“所有 API 串行，吞吐只等于一条请求”。

## 五、正确性风险与共享后端能力缺口

### 5.1 优先级说明

沿用原 P0-1～P0-5 编号便于追踪，但修正其根因及范围，并补入遗漏的基础问题。这里 P0 表示测试门禁失真或足以破坏租约核心保证的阻断项，不代表每一种部署都会触发全部问题。

#### P0-1：现有测试会空跑，采样器还可能完全失效

- [多进程压力测试 worker](tests/test_multiprocess_stress.py#L33) 没有 `start()`；`NotStarted` 被 `PurserError` 分支吞掉。实测三个 worker 的成功次数为 `[0, 0, 0]`，测试仍通过。
- 同文件在主线程创建 SQLite 连接、子线程采样使用，并吞掉采样异常：[采样器](tests/test_multiprocess_stress.py#L98)。实测有效采样 0 次，`ProgrammingError` 被吞 96 次。
- [多实例 chaos 用例](tests/test_chaos_storm.py#L194) 同样未启动实例；实测 56 次获取尝试、0 次成功，均被允许异常集合吞掉。
- [真实 Redis 压测采样器](tests/test_redis_real.py#L400) 存在同类跨线程连接使用问题；它已经有 `sum(ok) > 0`，但未发生有效采样时 `max_seen <= 2` 不能证明峰值。
- 原多进程测试**已有**最终 `used_slots == 0` 断言；该断言在零成功时也能通过，问题不是单纯缺少收尾断言。

整改要求：正确启动；在采样线程内创建并关闭连接；显式记录成功次数、有效样本数和异常；不吞 `NotStarted` 等契约违例；同时使用确定性交错测试和真实服务测试。真实 Redis fixture 会清空其测试 DB，必须只连接明确专用环境。本轮没有执行该真实服务测试。

#### P0-2：memory 协调看不到别的实例，启动对账会误清占用

两个实例共享 SQLite，A 持有唯一槽位，B 启动时只能看到自己的空租约表，于是把计数改为 0；B 随后获取成功，两个租约占据一个槽位。该问题在同一进程两个实例之间同样成立。

当前对账**已经读取协调层**，把代码再改成“先读协调层全量租约”不能解决 memory 的不可见性。直接跳过对账又会损失单实例崩溃后的占用恢复。

建议把“同一资源集合 + memory 协调”明确限定为单实例，并在需要强保证时使用完整实例存活期的独占机制或改用共享租约协议。仅拿到一把短暂启动锁，不代表其他工作实例不存在；其他实例不参与的锁也不能保护它们的获取与归还。若引入 owner、存活检测及数据库协调，则属于新的共享协调设计，不能称为现有 memory 模式已支持多实例。

#### P0-3：通用等待不会被其他实例释放唤醒

[通用 waiter](purser/facade.py#L1373) 保存在实例内；释放只服务本地等待者。共享 Redis 后，B 已入队、A 释放，B 仍可等到 `PoolExhausted`，此时存储已有空闲槽位。

定向路径通过存储轮询可观察释放：[定向获取](purser/facade.py#L1440)。`sleep(min(0.1, remaining))` 表示一次轮询休眠上限，不是“100ms 内一定获取成功”；调度、查询、竞争和故障都会增加延迟。

可增加通知加重扫，并考虑断线、漏通知和重连后的补扫；周期轮询也是一种权衡。**pub/sub 只通知，不提供全局 FIFO 或优先级公平。** 若目标包含全局公平，须另外设计共享排队及授予协议，不能把多个本地公平队列收到同一通知当成全局公平。

#### P0-4：consumer 去重不是原子的，定向获取也不能保证

通用候选过滤使用实例内 `_consumer_holds`；`mutex_group` 与亲和性也没有全局协调。定向路径会读取协调层，但采用“查询已有 → 再提交”，没有原子条件授予：[consumer 查询](purser/facade.py#L1465)。

本轮区分了两类结果：

- 顺序场景：另一实例的租约已经可见，Redis 定向获取能返回已有 token；通用获取仍可能新建。
- 并发场景：暂停第一个获取于注册前，第二个同 consumer 请求通过查询并提交，最终 `distinct=true, used=2, live=2`。两个协程处于同一实例时也能复现。

该查询还未过滤过期租约，因此“查到即返回”不能证明租约仍有效。修复需要在权威协调协议内原子地保留 / 授予 consumer 与资源或互斥组的关系，并处理取消、过期、重入和清理。增加一次查询只能减少部分重复，不能成为正确性保证。

还应先明确产品契约：通用获取当前会过滤本地已持有资源并尝试其他候选，与定向“返回已有租约”并不完全相同。不能在实现前把所有情况概括成“一个 consumer 全池只能有一份租约”。定向与通用路径的冷却、请求钩子等校验范围也需统一或明确差异；补充探针已观察到定向绕过部分通用过滤的行为。

#### P0-5：对账与获取 / 归还跨两平面交错

确定性反例：A 第二次获取在存储 +1 后、Redis 注册前暂停，B 对账读到存储占用 2、租约槽位 1，按 1 重置；A 注册完成后已有两份租约但计数仍为 1，第三次获取成功：

```text
used=2, live=3, max_slots=2
```

即使增加“扫描时版本 → reset 时版本”CAS，反例仍成立：探针中扫描和写入时版本均为 5，中间没有新的存储修改，但第一次扫描时就已处于跨平面中间态。

归还也有对称窗口：先删除协调记录、尚未减存储时暂停，对账已经按较少租约重置，归还恢复后又减一次。实测 `used=0, live=1`，不需要并发 acquire 即可出现。

修复必须给**所有改变租约或占用的路径**建立一致性协议，包括获取、归还、撤销、续租/过期、对账、GC、管理更新及其失败补偿。可评估统一事务中的权威租约账本，或完整参与者共享的互斥与可恢复操作记录等方案；跨数据库和 Redis 时还需解决进程死亡、锁过期、旧持有者继续执行及提交结果不确定。

“加一把带 TTL 的 Redis 锁”只有在参与范围、所有权校验、续期、fencing 和故障恢复均定义后才是方案的一部分；“只锁对账”或“再加一个 CAS”不是充分修复。后台 GC 也不能把当前不安全的对账原样搬成定时任务。

#### P0-6：获取取消与异常补偿不闭合

[提交实现](purser/facade.py#L1624) 有两处已验证问题：

| 交错点 | 当前行为 | 实测结果 |
| --- | --- | --- |
| `register_lease` 等待中取消 | 内层取消处理减槽，外层取消处理再次减槽 | 已有正常持有者时，取消后 `used=0, live=1`；继续分配形成 2 槽 3 租约 |
| 占用增加后的状态通知处取消 | 尚未进入后面的回滚保护 | `used=1, live=0, tracked=0`，占用泄漏 |

这不是共享后端专属问题，同实例也会遇到。需要明确提交阶段及唯一补偿责任，取消 / 异常只能归还本次操作实际拥有的占用，不能靠多层 `except` 各自减槽。

#### P0-7：Redis 注册、删除、续租与 TTL 协议不完整

[RedisCoordination](purser/backends/coordination.py#L128) 使用多个独立命令维护资源 hash、token 索引和资源 ID 集合；TTL 施加在**整个资源 hash**上，而非每个 token。

| 问题 | 故障 / 交错方式 | 已观察结果 |
| --- | --- | --- |
| 部分注册 | 两次 HSET 后、SADD 前失败 | 存储补偿为 0，但租约 hash 残留且 `PTTL=-1`；可被定向查询拿到 |
| 删除非原子 | 先读取 raw，再分别删除记录和索引 | 不具备原子 pop 的唯一删除确认；需与重复归还及并发注册一起验证 |
| 续租与删除竞争 | 续租读旧值，删除完成后再 HSET | `renewed=true`，资源 hash 又出现租约，但 token 索引与 active 集合已无记录 |
| 短租约缩短整组 TTL | 先注册 600 秒长租约，再注册 1 秒短租约 | hash TTL 从约 604000ms 降为 5000ms；长租约未到期记录却消失 |
| 过期字段被保留 | 长租约维持 hash，短租约字段已过期 | `records=2, expired=1, active=2`，查询没有逐 token 过滤 |

短续租也可能缩短同资源其他租约的保留时间。Redis key TTL 既不等价于每租约有效期，也不会自动把存储占用减回去。

整改应覆盖原子注册 / 条件删除 / 条件续租、逐租约过期判定、索引一致性和重复操作语义。Lua 或事务可用于 Redis 内部原子性，但**不能单独解决 Redis 与 SQL 存储之间的跨平面事务**。全成功路径、命令报错、断线后结果未知和进程崩溃都需验收。

#### P0-8：归还、撤销、资源更新与失联处理可误减或丢失占用

| 路径 | 证据 | 问题与结果 |
| --- | --- | --- |
| `release` | [facade.py:1736](purser/facade.py#L1736) | 虽先按 token 删除，但减槽使用调用方 `resource_id/slots`；修改合法 Lease 副本的 slots 后可扣掉别人的占用 |
| `force_revoke` | [facade.py:2504](purser/facade.py#L2504) | 读到旧租约后暂停，旧租约已释放且新租约已获取；恢复后忽略 unregister 返回空仍减槽，得到 `used=0, live=1` |
| `update_resource` | [facade.py:2568](purser/facade.py#L2568) | 先检查空闲并生成含旧 concurrency 的 patch，再按新读版本 CAS；中间获取可被旧 patch 覆盖 |
| 心跳失联处理 | [facade.py:2020](purser/facade.py#L2020)、[lease.py:48](purser/core/lease.py#L48) | 处理失联时 unregister 取消当前心跳任务自身，后续异步归还可被取消；实测占用仍为 1、租约及跟踪均已移除 |
| `release` 与 `force_revoke` 竞争 | 真实 Redis 专项 [test_fault_tolerance.py:102](tests/test_fault_tolerance.py#L102)，原 40 次中 1 次失败（V26） | **已修复**：Redis 删除改为 [Lua 原子删除](purser/backends/coordination.py#L45)，`force_revoke` 只在删除成功时减槽，`release` 仅在存储仍计数该槽位时恢复 token；真实 Redis 15 轮内核回归、60 轮同步竞争均无幽灵租约 |

因此“release 只依赖 token，任意复制 Lease 都安全”不准确；也不能以存储支持 CAS 为由推断管理操作和撤销安全。应使用权威记录驱动占用变化，条件删除成功者才取得归还权；管理 patch 需要与前置状态检查处于同一并发控制条件，失联清理不得在完成前取消自身。

#### P0-9：取消等待者会取消共享建池启动任务（已修复）

运行中的 `Purser.create_pool()` 把 `pool.start()` 包成共享 Task；原 `_ensure_started` 直接 `await` 该 Task：[建池](purser/facade.py#L3009)、[启动检查](purser/facade.py#L407)。Python 会将被等待 Task 的取消传播到上游，因此取消任意一个等待中的 `acquire` 会连带取消共享启动任务，其他等待者收到 `CancelledError`，对账中断。

真实 Redis 的 `test_waiter_cancel_stress_no_invalid_state_and_no_leak[memory-redis]` 原稳定 5/5 失败，fakeredis 3/3 通过（V25）：网络 I/O 扩大了启动窗口。修复为 `_ensure_started` 对共享 Task 使用 `asyncio.shield`，等待者只取消自身；真实 Redis 15 轮全部通过，全量套件不再出现该失败。

### 5.2 可用性、控制面与运维缺口

| 能力 | 当前状态 | 所需设计 |
| --- | --- | --- |
| 存储单次容量检查 | SQLite 探针中 20 次并发尝试、容量 2，成功 2、占用 2；MySQL 8.4 同步验收中 6 线程 60 次获取/归还无超卖，未单独做 20 并发容量探针 | 保留此局部保证，不上升为端到端租约安全 |
| 共享租约可见性 | Redis 能让记录跨实例可见，但存在 5.1 所列原子性和过期问题 | 修正协议后再定义活跃视图 |
| 崩溃后槽位回收 | 其他实例缺少可靠、无需重启的全局 orphan 回收闭环 | 与获取 / 归还共用一致性协议；有界恢复及持有者失效通知 |
| 周期维护唯一执行 | 每实例独立调度器 | 明确任务幂等性、选主和失效接管；锁本身不自动保证外部副作用恰好一次 |
| 事件跨实例 | 当前没有桥接 | 区分临时通知与可重放事件；pub/sub 本身不提供可靠历史 |
| 用量 / 指标 | 实例内累计 | 汇总范围、重复处理、持久化及故障丢失策略 |
| 验活任务 | 查询和取消主要依赖发起实例 | 共享任务状态或明确要求路由回所属实例 |
| 池注册、配置和类型 | 每实例本地注册 | 配置传播、插件部署及版本一致性；UI 保存配置文件不是分布式注册表 |
| 备份 / 恢复 / 删除池 | 主要按本实例注册表操作，没有全局维护屏障 | 定义操作范围，防止其他实例仍在写入或获取 |

SQLite 的写并发受到单写者机制和事务长度影响，MySQL 也受连接、锁及事务设计影响。删除“2～3 个 SQLite 进程以内必然可接受”这样的无基准阈值，按实际负载测试后选型。

## 六、部署、隔离、审计与密钥

### 6.1 独立后端的完整前提

1. **按真实外部实体分片。** 不同资源 ID 不一定代表不同账号、代理出口或上游额度。对同一个真实实体重复导入，仍可能形成不可见的双重使用。
2. **同一分片同时只有一个实例。** 旧实例未退出，新实例就指向同一 SQLite/MySQL 资源集合，会重新变为共享后端多实例；其中 memory 协调会形成共享存储 + 本地协调。滚动重启必须有独占和交接策略。
3. **请求路由与分片一致。** 指定资源的调用须送达所属分片；不能期待其他分片自动互借容量。
4. **分别定义每种状态的命名空间。** 不仅检查 storage 和 coordination，还包括 idempotency、审计、池配置、备份恢复等控制面状态。
5. **子进程不要继承已启动的 loop、后台线程或连接。** 各进程自建运行时，或按 §1.5.2 通过本地 IPC 接入唯一调度进程；RPC 连接按进程创建，不能在 `fork` 后复用父进程的连接对象。
6. **接受并验证单实例边界。** 分片仍继承取消、去重、管理并发、生命周期等基础缺陷；Redis 分片还继承 Redis 租约问题。

### 6.2 Redis 隔离不是一个开关

协调层 key 含 `key_prefix`、pool 和 rid：[键构造](purser/backends/coordination.py#L150)。当前 Purser 工厂没有暴露协调层 `key_prefix` 参数：[工厂](purser/facade.py#L2999)。

| 隔离手段 | 能隔离什么 | 不能据此推断什么 |
| --- | --- | --- |
| 独立 Redis 实例 | 可分开数据和部分故障影响面 | 同宿主机等共享基础设施的故障仍可能相关 |
| 不同 Redis DB | 分开逻辑键空间 | 不是物理隔离，也不是独立容量或故障域；需核对实际部署支持 |
| 同 DB，不同 pool 名 | 分开当前协调租约键空间 | 不能自动隔离 Purser 的幂等键和全部控制面状态 |
| 独立 SQL / SQLite 存储 | 分开资源主数据和计数 | 若租约仍共用同名 pool/rid，仍会发生串扰 |

实测独立存储 + 同 Redis 同 pool：B 对账把 A 的租约算进自己的占用；不同 pool 后双方可独立获取，但 A 写入的同名幂等 key，B 仍能读到。原因是 Redis 幂等存储默认前缀为 `purser:idem:`，不带 pool：[实现](purser/idempotency.py#L49)。即便协调选择 memory，只要构造时传入 Redis 参数，也要检查是否启用了共享幂等存储。

`delete_pool(force=True)` 当前会先检查活跃租约并拒绝删除，不能写成“force 无条件清掉别人的活跃租约”。但检查与后续 flush 不在全局原子边界内，仍需评估其他实例新建租约的竞态：[删除池](purser/facade.py#L3370)。

### 6.3 配额按资源 / 外部实体计算

`PoolConfig.max_slots` 在导入时作为每个资源的默认上限，而非进程总并发额度：[导入赋值](purser/facade.py#L1011)。两个分片各有 3 个资源、每资源 2 槽，容量合计是 12，不是配置值 `2 + 2 = 4`。

应按真实外部实体 e 建立约束：

```text
sum(分配给各分片中、对应实体 e 的资源容量) <= 实体 e 的真实允许容量
sum(消耗同一共享上游额度的容量分配) <= 该共享额度
```

同时检查并发、次数配额、时间窗口等不同限制，不能把它们混成一个 `max_slots`。这些跨分片约束和独占路由由应用层负责，当前 purser 不会自动校验。

### 6.4 审计：磁盘链、内存视图、阻塞开销分别判断

[审计存储 append](purser/core/audit.py#L82) 使用 `BEGIN IMMEDIATE`，在事务中读取链尾、分配 seq/prev_hash 并写入；不是完全无保护的单写者假设。

但 [AuditLog.record](purser/core/audit.py#L196) 先同步内存再写盘，其他写者可能在两步之间插入记录。当前写者取得更新后的 seq，却没有把插队记录补进自身内存，之后增量同步也可能跳过它。

实测结果：

- 确定性交错：A 内存只有 `seq=[2]`，`verify=False`；新读取者从磁盘看到 `[1, 2]`，`verify=True`。
- 4 个真实 spawn 进程各写 50 条：退出正常、磁盘 200 条且链验证通过；3 个写者的内存验证失败。

因此应修复内存同步的一致性，不能把磁盘链说成已必然断裂；反过来，一次磁盘通过也不代表全部审计 API 并发安全。同步 SQLite 写和锁等待仍会占用调用线程 / loop，异步 writer 若采用则还需定义落盘确认与关闭排空语义。

独立分片可选择独立审计文件以实现真正的审计隔离和减少竞争；若有意共享审计库，须把它视为共享组件并验收，而不是既宣称无共享又假设没有并发问题。

### 6.5 密钥和连接生命周期

- SQLite 后端必须显式提供 `sqlite_path`；省略路径的推荐简写不能实际建池。
- MySQL 池若使用秘密字段，缺少稳定密钥时会被配置检查拒绝。这是现有保护，不是已连接 MySQL 后才发现的问题。
- SQLite 路径存在时，Purser 会先调用 sidecar key 加载，再把结果传给 codec；codec 只有 key 为 `None` 才读环境变量：[构造](purser/facade.py#L2906)、[codec](purser/crypto.py#L40)。本轮设置 `PURSER_SECRET_KEY` 后，新 SQLite 路径仍未使用该环境密钥。当前部署应明确提供稳定 `secret_key` 或先修正优先级，不能仅建议“配置环境变量即可”。
- [密钥首次创建](purser/crypto.py#L135) 采用读取失败后生成并直接写文件，存在多创建者竞争。临时文件加覆盖式 rename 只防半文件，不保证所有进程取得相同密钥；需要不覆盖创建 / 互斥和完整读取协议，或统一注入。
- `RedisIdempotencyStore.aclose()` 已存在，但 `Purser.close()` 未调用；协调层 Redis close 当前也没有完整释放协议。应区分自建和外部注入客户端，避免泄漏，也不能随意关闭其他使用者共享的客户端：[幂等关闭](purser/idempotency.py#L85)、[协调关闭](purser/backends/coordination.py#L248)。

以上有数据可读性和资源生命周期影响，不能全部推迟到性能优化阶段。

## 七、验证记录与可复现方法

### 7.1 范围与证据等级

主要验证环境为 Windows、Python 3.11.9、项目 `.venv`。存储与协调分三档：临时 SQLite 与 fakeredis 2.40.0 用于同 loop 屏障与故障注入；异步共享后端双实例用例在配置 `PURSER_TEST_REDIS_URL` 时直连真实 Redis，否则跳过；真实 MySQL 8.4.0（专用库 `purser_sync_acceptance`，root/空密码）与真实 Redis 7.2.4（专用 DB 15，测试前后 flush 该库）用于同步门面单实例验收、跨实例专项、静态分片专项、中心调度服务六组合验收、`test_mysql.py` 与真实 Redis 专项。真实服务仅在本机内网实测，未覆盖网络分区、跨机延迟或共享库的长时间多写者压力。

| 证据类别 | 本轮范围 | 可支持的结论 |
| --- | --- | --- |
| 源码核对 | 同步门面、租约、SQLite/MySQL、Redis、审计、密钥、测试 | 当前操作顺序与已实现边界；不是全部服务实测 |
| 同 loop 多实例屏障 | SQLite + memory / fakeredis | 确定性验证跨平面和多实例交错；不能冒充真实网络多进程性能结果 |
| 真实服务单实例 | 同步门面 6 组合 × 49 项，MySQL 8.4 + Redis 7.2.4，重复 12 轮 | 单进程单实例路径与后端真实协议一致；跨进程共享后端见下一条 |
| 真实服务跨实例 | [test_sync_cross_instance.py](tests/test_sync_cross_instance.py)：双同步实例 + 双 OS 进程，SQLite/MySQL + Redis 7.2.4，每组合 7 场景，重复 2 轮 | 核心租约语义（容量、可见性、撤销、陈旧归还、启动对账、TTL 回收、进程竞争）与定向/通用等待轮询唤醒通过；无 xfail |
| 真实服务静态分片 | [test_sync_static_shards.py](tests/test_sync_static_shards.py)：五种存储/协调形态 × 双 OS 进程，每进程独立分片 | 分片内单槽容量、跨分片资源/租约隔离、重启持久化通过；重叠实例与同名 Redis 池两条边界已固化（1 xfail + 1 反例断言） |
| 中心调度服务 | [test_service_central_matrix.py](tests/test_service_central_matrix.py)：真实 uvicorn 子进程 × 六种存储/协调组合，HTTP 客户端仅经 REST 访问 | 六组合均通过健康检查、建池/导入、幂等获取与重放、容量上限 409、跨客户端等待者唤醒、释放与事件历史；证明单副本服务可让六组合对客户端表现为共享 |
| 本地 IPC 调度 | [test_local_scheduler.py](tests/test_local_scheduler.py)：`spawn` Process / `Pool` / `ProcessPoolExecutor` worker，真实 SQLite+Redis、MySQL+Redis 各一组合 | 同机 worker 经命名管道/UDS 共享唯一调度进程；容量、幂等获取/释放、等待唤醒、worker 崩溃即回收、进程死亡上报通过；不经过 HTTP |
| 同步故障/压力 | [test_sync_fault_stress.py](tests/test_sync_fault_stress.py)：协调写/删除失败、存储归还失败、撤销竞争、8×40 争用、跨线程心跳 | 故障不泄槽、失败归还可重试、竞争无幽灵租约、心跳跨线程有效；fakeredis 与真实 Redis 均通过 |
| 真实 Redis 专项 | `test_redis_real.py` + `test_redis_more_edges.py` 共 9 项 | 跨实例可见性、撤销/陈旧归还、TTL 自愈、失败回滚；仍是单机 Redis |
| 性能基准 | [benchmarks/sync_facade_bench.py](benchmarks/sync_facade_bench.py)，6 组合 × 8 线程 × 200 次 | 本机单实例相对吞吐与尾延迟；不是生产容量或跨进程结论 |
| 真进程探针 | 原多进程 worker、4 写者审计 | 对应进程路径及审计持久化现象 |
| 同步门面探针 | 后台 loop、线程池、故障注入 | 桥接正常路径及历史草案缺陷；不代表完整契约或生产验收 |
| 现有测试 | hardening 回归 72 passed；文中点名的 10 个专项文件当前共收集 138 项（参数化后，无真实服务时部分跳过）；当前全套 1042 passed / 40 skipped / 1 xfailed（真实 MySQL 8.4 + Redis 7.2.4；收集 1083 项，与基线自洽） | 两组可能重叠，不相加为总数；绿色测试不能推翻反例 |
| 尚未实测 | 其他操作系统及完整 Python 支持矩阵；共享后端的推送通知、幂等去重、维护接管与报表作用域；审计写阻塞与目标环境容量 | 后续验收必需项，不标成已通过 |

### 7.2 关键探针摘要

输出仅保留决定结论的字段。`live` 在涉及 TTL 的用例中须结合“原始记录”与“未过期权威租约”解释。

| 编号 | 场景 | 结果 / 结论 |
| --- | --- | --- |
| V01 | SQLite 两连接、20 次并发占用、容量 2 | `successes=2, used=2`；单次存储原语有效 |
| V02 | memory 协调，A 持有后 B 启动 | 占用由 1 变 0；最终 1 槽 2 租约 |
| V03 | Redis 通用 waiter 已入队后另一实例释放 | **已修复**：同步门面等待者按 100 ms 轮询共享存储，自检到释放后自行分配；两个共享后端组合均通过，唤醒耗时断言 < 1.2 s。仍无推送通道，异步内核不适用 |
| V04 | 同 consumer 定向获取，暂停首个注册 | 两个不同 token；单实例两个协程亦可复现 |
| V05 | 存储占用后暂停注册，让另一实例对账 | 原实现及模拟 scan-version CAS 均出现 2 槽 3 租约 |
| V06 | 协调删除后暂停存储归还，同时对账 | `used=0, live=1` |
| V07 | 注册等待中取消第二次获取 | 双回滚；正常持有者仍活跃但计数为 0 |
| V08 | 占用后的状态通知中取消 | `used=1, live=0, tracked=0` |
| V09 | Redis 注册中途报错 | `PTTL=-1, used=0, live=1`；幽灵记录可被定向返回 |
| V10 | 长短租约混用、续租与删除交错 | 长租约记录提前消失；已删除字段复活且索引不一致 |
| V11 | stale revoke / 修改 Lease.slots 后归还 / 旧资源 patch | 均可导致其他有效持有者占用被扣除或覆盖 |
| V12 | 心跳失联清理，阻塞后续存储归还 | 自取消导致 `used=1, live=0, tracked=0` |
| V13 | 原门面 6 线程 × 40 次正常获取归还 | 240 次成功；仅证明正常桥接路径 |
| V14 | 复制 Lease 后修改 used、等待多个心跳 | 调用副本已过期而内核仍续租，used 未同步 |
| V15 | 调用线程 deepcopy 时 loop 修改同一 dict | `RuntimeError: dictionary changed size during iteration` |
| V16 | 获取已提交，返回值含不可 deepcopy 的锁 | 调用方失败，内核仍维持一份占用及心跳 |
| V17 | 建池启动失败 / close 遇到未结束 executor | 失败任务可被忽略；close 返回时线程仍存活 |
| V18 | executor 事件回调调用 stats / 两协程等待钩子 | 前者成功；后者交错运行，否定普遍死锁与整次 API 串行说法 |
| V19 | 审计写者交错 | 磁盘链通过，但部分写者内存链缺漏 |
| V20 | 独立存储、Redis 不同 pool、同幂等 key | 租约隔离成立，幂等结果仍共享 |
| V21 | SQLite 设置环境密钥、新建路径 | 实际使用 sidecar key，未采用环境密钥 |
| V22 | 每分片 3 资源、每资源 2 槽、2 分片 | 总容量 12，不能把两份池配置值相加当总容量 |
| V23 | 同步门面基础套件：生命周期、租约回写、批量、迭代、事件、多线程、关闭竞争、交付失败补偿 | 17 项通过；覆盖正常与门面生命周期路径，不代替内核故障注入 |
| V24 | 同步单实例验收矩阵：6 存储/协调组合 × 公开方法面、CRUD、租约、迭代、事件、备份恢复、重启、6 线程争用、超时、关闭、管理操作 | 49 项通过；六个组合均连真实服务（MySQL 8.4.0、Redis 7.2.4）；既有异步 MySQL 5 项、真实 Redis 专项 9 项通过；当时全量 963 passed / 37 skipped（加入跨实例专项后为 975 / 37 / 2 xfailed）；不覆盖其他内核 P0 与生产容量 |
| V25 | 真实 Redis 上取消获取等待者，同时有多个等待者在建池启动期间 | **已修复**：`_ensure_started` 使用 `asyncio.shield` 保护共享启动任务；真实 Redis 15 轮全部通过，全量套件不再出现该失败 |
| V26 | 真实 Redis 上 `force_revoke` 与 `release` 并发 | **已修复**：Redis `unregister_lease` 改为 Lua 原子删除；`force_revoke` 删除失败即放弃减槽；`release` 仅在存储仍计数槽位时恢复 token。真实 Redis 15 轮内核回归、60 轮同步竞争无幽灵租约 |
| V27 | 同步门面故障注入与长时压力：协调写失败、协调删除失败、存储归还失败、60 轮撤销/归还竞争、8 线程 × 40 次、1.5 秒跨线程心跳 | 6/6 通过；fakeredis 与真实 Redis 各 5 轮一致；失败不泄槽、token 可重试归还、无幽灵租约、无超卖。协调写失败当前可能表现为 `PoolExhausted`，错误映射仍需改进 |
| V28 | 同步门面性能基准：8 线程 × 200 次获取/归还，4 资源 × 4 槽，真实 MySQL/Redis | memory/memory 3003 ops/s；memory/redis 967 ops/s；sqlite/memory 213 ops/s；sqlite/redis 195 ops/s；mysql/memory 59 ops/s；mysql/redis 62 ops/s；详见 §7.4 |
| V29 | 跨实例并发归还：两个实例各自释放同一资源的有效租约（MySQL） | **已修复**：MySQL `transition_release` 版本 CAS 失败原直接返回 `ok=False`，合法归还会被误报 `LeaseConflict`；现带抖动重试 10 次。SQLite 同步对齐为 10 次带抖动（V32）。双实例 × 6 线程 × 15 轮重复通过，修复前可稳定复现 |
| V30 | 同步门面跨实例验收：容量/可见性/撤销/陈旧归还/启动对账/定向等待/通用等待轮询/硬崩溃 TTL 回收/双进程 12 轮竞争 | SQLite+Redis 与 MySQL+Redis 各 7 项全通过、无 xfail；V03 由 100 ms 轮询修复；Redis DB 15 测试前后为空，专项池通过 `delete_pool(force=True)` 清理 |
| V31 | 同步门面静态分片验收：五形态 × 双 OS 进程，每进程独立分片、同一资源 id 并发持锁 | 五形态均通过分片内单槽容量、跨分片资源/租约不可见、释放归零；SQLite/MySQL 重启保留资源且占用 0，memory 重启为空；同一 SQLite 分片重叠实例重复占槽 xfail（V02/P0-2）；私有分片共用 Redis 池名会被启动对账污染（反例断言）；MySQL `static-*` 行与 Redis DB 15 清理后为空 |
| V32 | 全量套件下 SQLite 跨实例并发归还 | 原 3 次、无抖动重试在 6 线程 × 15 轮满负载下偶发 `LeaseConflict`；对齐 MySQL 为 10 次带抖动后，专项 5/5 与全量 983 passed 复跑通过 |
| V33 | 中心调度服务六组合验收：真实 uvicorn 子进程分别以 memory/memory、memory/redis、sqlite/memory、sqlite/redis、mysql/memory、mysql/redis 启动，客户端只发 HTTP | 六组合各 1 项全部通过（6 passed，11.84 s）：`/service` 报告所选组合，建池/导入、`Idempotency-Key` 获取与重放同 token、容量满返回 409、第二客户端等待被释放唤醒、事件历史可查、释放后占用归零。结论仅限单副本；多副本回到 §1.5 的共享语义，且 API 可用不等于生产就绪 |

### 7.3 两个自包含的确定性复现

下面脚本可从项目根目录用项目 Python 运行；依赖项目及开发环境中的 fakeredis，只使用自动清理的临时目录和 FakeRedis，不访问真实服务。它故意断言**当前错误现象**以复现 V02、V05；将来修复源码后，断言应改成目标不变量，不能保留这些错误行为作为产品契约。

使用私有方法替换只为制造稳定屏障，不是公开 API 示例。每个用例设外层超时，结束时取消未完成任务并关闭测试实例与客户端。

脚本在同一进程内让两个实例共享同一 SQLite 文件，现行 §1.4.1 守卫会拒绝第二个实例；因此它显式设置 `PURSER_ALLOW_SHARED_BACKENDS=1`，模拟两台主机来复现 V02/V05。该开关只用于缺陷复现与验收夹具，生产代码不得开启。

```python
import asyncio
import os
import tempfile
from contextlib import AsyncExitStack
from pathlib import Path
from unittest.mock import patch

import fakeredis
from fakeredis.aioredis import FakeRedis

from purser import Purser
from purser.core.health import HealthReport
from purser.core.resource import Resource
from purser.types.base import ResourceType

# Reproduce V02/V05 in one process: this deliberately simulates two hosts.
os.environ["PURSER_ALLOW_SHARED_BACKENDS"] = "1"


class ProbeType(ResourceType):
    kind = "probe"

    async def generate(self, spec):
        return Resource(id=spec["id"], kind=self.kind, payload={})

    async def validate(self, resource):
        return HealthReport.ok()


def build(path, capacity, client=None):
    ps = Purser(
        storage="sqlite",
        sqlite_path=str(path),
        coordination="redis" if client is not None else "memory",
        redis_client=client,
        secret_key=b"x" * 32,
        maintenance_enabled=False,
    )
    ps.register_type(ProbeType())
    pool = ps.create_pool(
        "p", "probe", max_slots=capacity,
        lease_ttl=600, heartbeat_interval=300,
    )
    return ps, pool


async def acquire(pool, consumer):
    return await pool.acquire(consumer=consumer, timeout=0)


async def memory_reconcile():
    with tempfile.TemporaryDirectory(prefix="purser_memory_probe_") as tmp:
        async with AsyncExitStack() as stack:
            path = Path(tmp) / "resources.db"
            a, pa = build(path, 1)
            stack.push_async_callback(a.close)
            await a.start()
            await pa.import_resources([{"id": "r"}], import_validate="on_import")
            la = await acquire(pa, "A")
            assert (await pa.require("r")).concurrency.used_slots == 1

            b, pb = build(path, 1)
            stack.push_async_callback(b.close)
            await b.start()
            assert (await pa.require("r")).concurrency.used_slots == 0
            lb = await acquire(pb, "B")
            live = [lease for lease in await pa.leases("r") if not lease.expired()]
            live += [lease for lease in await pb.leases("r") if not lease.expired()]
            used = (await pa.require("r")).concurrency.used_slots
            assert la.token != lb.token
            assert used == 1 and sum(lease.slots for lease in live) == 2
            print(f"memory reconcile: used={used}, live={len(live)}, max=1")


async def redis_reconcile():
    with tempfile.TemporaryDirectory(prefix="purser_redis_probe_") as tmp:
        async with AsyncExitStack() as stack:
            server = fakeredis.FakeServer()
            ca, cb = FakeRedis(server=server), FakeRedis(server=server)
            stack.push_async_callback(ca.aclose)
            stack.push_async_callback(cb.aclose)
            path = Path(tmp) / "resources.db"
            a, pa = build(path, 2, ca)
            b, pb = build(path, 2, cb)
            stack.push_async_callback(a.close)
            stack.push_async_callback(b.close)
            await a.start()
            await pa.import_resources([{"id": "r"}], import_validate="on_import")
            await acquire(pa, "A1")

            entered, resume = asyncio.Event(), asyncio.Event()
            original = pa._coordination.register_lease

            async def paused_register(*args):
                entered.set()
                await resume.wait()
                return await original(*args)

            with patch.object(pa._coordination, "register_lease", paused_register):
                task = asyncio.create_task(acquire(pa, "A2"))
                try:
                    await asyncio.wait_for(entered.wait(), timeout=5)
                    await b.start()
                    resume.set()
                    await task
                finally:
                    if not task.done():
                        task.cancel()
                    await asyncio.gather(task, return_exceptions=True)

            await acquire(pb, "B")
            live = [lease for lease in await pa.leases("r") if not lease.expired()]
            used = (await pa.require("r")).concurrency.used_slots
            assert used == 2 and sum(lease.slots for lease in live) == 3
            print(f"redis reconcile: used={used}, live={len(live)}, max=2")


async def main():
    await asyncio.wait_for(memory_reconcile(), timeout=15)
    await asyncio.wait_for(redis_reconcile(), timeout=15)


if __name__ == "__main__":
    asyncio.run(main())
```

当前错误实现的关键输出：

```text
memory reconcile: used=1, live=2, max=1
redis reconcile: used=2, live=3, max=2
```

其他故障注入可按 7.2 和第五章给出的暂停位置转为独立回归：用事件屏障确认到达交错点，不依赖固定 sleep 猜测时序；每个测试同时核验权威租约、存储占用、本地跟踪、心跳任务及清理结果。临时探针和其本机路径不是长期测试资产，进入修复阶段后应将这些场景收录为正式测试。

### 7.4 同步门面性能基准

运行方式（真实服务地址通过环境变量传入；未设置时只跑本地组合）：

```bash
PURSER_TEST_MYSQL_URL=mysql://root:@HOST:3306/purser_sync_acceptance \
PURSER_TEST_REDIS_URL=redis://HOST:6379/15 \
python benchmarks/sync_facade_bench.py --threads 8 --ops 200
```

环境：Windows 10、Python 3.11.9、项目 `.venv`；单进程、单实例；4 个资源 × 每资源 4 槽；8 个调用线程各完成 200 次“获取 + 归还”；MySQL 8.4 与 Redis 7.2.4 位于内网测试主机。延迟为单次获取加归还的往返耗时，包含同步桥接、审计与指标开销。

| 存储 | 协调 | 吞吐（ops/s） | p50 | p95 | p99 | 最大 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| memory | memory | 3003.4 | 2.52 ms | 3.29 ms | 3.98 ms | 12.90 ms |
| memory | Redis | 966.7 | 8.08 ms | 9.84 ms | 11.18 ms | 17.76 ms |
| SQLite | memory | 213.4 | 34.26 ms | 57.23 ms | 117.76 ms | 146.89 ms |
| SQLite | Redis | 194.7 | 40.39 ms | 50.88 ms | 60.39 ms | 75.75 ms |
| MySQL | memory | 58.9 | 133.96 ms | 171.73 ms | 188.03 ms | 220.71 ms |
| MySQL | Redis | 62.3 | 126.54 ms | 155.91 ms | 184.34 ms | 216.80 ms |

结论边界：内存组合反映桥接与调度开销上限；SQLite 受本机磁盘与串行写锁影响；MySQL 组合受内网往返和数据库事务影响，约 60 ops/s，不能外推为生产容量。该基准没有覆盖跨进程、故障恢复期、长稳内存增长或审计写阻塞；这些仍需按目标环境单独压测。

## 八、实施顺序与验收门禁

### 8.1 建议路线

本轮移除固定行数和人日承诺：全量门面涉及 51 个方法外的公开对象与实时租约契约，共享后端还涉及故障协议，不能用转发函数数量估算整体可靠性工程。先完成契约和失败路径拆分，再按可验收工作包估时。

| 阶段 | 工作范围 | 出口条件 |
| --- | --- | --- |
| Phase 0：可信测试 | 修复未启动 worker、零成功通过、无效采样；加入确定性交错与故障注入 | 测试确实发放租约、确实采样；已知反例可稳定复现，错误不被吞掉 |
| Phase 1：基础正确性 | 取消与补偿、权威归还、同实例并发去重、Redis 原子性/TTL、对账一致性、管理竞争；启动/关闭与稳定密钥 | 该阶段适用的不变量与 P0 目标行为测试通过；全局等待、去重等语义另按 Phase 3 验收 |
| Phase 2：同步完整适配（单实例六组合验收通过） | 第三章的输入输出、SyncLease、交付补偿、51 方法和公开对象、事件、迭代、生命周期 | 单实例矩阵、故障注入、长时压力与性能基准已完成（MySQL 8.4 + Redis 7.2.4 实连）；跨实例核心租约语义已验证；其余内核 P0、跨实例通知/去重/维护接管与目标环境容量尚待补齐 |
| Phase 3：共享调度与控制面 | 跨实例通知和重扫、跨实例去重/互斥协议、崩溃回收、维护接管、任务/配置/事件/报表作用域 | 明确承诺的全局语义通过真多进程及真服务测试 |
| Phase 4：性能与运行验收 | 审计吞吐和视图、锁等待、容量、长时间运行、故障恢复、可观测性 | 满足目标负载和恢复时间；各部署组合有独立结果和限制说明 |

阶段表示依赖顺序，不要求机械串行：同步契约可以提前设计，审计视图与密钥问题也可并行修复。但不能先发布“完整同步等价”再补交付失败和生命周期；也不能在基础租约协议未成立时把后台 GC 当成生产可靠性补丁。

独立后端无需承担未承诺的全局调度功能，但仍必须通过其适用的 Phase 0、1、2 和运行验收。共享后端不能因若干阶段完成就自动标成全功能，必须按最终承诺清单验收。

若采用中心调度服务（E）作为交付形态，单副本六组合的可用性验收已完成（§1.5.1）；仍需新增服务级工作包：TLS/鉴权轮换/限流、客户端断线重试与对账、服务重启恢复、故障切换演练以及跨副本语义（若要多副本）。这些属于 Phase 4 与运维验收，不能用“HTTP API 已可用”代替。

若交付形态是同机多进程/进程池，优先采用本地 IPC 调度（§1.5.2），不需要 HTTP 服务；消息大小已有 32 MiB 上限（[protocol.py](purser/local/protocol.py)），但仍需补齐调度进程重启后的 worker 重连/重新建池、调度进程单点故障演练、慢消费者与背压保护，以及跨主机部署方案。IPC 只消除“把 HTTP 当作进程间数据面”的问题，不替代内核 P0 修复或共享后端多副本语义。

### 8.2 验收清单

**证据与测试有效性**

- [ ] 每个 worker 成功启动且实际获得租约；成功次数、错误分类和进程退出状态均被断言。
- [ ] 采样线程有独立连接，有效样本数大于零，任何采样错误可见；采样只是补充，不代替确定性测试。
- [ ] 内存协调共享存储的限制明确；已知失败可临时严格 xfail，但 xfail / skip 不计为该能力通过。
- [ ] 测试区分计数容量、有效租约槽位及真实外部使用者；在可解释的一致性检查点比较。
- [ ] 真 Redis / MySQL 使用明确专用测试环境；支持的 Python 与平台逐项执行，而非推断。

**基础租约协议**

- [ ] 获取各提交阶段取消、注册部分失败、结果未知及交付失败均有唯一补偿责任。
- [ ] 重复归还、撤销旧快照、心跳失联、管理 patch 不扣除其他持有者占用。
- [ ] Redis 注册、删除、续租及索引一致性可抵抗交错；过期按租约判定，短租约不提前删除长租约。
- [ ] 对账与 acquire、release、revoke、过期、GC、管理操作共同参与一致性协议；包含进程死亡和锁失效测试。
- [ ] 崩溃回收有界且不依赖所有存活实例重启；需要强外部排他时有相应失效 / fencing 机制。

**同步与多线程**

- [ ] 51 个 `owner.method`、属性、公开对象、上下文、批量租约和插件契约均有明确覆盖结果。
- [ ] 普通数据在所属 loop 内形成快照；输入无共享可变别名；不可复制对象有明确约束。
- [ ] SyncLease 的 used/outcome 回写、期限刷新和归还权威数据均正确；交付失败不留下持续心跳。
- [ ] 启动任务失败不被忽略；初始化失败、部分关闭失败和关闭超时可观测且不误报成功。
- [ ] 多线程提交与关闭竞争、loop 内误调、回调闭环和 executor 未退出均有测试。
- [ ] 迭代有界、不阻塞所属 loop，显式关闭有效；事件顺序、背压、丢失与重入规则明确。

**部署与运行**

- [ ] 分片按真实外部实体及共享额度核算；同分片新旧进程不重叠，入口路由一致。
- [ ] 存储、租约、幂等、审计和控制面各自的作用域有验证，不以不同 pool 名推断全部隔离。
- [ ] SQLite 路径、稳定密钥及首次创建协议已验证；自建与注入客户端的关闭责任明确。
- [ ] 审计磁盘链、写者内存视图与阻塞开销分别验收；备份恢复和删除池有对应操作边界。
- [ ] 明确区分本地公平、全局公平和仅有唤醒；吞吐与尾延迟来自目标环境压测。

**最终判断：同步门面方向可保留，独立分片是可选部署模式，但两者都不能绕过基础正确性。共享后端的关键不是再接一个 Redis，而是让租约、占用、取消、恢复与控制面的承诺形成可验证的一致协议。**
