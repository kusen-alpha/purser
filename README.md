# purSer

通用采集资源调度库：把代理、账号、自定义资源当作"钱包里的钱"，按状态、健康度、配额与公平性规则安全地**借出（lease）、验真（validate）、冻结（ban）、归还（release）**。

基于 [purser设计文档.md](docs/purser设计文档.md) 实现，遵循三条设计原则：

- **通用内核 + 类型插件**：内核只关心状态机、调度、租约与后端一致性，代理/账号等差异全部收敛到 `ResourceType` 插件。
- **异步优先**：全链路 `asyncio`，单事件循环内零锁竞争。
- **后端可插拔**：`memory` 后端开箱即用，`redis` 后端提供多实例共享与跨进程原子性，业务代码无感切换。

## 快速开始（嵌入式）

```bash
python -m venv .venv
.venv\Scripts\pip install -e ".[dev]"
```

```python
import asyncio
from purser import Purser
from purser.core.resource import AcquireRequest, UsageOutcome

async def main():
    ps = Purser()
    pool = ps.create_pool("proxies", "proxy")
    await pool.import_resources([
        {"host": "1.2.3.4", "port": 8080, "tags": ["US", "http"]},
        {"host": "5.6.7.8", "port": 3128, "tags": ["EU", "http"]},
    ])
    await ps.start()

    async with pool.lease(AcquireRequest(consumer="crawler-a", tags=frozenset({"US"}))) as proxy:
        print("拿到代理", proxy.payload)

    # 手动路径 + 使用结果反馈
    lease = await pool.acquire(AcquireRequest(consumer="crawler-b"))
    try:
        ...
    finally:
        await pool.release(lease, outcome=UsageOutcome.OK)

        await ps.close()

asyncio.run(main())
```

## 服务模式（REST API + Web UI + CLI）

```bash
purser serve --host 0.0.0.0 --port 8000
# 或
python -m purser.cli serve
```

打开 http://127.0.0.1:8000 进入控制台。

```bash
export PURSER_API_KEY=admin-secret          # 可选，配置后写操作需要鉴权
python -m purser.cli status
python -m purser.cli ls proxies
python -m purser.cli lease proxies --consumer demo
python -m purser.cli release proxies <token>
python -m purser.cli import proxies proxies.txt --import-validate none
# proxies.txt 每行支持：`host port` / `id host port` / `id protocol host port tag...`；
# `#` 开头为注释，行首为 `[` 的整段 JSON 数组直接导入
```

## 核心概念

| 概念 | 说明 |
| --- | --- |
| `Resource` | 一条资源（代理/账号/自定义对象），携带状态、健康分、租约、配额 |
| `ResourceType` | 类型插件：`generate` / `validate` / 请求级钩子 |
| `Pool` | 一个类型 + 一套配置对应的资源集合 |
| `Lease` | 一次租约：token + TTL + 心跳 + 槽位数 |
| `StorageBackend` | 系统记录层：`memory` / `sqlite` / `mysql`，资源主数据 + 版本 CAS，禁止丢 |
| `Coordination` | 调度记录层：`memory` / `redis`，租约/心跳/唤醒，丢了可重建 |
| `EventBus` | 有界队列异步分发，慢订阅者丢弃 + 背压告警 |
| `AuditLog` | append-only 审计 + SHA-256 哈希链 |

## 状态机

12 态（`NEW / VALIDATING / IDLE / ACTIVE / FULL / QUARANTINED / SUSPENDED / QUOTA_EXHAUSTED / BANNED / LOST / DRAINING / ARCHIVED`），转移表是内核唯一的 truth source，测试从表自动穷举生成。

```text
生成/导入(NEW) -> VALIDATING -> IDLE <-> ACTIVE/FULL(租约) -> 归还/过期 -> IDLE
失败 -> QUARANTINED(隔离退避) -> 连续失败 -> BANNED -> unban 重验 -> VALIDATING -> IDLE
IDLE -> SUSPENDED / QUOTA_EXHAUSTED / DRAINING -> ARCHIVED(可 restore)
```

## 算法（5.x）

- **分配**：硬约束直接排除（F6），软偏好只在候选内打分；`health*w1 + age*w2 - load*w3 + preference + jitter`，Top-K 加权随机抗惊群。
- **租约**：token + 心跳续约（TTL/3）+ 看门狗 reaper 回收过期租约；`force_revoke` 带审计。
- **公平队列**：`priority_quota` 默认，高优先级插队，FIFO 兜底，等待超时抛 `PoolExhausted`，空池抛 `PoolEmpty`。
- **隔离退避**：`min(cap, base*2^(n-1)) + jitter`，试用期保护（24h 内失败不直接封禁）。
- **健康分**：EWMA（首验 alpha 0.6，之后 0.3），冷启动默认 0.5。
- **站点失败索引**：请求级校验按目标站记录失败，到期自动清，只降权不误封。

## 存储 × 协调（两平面，自由组合）

```python
# 单机持久化：sqlite 存主数据 + 内存做调度
ps = Purser(storage="sqlite", coordination="memory",
            sqlite_path="/var/lib/purser/purser.db", audit_path="/var/lib/purser/purser.db")
pool = ps.create_pool("proxies", "proxy")

# 多实例：sqlite/mysql 存主数据 + redis 做跨实例调度
ps = Purser(storage="sqlite", coordination="redis",
            sqlite_path="/var/lib/purser/purser.db",
            redis_url="redis://localhost:6379")
pool = ps.create_pool("proxies", "proxy")
```

**系统记录层（主张：绝不能丢）**：资源基本信息、重要状态、槽位计数、`version` CAS、审计。`sqlite` 用 `resources` / `resource_tags` 两表持久化，payload 密文字段 AES-GCM 落库，单机“重启不丢”；`mysql` 按同契约落地（`pip install "purser[mysql]"`，aiomysql 懒连接），适合已有 MySQL 基建的多实例存储。**调度记录层（丢了能重建）**：在途租约、心跳、等待唤醒、校验缓存，`memory` 单进程、`redis` 跨实例（`redis.asyncio` + 每资源哈希 + token 索引，强制 RESP2 兼容 Redis < 6，连接/操作带 5s 超时 fail-closed，断连抛 `BackendUnavailable`）。两个平面独立取值，行为套件按 存储×协调 笛卡尔积跑；异常重启后 `start()` 会做对账：槽位计数按协调端**当前存活租约之和**对齐（既支持全孤儿归零，也支持“一人活着、多余人死”的部分孤儿收敛），资源基本信息不受影响；迟到 release（token 已不在协调端）绝不触碰存储槽位，避免偷扣新持有者。

MySQL 集成测试需要真实实例：设 `PURSER_TEST_MYSQL_URL`（如 `mysql://root:pass@127.0.0.1:3306/purser_test`）后运行 `pytest tests/test_mysql.py`；未设置时自动跳过。

Redis 集成测试需要真实实例：设 `PURSER_TEST_REDIS_URL`（如 `redis://127.0.0.1:7001/15`，建议指向独立 DB，测试前后会 flush 该 DB）后运行 `pytest tests/test_redis_real.py`，或直接整包跑让所有 redis 协调用例走真实 Redis；未设置时自动跳过（默认用 `fakeredis`）。`tests/test_redis_real.py` 覆盖：租约哈希/token 索引往返与 `PEXPIRE` TTL、断连重连后租约跨连接可见、硬崩溃后 TTL 过期 + 重启对账回收、跨实例可见/强踢/迟到 release、三进程共享 sqlite + 真实 redis 并发不超卖。

## 自定义类型

```python
class SmsPoolType(ResourceType):
    kind = "sms"
    default_concurrency = 3

    async def generate(self, spec):
        return Resource(id=spec["id"], kind="sms", payload=spec)

    async def validate(self, resource):
        return HealthReport.ok(latency_ms=5)

ps.register_type(SmsPoolType())
pool = ps.pool("sms", "sms")
```

## 测试

```bash
.venv\Scripts\python -m pytest tests -q
```

覆盖：转移表穷举、分配器属性（hypothesis）、存储×协调矩阵行为、租约看门狗/心跳、隔离退避、配额/冷却/站点失败、REST API 鉴权与幂等、真实 HTTP CONNECT/SOCKS5 探测，以及 `tests/test_fault_tolerance.py` 的故障回测（协调层故障注入、并发抢占、revoke/release 竞争、批量回滚不泄漏、等待队列不重复唤醒、sqlite 下各类重启对账：持有租约/封禁/配额/DRAINING/隔离/密文/重复导入）。
`tests/test_edge_cases.py` 追加边界补充：等待者超时后残留句柄清理、定向获取非法状态、`UsageOutcome` 全矩阵副作用（OK/网络错误/限流/抢占/封禁）、忙时禁止 move/update、租约时钟到期后对账、双实例共享 sqlite+redis 不超卖、双重 close 幂等、批量导入后台任务重启排空。
`tests/test_boundary_data_edges.py` 追加数据与配额边界：`release(used<0)` 拒绝且不动状态、`used=0` 不计量、`use_limit` 在续约时强制生效、`top_up` 非正数拒绝、`update_resource(max_slots<=0)` 拒绝、空导入返回空报告、`drain` 幂等、`restore` 重新验活、ACTIVE 部分占用重启对账、心跳对瞬时续约异常重试后再判丢。
`tests/test_boundary_concurrency_edges.py` 追加等待者与恢复边界：等待任务被外部取消后句柄被清扫、高优先级等待者越过 FIFO、并发 `validate_now` 不卡 `VALIDATING`、`top_up` 与 acquire 竞争、reaper 与 holder release 竞争、双 `force_revoke` 竞争、`LOST`/`VALIDATING` 陈旧资源重启后由维护任务回收、key 文件损坏抛类型化错误、key 轮换后旧密文抛 `BadPayload`、`use_limit<0`/`query(limit<0)` 拒绝；`EventBus` 溢出只逐出最旧且正确计数。
`tests/test_boundary_admin_edges.py` 追加运维边界：`move_resource` 目标撞 id 拒绝（防静默覆盖）、占用中 `admin_set_state` 拒绝、`ban` 先回收全部活租约再封禁、`reconfigure` 禁改 storage/coordination、空 states 查询与全量等价、`CAPTCHA`/`TIMEOUT` 结果副作用、`NEW`/`SUSPENDED`/冷却重启保真并继续流转、非 id 字段去重导入。对应修复：`admin_set_state` 表缺 `NEW` 行导致对所有 NEW 资源必失败的预检、事件分发用 `wait_for(裸协程)` 在 Windows Proactor 下可冻结（改 `ensure_future + asyncio.wait(超时)`，`stop()` 加 shield+超时兜底）。
`tests/test_boundary_streaming_edges.py` 追加流式与容量边界：`acquire_many` 尊重请求的 `slots`（此前被强制改为 1）、混合槽位等待者在“大请求暂时不可满足”时不饿死小请求、`slots` 超池容量超时而非挂起、`query(limit=0)` 与 counts 一致性、导入覆盖保留状态/用量并更新 payload、`lease_ttl=NaN` 等非有限配置拒绝、`release(outcome="字符串")` 拒绝且不触碰槽位、归还后 `renew_manual` 返回 False。事件侧新增 `history()` 回放不随分发消失（此前回放读的是分发队列，SSE 游标续传拿不到已分发事件），`EventBus` 以独立环形缓冲承载历史。
`tests/test_boundary_lifecycle_edges.py` 追加生命周期边界：`pool.close()` 立即用 `PoolExhausted` 唤醒所有在途等待者（此前要等各自超时）、reaper 的 `on_expire` 抛错不阻断其余租约回收、并发重复 `quarantine_gc` 幂等不损坏状态、`allow_duplicate` 同消费者可合法持有第二份租约。
`tests/test_boundary_hooks_edges.py` 追加插件钩子与基础设施边界：`on_release`/`on_state_changed` 在获取、归还、进入 `VALIDATING` 等核心迁移上触发（此前仅管理操作触发，且 memory 后端返回活对象导致 old/new 被别名成同一状态——已改为迁移前快照）、钩子异常被 `on_error` 承接且不破坏主流程、类型注册表增删改查、`Metrics` 快照与 Prometheus 渲染、`IdempotencyStore` TTL 过期、`SecretCodec` 环境变量密钥与 key 文件复用。`tests/test_scheduler.py` 覆盖调度循环的 run/stop/remove/失败隔离。流式补充：并发导入同批去重后仅剩一行、`force_revoke` 清消费者持有且被踢者进入 F5 冷却、迁移后可从目标池直接获取。
`tests/test_boundary_misc_edges.py` 追加配置生效与杂项边界：`max_lease_duration` 此前只是配置摆设，现在 `_renew` 会把续约到期时间封顶并在总寿命用尽后拒续（heartbeat 随之停跳、reaper 回收）；`maintenance_enabled=True` 的启动态池在真实 reaper 循环下自动回收过期租约；`recall_batch` 的 retire/requalify/isolate 三种动作；`Resource.next_usable_at` 对冷却/过期/隔离/消费者级冷却的取最大合并；serde 编解码往返；审计分页 `entries(limit, after_seq)` 取最新 N 条语义；`Purser.start()` 幂等与双 close；CLI `lease`/`release` 处理器请求构造。
`tests/test_boundary_final_edges.py` 追加收敛期锁定：`_on_lease_lost` 现在注销 `LeaseManager` 中已被心跳放弃的租约（此前非过期租约残留导致 `stats.active_leases` 虚高且无人回收）、`force_revoke` 处理 `DRAINING` 资源直接归档、空标签查询与全量等价、事件总线 stop 后 publish 再 start 会排空积压、指标直方图 1 万上限、16/24 字节密钥往返、代理验活 2/3 多数派与最低延迟、同一 Purser 内存态整周期重启。
`tests/test_chaos_storm.py` 追加确定性混沌风暴：固定种子复现的故障雨云（获取/归还/强踢/封禁/解封/验活/退役/充值/维护随机交错，协调端 register/renew 约 5% 随机抛错），每步校验槽位不超卖、不为负、状态合法，静默后协调端零残留；跨 churn 冷重启对账保证主数据与 payload 保真、占用槽位归零；三实例共享 sqlite+redis 并发乱序不超卖、最终全部归还。
`tests/test_multiprocess_stress.py` 追加真多进程压测：三个独立 OS 进程各持连接写同一 SQLite 文件（各自内存协调），在版本 CAS 下并发 40 轮获取/归还，父进程 3ms 采样断言 `used_slots` 永不超 `max_slots`、结束时无槽位泄漏、worker 零意外异常。运行中暴露 SQLite 写锁竞争下 `transition_release` 偶发 `database is locked` 导致槽位残留——`SqliteStorage` 写路径新增锁冲突短重试（最多 3 次、退避）。`tests/test_api_fuzz.py` 追加确定性 REST 模糊风暴：250 步脏请求（null/类型错/越界/unicode），断言永不 500 且运行后存储不变量成立；据此修复 6 处真实 500（重复建池、动作/强踢/删除/释放/整池退役的 PurserError 未捕获、`slots:null`/`tags` 非列表/`records` 非列表/建池 config 脏类型的类型错误）。
`tests/test_boundary_lifecycle_knobs.py` 与 `tests/test_ui_contract.py` 追加到最后一批：`expires_in`（S18 绝对过期）修复——`Resource.next_usable_at` 原先把 `expires_at` 并进“最早可用时间”的取最大集合，等于把过期时刻当冷却开始，导致任何带 `expires_in` 的资源终身不可获取（现由过滤层的独立 `expires_at <= now` 检查处理）、`expire_policy=quota` 到期转 `QUOTA_EXHAUSTED`、`target_quota` 计量消耗至耗尽并可 `top_up` 恢复、`cooldown_consumer_only` 消费者级冷却隔离；UI 控制台静态契约：`/` 服务 HTML 且 JS 引用的每个端点（池/资源/租约/事件）在路由表中存在。
`tests/test_boundary_misc2_edges.py` 追加真实未测路径：`AcquireRequest.required_tag` 硬约束、`affinity` 亲和记录与 `_affinity_scores` 归一化、`schedule_window` 跨夜/普通时段窗口判定、`pool.generate()` 内建导入路径、`force_revoke(token=...)` 定向踢出共享资源的单条租约、`SecretCodec` base64 环境变量密钥。
`tests/test_redis_real.py`（真实 Redis 回测）暴露并修复：redis-py ≥8 默认 RESP3 握手在 Redis < 6 直接报 `HELLO` 未知命令（强制 `protocol=2`）；revoke 后新持有者重获资源时，原持有者迟到 release 会偷扣新持有者槽位（改为 token 消失即幂等不碰存储）；`_serve_waiters` 在 `await _try_allocate` 后未复查 waiter 取消状态导致 `InvalidStateError`（改为分配后若 waiter 已亡则 `_abandon_lease` 归还）；sqlite 多进程下 `transition_release` 版本 CAS 竞争偶发误报 `LeaseConflict`（改为重读重试）；`_reconcile_leases` 全有全无导致“活跃租约 + 孤儿槽位”永久卡住（改为按存活租约之和对齐）；`ResourcePool.close()` 可能无限挂起（改为有界关闭 + 超时告警）。
`tests/test_boundary_reconcile_edges.py` 追加对账与 `reset_usage` 边界：`reset_usage(slots=N)` 部分对齐/全对齐 no-op/负值拒绝/超上限收敛到 `max_slots`、DRAINING 有存活租约不归档、部分孤儿对账（活租约之外的多余额度收敛）、全孤儿归零、迟到 release 保留他人活槽位、等待者批量取消风暴（据此把 `_commit` 改造成取消安全：任意点被取消都会回滚已占槽位并注销协调租约，杜绝 `CancelledError` 泄漏槽位）。`tests/test_redis_more_edges.py`（真实 Redis）追加：`_ids_key` / `_token_key` 在 TTL 过期后由 `active()` 自愈清理（此前永久累积）、`use_limit` 停机后重启对账回收、协调端连接失败时存储槽位回滚且恢复后可继续获取、共享真实 Redis 下的部分孤儿对账。

## 工程门禁（合入门槛）

合并前必须全部通过，缺一不可：

- 公共 API 全类型标注，`mypy purser`（`strict`）零告警；
- `ruff check .` 全绿（`E/F/W/I/UP/B`，line-length 88，豁免 `E501` / `UP009`）；
- `pytest tests -q --cov=purser --cov-report=term-missing` 通过并上报覆盖率；
- CI（`.github/workflows/ci.yml`）lint / typecheck / benchmark 与 Python
  3.8-3.12 测试矩阵全部通过，其中真实 Redis / MySQL 集成用例由
  `PURSER_TEST_REDIS_URL` / `PURSER_TEST_MYSQL_URL` 环境变量门控，未设置时自动跳过；
- 代码改动同步更新 README / docs / CHANGELOG，版本号按语义化递增。

## 目录

```text
purser/                       # 项目根
  purser/           # Python 包
    facade.py       # Purser / ResourcePool 门面与编排
    config.py       # 池配置
    crypto.py       # AES-GCM 敏感字段加密
    core/           # 状态机 / 域模型 / 分配器 / 租约 / 健康 / 隔离 / 调度 / 事件 / 审计
    backends/       # storage(memory/sqlite) / coordination(memory/redis)
    types/          # proxy / account / 自定义插件
    ui/             # FastAPI + 单页控制台
    cli.py          # rich CLI
  tests/            # 行为/状态机/分配器/API 测试
  docs/             # API 契约
  examples/         # 端到端示例
  pyproject.toml
  README.md
```

## 实现范围

设计文档第 14 章：M0 内核（P0）已全部落地；M1 插件与验活（P0+P1 核心项：请求级按站校验、配额/计量、冷却、导入去重、payload 加密、幂等、trace 事件）已落地；M2 分布式（Redis + Lua CAS + hash tag + fail-closed）已落地；M3 操作台（REST + Web UI + CLI + `/healthz` + 指标）已落地；SQLite 持久化后端与审计落库已落地。P2/P3 项（bundle、自动抢占、blue-green 迁移、事件游标等）按文档预留，未进入首版。
