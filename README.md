# Purser

通用采集资源调度库：统一管理代理、账号和自定义资源，按状态、健康度、配额与公平性规则完成**获取（acquire）、验活（validate）、封禁（ban）与归还（release）**。

基于 [purser 设计文档](docs/design/design-doc.md) 实现，遵循三条设计原则：

- **通用内核 + 适配器插件**：内核只关心状态机、调度、租约与后端一致性，代理/账号等差异全部收敛到 `ResourceAdapter` 插件。
- **异步优先**：全链路 `asyncio`，单事件循环内零锁竞争。
- **后端可插拔**：`memory` 后端开箱即用，`redis` 后端提供多实例共享与跨进程原子性，切换后端不需要修改业务代码。

## 快速开始（嵌入式）

```bash
python -m venv .venv
.venv\Scripts\pip install -e .                  # 嵌入式内核
.venv\Scripts\pip install -e ".[server,cli]"    # REST + Web UI + CLI
.venv\Scripts\pip install -e ".[dev]"           # 开发与测试
```

```python
import asyncio
from purser import Purser

async def main():
    ps = Purser()
    pool = ps.create_pool("proxies", "proxy")
    await pool.import_resources(
        [{"host": "1.2.3.4", "port": 8080, "country": "US", "tags": ["http"]}]
    )

    async with pool.lease(consumer="crawler-a", tags=["http"]) as lease:
        res = lease.resource  # acquire 时加载的类型化快照，不再多读一次库
        print(res.host, res.port, res.country, res.extra)  # 8080 已是 int

asyncio.run(main())
```

就这么多：首次 `acquire` 会自动启动运行时，结束时用 `await ps.close()`（或
`async with ps:`）做清理。`lease()` 退出块时无条件归还租约；手动路径用
`await pool.acquire(consumer=...)` + `await pool.release(lease, outcome=UsageOutcome.OK)`。

低频管理操作（封禁、排空、强制撤销、维护任务）统一放在 `pool.admin` 命名空间，
让数据面只保留 acquire / lease / release / renew 几个动词：

```python
await pool.admin.ban("p2", reason="dead upstream")
await pool.admin.drain("p3", reason="rotate")
await pool.admin.run_maintenance("validating_gc")
```

> `tags` 是用户标签，只用于调度筛选；`host` / `port` / `country` / `province` /
> `city` / `district` 等是类型化字段，读取时自动合并进 `payload`。字典导入中未
> 声明的键不会丢弃，而是作为自定义 payload 保存，可通过 `Resource.extra` 读取。
> 字段声明与规范化规则见 [Python API 契约](docs/reference/python-api/resources.md)。

> 导入验活默认 `auto`：≤10 条记录在导入时同步验活（导入返回即可用），更多记录
> 自动转后台任务（导入立即返回，首次 `acquire()` 等待验活完成）。需要固定行为时
> 显式传 `import_validate="on_import" | "batch" | "none"`；`none` 只入库，
> 资源停在 `NEW`，要用 `await pool.admin.validate_now(<id>)` 手动验活。详见
> [导入与验活](docs/guides/import-and-validate.md)。

> 示例里的 IP 是占位符：会真实验活，请替换成可达代理；想完全离线跑通流程，
> 参照 [examples/quickstart.py](examples/quickstart.py) 注册一个 `validate()`
> 恒为 OK 的自定义 `ResourceAdapter`。

> `close()` 之后再 acquire 会抛 `NotStarted`（可从顶层导入）；同一进程内重启
> 不受支持，新建一个 `Purser` 即可。

### 同机多进程：`connect()`

调度进程持有唯一的 `Purser`，业务进程用 `connect()` 拿到覆盖数据面与只读目录的异步门面
（`acquire`/`lease`/`release`/`renew`/`stats`/`query`/`counts`/`get` 等；IPC 调用跑在线程池里，
不阻塞事件循环）：

```python
from purser import AcquireRequest, LocalScheduler, connect

sched = LocalScheduler(adapters=[MyAdapter()]).start()
sched.create_pool("proxies", "proxy")
sched.import_resources("proxies", [{"host": "1.2.3.4", "port": 8080}])

pool = connect("proxies", sched.address)   # 或省略地址，读 PURSER_LOCAL_SCHEDULER
async with pool.lease(consumer="worker") as lease:
    print(lease.resource_id, lease.payload)
```

`connect(..., resource_types={"proxy": ProxyResource})` 后，跨进程的 `lease.resource` 会恢复为
类型化对象（未注册时是通用 `Resource`）。所有租约对象都满足 `LeaseView` 协议
（`token` / `pool` / `resource_id` / `payload` / `resource` / `used` / `outcome` / `expired()`），
异步、同步、远端三种视图可以共用同一段只读代码。

`release(..., outcome=...)` 同时接受 `UsageOutcome` 与字符串值（如 `"banned"`）。异常对象带
`code` 与 `retryable`，可直接用 `from purser import is_retryable` 驱动重试。

## 服务模式（REST API + Web UI + CLI）

```bash
pip install "purser[server,cli]"
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
python -m purser.cli import proxies proxies.txt          # 默认 auto：小批量同步验活，大批量转后台
python -m purser.cli import proxies proxies.txt --import-validate none
# proxies.txt 每行支持：`host port` / `id host port` / `id protocol host port tag...`；
# `#` 开头为注释，行首为 `[` 的单行 JSON 数组直接导入
# 需要“重复提交复用首次结果”时显式加 --idempotency-key <key>（默认每次真正导入）
```

## 部署与配置

`purser serve` 支持环境变量和命令行参数，无需再写 `create_app(Purser(...))`：

```bash
# 单机持久化：sqlite 存主数据 + 内存做调度
export PURSER_STORAGE=sqlite
export PURSER_SQLITE_PATH=/var/lib/purser/purser.db
export PURSER_AUDIT_PATH=/var/lib/purser/audit.db
purser serve --host 0.0.0.0 --port 8000

# 多实例：sqlite/mysql 存主数据 + redis 做跨实例调度
export PURSER_COORDINATION=redis
export PURSER_REDIS_URL=redis://localhost:6379
# mysql 另加：PURSER_STORAGE=mysql PURSER_MYSQL_URL=mysql://user:pass@host/db

# 也可以用 CLI 参数覆盖环境变量
purser serve --storage sqlite --sqlite-path ./purser.db --coordination redis --redis-url redis://localhost:6379
```

嵌入式用法一致：

```python
# 单机持久化：sqlite 存主数据 + 内存做调度
ps = Purser(storage="sqlite", coordination="memory",
            sqlite_path="/var/lib/purser/purser.db", audit_path="/var/lib/purser/purser.db")

# 多实例：sqlite/mysql 存主数据 + redis 做跨实例调度
ps = Purser(storage="sqlite", coordination="redis",
            sqlite_path="/var/lib/purser/purser.db",
            redis_url="redis://localhost:6379")
```

库分两个平面：**存储层**（资源主数据、状态、审计，sqlite/mysql，重启不丢）与**协调层**
（运行期租约、心跳、唤醒，memory/redis）。池级调度参数可按需传字段
（`ps.create_pool("proxies", "proxy", lease_ttl=120)`），也可用 `profile="validated"`
等场景预设；完整字段、合并顺序与丢失语义见[配置](docs/reference/configuration.md)
与[数据平面](docs/design/data-planes.md)。注意多实例下 Redis 必须开启持久化，不能当纯缓存使用。

## 更多文档

文档站按“上手 / 指南 / 参考 / 设计 / 贡献”组织：

- [文档首页](docs/index.md)：安装、两个快速开始与按角色划分的文档地图。
- 指南：[资源可用性判定](docs/guides/availability.md)、[租约生命周期](docs/guides/lease-lifecycle.md)、[导入与验活](docs/guides/import-and-validate.md)、[多实例部署](docs/guides/multi-instance.md)、[门面选择](docs/guides/facades.md)。
- 参考：[Python API](docs/reference/python-api/index.md)、[REST API](docs/reference/rest-api/index.md)、[CLI](docs/reference/cli.md)、[配置](docs/reference/configuration.md)、[内置资源适配器](docs/reference/resource-adapters.md)、[LocalScheduler](docs/reference/local-scheduler.md)、[错误处理](docs/reference/errors.md)、[状态机](docs/reference/state-machine.md)。
- 设计与内部：[架构](docs/design/architecture.md)、[数据平面与丢失语义](docs/design/data-planes.md)、[算法与不变量](docs/design/algorithms.md)。
- [贡献指南](docs/contributing/index.md)与[测试规范](docs/contributing/testing.md)。
- 归档：[设计文档](docs/design/design-doc.md)、[可行性分析](docs/design/feasibility.md)。

本地构建：

```bash
pip install -e ".[docs]"
mkdocs serve -a 127.0.0.1:8001   # http://127.0.0.1:8001，避免与 purser serve 的 8000 端口冲突
```
