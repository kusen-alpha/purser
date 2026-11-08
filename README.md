# Purser

通用采集资源调度库：统一管理代理、账号和自定义资源，按状态、健康度、配额与公平性规则完成**获取（acquire）、验活（validate）、封禁（ban）与归还（release）**。

基于 [purser 设计文档](docs/design/design-doc.md) 实现，遵循三条设计原则：

- **通用内核 + 类型插件**：内核只关心状态机、调度、租约与后端一致性，代理/账号等差异全部收敛到 `ResourceType` 插件。
- **异步优先**：全链路 `asyncio`，单事件循环内零锁竞争。
- **后端可插拔**：`memory` 后端开箱即用，`redis` 后端提供多实例共享与跨进程原子性，切换后端不需要修改业务代码。

## 快速开始（嵌入式）

```bash
python -m venv .venv
.venv\Scripts\pip install -e .                  # 嵌入式内核
.venv\Scripts\pip install -e ".[server,cli]"    # REST + Web UI + CLI
.venv\Scripts\pip install -e ".[dev]"           # 开发与测试
```

内核只依赖 `redis`（协调层）与 `cryptography`（密文字段）；FastAPI/uvicorn
只在 `serve` 时需要，`httpx`/rich 只在 CLI 时需要。

```python
import asyncio
from purser import AcquireRequest, ProxyResource, Purser, UsageOutcome

async def main():
    ps = Purser()
    pool = ps.create_pool("proxies", "proxy")
    await pool.import_resources(
        [
            {"host": "1.2.3.4", "port": 8080, "country": "US", "tags": ["http"]},
            ProxyResource(
                id="p2", host="5.6.7.8", port="3128", country="DE",
                tags=["http"], payload={"isp": "CT"},  # payload 只放自定义键
            ),
        ],
        import_validate="on_import",  # 验活通过后才进入 IDLE、可被分配
    )

    async with ps:  # start() 可以晚于 import；退出时自动 close()
        async with pool.lease(
            AcquireRequest(consumer="crawler-a", tags=["http"])
        ) as lease:
            res = lease.resource  # acquire 时加载的类型化快照，不再多读一次库
            if res is not None:
                print(res.host, res.port, res.country, res.extra)  # 3128 已是 int
            print("租约快照", lease.payload)

        # 手动路径 + 使用结果反馈
        lease = await pool.acquire(AcquireRequest(consumer="crawler-b"))
        try:
            ...
        finally:
            await pool.release(lease, outcome=UsageOutcome.OK)

asyncio.run(main())
```

> `lease.resource` 是本进程 acquire 时加载的类型化快照；需要最新状态，或租约由其他进程
> 重建（此时 `lease.resource is None`），请用 `await pool.require(lease.resource_id)`。

> `import_resources`、查询、状态变更等管理面操作可以在 `start()` 之前执行（适合离线预装数据）；
> 但 `acquire()` / `lease()` / `acquire_many()` 必须先 `await ps.start()`，否则抛
> `NotStarted`（可从顶层 `from purser import NotStarted` 导入）。池的 `started` 属性可用于判断运行时是否已就绪。

> `tags` 是用户标签，只用于调度筛选；`host` / `port` / `country` / `province` / `city` /
> `district` 等是类型化字段，读取时自动合并进 `payload`。字典导入中未声明的键不会被
> 丢弃，而是作为自定义 payload 保存，可通过 `Resource.extra` 读取；也可以显式构造
> `ProxyResource(payload={...})`。字段声明与规范化规则见 [Python API 契约](docs/reference/python-api/resources.md)。

> 示例里的 IP 是占位符：`import_validate="on_import"` 会真实验活，请替换成可达代理；
> 想离线完成流程，可参照 [examples/quickstart.py](examples/quickstart.py) 注册一个
> `validate()` 恒为 OK 的自定义 `ResourceType`。

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
python -m purser.cli import proxies proxies.txt          # 默认 batch：后台验活，导入立即返回
python -m purser.cli import proxies proxies.txt --import-validate none
# proxies.txt 每行支持：`host port` / `id host port` / `id protocol host port tag...`；
# `#` 开头为注释，行首为 `[` 的单行 JSON 数组直接导入
# 需要“重复提交复用首次结果”时显式加 --idempotency-key <key>（默认每次真正导入）
```

> `--import-validate` 三档：`batch`（默认，后台任务验活）、`on_import`（逐条同步验活，
> 大批量导入会很慢）、`none`（只入库，资源停在 `NEW`，需再调用 `validate` 或重新以
> `batch` 导入才会进入 `IDLE`）。`NEW` 资源不可被分配；当池内资源全部处于 `NEW` 时，
> `acquire()` 会立即失败并提示验活方式，而不是一直等待到 `wait_timeout`。

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

> `PURSER_REDIS_URL` 只提供连接串，不会自动切换调度层；跨实例调度仍需
> `PURSER_COORDINATION=redis`（或 `--coordination redis`）。

嵌入式用法保持不变：

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

**存储层（主张：绝不能丢）**：资源基本信息、重要状态、槽位计数、`version` CAS、审计。`sqlite` 用 `resources` / `resource_tags` 两表持久化，payload 密文字段 AES-GCM 落库，单机“重启不丢”；`mysql` 按同一契约实现（`pip install "purser[mysql]"`，aiomysql 懒连接），适合已有 MySQL 基建的多实例存储。**协调层（运行期租约权威，丢失需分级处理）**：在途租约、心跳、等待唤醒、校验缓存，`memory` 单进程、`redis` 跨实例（`redis.asyncio` + 每资源哈希 + token 索引，强制 RESP2 兼容 Redis < 6，连接/操作带 5s 超时 fail-closed，断连抛 `BackendUnavailable`）。两个平面独立取值，行为套件按 存储 × 协调 组合运行；异常重启后 `start()` 会做对账：槽位计数按协调端**当前存活租约之和**对齐（既支持全孤儿归零，也支持“部分租约存活、其余为孤儿”的部分孤儿收敛），资源基本信息不受影响；迟到 release（token 已不在协调端）绝不触碰存储槽位，避免误扣新持有者。注意：**多实例下协调记录丢失会让其他实例看不到在途租约，可能重复占用同一资源**，因此 Redis 必须开启持久化，不能当纯缓存使用；详见 [数据平面与丢失语义](docs/design/data-planes.md)。

## 更多文档

文档站按“上手 / 指南 / 参考 / 设计 / 贡献”组织：

- [文档首页](docs/index.md)：安装、两个快速开始与按角色划分的文档地图。
- 指南：[资源可用性判定](docs/guides/availability.md)、[租约生命周期](docs/guides/lease-lifecycle.md)、[导入与验活](docs/guides/import-and-validate.md)、[多实例部署](docs/guides/multi-instance.md)、[同步 Purser](docs/guides/sync-facade.md)。
- 参考：[Python API](docs/reference/python-api/index.md)、[REST API](docs/reference/rest-api/index.md)、[CLI](docs/reference/cli.md)、[配置](docs/reference/configuration.md)、[内置资源类型](docs/reference/resource-types.md)、[LocalScheduler](docs/reference/local-scheduler.md)、[错误处理](docs/reference/errors.md)、[状态机](docs/reference/state-machine.md)。
- 设计与内部：[架构](docs/design/architecture.md)、[数据平面与丢失语义](docs/design/data-planes.md)、[算法与不变量](docs/design/algorithms.md)。
- [贡献指南](docs/contributing/index.md)与[测试规范](docs/contributing/testing.md)。
- 归档：[设计文档](docs/design/design-doc.md)、[可行性分析](docs/design/feasibility.md)。

本地构建：

```bash
pip install -e ".[docs]"
mkdocs serve -a 127.0.0.1:8001   # http://127.0.0.1:8001，避免与 purser serve 的 8000 端口冲突
```
