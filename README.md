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

## 部署与配置

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

## 更多文档

- [purser 设计文档](docs/purser设计文档.md)：架构、节点进度与里程碑。
- [API 契约](docs/API.md)：Purser / ResourcePool 等公共接口。
- [架构与算法](docs/架构与算法.md)：核心概念、状态机、算法、自定义类型、目录与实现范围。
- [测试与工程规范](docs/测试与工程规范.md)：测试覆盖清单、合入门槛与 CI。
