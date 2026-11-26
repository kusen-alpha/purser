# 教程：从脚本到服务

同一个资源池可以从“单进程脚本”平滑升级到“同机多进程”，再到“独立服务 + 多实例”。本教程按这三种形态推进，每一步都保留同一套 `acquire → 使用 → release` 语义。

## 形态对照

| 形态 | 适用 | 调度进程 | 业务进程如何接入 | 状态是否跨重启 |
| --- | --- | --- | --- | --- |
| 嵌入式 | 单脚本、CLI 工具、测试 | 业务进程自己 | 直接持有 `Purser` | 取决于存储层 |
| 同机多进程 | 一台机器多个 worker、需要共享槽位与租约 | 独立调度进程 | `connect()` / IPC | 取决于存储层 |
| REST 服务 | 跨机器、多语言、需要控制台 | `purser serve` | HTTP | 取决于存储层 |

三种形态的池配置、适配器与调度行为一致；变化的是“谁持有 `Purser`”和“调用怎么到达它”。

## 0. 起点：嵌入式脚本

```python title="worker.py"
import asyncio

from purser import AcquireRequest, Purser, UsageOutcome


async def main() -> None:
    ps = Purser(storage="sqlite", sqlite_path="purser.db")
    pool = ps.ensure_pool("proxies", "proxy", lease_ttl=120)

    async with pool.lease(AcquireRequest(consumer="worker-1")) as lease:
        try:
            await do_work(lease)
            lease.outcome = UsageOutcome.OK
        except Exception:
            lease.outcome = UsageOutcome.NETWORK_ERROR
            raise

    await ps.close()


async def do_work(lease) -> None:
    ...
```

先用这个形态把业务逻辑跑通：资源获取、异常归类、归还结果都在这一段里。后面的迁移只替换“怎么拿到 `pool`”，不改 `do_work()`。

## 1. 同机多进程：LocalScheduler

当多个 worker 需要共享同一批槽位时，不要让每个进程各自建池——`Purser` 实例不能跨进程共享，各自建池会各自计数、重复分配。改由一个调度进程独占 `Purser`：

```python title="scheduler.py"
from purser import LocalScheduler, ProxyAdapter

sched = LocalScheduler(
    storage="sqlite",
    sqlite_path="purser.db",
    adapters=[ProxyAdapter(probe_targets=["https://example.com"])],
).start()

sched.create_pool("proxies", "proxy", lease_ttl=120)
sched.import_resources("proxies", [
    {"host": "1.2.3.4", "port": 8080, "tags": ["http"]},
])
print(sched.address)  # worker 通过它或 PURSER_LOCAL_SCHEDULER 找到调度进程
```

worker 侧：

```python title="worker_remote.py"
import asyncio

from purser import AcquireRequest, UsageOutcome, connect


async def main(address) -> None:
    pool = connect("proxies", address)
    try:
        async with pool.lease(AcquireRequest(consumer="worker-1")) as lease:
            try:
                await do_work(lease)
                lease.outcome = UsageOutcome.OK
            except Exception:
                lease.outcome = UsageOutcome.NETWORK_ERROR
                raise
    finally:
        await pool.aclose()
```

要点：

- `connect()` 返回 `RemotePool`，数据面方法与本地池同名；`lease.resource` 默认是通用 `Resource`，传 `resource_types={"proxy": ProxyResource}` 可恢复类型化对象。
- IPC 调用跑在线程池里，不阻塞事件循环；但每次调用有超时，调度进程挂掉会抛 `SchedulerUnavailable` / `SchedulerTimeout`。
- `LocalScheduler` 默认 `start_method="spawn"`，Windows 与 macOS 行为一致；worker 侧不要创建新的 `Purser`。
- 调度进程退出后，worker 的租约按 TTL 回收；重启后调度进程会对账协调层，见[重启对账](../guides/multi-instance.md#restart-reconcile)。

完整 API 与地址格式见 [LocalScheduler](../reference/local-scheduler.md)。

## 2. REST 服务：purser serve

跨机器或需要 Web 控制台时，用服务模式：

```bash
export PURSER_STORAGE=sqlite
export PURSER_SQLITE_PATH=/var/lib/purser/purser.db
export PURSER_AUDIT_PATH=/var/lib/purser/audit.db
export PURSER_API_KEY=admin-secret
purser serve --host 0.0.0.0 --port 8000
```

建池与导入可以继续用 Python 管理面，也可以走 REST/CLI：

```bash
curl -X POST http://127.0.0.1:8000/pools \
  -H "Authorization: Bearer $PURSER_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name": "proxies", "kind": "proxy", "config": {"lease_ttl": 120}}'

purser import proxies proxies.txt
```

业务侧只需要 HTTP：

```python title="worker_http.py"
import os

import httpx

BASE = os.environ.get("PURSER_URL", "http://127.0.0.1:8000")
TOKEN = os.environ["PURSER_API_KEY"]
HEADERS = {"Authorization": f"Bearer {TOKEN}"}


async def lease_one() -> dict:
    async with httpx.AsyncClient(base_url=BASE, headers=HEADERS, timeout=30) as client:
        resp = await client.post(
            "/pools/proxies/leases",
            json={"consumer": "worker-1", "timeout": 10},
        )
        resp.raise_for_status()
        return resp.json()


async def release(token: str, outcome: str = "ok", used: int = 1) -> None:
    async with httpx.AsyncClient(base_url=BASE, headers=HEADERS, timeout=30) as client:
        resp = await client.request(
            "DELETE",
            f"/pools/proxies/leases/{token}",
            json={"outcome": outcome, "used": used},
        )
        resp.raise_for_status()
```

把 `do_work()` 包在 `lease_one()` / `release()` 之间即可。和嵌入式相比，额外要处理：

- **401/403**：密钥缺失或只读角色执行写操作。
- **409**：资源忙、池耗尽、租约冲突；先读状态再重试，不要盲重试。
- **422**：请求字段非法；重试无意义。
- **幂等**：获取租约支持 `Idempotency-Key`，网络重试时带上同一个键可避免拿到两份租约。

契约见 [REST API](../reference/rest-api/index.md) 与[错误处理](../reference/errors.md)。

## 3. 多实例

单机 SQLite 只能被一个进程写。需要多个服务实例时：

```bash
export PURSER_STORAGE=mysql          # 或 sqlite（每实例独立文件 + 共享 redis）
export PURSER_MYSQL_URL=mysql://user:pass@db/purser
export PURSER_COORDINATION=redis     # 必须
export PURSER_REDIS_URL=redis://redis:6379/0
purser serve --host 0.0.0.0 --port 8000
```

约束：

- **协调层必须共享**：租约、槽位、心跳与唤醒都在 Redis；只共享存储会让两个实例同时认为同一个槽空闲。
- **Redis 必须持久化**：它不是纯缓存，丢失会丢租约与在途状态。
- **时钟**：TTL 与心跳基于时间；跨机器部署要保证 NTP 同步，否则会提前/延后回收。
- **密钥一致**：所有实例使用同一个 `PURSER_SECRET_KEY`，否则互相无法解密 payload。

详见[多实例部署](../guides/multi-instance.md)与[后端配置示例](../guides/backend-configuration.md)。

## 4. 迁移检查单

- [ ] 业务代码里不再直接构造 `Purser`（服务形态）或不再每个 worker 建池（IPC 形态）。
- [ ] `consumer` 在进程/实例间唯一，避免幂等与公平队列把两个 worker 当成同一个。
- [ ] 失败路径都设置了 `outcome`；`finally` 或上下文管理器保证归还。
- [ ] 关闭流程：嵌入式 `await ps.close()`；IPC `sched.close()`；服务发送 SIGTERM 并等待 `drain_timeout`。
- [ ] 存储层不再是 `memory`（除非明确接受重启丢状态）。
- [ ] 打开 `/readyz`、`/metrics` 与审计落盘，见[可观测性](../guides/observability.md)与[生产检查单](../guides/production-checklist.md)。

## 下一步

- 需要定时验活、批次召回与解封：看[验活、召回与封禁](validate-and-recall.md)。
- 需要安全边界与密钥轮换：看[安全模型](../guides/security.md)。