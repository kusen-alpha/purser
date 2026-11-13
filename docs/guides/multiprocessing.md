# 多进程使用

跨进程共享资源集合有两条路：**同主机多进程**用 `LocalScheduler`（IPC，不经过 HTTP），**跨主机或大规模**用 `purser serve` + Redis 多实例。共同的红线是：`Purser` / `SyncPurser` 实例**不可 pickle、不可跨进程传递**，每个进程必须自建实例，或用客户端连接共享的调度进程。

## 为什么实例不能跨进程

- `Purser` / `SyncPurser` / `SyncPool` 不可 pickle，尝试序列化抛 `IsolationConflict`；
- `fork` 继承的实例在子进程首次调用时报错；
- 同一进程内的两个实例不能共享同一 SQLite 文件、MySQL 库、Redis DB 或适配器实例（装配时抛 `IsolationConflict`）；半共享会产生无法对账的租约 / 槽位视图。

## 两条路径怎么选

| 需求 | 方案 | 入口 |
| --- | --- | --- |
| 同主机多 worker 共享一批代理/账号，不想引 Redis | `LocalScheduler` | `purser.local` |
| 跨主机、多实例横向扩容，需要 REST / Web UI | `purser serve` + Redis | [多实例部署](multi-instance.md) |
| 仅单进程内多线程 | `SyncPurser` | [同步 Purser](sync-facade.md) / [多线程使用](threading.md) |

## 同主机：LocalScheduler

调度进程持有唯一一个 `SyncPurser`、存储与协调；worker 进程不建实例，只通过 `multiprocessing.connection`（Windows 命名管道 / POSIX Unix socket）连接它。

```python
# main.py
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import freeze_support

from purser import AcquireRequest, LocalScheduler

def worker_init() -> None:
    from purser.local import init_worker
    init_worker()                      # 读取 PURSER_LOCAL_SCHEDULER

def task(seed: int) -> str:
    from purser.local import get_client
    client = get_client()              # 进程内懒创建，复用连接
    with client.lease("proxies", AcquireRequest(consumer=f"p{seed}")) as lease:
        return lease.resource_id       # RemoteLease 是快照

if __name__ == "__main__":
    freeze_support()                   # Windows / spawn 必需
    with LocalScheduler(storage="sqlite", sqlite_path="purser.db") as sched:
        sched.create_pool("proxies", "proxy")
        sched.import_resources("proxies", [
            {"id": "p1", "host": "1.2.3.4", "port": 8080},
            {"id": "p2", "host": "5.6.7.8", "port": 3128},
        ])
        with sched.env():              # 设置 PURSER_LOCAL_SCHEDULER，退出时恢复
            with ProcessPoolExecutor(max_workers=4, initializer=worker_init) as ex:
                print(list(ex.map(task, range(8))))
```

要点：

- `LocalScheduler` 默认 `start_method="spawn"`，worker 只在 `initializer` 里取地址，不继承任何 purser 状态；
- `coordination="memory"` 也成立：协调状态始终在调度进程内，worker 只需 IPC 地址；
- 自定义 `ResourceAdapter` 用 `adapters=[MyResourceAdapter()]` 传给 `LocalScheduler`，在调度进程内注册；
- 显式传地址（`spawn` / 自定义队列）：`SchedulerAddress` 可 pickle，直接作为子进程参数传给 `LocalSchedulerClient(address)`；
- `task` / `worker_init` 必须在模块顶层定义，不要在导入时创建任何 purser 对象。

## 跨主机：服务多实例

多实例部署见 [多实例部署](multi-instance.md)：每个实例连同一套 `sqlite` / `mysql` + `redis`，通过 REST API 或各实例自己的客户端共享同一资源集合。此时注意：

- `PURSER_REDIS_URL` 只提供连接串，**不会**自动切换协调层；必须同时设置 `PURSER_COORDINATION=redis`；
- 协调层 Redis 必须按持久化组件部署（AOF），丢失在途租约会导致同一资源被并发使用；
- worker 与实例之间不传 purser 对象，只通过服务地址交互。

## 常见问题

| 现象 | 原因 | 处理 |
| --- | --- | --- |
| `IsolationConflict`（pickle / 跨进程） | 传递了实例对象 | 每进程独立创建，共享走 `LocalScheduler` / 服务 |
| 子进程首次调用报错（fork 继承） | fork 继承了实例 | 用 `spawn`，子进程内重新构造或连客户端 |
| `SchedulerUnavailable` | 连不上调度进程 / 已关闭 | 检查进程存活与 `PURSER_LOCAL_SCHEDULER` |
| worker 拿到的数据不刷新 | `RemoteLease` 是快照 | 需要最新状态时重新 `acquire` / `stats` |

客户端 API 与错误表见 [LocalScheduler](../reference/local-scheduler.md)。
