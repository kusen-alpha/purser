# LocalScheduler

`LocalScheduler` 让**同一台机器上的多个 worker 进程**共享一个调度进程。它通过 `multiprocessing.connection`（Windows 命名管道 / POSIX Unix socket）通信，不经过 HTTP，与 `purser serve` 是两套独立入口。

## 适用场景

- 多进程爬虫/任务池需要共享同一批代理或账号；
- 不想引入 Redis，但仍需要跨进程串行化租约；
- 希望调度状态集中在一个进程，worker 崩溃不影响租约回收。

不适用：跨主机共享（用 `purser serve` + Redis）、需要 REST/UI（用服务模式）。

## 结构

```text
父进程
  └─ LocalScheduler            # 创建并管理调度子进程
       └─ 调度子进程            # 唯一持有 SyncPurser、存储与协调
            ▲
            │ JSON over multiprocessing.connection
            │（长度前缀、协议版本 1、单条上限 32 MB）
       ┌────┴────┐
     worker 1  worker 2 ...     # 各自持有 LocalSchedulerClient
```

默认 `start_method="spawn"`，因此调度进程不会继承父进程的 `Purser` 实例；类型通过 `types=[...]` 显式传入并在子进程内注册。

## 启动调度进程

```python
from purser import AcquireRequest, LocalScheduler

with LocalScheduler(
    storage="sqlite",
    coordination="memory",      # 协调状态始终在调度进程内
    sqlite_path="/var/lib/purser/purser.db",
    types=[MyResourceType()],
) as sched:
    sched.create_pool("workers", "my-resource", config={"lease_ttl": 120})
    sched.import_resources("workers", [{"id": "r1"}, {"id": "r2"}])
    print(sched.stats("workers"))

    with sched.lease("workers", AcquireRequest(consumer="parent")) as lease:
        print(lease.resource_id, lease.payload)
```

构造参数：

```python
LocalScheduler(
    *, storage="memory", coordination="memory",
    sqlite_path=None, mysql_url=None, redis_url=None, audit_path=None,
    secret_key=None, authkey=None,
    start_method="spawn", startup_timeout=30.0, close_timeout=30.0,
    types=(), maintenance_enabled=None,
)
```

`storage` / `coordination` 接受与 `Purser` 相同的组合；即使 `coordination="memory"`，协调状态也只在调度进程内，worker 只需获取 IPC 地址。

## worker 侧

### 方式一：环境变量（推荐给进程池 initializer）

```python
from purser.local import get_client, init_worker

def worker_init():
    init_worker()          # 读取 PURSER_LOCAL_SCHEDULER

def task():
    client = get_client()
    with client.lease("workers", AcquireRequest(consumer="w1")) as lease:
        ...
```

父进程开启环境发布：

```python
with LocalScheduler(types=[MyResourceType()]) as sched:
    with sched.env():       # 设置 PURSER_LOCAL_SCHEDULER，退出时恢复
        with ProcessPoolExecutor(initializer=init_worker) as pool:
            ...
```

### 方式二：显式传地址（推荐给 `spawn`/跨进程队列）

`SchedulerAddress`（可从顶层 `purser` 或 `purser.local` 导入）可 pickle，可直接作为子进程参数：

```python
sched = LocalScheduler(types=[MyResourceType()]).start()
child = ctx.Process(target=worker, args=(sched.address,))
```

worker 内：

```python
from purser.local import LocalSchedulerClient

def worker(address):
    with LocalSchedulerClient(address) as client:
        lease = client.acquire("workers", {"consumer": "w1"})
        try:
            ...
        finally:
            lease.release()
```

地址里包含 `authkey`，`address.to_env()` 应按密钥保管，不要写进日志。

## 客户端 API

| 方法 | 说明 |
| --- | --- |
| `acquire(pool, req=None, *, timeout=None, idempotency_key=None)` | 返回 `RemoteLease` |
| `release(pool, lease_or_token, *, outcome=None, used=None)` | 幂等归还 |
| `renew(pool, lease_or_token)` | 返回新的 `expires_at` |
| `lease(pool, req=None)` | 上下文管理器，退出时自动归还 |
| `health()` / `pool_names()` / `stats(pool)` | 观测 |
| `create_pool(name, kind, config=None)` / `delete_pool(name, force=False)` | 池管理 |
| `import_resources(pool, records, ...)` / `generate(pool, specs)` | 导入与内建生成 |
| `shutdown()` / `close()` | 关闭调度进程 / 仅断开客户端 |

`RemoteLease` 暴露 `token` / `resource_id` / `consumer` / `slots` / `expires_at` / `payload` / `resource` / `replayed` 等属性，并支持 `with` 与显式 `release()`。它是一次**快照**，不随调度进程状态自动刷新。

> IPC 客户端只覆盖数据面与基础池管理。强制撤销、封禁、立即验活等管理操作没有对应方法；需要时在调度进程内调用，或改用 `purser serve` 的 REST API。

## 错误

| 异常 | 场景 |
| --- | --- |
| `SchedulerUnavailable` | 无法连接、调度进程已关闭、启动超时 |
| `SchedulerTimeout` | 等待响应超时 |
| `SchedulerProtocolError` | 地址格式错误、响应 id 不匹配、消息格式非法 |
| 远程业务异常 | 调度进程把 `PoolExhausted` / `LeaseConflict` 等原样抛回 worker |

`acquire` 的响应超时会按请求的 `timeout` 加 10 秒宽限；未设置时默认 300 秒。导入/生成单次上限 120 秒，超大导入请分批。

## 与同步 Purser 的关系

调度进程内部使用 `SyncPurser`，因此它同样受“一个进程一个实例”的隔离约束。worker 不要把 `Purser` / `SyncPurser` 对象传给子进程；只传 `SchedulerAddress`。详见[同步 Purser](../guides/sync-facade.md)与[多实例部署](../guides/multi-instance.md)。
