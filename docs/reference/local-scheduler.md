# LocalScheduler

`LocalScheduler` 让**同一台机器上的多个 worker 进程**共享一个调度进程。它通过 `multiprocessing.connection`（Windows 命名管道 / POSIX Unix socket）通信，不经过 HTTP，与 `purser serve` 是两套独立入口。

如果你想要 Purser 形状的异步门面（同步管理方法 + async 池数据面），用 [`ProcessPurser`](../guides/facades.md)：它内部管理 `LocalScheduler`，也可以 attach 到已有地址，并支持把 attach 句柄 pickle 给 worker。

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

默认 `start_method="spawn"`，因此调度进程不会继承父进程的 `Purser` 实例；适配器通过 `adapters=[...]` 显式传入并在子进程内注册。

## 启动调度进程

```python
from purser import AcquireRequest, LocalScheduler

with LocalScheduler(
    storage="sqlite",
    coordination="memory",      # 协调状态始终在调度进程内
    sqlite_path="/var/lib/purser/purser.db",
    adapters=[MyResourceAdapter()],
) as sched:
    sched.create_pool("workers", "my-resource", lease_ttl=120)
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
    adapters=(), maintenance_enabled=None, default_adapters="global",
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
with LocalScheduler(adapters=[MyResourceAdapter()]) as sched:
    with sched.env():       # 设置 PURSER_LOCAL_SCHEDULER，退出时恢复
        with ProcessPoolExecutor(initializer=init_worker) as pool:
            ...
```

### 方式二：显式传地址（推荐给 `spawn`/跨进程队列）

`SchedulerAddress`（可从顶层 `purser` 或 `purser.local` 导入）可 pickle，可直接作为子进程参数：

```python
sched = LocalScheduler(adapters=[MyResourceAdapter()]).start()
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
| `acquire(pool, req=None, *, timeout=None, idempotency_key=None, resource_cls=None)` | 返回 `RemoteLease`；`resource_cls` 让 `lease.resource` 恢复为类型化对象 |
| `release(pool, lease_or_token, *, outcome=None, used=None)` | 幂等归还；`outcome` 接受枚举或字符串值 |
| `renew(pool, lease_or_token, *, used=None)` | 返回新的 `expires_at`；`used` 省略时取 `RemoteLease.used`，调度进程按最大值更新租约计数；被拒抛 `LeaseLimitReached` / `LeaseLost` |
| `lease(pool, req=None)` | 上下文管理器，退出时自动归还 |
| `register_resource_type(kind, cls)` | 按资源 kind 注册反序列化类型（等价于构造参数 `resource_types=`） |
| `health()` / `pool_names()` / `stats(pool)` | 观测 |
| `create_pool(name, kind, *, config=None, profile=None, **overrides)` / `update_pool(name, *, config=None, **overrides)` / `delete_pool(name, force=False)` | 池管理；与 Python/REST 共用同一配置契约 |
| `import_resources(pool, records, ...)` / `generate(pool, specs)` | 导入与内建生成 |
| `pool_call(pool, method, ...)` | 调用白名单内的池方法；管理方法（`ban` / `drain` / `validate_now` 等）路由到调度进程的 `pool.admin` |
| `shutdown()` / `close()` | 关闭调度进程 / 仅断开客户端 |

`RemoteLease` 暴露 `token` / `resource_id` / `consumer` / `slots` / `expires_at` / `payload` / `resource` / `replayed` 等属性，并支持 `with` 与显式 `release()`。它是一次**快照**：调度进程会自动续约，但 worker 手里的 `expires_at` 只在 `acquire()` / `renew()` 时刷新，因此 `expired()` 仅供本地参考。

### 异步门面 `connect()`

需要 `async`/`await` 时，用 `connect(pool, address)` 得到与 `ResourcePool` 同形的远端池，IPC 调用跑在线程池里：

```python
from purser import AcquireRequest, connect

async with connect("workers", sched.address) as pool:
    async with pool.lease(AcquireRequest(consumer="w1")) as lease:
        print(lease.resource_id, lease.payload)
    print(await pool.stats())
```

`connect(..., resource_types={"proxy": ProxyResource})` 会在该池上注册类型化反序列化；`RemotePool` 同样提供 `acquire` / `release` / `renew` / `stats` / `import_resources` / `generate`，并带有 `admin` 命名空间（当前覆盖 `ban` / `unban`）。

> IPC 客户端只覆盖数据面与基础池管理。封禁 / 解封可用 `pool.admin.ban(...)` / `pool.admin.unban(...)`；强制撤销、立即验活等其余管理操作可通过 `client.pool_call(pool, "force_revoke", ...)` 调用（白名单内的管理方法会路由到调度进程的 `pool.admin`），或改用 `purser serve` 的 REST API。

## 错误

| 异常 | 场景 |
| --- | --- |
| `SchedulerUnavailable` | 无法连接、调度进程已关闭、启动超时 |
| `SchedulerTimeout` | 等待响应超时 |
| `SchedulerProtocolError` | 地址格式错误、响应 id 不匹配、消息格式非法 |
| 远程业务异常 | 调度进程按异常类的 `code` 原样重建：`PoolExhausted` / `ResourceBusy` / `BackendUnavailable` / `QuotaExceeded` 等 |

`PurserError` 及其子类都带 `code` 与 `retryable`；worker 侧可直接 `from purser import is_retryable` 判断是否值得重试。`NoSuitableResource` 等异常还会通过 IPC 保留 `target` / `checked` 等结构化细节。

`acquire` 的响应超时会按请求的 `timeout` 加 10 秒宽限；未设置时默认 300 秒。导入/生成单次上限 120 秒，超大导入请分批。

## 与同步 Purser 的关系

调度进程内部使用 `SyncPurser`，因此它同样受“一个进程一个实例”的隔离约束。worker 不要把 `Purser` / `SyncPurser` 对象传给子进程；只传 `SchedulerAddress`。详见[同步 Purser](../guides/sync-facade.md)与[多实例部署](../guides/multi-instance.md)。
