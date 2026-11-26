# 门面族与运行时选择

purser 只有一个内核，但内核可以跑在三种运行时里：当前事件循环、专属线程、专属进程。门面族把三种运行时包装成同一套调用约定，让你按部署形态换一个类，业务代码不用改。

日常只需要两个入口：单进程用 `Purser`，跨进程共享用 `LocalScheduler` + `connect()`；`SyncPurser` / `ThreadPurser` / `ProcessPurser` 是为同步代码、跨线程共享等特殊形态准备的备选项。

## 为什么没有 `AsyncPurser`

`Purser` 本身就是异步的：它的池数据面全是 `async`，直接在当前事件循环里运行。`EmbeddedPurser` 只是 `Purser` 的别名，用来说明“内核嵌入本进程”，两者行为完全一致。

需要换的从来不是“同步/异步”，而是内核放在哪里：

| 门面 | 内核位置 | 管理方法 | 池数据面 | 适用场景 |
| --- | --- | --- | --- | --- |
| `Purser` / `EmbeddedPurser` | 当前事件循环 | 同步 | `async` | 纯异步应用、单进程 |
| `SyncPurser` | 专属事件循环线程 | 同步 | 同步（阻塞） | 同步代码、线程池 |
| `ThreadPurser` | 专属事件循环线程 | 同步 | `async`（自动转线程） | 异步业务代码，但需要在线程间共享同一内核 |
| `ProcessPurser` | 本机子进程（调度进程） | 同步 | `async`（IPC） | 多进程 worker 共享资源池 |

所有门面遵循同一约定：

- 管理方法（`register_adapter`、`create_pool`、`ensure_pool`、`get_pool`、`get_pool_names`，只读属性 `pools`）是同步调用；
- `delete_pool()` 是异步的（要关闭池任务与后端连接）：`Purser` / `ThreadPurser` / `ProcessPurser` 用 `await`，`SyncPurser` 为同步调用；
- 池对象都提供同一套异步数据方法（`acquire` / `release` / `renew` / `stats` / `import_resources` / `generate`、以及 `lease()` 异步上下文）；运维操作统一放在 `pool.admin` 命名空间（`validate_now` / `ban` / `drain` 等 18 项）；
- `RemotePool` 经 IPC 在调度进程中执行数据面与只读目录方法（`query` / `counts` / `get` / `require` / `count_matching` / `batches` / `leases` / `describe`），并在 `connect()` 时做一次 `stats` 往返以填充 `kind` / `profile`；
- 内嵌与线程门面的池提供 `start()` / `close()`（`async`，也可 `async with`）；`RemotePool` 的池生命周期归调度进程，客户端只负责关闭连接（`await pool.aclose()`，或 `async with`）。

### 能力矩阵

| 能力 | `ResourcePool` / `ThreadPool` | `SyncPool` | `RemotePool` |
| --- | --- | --- | --- |
| 数据面（`acquire` / `release` / `renew` / `lease`） | 支持 | 支持（阻塞） | 支持（IPC） |
| 导入（`import_resources` / `generate`） | 支持 | 支持 | 支持（IPC） |
| 只读目录（`query` / `counts` / `get` / `require` / `count_matching` / `batches` / `leases` / `describe`） | 支持 | 支持 | 支持（IPC） |
| 运维（`pool.admin`） | `ResourcePool` 18 项；`ThreadPool` 仅 `ban` / `unban` | 18 项 | 仅 `ban` / `unban`（其余经 `pool_call` 路由） |
| 组合租约（`acquire_many` / bundles） | 支持 | 支持 | 未暴露 |
| 报告 / 备份 / 事件（`report_usage` / `backup` / `restore_backup` / `events`） | 属 `Purser` 管理层 | 属 `SyncPurser` 管理层 | 未提供 |

`Report/backup/event` 能力不随池对象暴露：`ThreadPurser` / `ProcessPurser` 目前只对齐池管理（`register_adapter` / `create_pool` / `ensure_pool` / `get_pool` / `get_pool_names` / `delete_pool`）。需要这些能力时直接使用 `Purser` 或 `SyncPurser`。

```python
from purser import AcquireRequest

async with ProcessPurser() as ps:
    ps.register_adapter(AdapterSpec.of(MyAdapter, region="cn"))
    pool = ps.create_pool("accounts", "myadapter")
    await pool.import_resources([{"id": "a1"}])
    async with pool.lease(AcquireRequest(consumer="worker-1")) as lease:
        ...
```

## ThreadPurser

`ThreadPurser` 复用 `SyncPurser`：内核仍然在 `purser-sync-loop` 线程里，每个数据面调用通过 `asyncio.to_thread` 落到内核线程。适合“业务是 async，但内核要跨线程共享”的场景——例如一个事件循环在跑采集、另一个线程在做运维操作。

- 多个调用线程 / 事件循环可以安全共享同一个 `ThreadPurser`；
- 拿到的是 `SyncLease`：在获取它的线程/协程里使用和归还，不要跨线程传递；
- 事件回调仍在线程池里执行，规则与 [同步 Purser](sync-facade.md) 相同；
- `ThreadPurser` 与 `ThreadPool` 不可 pickle；`close()` 只会关闭自己创建的 `SyncPurser`，attach 模式（`ThreadPurser(sync=...)`）不会关闭外部内核。

```python
async with ThreadPurser() as ps:
    ps.register_adapter(MyAdapter())
    pool = ps.create_pool("accounts", "myadapter")
    await pool.import_resources(records)
    async with pool.lease(consumer="worker") as lease:
        print(lease.payload)
```

## ProcessPurser

`ProcessPurser` 把 `LocalScheduler` 包成 Purser 形状，调度进程仍是唯一权威。它有两种生命周期：

- **owned**（默认）：构造并启动自己的调度进程，`close()` 会关掉它；
- **attach**（`address=`）：连接一个已存在的调度进程，`close()` 只关自己的 client。

```python
# 父进程：owned
async with ProcessPurser(storage="sqlite", sqlite_path="purser.db") as ps:
    ps.register_adapter(AdapterSpec.of(MyAdapter))
    ps.create_pool("accounts", "myadapter")
    address = ps.address.to_env()      # 交给子进程

# 子进程：attach（只含 address，可 pickle）
ps = ProcessPurser(address=address)
await ps.start()
pool = ps.ensure_pool("accounts", "myadapter")   # 已存在则接上，未存在则创建
```

要点：

- `create_pool()` 返回 `purser.RemotePool`，数据面与 `ResourcePool` 同名同语义；
- attach 模式只持有地址与资源类型，因此可以 pickle 后传给子进程或线程池；owned 模式与 `Purser` 一样拒绝 pickle（`IsolationConflict`）；
- 管理调用会按需启动 owned 调度进程，因此可以在 `start()` 之前先 `create_pool()`；`start()` 本身是一次健康检查；
- `register_adapter` 只能传可跨 IPC 的描述：`AdapterSpec`、`{"path": ..., "kwargs": ...}` 映射、零参类，或零参可构造的实例。带 live 状态的 adapters 请在构造时用 `adapters=` 传入。

### AdapterSpec 与远程注册

```python
from purser import AdapterSpec

ps.register_adapter(AdapterSpec.of(MyAdapter, region="cn"))
```

`AdapterSpec.of(Cls, **kwargs)` 要求：

- 类是模块顶层可导入的（`module:qualname`，不允许 `<locals>`）；
- 构造参数可 JSON 序列化。

调度进程收到后会在自己的进程内 `import` 并实例化。IPC 已有 authkey，视为可信通道；不要用它加载不受信任的路径。

## 如何选择

```text
内核要在本进程内，业务是 async        → Purser（默认选择）
内核要在别的进程，多个 worker 共享    → LocalScheduler + connect()
内核要在本进程内，业务是同步代码      → SyncPurser
内核要在本进程内，但要跨线程给 async 用 → ThreadPurser
子进程形态但想要 Purser 同款管理面    → ProcessPurser
```

pcrawl 是这套约定的一个使用者：`PurserRuntime` 做运行模式选择（inline/async → `EmbeddedPurser`，thread/process → `ProcessPurser`），并把 `purser_runtime` 以统一门面交给 Spiders。多线程模式仍走 `ProcessPurser`，以保证与多进程一致的行为；`ThreadPurser` 目前不接入 pcrawl。

## 进程边界 {#process-boundary}

- **同进程内的两个 `Purser` 实例不能共享** SQLite 文件、MySQL 库、Redis DB 或适配器实例；半共享会产生无法对账的租约/槽位视图，库在装配时抛 `IsolationConflict`。同线程共享一个实例是推荐用法。
- **一个实例不能跨进程共享**：不可 pickle；`fork` 继承的实例在子进程首次调用时报错。
- 需要跨进程共享同一资源集合时，用 `LocalScheduler` + `connect()`，不要传递 `Purser` 对象。
- `PURSER_ALLOW_SHARED_BACKENDS=1` 只对“一个进程内模拟多主机”的测试脚手架放宽后端身份校验，不改变跨进程隔离。

相关页面：[同步 Purser](sync-facade.md#threadpool)、[多进程使用](multiprocessing.md)、[LocalScheduler 参考](../reference/local-scheduler.md)。
