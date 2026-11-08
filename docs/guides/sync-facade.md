# 同步 Purser

`SyncPurser` 把异步内核包装成阻塞 API，供同步代码、线程池任务或旧框架使用。它不是“复制一份实现”，而是在独立线程里运行同一个内核。

## 线程模型

```text
调用线程 ──阻塞调用──> SyncPurser 桥接层 ──run_coroutine_threadsafe──> 专属事件循环线程
                                                              │
                                                              └─ 事件回调 → callback_workers 线程池
```

- 每个 `SyncPurser` 独占一个事件循环线程（`purser-sync-loop`）。
- 所有公开方法都是阻塞调用，超时抛 `TimeoutError`。
- 事件订阅回调在固定大小线程池里执行（默认 4 个 worker），参数是深拷贝的事件快照。
- 返回值也是深拷贝快照：修改返回的 `Resource` 不会写回存储，需调用更新 API。

## 快速开始

```python
from purser import AcquireRequest, SyncPurser, UsageOutcome

with SyncPurser() as ps:
    ps.register_type(MyType())
    pool = ps.create_pool("demo", "my-kind")
    pool.import_resources([{"id": "r1"}], import_validate="on_import")

    with pool.lease(AcquireRequest(consumer="worker")) as lease:
        print(lease.resource_id, lease.payload)
        lease.outcome = UsageOutcome.OK

    print(pool.counts(by_name=True))
```

`SyncPurser` 与 `SyncPool` 都支持上下文管理器；`SyncPool.lease()` 是 `@contextmanager`，退出时自动归还。`SyncLease` 本身是快照代理，不是上下文管理器。

## 生命周期

```python
ps = SyncPurser(
    startup_timeout=30.0,   # 等待事件循环就绪
    close_timeout=30.0,     # close 时等待在途调用与内核收尾
    callback_workers=4,     # 事件回调线程数
    storage="sqlite",
    sqlite_path="purser.db",
)
ps.start()                  # 幂等
...
ps.close()                  # 幂等；关闭后调用其他方法抛 RuntimeError
```

构造参数中除 `startup_timeout` / `close_timeout` / `callback_workers` 外，其余原样传给 `Purser`。

## API 映射

| 异步 | 同步 |
| --- | --- |
| `Purser(...)` | `SyncPurser(...)` |
| `await ps.start()` / `await ps.close()` | `ps.start()` / `ps.close()` |
| `ps.create_pool(...)` | `ps.create_pool(...)` → `SyncPool` |
| `async with pool.lease(req) as lease` | `with pool.lease(req) as lease` |
| `await pool.acquire(req)` | `pool.acquire(req)` → `SyncLease` |
| `await pool.acquire_many(n, ...)` | `pool.acquire_many(n, ...)` → `SyncBatchLease` |
| `await pool.release(lease, ...)` | `pool.release(lease, ...)` |
| `await pool.release_consumer(...)` | `pool.release_consumer(...)` |
| `await pool.renew_manual(lease)` | `pool.renew_manual(lease)` |
| `pool.iter_resources(...)` | `pool.iter_resources(...)` → `SyncResourceIterator` |
| `ps.bus.subscribe(listener)` | `ps.subscribe(callback)` → `SyncSubscription` |
| `ps.report_usage(...)` | `ps.report_usage(...)` |
| `await ps.backup()/restore()` | `ps.backup()/restore()` |

`SyncBatchLease` 支持 `with` 与 `release_all()`；`SyncResourceIterator` 支持 `with` 与 `close()`；`SyncSubscription` 支持 `with` 与 `close()`。

## 回调规则

```python
def on_event(event):
    if event["type"] == "lease_lost":
        alert(event["data"])

sub = ps.subscribe(on_event)
...
sub.close()
```

- 回调在回调线程池执行，**不要在里面执行长时间阻塞或耗时操作**；会拖慢事件分发。
- 回调里调用普通同步 API 是允许的，但会占用一个回调线程；不要在回调里调用 `close()`（会抛 `RuntimeError`）。
- 事件以深拷贝快照传入，回调修改它不会影响内核。
- 订阅返回 `SyncSubscription`，不用时 `close()`。

## 隔离与序列化

- `SyncPurser` / `SyncPool` **不可 pickle，也不可跨进程传递**，尝试序列化抛 `IsolationConflict`。
- 每个进程各自创建实例；需要跨进程共享同一资源集合时使用 `LocalScheduler`。
- 不能从专属事件循环线程内部调用同步 API；正常业务代码不会遇到，自定义线程回调时需注意。

## 常见问题

| 现象 | 原因 | 处理 |
| --- | --- | --- |
| `TimeoutError: purser sync call timed out` | 调用超过超时，协程被取消 | 检查池是否启动、是否有可用资源 |
| `RuntimeError: SyncPurser is closing or closed` | 在 `close()` 之后调用 | 调整生命周期 |
| `IsolationConflict` | pickle 或跨进程传递 | 每进程独立创建；共享用 `LocalScheduler` |
| 回调延迟越来越大 | 回调里阻塞 | 把耗时操作放到自己的队列/线程池 |
| 返回的 `Resource` 修改后不生效 | 返回的是深拷贝快照 | 调用 `update_resource` 等写 API |

异步用法见 [租约生命周期](lease-lifecycle.md)。
