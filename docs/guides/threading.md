# 多线程使用

Purser 的内核是 `asyncio`；在同步代码、线程池任务或旧框架里使用请走 `SyncPurser`。它不是在每个线程里各跑一份内核，而是把一份内核放进专属线程的事件循环，把所有公开方法变成阻塞调用。

## 适用场景

- 爬虫 worker 在线程池里并发租用代理 / 账号；
- 现有同步代码（请求库、ORM、业务框架）不想迁移 `asyncio`；
- 需要事件回调（如 `lease_lost` 告警），希望在独立线程池里执行。

纯异步项目不要用 `SyncPurser`：直接用 `Purser` 就好，见 [租约生命周期](lease-lifecycle.md)。

## 线程模型

```text
调用线程 ──阻塞调用──> SyncPurser 桥接层 ──run_coroutine_threadsafe──> 专属事件循环线程
                                                              │
                                                              └─ 事件回调 → callback_workers 线程池
```

- 每个 `SyncPurser` 独占一个事件循环线程（`purser-sync-loop`）；
- 所有公开方法都是阻塞调用，多个调用线程可以**共享同一个** `SyncPurser` / `SyncPool`；
- 事件订阅回调在固定大小线程池执行（默认 4 个 worker），收到的是深拷贝事件快照；
- 返回值是深拷贝快照：修改返回的 `Resource` 不会写回存储，需要走更新 API。

## 线程池示例

```python
from concurrent.futures import ThreadPoolExecutor

from purser import AcquireRequest, SyncPurser, UsageOutcome

def crawl(task_id: int) -> str:
    with pool.lease(AcquireRequest(consumer=f"crawler-{task_id}", tags=("http",))) as lease:
        proxy = lease.payload
        # ... 这里做真实请求 ...
        lease.outcome = UsageOutcome.OK
    return lease.resource_id

with SyncPurser(callback_workers=8) as ps:
    pool = ps.create_pool("proxies", "proxy")
    pool.import_resources([...], import_validate="on_import")

    with ThreadPoolExecutor(max_workers=8) as ex:
        ids = list(ex.map(crawl, range(80)))
```

要点：

- `with SyncPurser() as ps:` 已经完成 `start()`，退出时自动 `close()`；
- `SyncPool.lease()` 是 `@contextmanager`，退出 `with` 块自动按 `lease.outcome` 归还；
- 同一时刻多个线程可以拿到同一条并发资源（按 `max_slots` 计数）；独占资源拿不到会进入公平等待队列，超时抛异常；
- `SyncLease` / `SyncResourceIterator` / `SyncSubscription` 在拿到它的线程内使用和关闭，不要跨线程传递。

## 线程安全边界

| 对象 | 是否可跨线程共享 | 说明 |
| --- | --- | --- |
| `SyncPurser` | 可以 | 内部统一走专属事件循环，阻塞调用天然串行 |
| `SyncPool` | 可以 | 同上；建池后全局复用 |
| `SyncLease` | 不建议 | 在获取它的线程内完成使用与归还 |
| 返回的 `Resource` / `payload` | 不建议 | 深拷贝快照，跨线程传递会看到过期数据；写回必须走 API |

## 事件回调

```python
def on_event(event):
    if event["type"] == "lease_lost":
        notify(event["data"])

with SyncPurser(callback_workers=4) as ps:
    sub = ps.subscribe(on_event)
    ...
    sub.close()
```

- 回调在 `callback_workers` 线程池执行，**不要在里面做长时间阻塞或耗时操作**；会拖慢事件分发。
- 回调里调用普通同步 API 是允许的，但会占用一个回调线程；不要在回调里 `close()`。
- 回调收到的是深拷贝快照，修改它不会影响内核。

## 常见问题

| 现象 | 原因 | 处理 |
| --- | --- | --- |
| `TimeoutError: purser sync call timed out` | 调用超过超时，协程被取消 | 检查池是否启动、是否有可用资源 |
| `RuntimeError: SyncPurser is closing or closed` | 在 `close()` 之后调用 | 调整生命周期 |
| 回调延迟越来越大 | 回调里阻塞 | 把耗时操作放到自己的队列 / 线程池 |
| 修改了返回的 `Resource` 但不生效 | 返回的是深拷贝快照 | 调用 `update_resource` 等写 API |

API 映射与构造参数见 [同步 Purser](sync-facade.md)。
