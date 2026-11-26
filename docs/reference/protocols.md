# 协议与门面契约

Purser 用三个结构化 `Protocol` 描述“不同运行时应该长什么样”。它们不是基类，不要求继承；只用于类型检查与契约测试。源码在 [`purser/protocols.py`](https://github.com/kusen-alpha/purser/blob/master/purser/protocols.py)。

## `LeaseView`

异步、同步、远程租约共享的**只读**视图：

```python
class LeaseView(Protocol):
    token: str
    pool: str
    resource_id: str
    consumer: str | None
    slots: int
    started_at: float
    expires_at: float
    heartbeat_at: float
    priority: int
    use_limit: int | None
    used: int
    trace_id: str | None
    target: str | None
    payload: dict | None
    resource: Resource | None
    outcome: UsageOutcome

    def expired(self, now: float | None = None) -> bool: ...
```

续约与归还**不在**协议里：异步是 `await pool.renew()`，同步是 `pool.renew()`，签名不同。只读逻辑可以写成一份：

```python
def summarize(lease: LeaseView) -> dict:
    return {
        "resource": lease.resource_id,
        "consumer": lease.consumer,
        "remaining": lease.expires_at - time.time(),
    }
```

`Lease`（嵌入式）、`SyncLease`（同步门面）、`RemoteLease`（IPC）都满足它。注意 `resource` 在跨进程租约上可能为 `None`。

## `PoolProtocol`

池数据面的最小集合：

| 方法 | 说明 |
| --- | --- |
| `acquire(req=None, **kwargs)` | 获取一条租约 |
| `lease(req=None, **kwargs)` | 异步上下文管理器 |
| `release(lease, *, outcome=None, used=None)` | 归还 |
| `renew(lease, *, used=None)` | 续约，返回新的 `expires_at` |
| `stats()` | `PoolStats` |
| `import_resources(records, **kwargs)` | 导入 |
| `generate(specs, **kwargs)` | 生成并同步验活 |

查询、管理操作与事件不在这个最小协议里；具体门面提供更多方法。`SyncPoolProtocol` 是它的阻塞孪生：方法同名，去掉 `async` / `await`，`lease()` 返回同步上下文管理器。

## `PurserProtocol`

管理面：

| 成员 | 说明 |
| --- | --- |
| `adapters` | 适配器注册表 |
| `pools` | 池名到池的只读映射快照 |
| `started` | 运行时是否已启动 |
| `register_adapter(adapter)` | 注册/覆盖适配器 |
| `create_pool(...)` | 严格新建 |
| `ensure_pool(...)` | 幂等取或建 |
| `get_pool(name, *, default=...)` | 只读取 |
| `get_pool_names()` | 池名列表 |
| `delete_pool(name, *, force=False, operator="system")` | 删除 |
| `start()` / `close()` | 生命周期 |

`Purser`、`ThreadPurser`、`ProcessPurser` 共享这组方法。`ProcessPurser` 的 `delete_pool` 通过 IPC 在调度进程执行。

## 能力差异

| 能力 | `Purser` | `SyncPurser` | `ThreadPurser` | `ProcessPurser` | `RemotePool` |
| --- | --- | --- | --- | --- | --- |
| 数据面 | 异步 | 阻塞 | 异步 | 异步 | 异步 |
| 完整 `pool.admin`（18 项） | 是 | 是 | 否（仅 `ban`/`unban`） | 否（仅 `ban`/`unban`） | 否（仅 `ban`/`unban`） |
| 事件订阅 | 本进程 | 本进程 | 本进程 | 调度进程 | 否 |
| 直接持有内核 | 是 | 是 | 是（专属线程） | 否 | 否 |
| 可 pickle | 否 | 否 | 否 | attach 模式可 | 否 |

跨进程的其余管理操作可以经 `client.pool_call` 路由；见 [LocalScheduler](local-scheduler.md)。完整门面选型见[门面族与运行时选择](../guides/facades.md)。

## 运行时检查

协议标了 `@runtime_checkable`，因此可以：

```python
from purser import PoolProtocol

if isinstance(pool, PoolProtocol):
    ...
```

但 `runtime_checkable` 只检查方法/属性是否存在，不检查签名。真正的契约由 `tests/test_facade_contract.py` 守卫。