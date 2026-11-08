# 错误处理

所有异常继承 `PurserError`，可与主 API 一起从顶层导入：

```python
from purser import NotStarted, PoolExhausted, PurserError

try:
    lease = await pool.acquire()
except PoolExhausted:
    ...
except PurserError as exc:
    logger.warning("purser failed: %s", exc)
```

## 异常清单

| 异常 | 含义 | 常见恢复动作 |
| --- | --- | --- |
| `PurserError` | 所有错误的基类 | 最终捕获 |
| `ConfigError` | 池或库配置非法 | 修正 `PoolConfig` / 构造参数 |
| `IsolationConflict` | 实例被跨线程/进程共享；sync 对象不可 pickle | 每个进程/拥有者单独创建实例 |
| `NotStarted` | 池运行时未启动 | `await ps.start()` 或 `async with Purser()` |
| `PoolNotFound` | 池不存在 | 检查池名或先建池 |
| `PoolExists` | 同名池已存在 | 复用已有池 |
| `ResourceNotFound` | 资源不存在 | 检查 `rid` |
| `PoolEmpty` | 池内没有任何资源 | 先导入资源 |
| `PoolExhausted` | 等待超时仍未满足请求；池内资源全部为 `NEW` 时立即抛出 | 扩容、缩短占用、提高超时或先验活 |
| `NoSuitableResource` | 候选存在但全部被请求级校验拒绝 | 调整 `target` / 探测配置 |
| `ResourceBusy` | 定向获取等待超时 | 重试或换资源 |
| `LeaseConflict` | token 不匹配或乐观锁冲突 | 用最新租约状态重试 |
| `LeaseExpired` | 租约已过期 | 续约或重新获取 |
| `LeaseRevokedError` | 租约被管理员强制撤销 | 停止使用该资源 |
| `LeaseLostError` | 租约在持有期间消失（过期/后端重置） | 重新获取，检查心跳 |
| `LeaseLimitReached` | 达到 `use_limit` 或 `max_lease_duration` | 归还并重新获取 |
| `BadState` | 资源状态不允许该操作 | 查询当前状态后重试 |
| `InvalidTransition` | 状态机拒绝 `(state, event)` | 同上；检查操作顺序 |
| `QuotaExceeded` | 用量/周期/目标配额拒绝操作 | `top_up` 或等待周期重置 |
| `BadPayload` | payload 校验或序列化失败 | 修正字段；检查 `coerce` |
| `VersionConflict` | 存储版本 CAS 冲突 | 重新读取后重试 |
| `BackendUnavailable` | 存储后端不可用（fail-closed） | 检查 Redis/MySQL 连接，恢复后重试 |
| `SchedulerUnavailable` | `LocalScheduler` 不可连接、已关闭或启动超时 | 检查调度进程是否存活与地址 |
| `SchedulerTimeout` | 本地调度 IPC 在期限内未响应 | 检查调度进程负载与请求超时 |
| `SchedulerProtocolError` | IPC 地址/消息格式非法或响应 id 不匹配 | 检查 `PURSER_LOCAL_SCHEDULER` 与协议版本 |

`Scheduler*` 异常从 `purser.local` 导入，同样继承 `PurserError`。

## REST 映射

服务端把 `PurserError` 映射为客户端错误而不是 500：

| 状态码 | 条件 |
| --- | --- |
| `404` | `ResourceNotFound`、`PoolNotFound` |
| `422` | `ConfigError`、`BadPayload`，以及字段校验失败 |
| `409` | 其余状态/租约冲突（含 `PoolExhausted`、`LeaseConflict`、`BadState`、`NotStarted`、`IsolationConflict`） |
| `401` / `403` | 鉴权失败或角色权限不足 |

重试建议：

- `409` 先重新读取资源/租约状态，再决定重试；不要盲重试获取请求。
- `422` 是请求本身有问题，重试无意义。
- `BackendUnavailable` 属于基础设施故障；恢复连接后由 `start()` 对账收敛。
