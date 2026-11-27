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
| `IsolationConflict` | pickle / 跨进程传递，或同进程多个实例共享后端或适配器 | 每个进程单独创建实例；同线程共享一个实例是推荐用法 |
| `NotStarted` | 运行时已 `close()`，不支持重启（首次获取会自动启动，无需手动 `start()`） | 重新创建实例，或把运行时操作移到 `close()` 之前 |
| `PoolNotFound` | 池不存在 | 检查池名或先建池 |
| `PoolExists` | 同名池已存在 | 复用已有池 |
| `ResourceNotFound` | 资源不存在 | 检查 `rid` |
| `PoolEmpty` | 池内没有任何资源 | 先导入资源 |
| `PoolExhausted` | 等待超时仍未满足请求；池内资源全部为 `NEW` 时立即抛出 | 扩容、缩短占用、提高超时，或先用 `pool.admin.validate_now()` 验活 |
| `NoSuitableResource` | 候选存在但全部被请求级校验拒绝 | 调整 `target` / 探测配置 |
| `ResourceBusy` | 定向获取等待超时 | 重试或换资源 |
| `BundleAcquireFailed` | 跨池组合获取未完成，已获取的成员已回滚释放 | 补充资源、调整约束或改用 `best_effort` |
| `LeaseConflict` | token 不匹配或乐观锁冲突 | 用最新租约状态重试 |
| `LeaseExpired` | 租约已过期 | 续约或重新获取 |
| `LeaseRevoked` | 租约被管理员强制撤销 | 停止使用该资源 |
| `LeaseLost` | 租约在持有期间消失（过期/后端重置） | 重新获取，检查心跳 |
| `LeaseLimitReached` | 达到 `use_limit` 或 `max_lease_duration` | 归还并重新获取 |
| `BadState` | 资源状态不允许该操作 | 查询当前状态后重试 |
| `InvalidTransition` | 状态机拒绝 `(state, event)` | 同上；检查操作顺序 |
| `QuotaExceeded` | 用量/周期/目标配额拒绝操作 | `top_up` 或等待周期重置 |
| `BadPayload` | payload 校验或序列化失败 | 修正字段；检查 `coerce` |
| `VersionConflict` | 存储版本 CAS 冲突 | 重新读取后重试 |
| `BackendUnavailable` | 存储后端不可用（fail-closed） | 检查 Redis/MySQL 连接，恢复后重试 |
| `SchedulerUnavailable` | `LocalScheduler` 不可连接、已关闭或启动超时 | 检查调度进程是否存活与地址 |
| `SchedulerTimeout` | 本地调度 IPC 在期限内未响应 | 检查调度进程负载与请求超时 |
| `SchedulerProtocolError` | IPC 地址/消息格式非法或响应 id 不匹配 | 检查 `PURSER_LOCAL_SCHEDULER` 与消息格式 |

`Scheduler*` 异常从 `purser.local` 导入，同样继承 `PurserError`。

## REST 映射

服务端把 `PurserError` 映射为客户端错误而不是 500：

| 状态码 | 条件 |
| --- | --- |
| `404` | `ResourceNotFound`、`PoolNotFound` |
| `422` | `ConfigError`（含 `IsolationConflict`）、`BadPayload`，以及字段校验失败 |
| `409` | 其余状态/租约冲突（含 `PoolExhausted`、`LeaseConflict`、`BadState`、`NotStarted`、`BackendUnavailable`） |
| `401` / `403` | 鉴权失败或角色权限不足 |

响应体为 `{"detail": "..."}`，不携带 `code` / `retryable` 字段；字段校验失败的 `detail` 是数组。完整约定见 [REST API](rest-api/index.md)。

重试建议：

- `409` 先重新读取资源/租约状态，再决定重试；不要盲重试获取请求。
- `422` 是请求本身有问题，重试无意义。
- `BackendUnavailable` 属于基础设施故障；恢复连接后由 `start()` 对账收敛。
