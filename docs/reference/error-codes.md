# 错误码

每个异常同时具备三层信息：

1. **Python 类型**：用于 `except`。
2. **`code`**：稳定的线上标识，用于 IPC 重建与日志聚合。
3. **`retryable`**：同一请求稍后重试是否可能成功。

```python
from purser import PoolExhausted, is_retryable

try:
    lease = await pool.acquire(consumer="w1")
except PoolExhausted as exc:
    print(exc.code, is_retryable(exc))
```

REST 业务错误**不返回** `code` / `retryable`，只返回 `{"detail": "..."}`；需要结构化错误码时使用 Python API。完整异常说明见[错误处理](errors.md)。

## 错误码全集

<!-- catalog:errors:start -->
| `code` | 异常 | 可重试 | REST | 含义 |
| --- | --- | --- | --- | --- |
| `config_error` | `ConfigError` | 否 | 422 | 池或库配置非法 |
| `isolation_conflict` | `IsolationConflict` | 否 | 422 | pickle / 跨进程传递，或同进程多个实例共享后端或适配器 |
| `not_started` | `NotStarted` | 否 | 409 | 运行时已关闭，不能重启 |
| `pool_not_found` | `PoolNotFound` | 否 | 404 | 池不存在 |
| `pool_exists` | `PoolExists` | 否 | 409 | 同名池已存在 |
| `resource_not_found` | `ResourceNotFound` | 否 | 404 | 资源不存在 |
| `pool_empty` | `PoolEmpty` | 否 | 409 | 池内没有任何资源 |
| `pool_exhausted` | `PoolExhausted` | 是 | 409 | 等待超时仍未满足请求；全为 `NEW` 时立即抛出 |
| `no_suitable_resource` | `NoSuitableResource` | 否 | 409 | 候选全部被请求级校验拒绝 |
| `bundle_acquire_failed` | `BundleAcquireFailed` | 否 | 409 | 组合获取失败，已回滚成员 |
| `bundle_quota_exceeded` | `BundleQuotaExceeded` | 否 | 409 | 组合准入配额拒绝 |
| `resource_busy` | `ResourceBusy` | 是 | 409 | 定向获取等待超时 |
| `lease_conflict` | `LeaseConflict` | 否 | 409 | token 不匹配或乐观锁冲突 |
| `lease_expired` | `LeaseExpired` | 否 | 409 | 租约已过期 |
| `lease_revoked` | `LeaseRevoked` | 否 | 409 | 租约被强制撤销 |
| `lease_lost` | `LeaseLost` | 否 | 409 | 租约在持有期间消失 |
| `lease_limit` | `LeaseLimitReached` | 否 | 409 | 达到 `use_limit` 或 `max_lease_duration` |
| `bad_state` | `BadState` | 否 | 409 | 当前状态不允许该操作 |
| `invalid_transition` | `InvalidTransition` | 否 | 409 | 状态机拒绝 `(state, event)` |
| `quota_exceeded` | `QuotaExceeded` | 否 | 409 | 用量/周期/目标配额拒绝 |
| `bad_payload` | `BadPayload` | 否 | 422 | payload 校验或序列化失败 |
| `version_conflict` | `VersionConflict` | 是 | 409 | 存储版本 CAS 冲突 |
| `backend_unavailable` | `BackendUnavailable` | 是 | 409 | 存储/协调后端不可用（fail-closed） |
| `scheduler_unavailable` | `SchedulerUnavailable` | 是 | — | 本地调度进程不可达或已关闭 |
| `scheduler_timeout` | `SchedulerTimeout` | 是 | — | 本地调度 IPC 超时 |
| `scheduler_protocol` | `SchedulerProtocolError` | 否 | — | IPC 地址/消息格式非法或响应 id 不匹配 |
<!-- catalog:errors:end -->

`Scheduler*` 来自 `purser.local`，不出现在 REST 映射里；它们只发生在 LocalScheduler / `connect()` 路径。

## 重试策略

```python
import asyncio
from purser import is_retryable

async def acquire_with_retry(pool, **kwargs):
    for attempt in range(5):
        try:
            return await pool.acquire(**kwargs)
        except Exception as exc:
            if not is_retryable(exc) or attempt == 4:
                raise
            await asyncio.sleep(min(0.2 * 2 ** attempt, 5))
```

原则：

- **可重试**通常意味着“稍后资源/后端可能恢复”：`PoolExhausted`、`ResourceBusy`、`BackendUnavailable`、`VersionConflict`、`SchedulerUnavailable`、`SchedulerTimeout`。组合租约失败（`BundleAcquireFailed` / `BundleQuotaExceeded`）默认不可重试，需要由调用方决定是否重新编排。
- `PoolExhausted` 重试前先看异常消息里的过滤原因；如果是标签写错或 `NEW` 卡住，重试不会解决。
- `lease_*` 系列不可重试：租约已经失效，必须重新获取，而不是重复归还/续约。
- `version_conflict` 不能原样重放写操作；应重新读取最新状态后重算。
- `scheduler_protocol` 是配置/版本问题，重试无意义。

## 与 REST 状态码的对应

REST 只保留四类：

| 状态码 | 异常 |
| --- | --- |
| 404 | `PoolNotFound`、`ResourceNotFound` |
| 422 | `ConfigError`（含 `IsolationConflict`）、`BadPayload`、字段校验失败 |
| 409 | 其余业务异常，包括 `PoolExhausted`、`Lease*`、`BadState`、`NotStarted`、`BackendUnavailable` |
| 401 / 403 | 鉴权失败或角色权限不足（不是 `PurserError`） |

`409` 不等于“可以盲重试”：先重新读取资源/租约状态。`422` 重试无意义。