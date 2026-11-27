# 排障

按“看到的现象”查找。每条给出最可能的原因和下一步动作；字段级契约仍以[参考](../reference/index.md)为准。

## 先确认三件事

1. **运行形态**：嵌入式、LocalScheduler、REST 服务？异常类型与看日志的位置不同。
2. **后端组合**：`storage` 与 `coordination` 是否符合预期？控制台左栏、`purser status` 或 `/service` 都会显示。
3. **资源状态**：`purser ls <pool>` 或 `GET /pools/{pool}/resources`，先看是 `NEW`、`QUARANTINED` 还是根本没有资源。

## 获取不到资源

**`PoolEmpty`**

池内没有任何资源。

```bash
purser ls proxies
```

先导入资源。注意 `Archived` 资源仍存在但不可分配；用 `state=archived` 过滤确认。

### `PoolExhausted`（等待超时或立即失败）

**等待超时**：异常消息会附带被跳过的过滤原因，例如 `tags=1`、`cooldown=2`、`consumer_limit`。按原因处理：

| 消息片段 | 含义 | 动作 |
| --- | --- | --- |
| `state=new` | 资源停在 `NEW`，没有验活任务 | `admin.validate_now()`，或改用 `import_validate="auto"` |
| `state=validating` | 验活尚未完成 | 等待；检查验活任务是否卡住 |
| `tags=N` | 没有任何资源包含全部请求标签 | 检查标签拼写与导入时的 `tags` |
| `cooldown=N` | 资源或消费者仍在冷却 | 等冷却结束，或确认是否误用了 `RATE_LIMITED` |
| `consumer_limit` | 达到 `max_leases_per_consumer` | 归还部分租约或调高上限 |
| `target=...` | 请求级校验全部拒绝 | 看 `NoSuitableResource` 与站点失败记忆 |

**立即失败**（`timeout <= 0` 或当前无法立即满足）：

- 池内资源全部是 `NEW` 且没有进行中的验活任务。这是 `import_validate="none"` 的典型表现。
- `timeout <= 0` 且当前无法立即满足。
- `scheduler="token_bucket"` 且令牌不足。

### `NoSuitableResource`

候选存在，但全部被请求级校验（`probe` / `on_request_check`）拒绝。检查：

- `probe` 目标是否可达、资源侧是否支持该协议；
- 站点失败记忆是否把目标判死（`site_failure_ttl`）；
- 是否把“目标站点错误”误报成 `network_error`，导致资源被隔离。

**`ResourceBusy`**

定向获取（`rid`）等待超时，资源仍被其他租约占用。查看当前租约：

```python
print(await pool.leases(rid=rid))
```

不要对定向获取做无退避的紧循环重试。

## 启动与运行时

**`NotStarted`**

运行时已经 `close()`，或者你显式调用了已关闭池的方法。同进程不支持重启；新建 `Purser` 实例。首次 `acquire()` 会自动启动，通常不需要手动 `start()`。

**`IsolationConflict`**

同一个 `Purser` 实例被跨线程/进程共享，或 sync 对象被 pickle。每个进程/拥有者单独创建实例；同机多进程用 `LocalScheduler` + `connect()`，见[多进程使用](multiprocessing.md)。

### `BackendUnavailable`

存储或协调后端不可用，默认 fail-closed（`fail_open=False`）。检查：

```bash
purser status          # 服务健康
curl http://127.0.0.1:8000/readyz
```

- Redis/MySQL 连接串、网络、认证；
- Redis 是否配置了持久化（多实例下不能当纯缓存）；
- 恢复连接后由启动对账收敛，不要在故障期间反复重建池。

### 调度器通信异常

只出现在 LocalScheduler / `connect()` 路径，涉及 `SchedulerUnavailable` / `SchedulerTimeout` / `SchedulerProtocolError`：

| 异常 | 常见原因 |
| --- | --- |
| `SchedulerUnavailable` | 调度进程未启动、已关闭、启动超时，或地址不对 |
| `SchedulerTimeout` | 调度进程过载、请求超时；检查池是否被大量同步操作阻塞 |
| `SchedulerProtocolError` | `PURSER_LOCAL_SCHEDULER` 地址格式错误、消息格式非法或响应 id 不匹配 |

确认 worker 与调度进程使用同一个地址；Windows 下地址是命名管道，跨用户/跨会话不可见。

## 租约

### 冲突、失联与到期

**`LeaseConflict`**

token 不匹配或乐观锁冲突。通常发生在：

- 用旧租约（已被撤销/过期/其他实例归还）再次归还；
- 同一资源被并发强制撤销；
- 存储侧槽位与协调层 token 对不上。

先重新读取租约状态，再决定重试；不要盲目重复 `release()`。

**`LeaseLost`**

租约在持有期间消失：协调层重置、TTL 到期、reaper 判定失联。检查心跳是否仍在运行、进程是否长时间阻塞事件循环、Redis 是否被清空。重新获取资源。

**`LeaseExpired` / `LeaseLimitReached`**

- `LeaseExpired`：租约已过期，续约或重新获取。
- `LeaseLimitReached`：达到 `use_limit` 或 `max_lease_duration`；归还有关租约后重新获取。心跳续约遇到它会把租约视为终态回收。

### 归还成功但槽位未释放

如果 token 已不在协调端，`release()` 是幂等成功的，且不会扣减存储槽位——避免影响新持有者。确认不是重复归还。

## REST / 控制台

| 状态码 | 首先检查 |
| --- | --- |
| `401` | 是否带了 `Authorization: Bearer <key>`；密钥是否与启动时一致 |
| `403` | 只读密钥执行写操作；或非 admin 读取密文 |
| `404` | 池/资源/租约 id 是否正确；池是否在另一个实例上 |
| `409` | 状态/租约冲突；先读状态再重试，见上 |
| `422` | 请求字段非法或字段校验失败；`detail` 可能是数组 |

业务错误体统一是 `{"detail": "..."}`，不携带 `code` / `retryable`。需要结构化错误码时用 Python API 的 `error.code` / `is_retryable()`。

控制台空白或“未授权”：

- 同时在浏览器里清掉旧的访问密钥再重试；
- 配了密钥时 SSE `EventSource` 无法带 header，控制台会退回历史轮询，属正常现象；
- 反向代理是否剥掉了 `Authorization` 或 SSE 的 `text/event-stream`。

## 导入与验活

### 导入后不可分配或成批隔离

**导入成功但资源一直不可分配**：默认 `auto` 下大批量会转后台任务。查 `batch_id` 对应任务：

```python
print(pool.validate_job(job_id=report.batch_id))
```

若状态长期 `running`，检查出口网络、`validate_workers` 与探测超时。若显式用了 `none`，资源会停在 `NEW`。

**资源成批进入隔离**：通常是探测目标不可达，而不是资源全坏。`ProxyAdapter` 在所有探针同时不可达时会暂停判定，但目标持续失败仍会隔离。先手动验证一条：

```python
await pool.admin.validate_check(rid=rid, check="check_name")
```

确认出口网络、DNS、代理认证与目标站点。

### 解封后再次封禁

`unban_mode="auto_after_ttl"` 或 `mixed` 会在 TTL 后自动解封；如果上游仍未恢复，会立即再次失败。把 `unban_after` 调到与供应商恢复时间匹配，或改 `manual_only`。

## 存储与持久化

**SQLite `database is locked`**

多个进程同时写同一个 SQLite 文件。SQLite 只适合单实例；多实例改用 MySQL，或让每个实例使用独立 SQLite 文件 + 共享 Redis 协调（仍要确认没有多个实例写同一文件）。

**重启后租约丢失**

- `coordination="memory"`：租约只存在于进程内，重启即丢。
- Redis 未开持久化或实例被清空。
- 多实例没有共享 `PURSER_COORDINATION=redis`。

存储层只保存资源主数据；租约在协调层。两者要一起配置，见[多实例部署](multi-instance.md)。

### 密钥错误或无法解密

- 所有实例是否使用同一个 `PURSER_SECRET_KEY`？
- SQLite 下的 `<db>.key` 是否被删除、替换或不可读？
- 轮换时旧密钥是否放进了 `previous_secret_keys`？

密钥文件存在但不可读/损坏会直接抛 `ConfigError`，不会静默生成新钥。

## 性能与阻塞

- 获取路径上的同步 I/O 会拖垮整个事件循环；CPU 密集任务放线程池。
- 大量同步管理操作会占满 LocalScheduler 的 IPC 队列，表现为 worker 侧 `SchedulerTimeout`。
- 事件监听器超时会被暂停 10 秒；不要在监听器里做长任务。
- 验活并发过高会打满出口带宽，表现为大量超时与隔离。

更多容量规划见[性能与容量](performance.md)。

## 仍然无法定位

1. 打开 `/metrics` 与 `/metrics/snapshot`，看是否有持续增长的失败计数。
2. 导出事件历史与审计：
   ```bash
   curl "http://127.0.0.1:8000/pools/proxies/events/history?limit=200"
   curl "http://127.0.0.1:8000/audit?limit=200"
   curl "http://127.0.0.1:8000/audit/verify"
   ```
3. 最小化复现：单池、单资源、`import_validate="none"` + 手动 `validate_now()`，排除并发与标签因素。
4. 附上版本、后端组合、异常类型与 `trace_id` 提问，见[常见问题](faq.md)。