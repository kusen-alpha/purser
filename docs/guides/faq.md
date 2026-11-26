# 常见问题

每条只给最短答案，并指向展开该话题的页面；按现象排查看[排障](troubleshooting.md)。

## 定位

**Purser 是代理池吗？**

不只是。它把代理、账号和自定义资源统一成“可租用的资源”，负责状态、健康、配额、隔离与公平分配。代理和账号只是内置的两个适配器；你也可以接入短信号、浏览器实例、API key 等。

**它和任务队列 / 连接池有什么区别？**

任务队列调度“任务”，Purser 调度“资源”。它关心的是资源当前能不能用、被谁占用、失败后要不要隔离，以及多消费者之间怎么公平分配；不执行任务本身，也不重试业务逻辑。

**必须用异步吗？**

内核是异步优先。业务侧可以用 `SyncPurser` / `ThreadPurser` / `ProcessPurser`，或在服务模式下发 HTTP。选型见[门面族与运行时选择](facades.md)。

## 安装与版本

**支持哪些 Python 版本？**

3.10–3.14。见[版本与弃用政策](../reference/stability.md)。

**为什么没有 `AsyncPurser`？**

`Purser` 本身就是异步门面；再包一层只会增加命名负担。`SyncPurser` 是唯一需要额外门面的场景。见[门面族](facades.md)。

**`pip install purser` 为什么没有 REST / CLI？**

服务端依赖是可选的：`pip install "purser[server,cli]"`。嵌入式内核只需要基础安装。

## 资源与状态

**导入成功为什么获取不到？**

最常见是 `import_validate="none"`，资源停在 `NEW`。改用默认 `auto`，或调用 `pool.admin.validate_now()`。批量导入则用 `batch_id` 查看后台任务进度。见[排障](troubleshooting.md)。

**`tags` 和 `required_tag` 有什么区别？**

`tags` 要求资源包含**全部**给定标签；`required_tag` 是单个硬约束，常用于组合租约中由锚点资源派生。标签只用于调度筛选，不是业务字段。

**一个资源能被多条租约同时占用吗？**

可以，取决于 `max_slots`。`max_slots=1` 是独占（`FULL`），大于 1 时是共享（`ACTIVE`）。同一消费者重复获取同一资源默认返回已有租约，除非 `allow_duplicate=True`。

**`stateless` 模式是什么？**

`acquire_mode="stateless"` 只取样、不占槽位，也不需要归还；`release()` 仅用于反馈用量与健康。适合只读的随机取样场景，不适合需要互斥的资源。

**资源被封后会丢数据吗？**

不会。`BANNED` / `QUARANTINED` / `SUSPENDED` 都保留资源；只有 `archive_gc` 会在 `archive_retention` 到期后擦除 payload 并删除。备份/恢复也会保留封禁与配额状态。

**可以手动把资源改成任意状态吗？**

只有 `admin_set_state` 允许的目标状态，且必须符合状态机；非法迁移抛 `InvalidTransition` / `BadState`。常规操作优先用 `ban` / `suspend` / `drain` 等具名方法。

## 租约

**租约过期后资源会怎样？**

reaper 回收槽位并发布 `lease_expired`；资源回到可分配状态（或按隔离/配额策略转移）。持有者再使用时可能遇到 `LeaseLost`。

**为什么 `release()` 返回 `True`，但我不确定资源是否真的释放了？**

token 已不在协调端时，归还是幂等成功的，且不扣减存储槽位，以避免影响新持有者。用 `pool.leases(rid)` 确认当前占用。

**长任务怎么保活？**

心跳默认按 `lease_ttl/3` 自动续约；也可以用 `renew()` 显式续约。`max_lease_duration` 是总寿命上限，达到后停止续约。不要用超长 TTL 代替心跳。

**能重启已关闭的 `Purser` 吗？**

不能。`close()` 后运行时操作抛 `NotStarted`；新建实例即可。管理面操作（导入、查询）不需要启动。

## 部署与后端

**memory 后端能用于生产吗？**

单进程、可接受重启丢状态时可以。多实例必须使用共享协调层（Redis）；存储层选 SQLite（单实例）或 MySQL（多实例）。

**SQLite 能多实例共享吗？**

不建议。多个进程写同一个文件会锁冲突。多实例用 MySQL；或每个实例独立 SQLite + 共享 Redis，但要明确各实例只写自己的库。

**Redis 只是缓存吗？**

不是。协调层保存租约、槽位、心跳与唤醒；丢失会破坏在途状态。必须开启持久化。

**怎么水平扩容消费者？**

消费者是无状态的，直接加进程/实例即可；关键是 `consumer` 唯一、共享同一个协调层。调度是去中心化的，不需要专门的 master。

**能按业务拆池吗？**

可以，而且推荐。不同供应商、不同地区、不同配额策略分别建池；跨池成套获取用[组合租约](../tutorials/account-proxy-bundle.md)。

## 安全

**备份里是明文密钥吗？**

是。`backup()` 为了让恢复能精确重现资源，包含明文 payload。备份文件必须按密钥材料管理。见[安全模型](security.md)。

**为什么没配密钥也能读明文？**

未设置 `PURSER_API_KEY` / `PURSER_READONLY_KEY` 时，服务按匿名 admin 处理，这是为了本地开发方便。对共享网络暴露前必须配置密钥或外层鉴权。

**payload 加密保护什么？**

保护存储层里的敏感字段（密码、cookie、token）不以明文落盘；不保护进程内存、日志或备份。密钥丢失会导致密文不可解，轮换时用 `previous_secret_keys` 过渡。

**审计能防篡改吗？**

审计是 SHA-256 哈希链；`audit.verify()` / `/audit/verify` 能发现链上任意条目被修改。它不能阻止有写权限的人删除整个文件；需要外部日志归档做第二层保护。

## 运维

**报表能长期保存吗？**

不能。报表来自有界内存账本，重启清空；需要长期留存时定时拉取 `/reports/usage` 落库。见[数据保留](data-retention.md)。

**事件丢了怎么办？**

事件总线是有界环形缓冲；慢消费者会被丢弃/暂停，不会拖垮发布方。需要可靠投递时订阅后立即落库，或定期拉取 `/events/history`。

**怎么优雅停机？**

设置 `drain_timeout`，服务收到 SIGTERM 后等待在途租约归还；超时强制回收。IPC 形态调用 `sched.close()`。见[多实例部署](multi-instance.md)。

**Windows 上有什么不同？**

多进程使用 `spawn` 与命名管道；worker 入口必须有 `if __name__ == "__main__":`；信号与文件锁行为与 Unix 不同。见[平台差异](platform-differences.md)。

## 还是没解决

按[排障](troubleshooting.md)的顺序收集：后端组合、资源状态、异常类型、`trace_id`、`/metrics` 与审计片段。提 issue 时附上最小复现与版本。