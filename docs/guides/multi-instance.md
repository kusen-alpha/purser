# 多实例部署

多实例的关键不是“共享一个池”，而是把**存储层**与**协调层**分开，并保证两者可以独立对账。

## 平面组合

存储层与协调层的组合、各自保存什么、丢失后能否对账，统一见[数据平面与丢失语义](../design/data-planes.md)；环境变量与逐场景配置见[后端配置示例](backend-configuration.md#multi-instance-setup)。

`PURSER_REDIS_URL` 只提供连接串，**不会**自动切换协调层；多实例必须同时设置 `PURSER_COORDINATION=redis`（或 `--coordination redis`）。

## 重启对账 {#restart-reconcile}

异常重启后，`Pool.start()` 会：

1. 读取协调端当前存活租约；
2. 按“存活租约槽位之和”重设存储里的占用计数；
3. 全孤儿归零，部分孤儿收敛，资源主数据不变。

但“可对账”不等于“可以随意丢弃”：单实例重启后协调端清空，存储里的占用会按对账规则收敛。迟到 release（token 已不在协调端）是幂等的，不会误扣新持有者槽位。详见[数据平面与丢失语义](../design/data-planes.md)。

!!! danger "多实例下协调记录丢失 = 重复占用"

    协调记录丢失会让其他实例看不到在途租约，同一资源可能被重复并发使用。生产环境必须把 Redis 当持久化组件部署，完整语义见[数据平面：多实例丢失](../design/data-planes.md#redis-persistence)。

## fail-closed

协调端连接或操作失败时抛 `BackendUnavailable`，不会静默降级为本地内存状态。Redis 连接与操作带 5 秒超时，强制 RESP2 以兼容 Redis < 6。

需要“协调端抖动时本实例继续工作”的场景，可以显式开启 `PoolConfig(fail_open=True)`：注册、续约、归还遇到 `BackendUnavailable` 时不再抛错，而是记录 `pool.<name>.fail_open_total`、发布 `coordination_degraded` 事件，并按本地状态继续。此时其他实例看不到本实例的新租约，可能重复占用同一资源——只建议在同实例独占资源、或短时抖动可接受的部署中开启；默认仍是 fail-closed。

## 优雅停机

`PoolConfig(drain_timeout=...)` 大于 0 时，`close()` 会先停止接收新获取，等待在途租约归还最多 `drain_timeout` 秒；到期仍未归还的租约由 `force_revoke(reason="drain_timeout")` 强制回收。默认 `0.0` 表示立即关闭。

## 隔离约束

同一进程内的两个实例不能共享后端或适配器，实例也不能跨进程传递；完整规则与 `PURSER_ALLOW_SHARED_BACKENDS` 的适用范围见[门面族：进程边界](facades.md#process-boundary)。跨进程共享同一资源集合用 `LocalScheduler`。

## 锁与重试

- SQLite 写路径对 `database is locked` 做最多 3 次短重试；仍有高频并发写时优先 MySQL。
- 版本 CAS 冲突抛 `VersionConflict`；调用方重新读取后重试。
- 多实例下租约状态以协调端为准；本地 `pool._leases` 只是缓存。

## 运维检查

```bash
curl http://127.0.0.1:8000/readyz
curl http://127.0.0.1:8000/service
curl http://127.0.0.1:8000/audit/verify
```

`/service` 返回当前 `storage` / `coordination` 与池列表，用于确认每个实例确实接入了同一套后端。

相关页面：[后端配置示例](backend-configuration.md)、[数据平面与丢失语义](../design/data-planes.md)、[生产检查清单](production-checklist.md)、[可观测性](observability.md)。
