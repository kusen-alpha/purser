# 多实例部署

多实例的关键不是“共享一个池”，而是把**存储层**与**协调层**分开，并保证两者可以独立对账。

## 平面组合

| 存储层（system of record） | 协调层（scheduling） | 形态 |
| --- | --- | --- |
| `memory` | `memory` | 单进程，最小依赖 |
| `sqlite` | `memory` | 单机持久化，仍只有一个调度进程 |
| `sqlite` / `mysql` | `redis` | 多实例共享，横向扩容 |
| `mysql` | `memory` | 单实例持久化，使用现有 MySQL 基建 |

## 服务配置

```bash
# 多实例：sqlite + redis
export PURSER_STORAGE=sqlite
export PURSER_SQLITE_PATH=/var/lib/purser/purser.db
export PURSER_COORDINATION=redis
export PURSER_REDIS_URL=redis://localhost:6379
purser serve --host 0.0.0.0

# 或使用 MySQL
export PURSER_STORAGE=mysql
export PURSER_MYSQL_URL=mysql://user:pass@host/db
```

`PURSER_REDIS_URL` 只提供连接串，**不会**自动切换协调层；必须同时设置 `PURSER_COORDINATION=redis`（或 `--coordination redis`）。

嵌入式等价写法：

```python
ps = Purser(
    storage="sqlite",
    coordination="redis",
    sqlite_path="/var/lib/purser/purser.db",
    redis_url="redis://localhost:6379",
)
```

## 重启对账

异常重启后，`Pool.start()` 会：

1. 读取协调端当前存活租约；
2. 按“存活租约槽位之和”重设存储里的占用计数；
3. 全孤儿归零，部分孤儿收敛，资源主数据不变。

但“可对账”不等于“可以随意丢弃”：单实例重启后协调端清空，存储里的占用会按对账规则收敛。迟到 release（token 已不在协调端）是幂等的，不会误扣新持有者槽位。详见[数据平面与丢失语义](../design/data-planes.md)。

!!! danger "多实例下协调记录丢失 = 重复占用"

    多实例场景下，协调记录丢失会让其他实例看不到在途租约，同一资源可能被重复并发使用。生产环境必须把 Redis 当持久化组件部署（开启 AOF 或等价的持久化策略），不能当纯缓存。

## fail-closed

协调端连接或操作失败时抛 `BackendUnavailable`，不会静默降级为本地内存状态。Redis 连接与操作带 5 秒超时，强制 RESP2 以兼容 Redis < 6。

## 隔离约束

- **同一进程内**的两个 `Purser` 实例不能共享 SQLite 文件、MySQL 库、Redis DB 或类型实例；半共享会产生无法对账的租约/槽位视图，库会在装配时抛 `IsolationConflict`。
- **一个实例不能跨进程共享**：不可 pickle；`fork` 继承的实例在子进程首次调用时报错。
- 需要跨进程共享同一资源集合时，使用 `LocalScheduler`，而不是传递 `Purser` 对象。
- `PURSER_ALLOW_SHARED_BACKENDS=1` 只对“一个进程内模拟多主机”的测试脚手架放宽后端身份校验，不会允许跨进程共享实例。

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
