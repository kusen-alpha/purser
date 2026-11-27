# 数据保留

Purser 的“数据”分散在几个不同寿命的容器里。做合规、容量规划或备份策略时，必须分别处理；没有一个全局的“保留 N 天”开关。

## 一张表看清

| 数据 | 存在哪 | 默认寿命 | 如何控制 |
| --- | --- | --- | --- |
| 活跃资源 | 存储层 | 持久 | 删除 / `archive_retention` |
| 归档资源 | 存储层 | 30 天 | `archive_retention`，`archive_gc` 到期硬删除 |
| `LOST` 资源 | 存储层 | 4 小时 | `lost_retention`，之后转 `ARCHIVED` |
| 过期资源 | 存储层 | 永久（默认） | `expired_retention`（默认 `None`，不自动归档） |
| 租约 | 协调层 | `lease_ttl` | 心跳续约；过期由 reaper 回收 |
| 事件 | 进程内存 | 最近 10000 条 | `Purser(event_queue_size=...)` |
| 用量明细 | 进程内存 | 最近 50000 条 | `Purser(usage_ledger_size=...)` |
| 指标 | 进程内存 | 进程生命周期 | 无持久化；外部抓取决定历史 |
| 审计 | 内存或 SQLite | 无自动清理 | `audit_path`；只能整库归档，不能删链首 |
| 幂等结果 | 内存或 Redis | 600 秒 | `Idempotency-Key` 缓存 TTL |
| 备份 | 调用方决定 | 无自动清理 | `backup()` / `GET /backup` 由你保存 |
| 日志 | 部署层 | 无 | 外部日志系统 |

## 资源

### 正常流转

资源不会因为“很久没被使用”而自动删除。离开调度只有几条路径：

- 人工 `ban` / `suspend` / `drain`；
- 验活失败进入 `QUARANTINED`，到期后由 `quarantine_gc` 放回；
- `LOST` 超过 `lost_retention` 转 `ARCHIVED`；
- 绝对过期（`expires_at`）转 `SUSPENDED`；
- 管理员显式归档。

`BANNED` 与 `QUARANTINED` 都保留 payload；只有进入 `ARCHIVED` 并超过保留期才会被擦除。

### 真正的删除

```python
pool = ps.create_pool(
    name="proxies",
    kind="proxy",
    archive_retention=7 * 86400,   # 归档 7 天后删除
    lost_retention=2 * 3600,
)
```

`archive_gc` 到期时先把 payload 置为 `{"erased": True}`，再从存储删除整行，并写一条 `archive_maintenance_delete` 审计。备份里如果还留着旧副本，删除不会追溯备份。

`expire_gc` 是**可选**的：只有显式设置 `expired_retention` 才会启用。它只处理带 `expired_at` 的 `SUSPENDED` 资源，不会碰人工暂停的资源；处理方式是转 `ARCHIVED`，再等 `archive_retention` 到期删除。

```python
pool = ps.create_pool(name="accounts", kind="account", expired_retention=30 * 86400)
```

维护任务默认由后台周期执行，也可手动补执行：

```bash
curl -X POST http://127.0.0.1:8000/pools/proxies/maintenance/archive_gc
curl -X POST http://127.0.0.1:8000/pools/proxies/maintenance/expire_gc
```

### 删除整个池

`DELETE /pools/{pool}?force=true` 会移除池及其资源。这是即时且不可恢复的操作；需要留档时先 `backup()`。活跃租约存在时，不加 `force` 会被拒绝。

## 事件、指标与用量

这三种都是**进程内、重启清空**的观测数据：

- 事件队列与历史各是一个有界环形缓冲，容量来自 `event_queue_size`（默认 10000）。队列满时淘汰最旧事件，`bus.dropped` 递增；REST 历史单次最多返回 1000 条。
- 指标是累计计数器，重启归零；Prometheus 的历史取决于抓取与远端保留策略。
- 用量账本默认保留 50000 条归还记录，超出淘汰最旧；`Purser(usage_ledger_size=...)` 可调。

如果这些数据有合规或计费价值，必须在外部留存：

```bash
# 定时拉取并落库
curl -H "Authorization: Bearer $PURSER_READONLY_KEY" \
  "http://127.0.0.1:8000/reports/usage?group_by=consumer&limit=500"
```

事件同理：订阅后立即写入外部存储，或定期按 `after_seq` 拉取历史。不要把它们当作可靠账本。

## 审计

审计是唯一“默认应该长期保留”的内置数据，但库不替你清理：

- 未配置 `audit_path` 时只在内存，重启即丢。
- 配置后写入独立 SQLite（WAL），启动时把**全部历史加载进内存**。审计量很大时，启动内存与时间会随之增长。
- 没有 TTL、没有自动截断；`verify()` 从链首开始校验，删除开头任意行都会让校验失败。

### 归档方式

需要限制单库大小时，按“整库归档 + 新链”处理，而不是删旧行：

1. 选一个维护窗口，暂停写操作（或短暂停机）。
2. 复制审计 SQLite 文件到归档位置，保留原始文件作为证据。
3. 让服务指向新的 `audit_path`（或移走旧文件后重启）。
4. 归档文件单独保存；需要校验时用对应版本的代码对整库执行 `verify()`。

不要用 `DELETE FROM audit_log WHERE seq < ...` 之类的语句“清理”审计；这会直接破坏哈希链。

## 备份

`backup()` 不在服务端保留历史，也不会自动轮转。你需要自己决定：

- 备份频率与保留代数；
- 是否加密（**备份含明文 payload**）；
- 谁能下载 `GET /backup`（仅 admin）；
- 恢复演练的频率。

恢复时租约不迁移，占用态归一为 `IDLE`；池的存储/协调取自当前部署。详见[Admin](../reference/rest-api/admin.md)。

## 幂等与协调数据

- 幂等结果默认保留 600 秒；单实例存在内存，多实例用 Redis 时共享。过期后同一个 `Idempotency-Key` 会重新执行。
- 租约、槽位与唤醒键都在协调层，随租约过期或归还消失。Redis 的持久化策略决定崩溃后能恢复多少在途状态；它不属于“可清理的缓存”。
- SQLite 的 `.key` 侧车文件与审计库、业务库一样需要纳入备份；丢了它，历史密文不可解。

## 建议策略

| 场景 | 建议 |
| --- | --- |
| 开发/测试 | 全部默认；接受重启清空 |
| 单机生产 | 备份 SQLite + 审计库；`archive_retention` 按合规设定；外部抓指标 |
| 多实例生产 | 备份 MySQL；审计单独归档；用量报表定时入仓；Redis 开 AOF 并独立备份 |
| 含个人数据 | 缩短 `archive_retention`、启用 `expired_retention`；备份加密并设定销毁周期；审计访问留痕 |

相关页面：[运维操作](operations.md)、[审计目录](../reference/audit.md)、[事件目录](../reference/events.md)、[安全模型](security.md)、[生产检查清单](production-checklist.md)。