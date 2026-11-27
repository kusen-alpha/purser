# 生产检查清单

这是一份可逐项勾选的上线清单，覆盖“选型 → 配置 → 发布 → 观察 → 演练”。它不替代安全页与运维页，而是把散落的约束组合成一条可执行的路径。

## 1. 先确定形态

| 需求 | 推荐组合 | 不要用 |
| --- | --- | --- |
| 单进程脚本、可丢状态 | `memory` + `memory` | 多 worker 共享 |
| 单机、重启不丢资源 | `sqlite` + `memory` | 多进程同时写同一文件 |
| 多实例 / 多主机 | `mysql` 或 `sqlite` + `redis` | 纯 `memory` 协调 |
| 同机多 worker、不想引 Redis | `LocalScheduler` + `spawn` | 把 `Purser` 对象传给子进程 |

判断标准只有一条：**存储层与协调层必须能独立对账**。单进程内 `sqlite + memory` 合法；但只要同一个后端被两个活进程共享、而协调层仍是 `memory`，就属于半共享，库会在启动时拒绝（`IsolationConflict`）。

- [ ] 写清每个池的 `storage` / `coordination`，不要依赖“默认值应该够用”。
- [ ] 多实例时确认所有实例的 `/service` 返回同一套后端。

## 2. 依赖与运行时

- [ ] Python 3.10–3.14；生产镜像固定小版本，升级前运行 `pytest` 与自己的契约测试。
- [ ] 安装正确的 extras：服务端 `purser[server]`，CLI `purser[cli]`，MySQL `purser[mysql]`。
- [ ] 时区与 NTP 已同步。协调层依赖时间判断租约过期；时钟漂移会表现为莫名 `LeaseLost`。
- [ ] Windows 使用 `spawn`：入口加 `if __name__ == "__main__":` 与 `freeze_support()`，详见[平台差异](platform-differences.md)。

## 3. 后端准备

### SQLite

- [ ] `sqlite_path` 是绝对路径，位于本地磁盘；不要放在 SMB/NFS 上。
- [ ] 目录与 `.key` 侧车文件只对服务账号可读写。
- [ ] 已接受“单写者”约束；高频并发写改用 MySQL。
- [ ] 审计如果同库，评估 WAL 文件与备份体积；更推荐独立 `audit_path`。

### MySQL

- [ ] 已建库并使用 `utf8mb4`；应用账号只有该库的必要权限。
- [ ] 资源 id 不超过 255 字符；更长时需要投影。
- [ ] 连接串通过环境变量/密钥管理注入；连接失败会 fail-closed 为 `BackendUnavailable`。

### Redis

- [ ] `PURSER_COORDINATION=redis` 已显式设置；只有 URL 不会切换协调层。
- [ ] 开启 AOF（`appendfsync everysec` 或更强），并纳入高可用与备份。
- [ ] 使用独立 DB（如 `/2`）；当前公开接口不暴露 key 前缀，键统一以 `purser:` 开头，不要与其它业务共用 DB。
- [ ] 已接受 Redis 丢数据 = 在途租约丢失 = 可能重复占用。

## 4. 密钥、认证与审计

- [ ] `PURSER_SECRET_KEY` 固定且已备份；或使用受保护的 SQLite `.key` 文件。验证重启后密文仍可解。
- [ ] `PURSER_API_KEY` 已配置；如需要只读角色，`PURSER_READONLY_KEY` 也已配置并验证写请求返回 `403`。
- [ ] 服务未直接暴露公网；TLS、IP 白名单与网关鉴权在反向代理完成。
- [ ] `/metrics` 没有被公网直接抓取。
- [ ] `audit_path` 已配置；启动后 `/audit/verify` 返回 `ok: true`。
- [ ] 备份文件加密保存，访问有审计；明确知道备份含明文 payload。

细节与威胁模型见[安全模型](security.md)。

## 5. 池参数

按业务压测结果填写，不要只抄默认值：

| 参数 | 检查点 |
| --- | --- |
| `lease_ttl` / `heartbeat_interval` | 心跳约为 TTL 的 1/3；长任务不要用超长 TTL 代替心跳 |
| `wait_timeout` | 等待超时与上游请求超时匹配；超时会抛 `PoolExhausted` |
| `import_validate` | 大批量导入用后台任务；小批量保持默认 `auto` |
| `fail_open` | 生产默认 `false`；只有明确接受“可能重复占用”时才开 |

其余参数（`max_lease_duration`、`max_leases_per_consumer`、`validate_workers`、`unban_mode`、各类 `*_retention`、`event_queue_size`）逐项见[配置参考](../reference/configuration.md)。

`fail_open=True` 不是高可用开关：它让本实例在协调层故障时继续放行，但其他实例看不到这些租约。

## 6. 发布与停机

- [ ] 每个进程只创建一个 `Purser` / `SyncPurser`；多 worker 用多进程 + 共享后端，或 `LocalScheduler`。
- [ ] 服务由进程管理器托管（systemd / supervisord / 容器编排），收到 SIGTERM 后走 uvicorn 的 lifespan 关闭。
- [ ] 停机前停止上游流量，让 `drain_timeout` 有机会等待归还；超时租约会被强制回收。
- [ ] 升级采用滚动重启；确认新实例 `/readyz` 与 `/service` 正常后再摘旧实例。
- [ ] 回滚包版本与配置一起回滚；密钥与后端 schema 不随包回滚。

## 7. 可观测性

- [ ] Prometheus 抓取每个实例的 `/metrics`，并在查询侧按实例聚合（计数器进程内累计，重启归零）。
- [ ] 已为 `fail_open_total`、`feedback_dropped_total`、`lease_lost_total` 建立告警。
- [ ] 已订阅或轮询 `coordination_degraded`、`pool_draining`、`lease_lost` 等关键事件。
- [ ] 日志集中收集，并确认不包含明文 payload 与连接串。
- [ ] 有容量看板：池总量、可分配数、活跃租约、等待者、`avg_health`。

不要依赖 `/readyz` 判断后端健康：它只表示进程能响应并返回池列表，不探测 MySQL/Redis。

## 8. 演练

- [ ] **恢复演练**：用备份在隔离环境恢复，核对资源数量、封禁状态与明文可用性。
- [ ] **密钥轮换演练**：带 `previous_secret_keys` 滚动一次，确认旧密文可读、新写入使用新 `key_id`。
- [ ] **Redis 故障演练**：确认 fail-closed 行为符合预期；若开启 `fail_open`，确认降级告警与恢复步骤。
- [ ] **进程崩溃演练**：杀掉实例后重启，观察启动对账是否把槽位收敛到协调端真实值。
- [ ] **审计校验演练**：模拟篡改一条记录，确认 `/audit/verify` 能发现；再演练整库归档流程。

## 9. 上线后 24 小时

- [ ] 获取失败率、`PoolExhausted`、`NoSuitableResource` 没有持续上升。
- [ ] 租约丢失、反馈丢弃、降级计数保持为 0。
- [ ] 审计链完整，事件没有大量丢弃（`bus.dropped` / 慢消费者告警）。
- [ ] 内存与 SQLite/MySQL 体积增长符合预期。
- [ ] 备份任务已成功执行至少一次，且文件可读。

相关页面：[安全模型](security.md)、[可观测性](observability.md)、[数据保留](data-retention.md)、[性能与容量](performance.md)、[运维操作](operations.md)、[多实例部署](multi-instance.md)。