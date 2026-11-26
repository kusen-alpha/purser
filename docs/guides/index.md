# 指南

指南按“任务”组织，而不是按模块组织。先读两页概念，再按需要跳到具体做法。

## 先读这两页

| 页面 | 解决什么问题 |
| --- | --- |
| [资源可用性判定](availability.md) | 为什么这条资源当前获取不到？过滤链的 13 个检查 |
| [租约生命周期](lease-lifecycle.md) | 获取、续约、归还、强制撤销、失联的完整语义 |

## 按任务查找

| 任务 | 页面 |
| --- | --- |
| 导入资源、决定何时验活 | [导入与验活](import-and-validate.md) |
| 一次拿整套（账号+代理）、约束配对、准入配额 | [组合租约](bundle-leases.md) |
| 调公平性、健康度、隔离与配额 | [公平性与健康度](fairness-and-health.md) |
| 部署多实例、共享 Redis | [多实例部署](multi-instance.md) |
| 选择并配置存储层 / 协调层 | [后端配置示例](backend-configuration.md) |
| 在多线程里并发使用 | [同步 Purser：线程池示例](sync-facade.md#threadpool) |
| 同主机多进程共享资源池 | [多进程使用](multiprocessing.md) |
| 在同步代码里调用 | [同步 Purser](sync-facade.md) |
| 在嵌入式 / 线程 / 进程运行时之间切换 | [门面族与运行时选择](facades.md) |
| 接入代理/账号之外的新资源 | [自定义资源适配器](custom-resource-adapter.md) |
| 在浏览器里建池、加资源、看租约 | [Web 控制台](web-console.md) |
| 退役、备份、审计、轮换密钥 | [运维操作](operations.md) |
| 威胁模型、认证、字段加密、密钥轮换 | [安全模型](security.md) |
| 上线前逐项检查部署、密钥、告警与演练 | [生产检查清单](production-checklist.md) |
| 搭指标、事件、审计与告警 | [可观测性](observability.md) |
| 决定资源、事件、审计、备份保留多久 | [数据保留](data-retention.md) |
| 做容量估算、调参、跑基准 | [性能与容量](performance.md) |
| 处理 Windows/POSIX 的进程、IPC、文件锁差异 | [平台差异](platform-differences.md) |
| 从 0.4 升级到 0.6 | [版本迁移](migration-0.4-to-0.5.md) |
| 按现象定位故障 | [排障](troubleshooting.md) |
| 快速回答常见疑问 | [常见问题](faq.md) |
| 查中英文术语 | [术语表](glossary.md) |

## 概念顺序

建议按以下顺序建立心智模型：

1. **资源状态**：12 个状态中只有 `IDLE` / `ACTIVE` 可分配，见[状态机](../reference/state-machine.md)。
2. **租约**：占用槽位的唯一凭据，TTL/心跳/reaper 保证回收。
3. **过滤与打分**：先硬约束，再 Top-K 加权随机，最后请求级校验。
4. **两个数据平面**：存储层不能丢，协调层在多实例下必须持久化，见[数据平面与丢失语义](../design/data-planes.md)。

需要字段级契约时查[参考](../reference/index.md)；需要“为什么这样设计”时查[设计与内部](../design/index.md)。
