# 上手

这一组页面只回答一个问题：**怎样在 10 分钟内完成第一次租约。**

## 选择入口

| 你的情况 | 从这里开始 |
| --- | --- |
| 在 Python 服务里嵌入调度能力 | [安装](install.md) → [嵌入式快速开始](quickstart-async.md) |
| 想要 REST API / Web 控制台 / CLI | [安装](install.md) → [服务快速开始](quickstart-service.md) |
| 已有同步代码，不想改 async | 先看[嵌入式快速开始](quickstart-async.md)，再看[同步 Purser](../guides/sync-facade.md) |
| 多进程 worker 共享同一批资源 | 先看[多实例部署](../guides/multi-instance.md)，再看 [LocalScheduler](../reference/local-scheduler.md) |

## 最短路径

1. [安装](install.md)：Python ≥ 3.8，按需选择 `server` / `cli` / `mysql` 等 extras。
2. [嵌入式快速开始](quickstart-async.md)：自定义一个恒通过的 `ResourceAdapter`，导入、获取、归还。
3. 理解两个边界：**资源在 `NEW` 时不可分配**，**租约有 TTL 与心跳**。
4. 需要服务化时进入[服务快速开始](quickstart-service.md)。

## 完成之后

- 想理解调度行为：[资源可用性判定](../guides/availability.md)、[公平性与健康度](../guides/fairness-and-health.md)。
- 想接真实代理/账号：[内置资源适配器](../reference/resource-adapters.md)。
- 想上线多实例：[数据平面与丢失语义](../design/data-planes.md)、[多实例部署](../guides/multi-instance.md)。
