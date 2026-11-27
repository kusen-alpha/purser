# 教程

教程以完整实操为主线组织：每页从一个能运行的小目标开始，逐步加上并发、失败处理与部署形态。和[指南](../guides/index.md)的区别是：指南解释某个机制，教程负责把多个机制组合成一条可交付的路径。

## 选择入口

| 教程 | 你会得到 | 前置 |
| --- | --- | --- |
| [资源消费](proxy-pool.md) | 一个带验活、失败反馈与并发的抓取循环 | 已安装 `purser`，有可用的代理 |
| [组合租约实战](account-proxy-bundle.md) | 一次获取成套的资源，失败整体回滚 | 读完上一个教程或熟悉 `lease()` |
| [从脚本到服务](script-to-service.md) | 同一套资源在嵌入式、同机多进程、REST 服务三种形态间迁移 | 有一个可运行的单进程脚本 |
| [验活、召回与封禁](validate-and-recall.md) | 导入后的验活策略、批次召回、隔离与解封的完整操作 | 了解资源状态机 |

## 通用约定

- 示例默认使用异步 API。同步代码把 `async with` / `await` 换成 `SyncPurser` 的阻塞方法即可，见[同步 Purser](../guides/sync-facade.md)。
- `host` / `port` / `country` 等是类型化字段；`tags` 只用于调度筛选，不是业务数据。
- 所有示例中的 IP、账号、目标站点都是占位符。`proxy` / `account` 的验活会发起真实网络请求，先确认出口网络与目标可达。
- 首次 `acquire()` 会自动启动运行时；结束时调用 `await ps.close()`。`close()` 之后不能重启，重新创建 `Purser` 即可。

## 建议阅读顺序

1. [安装](../getting-started/install.md) → [嵌入式快速开始](../getting-started/quickstart-async.md)，确认环境可用。
2. 按上表选一条教程完整执行。
3. 遇到获取不到资源时查[资源可用性判定](../guides/availability.md)与[排障](../guides/troubleshooting.md)。
4. 需要字段级细节时回查[参考](../reference/index.md)。