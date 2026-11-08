# 参考

参考页描述**公开契约**：签名、字段、端点、错误码与状态迁移。它们不展开“应该怎么用”，那部分在[指南](../guides/index.md)。

## 权威层级

发生冲突时，按以下顺序采信：

1. **源码**：`purser/` 下的公开导出、`core/state.py` 的 `TRANSITIONS`、`ui/server.py` 的路由表。
2. **参考页**：本组页面，描述当前版本的公开契约。
3. **指南与设计页**：解释语义与取舍，可能滞后于实现。

归档的 `design-doc.md` / `feasibility.md` 只记录历史决策，不作为行为依据。

## 页面

| 页面 | 内容 |
| --- | --- |
| [Python API](python-api/index.md) | `Purser` / `ResourcePool` / `Lease` / `ResourceType` 的签名、字段与后端契约 |
| [REST API](rest-api/index.md) | 端点、鉴权、幂等、错误映射 |
| [CLI](cli.md) | 命令、参数、导入文件格式 |
| [配置](configuration.md) | 环境变量、`PoolConfig`、预留字段、热更新限制 |
| [内置资源类型](resource-types.md) | `ProxyType` / `AccountType` 的字段、并发与验活 |
| [LocalScheduler](local-scheduler.md) | 同机多进程调度的 API、协议与环境变量 |
| [错误处理](errors.md) | 异常清单与 REST 状态码 |
| [状态机](state-machine.md) | 12 个状态、事件与转移表 |

## 版本

当前文档对应 `0.2.0`。REST 端点级签名以运行中服务的 `/docs`（OpenAPI）为准。
