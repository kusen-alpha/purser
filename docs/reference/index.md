# 参考

参考页描述**公开契约**：签名、字段、端点、错误码与状态迁移。它们不展开“应当如何使用”，那部分在[指南](../guides/index.md)。

## 权威层级

发生冲突时，按以下顺序采信：

1. **源码**：`purser/` 下的公开导出、`core/state.py` 的 `TRANSITIONS`、`ui/server.py` 的路由表。
2. **参考页**：本组页面，描述当前实现的公开契约。
3. **指南页**：解释语义与取舍，可能滞后于实现。

## 页面

| 页面 | 内容 |
| --- | --- |
| [Python API](python-api/index.md) | `Purser` / `ResourcePool` / `Lease` / `ResourceAdapter` 的签名、字段与后端契约 |
| [REST API](rest-api/index.md) | 端点、鉴权、幂等、错误映射 |
| [CLI](cli.md) | 命令、参数、导入文件格式 |
| [配置](configuration.md) | 环境变量、`PoolConfig`、场景预设、`/config/schema`、热更新限制 |
| [内置资源适配器](resource-adapters.md) | `ProxyAdapter` / `AccountAdapter` 的字段、并发与验活 |
| [LocalScheduler](local-scheduler.md) | 同机多进程调度的 API、协议与环境变量 |
| [错误处理](errors.md) | 异常清单与 REST 状态码 |
| [错误码](error-codes.md) | 26 个错误码的类、可重试性与 REST 映射 |
| [状态机](state-machine.md) | 12 个状态、事件与转移表 |
| [事件目录](events.md) | 事件信封、订阅方式、全部事件与载荷 |
| [指标目录](metrics.md) | `/metrics` 与 `/metrics/snapshot`、内置计数器、告警思路 |
| [审计目录](audit.md) | 审计写入、哈希链、查询与校验 |
| [数据模型](data-models.md) | `Resource` / `Lease` / 统计与报表视图的字段形状 |
| [分配器](allocators.md) | 内建策略、评分公式、自定义分配器 |
| [AdapterSpec](adapter-spec.md) | 适配器目标的序列化、远程注册与加载 |
| [协议与门面契约](protocols.md) | `PoolProtocol` / `PurserProtocol` 与各门面能力矩阵 |
| [API 速查](api-cheatsheet.md) | 按任务查 Python / REST / CLI 入口 |

