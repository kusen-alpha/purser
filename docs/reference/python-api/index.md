# Python API

`purser` 是 async-first 库：`Purser` 持有池与插件注册表，`ResourcePool` 负责资源与租约，`ResourceAdapter` 描述一类资源的生成与验活。本组页面以嵌入式异步 `Purser` / `ResourcePool` 为主，按对象分组给出公开契约，与 [REST API](../rest-api/index.md) 的端点分组一一对应。

发生冲突时按[参考页权威层级](../index.md)采信源码；本组页面只描述当前实现的公开导出。

## 导入面

顶层 `purser` 导出嵌入式异步主路径、同步门面、内置后端和常用类型：

```python
from purser import (
    # Purser 与资源池
    Purser, ResourcePool,
    # 请求、租约与结果
    AcquireRequest, Lease, BatchLease, ImportReport, HealthReport,
    # 资源与插件
    Resource, ResourceAdapter, PayloadField, ProxyAdapter, AccountAdapter,
    # 枚举与视图
    ResourceState, UsageOutcome, CheckVerdict,
    PoolStats, UsageReport, ValidateJobView,
    # 同步与同机多进程
    SyncPurser, SyncPool, SyncLease, SyncBatchLease, LocalScheduler,
    # 异常
    PurserError, PoolEmpty, PoolExhausted, NoSuitableResource, LeaseConflict,
)
```

`StorageBackend`、`Coordination`、`ResourcePatch`、`TransitionResult` 是公开扩展契约，但不在顶层 `purser.__all__`；需要自定义后端时从 `purser.backends.base` 导入。内置 `MemoryStorage`、`SqliteStorage`、`MySqlStorage`、`MemoryCoordination`、`RedisCoordination` 可从顶层 `purser` 导入。

`Sync*` 的用法见[同步 Purser](../../guides/sync-facade.md)；同机多进程的独立 API 见 [LocalScheduler](../local-scheduler.md)。它们复用同一内核，但门面签名和能力并不完全一致：例如 `SyncPool.release()` 支持 `latency_ms`，`ThreadPool.release()` 与 `RemotePool.release()` 不支持。本组页面的详细签名以嵌入式异步实现为准，其他门面的差异在使用处注明。

## 对象地图

| 对象 | 页面 | 职责 |
| --- | --- | --- |
| `Purser` | [Purser](purser.md) | 生命周期、池注册表、适配器注册、用量报表、备份恢复、事件/审计/指标 |
| `ResourcePool` | [ResourcePool](pools.md) | 导入、查询、遍历、统计等数据面方法，以及 `pool.admin` 管理操作、验活任务、维护扫描 |
| `AcquireRequest` / `Lease` / `BatchLease` | [AcquireRequest / Lease / BatchLease](acquisition.md) | 获取参数、租约字段、续约、归还、批量与强制撤销 |
| `Resource` / `ResourceAdapter` / `PayloadField` | [Resource / ResourceAdapter](resources.md) | 资源字段、类型化 payload、插件钩子与注册 |
| `StorageBackend` / `Coordination` | [StorageBackend / Coordination](backends.md) | 两个数据平面的契约、内置实现与对账语义 |

## 最小生命周期

```python
import asyncio

from purser import AcquireRequest, Purser, UsageOutcome


async def main() -> None:
    async with Purser() as ps:
        ps.register_adapter(adapter=MyAdapter())
        pool = ps.create_pool(name="demo", kind="my-kind")

        await pool.import_resources(
            records=[{"id": "r1", "tags": ["us"]}],
            import_validate="on_import",
        )

        async with pool.lease(
            AcquireRequest(consumer="worker", tags=frozenset({"us"}))
        ) as lease:
            lease.outcome = UsageOutcome.OK
            lease.used = 1


asyncio.run(main())
```

`pool.lease()` 在退出时一定归还；`async with Purser()` 在退出时关闭池、事件总线与审计。首次 `acquire()` / `lease()` / `acquire_many()` 会自动启动运行时，`Purser.start()` 是可选的提前启动。关闭后不支持隐式自动重启；显式 `Purser.start()` 能否重新启动取决于后端，内存后端支持同一实例重新启动，SQLite 等已关闭连接的后端不能直接重开。导入、查询与状态变更始终可用于离线预装数据。

## 通用约定

- **操作者与原因**：嵌入式管理方法的 `operator` 默认 `"system"`；`pool.admin` 的 `ban`、`suspend`、`drain`、`admin_set_state`、`force_revoke`、`update_resource`、`move_resource`、`recall_batch` 的 `reason` 必填。REST 层的 `operator` 来自 `X-Operator` 头（缺省 `api`）。
- **管理操作命名空间**：18 个低频管理操作（`apply_defaults`、`ban`、`drain`、`validate_now`、`force_revoke`、`run_maintenance` 等）位于 `pool.admin`，方法名不变；数据面方法（`acquire`、`release`、`import_resources`、`stats` 等）仍在池顶层。`SyncPool` / `ThreadPool` / `RemotePool` 同样提供 `.admin`。
- **返回值视图**：`stats()`、`report_usage()`、`validate_job*()` 返回 `TypedDict` 视图（运行时是普通 `dict`，可直接 JSON 序列化）；资源对象返回类型化 `Resource` 子类。
- **状态与异常**：状态取值见[状态机](../state-machine.md)，异常清单与 REST 状态码映射见[错误处理](../errors.md)。
- **数据平面**：池的 `storage` / `coordination` 是两个独立平面；丢失语义与多实例风险见[多实例部署](../../guides/multi-instance.md)。
- **请求对象复用**：`acquire()` 不修改传入的 `AcquireRequest`，可安全复用同一个请求对象。
- **同步调用**：不要在 `SyncPurser` 的专属事件循环线程内调用其同步 API；跨进程共享请使用 `LocalScheduler`。

## 参数与方法写法

每个公开符号尽量按同一顺序展开，便于横向对照：

1. **用途**：一句话说明它解决什么问题，以及不适用的情况。
2. **签名**：完整参数、默认值与返回类型。
3. **参数表**：字段、类型、默认值、语义；布尔参数写清 `True` 时的行为。
4. **返回**：返回对象或视图的字段；失败时抛什么。
5. **示例**：最短可运行片段，含必须的 import。
6. **注意**：并发、幂等、跨进程或版本差异等容易踩的点。

新增或改写页面时沿用该顺序；没有的参数表、返回或注意事项可以省略，但不要更换顺序。签名、默认值和异常以源码实现为准，注解与运行时接受值不一致时以实现为准。REST 端点按[端点页](../rest-api/index.md)的“方法 / 路径 / 参数 / 响应 / 错误”结构组织。

## 页面

| 页面 | 内容 |
| --- | --- |
| [Purser](purser.md) | 构造参数、启动/关闭、池管理、报表、备份恢复、运行时属性 |
| [ResourcePool](pools.md) | 导入与去重、查询与遍历、`pool.admin` 管理操作、验活任务、维护扫描 |
| [AcquireRequest / Lease / BatchLease](acquisition.md) | 请求字段、获取失败矩阵、租约与批量、强制撤销 |
| [Resource / ResourceAdapter](resources.md) | `Resource` 字段与属性、`PayloadField`、`ResourceAdapter` 钩子 |
| [StorageBackend / Coordination](backends.md) | 后端选择、两个 ABC 契约、对账 |
