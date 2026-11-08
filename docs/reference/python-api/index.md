# Python API

`purser` 是 async-first 库：`Purser` 持有池与插件注册表，`ResourcePool` 负责资源与租约，`ResourceType` 描述一类资源的生成与验活。本组页面按对象分组给出公开契约，与 [REST API](../rest-api/index.md) 的端点分组一一对应。

发生冲突时按[参考页权威层级](../index.md)采信源码；本组页面只描述当前版本的公开导出。

## 导入面

顶层 `purser` 导出运行期所需的全部对象：

```python
from purser import (
    # Purser 与资源池
    Purser, ResourcePool,
    # 请求、租约与结果
    AcquireRequest, Lease, BatchLease, ImportReport, HealthReport,
    # 资源与插件
    Resource, ResourceType, PayloadField, ProxyType, AccountType,
    # 枚举与视图
    ResourceState, UsageOutcome, CheckVerdict,
    PoolStats, UsageReport, ValidateJobView,
    # 同步与同机多进程
    SyncPurser, SyncPool, SyncLease, SyncBatchLease, LocalScheduler,
    # 异常
    PurserError, PoolEmpty, PoolExhausted, NoSuitableResource, LeaseConflict,
)
```

`Sync*` 的用法见[同步 Purser](../../guides/sync-facade.md)；同机多进程的独立 API 见 [LocalScheduler](../local-scheduler.md)。两者都是对同一内核的包装，不改变本组页面的语义。

## 对象地图

| 对象 | 页面 | 职责 |
| --- | --- | --- |
| `Purser` | [Purser](purser.md) | 生命周期、池注册表、类型注册、用量报表、备份恢复、事件/审计/指标 |
| `ResourcePool` | [ResourcePool](pools.md) | 导入、查询、遍历、统计、运维动作、验活任务、维护扫描 |
| `AcquireRequest` / `Lease` / `BatchLease` | [AcquireRequest / Lease / BatchLease](acquisition.md) | 获取参数、租约字段、续约、归还、批量与强制撤销 |
| `Resource` / `ResourceType` / `PayloadField` | [Resource / ResourceType](resources.md) | 资源字段、类型化 payload、插件钩子与注册 |
| `StorageBackend` / `Coordination` | [StorageBackend / Coordination](backends.md) | 两个数据平面的契约、内置实现与对账语义 |

## 最小生命周期

```python
import asyncio

from purser import AcquireRequest, Purser, UsageOutcome


async def main() -> None:
    async with Purser() as ps:
        ps.register_type(MyType())
        pool = ps.create_pool("demo", "my-kind")

        await pool.import_resources(
            [{"id": "r1", "tags": ["us"]}],
            import_validate="on_import",
        )

        async with pool.lease(
            AcquireRequest(consumer="worker", tags=frozenset({"us"}))
        ) as lease:
            lease.outcome = UsageOutcome.OK
            lease.used = 1


asyncio.run(main())
```

`pool.lease()` 在退出时一定归还；`async with Purser()` 在退出时关闭池、事件总线与审计。未调用 `Purser.start()` 时，`acquire()` / `lease()` / `acquire_many()` 抛 `NotStarted`，而导入、查询与状态变更仍可用于离线预装数据。

## 通用约定

- **操作者与原因**：嵌入式管理方法的 `operator` 默认 `"system"`；`ban`、`suspend`、`drain`、`admin_set_state`、`force_revoke`、`update_resource`、`move_resource`、`recall_batch` 的 `reason` 必填。REST 层的 `operator` 来自 `X-Operator` 头（缺省 `api`）。
- **返回值视图**：`stats()`、`report_usage()`、`validate_job*()` 返回 `TypedDict` 视图（运行时是普通 `dict`，可直接 JSON 序列化）；资源对象返回类型化 `Resource` 子类。
- **状态与异常**：状态取值见[状态机](../state-machine.md)，异常清单与 REST 状态码映射见[错误处理](../errors.md)。
- **数据平面**：池的 `storage` / `coordination` 是两个独立平面；丢失语义与多实例风险见[数据平面与丢失语义](../../design/data-planes.md)。
- **请求对象复用**：`acquire()` 不修改传入的 `AcquireRequest`，可安全复用同一个请求对象。
- **同步调用**：不要在 `SyncPurser` 的专属事件循环线程内调用其同步 API；跨进程共享请使用 `LocalScheduler`。

## 页面

| 页面 | 内容 |
| --- | --- |
| [Purser](purser.md) | 构造参数、启动/关闭、池管理、报表、备份恢复、运行时属性 |
| [ResourcePool](pools.md) | 导入与去重、查询与遍历、运维动作、验活任务、维护扫描 |
| [AcquireRequest / Lease / BatchLease](acquisition.md) | 请求字段、获取失败矩阵、租约与批量、强制撤销 |
| [Resource / ResourceType](resources.md) | `Resource` 字段与属性、`PayloadField`、`ResourceType` 钩子 |
| [StorageBackend / Coordination](backends.md) | 后端选择、两个 ABC 契约、对账 |

## 版本

当前文档对应 `0.2.0`。Python 侧签名以 `purser/` 源码与 `purser` 顶层导出为准；REST 端点级签名以运行中服务的 `/docs`（OpenAPI）为准。
