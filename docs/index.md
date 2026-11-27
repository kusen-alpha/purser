# Purser

<style>
.md-content .md-typeset h1 { display: none; }
</style>

<div class="purser-hero">
  <img class="purser-hero__logo" src="assets/logo.svg" alt="">
  <p class="purser-hero__title">Purser</p>
  <p class="purser-hero__tagline">通用资源调度库：把任意可租用对象统一为资源，按状态、健康度、配额与公平性规则完成获取、验活、封禁与归还。</p>
  <p class="purser-hero__actions">
    <a class="md-button md-button--primary" href="getting-started/install/">开始使用</a>
    <a class="md-button" href="getting-started/quickstart-async/">快速开始</a>
    <a class="md-button" href="reference/">API 参考</a>
  </p>
  <p class="purser-hero__badges">
    <img src="https://img.shields.io/badge/python-3.10%2B-009688" alt="Python 3.10+">
    <img src="https://img.shields.io/badge/license-MIT-009688" alt="MIT License">
  </p>
</div>

## 为什么选择 Purser

<div class="grid cards" markdown>

-   :material-cube-outline:{ .lg .middle } __统一资源模型__

    ---

    任意资源都收敛到 `Resource` 与 `ResourceAdapter`，调度内核不感知业务差异。

    [:octicons-arrow-right-24: 自定义资源适配器](guides/custom-resource-adapter.md)

-   :material-shield-check-outline:{ .lg .middle } __状态机与验活__

    ---

    12 个生命周期状态，配合验活、隔离、封禁、配额与到期策略，过滤掉不可用资源。

    [:octicons-arrow-right-24: 资源可用性判定](guides/availability.md)

-   :material-scale-balance:{ .lg .middle } __租约与公平调度__

    ---

    TTL、心跳与回收器保证异常退出后可以收敛；优先级配额与加权随机避免单一消费者独占。

    [:octicons-arrow-right-24: 租约生命周期](guides/lease-lifecycle.md)

-   :material-database-cog-outline:{ .lg .middle } __可插拔后端__

    ---

    存储层 `memory / sqlite/mysql`，协调层 `memory / redis`；切换后端不需要修改业务代码。

    [:octicons-arrow-right-24: 后端配置示例](guides/backend-configuration.md)

</div>

## 最小示例

```python title="main.py"
import asyncio
from purser import Purser


async def main():
    ps = Purser()
    pool = ps.create_pool(name="resources", kind="custom")
    await pool.import_resources(records=[...])  # (1)!

    async with pool.lease(consumer="worker") as lease:  # (2)!
        print(lease.rid, lease.payload)


asyncio.run(main())
```

1. `[...]` 代表一组资源记录；小批量默认同步验活，导入返回即可分配。完整可运行示例见[嵌入式快速开始](getting-started/quickstart-async.md)。
2. 首次获取会自动启动运行时；低频管理操作在 `pool.admin` 命名空间（如 `await pool.admin.ban(...)`）。

## 文档地图

### 集成方

| 目标 | 入口 |
| --- | --- |
| 首次完成接入 | [安装](getting-started/install.md) → [嵌入式快速开始](getting-started/quickstart-async.md) |
| 完整运行示例 | [教程：资源消费](tutorials/proxy-pool.md) |
| 成套获取多个资源 | [教程：组合租约实战](tutorials/account-proxy-bundle.md) |
| 理解资源为何不可获取 | [资源可用性判定](guides/availability.md) |
| 理解租约、验活、强制撤销 | [租约生命周期](guides/lease-lifecycle.md) |
| 接入自定义资源 | [自定义资源适配器](guides/custom-resource-adapter.md) |
| 在同步代码中调用 | [同步 Purser](guides/sync-facade.md) |
| 查询签名、端点与错误码 | [参考](reference/index.md) |

### 运维与平台方

| 目标 | 入口 |
| --- | --- |
| 启动服务 | [服务快速开始](getting-started/quickstart-service.md) |
| 使用控制台完成日常操作 | [Web 控制台](guides/web-console.md) |
| 部署多实例 | [多实例部署](guides/multi-instance.md) |
| 退役、备份、审计 | [运维操作](guides/operations.md) |

## 权威层级

源码是最终权威；[参考](reference/index.md)描述公开契约；指南页解释语义，可能滞后于实现。行为冲突时按此顺序采信。
