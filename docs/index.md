# Purser

<style>
.md-content .md-typeset h1 { display: none; }
</style>

<div class="purser-hero">
  <img class="purser-hero__logo" src="assets/logo.svg" alt="">
  <p class="purser-hero__title">Purser</p>
  <p class="purser-hero__tagline">通用采集资源调度库：统一管理代理、账号和自定义资源，<br>按状态、健康度、配额与公平性规则完成获取、验活、封禁与归还。</p>
  <p class="purser-hero__actions">
    <a class="md-button md-button--primary" href="getting-started/install/">开始使用</a>
    <a class="md-button" href="getting-started/quickstart-async/">快速开始</a>
    <a class="md-button" href="reference/">API 参考</a>
  </p>
  <p class="purser-hero__badges">
    <img src="https://img.shields.io/badge/version-0.2.0-009688" alt="版本 0.2.0">
    <img src="https://img.shields.io/badge/python-3.8%2B-009688" alt="Python 3.8+">
    <img src="https://img.shields.io/badge/license-MIT-009688" alt="MIT License">
  </p>
</div>

## 为什么用 Purser

<div class="grid cards" markdown>

-   :material-cube-outline:{ .lg .middle } __统一资源模型__

    ---

    代理、账号和自定义资源都收敛到 `Resource` 与 `ResourceType`，调度内核不感知业务差异。

    [:octicons-arrow-right-24: 自定义资源类型](guides/custom-resource-type.md)

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

    存储层 `memory / sqlite / mysql`，协调层 `memory / redis`；切换后端不需要修改业务代码。

    [:octicons-arrow-right-24: 数据平面与丢失语义](design/data-planes.md)

</div>

## 最小示例

```python title="main.py"
import asyncio
from purser import AcquireRequest, Purser


async def main():
    ps = Purser()
    pool = ps.create_pool("proxies", "proxy")
    await pool.import_resources([...], import_validate="on_import")  # (1)!

    async with ps, pool.lease(AcquireRequest(consumer="crawler")) as lease:
        print(lease.resource_id, lease.payload)


asyncio.run(main())
```

1. `[...]` 代表代理记录；完整可运行示例见[嵌入式快速开始](getting-started/quickstart-async.md)。

## 文档地图

### 我是集成方

| 目标 | 入口 |
| --- | --- |
| 第一次完成 | [安装](getting-started/install.md) → [嵌入式快速开始](getting-started/quickstart-async.md) |
| 理解资源为什么获取不到 | [资源可用性判定](guides/availability.md) |
| 理解租约、验活、强制撤销 | [租约生命周期](guides/lease-lifecycle.md) |
| 接代理/账号以外的资源 | [自定义资源类型](guides/custom-resource-type.md) |
| 在同步代码里调用 | [同步 Purser](guides/sync-facade.md) |

### 我是运维/平台方

| 目标 | 入口 |
| --- | --- |
| 启动服务 | [服务快速开始](getting-started/quickstart-service.md) |
| 用控制台完成日常操作 | [Web 控制台](guides/web-console.md) |
| 部署多实例 | [多实例部署](guides/multi-instance.md) |
| 决定备份与 Redis 策略 | [数据平面与丢失语义](design/data-planes.md) |
| 退役、备份、审计 | [运维操作](guides/operations.md) |

### 我是贡献者

| 目标 | 入口 |
| --- | --- |
| 运行测试与合入门槛 | [贡献指南](contributing/index.md) |
| 查签名、端点、错误码 | [参考](reference/index.md) |
| 看架构与不变量 | [设计与内部](design/index.md) |

## 权威层级

源码是最终权威；[参考](reference/index.md)描述公开契约；指南与设计页解释语义，可能滞后于实现。行为冲突时按此顺序采信。

当前版本 `0.2.0`。