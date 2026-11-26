# REST API

`purser serve` 启动的 FastAPI 应用同时服务 REST API、Web 控制台（`/`）与 OpenAPI 文档（`/docs`）。本组页面按资源组组织端点；端点级签名以 OpenAPI 为准。

## 基础约定

- 所有路径以 `/` 为根，服务默认 `http://127.0.0.1:8000`。
- 写操作从 `X-Operator` 头读取操作者（缺省 `api`），并写入审计。
- 配置 `PURSER_API_KEY` 后，请求需带 `Authorization: Bearer <key>`；写操作要求 admin。
- 配置 `PURSER_READONLY_KEY` 时，该密钥只能读；写操作返回 403。
- 未配置任何密钥时服务完全开放：匿名请求按 **admin** 处理，包括 `include_secrets` 与所有写操作。仅应在受信网络中这样部署。

!!! warning "无密钥 = 匿名 admin"

    如果没有设置 `PURSER_API_KEY` / `PURSER_READONLY_KEY`，任何能访问端口的人都能读明文密钥、下载备份并执行写操作。绑定 `0.0.0.0` 前先配置密钥，或在外层网关做鉴权。

## 错误映射

| 状态码 | 含义 | 典型异常 |
| --- | --- | --- |
| `401` | 缺少或无效密钥 | — |
| `403` | 只读角色执行写操作 / 非 admin 读取密文 | — |
| `404` | 池、资源或租约不存在 | `PoolNotFound`、`ResourceNotFound` |
| `409` | 状态或租约冲突，可重试 | `LeaseConflict`、`PoolExhausted`、`BadState`、`PoolExists` |
| `422` | 参数或配置非法 | `ConfigError`、`BadPayload`、字段校验失败 |

完整异常清单见 [错误处理](../errors.md)。

业务错误统一返回 `{"detail": "..."}`；业务异常不携带 `PurserError.code` / `retryable`。FastAPI 参数校验失败时，`detail` 为字段错误数组。需要结构化错误码时使用 Python API 的 `error_by_code` / `is_retryable`。

## 幂等

创建池、导入资源、获取租约、恢复备份支持 `Idempotency-Key` 头：同一键重复提交返回首次结果；租约与恢复场景会额外带 `"replayed": true`。

默认 TTL 为 600 秒，只有成功结果会写入缓存，失败请求不占用键。租约键按 `lease:<pool>:<role>:<key>`、恢复按 `restore:<role>:<key>` 组合，创建池与导入资源直接使用请求头值，因此同键应在全局唯一，跨池复用会命中旧结果。当前没有进行中请求的合流：并发提交同一键可能各自执行，后完成者覆盖缓存。单进程使用内存存储；多实例部署需让 `Purser` 共享 Redis 存储。

## 端点地图

| 资源组 | 页面 | 主要路径 |
| --- | --- | --- |
| 池 | [Pools](pools.md) | `/pools`、`/pools/{pool}`、`/pools/{pool}/drain`、`/pools/{pool}/recall`、`/pools/{pool}/maintenance/{task}` |
| 资源 | [Resources](resources.md) | `/pools/{pool}/resources`、`/pools/{pool}/resources/{rid}`、动作与 `revoke`、`move`、`admin-state` |
| 租约 | [Leases](leases.md) | `/pools/{pool}/leases`、`/pools/{pool}/leases/{token}`、`/renew` |
| 事件 | [Events](events.md) | `/pools/{pool}/events`、`/pools/{pool}/events/history` |
| 报表 | [Reports](reports.md) | `/reports/usage`、`/pools/{pool}/validate-jobs` |
| 管理 | [Admin](admin.md) | `/healthz`、`/readyz`、`/metrics`、`/audit`、`/adapters`、`/config/schema`、`/backup`、`/restore` |
