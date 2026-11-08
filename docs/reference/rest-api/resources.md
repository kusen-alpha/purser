# 资源

## 端点

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/pools/{pool}/resources` | 列表、过滤、排序、分页 |
| `POST` | `/pools/{pool}/resources` | 导入资源；支持 `Idempotency-Key` |
| `GET` | `/pools/{pool}/resources/{rid}` | 单个资源；`?include_secrets=true` 需 admin |
| `PUT` | `/pools/{pool}/resources/{rid}` | 更新 payload / tags / 配额 / 调度参数 |
| `DELETE` | `/pools/{pool}/resources/{rid}` | 有租约则退役，否则归档 |
| `POST` | `/pools/{pool}/resources/{rid}/{action}` | 状态动作，见下表 |
| `POST` | `/pools/{pool}/resources/{rid}/revoke` | 强制撤销租约，需 `reason` |
| `POST` | `/pools/{pool}/resources/{rid}/move` | 迁移到目标池 |
| `GET` | `/pools/{pool}/batches` | 导入批次列表 |

## 列表与排序

| 参数 | 取值 | 默认 | 说明 |
| --- | --- | --- | --- |
| `state` | 逗号分隔的状态名 | 全部 | 如 `idle,active` |
| `tag` | 逗号分隔的标签 | 全部 | 资源需包含全部标签 |
| `q` | 字符串 | — | 搜索 |
| `sort` | `id` / `health` / `recent` | `id` | 按 ID、EWMA 健康分、最近获取时间 |
| `order` | `asc` / `desc` | `id` 为 `asc`，其余 `desc` | 方向 |
| `limit` | `1..1000` | `100` | 页大小 |
| `offset` | `>=0` | `0` | 偏移 |

先在全量匹配集上排序再切页；同分同时间按 ID 升序稳定排序。从未使用的资源 `last_used_at` 视为 `0`。响应：

```json
{
  "resources": ["..."],
  "counts": {"idle": 8, "active": 2},
  "total": 10,
  "limit": 100,
  "offset": 0,
  "sort": "id",
  "order": "asc"
}
```

## 导入

```bash
curl -X POST http://127.0.0.1:8000/pools/proxies/resources \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: import-20261108" \
  -d '{
    "records": [{"host": "1.2.3.4", "port": 8080}],
    "import_validate": "on_import",
    "dedup_key": "id",
    "fail_fast": false
  }'
```

- `import_validate`：`none` / `on_import` / `batch`；省略时取池配置。
- `none` 只入库，资源停在 `NEW`，不可分配。
- `on_import` 逐条同步验活，结果写入导入报告的 `validation`。
- `batch` 交给后台验活任务，任务 id 即报告中的 `batch_id`。
- `dedup_key` 默认 `"id"`；`fail_fast=true` 遇到首条错误即中断。

## 更新

```bash
curl -X PUT http://127.0.0.1:8000/pools/proxies/resources/r1 \
  -H "Content-Type: application/json" \
  -d '{
    "payload": {"isp": "CT"},
    "tags": ["http", "US"],
    "max_slots": 2,
    "usage_quota": 100,
    "expires_at": 1767225600,
    "mutex_group": "account-a",
    "schedule_window": ["08:00", "20:00"],
    "reason": "rotate credentials"
  }'
```

忙时（有活跃租约）修改 `max_slots` 等会返回 `409`。`payload` / `tags` 为整体替换语义。`schedule_window` 必须传 `["HH:MM", "HH:MM"]`；开始时间晚于结束时间表示跨夜窗口；传 `null` 清除。

## 删除

无活跃租约时直接归档（`ARCHIVED`）；存在占用槽位时转为 `DRAINING`，等租约归还后归档：

```json
{"ok": true, "rid": "r1", "state": "draining"}
```

## 状态动作

`POST /pools/{pool}/resources/{rid}/{action}`，`body` 可带 `reason`（部分动作）与 `amount`：

| action | 说明 | body |
| --- | --- | --- |
| `validate` | 立即验活 | — |
| `ban` / `unban` | 封禁 / 解封（解封先重验） | `reason` |
| `suspend` / `resume` | 计划内暂停 / 恢复 | `reason` |
| `top-up` | 补充配额 | `amount`（正整数）、`reason` |
| `drain` / `cancel-drain` | 退役 / 取消退役 | `reason` |
| `restore` | 从归档恢复并重验 | `reason` |
| `admin-state` | 人工改状态 | `target`、`reason` |

响应统一为 `{"ok": true, "rid": "...", "state": "..."}`。

## 强制撤销租约

```bash
curl -X POST http://127.0.0.1:8000/pools/proxies/resources/r1/revoke \
  -H "Content-Type: application/json" \
  -d '{"reason": "leaked credentials", "token": "optional-single-token"}'
```

每次调用只撤销**一条**租约：不传 `token` 时由服务端从该资源的活跃租约中任选一条，传入时撤销指定租约。返回 `{"ok": true, "revoked_token": "l_..."}`。被撤销租约的 `consumer` 在该资源上冷却 10 秒（匿名租约不冷却），同时写入审计并发布 `lease_revoked` 事件。需要全部撤销时按 token 逐条调用。

## 迁移资源

```bash
curl -X POST http://127.0.0.1:8000/pools/proxies/resources/r1/move \
  -H "Content-Type: application/json" \
  -d '{"target_pool": "proxies-backup", "reason": "rebalance"}'
```

目标池必须存在且 `target_pool != pool`；占用中的资源不可迁移。
