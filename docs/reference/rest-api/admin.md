# Admin

管理端点：健康探针（`/healthz`、`/readyz`、`/service`）、指标、审计校验与备份恢复，以及事件历史。

## 端点

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/healthz` | 存活探针（无鉴权） |
| `GET` | `/readyz` | 就绪探针，返回池列表（无鉴权） |
| `GET` | `/service` | 版本、运行时长、后端、适配器与维护开关（需读权限） |
| `GET` | `/metrics` | Prometheus 文本指标（无鉴权） |
| `GET` | `/metrics/snapshot` | 计数器与直方图快照（需读权限） |
| `GET` | `/audit` | 审计分页（需读权限） |
| `GET` | `/audit/verify` | 校验哈希链（需读权限） |
| `GET` | `/adapters` | 已注册适配器及字段（需读权限） |
| `GET` | `/config/schema` | 池配置与资源调度字段 schema（需读权限） |
| `GET` | `/backup` | 全量导出，含明文密钥（需 admin） |
| `POST` | `/restore` | 导入备份；支持 `Idempotency-Key`（需写权限） |

## 健康与指标

```bash
curl http://127.0.0.1:8000/healthz
# {"status":"ok"}

curl http://127.0.0.1:8000/readyz
# {"status":"ready","pools":["proxies"]}

curl http://127.0.0.1:8000/metrics
curl http://127.0.0.1:8000/metrics/snapshot
```

## 审计

```bash
curl "http://127.0.0.1:8000/audit?tail=true&limit=50&pool=proxies&action=lease"
curl "http://127.0.0.1:8000/audit?before_seq=1200&limit=100"
curl http://127.0.0.1:8000/audit/verify
# {"ok": true, "entries": 4231}
```

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `after_seq` | `0` | 只返回更新的记录（向前翻页） |
| `before_seq` | — | 只返回更旧的记录（向后翻页） |
| `tail` | `false` | 取最新一页 |
| `limit` | `200` | `1..1000` |
| `pool` / `action` / `rid` / `operator` | — | 过滤条件 |

响应为 `{entries, cursor, has_older}`。审计是 append-only 哈希链；配置 `audit_path` 时落 SQLite，重启后继续校验。

## 适配器

```bash
curl http://127.0.0.1:8000/adapters
```

返回每个已注册 `kind` 的资源类、默认并发度与 `PayloadField` 字段（名称、类型、required、secret、default、doc），控制台据此渲染表单。

## 配置 schema

```bash
curl http://127.0.0.1:8000/config/schema
```

返回 `profiles`、`immutable`、`fields` 与 `resource_fields`：每个字段带类型、分组、标签、可选值、范围、是否可空、是否可热更新、默认值与说明。控制台的建池弹窗和资源调度字段直接由此渲染，不再维护另一份硬编码表单。字段语义见[配置参考](../configuration.md)。

## 备份

`GET /backup` 需要 admin（只读密钥 403，未带密钥 401）。导出所有池的配置、资源（**含明文密钥**）以及池级活动租约摘要：

```text
{
  "format": "purser-backup",
  "version": 1,
  "schema_version": 1,
  "created_at": 1731000000.0,
  "pools": [
    {
      "name": "proxy-a",
      "kind": "proxy",
      "config": {"...": "PoolConfig.to_dict()"},
      "resources": [{"...": "Resource.to_dict()，leases 已剔除"}],
      "leases": [{"token": "...", "resource_id": "r2", "consumer": "c1", "slots": 1}]
    }
  ]
}
```

## 恢复

```bash
curl -X POST "http://127.0.0.1:8000/restore?conflict=skip" \
  -H "Authorization: Bearer $PURSER_API_KEY" \
  -H "Content-Type: application/json" \
  --data @backup.json
```

| `conflict` | 行为 |
| --- | --- |
| `skip`（默认） | 已存在的资源原样保留，计入 `skipped` |
| `overwrite` | 用备份字段覆盖（保留原 `created_at`），计入 `updated` |

其他约定：

- **租约不迁移**：恢复后所有租约计 0，占用态（`new/validating/active/full/draining/lost`）归一为 `IDLE`（计入 `normalized`）；`banned/quarantined/suspended/quota_exhausted/archived` 原样保留。
- **池存储/协调取自当前部署**：备份中的 `storage`、`coordination`、`schema_version` 会被剥离。
- 池不存在按备份 `kind` 创建（`created_pools`）；同名同 `kind` 复用（`reused_pools`）；同名不同 `kind` 报错并跳过。
- 响应报告：`{ok, format, conflict, created_pools, reused_pools, imported, updated, skipped, normalized, leases_ignored, errors[]}`；单条失败不中断整体。
- 每个池写一条 `restore` 审计；备份端写一条 `pool="*"` 的 `backup` 审计。
- 同一 `Idempotency-Key` 重复提交返回首次报告并附 `"replayed": true`。
- `format != "purser-backup"` 或 `version != 1` 返回 `422`。
