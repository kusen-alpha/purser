# 池

## 端点

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/pools` | 所有池的统计概览 |
| `POST` | `/pools` | 创建池；支持 `Idempotency-Key` |
| `GET` | `/pools/{pool}` | 单池统计 |
| `PATCH` | `/pools/{pool}` | 热更新池配置 |
| `DELETE` | `/pools/{pool}` | 删除池；`?force=true` 强制 |
| `POST` | `/pools/{pool}/drain` | 批量退役资源 |
| `POST` | `/pools/{pool}/recall` | 按批次召回资源 |
| `POST` | `/pools/{pool}/maintenance/{task}` | 手动执行维护任务 |

## 创建池

```bash
curl -X POST http://127.0.0.1:8000/pools \
  -H "Content-Type: application/json" \
  -d '{
    "name": "proxies",
    "kind": "proxy",
    "config": {"lease_ttl": 120, "max_slots": 2}
  }'
```

```json
{"pool": "proxies", "kind": "proxy"}
```

`kind` 必须是已注册适配器。池已存在返回 `409`。

## 统计

`GET /pools/{pool}` 返回 `PoolStats`，常用字段：

```json
{
  "name": "proxies",
  "kind": "proxy",
  "total": 10,
  "counts": {"idle": 8, "active": 2},
  "allocatable": 8,
  "active_leases": 2,
  "waiting": 0,
  "avg_health": 0.83
}
```

`GET /pools` 返回 `{"pools": [PoolStats, ...]}`。

## 热更新配置

```bash
curl -X PATCH http://127.0.0.1:8000/pools/proxies \
  -H "Content-Type: application/json" \
  -d '{"config": {"lease_ttl": 180, "weights": {"health": 1.0, "load": 0.5}}}'
```

即时重建分配器、校验门、心跳与调度参数。以下键不可在运行时修改，尝试修改返回 `422`：

- `secret_fields`
- `event_queue_size`
- `schema_version`

`storage` / `coordination` 属于部署层配置：修改会返回 `422`；原样回传当前值会被忽略。

## 删除池

```bash
curl -X DELETE http://127.0.0.1:8000/pools/proxies
curl -X DELETE "http://127.0.0.1:8000/pools/proxies?force=true"
```

非空池在未加 `force` 时会被拒绝。

## 批量退役 / 召回 / 维护

```bash
# 默认退役 IDLE / ACTIVE / FULL
curl -X POST http://127.0.0.1:8000/pools/proxies/drain \
  -H "Content-Type: application/json" \
  -d '{"reason": "rotate subnet", "states": ["idle"]}'

# 按导入批次召回：requalify | retire | isolate
curl -X POST http://127.0.0.1:8000/pools/proxies/recall \
  -H "Content-Type: application/json" \
  -d '{"batch_id": "b-20261108", "action": "requalify", "reason": "recheck"}'

# 维护任务：quota_reset | periodic_unban | lost_gc | validating_gc | quarantine_gc | archive_gc | expire_gc
curl -X POST http://127.0.0.1:8000/pools/proxies/maintenance/quarantine_gc
```

`drain` 返回实际退役的资源列表与逐条错误；`recall` 返回批次处理结果；`maintenance` 返回任务统计。
