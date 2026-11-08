# 运维操作

这一页汇总日常会用到的手动操作：维护任务、退役召回、备份恢复、审计、报表与指标。

## 维护任务

```bash
curl -X POST http://127.0.0.1:8000/pools/proxies/maintenance/quarantine_gc
```

| 任务 | 作用 |
| --- | --- |
| `quota_reset` | 周期配额重置 |
| `periodic_unban` | 到期自动解封 |
| `lost_gc` | 失联资源判定失效 |
| `validating_gc` | 清理滞留验活 |
| `quarantine_gc` | 清理到期隔离 |
| `archive_gc` | 清理过期归档 |

维护任务默认由后台周期任务执行；手动触发适合排障或补执行。返回任务统计。

## 批量退役与召回

```bash
# 退役资源（默认 IDLE / ACTIVE / FULL）
curl -X POST http://127.0.0.1:8000/pools/proxies/drain \
  -H "Content-Type: application/json" \
  -d '{"reason": "rotate subnet", "states": ["idle"]}'

# 按导入批次处理：requalify | retire | isolate
curl -X POST http://127.0.0.1:8000/pools/proxies/recall \
  -H "Content-Type: application/json" \
  -d '{"batch_id": "b-20261108", "action": "requalify"}'
```

- `drain` 只停止新分配，存量租约归还后归档。
- `cancel-drain` 可撤销退役。
- `recall` 按 `batch_id` 批量处理，单条失败不中断。

## 备份与恢复

```bash
# 备份需要 admin
curl -H "Authorization: Bearer $PURSER_API_KEY" \
  http://127.0.0.1:8000/backup > backup.json

# 恢复：conflict=skip|overwrite
curl -X POST "http://127.0.0.1:8000/restore?conflict=skip" \
  -H "Authorization: Bearer $PURSER_API_KEY" \
  -H "Content-Type: application/json" \
  --data @backup.json
```

- 备份包含明文密钥，只应保存在受控位置。
- 租约不迁移；占用态归一为 `IDLE`。
- 池的存储/协调取自当前部署，不随备份导入。
- 支持 `Idempotency-Key`。

详见 [管理端点](../reference/rest-api/admin.md)。

## 审计

```bash
curl "http://127.0.0.1:8000/audit?tail=true&limit=50"
curl "http://127.0.0.1:8000/audit?pool=proxies&action=force_revoke"
curl http://127.0.0.1:8000/audit/verify
```

审计为 append-only 哈希链；配置 `audit_path` 时落 SQLite，重启后继续校验。所有强制操作（`force_revoke`、`admin-state`、`ban`、`drain` 等）必须带 `reason`。

## 报表与指标

```bash
curl "http://127.0.0.1:8000/reports/usage?group_by=consumer&limit=50"
curl http://127.0.0.1:8000/metrics
curl http://127.0.0.1:8000/metrics/snapshot
```

- 用量账本在进程内有界，重启清空；长期留存请定期拉取报表。
- `/metrics` 为 Prometheus 文本格式；`/metrics/snapshot` 返回结构化计数器与直方图。

## 控制台与事件流

- `/`：Web 控制台，可查看池、资源、租约、事件。
- `/docs`：OpenAPI 文档。
- `/pools/{pool}/events`：SSE；`/events/history`：游标回放。

## 密钥轮换

```python
Purser(
    secret_key="new-key",
    secret_key_id="2",
    previous_secret_keys={"1": "old-key"},   # 轮换期保留旧密钥解密
)
```

新写入使用当前 `secret_key_id`，旧密文仍可用 `previous_secret_keys` 解密。轮换完成并重新加密后再移除旧密钥。

## 故障排查顺序

1. `GET /healthz`、`GET /readyz` 确认进程与池；
2. `GET /service` 确认 storage / coordination 是否为预期；
3. `GET /audit/verify` 确认审计链完整；
4. 检查 `BackendUnavailable`、`VersionConflict` 日志；
5. 重启后观察 `start()` 对账是否收敛槽位。
