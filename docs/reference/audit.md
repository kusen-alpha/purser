# 审计目录

审计记录“谁在什么时候对哪条资源做了什么”。它是**追加写**的 SHA-256 哈希链，旨在发现篡改，不是访问控制，也不能阻止有写权限的人删除整个文件。

## 存储

```python
ps = Purser(audit_path="/var/lib/purser/audit.db")
```

- 未配置 `audit_path` 时只保存在内存，重启即丢。
- 配置后写入独立 SQLite 文件（WAL 模式），启动时加载已有链继续追加。
- 链的 `seq` 从 1 开始连续递增；每条记录的摘要覆盖前一条的 `digest`。
- 追加使用 `BEGIN IMMEDIATE` 串行化；并发写失败会重试，避免分叉。

## 记录字段

| 字段 | 说明 |
| --- | --- |
| `seq` | 链内序号 |
| `ts` | epoch 秒 |
| `operator` | 操作者；REST 来自 `X-Operator`，缺省 `api` |
| `action` | 动作名，见下表 |
| `pool` / `rid` | 目标池与资源；池级操作 `rid` 为空 |
| `reason` | 操作原因（管理动作通常必填） |
| `trace_id` | 关联的请求/trace |
| `detail` | 动作相关结构化信息，如 `old_id` / `new_id` |
| `prev_hash` / `digest` | 前一条摘要与本条摘要 |

`digest = SHA-256(sorted compact JSON(seq, ts, operator, action, pool, rid, reason, trace_id, detail, prev_hash))`。

## 动作全集

| 动作 | 触发 |
| --- | --- |
| `create_pool` / `delete_pool` / `update_pool_config` | 池的创建、删除、配置热更新 |
| `import_resources` | 导入资源 |
| `update_resource` / `move_resource` / `switch` | 修改资源、迁移到其他池、适配器切换 |
| `ban` / `unban` / `suspend` / `resume` / `drain` / `cancel_drain` | 生命周期管理 |
| `admin_set_state` / `apply_defaults` | 强制状态、补齐适配器默认值 |
| `top_up` | 配额充值 |
| `force_revoke` | 强制撤销租约 |
| `release_consumer` | 按消费者批量归还 |
| `validate` / `validate_check` / `cancel_validate_job` | 验活与后台任务操作 |
| `maintenance` | 手动触发维护任务 |
| `archive_maintenance_delete` / `expire_gc_archive` | 归档清理 |
| `backup` / `restore` | 备份与恢复 |

## 查询与校验

Python：

```python
for entry in ps.audit.entries(limit=50):
    print(entry.seq, entry.action, entry.pool, entry.rid, entry.operator)

print(len(ps.audit))
print(ps.audit.verify())   # False 表示链被修改
```

`entries(after_seq=...)` 只返回更新的记录；`limit` 取尾部若干条。

REST：

```bash
curl "http://127.0.0.1:8000/audit?limit=200&action=ban&pool=proxies"
curl "http://127.0.0.1:8000/audit?before_seq=500&limit=200"
curl "http://127.0.0.1:8000/audit/verify"
```

`GET /audit` 参数：`after_seq`（严格更新）、`before_seq`（严格更旧，向前翻页）、`limit`（1–1000）、`tail`、`pool`、`action`、`rid`、`operator`。返回：

```json
{
  "entries": [...],
  "cursor": 42,
  "has_older": true
}
```

响应中的条目**不包含** `prev_hash` / `digest`，只暴露可读字段；完整性由 `/audit/verify` 统一校验。

## `verify()` 能发现什么

- 任意条目字段被修改；
- 条目被插入或删除导致 `prev_hash` 不连续；
- 持久化文件与内存视图不一致。

它**不能**发现：

- 整个审计文件被删除后重新开始（新链自身是自洽的）；
- 拥有文件写权限的人重算整条链；
- 内存模式下的进程内伪造。

需要更强保证时，把审计定期导出到只追加的外部存储（对象存储、日志平台），并保留导出时的最后 `seq` 与 `digest`。

## 运维建议

- 审计与主库分开文件（`PURSER_AUDIT_PATH`），避免主库迁移影响链。
- 定期执行 `/audit/verify`，失败即告警。
- 备份包含审计文件；恢复时不要与旧主库混用，见[数据保留](../guides/data-retention.md)。
- 审计条目会随管理动作增长，归档策略按合规要求自行制定；Purser 不自动删除审计记录。