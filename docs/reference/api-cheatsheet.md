# API 速查

按任务查入口。Python 以异步 `Purser` 为例；同步门面去掉 `await` 即可。REST 默认 `http://127.0.0.1:8000`，写操作带 `Authorization: Bearer $PURSER_API_KEY` 与可选的 `X-Operator`。

## 池

| 任务 | Python | REST | CLI |
| --- | --- | --- | --- |
| 列出池 | `ps.pools` / `ps.get_pool_names()` | `GET /pools` | `purser status` |
| 查看池统计 | `await pool.stats()` | `GET /pools/{pool}` | `purser status` |
| 新建池 | `ps.create_pool(name, kind, **cfg)` | `POST /pools` | — |
| 幂等取或建 | `ps.ensure_pool(...)` | — | — |
| 热更新配置 | `await pool.reconfigure(**kw)` | `PATCH /pools/{pool}` | — |
| 删除池 | `await ps.delete_pool(name, force=False)` | `DELETE /pools/{pool}?force=` | — |
| 排空池 | `await pool.admin.drain(...)`（资源级） | `POST /pools/{pool}/drain` | — |
| 维护任务 | `await pool.admin.run_maintenance(name)` | `POST /pools/{pool}/maintenance/{task}` | — |
| 配置 schema | — | `GET /config/schema` | — |
| 适配器目录 | `ps.adapters.names()` | `GET /adapters` | — |

## 资源

| 任务 | Python | REST | CLI |
| --- | --- | --- | --- |
| 导入 | `await pool.import_resources(records)` | `POST /pools/{pool}/resources` | `purser import <pool> <file>` |
| 生成并验活 | `await pool.generate(specs)` | 同上 + `import_validate` | `--import-validate on_import` |
| 列表/筛选 | `await pool.query(...)` | `GET /pools/{pool}/resources?state=&tag=` | `purser ls <pool> --state --tag` |
| 读取一条 | `await pool.get(rid)` / `require` | `GET /pools/{pool}/resources/{rid}` | — |
| 更新 | `await pool.admin.update_resource(...)` | `PUT /pools/{pool}/resources/{rid}` | — |
| 删除 | 无资源级删除方法：空闲时直接 `admin_set_state(rid, target=ResourceState.ARCHIVED, reason=...)`，忙时先 `drain` 等租约归还 | `DELETE /pools/{pool}/resources/{rid}` | — |
| 验活 | `await pool.admin.validate_now(rid)` | `POST .../{rid}/validate` | `purser validate <pool> <rid>` |
| 命名检查 | `await pool.admin.validate_check(rid, name)` | `POST .../{rid}/validate` `{"check": name}` | — |
| 封禁/解封 | `pool.admin.ban/unban` | `POST .../{rid}/ban` / `/unban` | `purser ban <pool> <rid>` / `unban` |
| 暂停/恢复 | `pool.admin.suspend/resume` | `POST .../{rid}/suspend` / `/resume` | `purser suspend <pool> <rid>` / `resume` |
| 配额充值 | `pool.admin.top_up` | `POST .../{rid}/top-up` | `purser top-up <pool> <rid>` |
| 排空/取消 | `pool.admin.drain/cancel_drain` | `POST .../{rid}/drain` / `/cancel-drain` | `purser drain <pool> <rid>` |
| 恢复归档资源 | `pool.admin.restore_resource` | `POST .../{rid}/restore` | — |
| 强制状态 | `pool.admin.admin_set_state` | `POST .../{rid}/admin-state` | — |
| 迁移到其他池 | `pool.admin.move_resource` | `POST .../{rid}/move` | — |
| 切换/替换 | `pool.admin.switch_resource` | — | — |
| 批次召回 | `pool.admin.recall_batch` | `POST /pools/{pool}/recall` | — |
| 批次列表 | `await pool.batches()` | `GET /pools/{pool}/batches` | — |
| 批量验活任务 | `pool.validate_jobs()` / `validate_job(id)` | `GET /pools/{pool}/validate-jobs[/{id}]` | — |
| 取消验活任务 | `pool.cancel_validate_job(id)` | `POST .../validate-jobs/{id}/cancel` | — |

## 租约

| 任务 | Python | REST | CLI |
| --- | --- | --- | --- |
| 获取 | `await pool.acquire(consumer=...)` | `POST /pools/{pool}/leases` | `purser lease <pool> --consumer` |
| 上下文获取 | `async with pool.lease(...)` | — | — |
| 批量获取 | `await pool.acquire_many(n, req=...)` | `POST /pools/{pool}/leases`（bundle/多租约见端点） | — |
| 组合租约 | `await ps.acquire_bundle(req)` | `POST /pools/{pool}/leases`（`bundle` 字段） | — |
| 列出活跃租约 | `await pool.leases(rid=None)` | `GET /pools/{pool}/leases` | — |
| 读取一条 | `await pool.lease_by_token(token)` | `GET /pools/{pool}/leases/{token}` | — |
| 续约 | `await pool.renew(lease)` | `POST .../leases/{token}/renew` | — |
| 归还 | `await pool.release(lease, outcome=...)` | `DELETE .../leases/{token}` | `purser release <pool> <token>` |
| 按 consumer 归还 | `await pool.release_consumer(name)` | `DELETE /pools/{pool}/leases?consumer=` | — |
| 组合归还 | `await ps.release_bundle(bundle)` | — | — |
| 强制撤销 | `pool.admin.force_revoke(rid, token=...)` | `POST .../{rid}/revoke` | — |

## 观察

| 任务 | Python | REST | CLI |
| --- | --- | --- | --- |
| 池状态分布 | `await pool.counts(by_name=True)` | `GET /pools/{pool}` 的 `counts` | `purser status` |
| 用量报表 | `await ps.report_usage(...)` | `GET /reports/usage` | — |
| 指标快照 | `ps.metrics.snapshot()` | `GET /metrics/snapshot` | — |
| Prometheus | `ps.metrics.render_prometheus()` | `GET /metrics` | — |
| 事件流 | `ps.bus.subscribe(fn)` | `GET /pools/{pool}/events`（SSE） | `purser events <pool>` |
| 事件历史 | `ps.bus.history(after_seq)` | `GET /pools/{pool}/events/history` | — |
| 审计 | `ps.audit.entries(...)` | `GET /audit` | — |
| 审计校验 | `ps.audit.verify()` | `GET /audit/verify` | — |
| 健康 | `ps.started` | `GET /healthz` / `/readyz` | — |
| 服务信息 | — | `GET /service` | — |
| 备份 | `await ps.backup()` | `GET /backup` | — |
| 恢复 | `await ps.restore_backup(doc)` | `POST /restore` | — |

## 最小示例

```python
from purser import Purser, UsageOutcome

ps = Purser()
pool = ps.create_pool("proxies", "proxy", lease_ttl=120)
await pool.import_resources([{"host": "1.2.3.4", "port": 8080}])

async with pool.lease(consumer="w1") as lease:
    ...
    lease.outcome = UsageOutcome.OK

await ps.close()
```

```bash
curl -X POST http://127.0.0.1:8000/pools \
  -H "Content-Type: application/json" \
  -d '{"name":"proxies","kind":"proxy"}'

curl -X POST http://127.0.0.1:8000/pools/proxies/leases \
  -H "Content-Type: application/json" \
  -d '{"consumer":"w1","timeout":10}'

curl -X DELETE http://127.0.0.1:8000/pools/proxies/leases/<token> \
  -H "Content-Type: application/json" \
  -d '{"outcome":"ok","used":1}'
```

## 查找规则

- 签名与字段：先看[Python API](python-api/index.md)与 [REST API](rest-api/index.md)，再以源码和 `/docs` 为准。
- 错误：看[错误处理](errors.md)与[错误码](error-codes.md)。
- 不知道用哪个门面：看[门面族](../guides/facades.md)。
- REST 端点级参数：运行中服务的 `/docs`（OpenAPI）。