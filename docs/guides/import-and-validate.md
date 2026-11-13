# 导入与验活

导入决定资源“从哪来”，验活决定资源“能否被分配”。两者的边界是三档 `import_validate`。

## 三档导入策略

| 值 | 行为 | 资源进入可分配状态 |
| --- | --- | --- |
| `none` | 只入库，停在 `NEW` | 否，需显式验活 |
| `on_import` | 逐条同步验活，结果写入报告 | 通过后进入 `IDLE` |
| `batch` | 交给后台验活任务，立即返回 | 任务完成后依次进入 `IDLE` |

省略时取池配置 `import_validate`（默认 `none`）。大批量导入优先用 `batch`；`on_import` 会显著延长导入时间。

```mermaid
flowchart TD
    A["purser import / POST resources"] --> B{"import_validate"}
    B -->|none| C["NEW（不可分配）"]
    B -->|on_import| D["同步 validate()"]
    B -->|batch| E["后台验活任务"]
    C -->|"validate_now()"| D
    D -->|通过| F["IDLE / FULL（可分配）"]
    D -->|失败| G["QUARANTINED"]
    E -->|通过| F
    E -->|失败| G
    G -->|"退避后重试 / 到期解禁"| F
```

## NEW 不可分配

`NEW` 不会被任何后台路径自动推进。池内资源全部为 `NEW` 时，`acquire()` 立即抛带提示的 `PoolExhausted`，而不是一直等待到 `wait_timeout`。推进方式：

```python
await pool.validate_now("r1", operator="alice")
```

或用 `on_import` / `batch` 重新导入。

!!! warning "导入成功不等于资源可用"

    `none` 只完成入库。此时资源停在 `NEW`，既不参与分配，也不会被后台验活自动推进；忘记验活会表现为“池里有资源但一直获取不到”。

## 嵌入式导入

```python
report = await pool.import_resources(
    [
        {"id": "r1", "host": "1.2.3.4", "port": 8080, "tags": ["http"]},
        {"id": "r2", "host": "5.6.7.8", "port": 3128, "tags": ["http"]},
    ],
    dedup_key="id",
    source="manual",
    operator="alice",
    fail_fast=False,
    import_validate="on_import",
)

print(report.imported, report.updated, report.skipped, report.total)
print(report.errors)
print(report.batch_id)
print(report.validation)   # on_import 的逐条结果
```

`ImportReport` 字段：

| 字段 | 说明 |
| --- | --- |
| `imported` / `updated` / `skipped` | 新增 / 覆盖更新 / 去重跳过 |
| `errors` | 逐条失败：`{line, error}`，部分路径额外带 `id`；`line` 是记录在本次输入中的下标 |
| `validation` | `on_import` 的 `id / alive / latency_ms / error / action` |
| `batch_id` | `batch` 模式的后台任务 id |
| `total` | `imported + updated + skipped + len(errors)`，包含失败行 |

`fail_fast=True` 遇到首条错误即中断；默认逐条继续，单条失败不阻断整批。

## 去重与幂等

- `dedup_key` 默认 `"id"`；同键资源视为同一条，导入会更新而非新增。
- 需要“同一批重复提交复用首次结果”时，在 REST/CLI 层使用 `Idempotency-Key` / `--idempotency-key`。
- 后台验活任务按 `batch_id` 查询，不在 `report.validation` 中。

## 验活语义

- 验活结果由 `ResourceAdapter.validate()` 返回 `HealthReport`；失败进入 `QUARANTINED`，连续失败升级 `BANNED`。
- 命名检查项（`@validator`）先于 `validate()` 自动执行：`both=True` 的检查项失败与验活失败同流程（`QUARANTINED` / 连续失败升 `BANNED`）；`both=False` 的检查项只按名手动触发，不改状态。详见[自定义资源适配器](custom-resource-adapter.md)。
- 请求级校验（`on_request_check`）发生在获取路径，按目标站拒绝不合适的候选；它与导入验活是两条独立路径。
- 验活并发度由 `validate_concurrency` 控制；后台任务 worker 数取 `min(validate_concurrency, 本批资源数)`。

## CLI 文本格式

`purser import` 每行支持：

```text
host port
id host port
id protocol host port tag1 tag2
```

`#` 开头为注释；行首为 `[` 的单行 JSON 数组可直接导入。

继续阅读：[批量验活任务](../reference/rest-api/reports.md)、[状态机](../reference/state-machine.md)。
