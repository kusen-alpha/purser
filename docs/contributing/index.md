# 贡献

本组页面面向修改 Purser 代码或文档的贡献者。使用者请从[上手](../getting-started/index.md)开始。

## 本地开发

```bash
python -m venv .venv
.venv\Scripts\pip install -e ".[dev,docs]"
```

Linux / macOS 把 `.venv\Scripts\` 换成 `.venv/bin/`。`dev` extras 包含测试、lint、类型检查与 benchmark 所需依赖。

## 合入门槛

合并前必须全部通过：

- `mypy purser`（`strict`）零告警；
- `ruff check .` 零告警；
- `pytest tests -q --cov=purser --cov-report=term-missing` 通过；
- CI 的 lint / typecheck / benchmark 与 Python 3.10–3.14 矩阵通过；
- 行为变更同步更新 README 与 `docs/`。

具体命令、真实 Redis/MySQL 集成测试门控与新增测试的写法见[测试与工程规范](testing.md)。

## 代码地图

| 目录 | 职责 |
| --- | --- |
| `purser/facade/` | `Purser` 组合根：池注册表、备份恢复、生命周期 |
| `purser/pool/` | `ResourcePool` 数据面：获取、归还、验活、对账 |
| `purser/core/` | 状态机、分配器、租约、健康度、隔离、事件、用量 |
| `purser/backends/` | 存储（memory/sqlite/mysql）与协调（memory/redis） |
| `purser/adapters/` | `ResourceAdapter` 基类与内置 proxy/account |
| `purser/local/` | 同机多进程调度进程与 IPC 客户端 |
| `purser/ui/` | FastAPI 应用、REST/SSE 与控制台 |
| `purser/sync.py` | 同步 Purser |
| `tests/` | 单元、边界、混沌、多进程与真实后端集成测试 |

## 文档贡献

- 新增行为优先更新参考页，再补指南中的做法说明；
- 不把逐文件测试变更日志写进文档，发布说明统一放 GitHub Releases；
- 修改后运行 `mkdocs build --strict`，确保导航与链接有效。

## 文档写作约定

- **标题**：Python / REST 参考页用代码标识符（`ResourcePool`、`Leases`），概念页用中文；H1 与 nav 文案保持一致。
- **术语**：统一用“解封、续约、验活、排空、组合租约”；`到期` 指资源有效期，`过期` 指租约 TTL。
- **代码块**：必须标语言；同一事实只写一处，其余页面链接到锚点，不复制整表。
- **页面结构**：H1 后一句导语说明本页解决什么；页末用“相关页面”给出下一步。
- **可校验事实**：状态数、错误码、`pool.admin.*` 方法名由 `scripts/check_docs.py` 对照源码校验，改代码时同步改文档即可被 CI 拦住。
