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
- CI 的 lint / typecheck / benchmark 与 Python 3.8–3.12 矩阵通过；
- 行为变更同步更新 README 与 `docs/`。

具体命令、真实 Redis/MySQL 集成测试门控与新增测试的写法见[测试与工程规范](testing.md)。

## 代码地图

| 目录 | 职责 |
| --- | --- |
| `purser/facade.py` | `Purser` / `ResourcePool` 编排：获取、归还、验活、对账 |
| `purser/core/` | 状态机、分配器、租约、健康度、隔离、事件、用量 |
| `purser/backends/` | 存储（memory/sqlite/mysql）与协调（memory/redis） |
| `purser/types/` | `ResourceType` 基类与内置 proxy/account |
| `purser/local/` | 同机多进程调度进程与 IPC 客户端 |
| `purser/ui/` | FastAPI 应用、REST/SSE 与控制台 |
| `purser/sync.py` | 同步 Purser |
| `tests/` | 单元、边界、混沌、多进程与真实后端集成测试 |

## 文档贡献

- 新增行为优先更新参考页，再补指南中的做法说明；
- 不把逐文件测试变更日志写进文档，发布说明统一放 GitHub Releases；
- 修改后运行 `mkdocs build --strict`，确保导航与链接有效。
