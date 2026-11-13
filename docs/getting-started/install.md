# 安装

要求 Python `>=3.8`。内核只依赖 `redis`（协调层）与 `cryptography`（密文字段）；Web 服务与 CLI 的依赖按需安装。

## 按用途选择 extras

| 场景 | 命令 | 包含 |
| --- | --- | --- |
| 嵌入式内核 | `pip install -e .` | 内核、memory 后端、Redis 协调客户端、payload 加密 |
| 服务模式 | `pip install -e ".[server,cli]"` | 内核 + FastAPI/uvicorn + httpx/rich |
| 开发测试 | `pip install -e ".[dev]"` | 上述全部 + pytest / hypothesis / mypy / ruff |
| MySQL 存储 | `pip install -e ".[mysql]"` | `aiomysql`（懒连接，不装不影响其他后端） |
| 文档站 | `pip install -e ".[docs]"` | `mkdocs-material` |

## 创建环境

```bash
python -m venv .venv

# Windows
.venv\Scripts\pip install -e ".[dev]"

# macOS / Linux
.venv/bin/pip install -e ".[dev]"
```

## 验证安装

```bash
python -c "import purser; print(purser.__version__)"
python -m pytest tests -q          # 可选：运行完整测试集
```

## 本地构建文档

```bash
pip install -e ".[docs]"
mkdocs serve -a 127.0.0.1:8001      # http://127.0.0.1:8001，避免与 purser serve 的 8000 端口冲突
mkdocs build                        # 输出到 site/
```

下一步：[嵌入式快速开始](quickstart-async.md)。
