# 参与贡献

感谢你愿意改进 purser。完整指南见 [docs/contributing/](docs/contributing/index.md)，
本文件给出最短路径。

## 开发环境

```bash
python -m venv .venv
.venv\Scripts\pip install -e ".[dev]"    # Windows
# source .venv/bin/activate && pip install -e ".[dev]"   # macOS / Linux
```

## 提交前检查

```bash
ruff check .
mypy purser
pytest -q
mkdocs build --strict
```

CI 会在 Python 3.10–3.14、Windows 以及 Redis/MySQL 后端上运行同一组检查；
提交 PR 前请确保本地全绿。

## 约定

- 行为变更需附带测试；纯文档改动可直接提交。
- 公共 API 变更必须同步更新 `docs/` 与 `CHANGELOG.md` 的 `Unreleased` 段。
- 提交信息使用 `feat:` / `fix:` / `docs:` / `refactor:` / `test:` / `chore:` 前缀。
- 不在公共 docstring 中引用内部工单号；`tests/test_public_docstrings.py` 会检查。

## 发布流程（维护者）

1. 更新 `CHANGELOG.md` 与版本号（`pyproject.toml`、`purser/__init__.py`、文档版本标记）。
2. 打 tag：`git tag v0.5.0 && git push origin v0.5.0`（tag 必须与包版本一致，工作流会校验）。
3. `.github/workflows/publish.yml` 构建 sdist+wheel、`twine check` 后通过
   Trusted Publishing 发布到 PyPI（首次需在 PyPI 配置该工作流为可信发布者），
   随后用 CHANGELOG 对应小节自动创建 GitHub Release。
4. 文档由 `docs.yml` 在 master 上自动部署到 GitHub Pages；仓库需在
   Settings → Pages 将 Source 设为 `gh-pages` 分支。
