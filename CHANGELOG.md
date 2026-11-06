# Changelog

## [0.2.0] - 2026-11-06

与「合入门禁」对齐的工程基线版本：公共 API 全标注、mypy strict 零告警、
ruff 全绿、测试全量通过、CI 全矩阵、文档与 CHANGELOG 同步维护。

### 工程基线
- 初始化 Git 仓库（此前无 `.git`），新增 GitHub Actions CI：
  lint / typecheck / test（Python 3.8–3.12 矩阵，含真实 Redis 与 MySQL 服务）/
  benchmark（混沌与多进程压测）。
- `pyproject.toml` 增加 `[tool.ruff]`（E/F/W/I/UP/B，line-length 88，豁免 E501/UP009
  并注明理由）与 `[tool.mypy]`（strict，`follow_imports=skip`，aiomysql 白名单豁免）。
- dev extras 增加 mypy / ruff / pytest-cov；包以可编辑方式安装后可任意目录跑测试。
- 版本号按语义化从 0.1.0 升到 0.2.0（首次规整后升 minor）。

### 类型与正确性
- 公共 API 全类型标注，`mypy purser`（strict）零告警，31 个源文件。
- `ruff check .` 全绿（含 tests / examples）。
- 修复一批真实缺陷：
  - `storage.scan` 抽象签名写成了 `async def -> AsyncIterator`，导致所有
    `async for` 调用点被 mypy 判为“协程未 await”，实际是异步生成器契约，已改为
    同步签名返回 `AsyncIterator`。
  - request-level BAN（`_on_ban_verdict`）只就地修改 `site_failures`，sqlite/mysql
    后端不落库导致站点失败丢失，现随状态变更在同一笔 CAS 中持久化。
  - `_on_ban_verdict` 对同步函数 `_record_site_failure` 误用 `await None`，
    运行时将抛 TypeError（该路径此前无测试覆盖）。
  - SOCKS5 域名 BND.ADDR 响应少一个 await（`await coroutine[0]` 下标协程对象），
    真实服务器回域名类型地址时会 TypeError。
  - FastAPI 路由 handler 缺失返回标注、`Body`/`dict` 泛型缺失等类型问题。

### 测试
- 新增回归用例：request-level BAN 记录站点失败（覆盖 sqlite/memory 双后端）、
  SOCKS5 域名 BND.ADDR 响应。
- 本地验收：ruff、mypy --strict 全绿；pytest 全量 656 passed / 50 skipped，
  覆盖率 81%（未连真实 Redis/MySQL 时对应集成用例自动跳过，CI 中由服务补齐）。

### 兼容性
- 最低 Python 版本调整为 3.8：`requires-python = ">=3.8"`，CI 矩阵
  3.8 / 3.9 / 3.10 / 3.11 / 3.12 全部通过。
- 移除 3.10 专属的 `dataclass(slots=True)`（改为普通 `@dataclass`，保留 `frozen`）；
  `asyncio.to_thread` 改为 `run_in_executor` 助手（3.9+）；`zoneinfo` 增加
  `backports.zoneinfo` 回退（3.8/3.9）；`ParamSpec` 改从 `typing_extensions` 导入；
  去掉 3.10 专属的 `zip(strict=)`。
