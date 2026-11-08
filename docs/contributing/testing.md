# 测试与工程规范

## 环境

```bash
python -m venv .venv
.venv\Scripts\pip install -e ".[dev]"
```

Linux / macOS 使用 `.venv/bin/pip`。真实 Redis / MySQL 用例由环境变量门控，未设置时自动跳过。

## 快速运行

```bash
# 全量
.venv\Scripts\python -m pytest tests -q

# 单个文件 / 单个用例
.venv\Scripts\python -m pytest tests/test_fault_tolerance.py -q
.venv\Scripts\python -m pytest tests/test_boundary_data_edges.py -q -k "quota"
```

## 测试分层

| 层次 | 代表文件 | 关注点 |
| --- | --- | --- |
| 单元与状态机 | `test_state_machine.py`、`test_allocator.py` | 转移表穷举、分配器属性（hypothesis） |
| 存储 × 协调矩阵 | `test_storage_matrix.py`、`test_coordination.py` | 各后端组合下行为一致、CAS、对账 |
| 核心行为 | `test_lease.py`、`test_fairness.py`、`test_quarantine.py`、`test_health.py` | 租约/心跳/租约回收器、公平队列、退避、EWMA |
| 服务面 | `test_api.py`、`test_cli.py`、`test_ui_contract.py` | 鉴权、幂等、错误映射、静态控制台契约 |
| 边界补充 | `test_boundary_*.py` | 数据/并发/生命周期/流式/管理/钩子等单点边界 |
| 故障与混沌 | `test_fault_tolerance.py`、`test_chaos_storm.py` | 协调层故障注入、并发抢占、确定性混沌风暴 |
| 真多进程 | `test_multiprocess_stress.py` | 多 OS 进程共享 SQLite，采样断言不超卖 |
| REST 模糊 | `test_api_fuzz.py` | 非法请求永不 500，运行后不变量成立 |
| 真实后端 | `test_redis_real.py`、`test_redis_more_edges.py`、`test_mysql.py` | TTL、跨实例可见、崩溃后对账、连接恢复 |
## 合入门禁

合并前必须全部通过，缺一不可：

- 公共 API 全类型标注，`mypy purser`（`strict`）零告警；
- `ruff check .` 零告警（`E/F/W/I/UP/B`，line-length 88，豁免 `E501` / `UP009`）；
- `pytest tests -q --cov=purser --cov-report=term-missing` 通过并上报覆盖率；
- CI（`.github/workflows/ci.yml`）lint / typecheck / benchmark 与 Python
  3.8-3.12 测试矩阵全部通过，其中真实 Redis / MySQL 集成用例由
  `PURSER_TEST_REDIS_URL` / `PURSER_TEST_MYSQL_URL` 环境变量门控，未设置时自动跳过；
- 代码改动同步更新 README / docs；变更记录发版时统一写入 GitHub Releases，
  版本号按语义化递增。

## 真实服务集成

### MySQL

```bash
export PURSER_TEST_MYSQL_URL="mysql://root:pass@127.0.0.1:3306/purser_test"
pytest tests/test_mysql.py -q
```

未设置时跳过。测试会使用独立库表，但仍建议指向专用测试实例。

### Redis

```bash
export PURSER_TEST_REDIS_URL="redis://127.0.0.1:7001/15"
pytest tests/test_redis_real.py -q
```

- 建议指向独立 DB（示例用 `15`）；测试前后会 flush 该 DB。
- 设置后，整包中所有协调用例也会走真实 Redis；未设置时用 `fakeredis`。
- 覆盖租约哈希/token 索引与 `PEXPIRE`、断连重连、进程崩溃后 TTL 过期与重启对账、跨实例可见/强制撤销/迟到 release、多进程共享 SQLite + Redis 不超卖。

## 新增测试约定

- 优先扩展已有测试文件：边界用例放 `test_boundary_*.py`，真实后端放 `*_redis*.py` / `test_mysql.py`。
- 涉及时间时使用可控时钟或短 TTL，不要 `sleep` 固定秒数。
- 涉及并发时显式设置种子（`test_chaos_storm.py` 的做法），保证可复现。
- 每个回归用例先断言可失败的旧行为，再修复；提交说明写清根因，不在测试文件里堆变更日志。
- 涉及槽位时至少断言：`0 <= used_slots <= max_slots`、租约归还后归零、迟到 release 不会误扣。

## CI

`.github/workflows/ci.yml` 在 Python 3.8–3.12 上运行测试矩阵，另有 Windows 3.11 与 benchmark job；lint/typecheck 使用 3.10。真实 Redis / MySQL 变量未配置时相关用例自动跳过，因此本地通过不等于集成通过。
