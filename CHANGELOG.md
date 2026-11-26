# 更新日志

本项目遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/) 与
[语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### 变更（破坏性）

- 池对象的 18 个运维方法（`validate_now` / `ban` / `unban` / `suspend` / `resume` /
  `top_up` / `drain` / `cancel_drain` / `restore_resource` / `admin_set_state` /
  `force_revoke` / `update_resource` / `switch_resource` / `move_resource` /
  `recall_batch` / `run_maintenance` / `apply_defaults` / `validate_check`）统一移入
  `pool.admin` 命名空间：`pool.ban(...)` → `pool.admin.ban(...)`；调度进程 IPC 白名单
  与 REST 同步拆分，数据面保持精简。`ThreadPurser` / `RemotePool` 同样仅保留
  `admin.ban` / `admin.unban`。
- `PoolConfig.import_validate` 默认值 `batch` → `auto`：≤10 条记录导入时同步验活
  （返回即可分配），更多记录自动转后台验活任务。显式传 `none` / `on_import` /
  `batch` 的行为不变。
- 异常统一去掉 `Error` 后缀：`LeaseRevokedError` → `LeaseRevoked`，
  `LeaseLostError` → `LeaseLost`；旧名保留一个发布周期，访问时抛
  `DeprecationWarning`。
- `Resource.next_usable_at()` → `Resource.usable_at()`；旧名同样以
  `DeprecationWarning` 过渡。
- 池与门面方法的资源标识参数统一为 `resource_id`（原 `rid`）；位置参数不变，
  按关键字传参的调用需要改名。
- 适配器装饰器 `purser.register` → `purser.register_default_adapter`，避免与
  `register_adapter` / `register_allocator` 的泛名混淆；旧名保留弃用警告。
- `SchedulerProtocolError` / `SchedulerTimeout` / `SchedulerUnavailable`
  进入顶层导出。

### 新增

- `acquire()` / `lease()` / `acquire_many()` 首次调用自动启动运行时，不再要求先
  `await ps.start()`；`close()` 之后的运行时操作仍抛 `NotStarted`（同进程不支持
  重启）。并发首启由内部锁串行化。
- `RemotePool` 补齐只读目录面：`kind` / `profile` / `refresh_info` / `get` /
  `require` / `query` / `count_matching` / `counts` / `batches` / `leases` /
  `describe`，门面能力矩阵写入 `docs/guides/facades.md`。
- `acquire_many` 不再改写调用方传入的请求对象；`n` / `n_min` / `mode` 在
  入口处校验并给出可读错误。

### 修复

- `import_validate="on_import"`（含默认 `auto` 的小批量路径）在资源处于
  `QUOTA_EXHAUSTED` / `SUSPENDED` 等不可验活状态时，不再让单条记录的
  `BadState` 中断整次导入：与后台任务一致，按记录记入 `report.validation`
  （`alive=false`、`action="revalidate"`），状态保持不变。
- 调度进程池调用的 IPC 白名单补齐 `restore_resource`，修复跨进程恢复归档资源
  时的方法名缺口。
- `ProcessPurser.delete_pool` 的 `operator` 现在真正透传到调度进程。
- `import_validate` / `acquire_many(mode=...)` 的非法取值在入口抛出
  `ConfigError`，不再静默回退到默认行为。

### 文档

- README 黄金路径重写（kwargs 风格、默认导入即验活、`pool.admin` 命名空间示例），
  quickstart / 门面 / 导入与验活 / 配置参考同步 auto 默认与自动启动行为。
- 公开门面方法补齐一行式 docstring，`tests/test_public_docstrings.py` 以 AST
  扫描全部包内 docstring，禁止内部设计编号回流。

## [0.5.0] - 2026-11-25

本次发布补齐工程闭环：失败语义、覆盖率门禁、供应链与文档发布。

### 变更

- Python 支持范围扩到 3.14（3.10 下限保持不变），CI 矩阵与 classifier 同步。
- CI 覆盖率门禁升级为 Line+Branch 90%，并把 spawn 子进程（`local/server.py`）
  纳入计量；覆盖率报告作为 artifact 上传。
- 文档站发布到 GitHub Pages（`docs.yml`），新增版本与弃用政策页。
- 发布流程校验 tag 与包版本一致，并按 CHANGELOG 小节自动创建 GitHub Release。
- 新增 dependabot、pip-audit、CodeQL 与每周定时 CI。

### 修复

- 密钥文件存在但不可读、损坏或不可写时立即抛 `ConfigError`，不再静默换钥导致
  既有密文永久不可解；临时密钥生成时记录 warning。
- `acquire_many` 回滚释放失败、bundle 成员释放/GC 失败、MySQL 关闭失败不再静默。
- pytest 增加超时保护；等待类测试改用条件等待，减少固定 sleep。

### 工程

- 版本一致性由 `tests/test_metadata_consistency.py` 守护（pyproject、`__version__`、
  CHANGELOG、文档、SECURITY、CONTRIBUTING）。
- 公共导出由 `tests/test_public_api_snapshot.py` 冻结。

## [0.4.0] - 2026-11-25

本次发布聚焦内部热路径与可维护性，公共 API 语义不变。

### 变更

- `Resource.user_meta` 不再包含框架写入的 `priority` / `preference` / `group`
  与 `switched_from`，这些键归入 `system_meta`；底层 `meta` 存储不变。
- `Resource.meta` 现在是 `ResourceMeta`（`dict` 子类），新增
  `priority` / `preference` / `group` / `quarantine_until` 类型化属性；
  字典行为、存储与 JSON 形态不变。
- `Resource.payload` 保持“分离副本”语义，内部读取改用零拷贝视图，
  削减 acquire 路径上的重复深拷贝。
- 错误码解析改为 `PurserError.__init_subclass__` 显式注册表，
  `error_by_code` 为 O(1) 且兼容运行期自定义错误子类。
- allocator 名单统一由 `purser.allocator_registry` 提供，
  注册自定义 allocator 后配置校验与 schema 同步。
- 新增 `ResourcePool.lease_by_token(...)`：先查本地租约、再回退协调平面。
- `purser.ui.server` 拆为 `ui/context.py` + `ui/_helpers.py` +
  `ui/routers/*`；`create_app` 仅负责装配，OpenAPI 路由集合不变。
  `FastAPI(version=...)` 现在报告包真实版本。

### 移除

- 删除无引用实现：`EventSink`、`HealthStats.apply_report`、
  `AllocationContext.cursor`、CLI 不可达的 `validate` 命令分支及
  `PoolConfig` 模块内 `_meta` 别名。

### 修复

- `RemotePool` 补齐 `ban` / `unban`（经 IPC `pool_call` 在调度进程执行），
  与文档及 `PoolProtocol` 承诺的数据面一致。
- `PoolProtocol` / `SyncPoolProtocol` 不再声明并非所有门面都提供的
  `kind` / `profile` 属性；身份属性仍由各具体门面自行提供。

### 文档

- `docs/reference/python-api/resources.md` 补充调度键归属与
  `payload` 分离副本说明。

## [0.3.0] - 2026-11-24

本次发布聚焦接口与发布工程收敛，调度行为不变；含公共 API 破坏性改名。

### 变更

- `ResourcePool` 由单一 5000 行模块拆分为组合式服务：
  `LeaseService` / `CatalogService` / `ValidationService` / `AdminService` /
  `MaintenanceService` 及 `WaitQueue` / `TaskRegistry` / `ValidationJobs` 等叶子协作者。
  公共 API 与调用方式不变。
- `Purser` 与 `ResourcePool` 移入 `purser.facade` / `purser.pool` 包；
  `from purser import Purser, ResourcePool` 保持不变。
- 配置多值字段统一为 `str` 枚举（`StorageKind`、`CoordinationKind`、`AcquireMode`、
  `SchedulerKind`、`UnbanMode`、`ImportValidate`、`ExpirePolicy`、`BundleMode`），
  字符串输入与存储记录兼容。
- `Purser.restore(...)` 改名为 `Purser.restore_backup(...)`，避免与资源归档恢复混淆。
- `ResourcePool.restore(...)` 改名为 `ResourcePool.restore_resource(...)`。
- `ResourcePool.renew_manual(...) -> bool` 改名为 `renew(...) -> float`，返回新的
  `expires_at`；失败仍抛 `LeaseLimitReached` / `LeaseLost`。
  `SyncPool` / `ThreadPool` / `RemotePool` / `RemoteLease` 同步对齐返回类型。

### 移除

- 删除从未生效的 `PoolConfig.probe_targets` 开关；探活目标请在
  `ProxyAdapter(probe_targets=...)` 中配置。

### 文档

- 修正 `create_pool` 异步用法等文档漂移，清理公共 docstring 中的内部工单号。
- 新增 `tests/test_public_docstrings.py` 与 `tests/test_facade_contract.py` 防回归。
- mkdocs nav 补全归档设计文档，`mkdocs build --strict` 通过。

### 发布工程

- 完整 PyPI 元数据（SPDX license、classifiers、keywords、project.urls）。
- 新增 `CHANGELOG.md`、`SECURITY.md`、`CONTRIBUTING.md`。
- 新增 `v*` tag 触发的 PyPI Trusted Publishing 工作流与 docs strict CI 任务。
- 类型化 `PoolProtocol` / `PurserProtocol` / `SyncPoolProtocol`，替代无使用者的
  `Any` 版 `PurserFacade`。

## [0.2.0]

### 新增

- 跨池 bundle 租约（`acquire_bundle` / `release_bundle`，含回滚与两阶段模式）。
- 资源适配器插件体系与 proxy/account 内置适配器。
- 远程门面 `connect()` / `RemotePool` 与 IPC 错误体系。
- 统一调度配置、`quota_limit` 配置项。
- 导入后的池默认可用；`Purser.ensure_pool` 幂等创建。

[Unreleased]: https://github.com/kusen-alpha/purser/compare/v0.5.0...HEAD
[0.5.0]: https://github.com/kusen-alpha/purser/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/kusen-alpha/purser/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/kusen-alpha/purser/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/kusen-alpha/purser/releases/tag/v0.2.0
