# 从 0.4 升级到 0.6

升级分两段：**0.4 → 0.5** 基本没有破坏性 API 变更，但有失败语义变化；**0.5 → 0.6** 把一批运维方法移入 `pool.admin`，并调整了默认导入行为。先读 [CHANGELOG](https://github.com/kusen-alpha/purser/blob/master/CHANGELOG.md)，再按下面的顺序迁移。

## 0.4 → 0.5

### 你要注意的行为变化

- **密钥文件失败不再静默换钥**：密钥文件存在但不可读、损坏或不可写时，构造 `Purser` 直接抛 `ConfigError`。升级后如果启动失败，先检查 SQLite 旁的 `.key` 文件权限/内容，而不是删除它。
- **部分后台失败不再被吞掉**：`acquire_many` 回滚释放失败、bundle 成员释放/GC 失败、MySQL 关闭失败会显式暴露。之前“看似成功”的路径现在可能看到异常或日志。
- **Python 支持扩到 3.14**：最低仍是 3.10；CI 与 classifier 已同步。

### 兼容性

- 公共 API 名称与 REST 端点不变。
- 当前备份格式为 `version=1`；升级前仍应做一次 `backup()` 和数据库文件备份。
- 内部模块拆分（`ui/context.py`、`ui/routers/*`）不属于公共契约；不要 import 内部路径。

## 0.5 → 0.6

以下改动是 0.6.0 的破坏性变更。

### 1. 运维方法移入 `pool.admin`

18 个低频方法从池对象移到 `pool.admin`，数据面方法保持原位：

```python
# 旧
await pool.ban(rid, reason="risk")
await pool.validate_now(rid)
await pool.drain(reason="rotate")

# 新
await pool.admin.ban(rid, reason="risk")
await pool.admin.validate_now(rid)
await pool.admin.drain(reason="rotate")
```

涉及：`validate_now` / `ban` / `unban` / `suspend` / `resume` / `top_up` / `drain` / `cancel_drain` / `restore_resource` / `admin_set_state` / `force_revoke` / `update_resource` / `switch_resource` / `move_resource` / `recall_batch` / `run_maintenance` / `apply_defaults` / `validate_check`。`SyncPool` / `ThreadPool` / `RemotePool` 同样提供 `.admin`。

REST 路径与 CLI 命令不变；需要改的是 Python 调用点。

### 2. `import_validate` 默认改为 `auto`

- ≤10 条记录：同步验活，`import_resources()` 返回时资源已可分配。
- 更多记录：自动转后台验活任务，返回的 `batch_id` 可用于查询进度。
- 显式传 `none` / `on_import` / `batch` 的行为不变。

影响：依赖“默认 `batch`、导入后立刻全部可分配”的测试或脚本会看到时序变化。需要严格同步时显式传 `on_import`，需要完全跳过时传 `none`。

### 3. 异常名去掉 `Error` 后缀

```python
# 旧
from purser import LeaseRevokedError, LeaseLostError

# 新
from purser import LeaseRevoked, LeaseLost
```

旧名保留一个发布周期，访问时发 `DeprecationWarning`。更新 `except` 分支与类型标注即可；错误码字符串不变。

### 4. `Resource.next_usable_at()` → `usable_at()`

旧名以 `DeprecationWarning` 过渡。方法签名与返回值不变。

### 5. 关键字参数 `rid` → `resource_id`

位置参数不变；按关键字传参的调用需要改名：

```python
# 旧
await pool.get(rid="p1")

# 新
await pool.get(resource_id="p1")
```

### 6. `purser.register` → `purser.register_default_adapter`

`register` 仍可用但发弃用警告。适配器内部或插件入口应改用新名，避免与 `register_adapter` / `register_allocator` 混淆。

### 7. 运行时自动启动

`acquire()` / `lease()` / `acquire_many()` 首次调用会自动启动运行时，不再要求先 `await ps.start()`。`close()` 之后的运行时操作仍抛 `NotStarted`，同进程不支持重启。并发首启由内部锁串行化。

这通常不破坏现有代码，但如果你依赖“未 start 就抛错”来阻止误用，需要显式检查 `ps.started`。

### 8. `acquire_many` 不修改请求对象

`n` / `n_min` / `mode` 在入口校验；非法值抛 `ConfigError`，不再静默回退。传入的 `AcquireRequest` 不会被改写，可以安全复用。

## 迁移步骤

1. **锁定版本并读 CHANGELOG**：先在测试环境安装目标版本，确认 `purser.__version__`。
2. **全局搜索调用点**：
   ```bash
   grep -rn "pool\.\(ban\|drain\|validate_now\|force_revoke\|recall_batch\)(" your_pkg
   grep -rn "\bLeaseRevokedError\b\|\bLeaseLostError\b" your_pkg
   grep -rn "next_usable_at\|purser\.register\b" your_pkg
   grep -rn "rid=" your_pkg
   ```
3. **改 Python 调用**：批量替换为 `pool.admin.*`、新异常名、`resource_id=`、`register_default_adapter`。
4. **审视导入时序**：把测试里的“导入后立即断言可分配”改为等待 `batch_id` 完成，或显式 `import_validate="on_import"`。
5. **验证持久化**：用旧数据启动新版本，检查资源、封禁状态、密文可读、审计 `verify()`。
6. **验证接口**：跑一遍 REST 冒烟（`/healthz`、`/readyz`、`/service`、建池/获取/归还/审计）；REST 路径未变，主要确认鉴权与错误体。
7. **灰度**：多实例先升级一个，确认协调层对账与租约语义正常，再滚动其余实例。

## 回滚

- 代码与配置一起回滚；单独回滚包可能留下已按新默认写入的导入任务。
- 如果已经用新密钥写入了密文，回滚旧代码时不要删除 `previous_secret_keys` 或 `.key` 文件。
- 审计链只追加；回滚不会破坏已有链。回滚前先确认旧版本能读取并校验当前审计文件。
- 数据库 schema 与备份格式未随本次迁移改变；仍建议回滚前再备份一次。

## 升级后检查

```bash
curl http://127.0.0.1:8000/healthz
curl -H "Authorization: Bearer $PURSER_API_KEY" http://127.0.0.1:8000/service
curl -H "Authorization: Bearer $PURSER_API_KEY" http://127.0.0.1:8000/audit/verify
```

- [ ] 服务能启动，密钥文件没有触发 `ConfigError`。
- [ ] 旧资源可读，敏感字段能解密；`include_secrets=true` 仅 admin 可用。
- [ ] 导入大批量资源后，`batch_id` 能查到后台验活进度。
- [ ] 运维调用已迁移到 `pool.admin`，没有 `AttributeError`。
- [ ] 审计校验通过，事件与指标正常增长。
- [ ] 回滚路径已在测试环境演练。

相关页面：[版本与弃用政策](../reference/stability.md)、[配置参考](../reference/configuration.md)、[安全模型](security.md)、[生产检查清单](production-checklist.md)。