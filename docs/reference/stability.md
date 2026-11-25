# 版本与弃用政策

## 版本号约定

purser 遵循[语义化版本](https://semver.org/lang/zh-CN/)：

- **patch**（`0.5.0 → 0.5.1`）：缺陷修复、日志与文档改进，不改变公共行为契约。
- **minor**（`0.5.0 → 0.6.0`）：新增功能、新增配置项或导出；可能伴随行为修正。
- **major**（`1.0.0`）：公共 API 冻结后的破坏性变更。

当前仍处于 `0.x`：minor 版本可能包含破坏性变更，但必须同时满足：

1. 在 `CHANGELOG.md` 的“变更/移除”小节逐条列出；
2. 公共名字的删除或改名先经过一个 minor 的 `DeprecationWarning` 过渡
   （`1.0.0` 之后此项转为强制）。

## 公共 API 的边界

以下视为公共契约：`purser` 顶层导出（`purser.__all__`）、`Purser` /
`ResourcePool` / `SyncPurser` / `RemotePool` 的方法、`AcquireRequest` /
`Lease` / `Resource` / `PoolConfig` 的公开字段、REST 端点与 CLI 命令。

`purser.__all__` 由 `tests/test_public_api_snapshot.py` 冻结：任何增删都会让
测试失败，必须显式更新快照，从而在评审中可见。

以下**不**属于公共契约：`purser` 内部模块路径（如 `purser.pool.leases`）、
以下划线开头的名字、日志文本、`describe()` 之外的内部数据结构。

## 安全支持窗口

安全修复只提供给最新的 minor 线。当前支持窗口与漏洞报告流程以仓库根目录
[`SECURITY.md`](https://github.com/kusen-alpha/purser/blob/master/SECURITY.md) 为准。

## Python 版本

支持 Python 3.10–3.14。3.10 已进入上游 EOL，本版本线仍保证兼容；移除 3.10
需要提升 minor 并在 `CHANGELOG.md` 中提前一个 minor 公告。
