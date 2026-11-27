# 安全模型

Purser 的安全边界由三部分组成：**谁能调用**（服务认证）、**数据落盘时如何保护**（字段加密）、**出了问题如何证明**（审计）。它不提供用户体系、租户隔离或内置 TLS；这些要么由部署层承担，要么由调用方按本页的约束自行补齐。

## 信任边界

| 形态 | 边界 | 默认安全姿态 |
| --- | --- | --- |
| 嵌入式库 | 进程内即信任边界 | 能 import 并构造 `Purser` 的代码拥有一切权限 |
| `purser serve` | HTTP 调用方 | 未配置密钥时**匿名即 admin** |
| `LocalScheduler` / `ProcessPurser` | 同机 IPC | 同一用户/主机内信任，靠随机 `authkey` 认证 |
| 后端（SQLite / MySQL / Redis） | 部署基础设施 | 凭据在连接串里，库内 payload 可选加密 |

## 服务认证

### 两种密钥

服务只认 Bearer token，角色由 `PURSER_API_KEY` 与 `PURSER_READONLY_KEY` 决定：

```bash
export PURSER_API_KEY="$(python -c 'import secrets; print(secrets.token_urlsafe(32))')"
export PURSER_READONLY_KEY="$(python -c 'import secrets; print(secrets.token_urlsafe(32))')"
purser serve
```

| 发送的 token | 角色 | 能做什么 |
| --- | --- | --- |
| `PURSER_API_KEY` | `admin` | 全部读写，包含 `include_secrets=true` 与备份 |
| `PURSER_READONLY_KEY` | `readonly` | 只读查询；任何写请求返回 `403` |
| 其他或缺失 | 无 | `401`（只要配置了任一密钥） |

```bash
curl -H "Authorization: Bearer $PURSER_READONLY_KEY" \
  http://127.0.0.1:8000/pools
```

比较使用 `hmac.compare_digest`，不会因长度或前缀泄露时序。密钥本身没有过期、轮换或作用域机制；轮换就是改环境变量并重启。

!!! danger "未配置任何密钥 = 匿名 admin"

    `api_key` 与 `readonly_key` 都为空时，所有请求都被当作 `admin`，包括读取明文 payload 和导出备份。这只适合本机开发。

    **只配置 `PURSER_READONLY_KEY`** 又是一种陷阱：此时缺失 token 是 `401`，只读 token 是 `readonly`，而没有任何 token 能成为 admin，备份与密钥读取会永远失败。生产环境要么同时配置两个，要么只配置 `PURSER_API_KEY`。

### 操作者与追踪

- `X-Operator` 写入审计的 `operator` 字段，缺省 `api`。它只是**自报身份**，任何持钥者都能伪造；不要把它当鉴权依据。
- `X-Trace-Id` 会随租约与审计记录保存，便于把一次业务请求串到事件、审计和日志。
- 服务没有按池的 ACL、IP 白名单、审计读取隔离或登录审计。需要这些能力时，在反向代理或网关层实现。

### 网络与传输

- `purser serve` 默认监听 `127.0.0.1:8000`，**不内置 TLS**。对外提供服务时必须置于支持 TLS 的反向代理之后。
- 服务没有配置 CORS 中间件；浏览器跨源调用默认不可用，控制台通过同源加载。
- `/healthz`、`/readyz`、`/metrics` **不鉴权**；`/metrics/snapshot` 需要读权限。不要把 `/metrics` 直接暴露到公网。
- 控制台静态页本身不校验密钥，密钥由页面输入后随请求发送。不要把它当作访问控制。

## 敏感字段加密

### 声明哪些字段是密钥

适配器通过 `PayloadField(secret=True)` 声明，或建池时显式指定：

```python
pool = ps.create_pool(
    name="accounts",
    kind="account",
    secret_fields=("password", "cookie"),
)
```

- 建池后 `secret_fields` **不可修改**（热更新会返回 `422`/`ConfigError`）。
- 敏感字段在存储层以 AES-GCM 加密；同一行里非敏感字段仍是明文。
- 读取时默认掩码为 `"***"`；只有 admin 且显式 `include_secrets=true` 才返回明文。CLI 的 `purser ls` 属于受信本地工具，会直接展示明文。

### 主密钥从哪里来

优先级从高到低：

1. 构造参数 `secret_key`；
2. SQLite 场景下 `<sqlite_path>.key` 侧车文件（不存在则自动创建）；
3. 环境变量 `PURSER_SECRET_KEY`（16 / 24 / 32 字节，或 base64）；
4. 以上都没有：生成**临时密钥**并记录 warning。

几个容易踩的点：

- 只要给了 `sqlite_path`，侧车文件就优先于环境变量；想让环境变量生效，请显式传 `secret_key`，或不要依赖自动密钥文件。
- 临时密钥不跨重启，密文重启后不可解；仅限测试。`storage="mysql"` 且声明了 `secret_fields` 时，临时密钥会直接报 `ConfigError`。
- 密钥文件存在但不可读、损坏或不可写时抛 `ConfigError`，**不会**静默换钥。这是为了避免既有密文永久不可解。
- 密钥一旦丢失，历史密文无法恢复。备份里虽含明文，但那也需要能读备份。

### 轮换

密文带 `key_id`，因此可以同时持有新旧密钥：

```python
ps = Purser(
    secret_key=new_key,
    secret_key_id="2",
    previous_secret_keys={"1": old_key},
)
```

- 新写入使用 `key_id="2"`；`key_id="1"` 的旧记录仍能解密。
- 目前没有批量重加密命令。常见做法是用 `backup()` 导出、再用 `restore_backup(..., conflict="overwrite")` 按当前密钥重写；或在轮换期逐条 `update_resource`。两种方式都会让存储层以新密钥加密。
- 确认没有旧 `key_id` 的记录后，再移除 `previous_secret_keys`。移除前先做一次全量读取验证。

## 备份与审计 {#backup}

- **备份是明文**：`backup()` / `GET /backup` 为了让恢复精确重现资源，包含解密后的 payload。备份文件按密钥材料管理：加密存储、限制下载权限、记录访问者。
- 备份不包含租约；恢复时占用态归一为 `IDLE`。恢复不会重新分配租约。
- **审计是哈希链**：`audit_path` 落 SQLite 后可发现单条记录被改；它不能阻止有文件写权限的人删除整个库。不要把审计目录和业务库放在同一权限域，最好定期把审计导出到独立日志系统。
- 审计文件没有自动清理。需要归档时整库搬走并开始新链；**不要删除链首的旧行**，否则 `verify()` 会失败。

详见[审计目录](../reference/audit.md)与[数据保留](data-retention.md)。

## 后端与 IPC

- MySQL / Redis 的连接串通常包含口令：用环境变量或密钥管理服务注入，不要提交到版本库，也不要在命令行历史里留下明文。
- 协调 Redis 必须按持久化组件部署（AOF）。丢失在途租约不仅是可用性问题，还会让同一资源被重复并发使用。
- 同进程内的两个 `Purser` 不能共享存储、协调或适配器实例；库会在装配时抛 `IsolationConflict`。`PURSER_ALLOW_SHARED_BACKENDS=1` 只用于模拟多主机的测试脚手架，不是安全开关。
- `LocalScheduler` 的 IPC 使用随机 `authkey`；Windows 走命名管道，POSIX 走临时目录下的 Unix socket。能读到 `PURSER_LOCAL_SCHEDULER` 环境变量的同用户进程可以连接它，因此不要把它注入到不可信子进程。

## 上线前的最小检查

- [ ] 配置 `PURSER_API_KEY`；如需要只读角色，再配置 `PURSER_READONLY_KEY`，并验证只读 token 写操作返回 `403`。
- [ ] 服务只监听内网/回环地址，前置 TLS 与访问控制；`/metrics` 不直接暴露公网。
- [ ] 设置稳定的 `PURSER_SECRET_KEY` 或受保护的 SQLite 密钥文件；验证重启后明文仍可读。
- [ ] 备份文件加密保存，并做过一次恢复演练。
- [ ] 配置 `audit_path`，并定期导出审计；`/audit/verify` 返回 `ok: true`。
- [ ] Redis 开启 AOF，MySQL 使用独立账号与最小权限。
- [ ] 检查日志与告警中没有明文 payload、连接串或 API key。

更完整的部署清单见[生产检查清单](production-checklist.md)；漏洞报告流程见仓库根目录 [`SECURITY.md`](https://github.com/kusen-alpha/purser/blob/master/SECURITY.md)。