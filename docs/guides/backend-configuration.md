# 后端配置示例

Purser 把状态拆成两个独立平面：**存储层（storage）** 保存资源主数据、状态、槽位与密文；**协调层（coordination）** 跟踪在途租约、心跳与唤醒。后端按**池**配置，`Purser(...)` 只是建池时的默认值。两平面的丢失语义与选型表见 [数据平面与丢失语义](../design/data-planes.md)，字段级说明见 [配置参考](../reference/configuration.md) 与 [后端契约](../reference/python-api/backends.md)。

## 平面组合

| 存储层 | 协调层 | 适合 |
| --- | --- | --- |
| `memory` | `memory` | 本地开发、测试、单进程 demo |
| `sqlite` | `memory` | 单机持久化，仍只有一个调度进程 |
| `sqlite` / `mysql` | `redis` | 多实例共享、横向扩容 |
| `mysql` | `memory` | 单实例、复用已有 MySQL 基建 |

## 场景一：本地开发（memory + memory）

零依赖，进程退出即清空。

```python
from purser import Purser

ps = Purser()                     # 默认 storage="memory", coordination="memory"
pool = ps.create_pool("proxies", "proxy")
```

服务方式：

```bash
PURSER_STORAGE=memory PURSER_COORDINATION=memory purser serve
```

## 场景二：单机持久化（sqlite + memory）

资源主数据落盘，调度仍在本进程。SQLite 自动启用 WAL，写锁冲突最多短重试 3 次。

```python
ps = Purser(
    storage="sqlite",
    coordination="memory",
    sqlite_path="/var/lib/purser/purser.db",   # 显式路径，不要省略
    audit_path="/var/lib/purser/purser.db",     # 审计哈希链落同一文件
)
```

服务方式：

```bash
export PURSER_STORAGE=sqlite
export PURSER_SQLITE_PATH=/var/lib/purser/purser.db
export PURSER_COORDINATION=memory
purser serve
```

准备与注意：

- `storage="sqlite"` 必须提供 `sqlite_path`，否则抛 `ConfigError`，不会静默退回内存；
- 默认值 `:memory:` 意味着"不落盘"，想持久化必须显式给文件路径；
- 敏感字段在库内 AES-GCM 加密，密钥由 `PURSER_SECRET_KEY` 提供，或用 sqlite 同目录自动生成的密钥；
- 数据库文件要纳入备份与权限管理，见 [运维操作](operations.md)。

## 场景三：多实例（sqlite / mysql + redis）

多个进程共享同一批资源：存储用共享的 sqlite / mysql，调度用 redis。协调 Redis 必须按持久化组件部署（AOF），不能当纯缓存。

```bash
export PURSER_STORAGE=sqlite
export PURSER_SQLITE_PATH=/var/lib/purser/purser.db
export PURSER_COORDINATION=redis                 # 必须显式切换，光有 URL 不够
export PURSER_REDIS_URL=redis://:password@redis.example:6379/2
purser serve --host 0.0.0.0 --port 8000
```

MySQL 版本：

```bash
pip install "purser[mysql]"
export PURSER_STORAGE=mysql
export PURSER_MYSQL_URL=mysql://purser:secret@mysql.example:3306/purser
export PURSER_COORDINATION=redis
export PURSER_REDIS_URL=redis://:password@redis.example:6379/2
purser serve --host 0.0.0.0 --port 8000
```

嵌入式等价写法：

```python
ps = Purser(
    storage="mysql",
    coordination="redis",
    mysql_url="mysql://purser:secret@mysql.example:3306/purser",
    redis_url="redis://:password@redis.example:6379/2",
)
pool = ps.create_pool("proxies", "proxy")
```

准备与注意：

- Redis 推荐独立 DB（如 `/2`）与 `key_prefix`，避免与其它业务混用；开启 AOF（`appendfsync everysec` 或更强）并纳入高可用方案；
- MySQL 需先建库（utf8mb4），连接失败抛 `BackendUnavailable`；id 超过 255 字符需要投影；
- 强制 RESP2 以兼容 Redis < 6，连接与操作带 5 秒超时，fail-closed；
- 同一进程内不要创建两个共享同一后端的实例，会抛 `IsolationConflict`。

## 每池覆盖后端

后端按池配置，不同意默认值时显式覆盖：

```python
ps = Purser(storage="sqlite", sqlite_path="purser.db", coordination="redis", redis_url="redis://...")
pool = ps.create_pool("proxies", "proxy")                       # 继承默认
pool2 = ps.create_pool("accounts", "account", storage="memory") # 单池改存储
```

## 密钥、审计与检查

- `PURSER_SECRET_KEY`：payload 加密主密钥，16 / 24 / 32 字节（或 base64）；sqlite 场景不提供时自动生成并持久化。
- `PURSER_AUDIT_PATH`：审计 append-only 哈希链落 SQLite，重启后继续校验。
- 轮换密钥用 `previous_secret_keys` 保留旧密钥解密，见 [运维操作](operations.md)。
- 启动后检查：

```bash
curl http://127.0.0.1:8000/readyz
curl http://127.0.0.1:8000/service    # 每个实例的 storage / coordination 必须一致
curl http://127.0.0.1:8000/audit/verify
```
