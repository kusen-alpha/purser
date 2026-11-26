# 平台差异

Purser 的 Python API 跨平台一致，但**进程、IPC、文件锁与信号**在 Windows 和 POSIX 上并不相同。部署前先确认下面几条，能避免大部分“本机好好的、服务器上起不来”的问题。

## 总览

| 维度 | Linux / macOS | Windows |
| --- | --- | --- |
| 默认多进程启动方式 | `spawn`（可显式改 `fork`，不推荐） | `spawn`（唯一可用） |
| `LocalScheduler` IPC | Unix domain socket（临时目录） | 命名管道 `\\.\pipe\purser-*` |
| 跨进程继承 `Purser` | `fork` 会继承，但子进程首次使用报错 | 不会发生（无 `fork`） |
| 协调层权威锁 | `fcntl.lockf` 侧车文件锁 | `msvcrt.locking` 侧车文件锁 |
| 优雅停机 | SIGTERM / SIGINT | 控制台事件 / 进程管理器终止 |
| 文件权限 | `chmod` / 属主 | ACL |
| 时区数据 | 系统 IANA 库 | 依赖 `tzdata` 包（已随依赖安装） |
| 虚拟环境路径 | `.venv/bin/python` | `.venv\Scripts\python.exe` |

## 多进程与入口保护

`LocalScheduler` 与 `ProcessPurser` 默认 `start_method="spawn"`。这意味着子进程会重新 import 主模块，**不会**继承父进程的 `Purser`、事件循环、连接或租约。

```python
# main.py
from multiprocessing import freeze_support
from purser import LocalScheduler

def main() -> None:
    with LocalScheduler(storage="sqlite", sqlite_path="purser.db") as sched:
        ...

if __name__ == "__main__":
    freeze_support()      # Windows / spawn 必需
    main()
```

- 顶层不能创建 `Purser`、连接或线程；否则 spawn 时会重复执行。
- worker 函数必须定义在模块顶层，不能是闭包或 lambda。
- 需要共享同一个资源集合时，让 worker 通过 `PURSER_LOCAL_SCHEDULER` / `SchedulerAddress` 连接调度进程，而不是把对象传过去。
- POSIX 上可以传 `start_method="fork"`，但 fork 会复制事件循环、线程与连接状态，极易出现死锁或重复写入。除非完全清楚后果，否则保持 `spawn`。

## IPC 与地址

- Windows 使用命名管道，Unix 使用临时目录下的 `.sock`；两者都由随机 `authkey` 认证。
- 地址通过 `PURSER_LOCAL_SCHEDULER` 传递，内容是带 `authkey` 的 JSON。不要把它注入到不可信子进程。
- 单条 IPC 消息上限为 32 MiB。资源 payload 很大时，不要整批塞进一次调用；分批导入或把大对象放在外部存储，只把引用放进 payload。
- Unix socket 文件会在关闭时清理；Windows 命名管道随进程消失。
- 容器内使用 `LocalScheduler` 时，worker 与调度进程必须共享同一 IPC 命名空间（同一容器或共享 socket 卷）。跨主机请改用 `purser serve` + Redis。

## 文件锁与 SQLite

- SQLite 的并发写是串行的，库用 WAL + 短重试缓解锁冲突；多进程高频写仍应改用 MySQL。
- Windows 上打开中的数据库文件不能被删除或重命名；备份/归档前先关闭实例。
- 不要把 SQLite 放在 SMB/NFS 等网络文件系统上：文件锁语义不可靠，WAL 也可能损坏。
- 同机多实例的权威锁来自侧车文件（`.db` 旁的锁文件）与 OS 建议锁；进程崩溃后由 OS 释放。网络盘上同样不可靠。
- `.key` 侧车文件与数据库同目录；Windows 上用 ACL 而不是 `chmod` 限制访问。

## 信号与停机

- Linux 容器/服务：SIGTERM 触发 uvicorn 的 lifespan 关闭，`Purser.close()` 执行；设置 `drain_timeout` 可等待在途租约。
- Windows：没有 POSIX 的 SIGTERM 语义。交互式运行时用 Ctrl+C；作为服务运行时由服务管理器发送控制台事件或直接终止进程。直接终止不会等待 `drain_timeout`，在途租约要等 TTL 到期后由 reaper 回收。
- `LocalScheduler` 的调度进程是 daemon 子进程，父进程退出时随之结束；需要独立生命周期时使用 `ProcessPurser` 的 attach 模式或 `purser serve`。
- 不要依赖 `atexit` 做关键清理；崩溃与 `kill -9` 都不会执行。

## 路径、权限与部署

- 生产环境用绝对路径配置 `sqlite_path`、`audit_path`；相对路径会随工作目录变化。
- 服务账号只应拥有数据目录与审计目录的读写权限；密钥文件不要进入版本库或镜像层。
- Windows 路径分隔符与大小写不敏感会掩盖拼写错误；跨平台脚本统一用 `pathlib`。
- Windows 的杀毒软件/索引服务可能短暂锁定 `.db`、`.db-wal`、`.key`；把数据目录加入排除项，或改用 MySQL。
- 时区：默认 `PoolConfig.timezone="Asia/Shanghai"`，用于每日配额重置。Windows 没有系统 IANA 数据库，依赖 `tzdata` 包（已作为平台依赖安装）；自定义时区前先验证 `ZoneInfo` 可用。

## 与平台无关的部分

以下行为在两个平台上一致，不需要额外分支：

- 状态机、租约语义、分配器与公平队列；
- REST API、Web 控制台、CLI；
- 审计哈希链与字段加密；
- MySQL / Redis 后端协议；
- 配置字段与错误码。

相关页面：[多进程使用](multiprocessing.md)、[同步 Purser：线程池示例](sync-facade.md#threadpool)、[后端配置示例](backend-configuration.md)、[生产检查清单](production-checklist.md)。