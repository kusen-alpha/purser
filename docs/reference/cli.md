# CLI

CLI 面向**已运行的服务**（`purser serve`），除 `serve` 外的命令都通过 HTTP 调用 REST API。

## 连接与鉴权

```bash
export PURSER_URL=http://127.0.0.1:8000     # 或每个命令加 --base-url
export PURSER_API_KEY=admin-secret          # 写操作需要；CLI 只读取这一个密钥
```

CLI 不支持 `PURSER_READONLY_KEY`；只读场景可直接用 `curl`。

## 命令

| 命令 | 说明 | 主要参数 |
| --- | --- | --- |
| `status` | 池概览 | — |
| `ls <pool>` | 资源列表 | `--state`、`--tag` |
| `validate <pool> <rid>` | 立即验活 | `--reason` |
| `ban` / `unban` | 封禁 / 解封 | `--reason` |
| `suspend` / `resume` | 暂停 / 恢复 | `--reason` |
| `top-up <pool> <rid>` | 补充配额 | `--amount` |
| `drain <pool> <rid>` | 退役资源 | `--reason` |
| `lease <pool>` | 获取租约 | `--consumer`、`--target`、`--timeout`、`--keep` |
| `release <pool> <token>` | 归还租约 | `--outcome`、`--used` |
| `import <pool> [file]` | 批量导入 | `--import-validate`、`--idempotency-key` |
| `events <pool>` | 订阅 SSE 事件流 | `--base-url` |
| `serve` | 启动服务 | 见下表 |

`lease` 默认在打印后立即归还；加 `--keep` 保留租约，之后用 `release` 手动归还。

## 示例

```bash
purser status
purser ls proxies --state idle
purser lease proxies --consumer demo --timeout 10 --keep
purser release proxies <token> --outcome ok --used 1

purser import proxies proxies.txt                    # 默认 batch
purser import proxies proxies.txt --import-validate none
purser validate proxies r1 --reason "manual recheck"
purser ban proxies r1 --reason "captcha storm"
purser events proxies
```

导入文件格式：

```text
# 注释
1.2.3.4 8080
r2 5.6.7.8 3128
r3 http 9.9.9.9 8080 http US
["单行 JSON 数组也可直接导入"]
```

## serve

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `--host` | `127.0.0.1` | 监听地址 |
| `--port` | `8000` | 监听端口 |
| `--storage` | 环境变量 / `memory` | `memory` / `sqlite` / `mysql` |
| `--sqlite-path` | — | SQLite 文件 |
| `--coordination` | 环境变量 / `memory` | `memory` / `redis` |
| `--redis-url` | — | Redis 连接串 |
| `--mysql-url` | — | MySQL 连接串 |
| `--audit-path` | — | 审计落库路径 |

```bash
purser serve --host 0.0.0.0 --port 8000 \
  --storage sqlite --sqlite-path ./purser.db \
  --coordination redis --redis-url redis://localhost:6379
```

CLI 参数优先于环境变量；完整项见 [配置参考](configuration.md)。
