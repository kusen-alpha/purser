# 服务快速开始

服务模式同时提供 REST API、Web 控制台与 CLI。`purser serve` 不需要再手写 `create_app(Purser(...))`。

## 1. 启动服务

```bash
pip install -e ".[server,cli]"
purser serve --host 0.0.0.0 --port 8000
```

打开 <http://127.0.0.1:8000> 进入控制台；OpenAPI 文档在 `/docs`。

可选鉴权：

```bash
export PURSER_API_KEY=admin-secret        # 请求头：Authorization: Bearer admin-secret
export PURSER_READONLY_KEY=reader-secret  # 同一请求头，只读角色
```

配置密钥后，读写都需带 `Authorization: Bearer <key>`；写操作要求 admin 角色。**两个密钥都不配置时服务完全开放，匿名请求按 admin 处理**。

## 2. 建池

```bash
curl -X POST http://127.0.0.1:8000/pools \
  -H "Content-Type: application/json" \
  -d '{"name": "proxies", "kind": "proxy"}'
```

`kind` 使用已注册的类型，例如内置的 `proxy` / `account`。自定义类型需要先通过 `ps.register_type(...)` 注册（可用 `create_app(ps)` 自行组装应用）；`PURSER_POOLS_FILE` 只用于恢复已有池的配置，不会注册新类型。

## 3. 导入资源

CLI 方式（文本文件每行支持 `host port` / `id host port` / `id protocol host port tag...`）：

```bash
purser import proxies proxies.txt
purser import proxies proxies.txt --import-validate on_import
```

REST 方式：

```bash
curl -X POST http://127.0.0.1:8000/pools/proxies/resources \
  -H "Content-Type: application/json" \
  -d '{"records": [{"host": "1.2.3.4", "port": 8080}], "import_validate": "batch"}'
```

`batch` 会把本次导入交给后台验活任务，任务 id 就是导入报告里的 `batch_id`。

## 4. 获取与归还

```bash
purser lease proxies --consumer demo
purser release proxies <token> --outcome ok --used 1
```

等价的 REST 调用：

```bash
curl -X POST http://127.0.0.1:8000/pools/proxies/leases \
  -H "Content-Type: application/json" \
  -d '{"consumer": "demo", "timeout": 10}'

curl -X DELETE http://127.0.0.1:8000/pools/proxies/leases/<token> \
  -H "Content-Type: application/json" \
  -d '{"outcome": "ok", "used": 1}'
```

## 5. 观察

```bash
purser status
purser ls proxies --state idle
purser events proxies            # SSE 事件流
```

报表与备份：

```bash
curl "http://127.0.0.1:8000/reports/usage?group_by=consumer"
curl -H "Authorization: Bearer $PURSER_API_KEY" http://127.0.0.1:8000/backup
```

## 持久化与多实例

```bash
# 单机：sqlite 存主数据 + memory 调度
export PURSER_STORAGE=sqlite
export PURSER_SQLITE_PATH=/var/lib/purser/purser.db
export PURSER_AUDIT_PATH=/var/lib/purser/audit.db

# 多实例：sqlite/mysql + redis 协调
export PURSER_COORDINATION=redis
export PURSER_REDIS_URL=redis://localhost:6379

purser serve
```

完整项见 [配置参考](../reference/configuration.md) 与 [多实例部署](../guides/multi-instance.md)。
