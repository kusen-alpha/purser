# 租约

## 端点

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/pools/{pool}/leases` | 获取租约；支持 `count` 批量；支持 `Idempotency-Key` |
| `GET` | `/pools/{pool}/leases` | 活跃租约，`?consumer=&resource_id=` 过滤 |
| `GET` | `/pools/{pool}/leases/{token}` | 单个租约 |
| `POST` | `/pools/{pool}/leases/{token}/renew` | 手动续约 |
| `DELETE` | `/pools/{pool}/leases/{token}` | 归还单个租约 |
| `DELETE` | `/pools/{pool}/leases` | 归还某 consumer 的全部租约 |

## 获取

```bash
curl -X POST http://127.0.0.1:8000/pools/proxies/leases \
  -H "Content-Type: application/json" \
  -d '{
    "consumer": "crawler-a",
    "slots": 1,
    "priority": 0,
    "tags": ["http"],
    "timeout": 10
  }'
```

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `consumer` | `null` | 消费者标识，用于公平队列与去重 |
| `target` | `null` | 目标站点，用于请求级校验与报表 |
| `priority` | `0` | 越高越优先 |
| `slots` | `1` | 占用槽位数 |
| `tags` | `[]` | 资源需包含全部标签 |
| `timeout` | `null` | 等待秒数；不传则用池 `wait_timeout` |
| `resource_id` | `null` | 定向获取；被占用按 timeout 等待 |
| `required_tag` | `null` | 硬约束标签 |
| `use_limit` | `null` | 本租约允许的使用次数 |
| `allow_duplicate` | `false` | 同 consumer 可重复持有同一资源 |
| `ignore_cooldown` | `false` | 忽略冷却 |
| `count` | `0` | 大于 0 时走批量获取 |
| `mode` | `atomic` | 批量模式：`atomic` / `best_effort` |
| `n_min` | `null` | `best_effort` 的最低数量 |

`probe` 可传目标 URL，启用“取得前按目标实时验活”（同 `AcquireRequest.probe`）；`affinity` 仍是未实现的保留字段，传入返回 `422`。

响应：

```json
{
  "leases": [
    {
      "token": "l_...",
      "pool": "proxies",
      "resource_id": "r2",
      "consumer": "crawler-a",
      "slots": 1,
      "started_at": 1731000000.0,
      "expires_at": 1731000060.0,
      "heartbeat_at": 1731000000.0
    }
  ],
  "count": 1
}
```

批量获取额外返回 `requested` / `mode` / `n_min`。`atomic` 任一项不足则整体回滚；`best_effort` 低于 `n_min` 抛 `PoolExhausted`。

## 列表与详情

```bash
curl "http://127.0.0.1:8000/pools/proxies/leases?consumer=crawler-a"
curl http://127.0.0.1:8000/pools/proxies/leases/<token>
```

租约可能由其他实例签发；列表与详情读取协调面，不限于本进程。

## 续约

```bash
curl -X POST http://127.0.0.1:8000/pools/proxies/leases/<token>/renew
```

成功返回新的 `expires_at`；续约被拒返回 `409`。常规场景由心跳自动按 TTL/3 续约，手动续约用于长任务显式保活。

## 归还

```bash
# 单个
curl -X DELETE http://127.0.0.1:8000/pools/proxies/leases/<token> \
  -H "Content-Type: application/json" \
  -d '{"outcome": "ok", "used": 1}'

# 按 consumer 批量
curl -X DELETE "http://127.0.0.1:8000/pools/proxies/leases?consumer=crawler-a&outcome=ok"
```

`outcome` 取值见 `UsageOutcome`（如 `ok` / `captcha` / `timeout` / `preempted`）。token 不匹配抛 `LeaseConflict`；迟到 release（token 已不在协调端）不会触碰新持有者的槽位。

## 获取失败

| 情况 | 结果 |
| --- | --- |
| 池内没有资源 | `PoolEmpty` → `409` |
| 资源忙且等待超时 | `PoolExhausted` → `409` |
| 定向获取被占用超时 | `ResourceBusy` → `409` |
| 候选全部被请求级校验拒绝 | `NoSuitableResource` → `409` |
| 池未启动 | `NotStarted`（嵌入式场景） |
