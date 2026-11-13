# 配置

配置分三层：**部署环境变量 / CLI 参数**决定进程怎么连后端；**`PoolConfig`** 决定单个池怎么调度；**单次调用参数**（如 `AcquireRequest`）只影响本轮请求。

## 环境变量

| 变量 | 作用 | 默认 |
| --- | --- | --- |
| `PURSER_STORAGE` | 存储层 | `memory` |
| `PURSER_SQLITE_PATH` | SQLite 文件 | `:memory:` |
| `PURSER_COORDINATION` | 协调层 | `memory` |
| `PURSER_REDIS_URL` | Redis 连接串；**不会**自动切换协调层 | — |
| `PURSER_MYSQL_URL` | MySQL 连接串 | — |
| `PURSER_AUDIT_PATH` | 审计落库路径 | — |
| `PURSER_SECRET_KEY` | payload 加密主密钥 | — |
| `PURSER_API_KEY` | REST admin 密钥；CLI 也读取 | — |
| `PURSER_READONLY_KEY` | REST 只读密钥 | — |
| `PURSER_POOLS_FILE` | 启动时恢复池配置的 JSON 文件 | — |
| `PURSER_URL` | CLI 的 `--base-url` 默认值 | `http://127.0.0.1:8000` |
| `PURSER_ALLOW_SHARED_BACKENDS` | 仅测试脚手架：放宽同进程后端身份校验 | 未设置 |

`purser serve` 的参数优先于环境变量。多实例必须同时设置 `PURSER_COORDINATION=redis`。

## PoolConfig

创建池时传入，或通过 `PATCH /pools/{pool}` 热更新（不可变项除外）。

### 调度与分配

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `allocator` | `health_weighted` | `round_robin` / `least_loaded` / `static_priority` |
| `scheduler` | `priority_quota` | 目前唯一实现；`fifo` / `token_bucket` 为预留值，传入会报错 |
| `top_k` | `8` | 随机候选数 |
| `max_slots` | `None` | 单资源槽位上限；`None` 由适配器决定 |
| `weights` | 见下 | 打分权重 |
| `affinity_weight` | `0.0` | 亲和性权重 |

`Weights`：`health=1.0`、`age=0.15`、`load=0.30`、`preference=0.0`、`affinity=0.0`、`jitter=0.05`。

### 租约与等待

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `lease_ttl` | `60.0` | 租约寿命（秒） |
| `heartbeat_interval` | `None` | 默认 `lease_ttl/3` |
| `max_lease_duration` | `None` | 单租约总寿命上限 |
| `wait_timeout` | `30.0` | 获取等待上限 |

### 验活

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `import_validate` | `none` | `none` / `on_import` / `batch` |
| `validate_timeout` | `20.0` | 单次验活超时 |
| `validate_concurrency` | `8` | 验活并发 |
| `background_interval` | `30.0` | 后台验活周期 |
| `background_validate_sample` | `1.0` | 每周期抽样比例 |
| `stale_after` | `30.0` | 健康样本过期时间 |
| `site_failure_ttl` | `300.0` | 站点失败记忆时长 |
| `site_failure_max` | `16` | 站点失败条目上限 |
| `check_cache_ttl` | `2.0` | 请求级校验缓存 |

!!! note "探测目标写在资源适配器上"

    代理探测目标不在池配置上设置：请使用 `ProxyAdapter(probe_targets=[...])`。`PoolConfig.probe_targets` 与 `fail_open` 为预留字段，传入非默认值会抛 `ConfigError`（校验失败时始终 fail-closed）。

### 隔离与封禁

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `quarantine.base` | `30.0` | 退避基数 |
| `quarantine.cap` | `1800.0` | 退避上限 |
| `quarantine.jitter` | `10.0` | 抖动 |
| `quarantine.max_failures` | `3` | 封禁阈值 |
| `quarantine.trial_hours` | `24.0` | 试用期；期内阈值减半 |
| `unban_mode` | `mixed` | `manual_only` / `auto_after_ttl` / `mixed` |
| `unban_after` | `86400.0` | 自动解封 TTL |

### 配额、冷却与过期

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `usage_quota` | `None` | 单资源累计用量上限；配合 `quota_cooldown` 得到“用完 N 次 → 冷却 → 自动恢复” |
| `quota_limit` | `None` | 每资源周期配额默认值，导入时应用到未显式设置配额的资源 |
| `expires_in` | `None` | 绝对过期时长；到期默认转 `SUSPENDED`，`expire_policy="quota"` 时转 `QUOTA_EXHAUSTED` |
| `expire_policy` | `suspended` | `suspended` / `quota`：到期后的处置策略 |
| `requalify_on_release` | `False` | 隔离到期时先进 `VALIDATING` 重新校验，而非直接 `IDLE` |
| `expired_retention` | `None` | 过期资源保留时长（秒）；`None` 关闭 `expire_gc`，设置后过期超过该时长的资源由 `expire_gc` 自动归档 |
| `cooldown_after_release` | `0.0` | 归还后冷却 |
| `cooldown_consumer_only` | `False` | 只冷却原消费者 |
| `quota_cooldown` | `0.0` | `usage_quota` 用尽后的冷却秒数；到期由 `quota_reset` 维护任务自动复位为 `IDLE`（`used_count` 清零）。`0` 保持原“扫批即复位”行为 |

### 生命周期

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `archive_retention` | `30d` | 归档保留 |
| `lost_retention` | `4h` | `LOST` 资源判定失效并归档的等待时长 |

### 存储与安全

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `storage` | `memory` | `memory` / `sqlite` / `mysql` |
| `coordination` | `memory` | `memory` / `redis` |
| `secret_fields` | `()` | 额外加密字段 |
| `schema_version` | `1` | 配置模式版本 |
| `event_queue_size` | `10000` | 事件队列容量 |
| `timezone` | `Asia/Shanghai` | 周期任务时区 |
| `scheduler_intervals` | `None` | 维护任务执行间隔覆盖，key 需来自 `MAINTENANCE_NAMES` |
| `meta` | `{}` | 业务自定义元数据 |

### 预留字段（未实现）

!!! warning "预留字段不是功能开关"

    以下字段只为未来版本保留，**传入非默认值会立即抛 `ConfigError`**。不要把它们写进部署配置来“提前启用”。

| 字段 | 唯一允许值 | 原因 |
| --- | --- | --- |
| `preemptible` | `False` | 自动抢占未实现 |
| `preempt_threshold` | `10` | 同上 |
| `drain_timeout` | `30.0` | 仅用于抢占式退役，未实现 |
| `fail_open` | `False` | 校验失败始终 fail-closed |
| `probe_targets` | `None` | 池级探测目标未实现，改用 `ProxyAdapter` |

## 不可热更新项

!!! warning "storage / coordination 只能在建池时确定"

    `PATCH /pools/{pool}` 不能修改：`secret_fields`、`event_queue_size`、`schema_version`。`storage` / `coordination` 属于部署层，修改会返回 `422`；原样回传当前值则被忽略。需要换后端时重建资源池，见[多实例部署](../guides/multi-instance.md)。

## 池注册文件

`PURSER_POOLS_FILE` 指向一个 JSON：

```json
{
  "pools": [
    {"name": "proxies", "kind": "proxy", "config": {"lease_ttl": 120}}
  ]
}
```

启动时按文件恢复缺失的池；不会注册新的资源适配器。
