# 内置资源适配器

`proxy` 与 `account` 是默认目录（default catalog）里的两个普通条目，和 `@register_default_adapter` 注册的自定义适配器遵循同一套规则：kind 全局唯一、重复即报错、每个 `Purser` 实例各自实例化一份。它们在 `purser.adapters` 导入时安装，因此默认可用。

`Purser(default_adapters=...)` 控制构造时装载多少：

| 取值 | 注册表内容 |
| --- | --- |
| `"global"`（默认） | 内置 `proxy` / `account` + 所有 `@register_default_adapter` 条目 |
| `"builtin"` | 只有内置 `proxy` / `account` |
| `"none"` | 空注册表；所有 kind（含 `proxy` / `account`）都需自行 `register_adapter()` |

需要替换内置实现时（例如自定义 `ProxyAdapter` 探测目标），用实例级 `register_adapter()` 覆盖同 kind 的条目；`@register_default_adapter` 不允许重复 kind。

## 对比

| 项目 | `ProxyAdapter` | `AccountAdapter` |
| --- | --- | --- |
| `kind` | `proxy` | `account` |
| 资源类 | `ProxyResource` | `AccountResource` |
| 默认并发 | `1`（独占） | `4`（共享，可用 `max_slots` 覆盖） |
| 验活 | 真实网络探测 | 默认恒通过（人工标记） |
| 典型用法 | 代理 IP 池 | 账号/会话池 |

## `ProxyAdapter`

```python
from purser import ProxyAdapter

ps.register_adapter(
    ProxyAdapter(
        probe_targets=["http://example.com:80", "https://example.com:443"],
        timeout=8.0,
    )
)
```

### 字段

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `host` | 是 | 主机名或 IP |
| `port` | 是 | 整数端口，自动 `int` 转换 |
| `protocol` | 否 | `http`（默认）/ `https` / `socks4` / `socks5` |
| `username` | 否 | 代理认证用户名 |
| `password` | 否 | 代理认证密码，**加密存储**（secret 字段） |
| `country` / `province` / `city` / `district` | 否 | 地理标签 |

字典里未声明的键不会被丢弃，会进入 `Resource.extra` / 自定义 payload。`generate()` 要求 `host` 与 `port`；未提供 `id` 时使用 `proxy://<host>:<port>`。

### 验活

- 构造参数 `probe_targets` 默认 `["http://example.com:80"]`，属于**适配器实例**配置，不是池配置。
- `http` / `https` 走真实 `CONNECT` 请求，`socks4` / `socks5` 走对应握手；支持代理用户名/密码。
- 多目标时至少 `ceil(2n/3)` 个通过才算健康（3 个目标即 2/3），延迟取通过目标的最小值。
- 所有探针都处于退避期时返回“alive + all probes disabled”，避免误判；全部目标同时不可达时暂停判断，而不是把整池判定为失败。
- 每次探测受 `timeout`（默认 8 秒）约束。
- 按目标实时验活：请求带 `AcquireRequest(probe=...)`（REST 同名字段）时，取得该代理前会对目标 URL 实测（复用同一探针），`PASS/FAIL` 走池的请求级校验与缓存；不传 `probe` 维持原来的生命周期探测，行为不变。

> 池配置不提供探测目标字段；探测目标只能配在 `ProxyAdapter` 上。

## `AccountAdapter`

```python
from purser import AccountAdapter

ps.register_adapter(AccountAdapter())
```

### 字段

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `username` | 三选一 | 账号名 |
| `email` | 三选一 | 邮箱 |
| `id` | 三选一 | 自定义标识 |
| `password` | 否 | 加密存储 |
| `cookie` | 否 | 加密存储 |
| `access_token` | 否 | 加密存储 |
| `refresh_token` | 否 | 加密存储 |

`generate()` 要求 `username` / `email` / `id` 至少一个；未提供 `id` 时使用 `account://<username>`。

### 验活

默认 `validate()` **恒返回健康**，不访问网络：账号是否可用通常取决于业务系统，库不替业务做登录判断。需要真实校验时继承 `AccountAdapter` 并覆写：

```python
class MyAccountAdapter(AccountAdapter):
    kind = "my-account"

    async def validate(self, resource):
        ok = await check_login(resource.cookie)
        return HealthReport.ok() if ok else HealthReport.fail("login rejected")
```

也可以在获取路径用 `on_request_check` 做按目标站的判断，而不改变资源状态。

## 常见组合

| 需求 | 配置 |
| --- | --- |
| 账号同时只能开一个会话 | 资源 `mutex_group="account-a"` |
| 账号只在工作时段可用 | 资源 `schedule_window=["08:00", "20:00"]` |
| 每条账号最多用 1000 次 | 资源或池 `usage_quota=1000` |
| 代理按国家筛选 | 导入时写 `country`，获取时用 `tags` 或 `required_tag` |
| 共享资源并发 8 | 池 `max_slots=8`（覆盖适配器的默认并发） |

字段级序列化、`secret_fields` 推导与规范化边界见 [Resource / ResourceAdapter](python-api/resources.md)；自定义适配器见[自定义资源适配器](../guides/custom-resource-adapter.md)。
