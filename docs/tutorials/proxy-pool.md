# 教程：资源消费

目标：把一个代理列表变成可并发获取、失败会反馈、坏代理会自愈的资源池，并完成一次真实抓取。

本教程用 `httpx` 演示；aiohttp 的差异只在“如何把代理拼进请求”，文末给出对应写法。

## 0. 准备

```bash
pip install purser httpx
# 需要 SOCKS 代理时：pip install "httpx[socks]"
```

准备至少一条可达代理，例如 `1.2.3.4:8080`。下面代码里的地址都是占位符。

## 1. 建池与导入

```python title="proxy_pool.py"
import asyncio

from purser import ProxyAdapter, Purser

PROXIES = [
    {"host": "1.2.3.4", "port": 8080, "protocol": "http", "country": "US", "tags": ["http"]},
    {"host": "5.6.7.8", "port": 1080, "protocol": "socks5", "country": "JP", "tags": ["socks"]},
]


async def build_pool() -> Purser:
    ps = Purser()
    # 探针目标是适配器实例配置，不是池配置
    ps.register_adapter(
        adapter=ProxyAdapter(probe_targets=["https://example.com"], timeout=8.0)
    )
    pool = ps.create_pool(
        name="proxies",
        kind="proxy",
        lease_ttl=120,   # 一次抓取通常不超过 2 分钟
        wait_timeout=15,
    )
    await pool.import_resources(records=PROXIES, source="tutorial")
    return ps
```

几个容易踩的点：

- `tags` 是调度标签，导入时写成列表即可；`host` / `port` / `country` 是类型化字段，`port` 会自动转成 `int`。
- `import_validate` 默认为 `auto`：这里只有 2 条记录，会在导入时同步验活。代理不可达时，导入报告里能看到 `alive=false`，资源进入隔离而不是报错中断。
- 同一个 `host:port` 不带 `id` 时会生成 `proxy://<host>:<port>` 作为资源 id；重复导入是更新，不是插入副本。

## 2. 一次抓取

```python title="crawl.py"
import httpx

from purser import AcquireRequest, PoolExhausted, Purser, UsageOutcome


def proxy_url(resource) -> str:
    auth = ""
    if resource.username:
        auth = f"{resource.username}:{resource.password}@"
    return f"{resource.protocol}://{auth}{resource.host}:{resource.port}"


async def fetch(pool, url: str, consumer: str = "crawler-a") -> str:
    req = AcquireRequest(
        consumer=consumer,
        tags=frozenset({"http"}),
        target=url,          # 写入报表与站点失败索引
        timeout=10,
    )
    try:
        async with pool.lease(req) as lease:
            resource = lease.resource
            try:
                async with httpx.AsyncClient(
                    proxy=proxy_url(resource),
                    timeout=20.0,
                    follow_redirects=True,
                ) as client:
                    resp = await client.get(url)
                    resp.raise_for_status()
                    lease.outcome = UsageOutcome.OK
                    lease.used = 1
                    return resp.text
            except httpx.TimeoutException:
                lease.outcome = UsageOutcome.TIMEOUT
                raise
            except httpx.ProxyError:
                lease.outcome = UsageOutcome.NETWORK_ERROR
                raise
            except httpx.HTTPStatusError as exc:
                code = exc.response.status_code
                if code in (403, 407):
                    lease.outcome = UsageOutcome.BANNED
                elif code == 429:
                    lease.outcome = UsageOutcome.RATE_LIMITED
                raise
    except PoolExhausted:
        # 当前没有可分配代理；交给上层退避重试，而不是把它当成抓取失败
        raise
```

`async with pool.lease(...)` 退出时会按 `lease.outcome` 自动归还，异常路径也会归还。归还不是“放回列表”这么简单，它同时更新：

| 结果 | 池内效果 |
| --- | --- |
| `ok` | 计数、最近使用时间、健康样本 |
| `rate_limited` | 记录 `target` 失败；没有单独配置冷却时至少冷却 5 秒 |
| `captcha` | 记录站点失败，资源本身仍可用 |
| `timeout` / `network_error` / `banned` | 降低健康度并标记 `needs_validate`，下次使用前可能重新验活 |
| `quota_exhausted` | 正常计量，是否进入 `QUOTA_EXHAUSTED` 由配额判定 |

完整副作用见[AcquireRequest / Lease / BatchLease](../reference/python-api/acquisition.md#usageoutcome)。

## 3. 并发执行

每个并发任务都应该用自己的 `consumer`，否则同消费者重复获取同一资源时默认返回已有租约：

```python title="main.py"
import asyncio

from purser import PoolExhausted


async def crawl_all(urls: list[str], workers: int = 8) -> None:
    ps = await build_pool()
    pool = ps.get_pool(name="proxies")
    sem = asyncio.Semaphore(workers)

    async def one(i: int, url: str) -> None:
        async with sem:
            for attempt in range(3):
                try:
                    html = await fetch(pool, url, consumer=f"worker-{i}")
                    print(url, "ok", len(html))
                    return
                except PoolExhausted:
                    await asyncio.sleep(2 ** attempt)
                except Exception as exc:  # 代理失败或目标失败
                    print(url, type(exc).__name__, exc)
                    return
            print(url, "gave up: no proxy available")

    await asyncio.gather(*(one(i, url) for i, url in enumerate(urls)))
    await ps.close()


if __name__ == "__main__":
    asyncio.run(crawl_all(["https://example.com"] * 20))
```

- 并发上限由 `workers` 与池内槽位共同决定。代理默认 `max_slots=1`（独占），账户默认 4（共享）；槽位不够时 `acquire()` 会在 `wait_timeout` 内等待，超时抛 `PoolExhausted`。
- 想限制单个消费者同时持有的租约数，建池时设置 `max_leases_per_consumer=4`，而不是把并发数写死在业务里。
- 需要一次获取多个代理再统一归还时用 `acquire_many()`，见[批量获取](../reference/python-api/acquisition.md#batch-acquire)。

## 4. 失败后的自愈机制

```python
from purser import UsageOutcome

await pool.release(lease, outcome=UsageOutcome.NETWORK_ERROR, used=1)
```

连续失败会依次触发：

1. **健康度下降**：分配器降低该资源的排序权重，但不会立刻停止分配。
2. **隔离**：失败次数达到 `quarantine.max_failures`（默认 3）后进入 `QUARANTINED`，按 `quarantine.base` 到 `quarantine.cap` 指数退避。
3. **封禁**：在试用期外达到阈值会进入 `BANNED`，是否自动解封由 `unban_mode` 决定。

不想让目标站点的 5xx 把代理“误杀”时，只在确定是代理侧问题时上报 `network_error` / `banned`；目标站点错误可以用 `ok` 正常归还，或单独用 `target` 做站点失败统计。

需要人工处置时：

```python
await pool.admin.ban(rid="proxy://1.2.3.4:8080", reason="stolen credential")
await pool.admin.unban(rid="proxy://1.2.3.4:8080", revalidate=True)
```

## 5. 观察结果

```python
print(await pool.stats())
print(await pool.counts(by_name=True))
print((await ps.report_usage(group_by="consumer"))["totals"])
```

服务模式下等价入口：

```bash
purser status
purser ls proxies --state quarantined
purser events proxies
curl "http://127.0.0.1:8000/reports/usage?group_by=consumer"
```

## 6. aiohttp 写法

```python
import aiohttp

async with aiohttp.ClientSession() as session:
    async with session.get(url, proxy=proxy_url(resource), timeout=aiohttp.ClientTimeout(total=20)) as resp:
        resp.raise_for_status()
        return await resp.text()
```

aiohttp 的 SOCKS 支持需要 `aiohttp_socks`，把 `proxy=` 换成对应的连接器。其余失败归类与归还逻辑保持不变。

## 下一步

- 需要多个资源必须成套获取：看[组合租约实战](account-proxy-bundle.md)。
- 需要把脚本变成常驻服务：看[从脚本到服务](script-to-service.md)。
- 代理总被误判：查[验活、召回与封禁](validate-and-recall.md)与[公平性与健康度](../guides/fairness-and-health.md)。