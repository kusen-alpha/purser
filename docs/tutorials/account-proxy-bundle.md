# 教程：账号 + 代理组合租约

目标：一次拿到“账号 + 与账号地区匹配的代理”，两者要么都成功，要么都不占用；任务结束后成套归还。

组合租约（bundle）解决的是**跨池原子性**：普通 `acquire()` 只保证单池内的一次占用，连续两次 `acquire()` 可能在第二次失败时把第一个资源白占住。bundle 会把已拿到的成员回滚释放。

## 0. 数据准备

配对靠标签。约定：

- 账号的自定义 payload 带 `region: "us"`；
- 代理的 `tags` 带 `region:us`。

```python title="setup.py"
from purser import Purser

ps = Purser()

accounts = ps.create_pool("accounts", "account", lease_ttl=300)
proxies = ps.create_pool("proxies", "proxy", lease_ttl=300)

await accounts.import_resources(
    [
        {
            "id": "acct-us-1",
            "username": "user-us-1",
            "password": "secret",
            "region": "us",          # 自定义 payload，MatchRule 会读它
            "tags": ["region:us"],
        },
        {
            "id": "acct-jp-1",
            "username": "user-jp-1",
            "password": "secret",
            "region": "jp",
            "tags": ["region:jp"],
        },
    ]
)

await proxies.import_resources(
    [
        {"host": "1.2.3.4", "port": 8080, "tags": ["region:us", "http"]},
        {"host": "5.6.7.8", "port": 1080, "protocol": "socks5", "tags": ["region:jp", "socks"]},
    ]
)
```

`region` 不是 `AccountResource` 的声明字段，因此会进入自定义 payload，通过 `resource.extra` 读取；`MatchRule` 查找 `from_field` 时先看类型化属性，再看 payload 键，所以直接写 `region` 即可。

## 1. 组合申请

```python title="bundle_job.py"
from purser import (
    AcquireRequest,
    BundleRequest,
    BundleSlot,
    MatchRule,
    UsageOutcome,
)


async def run_job(ps, region: str = "us"):
    bundle = await ps.acquire_bundle(
        BundleRequest(
            slots=(
                BundleSlot(
                    pool="accounts",
                    req=AcquireRequest(tags=frozenset({f"region:{region}"})),
                ),
                BundleSlot(
                    pool="proxies",
                    req=AcquireRequest(tags=frozenset({"http"})),
                    match=MatchRule(
                        anchor_pool="accounts",
                        from_field="region",
                        to_tag="region",
                    ),
                ),
            ),
            consumer="job-42",
            mode="atomic",
            order="scarce_first",
            timeout=20,
            max_retries=2,
        )
    )

    try:
        account = bundle.member("accounts")
        proxy = bundle.member("proxies")
        print(bundle.bundle_id, account.resource_id, proxy.resource_id)
        # ... 用 account.cookie / account.resource.username 与 proxy.resource.host ...
        outcome = {
            "accounts": UsageOutcome.OK,
            "proxies": UsageOutcome.OK,
        }
    except Exception:
        outcome = {
            "accounts": UsageOutcome.OK,
            "proxies": UsageOutcome.NETWORK_ERROR,
        }
        raise
    finally:
        await ps.release_bundle(
            bundle,
            outcome_by_pool=outcome,
            used_by_pool={"accounts": 1, "proxies": 1},
        )
```

`MatchRule` 生成的是硬约束 `required_tag="region:us"`，不是“优先选择”。如果没有任何代理带该标签，这个 bundle 会失败，而不是退而求其次拿一个不匹配的代理。

## 2. 失败语义

| 情况 | `mode="atomic"`（默认） | `mode="best_effort"` |
| --- | --- | --- |
| 第二个成员超时 | 释放第一个成员，抛 `BundleAcquireFailed` | 返回已拿到的成员 |
| 一个成员都没拿到 | 抛 `BundleAcquireFailed` | 同样抛错 |
| 匹配字段缺失 | 抛 `ConfigError`（锚点资源没有该字段） | 同左 |
| 同一 `bundle_id` 重复申请 | 返回已持有的 bundle（幂等） | 同左 |

`order="scarce_first"` 会先拿候选更少的池，降低“拿了热门资源却卡在稀缺资源上”的概率；`match` 槽始终排在锚点之后。

`max_retries` 用于两个 bundle 以相反顺序互相抢占时的抖动重试：每次失败都会先回滚已拿成员，再随机退避重试。它不会引入全局锁。

## 3. 控制准入

默认不做跨池配额。需要限制“每个消费者最多同时持有几个 bundle”或“整个池只允许 N 个 bundle”时：

```python
from purser import BundleFairness, Purser

ps = Purser(
    bundle_fairness=BundleFairness(
        max_per_consumer=2,
        max_total=50,
        per_pool_headroom=1,
        timeout=10,
    )
)
```

`per_pool_headroom=1` 表示申请前要求每个参与池至少还剩 1 个可分配槽位，避免 bundle 把池吃空。准入失败抛 `BundleQuotaExceeded`，此时还没有占用任何成员。

## 4. 固定绑定

如果账号与代理是固定的一对一关系，不需要匹配规则，直接指定资源：

```python
BundleSlot(pool="accounts", resource_id="acct-us-1")
BundleSlot(pool="proxies", resource_id="proxy://1.2.3.4:8080")
```

固定绑定仍然享有“任一失败就回滚”的原子性。更多边界与排序规则见[组合租约](../guides/bundle-leases.md)。

## 5. 检查

```python
print(bundle.ids())                 # {"accounts": "acct-us-1", "proxies": "proxy://..."}
print(await accounts.stats())
print((await ps.report_usage(group_by="consumer"))["totals"])
```

bundle 不迁移在途成员：备份/恢复只保存资源，不保存组合关系。进程崩溃后，成员按普通租约的 TTL 与心跳回收；`bundle_gc` 负责清理两阶段模式下的残留书签。

## 下一步

- 需要把同一套逻辑部署成多实例：看[从脚本到服务](script-to-service.md)与[多实例部署](../guides/multi-instance.md)。
- 需要调公平性：看[组合租约](../guides/bundle-leases.md)的准入与死锁小节。