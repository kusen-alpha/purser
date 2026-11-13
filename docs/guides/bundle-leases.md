# 组合租约（一次拿整套账号 + 代理）

组合租约让你**一次申请、同时拿到多池资源**，例如 `{account, proxy}` 一套。它解决三件事：把“账号先、代理后”的手写编排收进库内；一个账号只配同地区代理这类约束；以及给整套组合上准入配额。默认关闭全部高级流程，普通 `acquire()` / `lease()` 完全不受影响。

## 什么时候用，什么时候不用

| 场景 | 推荐做法 |
| --- | --- |
| 账号↔代理是**固定 1:1**，永不换 | 用[组合资源](#0-如果只是固定绑定先想清楚)（单池一条资源），更简单 |
| 只要一次拿一套，无绑定约束 | `acquire_bundle`（默认 `rollback` 模式） |
| 账号决定代理约束（地区/协议），范围内可换 | `acquire_bundle` + `MatchRule` |
| 多实例共享 Redis 时要求跨实例严格拼装 | `bundle_mode="two_phase"`（或 `strict=True`） |
| 需要限制“某任务组最多同时跑几套”、防止组合挤爆稀缺池 | `bundle_fairness` |

## 0. 如果只是固定绑定，先想清楚

“这个账号永远只用这一个代理”用**组合资源**建模，零跨池编排：

```python
class SessionAdapter(ResourceAdapter):
    kind = "session"
    default_concurrency = 1

    async def generate(self, spec):
        return Resource(
            id=spec["id"],
            kind=self.kind,
            payload={k: v for k, v in spec.items() if k not in ("id", "tags")},
        )

    async def validate(self, resource):
        return HealthReport.ok()
```

把成对数据建成单池一条资源：独占、租约、验活、封禁、心跳全部复用现有机制。只有“代理要能在账号之间复用”或“需要约束匹配”时才需要下面的组合租约。

## 1. 准备：建池并导入

账号、代理各一个池，数据就是普通的资源记录：

```python
from purser import Purser

ps = Purser()  # 默认 bundle_mode="rollback"、无 fairness，高级流程全关
accounts = ps.create_pool("accounts", "account")
proxies = ps.create_pool("proxies", "proxy")

await accounts.import_resources([
    {"id": "a1", "username": "u1", "password": "x", "region": "us"},
    {"id": "a2", "username": "u2", "password": "y", "region": "eu"},
])
await proxies.import_resources([
    {"id": "p1", "host": "1.1.1.1", "port": 8080, "protocol": "https", "tags": ["region:us"]},
    {"id": "p2", "host": "2.2.2.2", "port": 8080, "protocol": "https", "tags": ["region:eu"]},
])
```

## 2. 基础用法：一次拿整套

```python
from purser import BundleRequest, BundleSlot

bundle = await ps.acquire_bundle(
    BundleRequest(
        slots=(
            BundleSlot(pool="accounts"),
            BundleSlot(pool="proxies"),
        ),
        consumer="worker-1",
    )
)
try:
    account = bundle.member("accounts")   # 这是一个普通 Lease，payload 是类型化快照
    proxy = bundle.member("proxies")
    # ... 用账号 + 代理干活 ...
finally:
    await ps.release_bundle(bundle)
```

更省心的是上下文管理器形式：

```python
async with ps.bundle(
    BundleRequest(
        slots=(BundleSlot(pool="accounts"), BundleSlot(pool="proxies")),
        consumer="worker-1",
    )
) as bundle:
    account = bundle.member("accounts")
    proxy = bundle.member("proxies")
```

同步代码用 `SyncPurser`，方法同名：

```python
with SyncPurser() as ps:
    bundle = ps.acquire_bundle(BundleRequest(slots=(...), consumer="worker-1"))
    try:
        ...
    finally:
        ps.release_bundle(bundle)
```

每个成员都是**普通池租约**，各自在所属池内续期，所以 worker 长时间阻塞在浏览器操作上不影响心跳；只要 `PoolConfig.lease_ttl` / `max_lease_duration` 配得超过单次任务时长。

## 3. 约束配对：一个账号只配同地区代理

`MatchRule` 从先拿到的**锚点成员**取值，给后续成员生成硬过滤 `required_tag=f"{to_tag}:{value}"`：

```python
from purser import MatchRule

bundle = await ps.acquire_bundle(
    BundleRequest(
        slots=(
            BundleSlot(pool="accounts", resource_id="a1"),  # region=us
            BundleSlot(
                pool="proxies",
                match=MatchRule(anchor_pool="accounts", from_field="region", to_tag="region"),
            ),
        ),
        consumer="worker-1",
    )
)
# 代理槽自动变成 required_tag="region:us"，只会拿到 p1
```

`from_field` 先取资源的 typed 属性，再取 payload 键。锚点槽必须排在被匹配槽之前（`scarce_first` 排序会自动保证这一点）。

## 4. 定向绑定：白名单 / 固定代理

槽里直接给定 `resource_id` 就是定向获取（拿到指定的那条）：

```python
BundleSlot(pool="proxies", resource_id="p2")
```

可以把“账号字段里存的代理白名单”变成一个槽列表；要求整套资源必须匹配时，用 `MatchRule` 或 `resource_id` 组合即可。

## 5. 失败与重试语义

- `atomic`（默认）：任一槽失败，已拿成员逆序回滚，抛 `BundleAcquireFailed`。
- `best_effort`：失败时返回已拿到的部分（一个都没拿到同样抛错）。与 `two_phase` 互斥。
- `timeout`：每个槽的等待上限；`0` 表示立即失败。
- `max_retries`：瞬态失败（`PoolEmpty` / `PoolExhausted` / `ResourceBusy`）回滚后按随机退避重试，用于缓解“两个组合反向抢占成员”的死锁循环。
- `bundle_id`：同一 `consumer` 重复申请同 id 返回已持有的组合（幂等）。

## 6. 跨实例严格两阶段（高级，默认关）

同一台机器、单调度进程用默认 `rollback` 就够。多实例共享 Redis 时，开启两阶段：

```python
ps = Purser(
    bundle_mode="two_phase",   # 或逐请求 BundleRequest(strict=True)
    reservation_ttl=30.0,      # 拼装期书签 TTL
    bundle_gc_interval=30.0,   # 崩溃回收扫批周期
)
```

开启后成员按 `(pool, resource_id)` 确定性全序获取（从结构上消除 ABBA 死锁），每拿一个成员写入书签，全部到位 `confirm`；编排进程崩溃时 `bundle_gc` 按书签过期间隔回收 `assembling` 残留，已提交且成员租约仍存活的组合会被保活，不会误踢。

## 7. 跨池公平配额（高级，默认关）

限制“一次最多几套”“每个任务组最多几套”，并给稀缺池留空位：

```python
from purser import BundleFairness

ps = Purser(
    bundle_fairness=BundleFairness(
        max_per_consumer=5,        # 同一 consumer 并发组合数
        max_total=20,              # 全局并发组合数
        per_pool_headroom=1,       # 每个池为“纯资源请求”保留的空位
        wait=True,                 # 超额时排队等待；False 立即抛 BundleQuotaExceeded
        timeout=30.0,
    ),
)
```

准入在**拿任何成员之前**执行；超额抛 `BundleQuotaExceeded` 或排队。`BundleRequest(priority=...)` 决定实例内先唤醒谁。账号多、代理少时，`per_pool_headroom` 保证纯代理请求始终有资源可用，组合不会把稀缺池堵死。

## 8. REST 调用

`POST /pools/{pool}/leases` 带 `bundle` 字段即可组合获取，锚点池必须出现在 `slots` 里：

```bash
curl -X POST http://127.0.0.1:8000/pools/accounts/leases \
  -H 'X-Operator: ops' \
  -d '{
    "consumer": "worker-1",
    "bundle": {
      "order": "scarce_first",
      "slots": [
        {"pool": "accounts", "resource_id": "a1"},
        {"pool": "proxies", "match": {"anchor_pool": "accounts", "from_field": "region", "to_tag": "region"}}
      ]
    }
  }'
```

响应返回 `bundle_id` 和各成员的租约视图；归还走各成员的 `DELETE /pools/{pool}/leases/{token}`。

## 9. 运维要点

- 归还：成员是普通租约，`release_bundle` 按逆序释放并清理公平计数；忘记归还会导致资源被占满（和普通 lease 一样）。
- 崩溃回收：`bundle_gc()`（同步侧 `SyncPurser.bundle_gc()`）可手动触发，或由 `bundle_gc_interval` 周期扫描；两阶段 + 公平开启时它会自动托管。
- 替换自研调度器：如果你现在手写“先账号后代理、没代理还账号”，组合租约就是这一步的库内替代——`acquire_bundle` 自带回滚，`bundle_gc` 替代手写的心跳失联回收。
- 性能：单实例 high 频次小组合有 IPC/线程提交开销，先看 `benchmarks/sync_facade_bench.py`。

## 10. 对照一下

| 问题 | 答案 |
| --- | --- |
| 要不要开高级开关 | 单机、低频、无配额诉求：什么都不开 |
| 跨实例、要严格一致性 | `bundle_mode="two_phase"` + Redis 协调 |
| 要限并发/防稀缺池被占满 | `bundle_fairness` |
| 固定 1:1 绑定 | 别用 bundle，用组合资源 |

详细 API 见 [Purser / 组合租约](../reference/python-api/purser.md)，设计与边界见 [Bundle v2 设计](../design/bundle-v2.md)。
