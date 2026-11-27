# 组合租约

组合租约让你**一次请求、同时获取多池资源**，例如 `{account, proxy}` 一套。它解决三件事：把“账号先、代理后”的手写编排纳入库内统一管理；一个账号只配同地区代理这类约束；以及给整套组合上准入配额。默认关闭全部高级流程，普通 `acquire()` / `lease()` 完全不受影响。

## 适用场景

| 场景 | 推荐做法 |
| --- | --- |
| 账号↔代理是**固定 1:1**，永不换 | 用[组合资源](#fixed-binding)（单池一条资源），更简单 |
| 只要成套获取一次，无绑定约束 | `acquire_bundle`（默认 `rollback` 模式） |
| 账号决定代理约束（地区/协议），范围内可换 | `acquire_bundle` + `MatchRule` |
| 多实例共享 Redis 时要求跨实例严格拼装 | `bundle_mode="two_phase"`（或 `strict=True`） |
| 需要限制“某任务组最多同时运行几套”、防止组合耗尽稀缺池 | `bundle_fairness` |

## 固定绑定：先判断是否需要组合租约 {#fixed-binding}

“这个账号永远只用这一个代理”用**组合资源**建模，零跨池编排：

```python
class SessionAdapter(ResourceAdapter):
    kind = "session"
    default_slots = 1

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

## 准备：建池并导入

账号、代理各一个池，数据就是普通的资源记录：

```python
from purser import Purser

ps = Purser()  # 默认 bundle_mode="rollback"、无 fairness，高级流程全关
accounts = ps.create_pool(name="accounts", kind="account")
proxies = ps.create_pool(name="proxies", kind="proxy")

await accounts.import_resources(
    records=[
        {"id": "a1", "username": "u1", "password": "x", "region": "us"},
        {"id": "a2", "username": "u2", "password": "y", "region": "eu"},
    ]
)
await proxies.import_resources(
    records=[
        {"id": "p1", "host": "1.1.1.1", "port": 8080, "protocol": "https", "tags": ["region:us"]},
        {"id": "p2", "host": "2.2.2.2", "port": 8080, "protocol": "https", "tags": ["region:eu"]},
    ]
)
```

## 基础用法：成套获取

```python
from purser import BundleRequest, BundleSlot

bundle = await ps.acquire_bundle(
    request=BundleRequest(
        slots=(
            BundleSlot(pool="accounts"),
            BundleSlot(pool="proxies"),
        ),
        consumer="worker-1",
    )
)
try:
    account = bundle.member(pool="accounts")   # 这是一个普通 Lease，payload 是类型化快照
    proxy = bundle.member(pool="proxies")
    # ... 使用账号与代理执行任务 ...
finally:
    await ps.release_bundle(bundle)
```

也可使用上下文管理器形式：

```python
async with ps.bundle(
    request=BundleRequest(
        slots=(BundleSlot(pool="accounts"), BundleSlot(pool="proxies")),
        consumer="worker-1",
    )
) as bundle:
    account = bundle.member(pool="accounts")
    proxy = bundle.member(pool="proxies")
```

同步代码用 `SyncPurser`，方法同名：

```python
with SyncPurser() as ps:
    bundle = ps.acquire_bundle(request=BundleRequest(slots=(...), consumer="worker-1"))
    try:
        ...
    finally:
        ps.release_bundle(bundle)
```

每个成员都是**普通池租约**，各自在所属池内续约，所以 worker 长时间阻塞在浏览器操作上不影响心跳；只要 `PoolConfig.lease_ttl` / `max_lease_duration` 配得超过单次任务时长。

## 约束配对：一个账号只配同地区代理

`MatchRule` 从先获取到的**锚点成员**取值，给后续成员生成硬过滤 `required_tag=f"{to_tag}:{value}"`：

```python
from purser import MatchRule

bundle = await ps.acquire_bundle(
    request=BundleRequest(
        slots=(
            BundleSlot(pool="accounts", rid="a1"),  # region=us
            BundleSlot(
                pool="proxies",
                match=MatchRule(anchor_pool="accounts", from_field="region", to_tag="region"),
            ),
        ),
        consumer="worker-1",
    )
)
# 代理槽自动变成 required_tag="region:us"，只会获取到 p1
```

`from_field` 先取资源的 typed 属性，再取 payload 键。锚点槽必须排在被匹配槽之前（`scarce_first` 排序会自动保证这一点）。

## 定向绑定：白名单与固定代理

槽里直接给定 `rid` 就是定向获取（获取到指定的那条）：

```python
BundleSlot(pool="proxies", rid="p2")
```

可以把“账号字段里存的代理白名单”变成一个槽列表；要求整套资源必须匹配时，用 `MatchRule` 或 `rid` 组合即可。

## 失败与重试语义

- `atomic`（默认）：任一槽失败，已获取成员逆序回滚，抛 `BundleAcquireFailed`。
- `best_effort`：失败时返回已获取的部分（一个都没获取到同样抛错）。与 `two_phase` 互斥。
- `timeout`：每个槽的等待上限；`0` 表示立即失败。
- `max_retries`：瞬态失败（`PoolEmpty` / `PoolExhausted` / `ResourceBusy`）回滚后按随机退避重试，用于缓解“两个组合反向抢占成员”的死锁循环。
- `bundle_id`：同一 `consumer` 重复获取同 id 返回已持有的组合（幂等）。

## 跨实例严格两阶段（高级，默认关闭）

同一台机器、单调度进程用默认 `rollback` 就够。多实例共享 Redis 时，开启两阶段：

```python
ps = Purser(
    bundle_mode="two_phase",   # 或逐请求 BundleRequest(strict=True)
    reservation_ttl=30.0,      # 拼装期书签 TTL
    bundle_gc_interval=30.0,   # 崩溃回收扫批周期
)
```

开启后成员按 `(pool, rid)` 确定性全序获取（从结构上消除 ABBA 死锁），每获取一个成员写入书签，全部到位 `confirm`；编排进程崩溃时 `bundle_gc` 按书签过期间隔回收 `assembling` 残留，已提交且成员租约仍存活的组合会被保活，不会误撤销。

## 跨池公平配额（高级，默认关闭）

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

准入在**获取任何成员之前**执行；超额抛 `BundleQuotaExceeded` 或排队。`BundleRequest(priority=...)` 决定实例内先唤醒谁。账号多、代理少时，`per_pool_headroom` 保证纯代理请求始终有资源可用，组合不会把稀缺池堵死。

## REST 调用

`POST /pools/{pool}/leases` 带 `bundle` 字段即可组合获取，锚点池必须出现在 `slots` 里：

```bash
curl -X POST http://127.0.0.1:8000/pools/accounts/leases \
  -H 'X-Operator: ops' \
  -d '{
    "consumer": "worker-1",
    "bundle": {
      "order": "scarce_first",
      "slots": [
        {"pool": "accounts", "rid": "a1"},
        {"pool": "proxies", "match": {"anchor_pool": "accounts", "from_field": "region", "to_tag": "region"}}
      ]
    }
  }'
```

响应返回 `bundle_id` 和各成员的租约视图；归还走各成员的 `DELETE /pools/{pool}/leases/{token}`。

## 运维要点

- 归还：成员是普通租约，`release_bundle` 按逆序释放并清理公平计数；忘记归还会导致资源被占满（和普通 lease 一样）。
- 崩溃回收：`bundle_gc()`（同步侧 `SyncPurser.bundle_gc()`）可手动触发，或由 `bundle_gc_interval` 周期扫描；两阶段 + 公平开启时它会自动托管。
- 替换自研调度器：如果你现在手写“先账号后代理、没代理还账号”，组合租约就是这一步的库内替代——`acquire_bundle` 自带回滚，`bundle_gc` 替代手写的心跳失联回收。
- 性能：单实例高频小组合有 IPC/线程提交开销，容量建议见[性能与容量](performance.md)。

详细 API 见 [Purser / 组合租约](../reference/python-api/purser.md)。
