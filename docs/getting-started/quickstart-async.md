# 嵌入式快速开始

嵌入式模式适合单进程采集任务：`Purser` 直接管理池、租约与后台维护任务，不启动 HTTP 服务。

## 1. 定义资源类型

内置 `proxy` / `account` 需要真实网络或登录态才能验活。离线演示可以注册一个 `validate()` 恒为 OK 的类型：

```python
from purser import HealthReport, Purser, Resource, ResourceType


class LocalType(ResourceType):
    kind = "local"
    default_concurrency = 2

    async def generate(self, spec):
        return Resource(
            id=spec["id"],
            kind=self.kind,
            payload={"host": spec.get("host", "127.0.0.1")},
            tags=frozenset(spec.get("tags") or ()),
        )

    async def validate(self, resource):
        return HealthReport.ok(latency_ms=1.0)
```

## 2. 建池并导入资源

```python
ps = Purser(maintenance_enabled=False)
ps.register_type(LocalType())
pool = ps.create_pool("demo", "local")

await pool.import_resources(
    [
        {"id": "r1", "tags": ["US"]},
        {"id": "r2", "tags": ["EU"]},
    ],
    import_validate="on_import",   # 验活通过后才进入 IDLE
)
```

导入、查询、状态变更等管理面操作可以在 `start()` 之前执行，适合离线预装数据。

## 3. 获取与归还

```python
from purser import AcquireRequest, UsageOutcome

await ps.start()

async with pool.lease(AcquireRequest(consumer="crawler", tags=["US"])) as lease:
    print(lease.resource_id, lease.payload)
    lease.outcome = UsageOutcome.OK

await ps.close()
```

`pool.lease()` 退出时自动按 `lease.outcome` 归还。需要显式控制时使用手动路径：

```python
lease = await pool.acquire(AcquireRequest(consumer="crawler-b"))
try:
    ...
finally:
    await pool.release(lease, outcome=UsageOutcome.OK, used=1)
```

## 4. 查看状态与用量

```python
print(await pool.counts(by_name=True))
print(await pool.stats())

report = ps.report_usage(group_by="consumer")
print(report["totals"])
```

## 完整示例

仓库中的 `examples/quickstart.py` 同时演示嵌入式、服务态与 Redis 协调；运行：

```bash
python examples/quickstart.py
```

## 常见边界

- `acquire()` / `lease()` / `acquire_many()` 要求池已启动，否则抛 `NotStarted`。
- 池内资源全部是 `NEW` 时不会自动验活，`acquire()` 立即抛带提示的 `PoolExhausted`。
- 租约默认 TTL 60 秒，心跳按 TTL/3 续约；可用 `PoolConfig(lease_ttl=...)` 调整。

继续阅读：[租约生命周期](../guides/lease-lifecycle.md)、[导入与验活](../guides/import-and-validate.md)。
