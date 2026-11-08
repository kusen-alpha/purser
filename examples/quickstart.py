"""Purser 端到端演示：嵌入式、服务态、Redis、自定义类型。

运行：.venv\\Scripts\\python examples\\quickstart.py
"""

from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from purser import (
    AcquireRequest,
    HealthReport,
    Purser,
    Resource,
    ResourceType,
    UsageOutcome,
)


class LocalChecker(ResourceType):
    """不联网的确定性验活器，适合本地演示与测试。"""

    kind = "local"
    default_concurrency = 2

    async def generate(self, spec: dict):
        return Resource(
            id=spec["id"],
            kind=self.kind,
            payload={"host": spec.get("host", "127.0.0.1"), "port": spec.get("port", 80)},
            tags=frozenset(spec.get("tags") or ()),
        )

    async def validate(self, resource: Resource) -> HealthReport:
        return HealthReport.ok(latency_ms=12.0)


async def embedded() -> None:
    print("== 嵌入式 ==")
    ps = Purser(maintenance_enabled=False)
    ps.register_type(LocalChecker())
    pool = ps.create_pool("demo", "local")
    await pool.import_resources(
        [
            {"id": "n1", "tags": ["US"]},
            {"id": "n2", "tags": ["EU"]},
            {"id": "n3", "tags": ["US"]},
        ],
        import_validate="on_import",
    )
    await ps.start()

    async with pool.lease(AcquireRequest(consumer="crawler", tags=["US"])) as lease:
        print(f"拿到 {lease.resource_id} -> {lease.payload}")
        lease.outcome = UsageOutcome.OK

    stats = await pool.stats()
    print(f"IDLE={stats['counts']['idle']} 可分配={stats['allocatable']} 健康={stats['avg_health']}")
    await ps.close()


async def service() -> None:
    import threading
    import time

    import httpx
    import uvicorn

    from purser.ui.server import create_app

    ps = Purser(maintenance_enabled=False)
    ps.register_type(LocalChecker())
    ps.create_pool("demo", "local")
    app = create_app(ps, api_key=None, readonly_key=None)

    config = uvicorn.Config(app, host="127.0.0.1", port=8011, log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    time.sleep(0.5)

    # trust_env=False keeps local calls off any ambient HTTP proxy, including
    # malformed NO_PROXY values that make httpx fail to parse the URL.
    with httpx.Client(base_url="http://127.0.0.1:8011", trust_env=False) as client:
        client.post(
            "/pools/demo/resources",
            json={"records": [{"id": "s1"}, {"id": "s2"}], "import_validate": "on_import"},
        )
        pools = client.get("/pools").json()
        print("== 服务态 ==")
        print("GET /pools ->", [p["name"] for p in pools["pools"]])
        lease = client.post("/pools/demo/leases", json={"consumer": "remote", "timeout": 5}).json()["leases"][0]
        print("POST /leases ->", lease["resource_id"])
        client.request("DELETE", f"/pools/demo/leases/{lease['token']}", json={"outcome": "ok"})
    server.should_exit = True


async def redis_backend() -> None:
    import fakeredis

    print("== Redis 协调 + SQLite 存储（fakeredis 演示）==")
    ps = Purser(
        maintenance_enabled=False,
        storage="sqlite",
        coordination="redis",
        sqlite_path=":memory:",
        redis_client=fakeredis.aioredis.FakeRedis(),
    )
    ps.register_type(LocalChecker())
    pool = ps.create_pool("shared", "local")
    await pool.import_resources(
        [{"id": f"r{i}"} for i in range(3)], import_validate="on_import"
    )
    await ps.start()
    async with pool.lease(AcquireRequest(consumer="x")) as lease:
        print(f"Redis 池获取 {lease.resource_id}，槽位 {lease.slots}")
    counts = await pool.counts(by_name=True)
    print(f"Redis 池状态分布: {counts}")
    await ps.close()


async def main() -> None:
    await embedded()
    await service()
    await redis_backend()


if __name__ == "__main__":
    asyncio.run(main())
