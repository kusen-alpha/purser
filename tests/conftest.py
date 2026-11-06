from __future__ import annotations

import os
import uuid

import fakeredis
import pytest
from redis import asyncio as aioredis

from purser import Purser
from purser.core.health import HealthReport
from purser.core.resource import Resource
from purser.types.base import ResourceType


class FakeChecker(ResourceType):
    """Deterministic checker: alive unless programmed to fail."""

    kind = "fakecheck"
    default_concurrency = 1

    def __init__(self, *, alive: bool = True, latency_ms: float = 10.0, fail_until: int = 0) -> None:
        self.alive_ok = alive
        self.latency_ms = latency_ms
        self.fail_until = fail_until

    async def generate(self, spec: dict):
        rid = spec.get("id") or f"r-{uuid.uuid4().hex[:8]}"
        return Resource(
            id=rid,
            kind=self.kind,
            payload={"host": spec.get("host", "127.0.0.1"), "port": int(spec.get("port", 80))},
            tags=frozenset(spec.get("tags") or ()),
            mutex_group=spec.get("mutex_group"),
            meta={"source": spec.get("source", "manual")},
        )

    async def validate(self, resource: Resource) -> HealthReport:
        if not self.alive_ok:
            return HealthReport.fail("checker programmed dead")
        if self.fail_until > 0:
            self.fail_until -= 1
            return HealthReport.fail("transient failure")
        return HealthReport.ok(latency_ms=self.latency_ms)


REAL_REDIS_URL = os.environ.get("PURSER_TEST_REDIS_URL")


@pytest.fixture(autouse=True)
async def _real_redis_isolation():
    """Flush the dedicated Redis DB around every test when a real server is set.

    Coordination state is transient, so a shared real Redis cannot leak state
    between tests.  Point ``PURSER_TEST_REDIS_URL`` at a dedicated DB such as
    ``redis://127.0.0.1:7001/15``, never at a shared instance.
    """
    if not REAL_REDIS_URL:
        yield
        return
    client = aioredis.Redis.from_url(REAL_REDIS_URL, protocol=2)
    try:
        await client.flushdb()
    finally:
        await client.aclose()
    yield
    client = aioredis.Redis.from_url(REAL_REDIS_URL, protocol=2)
    try:
        await client.flushdb()
    finally:
        await client.aclose()


@pytest.fixture(params=["memory", "sqlite"])
def storage_kind(request: pytest.FixtureRequest) -> str:
    return request.param


@pytest.fixture(params=["memory", "redis"])
def coordination_kind(request: pytest.FixtureRequest) -> str:
    return request.param


@pytest.fixture
async def purser(storage_kind: str, coordination_kind: str):
    kwargs: dict = dict(
        maintenance_enabled=False,
        storage=storage_kind,
        coordination=coordination_kind,
    )
    if storage_kind == "sqlite":
        kwargs["sqlite_path"] = ":memory:"
    if coordination_kind == "redis":
        if REAL_REDIS_URL:
            kwargs["redis_url"] = REAL_REDIS_URL
        else:
            kwargs["redis_client"] = fakeredis.aioredis.FakeRedis()
    ps = Purser(
        **kwargs,
    )
    ps.register_type(FakeChecker())
    yield ps
    await ps.close()


@pytest.fixture
def make_pool(purser: Purser):
    created: list = []

    def factory(name: str = "pool", **cfg):
        pool = purser.pool(name, "fakecheck", **cfg)
        created.append(pool)
        return pool

    return factory


async def seed(pool, n: int = 3, *, tags=None, prefix="r") -> list[str]:
    records = [
        {"id": f"{prefix}-{i}", "host": "127.0.0.1", "port": 8000 + i, "tags": list(tags or ())}
        for i in range(n)
    ]
    await pool.import_resources(records, import_validate="on_import")
    return [r["id"] for r in records]
