"""Misc real-surface edges: lease caps, started loops, recall, model units."""

from __future__ import annotations

import asyncio
import json

import fakeredis
from conftest import FakeChecker, seed

from purser import Purser
from purser.backends.serde import (
    decode_health,
    decode_quota,
    decode_site_failures,
    encode_health,
    encode_meta,
    encode_quota,
    encode_site_failures,
)
from purser.core.audit import AuditLog
from purser.core.health import HealthStats
from purser.core.resource import AcquireRequest, Quota, Resource, SiteFailure
from purser.core.state import ResourceState


async def test_max_lease_duration_caps_renewal(make_pool):
    pool = make_pool(max_lease_duration=0.3, lease_ttl=10)
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    assert lease.resource_id == rid
    assert await pool.renew_manual(lease) is True
    assert lease.expires_at <= lease.started_at + 0.3 + 0.1  # capped, not ttl-sized
    await asyncio.sleep(0.5)
    assert await pool.renew_manual(lease) is False  # total lifetime is over


async def test_started_pool_reaper_auto_returns_expired(storage_kind, coordination_kind):
    ps = Purser(
        maintenance_enabled=True,
        storage=storage_kind,
        coordination=coordination_kind,
        sqlite_path=":memory:" if storage_kind == "sqlite" else None,
        redis_client=fakeredis.aioredis.FakeRedis() if coordination_kind == "redis" else None,
    )
    ps.register_type(FakeChecker())
    pool = ps.create_pool("px", "fakecheck", lease_ttl=0.3)
    await ps.start()
    try:
        [rid] = await seed(pool, n=1)
        await pool.acquire(AcquireRequest(consumer="c"))

        async def wait_until_freed() -> None:
            while True:
                res = await pool.get(rid)
                if res is not None and res.concurrency.used_slots == 0:
                    return
                await asyncio.sleep(0.05)

        await asyncio.wait_for(wait_until_freed(), timeout=10.0)
        res = await pool.get(rid)
        assert res is not None and res.state is ResourceState.IDLE
        assert res.concurrency.used_slots == 0
    finally:
        await ps.close()


async def test_recall_batch_retire(make_pool):
    pool = make_pool()
    await pool.import_resources(
        [{"id": "r1", "host": "1.1.1.1", "port": 1}], batch_id="b-retire"
    )
    report = await pool.recall_batch(
        "b-retire", action="retire", operator="ops", reason="recall"
    )
    assert report["processed"] == 1
    res = await pool.get("r1")
    assert res is not None and res.state is ResourceState.DRAINING


async def test_recall_batch_requalify(make_pool):
    pool = make_pool()
    await pool.import_resources(
        [{"id": "r1", "host": "1.1.1.1", "port": 1}], batch_id="b-req"
    )
    report = await pool.recall_batch(
        "b-req", action="requalify", operator="ops", reason="recall"
    )
    assert report["processed"] == 1
    res = await pool.get("r1")
    assert res is not None and res.state is ResourceState.IDLE


async def test_recall_batch_isolate(make_pool):
    pool = make_pool()
    await pool.import_resources(
        [{"id": "r1", "host": "1.1.1.1", "port": 1}], batch_id="b-iso"
    )
    report = await pool.recall_batch(
        "b-iso", action="isolate", operator="ops", reason="hold"
    )
    assert report["processed"] == 1
    res = await pool.get("r1")
    assert res is not None and res.state is ResourceState.QUARANTINED


def test_next_usable_at_merges():
    res = Resource(id="r", kind="k", payload={})
    res.next_available_at = 100.0
    res.expires_at = 200.0
    res.meta["quarantine_until"] = 300.0
    res.meta["cooldown:c"] = 50.0
    res.meta["cooldown:other"] = 400.0
    assert res.next_usable_at(0.0) == 300.0
    assert res.next_usable_at(0.0, consumer="c") == 300.0  # per-consumer max
    assert res.next_usable_at(0.0, consumer="other") == 400.0
    res.meta["quarantine_until"] = 10.0
    assert res.next_usable_at(0.0, consumer="other") == 400.0


def test_serde_roundtrip():
    health = HealthStats(score=0.7, samples=2, last_error="网络错误", last_latency_ms=12.5)
    assert decode_health(encode_health(health)) == health
    quota = Quota(limit=5, used=2, reset_at=123.0)
    decoded = decode_quota(encode_quota(quota))
    assert decoded.limit == 5 and decoded.used == 2 and decoded.reset_at == 123.0
    failures = {"site.com": SiteFailure(site="site.com", count=2, since=1.0, until=2.0)}
    decoded_failures = decode_site_failures(encode_site_failures(failures))
    assert decoded_failures["site.com"].count == 2
    meta = {"中 文": "值", "n": 1}
    assert json.loads(encode_meta(meta)) == meta


def test_audit_paging():
    audit = AuditLog()
    for i in range(5):
        audit.record(operator="o", action=f"a{i}")
    page = audit.entries(after_seq=0, limit=2)
    assert [e.seq for e in page] == [4, 5]  # limit keeps the newest events
    assert [e.seq for e in audit.entries(after_seq=2, limit=10)] == [3, 4, 5]


async def test_purser_start_idempotent():
    ps = Purser(maintenance_enabled=True)
    ps.register_type(FakeChecker())
    await ps.start()
    await ps.start()
    assert ps._running
    await ps.close()
    await ps.close()
