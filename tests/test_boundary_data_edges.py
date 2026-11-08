"""Data-validation, quota/use-limit and heartbeat-resilience edges."""

from __future__ import annotations

import asyncio

import fakeredis
import pytest
from conftest import FakeChecker, seed

from purser import Purser
from purser.core.lease import LeaseManager
from purser.core.resource import AcquireRequest, Lease
from purser.core.state import ResourceState
from purser.errors import ConfigError


async def test_release_negative_used_rejected(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    with pytest.raises(ConfigError):
        await pool.release(lease, used=-1)
    # Nothing was mutated: the slot is still held and the lease is untouched.
    res = await pool.get(rid)
    assert res is not None and res.concurrency.used_slots == 1
    assert await pool._coordination.get_lease(pool.name, lease.token) is not None
    assert await pool.release(lease) is True


async def test_release_zero_used_is_noop(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    assert await pool.release(lease, used=0) is True
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.IDLE
    assert res.used_count == 0


async def test_use_limit_blocks_renewal(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    exhausted = await pool.acquire(AcquireRequest(consumer="a", use_limit=0))
    assert exhausted.resource_id == rid
    assert await pool.renew_manual(exhausted) is False  # already at the limit
    await pool.release(exhausted)

    lease = await pool.acquire(AcquireRequest(consumer="b", use_limit=2))
    lease.used = 1
    assert await pool.renew_manual(lease) is True
    lease.used = 2
    assert await pool.renew_manual(lease) is False
    await pool.release(lease)


async def test_top_up_rejects_nonpositive(make_pool):
    pool = make_pool(usage_quota=1)
    [rid] = await seed(pool, n=1)
    with pytest.raises(ConfigError):
        await pool.top_up(rid, operator="ops", amount=0)
    with pytest.raises(ConfigError):
        await pool.top_up(rid, operator="ops", amount=-1)
    await pool.top_up(rid, operator="ops")  # full reset


async def test_update_resource_rejects_bad_max_slots(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    with pytest.raises(ConfigError):
        await pool.update_resource(rid, operator="ops", reason="x", max_slots=0)
    with pytest.raises(ConfigError):
        await pool.update_resource(rid, operator="ops", reason="x", max_slots=-1)
    updated = await pool.update_resource(rid, operator="ops", reason="x", max_slots=2)
    assert updated.concurrency.max_slots == 2


async def test_empty_import_report(make_pool):
    pool = make_pool()
    report = await pool.import_resources([])
    assert report.imported == 0 and report.errors == [] and report.batch_id
    assert await pool.counts() == {s: 0 for s in ResourceState}


async def test_drain_idempotent(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    await pool.drain(rid, operator="ops", reason="once")
    await pool.drain(rid, operator="ops", reason="twice")  # no-op, no error
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.DRAINING


async def test_restore_archived_requalifies(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    await pool.drain(rid, operator="ops", reason="retire")
    await pool.release(lease)
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.ARCHIVED
    await pool.restore(rid, operator="ops", reason="back")
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.IDLE


async def test_restart_partial_active_reconciled(tmp_path, storage_kind, coordination_kind):
    if storage_kind != "sqlite":
        pytest.skip("persistent storage required")
    path = str(tmp_path / "partial.db")

    def build() -> Purser:
        ps = Purser(
            maintenance_enabled=False,
            storage="sqlite",
            coordination=coordination_kind,
            sqlite_path=path,
            redis_client=fakeredis.aioredis.FakeRedis() if coordination_kind == "redis" else None,
        )
        ps.register_type(FakeChecker())
        return ps

    ps1 = build()
    pool = ps1.create_pool("px", "fakecheck", max_slots=2)
    await ps1.start()
    [rid] = await seed(pool, n=1)
    await pool.acquire(AcquireRequest(consumer="c"))  # ACTIVE: 1 of 2 slots used
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.ACTIVE
    await ps1.close()

    ps2 = build()
    try:
        pool2 = ps2.create_pool("px", "fakecheck", max_slots=2)
        await pool2.start()
        res = await pool2.get(rid)
        assert res is not None and res.state is ResourceState.IDLE
        assert res.concurrency.used_slots == 0
        assert res.concurrency.max_slots == 2
    finally:
        await ps2.close()


async def test_heartbeat_transient_errors_retried_then_lost():
    manager = LeaseManager(heartbeat_interval=0.01, max_renew_errors=3)
    lease = Lease.new("p", "r", ttl=3600.0)
    lost = asyncio.Event()
    calls = {"n": 0}

    async def on_lost(item):
        lost.set()

    manager.on_lost = on_lost

    async def renew(item) -> bool:
        calls["n"] += 1
        n = calls["n"]
        if n <= 2:
            raise OSError("transient blip")
        if n >= 6:
            raise OSError("hard down")
        return True

    manager.register(lease, renew)
    try:
        for _ in range(150):
            if calls["n"] >= 4:
                break
            await asyncio.sleep(0.02)
        assert calls["n"] >= 4  # transients were retried, lease survived
        assert not lost.is_set()
        await asyncio.wait_for(lost.wait(), timeout=2.0)  # hard failures -> lost
    finally:
        await manager.close()
