"""Additional boundary scenarios: timeouts, outcomes, admin races, restart."""

from __future__ import annotations

import asyncio
import time

import fakeredis
import pytest
from conftest import REAL_REDIS_URL, FakeChecker, seed

from purser import Purser
from purser.core.resource import AcquireRequest, UsageOutcome
from purser.core.state import ResourceState
from purser.errors import BadState, PoolExhausted


async def test_waiter_timeout_leaves_no_stale_waiter(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    holder = await pool.acquire(AcquireRequest(consumer="holder"))
    assert holder.resource_id == rid
    with pytest.raises(PoolExhausted):
        await pool.acquire(AcquireRequest(consumer="late", timeout=0.1))
    assert pool._waiters  # the cancelled handle is still queued
    await pool.release(holder)
    assert pool._waiters == []  # the drain swept the stale handle
    ok = await pool.acquire(AcquireRequest(consumer="next"))
    assert ok is not None
    await pool.release(ok)


async def test_directed_acquire_bad_state(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    await pool.ban(rid, operator="ops", reason="edge")
    with pytest.raises(BadState):
        await pool.acquire(AcquireRequest(resource_id=rid, timeout=0.2))


@pytest.mark.parametrize(
    "outcome",
    [
        UsageOutcome.OK,
        UsageOutcome.NETWORK_ERROR,
        UsageOutcome.RATE_LIMITED,
        UsageOutcome.PREEMPTED,
        UsageOutcome.BANNED,
    ],
)
async def test_usage_outcome_side_effects(make_pool, outcome):
    pool = make_pool(cooldown_after_release=0.0)
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c", target="site.com"))
    await pool.release(lease, outcome=outcome)
    res = await pool.get(rid)
    assert res is not None
    now = time.time()

    if outcome is UsageOutcome.OK:
        assert res.state is ResourceState.IDLE
        assert res.health.consecutive_failures == 0
        assert res.used_count == 1
    elif outcome is UsageOutcome.NETWORK_ERROR:
        assert res.state is ResourceState.IDLE
        assert res.health.last_error == "network_error"
        assert res.meta.get("needs_validate") is not None
        assert res.site_failures.get("site.com") is not None
    elif outcome is UsageOutcome.RATE_LIMITED:
        assert res.state is ResourceState.IDLE
        assert res.next_available_at is not None and res.next_available_at > now
        assert res.site_failures.get("site.com") is not None
    elif outcome is UsageOutcome.PREEMPTED:
        assert res.state is ResourceState.IDLE
        assert res.meta.get("cooldown:c") is not None and res.meta["cooldown:c"] > now
    elif outcome is UsageOutcome.BANNED:
        # "usage banned" escalates straight to a ban.
        assert res.state is ResourceState.BANNED
        assert res.health.consecutive_failures == 2
        assert "usage banned" in res.meta.get("banned_reason", "")


async def test_move_and_update_rejected_while_busy(make_pool, purser):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    with pytest.raises(BadState):
        await pool.update_resource(rid, operator="ops", reason="x", tags=frozenset())
    target = purser.pool("target", "fakecheck")
    with pytest.raises(BadState):
        await pool.move_resource(rid, target, operator="ops", reason="x")
    await pool.release(lease)
    moved = await pool.move_resource(rid, target, operator="ops", reason="migrate")
    assert moved.id == rid
    assert await pool.get(rid) is None
    res = await target.get(rid)
    assert res is not None and res.payload["host"] == "127.0.0.1"


async def test_expired_lease_reconciled_by_clock(make_pool):
    pool = make_pool(lease_ttl=0.3)
    [rid] = await seed(pool, n=1)
    await pool.acquire(AcquireRequest(consumer="clock"))
    await asyncio.sleep(0.6)
    await pool._reconcile_leases()
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.IDLE
    assert res.concurrency.used_slots == 0


async def test_two_instances_shared_sqlite_redis_no_oversell(
    tmp_path, storage_kind, coordination_kind, shared_backend_harness
):
    if storage_kind != "sqlite" or coordination_kind != "redis":
        pytest.skip("sqlite + redis combination only")
    if not REAL_REDIS_URL:
        pytest.skip("real Redis required for the shared-backend instance test")
    path = str(tmp_path / "shared.db")

    def build() -> Purser:
        ps = Purser(
            maintenance_enabled=False,
            storage="sqlite",
            coordination="redis",
            sqlite_path=path,
            redis_url=REAL_REDIS_URL,
        )
        ps.register_adapter(FakeChecker())
        return ps

    ps1, ps2 = build(), build()
    try:
        p1 = ps1.create_pool("shared", "fakecheck")
        p2 = ps2.create_pool("shared", "fakecheck")
        await ps1.start()
        await ps2.start()
        await p1.import_resources(
            [{"id": "r1", "host": "1.1.1.1", "port": 1}], import_validate="on_import"
        )
        outcomes = await asyncio.gather(
            p1.acquire(AcquireRequest(consumer="i1", timeout=1.0)),
            p2.acquire(AcquireRequest(consumer="i2", timeout=1.0)),
            return_exceptions=True,
        )
        ok = [o for o in outcomes if not isinstance(o, Exception)]
        assert len(ok) == 1
        res = await p1.get("r1")
        assert res is not None and res.concurrency.used_slots == 1
        await p1.release(ok[0])
    finally:
        await ps1.close()
        await ps2.close()


async def test_double_close_safe(purser, make_pool):
    pool = make_pool()
    await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    await pool.release(lease)
    await purser.close()
    await purser.close()  # idempotent
    del pool


async def test_restart_batch_import_background_drained(tmp_path, storage_kind, coordination_kind):
    if storage_kind != "sqlite":
        pytest.skip("persistent storage required")
    path = str(tmp_path / "batch.db")
    ps1 = Purser(
        maintenance_enabled=False,
        storage="sqlite",
        coordination=coordination_kind,
        sqlite_path=path,
        redis_client=fakeredis.aioredis.FakeRedis() if coordination_kind == "redis" else None,
    )
    ps1.register_adapter(FakeChecker())
    pool = ps1.create_pool("px", "fakecheck")
    report = await pool.import_resources(
        [{"id": f"r{i}", "host": "1.1.1.1", "port": i} for i in range(4)],
        import_validate="batch",
    )
    assert report.imported == 4
    await ps1.close()  # drains in-flight background validation

    ps2 = Purser(
        maintenance_enabled=False,
        storage="sqlite",
        coordination=coordination_kind,
        sqlite_path=path,
        redis_client=fakeredis.aioredis.FakeRedis() if coordination_kind == "redis" else None,
    )
    ps2.register_adapter(FakeChecker())
    try:
        pool2 = ps2.create_pool("px", "fakecheck")
        assert len(await pool2.query()) == 4
    finally:
        await ps2.close()
