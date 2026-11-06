"""Lifecycle edges: shutdown wakes waiters, reap resilience, duplicate runs."""

from __future__ import annotations

import asyncio
import time

import pytest
from conftest import seed

from purser.backends.base import ResourcePatch
from purser.core.lease import LeaseManager
from purser.core.resource import AcquireRequest, Lease
from purser.core.state import ResourceState
from purser.errors import PoolExhausted


async def test_close_fails_inflight_waiters_promptly(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    holder = await pool.acquire(AcquireRequest(consumer="holder"))
    assert holder.resource_id == rid
    waiter = asyncio.create_task(
        pool.acquire(AcquireRequest(consumer="waiter", timeout=30.0))
    )
    await asyncio.sleep(0.2)  # let the waiter enqueue
    await pool.close()
    with pytest.raises(PoolExhausted):
        await asyncio.wait_for(waiter, timeout=2.0)
    assert pool._waiters == []


async def test_reap_survives_failing_on_expire():
    manager = LeaseManager()
    reported: list[str] = []

    async def on_expire(lease: Lease) -> None:
        if lease.consumer == "boom":
            raise RuntimeError("reap handler failed")
        reported.append(lease.consumer or "")

    manager.on_expire = on_expire
    lease_a = Lease.new("p", "a", consumer="a", ttl=-1.0)
    lease_b = Lease.new("p", "b", consumer="b", ttl=-1.0)
    lease_boom = Lease.new("p", "x", consumer="boom", ttl=-1.0)
    manager._leases = {
        lease_a.token: lease_a,
        lease_b.token: lease_b,
        lease_boom.token: lease_boom,
    }
    await manager.reap()
    assert set(reported) == {"a", "b"}  # one bad handler does not stop the sweep


async def test_concurrent_duplicate_maintenance_safe(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    await pool._on_validation_fail(rid, "flaky", operator="system")
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.QUARANTINED
    await pool.storage.set_state(
        pool.name,
        rid,
        res.version,
        expect_state=res.state,
        patch=ResourcePatch(meta_patch={"quarantine_until": time.time() - 1.0}),
    )
    results = await asyncio.gather(
        pool.run_maintenance("quarantine_gc"),
        pool.run_maintenance("quarantine_gc"),
    )
    # Concurrent duplicate sweeps are idempotent: at least one moved it and no
    # double-transition corrupts the resource (a second run re-applies IDLE).
    assert sum(r["quarantine_gc"] for r in results) >= 1
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.IDLE
    assert res.concurrency.used_slots == 0


async def test_allow_duplicate_second_lease_same_consumer(make_pool):
    pool = make_pool(max_slots=2)
    [rid] = await seed(pool, n=1)
    first = await pool.acquire(AcquireRequest(consumer="c"))
    second = await pool.acquire(
        AcquireRequest(consumer="c", allow_duplicate=True)
    )
    assert first.resource_id == second.resource_id == rid
    assert first.token != second.token
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.FULL
    assert res.concurrency.used_slots == 2
    await pool.release(first)
    await pool.release(second)
    res = await pool.get(rid)
    assert res is not None and res.concurrency.used_slots == 0
