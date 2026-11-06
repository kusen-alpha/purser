"""Reconcile and ``reset_usage`` boundary edges (always-run, no external deps).

Covers the storage contract behind reconciliation across memory/sqlite:
``reset_usage(slots=N)`` alignment, negative / over-cap input rejection,
DRAINING with surviving leases, partial-orphan reconcile (only part of the
used slots has a live lease), full-orphan reset, a late release that must not
steal another live holder's slot, and a waiter-cancel stress that guards the
``_serve_waiters`` abandon path.
"""

from __future__ import annotations

import asyncio
import time

import pytest
from conftest import seed

from purser.core.resource import AcquireRequest, Concurrency, HealthStats, Resource
from purser.core.state import ResourceState
from purser.errors import PurserError

POOL = "p0"


def make_resource(
    rid: str,
    *,
    max_slots: int = 2,
    used: int = 0,
    state: ResourceState = ResourceState.IDLE,
) -> Resource:
    return Resource(
        id=rid,
        kind="fakecheck",
        payload={"host": "127.0.0.1", "port": 8080},
        state=state,
        concurrency=Concurrency(max_slots=max_slots, used_slots=used),
        health=HealthStats(score=0.9, samples=3),
        meta={"created_at": time.time()},
    )


@pytest.fixture
def storage(purser, storage_kind):
    from purser.config import PoolConfig

    return purser._make_storage(PoolConfig(kind="fakecheck", storage=storage_kind))


async def test_reset_usage_partial_alignment(storage):
    await storage.put(POOL, make_resource("a", used=2, state=ResourceState.FULL))
    assert await storage.reset_usage(POOL, "a", slots=1) is True
    res = await storage.get(POOL, "a")
    assert res is not None
    assert res.concurrency.used_slots == 1
    assert res.state is ResourceState.ACTIVE
    assert res.payload == {"host": "127.0.0.1", "port": 8080}  # master data intact


async def test_reset_usage_noop_when_aligned(storage):
    await storage.put(POOL, make_resource("a", used=1, state=ResourceState.ACTIVE))
    assert await storage.reset_usage(POOL, "a", slots=1) is False
    res = await storage.get(POOL, "a")
    assert res is not None and res.version == 1
    assert res.concurrency.used_slots == 1


async def test_reset_usage_negative_rejected(storage):
    await storage.put(POOL, make_resource("a", used=1, state=ResourceState.ACTIVE))
    assert await storage.reset_usage(POOL, "a", slots=-1) is False
    res = await storage.get(POOL, "a")
    assert res is not None and res.concurrency.used_slots == 1
    assert res.state is ResourceState.ACTIVE


async def test_reset_usage_over_cap_clamped(storage):
    await storage.put(POOL, make_resource("a", max_slots=2, used=1, state=ResourceState.ACTIVE))
    assert await storage.reset_usage(POOL, "a", slots=5) is True
    res = await storage.get(POOL, "a")
    assert res is not None and res.concurrency.used_slots == 2
    assert res.state is ResourceState.FULL


async def test_reset_usage_draining_survives_with_slots(storage):
    await storage.put(POOL, make_resource("a", used=1, state=ResourceState.DRAINING))
    assert await storage.reset_usage(POOL, "a", slots=1) is True
    res = await storage.get(POOL, "a")
    assert res is not None and res.state is ResourceState.DRAINING
    assert res.concurrency.used_slots == 1
    assert await storage.reset_usage(POOL, "a", slots=0) is True
    res = await storage.get(POOL, "a")
    assert res is not None and res.state is ResourceState.ARCHIVED


async def test_reconcile_partial_orphan_aligns_to_live_lease(make_pool):
    pool = make_pool("px", max_slots=2)
    [rid] = await seed(pool, n=1)
    lease_a = await pool.acquire(AcquireRequest(consumer="a", slots=1))
    lease_b = await pool.acquire(AcquireRequest(consumer="b", slots=1))
    res = await pool.get(rid)
    assert res is not None and res.concurrency.used_slots == 2
    # Simulate the other instance's lease vanishing (TTL/revoke elsewhere)
    # while its storage slot is still counted.
    await pool._coordination.unregister_lease(pool.name, lease_a.token)
    assert [lease.token for lease in await pool._coordination.active(pool.name)] == [
        lease_b.token
    ]
    await pool.start()
    res = await pool.get(rid)
    assert res is not None and res.concurrency.used_slots == 1
    assert res.state is ResourceState.ACTIVE
    assert res.payload == {"host": "127.0.0.1", "port": 8000}
    await pool.release(lease_b)
    res = await pool.get(rid)
    assert res is not None and res.concurrency.used_slots == 0


async def test_reconcile_full_orphan_resets_to_idle(make_pool):
    pool = make_pool("px", max_slots=2)
    [rid] = await seed(pool, n=1)
    lease_a = await pool.acquire(AcquireRequest(consumer="a", slots=1))
    lease_b = await pool.acquire(AcquireRequest(consumer="b", slots=1))
    await pool._coordination.flush(pool.name)
    await pool.start()
    res = await pool.get(rid)
    assert res is not None and res.concurrency.used_slots == 0
    assert res.state is ResourceState.IDLE
    assert await pool._coordination.active(pool.name) == []
    assert lease_a.token != lease_b.token


async def test_reconcile_draining_with_surviving_lease_not_archived(make_pool):
    pool = make_pool("px", max_slots=1)
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="a", slots=1))
    await pool.drain(rid, operator="ops", reason="sunset")
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.DRAINING
    assert res.concurrency.used_slots == 1
    # A live lease must keep the DRAINING resource open, not archive it.
    await pool.start()
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.DRAINING
    assert res.concurrency.used_slots == 1
    # Returning the last lease completes the drain.
    await pool.release(lease)
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.ARCHIVED


async def test_late_release_other_holder_slot_preserved(make_pool):
    pool = make_pool("px", max_slots=2)
    [rid] = await seed(pool, n=1)
    lease_a = await pool.acquire(AcquireRequest(consumer="a", slots=1))
    lease_b = await pool.acquire(AcquireRequest(consumer="b", slots=1))
    # A's token disappears (revoked elsewhere); B still holds a live lease.
    await pool._coordination.unregister_lease(pool.name, lease_a.token)
    assert await pool.release(lease_a) is True
    res = await pool.get(rid)
    # The late release is a no-op: B's live slot is preserved and A's orphan
    # slot is left for the restart reconcile, not stolen or double-freed.
    assert res is not None and res.concurrency.used_slots == 2
    assert [lease.token for lease in await pool._coordination.active(pool.name)] == [
        lease_b.token
    ]
    await pool.start()
    res = await pool.get(rid)
    assert res is not None and res.concurrency.used_slots == 1
    assert await pool.release(lease_b) is True
    res = await pool.get(rid)
    assert res is not None and res.concurrency.used_slots == 0


async def test_waiter_cancel_stress_no_invalid_state_and_no_leak(make_pool):
    pool = make_pool("px", max_slots=1)
    [rid] = await seed(pool, n=1)
    surprises: dict[str, Exception] = {}

    async def holder(i: int) -> None:
        try:
            async with pool.lease(AcquireRequest(consumer=f"h{i}", timeout=0.05)):
                await asyncio.sleep(0.001)
        except (PurserError, asyncio.TimeoutError):
            pass
        except Exception as exc:
            surprises[f"holder{i}"] = exc

    async def canceller(i: int) -> None:
        task = asyncio.create_task(
            pool.acquire(AcquireRequest(consumer=f"x{i}", timeout=0.2))
        )
        await asyncio.sleep(0.0)
        if task.done():
            # It won the race before we cancelled; return the lease cleanly.
            try:
                lease = task.result()
            except (PurserError, asyncio.TimeoutError):
                return
            except Exception as exc:
                surprises[f"done{i}"] = exc
                return
            try:
                await pool.release(lease)
            except Exception as exc:
                surprises[f"done-release{i}"] = exc
            return
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, PurserError, asyncio.TimeoutError):
            pass
        except Exception as exc:
            surprises[f"cancel{i}"] = exc

    await asyncio.gather(
        *(holder(i) for i in range(60)),
        *(canceller(i) for i in range(120)),
    )
    assert surprises == {}, surprises
    res = await pool.get(rid)
    assert res is not None and res.concurrency.used_slots == 0
    assert await pool._coordination.active(pool.name) == []
