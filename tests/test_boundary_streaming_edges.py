"""Streaming/capacity edges: slot-aware batches, replay, config sanity."""

from __future__ import annotations

import asyncio

import pytest
from conftest import seed

from purser.config import PoolConfig
from purser.core.resource import AcquireRequest
from purser.core.state import ResourceState
from purser.errors import ConfigError, PoolExhausted


async def test_acquire_many_honors_requested_slots(make_pool):
    pool = make_pool(max_slots=4)
    [rid] = await seed(pool, n=1)
    batch = await pool.acquire_many(2, req=AcquireRequest(slots=2))
    assert [lease.slots for lease in batch] == [2, 2]
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.FULL
    assert res.concurrency.used_slots == 4
    await batch.release_all(pool)
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.IDLE


async def test_acquire_many_atomic_rollback_with_slots(make_pool):
    pool = make_pool(max_slots=2)
    [rid] = await seed(pool, n=1)
    with pytest.raises(PoolExhausted):
        await pool.acquire_many(2, req=AcquireRequest(slots=2), timeout=0.1)
    res = await pool.get(rid)
    assert res is not None and res.concurrency.used_slots == 0
    assert res.state is ResourceState.IDLE


async def test_acquire_slots_exceeding_capacity_times_out(make_pool):
    pool = make_pool(max_slots=1)
    [rid] = await seed(pool, n=1)
    with pytest.raises(PoolExhausted):
        await pool.acquire(AcquireRequest(slots=2, timeout=0.1))
    res = await pool.get(rid)
    assert res is not None and res.concurrency.used_slots == 0


async def test_mixed_slot_waiters_no_starvation(make_pool):
    pool = make_pool(max_slots=3)
    [rid] = await seed(pool, n=1)
    h1 = await pool.acquire(AcquireRequest(consumer="h1", slots=2))
    h2 = await pool.acquire(AcquireRequest(consumer="h2", slots=1))  # pool is FULL
    leases: list = []

    async def waiter(name: str, slots: int) -> None:
        lease = await pool.acquire(
            AcquireRequest(consumer=name, slots=slots, timeout=5.0)
        )
        leases.append(lease)

    big = asyncio.create_task(waiter("A", 2))  # cannot fit a single free slot
    await asyncio.sleep(0.1)
    small = asyncio.create_task(waiter("B", 1))
    await asyncio.sleep(0.2)
    assert leases == []
    await pool.release(h2)  # exactly one slot becomes free
    await asyncio.sleep(0.3)
    assert [lease.consumer for lease in leases] == ["B"]  # small waiter served
    await pool.release(leases[0])  # two slots free now
    await pool.release(h1)  # three slots free -> the big waiter can proceed
    await asyncio.sleep(0.3)
    assert len(leases) == 2
    assert leases[1].consumer == "A" and leases[1].slots == 2
    await pool.release(leases[1])
    await asyncio.gather(big, small)
    res = await pool.get(rid)
    assert res is not None and res.concurrency.used_slots == 0


async def test_query_limit_zero_and_counts_consistency(make_pool):
    pool = make_pool()
    await seed(pool, n=3)
    assert await pool.query(limit=0) == []
    counts = await pool.counts()
    assert len(await pool.query()) == sum(counts.values())


async def test_import_over_existing_preserves_state_usage(make_pool):
    pool = make_pool(usage_quota=1)
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    await pool.release(lease, used=1)
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.QUOTA_EXHAUSTED
    await pool.import_resources([{"id": rid, "host": "9.9.9.9", "port": 9}])
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.QUOTA_EXHAUSTED
    assert res.used_count == 1
    assert res.payload["host"] == "9.9.9.9"
    await pool.top_up(rid, operator="ops", amount=1)
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.IDLE


def test_pool_config_rejects_nan_inf():
    with pytest.raises(ConfigError):
        PoolConfig(kind="x", lease_ttl=float("nan"))
    with pytest.raises(ConfigError):
        PoolConfig(kind="x", wait_timeout=float("inf"))
    with pytest.raises(ConfigError):
        PoolConfig(kind="x", heartbeat_interval=-1)
    PoolConfig(kind="x")  # defaults remain valid


async def test_release_rejects_bad_outcome_type(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    with pytest.raises(ConfigError):
        await pool.release(lease, outcome="bogus")
    res = await pool.get(rid)
    assert res is not None and res.concurrency.used_slots == 1  # untouched
    assert await pool.release(lease, outcome=None) is True


async def test_renew_after_release_returns_false(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    await pool.release(lease)
    assert await pool.renew_manual(lease) is False


async def test_concurrent_import_same_records_no_duplicates(make_pool):
    pool = make_pool()
    records = [{"id": "r1", "host": "1.1.1.1", "port": 80}]
    await asyncio.gather(
        pool.import_resources(records), pool.import_resources(records)
    )
    assert len(await pool.query()) == 1
    assert await pool.get("r1") is not None


async def test_force_revoke_clears_consumer_hold(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    await pool.acquire(AcquireRequest(consumer="c"))
    await pool.force_revoke(rid, operator="ops", reason="kick")
    assert pool._consumer_holds.get("c") == set()  # hold cleared
    # The kicked consumer enters a short cooldown (F5): a plain acquire waits.
    with pytest.raises(PoolExhausted):
        await pool.acquire(AcquireRequest(consumer="c", timeout=0.1))
    again = await pool.acquire(
        AcquireRequest(consumer="c", ignore_cooldown=True)
    )
    assert again.resource_id == rid
    await pool.release(again)


async def test_move_then_acquire_from_target(make_pool, purser):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    target = purser.pool("target", "fakecheck")
    moved = await pool.move_resource(rid, target, operator="ops", reason="migrate")
    assert moved.id == rid
    lease = await target.acquire(AcquireRequest(consumer="c"))
    assert lease.resource_id == rid
    await target.release(lease)
