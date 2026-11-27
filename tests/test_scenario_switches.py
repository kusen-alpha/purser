"""Configuration-driven scenario switches on the normal acquire/release flow."""

from __future__ import annotations

import asyncio
import time

import pytest
from conftest import seed

from purser import Allocator, allocator_names, register_allocator
from purser.core.allocator import AllocationContext
from purser.errors import BackendUnavailable, ConfigError, PoolExhausted
from purser.pool.leases import LeaseService


async def _wait_queued(pool, count: int, *, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while len(pool._waiters) < count:
        if time.monotonic() >= deadline:
            raise AssertionError(f"only {len(pool._waiters)}/{count} waiters queued")
        await asyncio.sleep(0.001)


async def test_stateless_mode_does_not_occupy(make_pool):
    pool = make_pool(acquire_mode="stateless", allocator="random")
    await seed(pool, 1)
    leases = [await pool.acquire(timeout=0) for _ in range(3)]
    assert len({lease.token for lease in leases}) == 3
    assert await pool.leases() == []
    resource = (await pool.query())[0]
    assert resource.slots.available == 1
    await pool.release(leases[0], used=2)
    # Second release of the same borrow stays a safe no-op.
    assert await pool.release(leases[0]) is True
    assert (await pool.query())[0].used_count == 2


async def test_random_allocator_spreads_picks(make_pool):
    pool = make_pool(allocator="random")
    await seed(pool, 3)
    picked = set()
    for _ in range(12):
        lease = await pool.acquire(timeout=0)
        picked.add(lease.rid)
        await pool.release(lease)
    assert len(picked) > 1


def test_allocator_registry_round_trip():
    class AlwaysLast(Allocator):
        name = "always_last"

        def rank(self, candidates, ctx: AllocationContext):
            return [(resource, 0.0) for resource in reversed(candidates)]

    register_allocator(AlwaysLast)
    assert "always_last" in allocator_names()
    assert "random" in allocator_names()

    class Duplicate(Allocator):
        name = "always_last"

        def rank(self, candidates, ctx: AllocationContext):
            return []

    with pytest.raises(ConfigError):
        register_allocator(Duplicate)


async def test_consumer_cap(make_pool):
    pool = make_pool(max_leases_per_consumer=1)
    await seed(pool, 2)
    await pool.acquire(consumer="alice", timeout=0)
    with pytest.raises(PoolExhausted):
        await pool.acquire(consumer="alice", timeout=0)
    assert await pool.acquire(consumer="bob", timeout=0) is not None


async def test_idempotent_acquire_replays_live_lease(make_pool):
    pool = make_pool(acquire_mode="stateless")
    await seed(pool, 1)
    first = await pool.acquire(consumer="c", idempotency_key="once")
    second = await pool.acquire(consumer="c", idempotency_key="once")
    assert first.token == second.token
    assert first.rid == second.rid


async def test_fifo_scheduler_keeps_arrival_order(make_pool, monkeypatch):
    # The 0.1s serve tick can straddle the release's storage CAS and grant a
    # later waiter from a fresher read, breaking arrival order under load.
    # Releases serve inline, so the tick adds nothing but nondeterminism.
    monkeypatch.setattr(LeaseService, "_SERVE_INTERVAL", 3600.0)
    pool = make_pool(scheduler="fifo")
    await seed(pool, 1)
    held = await pool.acquire(timeout=0)
    order: list[str] = []

    async def waiter(name: str, priority: int) -> None:
        lease = await pool.acquire(consumer=name, priority=priority, timeout=5)
        order.append(name)
        await pool.release(lease)

    tasks = [
        asyncio.create_task(waiter("low", 0)),
        asyncio.create_task(waiter("high", 10)),
        asyncio.create_task(waiter("mid", 5)),
    ]
    await _wait_queued(pool, 3)
    await pool.release(held)
    await asyncio.gather(*tasks)
    assert order == ["low", "high", "mid"]


async def test_priority_quota_scheduler_honours_priority(make_pool, monkeypatch):
    monkeypatch.setattr(LeaseService, "_SERVE_INTERVAL", 3600.0)
    pool = make_pool(scheduler="priority_quota")
    await seed(pool, 1)
    held = await pool.acquire(timeout=0)
    order: list[str] = []

    async def waiter(name: str, priority: int) -> None:
        lease = await pool.acquire(consumer=name, priority=priority, timeout=5)
        order.append(name)
        await pool.release(lease)

    tasks = [
        asyncio.create_task(waiter("low", 0)),
        asyncio.create_task(waiter("high", 10)),
    ]
    await _wait_queued(pool, 2)
    await pool.release(held)
    await asyncio.gather(*tasks)
    assert order == ["high", "low"]


async def test_token_bucket_paces_acquires(make_pool):
    pool = make_pool(scheduler="token_bucket", rate_limit=50, rate_burst=1)
    await seed(pool, 2)
    started = time.monotonic()
    for _ in range(3):
        await pool.release(await pool.acquire(timeout=5))
    assert time.monotonic() - started >= 0.03


def test_token_bucket_requires_rate_limit():
    from purser.config import PoolConfig

    with pytest.raises(ConfigError):
        PoolConfig(kind="fakecheck", scheduler="token_bucket")


async def test_preemption_kicks_low_priority_holder(make_pool):
    pool = make_pool(preemptible=True, preempt_threshold=5)
    await seed(pool, 1)
    victim = await pool.acquire(consumer="low", priority=0, timeout=0)
    taken = await pool.acquire(consumer="high", priority=10, timeout=0)
    assert taken.token != victim.token
    assert len(await pool.leases()) == 1


async def test_preemption_respects_threshold(make_pool):
    pool = make_pool(preemptible=True, preempt_threshold=50)
    await seed(pool, 1)
    await pool.acquire(consumer="low", priority=0, timeout=0)
    with pytest.raises(PoolExhausted):
        await pool.acquire(consumer="high", priority=10, timeout=0)


async def test_fail_open_absorbs_coordination_outage(make_pool):
    pool = make_pool(fail_open=True)
    await seed(pool, 1)

    async def boom(*args, **kwargs):
        raise BackendUnavailable("down")

    pool._coordination.register_lease = boom
    pool._coordination.unregister_lease = boom
    pool._coordination.renew_lease = boom
    lease = await pool.acquire(timeout=0)
    assert await pool.release(lease) is True


async def test_drain_timeout_force_revokes(make_pool):
    pool = make_pool(drain_timeout=0.2)
    await seed(pool, 1)
    await pool.acquire(timeout=0)
    started = time.monotonic()
    await pool.close()
    assert time.monotonic() - started >= 0.15
    assert await pool.leases() == []


async def test_request_affinity_key_is_shared(make_pool):
    pool = make_pool()
    await seed(pool, 2)
    lease = await pool.acquire(consumer="c", affinity="team", timeout=0)
    assert pool._affinity["team"][lease.rid] == 1.0
    assert "c" not in pool._affinity
    await pool.release(lease)
