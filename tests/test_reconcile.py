"""P1-4: periodic, leader-elected slot reconciliation."""

from __future__ import annotations

import asyncio

import fakeredis
from conftest import seed

from purser.backends.coordination import RedisCoordination
from purser.core.resource import AcquireRequest
from purser.core.state import ResourceState
from purser.pool.pool import ResourcePool


async def _orphan(pool, lease) -> None:
    """Drop the coordination lease the way a crashed instance would."""

    await pool._coordination.unregister_lease(pool.name, lease.token)


async def test_sweep_resets_orphaned_slots(make_pool, monkeypatch):
    monkeypatch.setattr(ResourcePool, "_RECONCILE_GRACE", 0.05)
    pool = make_pool(lease_ttl=30.0)
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    await _orphan(pool, lease)
    assert (await pool.get(rid)).concurrency.used_slots == 1

    await asyncio.sleep(0.1)
    assert await pool.run_maintenance("reconcile") == {"reconcile": 1}

    res = await pool.get(rid)
    assert res is not None
    assert res.concurrency.used_slots == 0
    assert res.state is ResourceState.IDLE


async def test_sweep_skips_recent_activity(make_pool):
    # Default grace (30s) covers the bump-slots-then-register window.
    pool = make_pool(lease_ttl=30.0)
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    await _orphan(pool, lease)

    assert await pool.run_maintenance("reconcile") == {"reconcile": 0}
    assert (await pool.get(rid)).concurrency.used_slots == 1


async def test_sweep_keeps_live_leases(make_pool, monkeypatch):
    monkeypatch.setattr(ResourcePool, "_RECONCILE_GRACE", 0.0)
    pool = make_pool(lease_ttl=30.0)
    [rid] = await seed(pool, n=1)
    await pool.acquire(AcquireRequest(consumer="c"))

    assert await pool.run_maintenance("reconcile") == {"reconcile": 0}
    assert (await pool.get(rid)).concurrency.used_slots == 1


async def test_sweep_is_leader_gated(make_pool, monkeypatch):
    monkeypatch.setattr(ResourcePool, "_RECONCILE_GRACE", 0.0)
    pool = make_pool(lease_ttl=30.0)
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    await _orphan(pool, lease)

    async def secondary(_pool, _name, _ttl):
        return False

    monkeypatch.setattr(pool._coordination, "try_leader", secondary)
    assert await pool.run_maintenance("reconcile") == {"reconcile": 0}
    assert (await pool.get(rid)).concurrency.used_slots == 1


async def test_reconcile_interval_is_configurable(purser):
    pool = purser.ensure_pool("cfg-reconcile", "fakecheck", scheduler_intervals={"reconcile": 5.0})
    task = pool._scheduler.get("reconcile")
    assert task is not None and task.interval == 5.0


async def test_reconcile_sweep_registered_on_start(make_pool):
    pool = make_pool()
    await pool.start()
    task = pool._scheduler.get("reconcile")
    assert task is not None and task.interval == 60.0


async def test_leader_election_excludes_second_instance():
    server = fakeredis.FakeServer()
    first = RedisCoordination(client=fakeredis.aioredis.FakeRedis(server=server))
    second = RedisCoordination(client=fakeredis.aioredis.FakeRedis(server=server))
    try:
        assert await first.try_leader("p", "reconcile", 0.2) is True
        assert await second.try_leader("p", "reconcile", 0.2) is False
        assert await first.try_leader("p", "reconcile", 0.2) is True  # renewal

        await asyncio.sleep(0.25)  # lease expires without a renewal
        assert await second.try_leader("p", "reconcile", 0.2) is True
        assert await first.try_leader("p", "reconcile", 0.2) is False
    finally:
        await first.close()
        await second.close()
