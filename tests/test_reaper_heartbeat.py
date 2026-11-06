"""Lease expiry, watchdog reaping and heartbeat renewal (5.2 / F3)."""

from __future__ import annotations

import asyncio
import time

from purser.core.resource import AcquireRequest
from purser.core.state import ResourceState


async def test_watchdog_reaps_expired_lease(make_pool):
    pool = make_pool()
    [rid] = await _seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="holder"))
    # Simulate a crashed holder: the lease expires without a release.
    tracked = pool._leases.get(lease.token)
    assert tracked is not None
    tracked.expires_at = time.time() - 1.0
    await pool._leases.reap()
    res = await pool.get(rid)
    assert res.state is ResourceState.IDLE
    assert res.concurrency.used_slots == 0
    # The pool can be re-acquired.
    again = await pool.acquire(AcquireRequest(consumer="next"))
    assert again.resource_id == rid


async def test_heartbeat_renewal_keeps_lease_alive(make_pool):
    pool = make_pool()
    await _seed(pool)
    lease = await pool.acquire(AcquireRequest(consumer="alive"))
    assert await pool.renew_manual(lease) is True
    assert lease.expires_at > time.time()
    held = await pool._coordination.get_lease(pool.name, lease.token)
    assert held is not None and held.expires_at > time.time()


async def test_lease_lost_event_on_renew_rejection(make_pool, purser):
    await purser.start()
    seen: list[str] = []
    purser.bus.subscribe(lambda event: seen.append(event.type))
    pool = make_pool()
    await _seed(pool)
    lease = await pool.acquire(AcquireRequest(consumer="victim"))
    # Force the coordination plane to drop the lease, then renew fails.
    await pool._coordination.unregister_lease(pool.name, lease.token)
    ok = await pool.renew_manual(lease)
    assert ok is False
    await pool._leases.on_lost(lease)  # heartbeat loop reports the loss
    await asyncio.sleep(0.05)
    assert "lease_lost" in seen
    await purser.close()


async def _seed(pool, n: int = 2):
    from conftest import seed

    return await seed(pool, n=n)
