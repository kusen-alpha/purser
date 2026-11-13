"""Lifecycle knobs that were never exercised: expiry, total quota, cooldown."""

from __future__ import annotations

import time

import pytest
from conftest import seed

from purser.backends.base import ResourcePatch
from purser.core.resource import AcquireRequest
from purser.core.state import ResourceState
from purser.errors import PoolExhausted


async def test_expires_in_auto_suspends(make_pool):
    pool = make_pool(expires_in=120)
    [rid] = await seed(pool, n=1)
    res = await pool.get(rid)
    assert res is not None and res.expires_at is not None
    lease = await pool.acquire(AcquireRequest(consumer="c", timeout=2.0))
    assert lease.resource_id == rid
    await pool.release(lease)
    res = await pool.get(rid)
    await pool.storage.set_state(
        pool.name, rid, res.version, expect_state=res.state,
        patch=ResourcePatch(expires_at=time.time() - 1),
    )
    with pytest.raises(PoolExhausted):
        await pool.acquire(AcquireRequest(timeout=0.05))
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.SUSPENDED
    await pool.resume(rid, operator="ops", reason="back")
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.IDLE


async def test_expires_in_quota_policy(make_pool):
    pool = make_pool(expires_in=120, expire_policy="quota")
    [rid] = await seed(pool, n=1)
    res = await pool.get(rid)
    await pool.storage.set_state(
        pool.name, rid, res.version, expect_state=res.state,
        patch=ResourcePatch(expires_at=time.time() - 1),
    )
    with pytest.raises(PoolExhausted):
        await pool.acquire(AcquireRequest(timeout=0.05))
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.QUOTA_EXHAUSTED


@pytest.mark.parametrize(
    "knob",
    [
        {"quota_limit": 1},
    ],
)
async def test_quota_exhausts_and_top_up_recovers(make_pool, knob):
    pool = make_pool(**knob)
    [rid] = await seed(pool, n=1)
    res = await pool.get(rid)
    assert res is not None and res.quota is not None and res.quota.limit == 1
    lease = await pool.acquire(
        AcquireRequest(consumer="c", target="site1", timeout=2.0)
    )
    await pool.release(lease, used=1)
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.QUOTA_EXHAUSTED
    await pool.top_up(rid, operator="ops", amount=1)
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.IDLE


async def test_cooldown_consumer_only_isolation(make_pool):
    pool = make_pool(cooldown_after_release=60, cooldown_consumer_only=True)
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c", timeout=2.0))
    await pool.release(lease)
    with pytest.raises(PoolExhausted):
        await pool.acquire(AcquireRequest(consumer="c", timeout=0.05))
    other = await pool.acquire(AcquireRequest(consumer="other", timeout=2.0))
    assert other.resource_id == rid
    await pool.release(other)
