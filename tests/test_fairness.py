"""P2-7: waiter aging keeps low-priority consumers from starving."""

from __future__ import annotations

import asyncio

from conftest import seed

from purser.core.resource import AcquireRequest
from purser.pool.leases import LeaseService


async def _first_served(pool, holder, *, low_wait: float) -> str:
    low = asyncio.create_task(
        pool.acquire(AcquireRequest(consumer="low", priority=0, timeout=5.0))
    )
    await asyncio.sleep(low_wait)
    high = asyncio.create_task(
        pool.acquire(AcquireRequest(consumer="high", priority=3, timeout=5.0))
    )
    await asyncio.sleep(0.02)

    await pool.release(holder)
    done, _ = await asyncio.wait({low, high}, timeout=5.0, return_when=asyncio.FIRST_COMPLETED)
    winner = next(iter(done)).result()
    for task in (low, high):
        if task.done() and task is not None:
            lease = task.result()
            if lease is not winner:
                await pool.release(lease)
    await pool.release(winner)
    return winner.consumer


async def test_aging_promotes_a_long_waiting_waiter(make_pool, monkeypatch):
    monkeypatch.setattr(LeaseService, "_AGING_STEP", 0.02)
    monkeypatch.setattr(LeaseService, "_AGING_CAP", 5)
    pool = make_pool()
    await seed(pool, n=1)
    holder = await pool.acquire(AcquireRequest(consumer="holder"))

    # 0.2s of waiting buys the full +5 boost, outranking priority 3.
    assert await _first_served(pool, holder, low_wait=0.2) == "low"


async def test_priority_wins_before_aging_kicks_in(make_pool, monkeypatch):
    monkeypatch.setattr(LeaseService, "_AGING_STEP", 100.0)
    pool = make_pool()
    await seed(pool, n=1)
    holder = await pool.acquire(AcquireRequest(consumer="holder"))

    assert await _first_served(pool, holder, low_wait=0.01) == "high"


async def test_boost_is_capped(make_pool, monkeypatch):
    monkeypatch.setattr(LeaseService, "_AGING_STEP", 0.01)
    monkeypatch.setattr(LeaseService, "_AGING_CAP", 2)
    pool = make_pool()
    await seed(pool, n=1)
    holder = await pool.acquire(AcquireRequest(consumer="holder"))

    # 0.2s would be +20 uncapped, but the cap keeps it at +2, so the
    # priority-3 newcomer still wins.
    assert await _first_served(pool, holder, low_wait=0.2) == "high"
