"""P2-7: waiter aging keeps low-priority consumers from starving."""

from __future__ import annotations

import asyncio
import time

from conftest import seed

from purser.core.resource import AcquireRequest
from purser.pool.leases import LeaseService


async def _wait_enqueued(pool, count: int, *, timeout: float = 2.0) -> None:
    # A fixed sleep after create_task() does not guarantee the waiter reached
    # the heap: under load the task may still be inside its first allocation
    # attempt when release() serves, so the ranking under test never happens.
    # Gate the release on observable queue state instead.
    deadline = time.monotonic() + timeout
    while len(pool._wait_queue.heap) < count:
        if time.monotonic() >= deadline:
            raise AssertionError(
                f"only {len(pool._wait_queue.heap)}/{count} waiters enqueued"
            )
        await asyncio.sleep(0.001)


async def _release_and_serve(pool, holder) -> None:
    # The merged serve loop grants waiters on a 0.1s tick.  Under load the
    # decisive tick can land 60ms+ after the newcomer enqueued -- long enough
    # for its own aging boost to outrank the waiter under test -- or read a
    # pre-release storage view on async backends.  Serve synchronously right
    # after the release so the ranking is decided on settled state.
    await pool.release(holder)
    await pool._notify_waiters()


async def _first_served(pool, holder, *, low_wait: float) -> str:
    low = asyncio.create_task(
        pool.acquire(AcquireRequest(consumer="low", priority=0, timeout=5.0))
    )
    await asyncio.sleep(low_wait)
    high = asyncio.create_task(
        pool.acquire(AcquireRequest(consumer="high", priority=3, timeout=5.0))
    )
    await _wait_enqueued(pool, 2)

    await _release_and_serve(pool, holder)
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
    # A 0.1s serve tick interleaving with the release can grant out of rank
    # order on async backends (per-waiter reads straddle the release CAS) and
    # lets the newcomer rack up aging boost; these tests verify ranking, not
    # tick timing, so the tick is disabled and release serves inline.
    monkeypatch.setattr(LeaseService, "_SERVE_INTERVAL", 3600.0)
    monkeypatch.setattr(LeaseService, "_AGING_STEP", 0.02)
    monkeypatch.setattr(LeaseService, "_AGING_CAP", 5)
    pool = make_pool()
    await seed(pool, n=1)
    holder = await pool.acquire(AcquireRequest(consumer="holder"))

    # 0.2s of waiting buys the full +5 boost, outranking priority 3.
    assert await _first_served(pool, holder, low_wait=0.2) == "low"


async def test_priority_wins_before_aging_kicks_in(make_pool, monkeypatch):
    monkeypatch.setattr(LeaseService, "_SERVE_INTERVAL", 3600.0)
    monkeypatch.setattr(LeaseService, "_AGING_STEP", 100.0)
    pool = make_pool()
    await seed(pool, n=1)
    holder = await pool.acquire(AcquireRequest(consumer="holder"))

    assert await _first_served(pool, holder, low_wait=0.01) == "high"


async def test_boost_is_capped(make_pool, monkeypatch):
    monkeypatch.setattr(LeaseService, "_SERVE_INTERVAL", 3600.0)
    monkeypatch.setattr(LeaseService, "_AGING_STEP", 0.01)
    monkeypatch.setattr(LeaseService, "_AGING_CAP", 2)
    pool = make_pool()
    await seed(pool, n=1)
    holder = await pool.acquire(AcquireRequest(consumer="holder"))

    # 0.2s would be +20 uncapped, but the cap keeps it at +2, so the
    # priority-3 newcomer still wins.
    assert await _first_served(pool, holder, low_wait=0.2) == "high"
