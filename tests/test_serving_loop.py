"""Merged serving loop (P0-3): one task per pool serves every waiter.

The loop replaced the per-waiter 100ms full-scan poll.  A PUBLISH-based push
was prototyped and dropped: redis-py/fakeredis can swallow a cancellation
delivered mid-command, which wedged pool teardown.  The tick below is the
only cross-instance wake-up path, so these tests pin its latency bound.
"""

from __future__ import annotations

import asyncio
import time

import fakeredis
import pytest
from conftest import FakeChecker

from purser import Purser
from purser.core.resource import AcquireRequest
from purser.pool.leases import LeaseService


async def test_single_loop_serves_every_waiter(make_pool, monkeypatch):
    monkeypatch.setattr(LeaseService, "_SERVE_INTERVAL", 0.02)
    pool = make_pool()
    await pool.import_resources(
        [{"id": "r1", "host": "1.1.1.1", "port": 1}], import_validate="on_import"
    )
    holder = await pool.acquire(AcquireRequest(consumer="holder"))
    first = asyncio.create_task(pool.acquire(AcquireRequest(consumer="a", timeout=5.0)))
    await asyncio.sleep(0.05)
    second = asyncio.create_task(pool.acquire(AcquireRequest(consumer="b", timeout=5.0)))
    await asyncio.sleep(0.05)

    await pool.release(holder)
    done, _ = await asyncio.wait({first, second}, timeout=5.0, return_when=asyncio.FIRST_COMPLETED)
    assert len(done) == 1
    served = next(iter(done)).result()
    await pool.release(served)

    remaining = second if served.consumer == "a" else first
    other = await asyncio.wait_for(remaining, 5.0)
    assert {served.consumer, other.consumer} == {"a", "b"}
    await pool.release(other)


async def test_serving_tick_grants_when_release_cannot_serve(
    make_pool, monkeypatch
):
    """The tick alone must deliver: the release path is muted here."""

    monkeypatch.setattr(LeaseService, "_SERVE_INTERVAL", 0.02)
    pool = make_pool()
    await pool.import_resources(
        [{"id": "r1", "host": "1.1.1.1", "port": 1}], import_validate="on_import"
    )
    holder = await pool.acquire(AcquireRequest(consumer="holder"))
    waiter = asyncio.create_task(pool.acquire(AcquireRequest(consumer="w", timeout=5.0)))
    await asyncio.sleep(0.05)
    assert not waiter.done()

    async def muted(*_a, **_k):
        return 0

    monkeypatch.setattr(pool._lease.releases, "serve_waiters", muted)
    await pool.release(holder)
    lease = await asyncio.wait_for(waiter, 5.0)
    assert lease.consumer == "w"
    await pool.release(lease)


async def test_tick_picks_up_a_foreign_release(tmp_path, shared_backend_harness, monkeypatch):
    """Cross-instance releases arrive on the tick, not a push notification."""

    monkeypatch.setattr(LeaseService, "_SERVE_INTERVAL", 0.05)
    server = fakeredis.FakeServer()
    path = str(tmp_path / "shared.db")

    def build() -> Purser:
        ps = Purser(
            maintenance_enabled=False,
            storage="sqlite",
            sqlite_path=path,
            coordination="redis",
            redis_client=fakeredis.aioredis.FakeRedis(server=server),
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
        holder = await p1.acquire(AcquireRequest(consumer="holder"))
        waiter = asyncio.create_task(
            p2.acquire(AcquireRequest(consumer="waiter", timeout=3.0))
        )
        await asyncio.sleep(0.15)

        started = time.monotonic()
        await p1.release(holder)  # instance 1 cannot notify instance 2
        lease = await asyncio.wait_for(waiter, 3.0)
        elapsed = time.monotonic() - started

        assert lease.consumer == "waiter"
        assert elapsed < 1.0  # one tick, not a 5s-or-bust stall
        await p2.release(lease)
    finally:
        await ps1.close()
        await ps2.close()


async def test_serve_loop_stops_on_cancel(make_pool, monkeypatch):
    monkeypatch.setattr(LeaseService, "_SERVE_INTERVAL", 0.02)
    pool = make_pool()
    await pool.start()
    task = asyncio.create_task(pool._lease.serve_loop())
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
