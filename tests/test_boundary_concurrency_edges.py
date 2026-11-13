"""Waiter cancellation/priority, admin races and deep-restart recovery edges."""

from __future__ import annotations

import asyncio
import time

import pytest
from conftest import FakeChecker, seed

from purser import Purser
from purser.backends.base import ResourcePatch
from purser.core.health import HealthReport
from purser.core.resource import AcquireRequest
from purser.core.state import ResourceState
from purser.errors import BadPayload, ConfigError, LeaseConflict


async def test_waiter_cancelled_during_wait_leaves_no_debt(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    holder = await pool.acquire(AcquireRequest(consumer="holder"))
    task = asyncio.create_task(
        pool.acquire(AcquireRequest(consumer="doomed", timeout=10.0))
    )
    await asyncio.sleep(0.2)  # let it enqueue
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass
    await pool.release(holder)
    assert pool._waiters == []  # the dead waiter's handle was swept
    ok = await pool.acquire(AcquireRequest(consumer="next"))
    assert ok.resource_id == rid
    await pool.release(ok)


async def test_priority_waiter_overtakes_fifo(make_pool):
    pool = make_pool()
    await seed(pool, n=1)
    holder = await pool.acquire(AcquireRequest(consumer="holder"))
    order: list[str] = []

    async def waiter(name: str, priority: int) -> None:
        lease = await pool.acquire(
            AcquireRequest(consumer=name, priority=priority, timeout=5.0)
        )
        order.append(name)
        held.append(lease)

    held: list = []
    low = asyncio.create_task(waiter("low", -1))
    await asyncio.sleep(0.1)
    high = asyncio.create_task(waiter("high", 5))
    await asyncio.sleep(0.2)
    await pool.release(holder)
    await asyncio.sleep(0.2)
    assert order == ["high"]  # high priority overtakes the FIFO low waiter
    await pool.release(held[0])
    await asyncio.sleep(0.3)
    assert set(order) == {"high", "low"}
    await pool.release(held[1])
    await asyncio.gather(low, high)


async def test_concurrent_validate_now_no_stuck(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    reports = await asyncio.gather(
        pool.validate_now(rid), pool.validate_now(rid)
    )
    assert all(isinstance(r, HealthReport) and r.alive for r in reports)
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.IDLE


async def test_top_up_races_acquire(make_pool):
    pool = make_pool(usage_quota=1)
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    await pool.release(lease, used=1)
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.QUOTA_EXHAUSTED

    results = await asyncio.gather(
        pool.top_up(rid, operator="ops", amount=1),
        pool.acquire(AcquireRequest(timeout=0.3)),
        return_exceptions=True,
    )
    for item in results:
        if item is not None and not isinstance(item, Exception):
            await pool.release(item, used=0)  # do not consume the top-up
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.IDLE
    assert res.used_count == 0


async def test_reaper_races_holder_release(make_pool):
    pool = make_pool(lease_ttl=0.2)
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    await asyncio.sleep(0.35)  # lease expired by the clock

    async def kill() -> None:
        await pool._leases.reap()

    results = await asyncio.gather(
        kill(),
        pool.release(lease),
        return_exceptions=True,
    )
    assert not [e for e in results if isinstance(e, Exception)]
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.IDLE
    assert res.concurrency.used_slots == 0
    again = await pool.acquire(AcquireRequest(consumer="next"))
    assert again.resource_id == rid
    await pool.release(again)


async def test_double_force_revoke_race(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    await pool.acquire(AcquireRequest(consumer="victim"))

    async def revoke() -> str:
        try:
            await pool.force_revoke(rid, operator="ops", reason="race")
            return "won"
        except (LeaseConflict, Exception):
            return "lost"

    outcomes = await asyncio.gather(revoke(), revoke())
    assert set(outcomes) == {"won", "lost"}
    res = await pool.get(rid)
    assert res is not None and res.concurrency.used_slots == 0
    assert await pool._coordination.active(pool.name) == []


async def test_restart_lost_archived_by_gc(tmp_path, storage_kind, coordination_kind):
    if storage_kind != "sqlite":
        pytest.skip("persistent storage required")
    path = str(tmp_path / "lost.db")

    def build() -> Purser:
        ps = Purser(
            maintenance_enabled=False,
            storage="sqlite",
            coordination=coordination_kind,
            sqlite_path=path,
            redis_client=__import__("fakeredis").aioredis.FakeRedis()
            if coordination_kind == "redis"
            else None,
        )
        ps.register_adapter(FakeChecker())
        return ps

    ps1 = build()
    pool = ps1.create_pool("px", "fakecheck")
    [rid] = await seed(pool, n=1)
    res = await pool.get(rid)
    await pool.storage.set_state(
        pool.name,
        rid,
        res.version,
        expect_state=res.state,
        patch=ResourcePatch(
            state=ResourceState.LOST, meta_patch={"lost_at": time.time() - 20000}
        ),
    )
    await ps1.close()

    ps2 = build()
    try:
        pool2 = ps2.create_pool("px", "fakecheck")
        await pool2.run_maintenance("lost_gc")
        res = await pool2.get(rid)
        assert res is not None and res.state is ResourceState.ARCHIVED
    finally:
        await ps2.close()


async def test_restart_validating_stale_gced(tmp_path, storage_kind, coordination_kind):
    if storage_kind != "sqlite":
        pytest.skip("persistent storage required")
    path = str(tmp_path / "stale.db")

    def build() -> Purser:
        ps = Purser(
            maintenance_enabled=False,
            storage="sqlite",
            coordination=coordination_kind,
            sqlite_path=path,
            redis_client=__import__("fakeredis").aioredis.FakeRedis()
            if coordination_kind == "redis"
            else None,
        )
        ps.register_adapter(FakeChecker())
        return ps

    ps1 = build()
    pool = ps1.create_pool("px", "fakecheck")
    [rid] = await seed(pool, n=1)
    res = await pool.get(rid)
    await pool.storage.set_state(
        pool.name,
        rid,
        res.version,
        expect_state=res.state,
        patch=ResourcePatch(
            state=ResourceState.VALIDATING,
            meta_patch={"validate_started_at": time.time() - 3600},
        ),
    )
    await ps1.close()

    ps2 = build()
    try:
        pool2 = ps2.create_pool("px", "fakecheck")
        await pool2.run_maintenance("validating_gc")
        res = await pool2.get(rid)
        assert res is not None and res.state is ResourceState.QUARANTINED
    finally:
        await ps2.close()


async def test_keyfile_corruption_raises_typed(tmp_path):
    path = str(tmp_path / "k.db")
    ps1 = Purser(
        maintenance_enabled=False,
        storage="sqlite",
        coordination="memory",
        sqlite_path=path,
    )
    pool = ps1.create_pool("px", "account")
    await pool.import_resources([{"id": "r1", "username": "u", "password": "hunter2"}])
    await ps1.close()
    assert len(open(path + ".key", "rb").read()) == 32
    with open(path + ".key", "wb") as fh:
        fh.write(b"tooshort")
    with pytest.raises(ConfigError):
        Purser(
            maintenance_enabled=False,
            storage="sqlite",
            coordination="memory",
            sqlite_path=path,
        )


async def test_key_rotation_raises_bad_payload(tmp_path):
    path = str(tmp_path / "rot.db")
    ps1 = Purser(
        maintenance_enabled=False,
        storage="sqlite",
        coordination="memory",
        sqlite_path=path,
    )
    pool = ps1.create_pool("px", "account")
    await pool.import_resources([{"id": "r1", "username": "u", "password": "hunter2"}])
    await ps1.close()
    with open(path + ".key", "wb") as fh:  # rotate to another valid key
        fh.write(bytes(range(32)))
    ps2 = Purser(
        maintenance_enabled=False,
        storage="sqlite",
        coordination="memory",
        sqlite_path=path,
    )
    try:
        pool2 = ps2.create_pool("px", "account")
        with pytest.raises(BadPayload):
            await pool2.get("r1")
    finally:
        await ps2.close()


async def test_use_limit_negative_rejected(make_pool):
    pool = make_pool()
    await seed(pool, n=1)
    with pytest.raises(ConfigError):
        await pool.acquire(AcquireRequest(use_limit=-1))


async def test_query_negative_limit_rejected(make_pool):
    pool = make_pool()
    await seed(pool, n=1)
    with pytest.raises(ConfigError):
        await pool.query(limit=-1)
    with pytest.raises(ConfigError):
        await pool.query(offset=-1)
