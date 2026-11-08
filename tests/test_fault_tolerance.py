"""Failure, fault-injection and restart scenarios (S10 / F2 / F4).

Runs across the storage x coordination matrix.  Persistent-restart tests only
make sense for sqlite storage (memory storage dies with the process), so they
document that constraint explicitly.
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
import time

import fakeredis
import pytest
from conftest import FakeChecker, seed

from purser import Purser
from purser.backends.base import ResourcePatch
from purser.core.resource import AcquireRequest
from purser.core.state import ResourceState
from purser.errors import (
    BadState,
    ConfigError,
    LeaseConflict,
    NoSuitableResource,
    PoolExhausted,
)


def _skip_unless_sqlite(storage_kind: str) -> None:
    if storage_kind != "sqlite":
        pytest.skip("persistent storage required")


def _make_sqlite_purser(path: str, coordination_kind: str, audit_path: str | None = None) -> Purser:
    ps = Purser(
        maintenance_enabled=False,
        storage="sqlite",
        coordination=coordination_kind,
        sqlite_path=path,
        audit_path=audit_path,
        redis_client=fakeredis.aioredis.FakeRedis() if coordination_kind == "redis" else None,
    )
    ps.register_type(FakeChecker())
    return ps


async def test_acquire_rolls_back_slot_on_coordination_failure(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)

    async def boom(*args, **kwargs):
        raise OSError("coordination down")

    pool._coordination.register_lease = boom
    with pytest.raises((NoSuitableResource, PoolExhausted)):
        await pool.acquire(AcquireRequest(timeout=0.1))
    res = await pool.get(rid)
    assert res is not None and res.concurrency.used_slots == 0
    assert res.state is ResourceState.IDLE
    assert await pool._coordination.active(pool.name) == []


async def test_release_after_coordination_loss_waits_for_reconcile(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    await pool._coordination.flush(pool.name)  # simulated wipe
    # A late release of a vanished lease never decrements storage: the slot
    # may already belong to someone else in another instance.
    assert await pool.release(lease) is True
    res = await pool.get(rid)
    assert res is not None and res.concurrency.used_slots == 1
    # A repeated release is idempotent rather than an exception.
    assert await pool.release(lease) is True
    # The restart path reconciles the orphaned slot.
    await pool._reconcile_leases()
    res = await pool.get(rid)
    assert res is not None and res.concurrency.used_slots == 0
    assert res.state is ResourceState.IDLE


async def test_concurrent_acquire_release_no_conflicts(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)

    async def worker(i: int) -> bool:
        async with pool.lease(AcquireRequest(consumer=f"c{i}")) as lease:
            assert lease.resource_id == rid
            return True

    results = await asyncio.gather(*(worker(i) for i in range(15)))
    assert all(results)
    res = await pool.get(rid)
    assert res is not None and res.concurrency.used_slots == 0
    assert res.state is ResourceState.IDLE
    assert await pool._coordination.active(pool.name) == []


async def test_force_revoke_races_release(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="victim"))

    async def revoke() -> str:
        try:
            await pool.force_revoke(rid, operator="ops", reason="race")
            return "revoke-won"
        except (LeaseConflict, BadState):
            return "revoke-lost"

    async def release() -> str:
        try:
            await pool.release(lease)
            return "release-won"
        except LeaseConflict:
            return "release-lost"

    await asyncio.gather(revoke(), release())
    res = await pool.get(rid)
    assert res is not None and res.concurrency.used_slots == 0
    assert await pool._coordination.active(pool.name) == []


async def test_acquire_many_atomic_rollback_no_leak(make_pool):
    pool = make_pool()
    await seed(pool, n=2)
    with pytest.raises(PoolExhausted):
        await pool.acquire_many(3, mode="atomic", timeout=0.1)
    resources = await pool.query()
    assert all(r.concurrency.used_slots == 0 for r in resources)
    assert await pool._coordination.active(pool.name) == []


async def test_waiters_never_double_issue(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    holder = await pool.acquire(AcquireRequest(consumer="holder"))
    held: list = []

    async def waiter(i: int) -> None:
        try:
            lease = await pool.acquire(AcquireRequest(consumer=f"w{i}", timeout=5.0))
            held.append(lease)
        except PoolExhausted:
            pass

    tasks = [asyncio.create_task(waiter(i)) for i in range(3)]
    await asyncio.sleep(0.2)  # let all waiters enqueue
    await pool.release(holder)
    await asyncio.sleep(0.2)
    assert len(held) == 1
    res = await pool.get(rid)
    assert res is not None and res.concurrency.used_slots == 1

    # Each release wakes exactly one waiter; slots never exceed the capacity.
    for expected in (2, 3):
        await pool.release(held[-1])
        await asyncio.sleep(0.2)
        assert len(held) == expected
        res = await pool.get(rid)
        assert res is not None and res.concurrency.used_slots == 1
    await pool.release(held[-1])
    await asyncio.gather(*tasks)


async def test_acquire_zero_slots_rejected(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    with pytest.raises(ConfigError):
        await pool.acquire(AcquireRequest(slots=0))
    res = await pool.get(rid)
    assert res is not None and res.concurrency.used_slots == 0


async def test_import_fail_fast_stops_cleanly(make_pool):
    pool = make_pool()
    records = [
        {"id": "ok-1", "host": "1.1.1.1", "port": 1},
        {"id": "bad-1", "host": "1.1.1.2", "port": "not-a-port"},
        {"id": "ok-2", "host": "1.1.1.3", "port": 3},
    ]
    report = await pool.import_resources(records, fail_fast=True)
    assert report.imported == 1 and len(report.errors) == 1
    assert await pool.get("ok-1") is not None
    assert await pool.get("ok-2") is None
    report2 = await pool.import_resources([records[2]], fail_fast=True)
    assert report2.imported == 1


# -- persistent restart scenarios (sqlite storage only) ----------------------


async def test_restart_held_lease_reconciled(tmp_path, storage_kind, coordination_kind):
    _skip_unless_sqlite(storage_kind)
    path = str(tmp_path / "restart.db")
    ps1 = _make_sqlite_purser(path, coordination_kind)
    pool = ps1.create_pool("px", "fakecheck")
    await ps1.start()
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    assert lease.resource_id == rid
    await ps1.close()

    ps2 = _make_sqlite_purser(path, coordination_kind)
    try:
        pool2 = ps2.create_pool("px", "fakecheck")
        await pool2.start()  # reconciliation
        res = await pool2.get(rid)
        assert res is not None and res.state is ResourceState.IDLE
        assert res.concurrency.used_slots == 0
        assert res.payload == {"host": "127.0.0.1", "port": 8000}
        again = await pool2.acquire(AcquireRequest(consumer="c"))
        assert again.resource_id == rid
        await pool2.release(again)
    finally:
        await ps2.close()


async def test_restart_preserves_banned_state_and_meta(tmp_path, storage_kind, coordination_kind):
    _skip_unless_sqlite(storage_kind)
    path = str(tmp_path / "banned.db")
    ps1 = _make_sqlite_purser(path, coordination_kind)
    pool = ps1.create_pool("px", "fakecheck")
    [rid] = await seed(pool, n=1)
    await pool.ban(rid, operator="ops", reason="abuse")
    await ps1.close()

    ps2 = _make_sqlite_purser(path, coordination_kind)
    try:
        pool2 = ps2.create_pool("px", "fakecheck")
        res = await pool2.get(rid)
        assert res is not None and res.state is ResourceState.BANNED
        assert res.meta["banned_reason"] == "abuse"
        await pool2.unban(rid, operator="ops", reason="cleared")
        res = await pool2.get(rid)
        assert res is not None and res.state is ResourceState.IDLE
    finally:
        await ps2.close()


async def test_restart_preserves_quota_and_usage(tmp_path, storage_kind, coordination_kind):
    _skip_unless_sqlite(storage_kind)
    path = str(tmp_path / "quota.db")
    ps1 = _make_sqlite_purser(path, coordination_kind)
    pool = ps1.create_pool("px", "fakecheck", usage_quota=2)
    await ps1.start()
    [rid] = await seed(pool, n=1)
    for _ in range(2):
        lease = await pool.acquire(AcquireRequest(consumer="c"))
        await pool.release(lease, used=1)
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.QUOTA_EXHAUSTED
    assert res.used_count == 2
    await ps1.close()

    ps2 = _make_sqlite_purser(path, coordination_kind)
    try:
        pool2 = ps2.create_pool("px", "fakecheck", usage_quota=2)
        res = await pool2.get(rid)
        assert res is not None and res.state is ResourceState.QUOTA_EXHAUSTED
        assert res.used_count == 2 and res.usage_quota == 2
        await pool2.top_up(rid, operator="ops", amount=2)
        res = await pool2.get(rid)
        assert res is not None and res.state is ResourceState.IDLE
        assert res.used_count == 0
    finally:
        await ps2.close()


async def test_restart_draining_lost_lease_archives(tmp_path, storage_kind, coordination_kind):
    _skip_unless_sqlite(storage_kind)
    path = str(tmp_path / "draining.db")
    ps1 = _make_sqlite_purser(path, coordination_kind)
    pool = ps1.create_pool("px", "fakecheck")
    await ps1.start()
    [rid] = await seed(pool, n=1)
    await pool.acquire(AcquireRequest(consumer="c"))
    await pool.drain(rid, operator="ops", reason="retiring")
    await ps1.close()

    ps2 = _make_sqlite_purser(path, coordination_kind)
    try:
        pool2 = ps2.create_pool("px", "fakecheck")
        await pool2.start()  # reconciliation must archive the orphaned drain
        res = await pool2.get(rid)
        assert res is not None and res.state is ResourceState.ARCHIVED
        assert res.concurrency.used_slots == 0
    finally:
        await ps2.close()


async def test_restart_idle_drained_archives(tmp_path, storage_kind, coordination_kind):
    _skip_unless_sqlite(storage_kind)
    path = str(tmp_path / "drained-idle.db")
    ps1 = _make_sqlite_purser(path, coordination_kind)
    pool = ps1.create_pool("px", "fakecheck")
    [rid] = await seed(pool, n=1)
    await pool.drain(rid, operator="ops", reason="retiring-idle")
    await ps1.close()

    ps2 = _make_sqlite_purser(path, coordination_kind)
    try:
        pool2 = ps2.create_pool("px", "fakecheck")
        await pool2.start()
        res = await pool2.get(rid)
        assert res is not None and res.state is ResourceState.ARCHIVED
    finally:
        await ps2.close()


async def test_restart_quarantine_preserved(tmp_path, storage_kind, coordination_kind):
    _skip_unless_sqlite(storage_kind)
    path = str(tmp_path / "quarantine.db")
    ps1 = _make_sqlite_purser(path, coordination_kind)
    pool = ps1.create_pool("px", "fakecheck")
    [rid] = await seed(pool, n=1)
    await pool._on_validation_fail(rid, "flaky", operator="system")
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.QUARANTINED
    until = res.meta["quarantine_until"]
    await ps1.close()

    ps2 = _make_sqlite_purser(path, coordination_kind)
    try:
        pool2 = ps2.create_pool("px", "fakecheck")
        res = await pool2.get(rid)
        assert res is not None and res.state is ResourceState.QUARANTINED
        assert res.meta["quarantine_until"] == until
        res.meta["quarantine_until"] = time.time() - 1.0
        await pool2.storage.set_state(
            pool2.name, rid, res.version, expect_state=res.state,
            patch=ResourcePatch(meta_patch={"quarantine_until": time.time() - 1.0}),
        )
        await pool2.run_maintenance("quarantine_gc")
        res = await pool2.get(rid)
        assert res is not None and res.state is ResourceState.IDLE
    finally:
        await ps2.close()


async def test_restart_secrets_decrypt(tmp_path, storage_kind):
    _skip_unless_sqlite(storage_kind)
    path = str(tmp_path / "secrets.db")
    ps1 = Purser(
        maintenance_enabled=False,
        storage="sqlite",
        coordination="memory",
        sqlite_path=path,
        audit_path=path,
    )
    pool = ps1.create_pool("px", "account")
    await pool.import_resources(
        [{"id": "r1", "username": "u", "password": "hunter2"}]
    )
    await ps1.close()
    assert os.path.exists(path + ".key")  # key file materialized for restart

    conn = sqlite3.connect(path)
    try:
        blob = conn.execute(
            "SELECT payload FROM resources WHERE pool = 'px' AND id = 'r1'"
        ).fetchone()[0]
    finally:
        conn.close()
    assert "hunter2" not in blob

    ps2 = Purser(
        maintenance_enabled=False,
        storage="sqlite",
        coordination="memory",
        sqlite_path=path,
        audit_path=path,
    )
    try:
        pool2 = ps2.create_pool("px", "account")
        res = await pool2.get("r1")
        assert res is not None
        assert res is not None and res.payload["password"] == "hunter2"
    finally:
        await ps2.close()


async def test_repeat_import_across_restart_idempotent(tmp_path, storage_kind, coordination_kind):
    _skip_unless_sqlite(storage_kind)
    path = str(tmp_path / "import.db")
    ps1 = _make_sqlite_purser(path, coordination_kind)
    pool = ps1.create_pool("px", "fakecheck")
    records = [{"id": f"r{i}", "host": "1.1.1.1", "port": i} for i in range(3)]
    first = await pool.import_resources(records)
    assert first.imported == 3
    await ps1.close()

    ps2 = _make_sqlite_purser(path, coordination_kind)
    try:
        pool2 = ps2.create_pool("px", "fakecheck")
        second = await pool2.import_resources(records)
        assert second.imported == 0 and second.updated == 3
        resources = await pool2.query()
        assert len(resources) == 3
    finally:
        await ps2.close()
