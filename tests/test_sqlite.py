"""SQLite storage plane: durability, restart reconciliation and edge cases."""

from __future__ import annotations

import asyncio
import sqlite3
import time

import fakeredis
import pytest
from conftest import FakeChecker

from purser import Purser
from purser.backends.sqlite import SqliteStorage
from purser.core.audit import AuditLog
from purser.core.resource import AcquireRequest, Concurrency, HealthStats, Resource
from purser.core.state import ResourceState

POOL = "p0"


def make_resource(
    rid: str, *, tags=(), max_slots: int = 1, payload=None,
) -> Resource:
    return Resource(
        id=rid,
        kind="fakecheck",
        payload=payload if payload is not None else {"host": "127.0.0.1", "port": 8080},
        state=ResourceState.IDLE,
        tags=frozenset(tags),
        concurrency=Concurrency(max_slots=max_slots),
        health=HealthStats(score=0.9, samples=3),
        meta={"created_at": time.time()},
    )


@pytest.fixture
def storage(tmp_path):
    s = SqliteStorage(path=str(tmp_path / "repo.db"))
    yield s
    s.close()


async def test_persists_across_restart(tmp_path):
    db = str(tmp_path / "persist.db")
    s1 = SqliteStorage(path=db)
    await s1.put(POOL, make_resource("a", tags={"us"}, max_slots=2))
    assert (await s1.transition_acquire(POOL, "a", 2)).ok
    s1.close()

    s2 = SqliteStorage(path=db)
    try:
        res = await s2.get(POOL, "a")
        assert res is not None and res.state is ResourceState.FULL
        assert res.version == 2 and res.concurrency.used_slots == 2
        # Slots survive the restart and can be returned without a token.
        result = await s2.transition_release(POOL, "a", 2)
        assert result.ok and result.state is ResourceState.IDLE
        assert {r.id for r in await s2.query(POOL, tags=frozenset({"us"}))} == {"a"}
    finally:
        s2.close()


async def test_purser_restart_reconciles_orphan_leases(tmp_path):
    db = str(tmp_path / "e2e.db")
    ps1 = Purser(
        maintenance_enabled=False,
        storage="sqlite",
        coordination="memory",
        sqlite_path=db,
        audit_path=db,
    )
    ps1.register_type(FakeChecker())
    pool = ps1.create_pool("px", "fakecheck")
    await ps1.start()
    await pool.import_resources(
        [{"id": "r1", "host": "10.0.0.1", "port": 8080, "tags": ["us"]}],
        import_validate="on_import",
    )
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    assert lease.resource_id == "r1"
    # Hard kill: no release, in-memory coordination is lost with the process.
    await ps1.close()

    ps2 = Purser(
        maintenance_enabled=False,
        storage="sqlite",
        coordination="memory",
        sqlite_path=db,
        audit_path=db,
    )
    try:
        ps2.register_type(FakeChecker())
        pool2 = ps2.create_pool("px", "fakecheck")
        await pool2.start()  # runs _reconcile_leases
        res = await pool2.get("r1")
        # Master data is accurate; orphaned slots were returned on start.
        assert res is not None and res.state is ResourceState.IDLE
        assert res.concurrency.used_slots == 0
        assert res.payload["host"] == "10.0.0.1"
        # The pool is usable again straight away.
        again = await pool2.acquire(AcquireRequest(consumer="c"))
        assert again.resource_id == "r1"
        await pool2.release(again)
        assert len(ps2.audit) >= 3
        assert ps2.audit.verify()
    finally:
        await ps2.close()


async def test_redis_coordination_survives_restart(tmp_path):
    db = str(tmp_path / "e2e-redis.db")
    redis = fakeredis.aioredis.FakeRedis()

    def build():
        ps = Purser(
            maintenance_enabled=False,
            storage="sqlite",
            coordination="redis",
            sqlite_path=db,
            redis_client=redis,
        )
        ps.register_type(FakeChecker())
        return ps

    ps1 = build()
    pool = ps1.create_pool("px", "fakecheck")
    await ps1.start()
    await pool.import_resources(
        [{"id": "r1", "host": "10.0.0.1", "port": 8080}],
        import_validate="on_import",
    )
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    await ps1.close()

    ps2 = build()
    try:
        pool2 = ps2.create_pool("px", "fakecheck")
        await pool2.start()
        # The lease is still alive in redis, so reconciliation keeps the slot.
        res = await pool2.get("r1")
        assert res is not None and res.state is ResourceState.FULL
        revoked = await pool2.force_revoke("r1", operator="ops", reason="restart check")
        assert revoked.token == lease.token
        res = await pool2.get("r1")
        assert res.state is ResourceState.IDLE
    finally:
        await ps2.close()


async def test_concurrent_transition_acquire_no_oversell(storage):
    await storage.put(POOL, make_resource("a"))
    results = await asyncio.gather(
        *(storage.transition_acquire(POOL, "a", 1) for _ in range(8))
    )
    assert sum(1 for r in results if r.ok) == 1
    res = await storage.get(POOL, "a")
    assert res is not None and res.concurrency.used_slots == 1


async def test_roundtrip_exotic_fields_and_ids(storage):
    rid = "??: a/b?c\"d 'e' # {}"
    res = make_resource(rid, tags={"? ?", "a&b"}, max_slots=3)
    res.usage_quota = 10
    res.used_count = 4
    res.expires_at = 123.0
    res.next_available_at = 456.0
    res.mutex_group = "g1"
    res.schedule_window = ("22:00", "06:00")
    await storage.put(POOL, res)
    loaded = await storage.get(POOL, rid)
    assert loaded.id == rid
    assert loaded.tags == frozenset({"? ?", "a&b"})
    assert loaded.concurrency.max_slots == 3
    assert loaded.usage_quota == 10 and loaded.used_count == 4
    assert loaded.expires_at == 123.0 and loaded.next_available_at == 456.0
    assert loaded.mutex_group == "g1"
    assert loaded.schedule_window == ("22:00", "06:00")
    assert {r.id for r in await storage.query(POOL, tags=frozenset({"? ?"}))} == {rid}


async def test_audit_persists_across_restart(tmp_path):
    db = str(tmp_path / "audit.db")
    a1 = AuditLog(sqlite_path=db)
    a1.record(operator="alice", action="ban", pool="px", rid="r1", reason="risk")
    a1.close()

    a2 = AuditLog(sqlite_path=db)
    try:
        assert [e.action for e in a2.entries()] == ["ban"]
        assert a2.verify()
        a2.record(operator="alice", action="unban", pool="px", rid="r1")
        assert len(a2.entries()) == 2
        assert a2.verify()
        entries = a2.entries()
        entries[0].reason = "forged"
        assert not a2.verify()
    finally:
        a2.close()


async def test_secret_at_rest(tmp_path):
    db = str(tmp_path / "secret.db")
    storage = SqliteStorage(path=db, secret_fields=("password",))
    try:
        resource = make_resource("a", payload={"username": "u", "password": "hunter2"})
        await storage.put(POOL, resource)
        conn = sqlite3.connect(db)
        try:
            blob = conn.execute(
                "SELECT payload FROM resources WHERE pool = ? AND id = ?", (POOL, "a")
            ).fetchone()[0]
        finally:
            conn.close()
        assert "hunter2" not in blob
        loaded = await storage.get(POOL, "a")
        assert loaded.payload["password"] == "hunter2"
    finally:
        storage.close()


async def test_reset_usage_preserves_master_data(storage):
    await storage.put(POOL, make_resource("a", max_slots=3, payload={"host": "keep.me"}))
    await storage.transition_acquire(POOL, "a", 2)
    assert await storage.reset_usage(POOL, "a") is True
    res = await storage.get(POOL, "a")
    assert res.state is ResourceState.IDLE and res.concurrency.used_slots == 0
    assert res.payload == {"host": "keep.me"}  # master data untouched
    assert "created_at" in res.meta
