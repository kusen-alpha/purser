"""MySQL storage: config wiring plus env-gated integration contract (S10 / F2).

Set ``PURSER_TEST_MYSQL_URL`` (e.g.
``mysql://root:pass@127.0.0.1:3306/purser_test``) to run the full contract,
persistence and end-to-end reconciliation suites against a live MySQL.
Without the variable the integration tests are skipped; the construction/
wiring test below always runs and never opens a connection.
"""

from __future__ import annotations

import os
import uuid

import pytest
from conftest import FakeChecker

from purser import Purser
from purser.backends.base import ResourcePatch
from purser.backends.mysql import MySqlStorage
from purser.config import PoolConfig
from purser.core.resource import (
    AcquireRequest,
    Concurrency,
    HealthStats,
    Resource,
)
from purser.core.state import ResourceState

MYSQL_URL = os.environ.get("PURSER_TEST_MYSQL_URL")
MYSQL_REQUIRES_SERVER = pytest.mark.skipif(
    not MYSQL_URL,
    reason="set PURSER_TEST_MYSQL_URL to run MySQL integration tests",
)


def make_resource(rid: str, *, tags=(), max_slots: int = 1, payload=None) -> Resource:
    return Resource(
        id=rid,
        kind="fakecheck",
        payload=payload if payload is not None else {"host": "127.0.0.1", "port": 8080},
        state=ResourceState.IDLE,
        tags=frozenset(tags),
        concurrency=Concurrency(max_slots=max_slots),
        health=HealthStats(score=0.9, samples=3),
        meta={"created_at": 1234.0},
    )


def pool_name() -> str:
    return f"t-{uuid.uuid4().hex[:10]}"


def test_my_sql_storage_constructs_lazily():
    store = MySqlStorage(url="mysql://user:pass@localhost:3306/db")
    assert store._conn is None  # no connection before the first operation
    ps = Purser(storage="mysql", mysql_url="mysql://user:pass@localhost:3306/db")
    backend = ps._make_storage(PoolConfig(kind="fakecheck", storage="mysql"))
    assert isinstance(backend, MySqlStorage)
    assert backend._conn is None


@MYSQL_REQUIRES_SERVER
async def test_contract_suite():
    store = MySqlStorage(url=MYSQL_URL, secret_fields=("password",))
    try:
        pool = pool_name()
        await store.put(pool, make_resource("a", tags={"us"}))
        res = await store.get(pool, "a")
        assert res is not None and res.state is ResourceState.IDLE and res.version == 1

        # Upsert bumps the version and syncs the tag index.
        await store.put(pool, make_resource("a", tags={"eu"}))
        res = await store.get(pool, "a")
        assert res.version == 2 and res.tags == frozenset({"eu"})
        assert await store.query(pool, tags=frozenset({"us"})) == []
        assert {r.id for r in await store.query(pool, tags=frozenset({"eu"}))} == {"a"}

        # Slot transitions with no oversell.
        assert (await store.transition_acquire(pool, "a", 1)).ok
        res = await store.get(pool, "a")
        assert res.state is ResourceState.FULL and res.concurrency.used_slots == 1
        assert not (await store.transition_acquire(pool, "a", 1)).ok
        result = await store.transition_release(pool, "a", 1)
        assert result.ok and result.state is ResourceState.IDLE
        assert not (await store.transition_release(pool, "a", 1)).ok

        await store.put(pool, make_resource("shared", max_slots=2))
        assert (await store.transition_acquire(pool, "shared", 1)).ok
        assert (await store.transition_acquire(pool, "shared", 1)).ok
        assert not (await store.transition_acquire(pool, "shared", 1)).ok

        # set_state CAS + meta merge + reset_usage.
        res = await store.get(pool, "shared")
        assert await store.set_state(
            pool, "shared", res.version, patch=ResourcePatch(meta_patch={"hello": "world"})
        )
        res = await store.get(pool, "shared")
        assert res.meta["hello"] == "world" and "created_at" in res.meta
        assert not await store.set_state(
            pool, "shared", res.version - 1, patch=ResourcePatch(state=ResourceState.BANNED)
        )
        assert await store.reset_usage(pool, "shared") is True
        res = await store.get(pool, "shared")
        assert res.state is ResourceState.IDLE and res.concurrency.used_slots == 0

        # Query paging + AND tags.
        for i in range(4):
            await store.put(pool, make_resource(f"r{i}", tags={("us" if i % 2 == 0 else "eu")}))
        page = await store.query(
            pool, states={ResourceState.IDLE}, tags=frozenset({"us"}), limit=2, offset=0
        )
        assert [r.id for r in page] == ["r0", "r2"]
        await store.put(pool, make_resource("multi", tags={"us", "eu"}))
        assert {r.id for r in await store.query(pool, tags=frozenset({"us", "eu"}))} == {"multi"}

        # Counts, scan, delete, flush.
        counts = await store.counts(pool)
        assert counts[ResourceState.IDLE] >= 4
        ids = [r.id async for r in store.scan(pool)]
        assert "a" in ids and "shared" in ids and "multi" in ids
        assert await store.delete(pool, "a") is True
        assert await store.get(pool, "a") is None
        await store.flush(pool)
        assert await store.counts(pool) == {s: 0 for s in ResourceState}
    finally:
        await store.close()


@MYSQL_REQUIRES_SERVER
async def test_persists_across_restart():
    pool = pool_name()
    s1 = MySqlStorage(url=MYSQL_URL)
    await s1.put(pool, make_resource("a", tags={"us"}))
    assert (await s1.transition_acquire(pool, "a", 1)).ok
    await s1.close()

    s2 = MySqlStorage(url=MYSQL_URL)
    try:
        res = await s2.get(pool, "a")
        assert res is not None and res.state is ResourceState.FULL
        assert res.version == 2 and res.concurrency.used_slots == 1
        result = await s2.transition_release(pool, "a", 1)
        assert result.ok and result.state is ResourceState.IDLE
        assert {r.id for r in await s2.query(pool, tags=frozenset({"us"}))} == {"a"}
    finally:
        await s2.close()


@MYSQL_REQUIRES_SERVER
async def test_purser_end_to_end_restart_reconcile(tmp_path):
    pool = pool_name()
    audit = str(tmp_path / "audit.db")
    ps1 = Purser(
        maintenance_enabled=False,
        storage="mysql",
        coordination="memory",
        mysql_url=MYSQL_URL,
        audit_path=audit,
    )
    ps1.register_type(FakeChecker())
    p1 = ps1.create_pool(pool, "fakecheck")
    await p1.import_resources(
        [{"id": "r1", "host": "10.0.0.1", "port": 8080}], import_validate="on_import"
    )
    lease = await p1.acquire(AcquireRequest(consumer="c"))
    assert lease.resource_id == "r1"
    await ps1.close()

    ps2 = Purser(
        maintenance_enabled=False,
        storage="mysql",
        coordination="memory",
        mysql_url=MYSQL_URL,
        audit_path=audit,
    )
    try:
        ps2.register_type(FakeChecker())
        p2 = ps2.create_pool(pool, "fakecheck")
        await p2.start()  # reconciliation returns orphaned slots to IDLE
        res = await p2.get("r1")
        assert res is not None and res.state is ResourceState.IDLE
        assert res.concurrency.used_slots == 0
        assert res.payload["host"] == "10.0.0.1"
        again = await p2.acquire(AcquireRequest(consumer="c"))
        assert again.resource_id == "r1"
        await p2.release(again)
        assert len(ps2.audit) >= 3
        assert ps2.audit.verify()
    finally:
        await ps2.close()


@MYSQL_REQUIRES_SERVER
async def test_secret_at_rest():
    pool = pool_name()
    store = MySqlStorage(url=MYSQL_URL, secret_fields=("password",))
    try:
        await store.put(pool, make_resource("a", payload={"username": "u", "password": "hunter2"}))
        await store._ensure_conn()
        async with store._conn.cursor() as cur:
            await cur.execute(
                "SELECT payload FROM resources WHERE pool = %s AND id = %s", (pool, "a")
            )
            row = await cur.fetchone()
        assert "hunter2" not in row[0]
        loaded = await store.get(pool, "a")
        assert loaded.payload["password"] == "hunter2"
    finally:
        await store.close()
