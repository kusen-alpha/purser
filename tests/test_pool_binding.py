"""Durable pin from a storage plane to the coordination plane of a pool.

Once a pool has started on a durable store, every later instance that opens
that store must use the same coordination kind and namespace.  A different
combination would split leases from rows and double-book slots, so it is
rejected at startup until the pool is deleted (the explicit rebind path).
"""

from __future__ import annotations

import json
import os
import sqlite3
import uuid

import fakeredis
import pytest
from conftest import FakeChecker
from test_cross_process_memory_guard import _build, _child

from purser import Purser
from purser.backends.coordination import RedisCoordination
from purser.backends.sqlite import SqliteStorage
from purser.binding import (
    RedisStorageBinding,
    make_binding,
    redis_coordination_id,
)
from purser.errors import IsolationConflict

REDIS_URL = os.environ.get("PURSER_TEST_REDIS_URL")
MYSQL_URL = os.environ.get("PURSER_TEST_MYSQL_URL")


def _rows(db: str) -> list[tuple]:
    conn = sqlite3.connect(db)
    try:
        return conn.execute(
            "SELECT pool, coordination_kind, coordination_id FROM pool_bindings"
        ).fetchall()
    finally:
        conn.close()


# -- cross-process public API ---------------------------------------------


@pytest.mark.skipif(not REDIS_URL, reason="PURSER_TEST_REDIS_URL not set")
def test_sqlite_memory_pin_rejects_redis_restart(tmp_path):
    db = str(tmp_path / "pin.db")
    pool_name = f"bind-mem-{uuid.uuid4().hex[:8]}"

    code, out = _child(
        tmp_path,
        "memory-first",
        storage="sqlite",
        coordination="memory",
        pool_name=pool_name,
        sqlite_path=db,
    )
    assert code == 0 and out["ok"] is True, out

    code, out = _child(
        tmp_path,
        "redis-second",
        storage="sqlite",
        coordination="redis",
        pool_name=pool_name,
        sqlite_path=db,
        redis_url=REDIS_URL,
    )
    assert code == 0
    assert out["ok"] is False
    assert "IsolationConflict" in out["error"]
    assert "bound to process-local (memory) coordination" in out["error"]

    code, out = _child(
        tmp_path,
        "memory-again",
        storage="sqlite",
        coordination="memory",
        pool_name=pool_name,
        sqlite_path=db,
    )
    assert code == 0 and out["ok"] is True, out


@pytest.mark.skipif(not REDIS_URL, reason="PURSER_TEST_REDIS_URL not set")
def test_sqlite_redis_pin_rejects_memory_restart(tmp_path):
    db = str(tmp_path / "pin.db")
    pool_name = f"bind-redis-{uuid.uuid4().hex[:8]}"

    code, out = _child(
        tmp_path,
        "redis-first",
        storage="sqlite",
        coordination="redis",
        pool_name=pool_name,
        sqlite_path=db,
        redis_url=REDIS_URL,
    )
    assert code == 0 and out["ok"] is True, out

    code, out = _child(
        tmp_path,
        "memory-second",
        storage="sqlite",
        coordination="memory",
        pool_name=pool_name,
        sqlite_path=db,
    )
    assert code == 0
    assert out["ok"] is False
    assert "IsolationConflict" in out["error"]
    assert "bound to redis coordination" in out["error"]

    code, out = _child(
        tmp_path,
        "redis-again",
        storage="sqlite",
        coordination="redis",
        pool_name=pool_name,
        sqlite_path=db,
        redis_url=REDIS_URL,
    )
    assert code == 0 and out["ok"] is True, out


@pytest.mark.skipif(
    not (MYSQL_URL and REDIS_URL),
    reason="PURSER_TEST_MYSQL_URL/PURSER_TEST_REDIS_URL not set",
)
async def test_mysql_pin_rejects_coordination_change(tmp_path):
    pool_name = f"bind-mysql-{uuid.uuid4().hex[:8]}"

    code, out = _child(
        tmp_path,
        "redis-first",
        storage="mysql",
        coordination="redis",
        pool_name=pool_name,
        redis_url=REDIS_URL,
        mysql_url=MYSQL_URL,
    )
    assert code == 0 and out["ok"] is True, out

    code, out = _child(
        tmp_path,
        "memory-second",
        storage="mysql",
        coordination="memory",
        pool_name=pool_name,
        mysql_url=MYSQL_URL,
    )
    assert code == 0
    assert out["ok"] is False
    assert "IsolationConflict" in out["error"]
    assert "bound to redis coordination" in out["error"]

    ps = _build("mysql", "redis", redis_url=REDIS_URL, mysql_url=MYSQL_URL)
    try:
        ps.create_pool(pool_name, "fakecheck")
        await ps.start()
        await ps.delete_pool(pool_name, force=True)
    finally:
        await ps.close()


# -- identity and scope ----------------------------------------------------


async def test_redis_namespace_mismatch_rejected(tmp_path):
    db = str(tmp_path / "ns.db")
    client = fakeredis.aioredis.FakeRedis()
    first = RedisCoordination(client=client, key_prefix="one")
    second = RedisCoordination(client=client, key_prefix="two")
    assert redis_coordination_id(first) != redis_coordination_id(second)

    s1 = SqliteStorage(path=db)
    s2 = SqliteStorage(path=db)
    try:
        await make_binding("p", "sqlite", "redis", s1, first).claim()
        with pytest.raises(IsolationConflict, match="redis coordination"):
            await make_binding("p", "sqlite", "redis", s2, second).claim()
        # A different pool name is a different resource set and stays free.
        await make_binding("q", "sqlite", "redis", s2, second).claim()
    finally:
        s1.close()
        s2.close()


async def test_sqlite_pool_pins_are_per_pool(tmp_path):
    db = str(tmp_path / "scope.db")
    client = fakeredis.aioredis.FakeRedis()
    redis = RedisCoordination(client=client, key_prefix="purser")
    ps = Purser(
        maintenance_enabled=False,
        storage="sqlite",
        coordination="memory",
        sqlite_path=db,
        redis_client=client,
    )
    ps.register_adapter(FakeChecker())
    try:
        memory_pool = ps.create_pool("memory-pool", "fakecheck")
        redis_pool = ps.create_pool(
            "redis-pool", "fakecheck", config={"coordination": "redis"}
        )
        await ps.start()
        assert memory_pool.started and redis_pool.started
        assert sorted(_rows(db)) == sorted(
            [
                ("memory-pool", "memory", "memory"),
                ("redis-pool", "redis", redis_coordination_id(redis)),
            ]
        )
    finally:
        await ps.close()


# -- rebinding and rollback ------------------------------------------------


async def test_delete_pool_allows_rebinding(tmp_path):
    db = str(tmp_path / "rebind.db")
    client = fakeredis.aioredis.FakeRedis()

    def build(coordination: str) -> Purser:
        ps = Purser(
            maintenance_enabled=False,
            storage="sqlite",
            coordination=coordination,
            sqlite_path=db,
            redis_client=client,
        )
        ps.register_adapter(FakeChecker())
        return ps

    first = build("memory")
    try:
        first.create_pool("p", "fakecheck")
        await first.start()
    finally:
        await first.close()

    blocked = build("redis")
    try:
        blocked.create_pool("p", "fakecheck")
        with pytest.raises(IsolationConflict, match="bound to process-local"):
            await blocked.start()
    finally:
        await blocked.close()

    owner = build("memory")
    try:
        owner.create_pool("p", "fakecheck")
        await owner.start()
        await owner.delete_pool("p", force=True)
    finally:
        await owner.close()

    rebound = build("redis")
    try:
        pool = rebound.create_pool("p", "fakecheck")
        await rebound.start()
        assert pool.started
    finally:
        await rebound.close()


def _marker_key(pool: str, prefix: str = "purser") -> str:
    return f"{prefix}:binding:{{{pool}}}:storage"


# -- reverse: one Redis namespace belongs to one storage -------------------


@pytest.mark.skipif(not REDIS_URL, reason="PURSER_TEST_REDIS_URL not set")
async def test_redis_namespace_rejects_other_sqlite_file(tmp_path):
    pool_name = f"rev-sqlite-{uuid.uuid4().hex[:8]}"
    db_a = str(tmp_path / "a.db")
    db_b = str(tmp_path / "b.db")
    owner = _build("sqlite", "redis", sqlite_path=db_a, redis_url=REDIS_URL)
    owner.create_pool(pool_name, "fakecheck")
    await owner.start()
    try:
        code, out = _child(
            tmp_path,
            "other-file",
            storage="sqlite",
            coordination="redis",
            pool_name=pool_name,
            sqlite_path=db_b,
            redis_url=REDIS_URL,
        )
        assert code == 0
        assert out["ok"] is False
        assert "IsolationConflict" in out["error"]
        assert "Redis namespace" in out["error"]
        assert "already used by another live Purser" in out["error"]

        # The same file + namespace is the supported shared combination.
        code, out = _child(
            tmp_path,
            "same-file",
            storage="sqlite",
            coordination="redis",
            pool_name=pool_name,
            sqlite_path=db_a,
            redis_url=REDIS_URL,
        )
        assert code == 0 and out["ok"] is True, out
    finally:
        await owner.close()

    # The live marker is released on close, so another storage may own it.
    code, out = _child(
        tmp_path,
        "after-close",
        storage="sqlite",
        coordination="redis",
        pool_name=pool_name,
        sqlite_path=db_b,
        redis_url=REDIS_URL,
    )
    assert code == 0 and out["ok"] is True, out


@pytest.mark.skipif(not REDIS_URL, reason="PURSER_TEST_REDIS_URL not set")
async def test_redis_namespace_rejects_memory_storage(tmp_path):
    pool_name = f"rev-mixed-{uuid.uuid4().hex[:8]}"
    db = str(tmp_path / "mixed.db")
    owner = _build("sqlite", "redis", sqlite_path=db, redis_url=REDIS_URL)
    owner.create_pool(pool_name, "fakecheck")
    await owner.start()
    try:
        code, out = _child(
            tmp_path,
            "memory-storage",
            storage="memory",
            coordination="redis",
            pool_name=pool_name,
            redis_url=REDIS_URL,
        )
        assert code == 0
        assert out["ok"] is False
        assert "IsolationConflict" in out["error"]
        assert "Redis namespace" in out["error"]
        assert "storage sqlite:" in out["error"]
    finally:
        await owner.close()

    code, out = _child(
        tmp_path,
        "after-close",
        storage="memory",
        coordination="redis",
        pool_name=pool_name,
        redis_url=REDIS_URL,
    )
    assert code == 0 and out["ok"] is True, out


@pytest.mark.skipif(not REDIS_URL, reason="PURSER_TEST_REDIS_URL not set")
async def test_redis_namespace_rejects_durable_against_memory(tmp_path):
    pool_name = f"rev-memown-{uuid.uuid4().hex[:8]}"
    db = str(tmp_path / "durable.db")
    owner = _build("memory", "redis", redis_url=REDIS_URL)
    owner.create_pool(pool_name, "fakecheck")
    await owner.start()
    try:
        code, out = _child(
            tmp_path,
            "durable-storage",
            storage="sqlite",
            coordination="redis",
            pool_name=pool_name,
            sqlite_path=db,
            redis_url=REDIS_URL,
        )
        assert code == 0
        assert out["ok"] is False
        assert "IsolationConflict" in out["error"]
        assert "Redis namespace" in out["error"]
        assert "storage memory" in out["error"]
    finally:
        await owner.close()

    code, out = _child(
        tmp_path,
        "after-close",
        storage="sqlite",
        coordination="redis",
        pool_name=pool_name,
        sqlite_path=db,
        redis_url=REDIS_URL,
    )
    assert code == 0 and out["ok"] is True, out


@pytest.mark.skipif(not REDIS_URL, reason="PURSER_TEST_REDIS_URL not set")
async def test_sqlite_memory_path_with_redis_rejected_cross_process(tmp_path):
    # ``sqlite :memory:`` has private rows but shares leases through Redis, the
    # same half-share as ``memory`` storage; it now takes the Redis guard.
    pool_name = f"rev-inmem-{uuid.uuid4().hex[:8]}"
    owner = _build("sqlite", "redis", sqlite_path=":memory:", redis_url=REDIS_URL)
    owner.create_pool(pool_name, "fakecheck")
    await owner.start()
    try:
        code, out = _child(
            tmp_path,
            "other-process",
            storage="sqlite",
            coordination="redis",
            pool_name=pool_name,
            sqlite_path=":memory:",
            redis_url=REDIS_URL,
        )
        assert code == 0
        assert out["ok"] is False
        assert "IsolationConflict" in out["error"]
        assert "in-memory storage" in out["error"]
    finally:
        await owner.close()


async def test_reverse_marker_claim_release_and_token_isolation(tmp_path):
    client = fakeredis.aioredis.FakeRedis()
    co = RedisCoordination(client=client, key_prefix="purser")
    storage_a = SqliteStorage(path=str(tmp_path / "a.db"))
    storage_b = SqliteStorage(path=str(tmp_path / "b.db"))
    key = _marker_key("p")
    first = RedisStorageBinding(co, "p", "sqlite", storage_a)
    try:
        await first.claim()
        assert await client.exists(key) == 1
        record = json.loads(await client.get(key))
        assert record["storage_kind"] == "sqlite"
        assert record["storage_id"].startswith("sqlite:")
        assert record["pid"] == os.getpid()

        with pytest.raises(IsolationConflict, match="Redis namespace"):
            await RedisStorageBinding(co, "p", "sqlite", storage_b).claim()

        # The same storage may re-claim; it is one deployment.
        again = RedisStorageBinding(co, "p", "sqlite", storage_a)
        await again.claim()
        # A storage that does not own the marker must not delete it.
        await RedisStorageBinding(co, "p", "sqlite", storage_b).release()
        assert await client.exists(key) == 1
        await again.release()
        assert await client.exists(key) == 0

        # The namespace is free once released.
        third = RedisStorageBinding(co, "p", "sqlite", storage_b)
        await third.claim()
        assert await client.exists(key) == 1
        await third.release()
    finally:
        await first.release()
        storage_a.close()
        storage_b.close()
        await client.aclose()


async def test_reverse_marker_treats_hardlink_as_same_storage(tmp_path):
    client = fakeredis.aioredis.FakeRedis()
    co = RedisCoordination(client=client, key_prefix="purser")
    original = tmp_path / "db.sqlite"
    storage_a = SqliteStorage(path=str(original))
    alias = tmp_path / "alias.sqlite"
    try:
        os.link(original, alias)
    except OSError:
        storage_a.close()
        await client.aclose()
        pytest.skip("hardlinks are not supported on this filesystem")
    storage_b = SqliteStorage(path=str(alias))
    first = RedisStorageBinding(co, "p", "sqlite", storage_a)
    try:
        await first.claim()
        record = json.loads(await client.get(_marker_key("p")))
        assert record["storage_id"].startswith("sqlite:d")

        # A hardlink is the same file, so it may share the namespace.
        twin = RedisStorageBinding(co, "p", "sqlite", storage_b)
        await twin.claim()
        taken = json.loads(await client.get(_marker_key("p")))
        assert taken["storage_id"] == record["storage_id"]
        await twin.release()
        assert await client.exists(_marker_key("p")) == 0
    finally:
        await first.release()
        storage_a.close()
        storage_b.close()
        await client.aclose()


async def test_reverse_marker_skipped_by_escape_hatch(tmp_path, monkeypatch):
    monkeypatch.setenv("PURSER_ALLOW_SHARED_BACKENDS", "1")
    client = fakeredis.aioredis.FakeRedis()
    co = RedisCoordination(client=client, key_prefix="purser")
    storage_a = SqliteStorage(path=str(tmp_path / "a.db"))
    storage_b = SqliteStorage(path=str(tmp_path / "b.db"))
    first = RedisStorageBinding(co, "p", "sqlite", storage_a)
    second = RedisStorageBinding(co, "p", "sqlite", storage_b)
    try:
        await first.claim()
        await second.claim()
        assert await client.exists(_marker_key("p")) == 0
    finally:
        await first.release()
        await second.release()
        storage_a.close()
        storage_b.close()
        await client.aclose()


async def test_close_releases_redis_storage_marker(tmp_path):
    client = fakeredis.aioredis.FakeRedis()
    db_a = str(tmp_path / "a.db")
    db_b = str(tmp_path / "b.db")

    def build(path: str) -> Purser:
        ps = Purser(
            maintenance_enabled=False,
            storage="sqlite",
            coordination="redis",
            sqlite_path=path,
            redis_client=client,
        )
        ps.register_adapter(FakeChecker())
        return ps

    owner = build(db_a)
    owner.create_pool("p", "fakecheck")
    await owner.start()
    try:
        assert await client.exists(_marker_key("p")) == 1
    finally:
        await owner.close()
    assert await client.exists(_marker_key("p")) == 0

    retry = build(db_b)
    retry.create_pool("p", "fakecheck")
    try:
        await retry.start()  # marker was released, not left to the TTL
    finally:
        await retry.close()
    assert await client.exists(_marker_key("p")) == 0


async def test_failed_start_rolls_back_new_pin(tmp_path):
    db = str(tmp_path / "rollback.db")
    client = fakeredis.aioredis.FakeRedis()
    first = Purser(
        maintenance_enabled=False,
        storage="sqlite",
        coordination="memory",
        sqlite_path=db,
    )
    first.register_adapter(FakeChecker())
    pool = first.create_pool("p", "fakecheck")

    async def boom() -> None:
        raise RuntimeError("reconcile failed")

    pool._reconcile_leases = boom  # type: ignore[method-assign]
    try:
        with pytest.raises(RuntimeError, match="reconcile failed"):
            await pool.start()
        assert _rows(db) == []
    finally:
        await first.close()

    second = Purser(
        maintenance_enabled=False,
        storage="sqlite",
        coordination="redis",
        sqlite_path=db,
        redis_client=client,
    )
    second.register_adapter(FakeChecker())
    try:
        second.create_pool("p", "fakecheck")
        await second.start()
    finally:
        await second.close()
