"""Same-process isolation: partially shared scheduling planes are rejected."""

from __future__ import annotations

import os

import fakeredis
import pytest
from conftest import FakeChecker

from purser import ConfigError, IsolationConflict, Purser
from purser.isolation import ALLOW_SHARED_ENV, redis_identity, sqlite_file_key


def make_purser(**kwargs) -> Purser:
    ps = Purser(maintenance_enabled=False, **kwargs)
    ps.register_adapter(FakeChecker())
    return ps


@pytest.fixture(autouse=True)
def _enforce_by_default(monkeypatch):
    monkeypatch.delenv(ALLOW_SHARED_ENV, raising=False)


async def test_same_sqlite_file_conflicts(tmp_path):
    db = str(tmp_path / "shared.db")
    ps1 = make_purser(storage="sqlite", sqlite_path=db)
    ps2 = make_purser(storage="sqlite", sqlite_path=db)
    try:
        ps1.create_pool("a", "fakecheck")
        with pytest.raises(IsolationConflict, match="SQLite file"):
            ps2.create_pool("b", "fakecheck")
    finally:
        await ps1.close()
        await ps2.close()

    # Once the first owner closes, the file is claimable again.
    ps3 = make_purser(storage="sqlite", sqlite_path=db)
    try:
        ps3.create_pool("c", "fakecheck")
    finally:
        await ps3.close()


async def test_same_owner_may_use_one_file_twice(tmp_path):
    ps = make_purser(storage="sqlite", sqlite_path=str(tmp_path / "own.db"))
    try:
        ps.create_pool("a", "fakecheck")
        ps.create_pool("b", "fakecheck")
    finally:
        await ps.close()


async def test_sqlite_memory_is_private_per_instance():
    ps1 = make_purser(storage="sqlite", sqlite_path=":memory:")
    ps2 = make_purser(storage="sqlite", sqlite_path=":memory:")
    try:
        ps1.create_pool("a", "fakecheck")
        ps2.create_pool("b", "fakecheck")
    finally:
        await ps1.close()
        await ps2.close()


async def test_redis_url_conflicts_even_with_memory_coordination():
    url = "redis://127.0.0.1:6379/15"
    ps1 = Purser(maintenance_enabled=False, redis_url=url)
    try:
        assert ps1.storage_kind == "memory"
        assert ps1.coordination_kind == "memory"
        # The idempotency store already owns this DB.
        with pytest.raises(IsolationConflict, match="Redis DB 15"):
            Purser(maintenance_enabled=False, redis_url=url)
    finally:
        await ps1.close()
    ps2 = Purser(maintenance_enabled=False, redis_url=url)
    await ps2.close()


def test_redis_identity_ignores_credentials_and_scheme():
    first = redis_identity("redis://user:secret@127.0.0.1:6379/15")
    second = redis_identity("rediss://:other@127.0.0.1:6379/15")
    assert first is not None and second is not None
    assert first[0] == second[0]
    assert "secret" not in first[1] and "other" not in second[1]


def test_sqlite_file_key_follows_hardlinks(tmp_path):
    original = tmp_path / "db.sqlite"
    original.write_bytes(b"")
    alias = tmp_path / "alias.sqlite"
    try:
        os.link(original, alias)
    except OSError:
        pytest.skip("hardlinks are not supported on this filesystem")

    key = sqlite_file_key(str(original))
    assert key is not None
    assert sqlite_file_key(str(alias)) == key

    other = tmp_path / "other.sqlite"
    other.write_bytes(b"")
    assert sqlite_file_key(str(other)) != key

    assert sqlite_file_key(":memory:") is None
    assert sqlite_file_key(str(tmp_path / "missing.sqlite")) is None


async def test_shared_redis_client_conflicts():
    client = fakeredis.aioredis.FakeRedis()
    ps1 = Purser(maintenance_enabled=False, redis_client=client)
    try:
        with pytest.raises(IsolationConflict, match="redis_client"):
            Purser(maintenance_enabled=False, redis_client=client)
    finally:
        await ps1.close()
        await client.aclose()


async def test_shared_mysql_connection_conflicts():
    connection = object()
    ps1 = make_purser(storage="mysql", mysql_connection=connection)
    ps2 = make_purser(storage="mysql", mysql_connection=connection)
    try:
        ps1.create_pool("a", "fakecheck")
        with pytest.raises(IsolationConflict, match="mysql_connection"):
            ps2.create_pool("b", "fakecheck")
    finally:
        await ps1.close()
        await ps2.close()


async def test_same_mysql_url_conflicts():
    first = "mysql://root:@127.0.0.1:3306/purser_isolation"
    second = "mysql://root:other@127.0.0.1:3306/purser_isolation"
    ps1 = make_purser(storage="mysql", mysql_url=first)
    ps2 = make_purser(storage="mysql", mysql_url=second)
    try:
        ps1.create_pool("a", "fakecheck")
        with pytest.raises(IsolationConflict, match="MySQL database"):
            ps2.create_pool("b", "fakecheck")
    finally:
        await ps1.close()
        await ps2.close()


async def test_independent_files_are_allowed(tmp_path):
    ps1 = make_purser(storage="sqlite", sqlite_path=str(tmp_path / "one.db"))
    ps2 = make_purser(storage="sqlite", sqlite_path=str(tmp_path / "two.db"))
    try:
        ps1.create_pool("a", "fakecheck")
        ps2.create_pool("b", "fakecheck")
    finally:
        await ps1.close()
        await ps2.close()


async def test_shared_resource_adapter_conflicts():
    adapter = FakeChecker()
    ps1 = Purser(maintenance_enabled=False)
    ps2 = Purser(maintenance_enabled=False)
    try:
        ps1.register_adapter(adapter)
        with pytest.raises(IsolationConflict, match="ResourceAdapter"):
            ps2.register_adapter(adapter)
        # Registering through the raw registry must not dodge the guard.
        ps2.adapters.register(adapter)
        with pytest.raises(IsolationConflict, match="ResourceAdapter"):
            ps2.create_pool("b", "fakecheck")
    finally:
        await ps1.close()
        await ps2.close()


async def test_delete_pool_releases_backend_claim(tmp_path):
    db = str(tmp_path / "released.db")
    ps1 = make_purser(storage="sqlite", sqlite_path=db)
    ps2 = make_purser(storage="sqlite", sqlite_path=db)
    try:
        ps1.create_pool("a", "fakecheck")
        await ps1.delete_pool("a", force=True)
        ps2.create_pool("b", "fakecheck")
    finally:
        await ps1.close()
        await ps2.close()


async def test_failed_pool_creation_releases_claims(tmp_path):
    db = str(tmp_path / "rollback.db")
    broken = make_purser(storage="sqlite", coordination="redis", sqlite_path=db)
    try:
        with pytest.raises(ConfigError):
            broken.create_pool("a", "fakecheck")
        # The abandoned sqlite claim must not block a fresh owner.
        healthy = make_purser(storage="sqlite", sqlite_path=db)
        try:
            healthy.create_pool("b", "fakecheck")
        finally:
            await healthy.close()
    finally:
        await broken.close()


async def test_escape_hatch_allows_shared_backends(tmp_path, monkeypatch):
    monkeypatch.setenv(ALLOW_SHARED_ENV, "1")
    db = str(tmp_path / "shared-hatch.db")
    ps1 = make_purser(storage="sqlite", sqlite_path=db)
    ps2 = make_purser(storage="sqlite", sqlite_path=db)
    try:
        ps1.create_pool("a", "fakecheck")
        ps2.create_pool("b", "fakecheck")
    finally:
        await ps1.close()
        await ps2.close()
