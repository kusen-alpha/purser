"""Authority guard edges: owner descriptions, SQLite locks, Redis markers.

Only ``fakeredis`` and local temporary files are used; no server is required.
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import time
from types import SimpleNamespace

import fakeredis
import pytest

from purser.authority import (
    MySqlNamedLockAuthority,
    NullAuthority,
    RedisAuthority,
    SqliteFileAuthority,
    _describe_owner,
    _same_process,
    make_authority,
)
from purser.backends.sqlite import SqliteStorage
from purser.errors import IsolationConflict

# -- helpers ---------------------------------------------------------------


def fake_client() -> fakeredis.aioredis.FakeRedis:
    return fakeredis.aioredis.FakeRedis()


def foreign_record(*, pid: int = 999_999, host: str = "another-host") -> str:
    return json.dumps(
        {
            "token": "foreign",
            "pid": pid,
            "host": host,
            "pool": "p",
            "kind": "memory+redis",
            "started_at": time.time(),
        },
        sort_keys=True,
    )


class _FakeCursor:
    def __init__(self, conn: _FakeMysql) -> None:
        self._conn = conn
        self._row: tuple | None = None

    async def __aenter__(self) -> _FakeCursor:
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False

    async def execute(self, sql: str, params: object = None) -> None:
        self._conn.executed.append((sql, params))
        if self._conn.raise_on_execute:
            raise RuntimeError("mysql went away")
        if "GET_LOCK" in sql:
            self._row = self._conn.get_lock_row
        elif "RELEASE_LOCK" in sql:
            self._row = (1,)
        elif "DATABASE()" in sql:
            self._row = (self._conn.database,)
        else:
            self._row = (None,)

    async def fetchone(self) -> tuple | None:
        return self._row


class _FakeMysql:
    """Minimal mysqldb-shaped storage for the named-lock authority."""

    def __init__(
        self,
        *,
        database: str = "purser",
        get_lock_row: tuple | None = (1,),
    ) -> None:
        self._lock = asyncio.Lock()
        self._conn: _FakeMysql | None = self
        self.database = database
        self.get_lock_row = get_lock_row
        self.executed: list[tuple[str, object]] = []
        self.raise_on_execute = False
        self.ensure_conn_calls = 0

    async def _ensure_conn(self) -> None:
        self.ensure_conn_calls += 1

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)


class _ScriptedRedis:
    """Client whose renew/reclaim answers are a fixed script."""

    def __init__(self, renews: list, reclaims: list) -> None:
        self.renews = list(renews)
        self.reclaims = list(reclaims)
        self.eval_calls = 0
        self.set_calls = 0

    async def eval(self, script: str, numkeys: int, key: str, value: str, ttl: int):
        self.eval_calls += 1
        return self.renews.pop(0)

    async def set(self, key: str, value: str, *, nx: bool = False, px: int | None = None):
        self.set_calls += 1
        return self.reclaims.pop(0)


class _BrokenRedis:
    async def eval(self, *args, **kwargs):
        raise RuntimeError("connection reset")

    async def set(self, *args, **kwargs):
        raise RuntimeError("connection reset")

    async def get(self, *args, **kwargs):
        raise RuntimeError("connection reset")


# -- _describe_owner -------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (foreign_record(pid=42, host="box"), "(pid 42, host box, since "),
        (foreign_record(pid=42, host="box").encode(), "(pid 42, host box, since "),
        ("not json", ""),
        (json.dumps([1, 2]), ""),
        (json.dumps({}), ""),
        (json.dumps({"started_at": "yesterday"}), ""),
        (None, ""),
        (b"\xff\xfe", ""),
        (json.dumps({"started_at": 1_700_000_000}), "(since "),
    ],
)
def test_describe_owner_handles_every_payload_shape(raw, expected):
    described = _describe_owner(raw)
    assert described.strip().startswith(expected)
    if expected == "":
        assert described == ""


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (foreign_record(pid=os.getpid(), host=socket.gethostname()), True),
        (
            foreign_record(pid=os.getpid(), host=socket.gethostname()).encode(),
            True,
        ),
        (foreign_record(pid=os.getpid(), host="other"), False),
        ("not json", False),
        (b"not json", False),
        (json.dumps([1]), False),
        (None, False),
    ],
)
def test_same_process_recognises_this_host_only(raw, expected):
    assert _same_process(raw) is expected


# -- RedisAuthority --------------------------------------------------------


async def test_redis_authority_acquires_marker_and_releases_it():
    client = fake_client()
    auth = RedisAuthority(client, "purser", "p")
    try:
        await auth.acquire()
        assert auth._held is True
        assert auth._task is not None
        assert await client.get(auth._key) == auth._token.encode()
        ttl = await client.pttl(auth._key)
        assert 0 < ttl <= 60_000

        await auth.release()
        assert auth._held is False
        assert auth._task is None
        assert await client.exists(auth._key) == 0
        await auth.release()  # idempotent
    finally:
        await auth.release()
        await client.aclose()


async def test_redis_authority_acquire_twice_keeps_the_first_marker():
    client = fake_client()
    first = RedisAuthority(client, "purser", "p")
    try:
        await first.acquire()
        token = first._token
        await first.acquire()  # already held: no second set
        assert first._token == token
        assert await client.get(first._key) == token.encode()
    finally:
        await first.release()
        await client.aclose()


async def test_redis_authority_conflict_names_the_live_owner():
    client = fake_client()
    auth = RedisAuthority(client, "purser", "p")
    await client.set(auth._key, foreign_record(pid=4242, host="elsewhere"))
    try:
        with pytest.raises(IsolationConflict) as err:
            await auth.acquire()
        message = str(err.value)
        assert "in-memory storage" in message
        assert "pid 4242" in message
        assert "host elsewhere" in message
        assert auth._held is False
    finally:
        await client.aclose()


async def test_redis_authority_takes_over_a_stale_same_process_marker(caplog):
    client = fake_client()
    auth = RedisAuthority(client, "purser", "p")
    stale = foreign_record(pid=os.getpid(), host=socket.gethostname())
    await client.set(auth._key, stale)
    try:
        with caplog.at_level("WARNING", logger="purser.authority"):
            await auth.acquire()
        assert auth._held is True
        assert await client.get(auth._key) == auth._token.encode()
        assert "stale Redis authority" in caplog.text
    finally:
        await auth.release()
        await client.aclose()


async def test_redis_authority_escape_hatch_skips_marker(monkeypatch):
    monkeypatch.setenv("PURSER_ALLOW_SHARED_BACKENDS", "1")
    client = fake_client()
    auth = RedisAuthority(client, "purser", "p")
    seeded = foreign_record()
    await client.set(auth._key, seeded)
    try:
        await auth.acquire()
        assert auth._held is False
        assert await client.get(auth._key) == seeded.encode()
        await auth.release()  # no-op without ownership
        assert await client.exists(auth._key) == 1
    finally:
        await client.aclose()


async def test_redis_authority_release_swallows_client_failures():
    auth = RedisAuthority(fake_client(), "purser", "p")
    auth._held = True
    auth._client = _BrokenRedis()
    await auth.release()  # logged, not raised
    assert auth._held is False


async def test_redis_heartbeat_renews_then_backs_off_when_marker_is_lost(
    monkeypatch,
):
    monkeypatch.setattr("purser.authority.REDIS_HEARTBEAT_SECONDS", 0)
    auth = RedisAuthority(fake_client(), "purser", "p")
    scripted = _ScriptedRedis(renews=[1, 0], reclaims=[False])
    auth._client = scripted
    await auth._heartbeat()  # exits after the failed reclaim
    assert scripted.eval_calls == 2
    assert scripted.set_calls == 1


async def test_redis_heartbeat_reclaims_a_lost_marker(monkeypatch):
    monkeypatch.setattr("purser.authority.REDIS_HEARTBEAT_SECONDS", 0)
    auth = RedisAuthority(fake_client(), "purser", "p")
    scripted = _ScriptedRedis(renews=[0, 1, 0], reclaims=[True, False])
    auth._client = scripted
    await auth._heartbeat()
    assert scripted.eval_calls == 3
    assert scripted.set_calls == 2


async def test_redis_heartbeat_swallows_errors(monkeypatch):
    monkeypatch.setattr("purser.authority.REDIS_HEARTBEAT_SECONDS", 0)
    auth = RedisAuthority(fake_client(), "purser", "p")
    auth._client = _BrokenRedis()
    await auth._heartbeat()  # warning only


async def test_redis_heartbeat_propagates_cancellation(monkeypatch):
    monkeypatch.setattr("purser.authority.REDIS_HEARTBEAT_SECONDS", 0)

    class _CancellingRedis:
        async def eval(self, *args, **kwargs):
            raise asyncio.CancelledError

    auth = RedisAuthority(fake_client(), "purser", "p")
    auth._client = _CancellingRedis()
    with pytest.raises(asyncio.CancelledError):
        await auth._heartbeat()


# -- SqliteFileAuthority ---------------------------------------------------


async def test_sqlite_authority_writes_owner_and_releases(tmp_path):
    path = str(tmp_path / "authority.db")
    auth = SqliteFileAuthority(path, "p")
    try:
        await auth.acquire()
        assert auth._fd is not None
        record = json.loads(auth._read_owner())
        assert record["pid"] == os.getpid()
        assert record["kind"] == "sqlite+memory"
        assert record["pool"] == "p"
        lock_file = tmp_path / "authority.db.purser-authority-"
        assert any(entry.name.startswith(lock_file.name) for entry in tmp_path.iterdir())
    finally:
        await auth.release()
    assert auth._fd is None
    await auth.release()  # idempotent


async def test_sqlite_authority_uses_inode_in_the_lock_name(tmp_path):
    path = tmp_path / "existing.db"
    path.write_bytes(b"")
    auth = SqliteFileAuthority(str(path), "p")
    assert auth._lock_path.lower().startswith(str(tmp_path).lower())
    assert os.path.basename(auth._lock_path).startswith(".purser-authority-")
    await auth.acquire()
    await auth.release()


async def test_sqlite_authority_rejects_a_second_live_owner(tmp_path):
    path = str(tmp_path / "shared.db")
    first = SqliteFileAuthority(path, "p")
    second = SqliteFileAuthority(path, "p")
    await first.acquire()
    try:
        try:
            await second.acquire()
        except IsolationConflict as exc:
            message = str(exc)
            assert "SQLite file" in message
            assert "pid" in message
        else:  # pragma: no cover - POSIX allows same-process re-locking
            await second.release()
            pytest.skip("this platform allows re-locking one file in a process")
        # A different pool has its own sidecar lock.
        other = SqliteFileAuthority(path, "other")
        await other.acquire()
        await other.release()
    finally:
        await first.release()


async def test_sqlite_authority_escape_hatch_and_missing_owner(tmp_path, monkeypatch):
    path = str(tmp_path / "escape.db")
    auth = SqliteFileAuthority(path, "p")
    monkeypatch.setenv("PURSER_ALLOW_SHARED_BACKENDS", "1")
    try:
        await auth.acquire()
        assert auth._fd is None  # escape hatch: nothing locked, no file opened
    finally:
        await auth.release()
    # _read_owner survives a missing sidecar file.
    auth._lock_path = str(tmp_path / "does-not-exist.lock")
    assert auth._read_owner() == ""


def test_sqlite_authority_requires_a_file_backed_path():
    with pytest.raises(ValueError, match="file-backed SQLite path"):
        SqliteFileAuthority(":memory:", "p")


async def test_sqlite_authority_survives_write_and_unlock_failures(
    tmp_path, monkeypatch
):
    import purser.authority as authority_module

    path = str(tmp_path / "flaky.db")
    auth = SqliteFileAuthority(path, "p")

    def _boom(*args, **kwargs):
        raise OSError("disk full")

    with monkeypatch.context() as patch:
        patch.setattr(authority_module.os, "write", _boom)
        await auth.acquire()  # owner record write is best effort
    assert auth._fd is not None

    with monkeypatch.context() as patch:
        patch.setattr(authority_module, "_unlock", _boom)
        await auth.release()  # unlock failure is swallowed, fd still closed
    assert auth._fd is None


# -- MySqlNamedLockAuthority ----------------------------------------------


async def test_mysql_authority_takes_and_releases_the_named_lock():
    storage = _FakeMysql(database="purser")
    auth = MySqlNamedLockAuthority(storage, "p")
    name = await auth._lock_name()
    assert name.startswith("purser-auth-")
    assert await auth._lock_name() == name  # cached
    assert any("DATABASE()" in sql for sql, _ in storage.executed)
    try:
        await auth.acquire()
        assert auth._held is True
        assert any("GET_LOCK" in sql for sql, _ in storage.executed)
        await auth.acquire()  # already held
        await auth.release()
        assert any("RELEASE_LOCK" in sql for sql, _ in storage.executed)
        assert auth._held is False
        await auth.release()  # idempotent
    finally:
        await auth.release()


async def test_mysql_authority_conflict_reports_the_database():
    storage = _FakeMysql(database="otherdb", get_lock_row=(0,))
    auth = MySqlNamedLockAuthority(storage, "p")
    with pytest.raises(IsolationConflict, match="MySQL database 'otherdb'"):
        await auth.acquire()
    assert auth._held is False


async def test_mysql_authority_release_edges():
    storage = _FakeMysql()
    auth = MySqlNamedLockAuthority(storage, "p")
    await auth.acquire()
    storage._conn = None  # connection already dropped
    await auth.release()
    assert auth._held is False

    storage = _FakeMysql()
    auth = MySqlNamedLockAuthority(storage, "p")
    await auth.acquire()
    storage.raise_on_execute = True
    await auth.release()  # logged, not raised


async def test_mysql_authority_escape_hatch(monkeypatch):
    monkeypatch.setenv("PURSER_ALLOW_SHARED_BACKENDS", "1")
    storage = _FakeMysql()
    auth = MySqlNamedLockAuthority(storage, "p")
    await auth.acquire()
    assert auth._held is False
    assert storage.executed == []


# -- factory ---------------------------------------------------------------


async def test_make_authority_picks_the_guard_for_each_mix(tmp_path):
    client = fake_client()
    memory_coordination = SimpleNamespace(_client=None)
    redis_coordination = SimpleNamespace(_client=client, key_prefix="purser")
    memory_storage = SimpleNamespace(_path=":memory:")
    file_storage = SqliteStorage(path=str(tmp_path / "auth.db"))
    mysql_storage = SimpleNamespace()
    try:
        assert isinstance(
            make_authority("p", "memory", "memory", memory_storage, memory_coordination),
            NullAuthority,
        )
        assert isinstance(
            make_authority("p", "memory", "redis", memory_storage, memory_coordination),
            NullAuthority,
        )
        assert isinstance(
            make_authority("p", "memory", "redis", memory_storage, redis_coordination),
            RedisAuthority,
        )
        assert isinstance(
            make_authority("p", "sqlite", "memory", memory_storage, memory_coordination),
            NullAuthority,
        )
        authority = make_authority(
            "p", "sqlite", "memory", file_storage, memory_coordination
        )
        assert isinstance(authority, SqliteFileAuthority)
        assert isinstance(
            make_authority("p", "sqlite", "redis", memory_storage, redis_coordination),
            RedisAuthority,
        )
        assert isinstance(
            make_authority("p", "sqlite", "redis", file_storage, redis_coordination),
            NullAuthority,
        )
        assert isinstance(
            make_authority("p", "mysql", "memory", None, memory_coordination),
            NullAuthority,
        )
        assert isinstance(
            make_authority("p", "mysql", "memory", mysql_storage, memory_coordination),
            MySqlNamedLockAuthority,
        )
        assert isinstance(
            make_authority("p", "mysql", "redis", mysql_storage, redis_coordination),
            NullAuthority,
        )
    finally:
        await file_storage.close()
        await client.aclose()
