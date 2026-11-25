"""Branch coverage for the durable coordination binding of a pool.

Covers the SQLite/MySQL ``pool_bindings`` claim, rollback and forget paths, the
Redis storage marker (claim, heartbeat, reclaim, release), the identity helpers
and ``make_binding`` dispatch.  All fakes are local and deterministic; Redis
uses ``fakeredis`` exactly like the rest of the suite.
"""

from __future__ import annotations

import asyncio
import json
import re
import sqlite3
import types

import fakeredis
import pytest

import purser.binding as binding_module
from purser.binding import (
    CompositeBinding,
    MySqlPoolBinding,
    NullBinding,
    PoolBinding,
    RedisStorageBinding,
    SqlitePoolBinding,
    _describe_remote,
    _forward_binding,
    _load_record,
    _mysql_storage_identity,
    _storage_identity,
    make_binding,
    redis_coordination_id,
)
from purser.errors import BackendUnavailable, IsolationConflict
from purser.isolation import ALLOW_SHARED_ENV

# -- identity helpers ------------------------------------------------------


class _RawCursor:
    def __init__(self, storage: _RawMysql) -> None:
        self._storage = storage

    async def execute(self, sql: str, params=None) -> None:
        self._storage.executed.append((sql, params))
        if self._storage.uuid_error and "@@server_uuid" in sql:
            raise RuntimeError("MariaDB lacks @@server_uuid")
        if "@@server_uuid" in sql:
            self._storage.row = self._storage.uuid_row
        elif "@@hostname" in sql:
            self._storage.row = self._storage.host_row

    async def fetchone(self):
        return self._storage.row

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info) -> bool:
        return False


class _RawMysql:
    """Minimal storage double exposing only what the binding touches."""

    def __init__(
        self,
        *,
        uuid_row=None,
        host_row=None,
        uuid_error=False,
        connect_error=False,
        url="mysql://user:pw@db.example:3307/app",
    ) -> None:
        self._lock = asyncio.Lock()
        self._conn = self
        self._url = url
        self.uuid_row = uuid_row
        self.host_row = host_row
        self.uuid_error = uuid_error
        self.connect_error = connect_error
        self.row = None
        self.forced_row = None
        self.executed: list[tuple[str, object]] = []

    async def _ensure_conn(self) -> None:
        if self.connect_error:
            raise RuntimeError("cannot connect")

    def cursor(self) -> _RawCursor:
        return _RawCursor(self)


async def test_mysql_identity_prefers_server_uuid():
    storage = _RawMysql(uuid_row=("uuid-1", "app"))
    assert await _mysql_storage_identity(storage) == "mysql:uuid-1/app"


async def test_mysql_identity_falls_back_to_hostname():
    storage = _RawMysql(uuid_error=True, host_row=("host-1", "app"))
    assert await _mysql_storage_identity(storage) == "mysql:host-1/app"


async def test_mysql_identity_handles_empty_uuid_row():
    storage = _RawMysql(uuid_row=None, host_row=("host-1", None))
    assert await _mysql_storage_identity(storage) == "mysql:host-1/"


async def test_mysql_identity_falls_back_to_url():
    storage = _RawMysql(connect_error=True)
    assert await _mysql_storage_identity(storage) == "mysql:db.example/3307/app"


async def test_mysql_identity_without_url_is_unknown():
    storage = _RawMysql(connect_error=True, url=None)
    assert await _mysql_storage_identity(storage) is None


async def test_storage_identity_dispatch(tmp_path):
    assert await _storage_identity("memory", object()) == "memory"

    in_memory = types.SimpleNamespace(_path=":memory:")
    assert await _storage_identity("sqlite", in_memory) == "memory"

    missing = types.SimpleNamespace(_path=str(tmp_path / "missing.sqlite"))
    identity = await _storage_identity("sqlite", missing)
    assert identity is not None and identity.startswith("sqlite:")
    assert "missing.sqlite" in identity

    existing = tmp_path / "exists.sqlite"
    existing.write_bytes(b"")
    identity = await _storage_identity(
        "sqlite", types.SimpleNamespace(_path=str(existing))
    )
    assert identity is not None
    assert re.fullmatch(r"sqlite:d[0-9a-f]+-i[0-9a-f]+", identity)

    mysql = _RawMysql(uuid_row=("uuid-2", "app"))
    assert await _storage_identity("mysql", mysql) == "mysql:uuid-2/app"
    assert await _storage_identity("postgres", object()) is None


def test_load_and_describe_record():
    payload = {"storage_id": "sqlite:/x", "pid": 7, "host": "web-1"}
    assert _load_record(json.dumps(payload).encode()) == payload
    assert _load_record("{not json") == {}
    assert _load_record(json.dumps(["a"])) == {}
    assert _load_record(None) == {}
    assert _describe_remote(json.dumps(payload)) == "sqlite:/x (pid 7, host web-1)"
    assert _describe_remote(json.dumps({"storage_id": "memory"})) == "memory"
    assert _describe_remote(b"garbage") == "an unknown storage"


def test_redis_coordination_id_variants():
    class _Client:
        def __init__(self, **kwargs):
            self.connection_pool = types.SimpleNamespace(connection_kwargs=kwargs)

    class _Coord:
        def __init__(self, client, prefix="pfx"):
            self._client = client
            self.key_prefix = prefix

    assert redis_coordination_id(types.SimpleNamespace(_client=None)) is None
    assert (
        redis_coordination_id(_Coord(_Client(host="H", port=6380, db=3)))
        == "redis:H:6380/3#pfx"
    )
    assert (
        redis_coordination_id(_Coord(_Client(hostname="HN")))
        == "redis:HN:6379/0#pfx"
    )
    assert (
        redis_coordination_id(_Coord(_Client(path="/var/run/redis.sock")))
        == "redis:unix:/var/run/redis.sock/0#pfx"
    )
    assert redis_coordination_id(_Coord(_Client(port=6379))) is None
    assert (
        redis_coordination_id(_Coord(_Client(host="H", db="")))
        == "redis:H:6379/0#pfx"
    )
    assert redis_coordination_id(types.SimpleNamespace(_client=_Client(host="H"))) == (
        "redis:H:6379/0#purser"
    )


async def test_redis_coordination_id_marks_fake_clients():
    client = fakeredis.aioredis.FakeRedis()
    try:
        coordination = types.SimpleNamespace(_client=client, key_prefix="pfx")
        assert redis_coordination_id(coordination) == "redis:fake/0#pfx"
    finally:
        await client.aclose()


# -- PoolBinding bookkeeping ----------------------------------------------


def test_pool_binding_accept_and_rollback_token():
    binding = PoolBinding("pool", "SQLite file 'x'", "redis", "ns")
    binding._accept("redis", "ns", "other")
    assert binding._accepted is True
    assert binding._rollback_token is None

    token = binding._new_token()
    binding._accept("redis", "ns", token)
    assert binding._rollback_token == token
    assert binding._take_rollback_token() == token
    assert binding._accepted is False
    assert binding._take_rollback_token() is None


def test_pool_binding_rejects_different_coordination():
    binding = PoolBinding("pool", "SQLite file 'x'", "redis", "ns")
    with pytest.raises(IsolationConflict) as exc:
        binding._accept("memory", "memory", None)
    message = str(exc.value)
    assert "already bound to" in message
    assert "process-local (memory) coordination" in message
    assert "delete_pool" in message

    memory_bound = PoolBinding("pool", "SQLite file 'x'", "memory", "memory")
    with pytest.raises(IsolationConflict):
        memory_bound._accept("redis", "redis:fake/0#pfx", None)


async def test_null_binding_is_inert():
    binding = NullBinding("pool", "", "redis", "ns")
    await binding.claim()
    await binding.release()
    await binding.rollback()
    await binding.forget()


# -- SQLite bindings -------------------------------------------------------


class _SqliteStorage:
    def __init__(self, conn: sqlite3.Connection, path: str) -> None:
        self._conn = conn
        self._path = path
        self._lock = asyncio.Lock()

    def _retry_locked(self, fn, *args):
        return fn(*args)


@pytest.fixture
def sqlite_binding(tmp_path):
    path = str(tmp_path / "binding.sqlite")
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.execute(
        "CREATE TABLE pool_bindings (pool TEXT, storage_kind TEXT, "
        "coordination_kind TEXT, coordination_id TEXT, claim_token TEXT, "
        "updated_at REAL)"
    )
    conn.commit()
    storage = _SqliteStorage(conn, path)
    yield storage, path
    conn.close()


def _rows(storage: _SqliteStorage) -> list[tuple]:
    return storage._conn.execute(
        "SELECT pool, coordination_kind, coordination_id, claim_token "
        "FROM pool_bindings"
    ).fetchall()


async def test_sqlite_binding_claim_selects_and_rolls_back(sqlite_binding):
    storage, path = sqlite_binding
    binding = SqlitePoolBinding(storage, "pool", path, "redis", "ns")
    await binding.claim()
    assert binding._accepted is True
    rows = _rows(storage)
    assert rows and rows[0][1:] == ("redis", "ns", binding._token)

    # Re-claiming an accepted binding is a no-op.
    await binding.claim()
    assert len(_rows(storage)) == 1

    await binding.rollback()
    assert _rows(storage) == []
    # A second rollback has no token left to delete.
    await binding.rollback()

    await binding.claim()
    assert len(_rows(storage)) == 1


async def test_sqlite_binding_rejects_other_coordination(sqlite_binding):
    storage, path = sqlite_binding
    first = SqlitePoolBinding(storage, "pool", path, "redis", "ns-1")
    await first.claim()

    second = SqlitePoolBinding(storage, "pool", path, "redis", "ns-2")
    with pytest.raises(IsolationConflict) as exc:
        await second.claim()
    assert "already bound to" in str(exc.value)


async def test_sqlite_binding_rollback_failure_is_logged(
    sqlite_binding, monkeypatch, caplog
):
    storage, path = sqlite_binding
    binding = SqlitePoolBinding(storage, "pool", path, "redis", "ns")
    await binding.claim()

    def broken(fn, *args):
        raise RuntimeError("database is locked")

    monkeypatch.setattr(storage, "_retry_locked", broken)
    import logging

    with caplog.at_level(logging.WARNING, logger="purser.binding"):
        await binding.rollback()
    assert "failed to roll back binding" in caplog.text


async def test_sqlite_binding_shared_backends_are_skipped(
    sqlite_binding, monkeypatch
):
    storage, path = sqlite_binding
    monkeypatch.setenv(ALLOW_SHARED_ENV, "1")
    binding = SqlitePoolBinding(storage, "pool", path, "redis", "ns")
    await binding.claim()
    assert binding._accepted is False
    assert _rows(storage) == []


async def test_sqlite_binding_forget_deletes_row(sqlite_binding):
    storage, path = sqlite_binding
    binding = SqlitePoolBinding(storage, "pool", path, "redis", "ns")
    await binding.claim()
    await binding.forget()
    assert _rows(storage) == []
    assert binding._rollback_token is None


# -- MySQL bindings --------------------------------------------------------


class _InsertCursor(_RawCursor):
    async def fetchone(self):
        if self._storage.forced_row is not None:
            return self._storage.forced_row
        insert = next(
            (params for sql, params in self._storage.executed if "INSERT" in sql),
            None,
        )
        if insert is not None:
            return (insert[2], insert[3], insert[4])
        return None


class _InsertMysql(_RawMysql):
    def cursor(self) -> _RawCursor:
        return _InsertCursor(self)


async def test_mysql_binding_claim_and_rollback():
    storage = _InsertMysql()
    binding = MySqlPoolBinding(storage, "pool", "db", "redis", "ns")
    await binding.claim()
    assert binding._accepted is True
    assert binding._token is not None

    await binding.claim()  # already accepted: no extra statements
    inserts = [sql for sql, _ in storage.executed if "INSERT" in sql]
    assert len(inserts) == 1

    await binding.rollback()
    deletes = [(sql, params) for sql, params in storage.executed if "DELETE" in sql]
    assert deletes and deletes[-1][1] == ("pool", binding._token)


async def test_mysql_binding_rejects_other_coordination():
    storage = _InsertMysql()
    storage.forced_row = ("memory", "memory", "other-token")
    binding = MySqlPoolBinding(storage, "pool", "db", "redis", "ns")
    with pytest.raises(IsolationConflict):
        await binding.claim()
    assert binding._accepted is False


async def test_mysql_binding_rollback_failure_is_logged(monkeypatch, caplog):
    import logging

    storage = _InsertMysql()
    binding = MySqlPoolBinding(storage, "pool", "db", "redis", "ns")
    await binding.claim()

    async def broken(fn):
        raise RuntimeError("gone")

    monkeypatch.setattr(binding, "_execute", broken)
    with caplog.at_level(logging.WARNING, logger="purser.binding"):
        await binding.rollback()
    assert "failed to roll back binding" in caplog.text


async def test_mysql_binding_forget_deletes_row():
    storage = _InsertMysql()
    binding = MySqlPoolBinding(storage, "pool", "db", "redis", "ns")
    await binding.claim()
    await binding.forget()
    deletes = [(sql, params) for sql, params in storage.executed if "DELETE" in sql]
    assert deletes and deletes[-1][1] == ("pool",)
    assert binding._rollback_token is None


# -- Redis storage marker --------------------------------------------------


class _RedisNamespace:
    def __init__(self, client, prefix: str = "purser") -> None:
        self._client = client
        self.key_prefix = prefix


class _BrokenRedis:
    async def eval(self, *args, **kwargs):
        raise RuntimeError("redis down")

    async def get(self, key):
        return None


class _ScriptedRedis:
    def __init__(self, renewals: list[int], reclaims: list[int]) -> None:
        self.renewals = list(renewals)
        self.reclaims = list(reclaims)
        self.calls: list[str] = []

    async def eval(self, script, *args, **kwargs):
        if "pexpire" in script:
            self.calls.append("renew")
            return self.renewals.pop(0)
        if "pcall" in script:
            self.calls.append("claim")
            return self.reclaims.pop(0)
        self.calls.append("release")
        return 1

    async def get(self, key):
        return None


async def test_redis_storage_binding_claims_and_releases():
    client = fakeredis.aioredis.FakeRedis()
    try:
        binding = RedisStorageBinding(
            _RedisNamespace(client), "pool", "memory", object()
        )
        await binding.claim()
        assert binding._held is True
        assert await client.get(binding._key) is not None

        await binding.claim()  # already held
        await binding.release()
        assert binding._held is False
        assert await client.get(binding._key) is None
        await binding.release()  # not held: nothing to do
    finally:
        await client.aclose()


async def test_redis_storage_binding_rejects_other_storage(tmp_path):
    client = fakeredis.aioredis.FakeRedis()
    try:
        first = RedisStorageBinding(_RedisNamespace(client), "pool", "memory", object())
        await first.claim()

        second = RedisStorageBinding(
            _RedisNamespace(client),
            "pool",
            "sqlite",
            types.SimpleNamespace(_path=str(tmp_path / "other.sqlite")),
        )
        with pytest.raises(IsolationConflict) as exc:
            await second.claim()
        message = str(exc.value)
        assert "already used by another live Purser" in message
        assert "pid" in message
        await first.release()
    finally:
        await client.aclose()


async def test_redis_storage_binding_skips_unknown_storage(caplog):
    import logging

    client = _ScriptedRedis(renewals=[], reclaims=[])
    binding = RedisStorageBinding(_RedisNamespace(client), "pool", "unknown", object())
    with caplog.at_level(logging.WARNING, logger="purser.binding"):
        await binding.claim()
    assert binding._held is False
    assert "binding skipped" in caplog.text
    await binding.release()


async def test_redis_storage_binding_wraps_client_errors():
    binding = RedisStorageBinding(
        _RedisNamespace(_BrokenRedis()), "pool", "memory", object()
    )
    with pytest.raises(BackendUnavailable):
        await binding.claim()


async def test_redis_storage_heartbeat_reclaims_then_gives_up(monkeypatch, caplog):
    import logging

    client = _ScriptedRedis(renewals=[0, 0], reclaims=[1, 0])
    binding = RedisStorageBinding(_RedisNamespace(client), "pool", "memory", object())
    binding._token = "token"
    binding._record = "record"
    binding._storage_id = "memory"

    monkeypatch.setattr(binding_module, "REDIS_BINDING_HEARTBEAT_SECONDS", 0)
    with caplog.at_level(logging.ERROR, logger="purser.binding"):
        await binding._heartbeat()
    assert client.calls == ["renew", "claim", "renew", "claim"]
    assert "lost Redis storage binding" in caplog.text


async def test_redis_storage_heartbeat_swallows_errors(monkeypatch, caplog):
    import logging

    binding = RedisStorageBinding(
        _RedisNamespace(_BrokenRedis()), "pool", "memory", object()
    )
    monkeypatch.setattr(binding_module, "REDIS_BINDING_HEARTBEAT_SECONDS", 0)
    with caplog.at_level(logging.WARNING, logger="purser.binding"):
        await binding._heartbeat()
    assert "heartbeat failed" in caplog.text


async def test_redis_storage_heartbeat_propagates_cancel(monkeypatch):
    binding = RedisStorageBinding(
        _RedisNamespace(_ScriptedRedis(renewals=[], reclaims=[])),
        "pool",
        "memory",
        object(),
    )

    async def cancelled(_delay):
        raise asyncio.CancelledError

    monkeypatch.setattr(binding_module.asyncio, "sleep", cancelled)
    with pytest.raises(asyncio.CancelledError):
        await binding._heartbeat()


async def test_redis_storage_release_tolerates_client_errors(caplog):
    import logging

    binding = RedisStorageBinding(
        _RedisNamespace(_BrokenRedis()), "pool", "memory", object()
    )
    binding._held = True
    binding._token = "token"
    with caplog.at_level(logging.WARNING, logger="purser.binding"):
        await binding.release()
    assert "failed to release Redis storage binding" in caplog.text


async def test_redis_storage_rollback_and_forget_release():
    client = _ScriptedRedis(renewals=[], reclaims=[])
    binding = RedisStorageBinding(_RedisNamespace(client), "pool", "memory", object())
    binding._held = True
    binding._token = "token"
    await binding.rollback()
    assert binding._held is False

    binding._held = True
    await binding.forget()
    assert binding._held is False


# -- composition and factory ----------------------------------------------


class _Recorder(PoolBinding):
    def __init__(self, name: str, events: list[str]) -> None:
        super().__init__("pool", "", "memory", "memory")
        self.name = name
        self.events = events

    async def claim(self) -> None:
        self.events.append(f"{self.name}.claim")

    async def rollback(self) -> None:
        self.events.append(f"{self.name}.rollback")

    async def release(self) -> None:
        self.events.append(f"{self.name}.release")

    async def forget(self) -> None:
        self.events.append(f"{self.name}.forget")


async def test_composite_binding_orders_lifecycle_calls():
    events: list[str] = []
    composite = CompositeBinding(
        [_Recorder("a", events), _Recorder("b", events)]
    )
    await composite.claim()
    await composite.rollback()
    await composite.release()
    await composite.forget()
    assert events == [
        "a.claim",
        "b.claim",
        "b.rollback",
        "a.rollback",
        "a.release",
        "b.release",
        "a.forget",
        "b.forget",
    ]


def test_forward_binding_kinds(tmp_path):
    assert isinstance(
        _forward_binding(
            "pool", "memory", "memory", "memory", types.SimpleNamespace()
        ),
        NullBinding,
    )
    assert isinstance(
        _forward_binding(
            "pool",
            "sqlite",
            "memory",
            "memory",
            types.SimpleNamespace(_path=":memory:"),
        ),
        NullBinding,
    )
    sqlite_binding = _forward_binding(
        "pool",
        "sqlite",
        "memory",
        "memory",
        types.SimpleNamespace(_path=str(tmp_path / "f.sqlite")),
    )
    assert isinstance(sqlite_binding, SqlitePoolBinding)

    mysql = _forward_binding(
        "pool", "mysql", "memory", "memory", types.SimpleNamespace(_url=None)
    )
    assert isinstance(mysql, MySqlPoolBinding)
    assert mysql._storage_label == "the shared MySQL database"

    labelled = _forward_binding(
        "pool",
        "mysql",
        "redis",
        "ns",
        types.SimpleNamespace(_url="mysql://u:p@db.example:3307/app"),
    )
    assert labelled._storage_label == "MySQL database db.example:3307/app"


def test_make_binding_matrix(tmp_path):
    memory = types.SimpleNamespace()
    assert isinstance(make_binding("pool", "memory", "memory", memory, None), NullBinding)

    sqlite = types.SimpleNamespace(_path=str(tmp_path / "f.sqlite"))
    assert isinstance(
        make_binding("pool", "sqlite", "memory", sqlite, None), SqlitePoolBinding
    )
    assert isinstance(
        make_binding("pool", "memory", "redis", memory, types.SimpleNamespace(_client=None)),
        NullBinding,
    )


async def test_make_binding_with_redis_namespace():
    client = fakeredis.aioredis.FakeRedis()
    try:
        coordination = _RedisNamespace(client)
        memory_only = make_binding("pool", "memory", "redis", object(), coordination)
        assert isinstance(memory_only, RedisStorageBinding)

        both = make_binding(
            "pool",
            "sqlite",
            "redis",
            types.SimpleNamespace(_path=":memory:"),
            coordination,
        )
        # In-memory SQLite has no durable identity, so only the marker is built.
        assert isinstance(both, RedisStorageBinding)
        await memory_only.release()
        await both.release()
    finally:
        await client.aclose()
