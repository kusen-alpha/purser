"""Durable pin from a storage plane to the coordination plane of a pool.

Lease/slot accounting only works when every ``Purser`` that opens one durable
store agrees on where the leases live.  The cross-process authority guard in
:mod:`purser.authority` rejects the memory-based mixes while another owner is
live, but it cannot stop a later instance from silently switching coordination
after the first one closed:

* storage ``sqlite``/``mysql`` + Redis, then the same store + ``memory`` --
  the second instance reconciles from its empty private lease view and zeroes
  slots the first instance's leases still hold;
* the same durable store + one Redis namespace, then another namespace --
  the two instances hold disjoint lease sets for the same rows.

The durable plane is the only thing both instances always see, so each pool
records the coordination identity that first started it in a
``pool_bindings`` row *inside that storage*.  Every later start reads the row
and rejects a different coordination kind or namespace with
:class:`IsolationConflict`.

The forward pin is a deployment invariant, not runtime state: it survives
``close()`` and is removed only when the pool is deleted (``delete_pool``),
which is the explicit rebind path.  A start that fails after claiming a new
pin rolls its own row back, identified by a per-claim token.

The reverse direction - one coordination namespace used by different storages
- cannot be written in the storage plane, because the instances do not share
one.  For Redis coordination each pool also writes a live
``{prefix}:binding:{pool}:storage`` marker that records the storage identity;
a different storage is rejected while the marker is owned, and the marker is
released on ``close()`` (a crashed owner frees it after the TTL).  This also
catches ``memory`` storage against a durable storage in the same Redis pool
namespace.

``memory`` + ``memory`` has no shared plane at all, so it has nothing to pin;
``PURSER_ALLOW_SHARED_BACKENDS=1`` skips both pins for harnesses that simulate
separate hosts inside one process.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import socket
import time
import uuid
from collections.abc import Callable
from typing import Any

from purser.errors import BackendUnavailable, IsolationConflict
from purser.isolation import (
    ALLOW_SHARED_ENV,
    mysql_identity,
    shared_backends_allowed,
    sqlite_file_key,
    sqlite_identity,
)

logger = logging.getLogger(__name__)

REDIS_BINDING_TTL_MS = 60_000
REDIS_BINDING_HEARTBEAT_SECONDS = 20.0

# Claim if the key is free or already belongs to the same storage; a different
# storage identity can never take the namespace.  The value is replaced on every
# same-storage claim so a restarted instance owns the heartbeat.
_CLAIM_LUA = (
    "local cur = redis.call('get', KEYS[1]) "
    "if not cur then "
    "redis.call('set', KEYS[1], ARGV[1], 'PX', ARGV[2]) return 1 end "
    "local ok, data = pcall(cjson.decode, cur) "
    "if ok and type(data) == 'table' and data['storage_id'] == ARGV[3] then "
    "redis.call('set', KEYS[1], ARGV[1], 'PX', ARGV[2]) return 1 end "
    "return 0"
)
_RENEW_LUA = (
    "if redis.call('get', KEYS[1]) == ARGV[1] "
    "then return redis.call('pexpire', KEYS[1], ARGV[2]) else return 0 end"
)
_RELEASE_LUA = (
    "if redis.call('get', KEYS[1]) == ARGV[1] "
    "then return redis.call('del', KEYS[1]) else return 0 end"
)


def _is_fake_client(client: Any) -> bool:
    # fakeredis invents a random host per client instance; two instances model
    # the same logical server in restart tests, so the host is not an identity.
    return type(client).__module__.split(".", 1)[0] == "fakeredis"


def redis_coordination_id(coordination: Any) -> str | None:
    """Stable ``host:port/db#key_prefix`` identity of a Redis coordination.

    Two clients are the same namespace only when all four components match.
    Unknown clients (no connection pool metadata) return ``None`` so the caller
    can decline to pin instead of inventing an identity.
    """

    client = getattr(coordination, "_client", None)
    if client is None:
        return None
    prefix = str(getattr(coordination, "key_prefix", "purser"))
    pool = getattr(client, "connection_pool", None)
    kwargs = dict(getattr(pool, "connection_kwargs", None) or {})
    path = kwargs.get("path")
    if _is_fake_client(client):
        where = "fake"
    elif path:
        where = f"unix:{path}"
    else:
        host = kwargs.get("host") or kwargs.get("hostname")
        if not host:
            return None
        where = f"{host}:{kwargs.get('port') or 6379}"
    db = kwargs.get("db")
    db = 0 if db in (None, "") else db
    return f"redis:{where}/{db}#{prefix}"


def _describe(kind: str, identity: str) -> str:
    if kind == "memory":
        return "process-local (memory) coordination"
    return f"{kind} coordination {identity!r}"


def _message(
    pool: str,
    storage_label: str,
    bound_kind: str,
    bound_id: str,
    want_kind: str,
    want_id: str,
) -> str:
    return (
        f"pool {pool!r} on {storage_label} is already bound to "
        f"{_describe(bound_kind, bound_id)}, but this Purser was configured with "
        f"{_describe(want_kind, want_id)}. Every process that opens the same "
        "durable storage must use the same coordination plane and namespace; "
        "mixing them splits leases from rows so both owners can hand out the "
        "same slot. Start with the bound coordination, or delete the pool "
        "(delete_pool(..., force=True)) to rebind it. Set "
        f"{ALLOW_SHARED_ENV}=1 only for harnesses that simulate separate hosts."
    )


def _load_record(raw: Any) -> dict[str, Any]:
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", "replace")
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def _describe_remote(raw: Any) -> str:
    data = _load_record(raw)
    storage = str(data.get("storage_id") or "an unknown storage")
    bits: list[str] = []
    if data.get("pid"):
        bits.append(f"pid {data['pid']}")
    if data.get("host"):
        bits.append(f"host {data['host']}")
    return f"{storage} ({', '.join(bits)})" if bits else storage


def _redis_storage_message(pool: str, namespace: str, existing: Any) -> str:
    return (
        f"pool {pool!r} in Redis namespace {namespace!r} is already used by "
        f"another live Purser on storage {_describe_remote(existing)}. Two "
        "different resource sets must not share one Redis pool namespace: "
        "startup reconcile in one instance counts the other instance's leases "
        "and clears its slots. Use a different pool name or a different Redis "
        "DB, or stop the other instance. Set "
        f"{ALLOW_SHARED_ENV}=1 only for harnesses that simulate separate hosts."
    )


async def _mysql_storage_identity(storage: Any) -> str | None:
    try:
        async with storage._lock:
            await storage._ensure_conn()
            try:
                async with storage._conn.cursor() as cur:
                    await cur.execute("SELECT @@server_uuid, DATABASE()")
                    row = await cur.fetchone()
                if row and row[0]:
                    return f"mysql:{row[0]}/{row[1] or ''}"
            except Exception:  # noqa: BLE001 - MariaDB lacks @@server_uuid
                pass
            async with storage._conn.cursor() as cur:
                await cur.execute("SELECT @@hostname, DATABASE()")
                row = await cur.fetchone()
            if row and row[0]:
                return f"mysql:{row[0]}/{row[1] or ''}"
    except Exception:  # noqa: BLE001 - fall back to the configured URL
        logger.warning("could not identify the MySQL storage for the Redis binding")
    identity = mysql_identity(getattr(storage, "_url", None))
    if identity is None:
        return None
    return "mysql:" + "/".join(str(part) for part in identity[0][1:])


async def _storage_identity(storage_kind: str, storage: Any) -> str | None:
    if storage_kind == "memory":
        return "memory"
    if storage_kind == "sqlite":
        path = getattr(storage, "_path", None)
        # :memory: databases are process-local, exactly like memory storage.
        identity = sqlite_identity(path)
        if identity is None:
            return "memory"
        # Prefer the inode so hardlink aliases of one file report one identity;
        # a path that does not exist yet falls back to its canonical path.
        key = sqlite_file_key(path)
        if key is not None:
            return f"sqlite:d{key[0]:x}-i{key[1]:x}"
        return f"sqlite:{identity[0][1]}"
    if storage_kind == "mysql":
        return await _mysql_storage_identity(storage)
    return None


class PoolBinding:
    """Pin for one pool: durable coordination binding plus live Redis marker."""

    def __init__(
        self,
        pool: str,
        storage_label: str,
        coordination_kind: str,
        coordination_id: str,
    ) -> None:
        self._pool = pool
        self._storage_label = storage_label
        self._coordination_kind = coordination_kind
        self._coordination_id = coordination_id
        self._token: str | None = None
        self._rollback_token: str | None = None
        self._accepted = False

    async def claim(self) -> None:
        return None

    async def rollback(self) -> None:
        return None

    async def release(self) -> None:
        """Release runtime state (the live Redis marker); durable pins stay."""

        return None

    async def forget(self) -> None:
        return None

    def _accept(self, bound_kind: Any, bound_id: Any, bound_token: Any) -> None:
        kind = str(bound_kind)
        identity = str(bound_id)
        if kind != self._coordination_kind or identity != self._coordination_id:
            raise IsolationConflict(
                _message(
                    self._pool,
                    self._storage_label,
                    kind,
                    identity,
                    self._coordination_kind,
                    self._coordination_id,
                )
            )
        self._accepted = True
        self._rollback_token = (
            self._token if self._token is not None
            and str(bound_token) == self._token
            else None
        )

    def _new_token(self) -> str:
        self._token = uuid.uuid4().hex
        return self._token

    def _take_rollback_token(self) -> str | None:
        token, self._rollback_token = self._rollback_token, None
        self._accepted = False
        return token


class NullBinding(PoolBinding):
    """Memory storage or an identity we cannot establish."""


class SqlitePoolBinding(PoolBinding):
    """``pool_bindings`` row inside the SQLite file of the pool."""

    _INSERT = (
        "INSERT OR IGNORE INTO pool_bindings "
        "(pool, storage_kind, coordination_kind, coordination_id, "
        "claim_token, updated_at) VALUES (?, ?, ?, ?, ?, ?)"
    )
    _SELECT = (
        "SELECT coordination_kind, coordination_id, claim_token "
        "FROM pool_bindings WHERE pool = ?"
    )
    _DELETE = "DELETE FROM pool_bindings WHERE pool = ?"

    def __init__(
        self,
        storage: Any,
        pool: str,
        path: str,
        coordination_kind: str,
        coordination_id: str,
    ) -> None:
        super().__init__(pool, f"SQLite file {path!r}", coordination_kind, coordination_id)
        self._storage = storage

    async def claim(self) -> None:
        if shared_backends_allowed() or self._accepted:
            return
        token = self._new_token()
        storage = self._storage
        async with storage._lock:
            row = await asyncio.get_running_loop().run_in_executor(
                None, storage._retry_locked, self._claim_sync, token
            )
        if row is None:  # pragma: no cover - the insert above guarantees it
            raise IsolationConflict(
                f"failed to record the coordination binding for pool {self._pool!r}"
            )
        self._accept(*row)

    def _claim_sync(self, token: str) -> Any:
        conn = self._storage._conn
        with conn:
            conn.execute(
                self._INSERT,
                (
                    self._pool,
                    "sqlite",
                    self._coordination_kind,
                    self._coordination_id,
                    token,
                    time.time(),
                ),
            )
        return conn.execute(self._SELECT, (self._pool,)).fetchone()

    async def rollback(self) -> None:
        token = self._take_rollback_token()
        if token is None:
            return
        storage = self._storage
        try:
            async with storage._lock:
                await asyncio.get_running_loop().run_in_executor(
                    None,
                    storage._retry_locked,
                    self._delete_sync,
                    token,
                )
        except Exception:  # noqa: BLE001 - the start is already failing
            logger.warning("failed to roll back binding for pool %s", self._pool)

    def _delete_sync(self, token: str) -> None:
        conn = self._storage._conn
        with conn:
            conn.execute(
                f"{self._DELETE} AND claim_token = ?", (self._pool, token)
            )

    async def forget(self) -> None:
        self._take_rollback_token()
        storage = self._storage
        async with storage._lock:
            await asyncio.get_running_loop().run_in_executor(
                None, storage._retry_locked, self._forget_sync
            )

    def _forget_sync(self) -> None:
        conn = self._storage._conn
        with conn:
            conn.execute(self._DELETE, (self._pool,))


class MySqlPoolBinding(PoolBinding):
    """``pool_bindings`` row inside the MySQL database of the pool."""

    _INSERT = (
        "INSERT IGNORE INTO pool_bindings "
        "(pool, storage_kind, coordination_kind, coordination_id, "
        "claim_token, updated_at) VALUES (%s, %s, %s, %s, %s, %s)"
    )
    _SELECT = (
        "SELECT coordination_kind, coordination_id, claim_token "
        "FROM pool_bindings WHERE pool = %s"
    )
    _DELETE = "DELETE FROM pool_bindings WHERE pool = %s"

    def __init__(
        self,
        storage: Any,
        pool: str,
        label: str,
        coordination_kind: str,
        coordination_id: str,
    ) -> None:
        super().__init__(pool, label, coordination_kind, coordination_id)
        self._storage = storage

    async def _execute(self, fn: Callable[[Any], Any]) -> Any:
        storage = self._storage
        async with storage._lock:
            await storage._ensure_conn()
            async with storage._conn.cursor() as cur:
                return await fn(cur)

    async def claim(self) -> None:
        if shared_backends_allowed() or self._accepted:
            return
        token = self._new_token()

        async def work(cur: Any) -> Any:
            await cur.execute(
                self._INSERT,
                (
                    self._pool,
                    "mysql",
                    self._coordination_kind,
                    self._coordination_id,
                    token,
                    time.time(),
                ),
            )
            await cur.execute(self._SELECT, (self._pool,))
            return await cur.fetchone()

        row = await self._execute(work)
        if row is None:  # pragma: no cover - the insert above guarantees it
            raise IsolationConflict(
                f"failed to record the coordination binding for pool {self._pool!r}"
            )
        self._accept(*row)

    async def rollback(self) -> None:
        token = self._take_rollback_token()
        if token is None:
            return

        async def work(cur: Any) -> None:
            await cur.execute(
                f"{self._DELETE} AND claim_token = %s", (self._pool, token)
            )

        try:
            await self._execute(work)
        except Exception:  # noqa: BLE001 - the start is already failing
            logger.warning("failed to roll back binding for pool %s", self._pool)

    async def forget(self) -> None:
        self._take_rollback_token()

        async def work(cur: Any) -> None:
            await cur.execute(self._DELETE, (self._pool,))

        await self._execute(work)


class RedisStorageBinding(PoolBinding):
    """Live marker: one Redis pool namespace belongs to one storage identity.

    Unlike the durable pin, this marker is released on ``close()``: two
    different storages may reuse a Redis namespace once the previous owner is
    gone.  The TTL frees it after a crash.
    """

    def __init__(
        self,
        coordination: Any,
        pool: str,
        storage_kind: str,
        storage: Any,
    ) -> None:
        namespace = redis_coordination_id(coordination) or "redis"
        super().__init__(pool, f"Redis namespace {namespace!r}", "redis", namespace)
        self._client: Any = getattr(coordination, "_client", None)
        prefix = str(getattr(coordination, "key_prefix", "purser"))
        self._key = f"{prefix}:binding:{{{pool}}}:storage"
        self._namespace = namespace
        self._storage_kind = storage_kind
        self._storage = storage
        self._storage_id: str | None = None
        self._record = ""
        self._held = False
        self._task: asyncio.Task[None] | None = None

    async def claim(self) -> None:
        if shared_backends_allowed() or self._held:
            return
        identity = await _storage_identity(self._storage_kind, self._storage)
        if identity is None:
            logger.warning(
                "cannot identify the storage of pool %s; Redis storage binding skipped",
                self._pool,
            )
            return
        self._storage_id = identity
        self._record = json.dumps(
            {
                "storage_kind": self._storage_kind,
                "storage_id": identity,
                "token": uuid.uuid4().hex,
                "pid": os.getpid(),
                "host": socket.gethostname(),
                "started_at": time.time(),
            },
            sort_keys=True,
        )
        # Renew/release compare the whole record, exactly like RedisAuthority.
        self._token = self._record
        try:
            accepted = await self._client.eval(
                _CLAIM_LUA,
                1,
                self._key,
                self._record,
                REDIS_BINDING_TTL_MS,
                identity,
            )
        except Exception as exc:  # noqa: BLE001 - mirror RedisCoordination
            raise BackendUnavailable(f"redis error: {exc}") from exc
        if not accepted:
            existing = await self._client.get(self._key)
            raise IsolationConflict(
                _redis_storage_message(self._pool, self._namespace, existing)
            )
        self._held = True
        self._task = asyncio.create_task(self._heartbeat())

    async def _heartbeat(self) -> None:
        try:
            while True:
                await asyncio.sleep(REDIS_BINDING_HEARTBEAT_SECONDS)
                renewed = await self._client.eval(
                    _RENEW_LUA,
                    1,
                    self._key,
                    self._token,
                    REDIS_BINDING_TTL_MS,
                )
                if renewed:
                    continue
                reclaimed = await self._client.eval(
                    _CLAIM_LUA,
                    1,
                    self._key,
                    self._record,
                    REDIS_BINDING_TTL_MS,
                    self._storage_id,
                )
                if not reclaimed:
                    logger.error(
                        "lost Redis storage binding for pool %s; another storage "
                        "owns the namespace",
                        self._pool,
                    )
                    return
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - a background refresh must not die loudly
            logger.warning(
                "Redis storage binding heartbeat failed for pool %s", self._pool
            )

    async def release(self) -> None:
        if not self._held:
            return
        self._held = False
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        try:
            await self._client.eval(_RELEASE_LUA, 1, self._key, self._token)
        except Exception:  # noqa: BLE001 - the TTL still frees the namespace
            logger.warning(
                "failed to release Redis storage binding for pool %s", self._pool
            )

    async def rollback(self) -> None:
        await self.release()

    async def forget(self) -> None:
        await self.release()


class CompositeBinding(PoolBinding):
    """Forward durable pin plus the reverse Redis storage marker."""

    def __init__(self, parts: list[PoolBinding]) -> None:
        self._parts = parts

    async def claim(self) -> None:
        for part in self._parts:
            await part.claim()

    async def rollback(self) -> None:
        for part in reversed(self._parts):
            await part.rollback()

    async def release(self) -> None:
        for part in self._parts:
            await part.release()

    async def forget(self) -> None:
        for part in self._parts:
            await part.forget()


def _forward_binding(
    pool: str,
    storage_kind: str,
    coordination_kind: str,
    coordination_id: str,
    storage: Any,
) -> PoolBinding:
    if storage_kind == "sqlite":
        identity = sqlite_identity(getattr(storage, "_path", None))
        if identity is None:
            return NullBinding(pool, "", coordination_kind, coordination_id)
        return SqlitePoolBinding(
            storage, pool, identity[0][1], coordination_kind, coordination_id
        )
    if storage_kind == "mysql":
        identity = mysql_identity(getattr(storage, "_url", None))
        label = identity[1] if identity is not None else "the shared MySQL database"
        return MySqlPoolBinding(
            storage, pool, label, coordination_kind, coordination_id
        )
    return NullBinding(pool, "", coordination_kind, coordination_id)


def make_binding(
    pool: str,
    storage_kind: str,
    coordination_kind: str,
    storage: Any,
    coordination: Any,
) -> PoolBinding:
    """Build the pins for a pool: storage -> coordination and the reverse."""

    if coordination_kind == "memory":
        coordination_id: str | None = "memory"
    elif coordination_kind == "redis":
        coordination_id = redis_coordination_id(coordination)
    else:  # pragma: no cover - config validation rejects unknown kinds
        coordination_id = None

    forward = (
        NullBinding(pool, "", coordination_kind, "")
        if coordination_id is None
        else _forward_binding(
            pool, storage_kind, coordination_kind, coordination_id, storage
        )
    )
    reverse: PoolBinding | None = None
    if coordination_kind == "redis" and coordination_id is not None:
        reverse = RedisStorageBinding(coordination, pool, storage_kind, storage)
    if reverse is None:
        return forward
    if isinstance(forward, NullBinding):
        return reverse
    return CompositeBinding([forward, reverse])
