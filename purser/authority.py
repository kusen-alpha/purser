"""Cross-process authority guard for the unsupported memory-based mixes.

Each OS process must own its own ``Purser``, but two processes may still point
at the same backends.  Only ``sqlite``/``mysql`` storage combined with Redis
coordination allows that: both planes are shared and can be reconciled.  The
mixes below half-share the resource set and are rejected when a pool starts:

* ``memory`` storage + Redis coordination - leases live in Redis, the rows
  live in one process.  Another process sees leases for rows it does not have,
  and neither owner can reconcile the other.
* SQLite/MySQL storage + ``memory`` coordination - rows are shared, leases are
  private.  Both processes hand out slots for the same rows and oversell.

``memory`` + ``memory`` has no shared plane at all; those instances are simply
independent resource sets, so there is nothing to detect.

The guard claims authority when :meth:`ResourcePool.start` runs and releases it
on close:

* Redis - a TTL marker key with a heartbeat (crash recovery after the TTL).
* SQLite - an OS advisory lock on a sidecar file next to the database (the OS
  drops it when the process dies).
* MySQL - ``GET_LOCK`` on the storage connection (the server drops it when the
  session ends).

``PURSER_ALLOW_SHARED_BACKENDS=1`` skips the guard for harnesses that simulate
several hosts inside one process.

:mod:`purser.binding` covers the other directions.  A durable store records the
first instance's coordination identity in ``pool_bindings``, so a later start
with another kind or namespace fails.  For Redis coordination each pool also
writes a live ``{prefix}:binding:{pool}:storage`` marker naming its storage, so
a different storage in the same namespace fails while the owner is live; the
marker is released on close and expires after a crash.  Memory-backed storages
(``memory`` and ``sqlite :memory:``) all report the same ``memory`` identity, so
two of them in one Redis namespace are still caught here instead of by the
marker.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import os
import socket
import time
import uuid
from typing import Any

from purser.errors import IsolationConflict
from purser.isolation import (
    ALLOW_SHARED_ENV,
    shared_backends_allowed,
    sqlite_file_key,
    sqlite_identity,
)

logger = logging.getLogger(__name__)

# Long enough that a garbage-collection pause or a slow heartbeat does not let a
# second process in; short enough that a hard crash frees the namespace.
REDIS_AUTHORITY_TTL_MS = 60_000
REDIS_HEARTBEAT_SECONDS = 20.0

_RENEW_LUA = (
    "if redis.call('get', KEYS[1]) == ARGV[1] "
    "then return redis.call('pexpire', KEYS[1], ARGV[2]) else return 0 end"
)
_RELEASE_LUA = (
    "if redis.call('get', KEYS[1]) == ARGV[1] "
    "then return redis.call('del', KEYS[1]) else return 0 end"
)

_LOCK_OFFSET = 1 << 20  # keep byte 0 readable for the owner record


def _owner_record(pool: str, kind: str) -> dict[str, Any]:
    return {
        "token": uuid.uuid4().hex,
        "pid": os.getpid(),
        "host": socket.gethostname(),
        "pool": pool,
        "kind": kind,
        "started_at": time.time(),
    }


def _describe_owner(raw: Any) -> str:
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", "replace")
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        return ""
    if not isinstance(data, dict):
        return ""
    bits: list[str] = []
    if data.get("pid"):
        bits.append(f"pid {data['pid']}")
    if data.get("host"):
        bits.append(f"host {data['host']}")
    since = data.get("started_at")
    if isinstance(since, (int, float)):
        bits.append(time.strftime("since %Y-%m-%d %H:%M:%S", time.localtime(since)))
    return f" ({', '.join(bits)})" if bits else ""


def _same_process(raw: Any) -> bool:
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", "replace")
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        return False
    return (
        isinstance(data, dict)
        and data.get("pid") == os.getpid()
        and data.get("host") == socket.gethostname()
    )


def _hint() -> str:
    return (
        "Use SQLite or MySQL storage with Redis coordination, give each process "
        "its own backend namespace, or run one LocalScheduler authority per "
        f"resource set. Set {ALLOW_SHARED_ENV}=1 only for harnesses that "
        "simulate separate hosts."
    )


def _redis_message(pool: str, detail: str) -> str:
    return (
        f"pool {pool!r} uses in-memory storage plus shared Redis coordination, but "
        f"another live Purser already owns that Redis namespace for this pool{detail}. "
        "Each process would hold a private copy of the resource set while Redis "
        "tracks leases for rows the other process cannot see. " + _hint()
    )


def _sqlite_message(pool: str, path: str, detail: str) -> str:
    return (
        f"pool {pool!r} shares SQLite file {path!r} with another live Purser{detail} "
        "while using process-local (memory) coordination; both processes would hand "
        "out leases for the same rows and oversell the pool. " + _hint()
    )


def _mysql_message(pool: str, database: str, detail: str) -> str:
    return (
        f"pool {pool!r} shares MySQL database {database!r} with another live "
        f"Purser{detail} while using process-local (memory) coordination; both "
        "processes would hand out leases for the same rows and oversell the pool. "
        + _hint()
    )


class PoolAuthority:
    """Cross-process authority for one pool; no-op by default."""

    async def acquire(self) -> None:
        return None

    async def release(self) -> None:
        return None


class NullAuthority(PoolAuthority):
    """Independent planes (memory+memory) or an unsupported backend object."""


class RedisAuthority(PoolAuthority):
    """Exclusive marker in the shared Redis namespace for memory storage."""

    def __init__(self, client: Any, key_prefix: str, pool: str) -> None:
        self._client = client
        self._key = f"{key_prefix}:authority:{{{pool}}}:owner"
        self._record = _owner_record(pool, "memory+redis")
        self._token = json.dumps(self._record, sort_keys=True)
        self._pool = pool
        self._held = False
        self._task: asyncio.Task[None] | None = None

    async def acquire(self) -> None:
        if shared_backends_allowed() or self._held:
            return
        ok = await self._client.set(
            self._key, self._token, nx=True, px=REDIS_AUTHORITY_TTL_MS
        )
        if not ok:
            existing = await self._client.get(self._key)
            if _same_process(existing):
                # A failed release from an earlier run in this process must not
                # lock us out for a whole TTL; in-process duplicates are already
                # caught by the isolation guard.
                await self._client.set(self._key, self._token, px=REDIS_AUTHORITY_TTL_MS)
                logger.warning(
                    "took over stale Redis authority for pool %s left by this process",
                    self._pool,
                )
            else:
                raise IsolationConflict(_redis_message(self._pool, _describe_owner(existing)))
        self._held = True
        self._task = asyncio.create_task(self._heartbeat())

    async def _heartbeat(self) -> None:
        try:
            while True:
                await asyncio.sleep(REDIS_HEARTBEAT_SECONDS)
                renewed = await self._client.eval(
                    _RENEW_LUA, 1, self._key, self._token, REDIS_AUTHORITY_TTL_MS
                )
                if renewed:
                    continue
                reclaimed = await self._client.set(
                    self._key, self._token, nx=True, px=REDIS_AUTHORITY_TTL_MS
                )
                if not reclaimed:
                    logger.error(
                        "lost Redis authority for pool %s; another process owns it",
                        self._pool,
                    )
                    return
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - a background refresh must not die loudly
            logger.warning("Redis authority heartbeat failed for pool %s", self._pool)

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
        except Exception:  # noqa: BLE001 - TTL still frees the namespace
            logger.warning("failed to release Redis authority for pool %s", self._pool)


def _try_lock(fd: int) -> bool:
    if os.name == "nt":
        import msvcrt

        os.lseek(fd, _LOCK_OFFSET, os.SEEK_SET)
        try:
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        except OSError:
            return False
        return True
    import importlib

    fcntl: Any = importlib.import_module("fcntl")
    try:
        fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB, 1, _LOCK_OFFSET, os.SEEK_SET)
    except OSError:
        return False
    return True


def _unlock(fd: int) -> None:
    if os.name == "nt":
        import msvcrt

        os.lseek(fd, _LOCK_OFFSET, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
        return
    import importlib

    fcntl: Any = importlib.import_module("fcntl")
    fcntl.lockf(fd, fcntl.LOCK_UN, 1, _LOCK_OFFSET, os.SEEK_SET)


class SqliteFileAuthority(PoolAuthority):
    """OS advisory lock on a sidecar file: the OS releases it on process death."""

    def __init__(self, db_path: str, pool: str) -> None:
        identity = sqlite_identity(db_path)
        if identity is None:
            raise ValueError("a file-backed SQLite path is required")
        self._path = identity[0][1]
        digest = hashlib.sha1(pool.encode("utf-8")).hexdigest()[:12]
        # Name the sidecar by inode so hardlink aliases of one file share the
        # lock instead of each taking a private one.  Paths that do not exist
        # yet (a database created later) keep the path-based name.
        key = sqlite_file_key(db_path)
        if key is None:
            self._lock_path = f"{self._path}.purser-authority-{digest}.lock"
        else:
            self._lock_path = os.path.join(
                os.path.dirname(self._path),
                f".purser-authority-{key[0]:x}-{key[1]:x}-{digest}.lock",
            )
        self._pool = pool
        self._record = json.dumps(_owner_record(pool, "sqlite+memory"), sort_keys=True)
        self._fd: int | None = None

    async def acquire(self) -> None:
        if shared_backends_allowed() or self._fd is not None:
            return
        fd = os.open(self._lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        if not _try_lock(fd):
            existing = self._read_owner()
            os.close(fd)
            raise IsolationConflict(
                _sqlite_message(self._pool, self._path, _describe_owner(existing))
            )
        try:
            os.lseek(fd, 0, os.SEEK_SET)
            os.ftruncate(fd, 0)
            os.write(fd, self._record.encode("utf-8"))
        except OSError:
            pass
        self._fd = fd

    def _read_owner(self) -> str:
        try:
            with open(self._lock_path, "rb") as fh:
                return fh.read(4096).decode("utf-8", "replace")
        except OSError:
            return ""

    async def release(self) -> None:
        fd, self._fd = self._fd, None
        if fd is None:
            return
        try:
            _unlock(fd)
        except OSError:
            pass
        finally:
            os.close(fd)


class MySqlNamedLockAuthority(PoolAuthority):
    """MySQL ``GET_LOCK`` scoped to the database + pool name of this pool."""

    def __init__(self, storage: Any, pool: str) -> None:
        self._storage = storage
        self._pool = pool
        self._name: str | None = None
        self._database = ""
        self._held = False

    async def _lock_name(self) -> str:
        if self._name is not None:
            return self._name
        storage = self._storage
        async with storage._lock:
            await storage._ensure_conn()
            async with storage._conn.cursor() as cur:
                await cur.execute("SELECT DATABASE()")
                row = await cur.fetchone()
        self._database = (row[0] if row and row[0] else "") or ""
        digest = hashlib.sha1(
            f"{self._database}\x00{self._pool}".encode()
        ).hexdigest()
        self._name = f"purser-auth-{digest}"
        return self._name

    async def acquire(self) -> None:
        if shared_backends_allowed() or self._held:
            return
        name = await self._lock_name()
        storage = self._storage
        async with storage._lock:
            await storage._ensure_conn()
            async with storage._conn.cursor() as cur:
                await cur.execute("SELECT GET_LOCK(%s, 0)", (name,))
                row = await cur.fetchone()
        if not row or row[0] != 1:
            raise IsolationConflict(_mysql_message(self._pool, self._database, ""))
        self._held = True

    async def release(self) -> None:
        if not self._held or self._name is None:
            return
        self._held = False
        storage = self._storage
        try:
            async with storage._lock:
                if storage._conn is None:
                    return
                async with storage._conn.cursor() as cur:
                    await cur.execute("SELECT RELEASE_LOCK(%s)", (self._name,))
                    await cur.fetchone()
        except Exception:  # noqa: BLE001 - session death releases the lock anyway
            logger.warning(
                "failed to release MySQL authority lock for pool %s", self._pool
            )


def make_authority(
    pool: str,
    storage_kind: str,
    coordination_kind: str,
    storage: Any,
    coordination: Any,
) -> PoolAuthority:
    """Pick the guard for the storage/coordination mix of one pool."""

    process_local = storage_kind == "memory" or (
        storage_kind == "sqlite"
        and sqlite_identity(getattr(storage, "_path", None)) is None
    )
    if process_local and coordination_kind == "redis":
        client = getattr(coordination, "_client", None)
        if client is None:
            return NullAuthority()
        return RedisAuthority(
            client, getattr(coordination, "key_prefix", "purser"), pool
        )
    if storage_kind == "sqlite" and coordination_kind == "memory":
        path = getattr(storage, "_path", None)
        if not path or sqlite_identity(path) is None:
            return NullAuthority()
        return SqliteFileAuthority(path, pool)
    if storage_kind == "mysql" and coordination_kind == "memory":
        if storage is None:
            return NullAuthority()
        return MySqlNamedLockAuthority(storage, pool)
    return NullAuthority()
