"""Process-wide guard against partially shared scheduling planes.

Two ``Purser`` instances inside one process must be fully independent: they may
not share a SQLite file, a MySQL database, a Redis DB, an external client
object, or a ``ResourceType`` instance.  Half-sharing (same storage but separate
coordination, or the opposite) produces lease/slot views that no single owner
can reconcile, so the library rejects it at the point the shared plane is
wired up.

Threads that need one resource set should share a single ``Purser`` or
``SyncPurser``.  Independent resource sets use different files/databases.
Separate OS processes are unaffected: the registry is process-local and is
cleared in a forked child.

A single instance may not be shared across processes either.  It is not
picklable (``__getstate__`` raises :class:`IsolationConflict`), and an instance
inherited through ``fork`` is stamped with the PID that created it, so the
first call in the child raises instead of silently using dead threads, a dead
event loop or duplicated connections.  Cross-process sharing of one resource
set goes through ``LocalScheduler``.

``PURSER_ALLOW_SHARED_BACKENDS=1`` disables the backend identity guard for
harnesses that deliberately simulate multiple hosts (separate processes)
inside one process.  It does not allow sharing one instance across processes.
"""

from __future__ import annotations

import os
import stat
import threading
import weakref
from typing import Any
from urllib.parse import parse_qs, urlsplit

from purser.errors import IsolationConflict

ALLOW_SHARED_ENV = "PURSER_ALLOW_SHARED_BACKENDS"

_TRUTHY = frozenset({"1", "true", "yes", "on"})

Identity = tuple[Any, str]


class _Claim:
    __slots__ = ("label", "owners")

    def __init__(self, label: str) -> None:
        self.label = label
        # owner token -> number of live claims (one Purser may own several pools)
        self.owners: dict[int, int] = {}


_lock = threading.RLock()
_claims: dict[tuple[Any, ...], _Claim] = {}
_owner_refs: dict[int, weakref.ref[Any]] = {}


def shared_backends_allowed() -> bool:
    return os.environ.get(ALLOW_SHARED_ENV, "").strip().lower() in _TRUTHY


PICKLE_MESSAGE = (
    "Purser objects cannot be pickled or sent to another process; each process "
    "must create its own instance. Use LocalScheduler to share one resource "
    "set across processes."
)


def _conflict_message(label: str) -> str:
    return (
        f"this Purser shares {label} with another live Purser in the same process; "
        "same-process instances must not share storage, coordination or "
        "ResourceType state. Share one Purser/SyncPurser across threads, give "
        "each instance its own backend, or run the extra instances in another "
        "process (LocalScheduler). Set "
        f"{ALLOW_SHARED_ENV}=1 only for harnesses that simulate separate hosts."
    )


def _inherited_message(owner_pid: int, current_pid: int) -> str:
    return (
        f"this Purser was created in process {owner_pid} but is being used in "
        f"process {current_pid}; a Purser instance (and its event loop, threads, "
        "connections and leases) must not cross a process boundary. Create one "
        "instance per process, or use LocalScheduler so a single scheduler "
        "process owns the resource set."
    )


def _reset_after_fork() -> None:
    # Only the forking thread survives, so the inherited registry is stale and
    # its lock may have been held by a thread that no longer exists.
    global _lock
    _lock = threading.RLock()
    _claims.clear()
    _owner_refs.clear()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_after_fork)


def _prune_dead_owners() -> None:
    dead = [token for token, ref in _owner_refs.items() if ref() is None]
    if not dead:
        return
    for token in dead:
        _owner_refs.pop(token, None)
    for key in list(_claims):
        claim = _claims[key]
        for token in dead:
            claim.owners.pop(token, None)
        if not claim.owners:
            del _claims[key]


class IsolationGuard:
    """Claims the scheduling-plane identities one ``Purser`` owns."""

    __slots__ = ("_owner_token", "_owner_ref", "_keys", "_pid")

    def __init__(self, owner: Any) -> None:
        self._owner_token = id(owner)
        self._owner_ref = weakref.ref(owner)
        self._keys: dict[tuple[Any, ...], int] = {}
        self._pid = os.getpid()

    def ensure_process(self) -> None:
        """Reject use of an instance inherited through ``fork``."""

        current = os.getpid()
        if current != self._pid:
            raise IsolationConflict(_inherited_message(self._pid, current))

    def claim_many(self, entries: list[Identity | None]) -> None:
        """Claim every identity atomically, raising before any claim on conflict."""

        if shared_backends_allowed():
            return
        prepared: dict[tuple[Any, ...], str] = {}
        for entry in entries:
            if entry is None:
                continue
            key, label = entry
            prepared.setdefault(key, label)
        if not prepared:
            return
        with _lock:
            _prune_dead_owners()
            for key in prepared:
                claim = _claims.get(key)
                if claim is None:
                    continue
                if any(token != self._owner_token for token in claim.owners):
                    raise IsolationConflict(_conflict_message(claim.label))
            for key, label in prepared.items():
                claim = _claims.get(key)
                if claim is None:
                    claim = _claims[key] = _Claim(label)
                claim.owners[self._owner_token] = (
                    claim.owners.get(self._owner_token, 0) + 1
                )
                self._keys[key] = self._keys.get(key, 0) + 1
                _owner_refs.setdefault(self._owner_token, self._owner_ref)

    def release_many(self, keys: list[tuple[Any, ...]]) -> None:
        if not keys:
            return
        with _lock:
            for key in keys:
                held = self._keys.get(key, 0)
                if held <= 0:
                    continue
                if held == 1:
                    del self._keys[key]
                else:
                    self._keys[key] = held - 1
                claim = _claims.get(key)
                if claim is None:
                    continue
                left = claim.owners.get(self._owner_token, 0) - 1
                if left > 0:
                    claim.owners[self._owner_token] = left
                else:
                    claim.owners.pop(self._owner_token, None)
                if not claim.owners:
                    del _claims[key]

    def release_all(self) -> None:
        with _lock:
            keys = list(self._keys)
        self.release_many(keys)


# -- identities ------------------------------------------------------------


def _split(url: str) -> Any | None:
    try:
        return urlsplit(url)
    except ValueError:
        return None


def sqlite_identity(path: str | None) -> Identity | None:
    """Identity of a SQLite file; in-memory databases are private per pool."""

    if not path or path == ":memory:":
        return None
    raw = path
    query = ""
    if raw.startswith("file:"):
        raw = raw[5:]
        if "?" in raw:
            raw, query = raw.split("?", 1)
        if "mode=memory" in query or not raw or raw.startswith(":memory:"):
            return None
    elif raw.startswith(":memory:"):
        return None
    try:
        full = os.path.realpath(os.path.abspath(raw))
    except OSError:
        full = os.path.abspath(raw)
    full = os.path.normcase(full)
    return ("sqlite", full), f"SQLite file {full!r}"


def sqlite_file_key(path: str | None) -> tuple[int, int] | None:
    """``(st_dev, st_ino)`` of an existing SQLite file: alias-proof identity.

    Hardlinks and other path aliases that reach the same inode share a key even
    when their canonical paths differ.  Returns ``None`` for in-memory databases
    and for paths that do not resolve to a regular file yet.
    """

    identity = sqlite_identity(path)
    if identity is None:
        return None
    try:
        st = os.stat(identity[0][1])
    except OSError:
        return None
    if not stat.S_ISREG(st.st_mode) or st.st_ino == 0:
        return None
    return int(st.st_dev), int(st.st_ino)


def mysql_identity(url: str | None) -> Identity | None:
    if not url:
        return None
    parts = _split(url)
    if parts is None or not parts.hostname:
        return ("mysql-url",), "a MySQL database"
    host = parts.hostname.lower()
    try:
        port = parts.port or 3306
    except ValueError:
        port = 3306
    database = (parts.path or "/").lstrip("/") or ""
    shown = f"{host}:{port}/{database}" if database else f"{host}:{port}"
    return ("mysql", host, port, database), f"MySQL database {shown}"


def redis_identity(url: str | None) -> Identity | None:
    if not url:
        return None
    parts = _split(url)
    if parts is None:
        return ("redis-url",), "a Redis database"
    query = parse_qs(parts.query)
    if parts.scheme == "unix":
        db = query.get("db", ["0"])[0]
        socket = parts.path or parts.netloc
        return ("redis", "unix", socket, db), f"Redis DB {db} on socket {socket!r}"
    if not parts.hostname:
        return ("redis-url",), "a Redis database"
    host = parts.hostname.lower()
    try:
        port = parts.port or 6379
    except ValueError:
        port = 6379
    db = (parts.path or "").lstrip("/") or query.get("db", ["0"])[0]
    return (
        ("redis", host, port, db),
        f"Redis DB {db} at {host}:{port}",
    )


def redis_client_identity(client: Any) -> Identity:
    return ("redis-client", id(client)), "a shared redis_client object"


def mysql_connection_identity(connection: Any) -> Identity:
    return ("mysql-connection", id(connection)), "a shared mysql_connection object"


def resource_type_identity(rtype: Any) -> Identity:
    kind = getattr(rtype, "kind", type(rtype).__name__)
    return ("resource-type", id(rtype)), f"ResourceType {kind!r} instance"
