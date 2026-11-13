"""SQLite storage plane: durable system of record (S10 / F2) without deps.

Resources live in ``resources`` (one row per ``(pool, id)``) plus a
``resource_tags`` join table for indexed tag queries.  Every mutation is a
version-checked ``UPDATE`` (optimistic concurrency), so a lost update against
another writer simply returns ``False`` and the caller retries, mirroring the
redis CAS semantics without Lua.  Leases never live here -- they belong to the
coordination plane; slot accounting stays authoritative in this table.

The stdlib ``sqlite3`` connection is guarded by an ``asyncio.Lock`` and all
blocking work runs through a thread executor, so the event loop never
blocks.  Sensitive payload fields are AES-GCM encrypted at rest via
:class:`SecretCodec`.  ``:memory:`` is available for tests and ephemeral use;
a file path persists across restarts.
"""

from __future__ import annotations

import asyncio
import random
import sqlite3
import time
from collections.abc import AsyncIterator, Callable
from typing import Any, Mapping, TypeVar, cast

from purser.backends.base import (
    ResourcePatch,
    StorageBackend,
    TransitionResult,
)
from purser.backends.serde import (
    STORAGE_COLUMNS,
    decode_health,
    deserialize_row,
    serialize_row,
)
from purser.core.resource import Resource
from purser.core.state import (
    ALL_STATES,
    ALLOCATABLE_STATES,
    Event,
    ResourceState,
    StateContext,
    StateMachine,
)
from purser.crypto import SecretCodec
from purser.errors import VersionConflict

_RT = TypeVar("_RT")


async def _to_thread(func: Callable[..., _RT], *args: Any) -> _RT:
    """Run a blocking callable in a thread (``_to_thread`` is 3.9+)."""

    return await asyncio.get_running_loop().run_in_executor(None, func, *args)


def _now() -> float:
    return time.time()


class SqliteStorage(StorageBackend):
    """File or in-memory SQLite implementation of the storage plane."""

    def __init__(
        self,
        *,
        path: str | None = None,
        codec: SecretCodec | None = None,
        secret_fields: tuple[str, ...] = (),
    ) -> None:
        self._path = path or ":memory:"
        self._codec = codec or SecretCodec()
        self.secret_fields = secret_fields
        self._lock = asyncio.Lock()
        self._conn = sqlite3.connect(self._path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA busy_timeout = 5000")
        if self._path != ":memory:":
            self._conn.execute("PRAGMA journal_mode = WAL")
        self._closed = False
        self._init_schema()

    # -- schema ------------------------------------------------------------

    def _init_schema(self) -> None:
        with self._conn:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS resources (
                    pool TEXT NOT NULL,
                    id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    state TEXT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    max_slots INTEGER NOT NULL DEFAULT 1,
                    used_slots INTEGER NOT NULL DEFAULT 0,
                    payload TEXT NOT NULL DEFAULT '{}',
                    tags TEXT NOT NULL DEFAULT '[]',
                    health TEXT NOT NULL DEFAULT '{}',
                    quota TEXT NOT NULL DEFAULT '',
                    site_failures TEXT NOT NULL DEFAULT '{}',
                    leases TEXT NOT NULL DEFAULT '{}',
                    usage_quota INTEGER,
                    used_count INTEGER NOT NULL DEFAULT 0,
                    expires_at REAL,
                    next_available_at REAL,
                    mutex_group TEXT,
                    schedule_window TEXT NOT NULL DEFAULT '',
                    meta TEXT NOT NULL DEFAULT '{}',
                    updated_at REAL,
                    PRIMARY KEY (pool, id)
                )
                """
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_resources_state ON resources(pool, state)"
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS resource_tags (
                    pool TEXT NOT NULL,
                    rid TEXT NOT NULL,
                    tag TEXT NOT NULL,
                    PRIMARY KEY (pool, rid, tag)
                )
                """
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_resource_tags_tag ON resource_tags(pool, tag)"
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pool_bindings (
                    pool TEXT NOT NULL PRIMARY KEY,
                    storage_kind TEXT NOT NULL,
                    coordination_kind TEXT NOT NULL,
                    coordination_id TEXT NOT NULL,
                    claim_token TEXT NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )

    # -- serialization -----------------------------------------------------

    def _serialize(self, resource: Resource) -> dict[str, Any]:
        return serialize_row(
            resource,
            codec=self._codec,
            secret_fields=self.secret_fields,
            updated_at=_now(),
        )

    def _deserialize(self, row: sqlite3.Row) -> Resource:
        return deserialize_row(
            cast(Mapping[str, Any], row),
            codec=self._codec,
            secret_fields=self.secret_fields,
        )

    def _values(self, resource: Resource) -> tuple[Any, ...]:
        serialized = self._serialize(resource)
        return tuple(serialized[col] for col in STORAGE_COLUMNS)

    def _update_row(
        self, pool: str, rid: str, resource: Resource, *, expected: int
    ) -> bool:
        assignments = ", ".join(f"{col} = ?" for col in STORAGE_COLUMNS)
        sql = (
            f"UPDATE resources SET {assignments}, version = version + 1 "
            "WHERE pool = ? AND id = ? AND version = ?"
        )
        cursor = self._conn.execute(sql, (*self._values(resource), pool, rid, expected))
        return cursor.rowcount == 1

    def _load_sync(self, pool: str, rid: str) -> Resource | None:
        row = self._conn.execute(
            "SELECT * FROM resources WHERE pool = ? AND id = ?", (pool, rid)
        ).fetchone()
        return self._deserialize(row) if row is not None else None

    def _set_tags_sync(self, pool: str, rid: str, tags: frozenset[str]) -> None:
        # Caller owns the surrounding transaction.
        self._conn.execute(
            "DELETE FROM resource_tags WHERE pool = ? AND rid = ?", (pool, rid)
        )
        self._conn.executemany(
            "INSERT OR IGNORE INTO resource_tags (pool, rid, tag) VALUES (?, ?, ?)",
            [(pool, rid, tag) for tag in sorted(tags)],
        )

    def _retry_locked(
        self, fn: Callable[..., _RT], *args: Any, retries: int = 3
    ) -> _RT:
        """Ride out transient ``SQLITE_BUSY`` so a contended write does not
        silently leak a slot or a transaction."""

        for attempt in range(retries):
            try:
                return fn(*args)
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower() or attempt == retries - 1:
                    raise
                time.sleep(0.05 * (attempt + 1))
        raise AssertionError("unreachable")  # pragma: no cover

    # -- contract ----------------------------------------------------------

    async def get(self, pool: str, rid: str) -> Resource | None:
        async with self._lock:
            return await _to_thread(self._load_sync, pool, rid)

    async def put(
        self,
        pool: str,
        resource: Resource,
        *,
        expected_version: int | None = None,
    ) -> Resource:
        async with self._lock:
            return await _to_thread(
                self._retry_locked, self._put_sync, pool, resource, expected_version
            )

    def _put_sync(
        self, pool: str, resource: Resource, expected_version: int | None = None
    ) -> Resource:
        values = self._values(resource)
        if expected_version is None:
            columns = ", ".join(("pool", "id", "version") + STORAGE_COLUMNS)
            placeholders = ", ".join("?" for _ in range(len(STORAGE_COLUMNS) + 3))
            updates = ", ".join(f"{col} = excluded.{col}" for col in STORAGE_COLUMNS)
            with self._conn:
                self._conn.execute(
                    f"""
                    INSERT INTO resources ({columns}) VALUES ({placeholders})
                    ON CONFLICT(pool, id) DO UPDATE SET
                        {updates},
                        version = resources.version + 1
                    """,
                    (pool, resource.id, 1, *values),
                )
                self._set_tags_sync(pool, resource.id, resource.tags)
        elif expected_version == 0:
            columns = ", ".join(("pool", "id", "version") + STORAGE_COLUMNS)
            placeholders = ", ".join("?" for _ in range(len(STORAGE_COLUMNS) + 3))
            with self._conn:
                try:
                    self._conn.execute(
                        f"INSERT INTO resources ({columns}) VALUES ({placeholders})",
                        (pool, resource.id, 1, *values),
                    )
                except sqlite3.IntegrityError as exc:
                    raise VersionConflict(
                        f"resource {resource.id!r} already exists"
                    ) from exc
                self._set_tags_sync(pool, resource.id, resource.tags)
        else:
            with self._conn:
                if not self._update_row(pool, resource.id, resource, expected=expected_version):
                    raise VersionConflict(
                        f"resource {resource.id!r} version changed "
                        f"(expected {expected_version})"
                    )
                self._set_tags_sync(pool, resource.id, resource.tags)
        stored = self._load_sync(pool, resource.id)
        assert stored is not None
        return stored

    async def delete(self, pool: str, rid: str) -> bool:
        async with self._lock:
            return await _to_thread(
                self._retry_locked, self._delete_sync, pool, rid
            )

    def _delete_sync(self, pool: str, rid: str) -> bool:
        with self._conn:
            cursor = self._conn.execute(
                "DELETE FROM resources WHERE pool = ? AND id = ?", (pool, rid)
            )
            self._conn.execute(
                "DELETE FROM resource_tags WHERE pool = ? AND rid = ?", (pool, rid)
            )
        return cursor.rowcount > 0

    async def query(
        self,
        pool: str,
        *,
        states: set[ResourceState] | None = None,
        tags: frozenset[str] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Resource]:
        async with self._lock:
            return await _to_thread(
                self._query_sync, pool, states, tags, limit, offset
            )

    def _query_sync(
        self,
        pool: str,
        states: set[ResourceState] | None,
        tags: frozenset[str] | None,
        limit: int,
        offset: int,
    ) -> list[Resource]:
        clauses = ["pool = ?"]
        params: list[Any] = [pool]
        if states:
            placeholders = ", ".join("?" * len(states))
            clauses.append(f"state IN ({placeholders})")
            params.extend(state.value for state in states)
        if tags:
            placeholders = ", ".join("?" * len(tags))
            clauses.append(
                "(SELECT COUNT(*) FROM resource_tags t "
                "WHERE t.pool = resources.pool AND t.rid = resources.id "
                f"AND t.tag IN ({placeholders})) = ?"
            )
            params.extend(sorted(tags))
            params.append(len(tags))
        sql = "SELECT * FROM resources WHERE " + " AND ".join(clauses)
        sql += " ORDER BY id LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        rows = self._conn.execute(sql, params).fetchall()
        return [self._deserialize(row) for row in rows]

    async def counts(self, pool: str) -> dict[ResourceState, int]:
        async with self._lock:
            return await _to_thread(self._counts_sync, pool)

    def _counts_sync(self, pool: str) -> dict[ResourceState, int]:
        rows = self._conn.execute(
            "SELECT state, COUNT(*) AS n FROM resources WHERE pool = ? GROUP BY state",
            (pool,),
        ).fetchall()
        out = {state: 0 for state in ALL_STATES}
        for row in rows:
            out[ResourceState(row["state"])] = int(row["n"])
        return out

    async def health_scores(self, pool: str) -> list[float]:
        async with self._lock:
            return await _to_thread(self._health_scores_sync, pool)

    def _health_scores_sync(self, pool: str) -> list[float]:
        rows = self._conn.execute(
            "SELECT health FROM resources WHERE pool = ?", (pool,)
        ).fetchall()
        scores: list[float] = []
        for row in rows:
            health = decode_health(row["health"] or "")
            if health.samples:
                scores.append(health.score)
        return scores

    async def transition_acquire(
        self, pool: str, rid: str, slots: int
    ) -> TransitionResult:
        async with self._lock:
            return await _to_thread(
                self._retry_locked, self._transition_acquire_sync, pool, rid, slots
            )

    def _transition_acquire_sync(
        self, pool: str, rid: str, slots: int
    ) -> TransitionResult:
        with self._conn:
            row = self._conn.execute(
                "SELECT * FROM resources WHERE pool = ? AND id = ?", (pool, rid)
            ).fetchone()
            if row is None:
                return TransitionResult(ok=False)
            resource = self._deserialize(row)
            if (
                resource.state not in ALLOCATABLE_STATES
                or not resource.concurrency.can_acquire(slots)
            ):
                return TransitionResult(ok=False)
            expected = resource.version
            used_before = resource.concurrency.used_slots
            target = StateMachine.trigger(
                resource.state,
                Event.ACQUIRE,
                ctx=StateContext(
                    used_slots=used_before,
                    slots=slots,
                    max_slots=resource.concurrency.max_slots,
                ),
            )
            resource.concurrency.acquire(slots)
            now = _now()
            resource.meta["last_used_at"] = now
            resource.meta["updated_at"] = now
            resource.state = target
            if not self._update_row(pool, rid, resource, expected=expected):
                return TransitionResult(ok=False)
        loaded = self._load_sync(pool, rid)
        assert loaded is not None
        return TransitionResult(ok=True, state=loaded.state, resource=loaded)

    async def transition_release(
        self, pool: str, rid: str, slots: int
    ) -> TransitionResult:
        async with self._lock:
            return await _to_thread(
                self._retry_locked, self._transition_release_sync, pool, rid, slots
            )

    def _transition_release_sync(
        self, pool: str, rid: str, slots: int
    ) -> TransitionResult:
        # A concurrent writer in another instance can win the optimistic-lock
        # CAS between our read and write.  Retry with jitter so a release is
        # not reported as a spurious LeaseConflict under contention.
        for attempt in range(10):
            with self._conn:
                row = self._conn.execute(
                    "SELECT * FROM resources WHERE pool = ? AND id = ?", (pool, rid)
                ).fetchone()
                if row is None:
                    return TransitionResult(ok=False)
                resource = self._deserialize(row)
                if resource.concurrency.used_slots <= 0:
                    return TransitionResult(ok=False)
                expected = resource.version
                return_slots = min(slots, resource.concurrency.used_slots)
                resource.concurrency.release(return_slots)
                resource.meta["updated_at"] = _now()
                if resource.state is ResourceState.DRAINING:
                    new_state = (
                        ResourceState.ARCHIVED
                        if resource.concurrency.used_slots <= 0
                        else ResourceState.DRAINING
                    )
                else:
                    new_state = StateMachine.trigger(
                        resource.state,
                        Event.RELEASE,
                        ctx=StateContext(
                            used_slots=resource.concurrency.used_slots + return_slots,
                            slots=return_slots,
                            max_slots=resource.concurrency.max_slots,
                        ),
                    )
                resource.state = new_state
                if self._update_row(pool, rid, resource, expected=expected):
                    loaded = self._load_sync(pool, rid)
                    assert loaded is not None
                    return TransitionResult(
                        ok=True, state=loaded.state, resource=loaded
                    )
            time.sleep(0.005 * (attempt + 1) + random.uniform(0.0, 0.005))
        return TransitionResult(ok=False)

    async def reset_usage(self, pool: str, rid: str, slots: int = 0) -> bool:
        async with self._lock:
            return await _to_thread(
                self._retry_locked, self._reset_usage_sync, pool, rid, slots
            )

    def _reset_usage_sync(self, pool: str, rid: str, slots: int = 0) -> bool:
        with self._conn:
            row = self._conn.execute(
                "SELECT * FROM resources WHERE pool = ? AND id = ?", (pool, rid)
            ).fetchone()
            if row is None:
                return False
            resource = self._deserialize(row)
            if slots < 0:
                return False
            slots = min(slots, resource.concurrency.max_slots)
            if resource.state is ResourceState.DRAINING:
                if slots <= 0:
                    new_state = ResourceState.ARCHIVED
                    resource.meta.setdefault("archived_at", _now())
                    resource.meta.setdefault("archived_reason", "reconcile: lost leases")
                else:
                    new_state = ResourceState.DRAINING
            elif (
                resource.state in set(ALLOCATABLE_STATES) | {ResourceState.FULL}
            ):
                if slots == 0:
                    if resource.concurrency.used_slots == 0:
                        return False
                    new_state = ResourceState.IDLE
                elif resource.concurrency.used_slots == slots:
                    return False
                else:
                    new_state = (
                        ResourceState.FULL
                        if slots >= resource.concurrency.max_slots
                        else ResourceState.ACTIVE
                    )
            else:
                return False
            expected = resource.version
            resource.concurrency.used_slots = slots
            resource.state = new_state
            resource.meta["updated_at"] = _now()
            return self._update_row(pool, rid, resource, expected=expected)

    async def set_state(
        self,
        pool: str,
        rid: str,
        expected_version: int,
        *,
        expect_state: ResourceState | None = None,
        patch: ResourcePatch,
    ) -> bool:
        async with self._lock:
            return await _to_thread(
                self._retry_locked,
                self._set_state_sync,
                pool,
                rid,
                expected_version,
                expect_state,
                patch,
            )

    def _set_state_sync(
        self,
        pool: str,
        rid: str,
        expected_version: int,
        expect_state: ResourceState | None,
        patch: ResourcePatch,
    ) -> bool:
        with self._conn:
            row = self._conn.execute(
                "SELECT * FROM resources WHERE pool = ? AND id = ?", (pool, rid)
            ).fetchone()
            if row is None:
                return False
            resource = self._deserialize(row)
            if resource.version != expected_version:
                return False
            if expect_state is not None and resource.state != expect_state:
                return False
            patch.apply(resource)
            if patch.tags is not None:
                self._set_tags_sync(pool, rid, resource.tags)
            return self._update_row(pool, rid, resource, expected=expected_version)

    async def scan(
        self, pool: str, *, states: set[ResourceState] | None = None, batch: int = 50
    ) -> AsyncIterator[Resource]:
        async with self._lock:
            resources = await _to_thread(self._scan_sync, pool, states)
        for resource in resources:
            yield resource

    def _scan_sync(
        self, pool: str, states: set[ResourceState] | None
    ) -> list[Resource]:
        if states:
            placeholders = ", ".join("?" * len(states))
            sql = (
                f"SELECT * FROM resources WHERE pool = ? AND state IN ({placeholders}) "
                "ORDER BY id"
            )
            rows = self._conn.execute(sql, (pool, *[s.value for s in states])).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM resources WHERE pool = ? ORDER BY id", (pool,)
            ).fetchall()
        return [self._deserialize(row) for row in rows]

    async def flush(self, pool: str) -> None:
        async with self._lock:
            return await _to_thread(self._retry_locked, self._flush_sync, pool)

    def _flush_sync(self, pool: str) -> None:
        with self._conn:
            self._conn.execute("DELETE FROM resources WHERE pool = ?", (pool,))
            self._conn.execute("DELETE FROM resource_tags WHERE pool = ?", (pool,))

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._conn.close()
        except sqlite3.Error:
            pass
