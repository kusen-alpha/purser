"""MySQL storage plane: durable system of record on aiomysql (S10 / F2).

Implements the same :class:`StorageBackend` contract as memory and sqlite.
The connection is created lazily on first use, so importing ``purser`` never
requires ``aiomysql``; install it with ``pip install purser[mysql]``.  All
state transitions are version-checked ``UPDATE`` statements (optimistic
concurrency), so a lost update simply returns ``False`` and the caller retries.

Schemas use ``utf8mb4``; resource ids longer than 255 characters need a
projection (ids shorter than 255 work as-is) because InnoDB primary keys are
bounded.  Connection-level failures surface as :class:`BackendUnavailable`.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import random
import time
from collections.abc import AsyncIterator, Callable, Coroutine
from typing import Any, TypeVar
from urllib.parse import unquote, urlsplit

from typing_extensions import ParamSpec

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
from purser.errors import BackendUnavailable, ConfigError, PurserError, VersionConflict


def _now() -> float:
    return time.time()


def _parse_url(url: str) -> dict[str, Any]:
    parts = urlsplit(url)
    return {
        "host": parts.hostname,
        "port": parts.port or 3306,
        "user": unquote(parts.username or "root"),
        "password": unquote(parts.password or ""),
        "db": (parts.path or "/").lstrip("/") or None,
    }


_P = ParamSpec("_P")
_R = TypeVar("_R")


def _fail_closed(
    fn: Callable[_P, Coroutine[Any, Any, _R]],
) -> Callable[_P, Coroutine[Any, Any, _R]]:
    @functools.wraps(fn)
    async def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        try:
            return await fn(*args, **kwargs)
        except (BackendUnavailable, PurserError):
            raise
        except Exception as exc:  # aiomysql raises broad OperationalError/OSError
            raise BackendUnavailable(f"mysql error: {exc}") from exc

    return wrapper


def _fail_closed_gen(
    fn: Callable[_P, AsyncIterator[_R]],
) -> Callable[_P, AsyncIterator[_R]]:
    @functools.wraps(fn)
    async def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> AsyncIterator[_R]:
        try:
            async for item in fn(*args, **kwargs):
                yield item
        except (BackendUnavailable, PurserError):
            raise
        except Exception as exc:
            raise BackendUnavailable(f"mysql error: {exc}") from exc

    return wrapper


class MySqlStorage(StorageBackend):
    """MySQL implementation of the storage plane via aiomysql."""

    def __init__(
        self,
        *,
        url: str | None = None,
        connection: Any = None,
        codec: SecretCodec | None = None,
        secret_fields: tuple[str, ...] = (),
    ) -> None:
        if url is None and connection is None:
            raise ConfigError("MySqlStorage requires a url or connection")
        self._url = url
        self._connection = connection
        self._codec = codec or SecretCodec()
        self.secret_fields = secret_fields
        self._lock = asyncio.Lock()
        self._conn: Any = None
        self._owns = connection is None
        self._schema_ready = False

    # -- connection --------------------------------------------------------

    async def _ensure_conn(self) -> None:
        if self._conn is not None:
            return
        import aiomysql

        if self._connection is not None:
            self._conn = self._connection
            self._owns = False
        elif self._url is not None:
            cfg = _parse_url(self._url)
            self._conn = await aiomysql.connect(
                host=cfg["host"],
                port=cfg["port"],
                user=cfg["user"],
                password=cfg["password"],
                db=cfg["db"],
                charset="utf8mb4",
                autocommit=True,
            )
        else:
            raise ConfigError("MySqlStorage requires a url or connection")
        await self._init_schema()

    async def _init_schema(self) -> None:
        if self._schema_ready:
            return
        async with self._conn.cursor() as cur:
            await cur.execute(
                """
                CREATE TABLE IF NOT EXISTS resources (
                    pool VARCHAR(64) NOT NULL,
                    id VARCHAR(255) NOT NULL,
                    kind VARCHAR(64) NOT NULL,
                    state VARCHAR(32) NOT NULL,
                    version BIGINT NOT NULL DEFAULT 1,
                    max_slots INT NOT NULL DEFAULT 1,
                    used_slots INT NOT NULL DEFAULT 0,
                    payload MEDIUMTEXT NOT NULL,
                    tags TEXT NOT NULL,
                    health TEXT NOT NULL,
                    quota TEXT NOT NULL,
                    site_failures TEXT NOT NULL,
                    usage_quota BIGINT NULL,
                    used_count BIGINT NOT NULL DEFAULT 0,
                    expires_at DOUBLE NULL,
                    next_available_at DOUBLE NULL,
                    mutex_group VARCHAR(255) NULL,
                    schedule_window TEXT NOT NULL,
                    meta MEDIUMTEXT NOT NULL,
                    updated_at DOUBLE NULL,
                    PRIMARY KEY (pool, id),
                    KEY idx_resources_state (pool, state)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
                """
            )
            await cur.execute(
                """
                CREATE TABLE IF NOT EXISTS resource_tags (
                    pool VARCHAR(64) NOT NULL,
                    rid VARCHAR(255) NOT NULL,
                    tag VARCHAR(255) NOT NULL,
                    PRIMARY KEY (pool, rid, tag),
                    KEY idx_resource_tags_tag (pool, tag)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
                """
            )
            await cur.execute(
                """
                CREATE TABLE IF NOT EXISTS pool_bindings (
                    pool VARCHAR(64) NOT NULL,
                    storage_kind VARCHAR(32) NOT NULL,
                    coordination_kind VARCHAR(32) NOT NULL,
                    coordination_id VARCHAR(255) NOT NULL,
                    claim_token VARCHAR(64) NOT NULL,
                    updated_at DOUBLE NOT NULL,
                    PRIMARY KEY (pool)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
                """
            )
        self._schema_ready = True

    # -- helpers -----------------------------------------------------------

    @contextlib.asynccontextmanager
    async def _transaction(self) -> AsyncIterator[Any]:
        await self._conn.begin()
        try:
            yield self._conn
            await self._conn.commit()
        except BaseException:
            await self._conn.rollback()
            raise

    def _serialize(self, resource: Resource) -> dict[str, Any]:
        return serialize_row(
            resource,
            codec=self._codec,
            secret_fields=self.secret_fields,
            updated_at=_now(),
        )

    def _deserialize(self, row: dict[str, Any]) -> Resource:
        return deserialize_row(
            row,
            codec=self._codec,
            secret_fields=self.secret_fields,
        )

    def _values(self, resource: Resource) -> tuple[Any, ...]:
        serialized = self._serialize(resource)
        return tuple(serialized[col] for col in STORAGE_COLUMNS)

    async def _load(self, pool: str, rid: str) -> Resource | None:
        async with self._conn.cursor(self._dict_cursor()) as cur:
            await cur.execute(
                "SELECT * FROM resources WHERE pool = %s AND id = %s", (pool, rid)
            )
            row = await cur.fetchone()
        return self._deserialize(row) if row else None

    def _dict_cursor(self) -> Any:
        import aiomysql

        return aiomysql.DictCursor

    async def _set_tags(self, pool: str, rid: str, tags: frozenset[str]) -> None:
        async with self._conn.cursor() as cur:
            await cur.execute(
                "DELETE FROM resource_tags WHERE pool = %s AND rid = %s", (pool, rid)
            )
            await cur.executemany(
                "INSERT IGNORE INTO resource_tags (pool, rid, tag) VALUES (%s, %s, %s)",
                [(pool, rid, tag) for tag in sorted(tags)],
            )

    async def _update_row(
        self, pool: str, rid: str, resource: Resource, *, expected: int
    ) -> bool:
        assignments = ", ".join(f"{col} = %s" for col in STORAGE_COLUMNS)
        sql = (
            f"UPDATE resources SET {assignments}, version = version + 1 "
            "WHERE pool = %s AND id = %s AND version = %s"
        )
        async with self._conn.cursor() as cur:
            await cur.execute(sql, (*self._values(resource), pool, rid, expected))
            return bool(cur.rowcount == 1)

    # -- contract ----------------------------------------------------------

    @_fail_closed
    async def get(self, pool: str, rid: str) -> Resource | None:
        async with self._lock:
            await self._ensure_conn()
            return await self._load(pool, rid)

    @_fail_closed
    async def put(
        self,
        pool: str,
        resource: Resource,
        *,
        expected_version: int | None = None,
    ) -> Resource:
        async with self._lock:
            await self._ensure_conn()
            async with self._transaction():
                if expected_version is None:
                    columns = ", ".join(("pool", "id", "version") + STORAGE_COLUMNS)
                    placeholders = ", ".join("%s" for _ in range(len(STORAGE_COLUMNS) + 3))
                    updates = ", ".join(f"{col} = new.{col}" for col in STORAGE_COLUMNS)
                    async with self._conn.cursor() as cur:
                        await cur.execute(
                            f"""
                            INSERT INTO resources ({columns}) VALUES ({placeholders}) AS new
                            ON DUPLICATE KEY UPDATE {updates}, version = resources.version + 1
                            """,
                            (pool, resource.id, 1, *self._values(resource)),
                        )
                elif expected_version == 0:
                    columns = ", ".join(("pool", "id", "version") + STORAGE_COLUMNS)
                    placeholders = ", ".join("%s" for _ in range(len(STORAGE_COLUMNS) + 3))
                    inserted = False
                    try:
                        async with self._conn.cursor() as cur:
                            await cur.execute(
                                f"INSERT INTO resources ({columns}) VALUES ({placeholders})",
                                (pool, resource.id, 1, *self._values(resource)),
                            )
                        inserted = True
                    except Exception as exc:
                        if "duplicate" not in str(exc).lower():
                            raise
                    if not inserted:
                        raise VersionConflict(
                            f"resource {resource.id!r} already exists"
                        )
                elif not await self._update_row(
                    pool, resource.id, resource, expected=expected_version
                ):
                    raise VersionConflict(
                        f"resource {resource.id!r} version changed "
                        f"(expected {expected_version})"
                    )
                await self._set_tags(pool, resource.id, resource.tags)
            stored = await self._load(pool, resource.id)
            assert stored is not None
            return stored

    @_fail_closed
    async def delete(self, pool: str, rid: str) -> bool:
        async with self._lock:
            await self._ensure_conn()
            async with self._transaction():
                async with self._conn.cursor() as cur:
                    await cur.execute(
                        "DELETE FROM resources WHERE pool = %s AND id = %s", (pool, rid)
                    )
                    deleted = bool(cur.rowcount > 0)
                await self._set_tags(pool, rid, frozenset())
            return deleted

    @_fail_closed
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
            await self._ensure_conn()
            clauses = ["pool = %s"]
            params: list[Any] = [pool]
            if states:
                placeholders = ", ".join("%s" for _ in states)
                clauses.append(f"state IN ({placeholders})")
                params.extend(state.value for state in states)
            if tags:
                placeholders = ", ".join("%s" for _ in tags)
                clauses.append(
                    "(SELECT COUNT(*) FROM resource_tags t "
                    "WHERE t.pool = resources.pool AND t.rid = resources.id "
                    f"AND t.tag IN ({placeholders})) = %s"
                )
                params.extend(sorted(tags))
                params.append(len(tags))
            sql = "SELECT * FROM resources WHERE " + " AND ".join(clauses)
            sql += " ORDER BY id LIMIT %s OFFSET %s"
            params.extend([limit, offset])
            async with self._conn.cursor(self._dict_cursor()) as cur:
                await cur.execute(sql, params)
                rows = await cur.fetchall()
            return [self._deserialize(row) for row in rows]

    @_fail_closed
    async def counts(self, pool: str) -> dict[ResourceState, int]:
        async with self._lock:
            await self._ensure_conn()
            async with self._conn.cursor(self._dict_cursor()) as cur:
                await cur.execute(
                    "SELECT state, COUNT(*) AS n FROM resources WHERE pool = %s GROUP BY state",
                    (pool,),
                )
                rows = await cur.fetchall()
            out = {state: 0 for state in ALL_STATES}
            for row in rows:
                out[ResourceState(row["state"])] = int(row["n"])
            return out

    @_fail_closed
    async def health_scores(self, pool: str) -> list[float]:
        async with self._lock:
            await self._ensure_conn()
            async with self._conn.cursor(self._dict_cursor()) as cur:
                await cur.execute(
                    "SELECT health FROM resources WHERE pool = %s", (pool,)
                )
                rows = await cur.fetchall()
        scores: list[float] = []
        for row in rows:
            health = decode_health(row["health"] or "")
            if health.samples:
                scores.append(health.score)
        return scores

    @_fail_closed
    async def transition_acquire(
        self, pool: str, rid: str, slots: int
    ) -> TransitionResult:
        async with self._lock:
            await self._ensure_conn()
            async with self._transaction():
                resource = await self._load(pool, rid)
                if (
                    resource is None
                    or resource.state not in ALLOCATABLE_STATES
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
                if not await self._update_row(pool, rid, resource, expected=expected):
                    return TransitionResult(ok=False)
            loaded = await self._load(pool, rid)
            assert loaded is not None
            return TransitionResult(ok=True, state=loaded.state, resource=loaded)

    @_fail_closed
    async def transition_release(
        self, pool: str, rid: str, slots: int
    ) -> TransitionResult:
        async with self._lock:
            await self._ensure_conn()
            # Another instance releasing a different lease on the same row can
            # win the version CAS between our read and write.  Retry with
            # jitter so legitimate releases are not reported as LeaseConflict.
            for attempt in range(10):
                async with self._transaction():
                    resource = await self._load(pool, rid)
                    if resource is None or resource.concurrency.used_slots <= 0:
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
                                used_slots=resource.concurrency.used_slots
                                + return_slots,
                                slots=return_slots,
                                max_slots=resource.concurrency.max_slots,
                            ),
                        )
                    resource.state = new_state
                    if await self._update_row(
                        pool, rid, resource, expected=expected
                    ):
                        break
                await asyncio.sleep(
                    0.005 * (attempt + 1) + random.uniform(0.0, 0.005)
                )
            else:
                return TransitionResult(ok=False)
            loaded = await self._load(pool, rid)
            assert loaded is not None
            return TransitionResult(ok=True, state=loaded.state, resource=loaded)

    @_fail_closed
    async def reset_usage(self, pool: str, rid: str, slots: int = 0) -> bool:
        async with self._lock:
            await self._ensure_conn()
            async with self._transaction():
                resource = await self._load(pool, rid)
                if resource is None:
                    return False
                if slots < 0:
                    return False
                slots = min(slots, resource.concurrency.max_slots)
                if resource.state is ResourceState.DRAINING:
                    if slots <= 0:
                        new_state = ResourceState.ARCHIVED
                        resource.meta.setdefault("archived_at", _now())
                        resource.meta.setdefault(
                            "archived_reason", "reconcile: lost leases"
                        )
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
                return await self._update_row(pool, rid, resource, expected=expected)

    @_fail_closed
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
            await self._ensure_conn()
            async with self._transaction():
                resource = await self._load(pool, rid)
                if resource is None:
                    return False
                if resource.version != expected_version:
                    return False
                if expect_state is not None and resource.state != expect_state:
                    return False
                patch.apply(resource)
                if patch.tags is not None:
                    await self._set_tags(pool, rid, resource.tags)
                return await self._update_row(pool, rid, resource, expected=expected_version)

    @_fail_closed_gen
    async def scan(
        self, pool: str, *, states: set[ResourceState] | None = None, batch: int = 50
    ) -> AsyncIterator[Resource]:
        async with self._lock:
            await self._ensure_conn()
            if states:
                placeholders = ", ".join("%s" for _ in states)
                sql = (
                    f"SELECT * FROM resources WHERE pool = %s AND state IN ({placeholders}) "
                    "ORDER BY id"
                )
                params: Any = (pool, *[s.value for s in states])
            else:
                sql = "SELECT * FROM resources WHERE pool = %s ORDER BY id"
                params = (pool,)
            async with self._conn.cursor(self._dict_cursor()) as cur:
                await cur.execute(sql, params)
                rows = await cur.fetchall()
        for row in rows:
            yield self._deserialize(row)

    @_fail_closed
    async def flush(self, pool: str) -> None:
        async with self._lock:
            await self._ensure_conn()
            async with self._transaction():
                async with self._conn.cursor() as cur:
                    await cur.execute(
                        "DELETE FROM resources WHERE pool = %s", (pool,)
                    )
                    await cur.execute(
                        "DELETE FROM resource_tags WHERE pool = %s", (pool,)
                    )

    async def close(self) -> None:
        async with self._lock:
            if self._conn is not None and self._owns:
                try:
                    await self._conn.ensure_closed()
                except Exception:
                    pass
            self._conn = None
            self._schema_ready = False
