"""Public facade: :class:`Purser` and :class:`ResourcePool`.

This is the orchestration layer.  It owns the acquisition pipeline
(filter -> score -> request-level check -> atomic commit), the fair wait
queue, lease heartbeat/reaping, validation scheduling, quarantine escalation,
periodic GC and all lifecycle operations.  The state machine in
``core.state`` remains the single source of truth for transitions; the
storage plane provides the atomic read-check-write primitives and the
coordination plane tracks in-flight leases.
"""

from __future__ import annotations

import asyncio
import contextlib
import copy
import heapq
import inspect
import logging
import math
import random
import time
import uuid
from collections import defaultdict
from collections.abc import (
    AsyncGenerator,
    AsyncIterator,
    Callable,
    Iterable,
    Iterator,
    Sequence,
)
from dataclasses import dataclass, field, replace
from typing import Any, Literal, cast, overload

from purser.adapters.base import (
    ResourceAdapter,
    ResourceAdapterRegistry,
    default_adapter_factories,
)
from purser.authority import make_authority
from purser.backends.base import Coordination, ResourcePatch, StorageBackend
from purser.backends.coordination import MemoryCoordination, RedisCoordination
from purser.backends.memory import MemoryStorage
from purser.backends.sqlite import SqliteStorage
from purser.binding import make_binding
from purser.config import MAINTENANCE_NAMES, PoolConfig, Weights, apply_adapter_defaults
from purser.core.allocator import AllocationContext, Allocator, make_allocator
from purser.core.audit import AuditLog
from purser.core.bundle import (
    BundleFairness,
    BundleLease,
    BundleRequest,
    BundleSlot,
    bundle_request_from_dict,
)
from purser.core.bundle_store import (
    BundleStore,
    MemoryBundleStore,
    RedisBundleStore,
    ReservationMember,
)
from purser.core.events import EventBus
from purser.core.health import HealthReport, ProbeGuard, ValidationGate
from purser.core.lease import LeaseManager
from purser.core.quarantine import QuarantinePolicy
from purser.core.resource import (
    AcquireRequest,
    CheckVerdict,
    Concurrency,
    HealthStats,
    Lease,
    Quota,
    Resource,
    SiteFailure,
    UsageOutcome,
    describe_resource,
)
from purser.core.scheduler import PeriodicScheduler
from purser.core.state import (
    ADMIN_ALLOWED,
    ALLOCATABLE_STATES,
    Event,
    ResourceState,
    StateContext,
    StateMachine,
)
from purser.core.usage import UsageLedger
from purser.core.views import (
    ActiveUsageGroup,
    PoolStats,
    PoolUsage,
    UsageReport,
    ValidateJobCancelView,
    ValidateJobView,
)
from purser.crypto import SecretCodec, load_or_create_key
from purser.errors import (
    BadState,
    BundleAcquireFailed,
    BundleQuotaExceeded,
    ConfigError,
    IsolationConflict,
    LeaseConflict,
    NoSuitableResource,
    NotStarted,
    PoolEmpty,
    PoolExhausted,
    PoolExists,
    PoolNotFound,
    ResourceBusy,
    ResourceNotFound,
    VersionConflict,
)
from purser.idempotency import IdempotencyStore, RedisIdempotencyStore
from purser.isolation import (
    PICKLE_MESSAGE,
    IsolationGuard,
    mysql_connection_identity,
    mysql_identity,
    redis_client_identity,
    redis_identity,
    resource_adapter_identity,
    sqlite_identity,
)
from purser.metrics import Metrics

logger = logging.getLogger("purser")

# Shared coordination has no push channel; general waiters poll storage on this
# interval so a release on another instance can wake them (V03).
WAITER_POLL_INTERVAL = 0.1

BACKUP_FORMAT = "purser-backup"
BACKUP_VERSION = 1

# Operational states cannot survive a restore: leases are not migrated, so
# anything that implies occupancy comes back as IDLE and reconciles normally.
_RESTORE_IDLE_STATES = frozenset(
    {
        ResourceState.NEW,
        ResourceState.VALIDATING,
        ResourceState.ACTIVE,
        ResourceState.FULL,
        ResourceState.DRAINING,
        ResourceState.LOST,
    }
)
_RESTORE_TRANSIENT_META = ("drain_reason", "drained_at", "drain_cancelled_at")


def _now() -> float:
    return time.time()


_MISSING = object()


def _record_value(record: dict[str, Any] | Resource, key: str) -> Any:
    """Look up a dedup key on an import record (dict spec or Resource)."""

    if isinstance(record, dict):
        return record[key] if key in record else _MISSING
    if key == "id":
        return record.id
    if key in record.payload:
        return record.payload[key]
    return record.meta.get(key, _MISSING)


# Scheduling fields accepted next to resource payload keys on the import path.
_IMPORT_RESERVED = (
    "max_slots",
    "usage_quota",
    "quota_limit",
    "expires_at",
    "next_available_at",
    "mutex_group",
    "schedule_window",
    "source",
)


def _optional_int(value: Any, name: str, *, minimum: int = 0) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ConfigError(f"{name} must be an integer >= {minimum} or null")
    return value


def _optional_float(value: Any, name: str = "value") -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigError(f"{name} must be a number or null")
    return float(value)


def _schedule_window(value: Any) -> tuple[str, str] | None:
    if value is None:
        return None
    if not isinstance(value, (list, tuple)) or len(value) != 2:
        raise ConfigError("schedule_window must be [start, end] in HH:MM")
    parts = []
    for raw in value:
        try:
            parsed = time.strptime(str(raw), "%H:%M")
        except ValueError:
            raise ConfigError(f"bad schedule_window time {raw!r}; expected HH:MM") from None
        parts.append(time.strftime("%H:%M", parsed))
    return (parts[0], parts[1])


def _apply_import_fields(resource: Resource, data: dict[str, Any]) -> None:
    """Apply scheduling fields carried alongside an import spec."""

    if "max_slots" in data:
        value = data["max_slots"]
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ConfigError("max_slots must be an integer > 0")
        resource.concurrency = Concurrency(max_slots=value)
    if "usage_quota" in data:
        resource.usage_quota = _optional_int(data["usage_quota"], "usage_quota")
    if "quota_limit" in data:
        raw = data["quota_limit"]
        if raw is None:
            resource.quota = None
        else:
            limit = _optional_int(raw, "quota_limit", minimum=1)
            if limit is not None:
                resource.quota = Quota(limit=limit)
    if "expires_at" in data:
        raw = data["expires_at"]
        if raw is not None and (isinstance(raw, bool) or not isinstance(raw, (int, float))):
            raise ConfigError("expires_at must be an epoch number or null")
        resource.expires_at = float(raw) if raw is not None else None
    if "next_available_at" in data:
        raw = data["next_available_at"]
        if raw is not None and (isinstance(raw, bool) or not isinstance(raw, (int, float))):
            raise ConfigError("next_available_at must be an epoch number or null")
        resource.next_available_at = float(raw) if raw is not None else None
    if "mutex_group" in data:
        raw = data["mutex_group"]
        resource.mutex_group = str(raw) if raw else None
    if "schedule_window" in data:
        resource.schedule_window = _schedule_window(data["schedule_window"])
    source = data.get("source")
    if source:
        resource.meta["source"] = str(source)


@dataclass
class ImportReport:
    imported: int = 0
    updated: int = 0
    skipped: int = 0
    errors: list[dict[str, Any]] = field(default_factory=list)
    batch_id: str | None = None
    validation: list[dict[str, Any]] = field(default_factory=list)

    @property
    def total(self) -> int:
        return self.imported + self.updated + self.skipped + len(self.errors)

    def to_dict(self) -> dict[str, Any]:
        return {
            "imported": self.imported,
            "updated": self.updated,
            "skipped": self.skipped,
            "errors": self.errors,
            "batch_id": self.batch_id,
            "validation": self.validation,
            "total": self.total,
        }


@dataclass
class _WaitHandle:
    seq: int
    priority: int
    future: asyncio.Future[Any]
    req: AcquireRequest
    cancelled: bool = False
    serving: bool = False

    def __lt__(self, other: _WaitHandle) -> bool:
        if self.priority != other.priority:
            return self.priority > other.priority  # higher priority first
        return self.seq < other.seq

    def cancel_pending(self) -> None:
        """Mark the waiter as gone and cancel its future so the fair queue
        sweeps it instead of handing a lease to a dead waiter."""

        if not self.future.done():
            self.future.cancel()
            self.cancelled = True


@dataclass
class BatchLease:
    """Result of ``acquire_many``: a list of leases released together (O13)."""

    leases: list[Lease] = field(default_factory=list)

    def __len__(self) -> int:
        return len(self.leases)

    def __iter__(self) -> Iterator[Lease]:
        return iter(self.leases)

    async def release_all(self, pool: ResourcePool, outcome: UsageOutcome = UsageOutcome.OK) -> None:
        for lease in self.leases:
            await pool.release(lease, outcome=outcome)
        self.leases.clear()


@dataclass
class _FairWaiter:
    """One bundle fairness admission waiter (priority heap entry payload)."""

    seq: int
    priority: int
    event: Any = field(default=None, repr=False, compare=False)
    cancelled: bool = False


class ResourcePool:
    """One named pool: a resource adapter, a storage plane and coordination."""

    def __init__(
        self,
        name: str,
        purser: Purser,
        adapter: ResourceAdapter,
        cfg: PoolConfig,
        storage: StorageBackend,
        coordination: Coordination,
    ) -> None:
        self.name = name
        self.purser = purser
        self.adapter = adapter
        self.cfg = cfg
        self.storage = storage
        self._coordination = coordination
        self._authority = make_authority(
            name, cfg.storage, cfg.coordination, storage, coordination
        )
        self._binding = make_binding(
            name, cfg.storage, cfg.coordination, storage, coordination
        )
        self._isolation_keys: list[tuple[Any, ...]] = []
        self._allocator: Allocator = make_allocator(cfg.allocator)
        self._leases = LeaseManager(
            on_expire=self._on_lease_expired,
            on_lost=self._on_lease_lost,
            heartbeat_interval=cfg.heartbeat_interval or max(1.0, cfg.lease_ttl / 3.0),
        )
        self._validate_gate = ValidationGate(
            stale_after=cfg.stale_after,
            background_interval=cfg.background_interval,
            sample=cfg.background_validate_sample,
        )
        self._validate_sem = asyncio.Semaphore(cfg.validate_concurrency)
        self._waiters: list[_WaitHandle] = []
        self._active_polls = 0
        self._polls_idle = asyncio.Event()
        self._polls_idle.set()
        self._seq = 0
        self._consumer_holds: dict[str, set[str]] = defaultdict(set)
        self._affinity: dict[str, dict[str, float]] = defaultdict(dict)
        self._check_cache: dict[tuple[str, str, str], tuple[CheckVerdict, float]] = {}
        self._rr_cursor = 0
        self._scheduler = PeriodicScheduler()
        self._tasks: list[asyncio.Task[Any]] = []
        self._bg_tasks: set[asyncio.Task[Any]] = set()
        self._validate_jobs: dict[str, dict[str, Any]] = {}
        self._validate_job_queues: dict[str, asyncio.Queue[str]] = {}
        self._start_task: asyncio.Task[None] | None = None
        self._started = False
        self._register_scheduler()

    def __getstate__(self) -> Any:
        raise IsolationConflict(PICKLE_MESSAGE)

    def _ensure_process(self) -> None:
        self.purser._isolation.ensure_process()

    # -- lifecycle ---------------------------------------------------------

    def _register_scheduler(self) -> None:
        intervals = self.cfg.scheduler_intervals or {}
        defaults = {
            "quota_reset": 300.0,
            "periodic_unban": 60.0,
            "lost_gc": 300.0,
            "validating_gc": 30.0,
            "quarantine_gc": 15.0,
            "archive_gc": 3600.0,
            "expire_gc": 3600.0,
        }
        for name in MAINTENANCE_NAMES:
            interval = float(intervals.get(name, defaults[name]))
            self._scheduler.add(
                name, interval, self._make_maintenance(name), enabled=self.purser.maintenance_enabled
            )

    def _make_maintenance(self, name: str) -> Callable[[], Any]:
        async def runner() -> None:
            await self.run_maintenance(name)

        return runner

    async def start(self) -> None:
        self._ensure_process()
        if self._started:
            return
        try:
            await self._binding.claim()
        except BaseException:
            await self._binding.rollback()
            raise
        try:
            await self._authority.acquire()
            self._started = True
            if self.purser.maintenance_enabled:
                await self._scheduler.start()
            self._tasks.append(asyncio.create_task(self._reaper_loop()))
            if self.purser.maintenance_enabled:
                self._tasks.append(asyncio.create_task(self._background_validate_loop()))
            await self._reconcile_leases()
        except BaseException:
            self._started = False
            for task in self._tasks:
                task.cancel()
            self._tasks.clear()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._scheduler.stop()
            await self._authority.release()
            await self._binding.rollback()
            raise

    @property
    def started(self) -> bool:
        """Whether the pool runtime (reaper, validation, wake-ups) is running."""

        return self._started

    async def _ensure_started(self, operation: str) -> None:
        self._ensure_process()
        task = self._start_task
        if task is not None and not task.done():
            # A pool created after Purser.start() is starting on a pending task;
            # wait for its reconcile to finish instead of racing acquisitions.
            # Shield it: cancelling one waiter (timeout/consumer cancellation)
            # must not cancel the start task shared by every other waiter.
            await asyncio.shield(task)
            return
        if self._started:
            return
        if self.purser.started:
            await self.start()
            return
        raise NotStarted(self.name, operation=operation)

    async def close(self) -> None:
        self._ensure_process()
        self._started = False
        start_task, self._start_task = self._start_task, None
        if start_task is not None and not start_task.done():
            start_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await start_task
        await self._scheduler.stop()
        try:
            # Shutdown must never wedge on a stuck heartbeat task.
            await asyncio.wait_for(self._leases.close(), timeout=10.0)
        except asyncio.TimeoutError:
            logger.warning("lease manager close timed out for pool %s", self.name)
        pending = list(self._tasks) + list(self._bg_tasks)
        for task in pending:
            task.cancel()
        if pending:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*pending, return_exceptions=True),
                    timeout=10.0,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "pool %s close timed out draining %d tasks", self.name, len(pending)
                )
                for task in pending:
                    task.cancel()
        self._tasks.clear()
        self._bg_tasks.clear()
        # Fail in-flight waiters promptly so callers do not sleep until their
        # own timeout after the pool is closed.
        while self._waiters:
            handle = heapq.heappop(self._waiters)
            if not handle.future.done():
                handle.future.set_exception(
                    PoolExhausted(f"pool {self.name} closed while waiting")
                )
        self._waiters.clear()
        # A waiter may be mid-poll on shared storage. Let that in-flight check
        # land before teardown: closing a sqlite connection under a scan thread
        # crashes the interpreter.
        if self._active_polls:
            try:
                await asyncio.wait_for(self._polls_idle.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning(
                    "pool %s close timed out draining waiter polls", self.name
                )
        try:
            closer = getattr(self.storage, "close", None)
            if closer is not None:
                result = closer()
                if inspect.isawaitable(result):
                    await result
            await self._coordination.close()
        finally:
            await self._authority.release()
            await self._binding.release()
            self.purser._release_pool_claims(self)

    async def _reconcile_leases(self) -> None:
        """F2: align slot usage with the currently live coordination leases.

        Losing transient leases on an abnormal restart is acceptable; resource
        master data stays untouched.  Orphaned slots, whether fully orphaned or
        in excess of the surviving leases, are re-aligned to the live total so
        a partial orphan cannot keep a resource busy forever.
        """
        states = set(ALLOCATABLE_STATES) | {ResourceState.FULL, ResourceState.DRAINING}
        async for resource in self.storage.scan(self.name, states=states):
            now = _now()
            live = [
                lease
                for lease in await self._coordination.leases_for(self.name, resource.id)
                if not lease.expired(now)
            ]
            target = sum(lease.slots for lease in live)
            if target == resource.concurrency.used_slots and (
                resource.state is not ResourceState.DRAINING or target > 0
            ):
                continue
            if await self.storage.reset_usage(self.name, resource.id, slots=target):
                self.purser.bus.publish(
                    "lease_reconciled",
                    self.name,
                    rid=resource.id,
                    data={"reason": "orphaned leases reset", "used_slots": target},
                )

    def _spawn(self, coro: Any) -> asyncio.Task[Any]:
        """Track a fire-and-forget task so :meth:`close` can drain it."""

        task = asyncio.create_task(coro)
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)
        return task

    def reconfigure(self, **updates: Any) -> PoolConfig:
        """Live-update pool knobs (O16): rebuilds the allocator, validation
        gate, heartbeat cadence and scheduler intervals from the new config.
        """

        immutable = {"storage", "coordination"} & set(updates)
        if immutable:
            fields = ", ".join(sorted(immutable))
            raise ConfigError(f"cannot change {fields} at runtime")
        self._ensure_process()
        cfg = self.cfg.with_overrides(**updates)
        self.cfg = cfg
        self._allocator = make_allocator(cfg.allocator)
        self._validate_gate = ValidationGate(
            stale_after=cfg.stale_after,
            background_interval=cfg.background_interval,
            sample=cfg.background_validate_sample,
        )
        self._validate_sem = asyncio.Semaphore(cfg.validate_concurrency)
        self._leases.heartbeat_interval = cfg.heartbeat_interval or max(
            1.0, cfg.lease_ttl / 3.0
        )
        self._register_scheduler()
        return cfg

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    # -- maintenance loops -------------------------------------------------

    async def _reaper_loop(self) -> None:
        interval = max(1.0, min(self.cfg.lease_ttl / 2.0, 5.0))
        while True:
            await asyncio.sleep(interval)
            try:
                await self._leases.reap()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("reaper loop failed for pool %s", self.name)

    async def _background_validate_loop(self) -> None:
        while True:
            await asyncio.sleep(max(1.0, self.cfg.background_interval))
            try:
                await self._background_validate_stale()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("background validation failed for pool %s", self.name)

    async def _background_validate_stale(self, budget: int = 32) -> int:
        """Y2: sample stale IDLE resources and refresh them proactively."""

        checked = 0
        async for resource in self.storage.scan(self.name, states={ResourceState.IDLE}):
            if checked >= budget:
                break
            if not self._validate_gate.due_background(resource):
                continue
            checked += 1
            await self._validate_in_background(resource.id)
        return checked

    async def _validate_in_background(self, rid: str) -> None:
        try:
            await self.validate_now(rid)
        except Exception:
            logger.exception("background validate %s failed", rid)

    # -- batch validate jobs (V2) -----------------------------------------

    def _start_validate_job(
        self, job_id: str, rids: Sequence[str], operator: str
    ) -> ValidateJobView:
        """Track a batch validation run under ``job_id`` (the import batch id).

        Workers pull from a shared queue so a cancellation stops pending
        items while in-flight probes finish normally.
        """

        now = _now()
        existing = self._validate_jobs.get(job_id)
        queue = self._validate_job_queues.get(job_id)
        if existing is not None and queue is not None and existing["status"] in (
            "running",
            "cancelling",
        ):
            for rid in rids:
                queue.put_nowait(rid)
            existing["total"] += len(rids)
            existing["updated_at"] = now
            return self._validate_job_view(existing)
        self._prune_validate_jobs()
        job = {
            "id": job_id,
            "pool": self.name,
            "status": "running",
            "total": len(rids),
            "done": 0,
            "alive": 0,
            "failed": 0,
            "skipped": 0,
            "errors": [],
            "created_at": now,
            "updated_at": now,
            "operator": operator,
            "cancelled": False,
        }
        self._validate_jobs[job_id] = job
        queue = asyncio.Queue()
        self._validate_job_queues[job_id] = queue
        for rid in rids:
            queue.put_nowait(rid)
        workers = max(1, min(self.cfg.validate_concurrency, len(rids)))
        for _ in range(workers):
            self._spawn(self._validate_job_worker(job_id))
        self.purser.bus.publish(
            "validate_job_started",
            self.name,
            data={"job_id": job_id, "total": job["total"]},
        )
        return self._validate_job_view(job)

    async def _validate_job_worker(self, job_id: str) -> None:
        queue = self._validate_job_queues.get(job_id)
        job = self._validate_jobs.get(job_id)
        if queue is None or job is None:
            return
        while True:
            try:
                rid = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            if job["cancelled"]:
                job["skipped"] += 1
                job["done"] += 1
                job["updated_at"] = _now()
                self._finish_validate_job(job_id)
                continue
            try:
                report = await self.validate_now(rid, operator=job["operator"])
                if report.alive:
                    job["alive"] += 1
                else:
                    job["failed"] += 1
            except Exception as exc:
                job["failed"] += 1
                job["errors"].append({"rid": rid, "error": str(exc)})
                if len(job["errors"]) > 100:
                    del job["errors"][0]
                logger.exception("batch validate %s failed for %s", job_id, rid)
            finally:
                job["done"] += 1
                job["updated_at"] = _now()
                self._finish_validate_job(job_id)

    def _finish_validate_job(self, job_id: str) -> None:
        job = self._validate_jobs.get(job_id)
        if job is None or job["done"] < job["total"]:
            return
        if job["status"] == "running" or job["status"] == "cancelling":
            job["status"] = "cancelled" if job["cancelled"] else "completed"
            job["updated_at"] = _now()
            self.purser.bus.publish(
                "validate_job_finished",
                self.name,
                data={
                    "job_id": job_id,
                    "status": job["status"],
                    "alive": job["alive"],
                    "failed": job["failed"],
                    "skipped": job["skipped"],
                },
            )

    def _validate_job_view(self, job: dict[str, Any]) -> ValidateJobView:
        return {
            "id": job["id"],
            "pool": job["pool"],
            "status": job["status"],
            "total": job["total"],
            "done": job["done"],
            "alive": job["alive"],
            "failed": job["failed"],
            "skipped": job["skipped"],
            "errors": list(job["errors"]),
            "created_at": job["created_at"],
            "updated_at": job["updated_at"],
            "operator": job["operator"],
            "pending": max(0, job["total"] - job["done"]),
        }

    def validate_jobs(self) -> list[ValidateJobView]:
        return [
            self._validate_job_view(job)
            for job in sorted(
                self._validate_jobs.values(),
                key=lambda item: item["created_at"],
                reverse=True,
            )
        ]

    def validate_job(self, job_id: str) -> ValidateJobView | None:
        job = self._validate_jobs.get(job_id)
        return None if job is None else self._validate_job_view(job)

    def cancel_validate_job(self, job_id: str) -> ValidateJobCancelView | None:
        job = self._validate_jobs.get(job_id)
        if job is None:
            return None
        if job["status"] not in ("running", "cancelling"):
            view = self._validate_job_view(job)
            return {**view, "cancelled": False}
        job["cancelled"] = True
        job["status"] = "cancelling" if job["done"] < job["total"] else "cancelled"
        job["updated_at"] = _now()
        queue = self._validate_job_queues.get(job_id)
        if queue is not None:
            while not queue.empty():
                queue.get_nowait()
                job["skipped"] += 1
                job["done"] += 1
        self._finish_validate_job(job_id)
        return {**self._validate_job_view(job), "cancelled": True}

    def _prune_validate_jobs(self, keep: int = 100) -> None:
        terminal = [
            job_id
            for job_id, job in self._validate_jobs.items()
            if job["status"] in ("completed", "cancelled")
        ]
        for job_id in terminal[:-keep]:
            self._validate_jobs.pop(job_id, None)
            self._validate_job_queues.pop(job_id, None)

    # -- reads -------------------------------------------------------------

    async def get(self, rid: str) -> Resource | None:
        resource = await self.storage.get(self.name, rid)
        return None if resource is None else self.adapter.hydrate(resource)

    async def require(self, rid: str) -> Resource:
        resource = await self.storage.get(self.name, rid)
        if resource is None:
            raise ResourceNotFound(f"{self.name}:{rid}")
        return self.adapter.hydrate(resource)

    async def query(
        self,
        *,
        states: set[ResourceState] | None = None,
        tags: frozenset[str] | None = None,
        limit: int = 100,
        offset: int = 0,
        search: str | None = None,
        sort: str | None = None,
        order: str | None = None,
    ) -> list[Resource]:
        if limit < 0 or offset < 0:
            raise ConfigError("limit and offset must be >= 0")
        sort_key = sort or "id"
        if sort_key not in ("id", "health", "recent"):
            raise ConfigError(f"unsupported sort {sort_key!r}")
        direction = (order or ("asc" if sort_key == "id" else "desc")).lower()
        if direction not in ("asc", "desc"):
            raise ConfigError(f"unsupported order {order!r}")
        if search or sort:
            matched = await self._search_resources(states=states, tags=tags, search=search)
            if sort:
                # _search_resources already ordered by id, and Python sorts are
                # stable, so equal scores keep an id tiebreak in either direction.
                if sort_key == "health":
                    matched.sort(
                        key=lambda r: r.health.score, reverse=direction == "desc"
                    )
                elif sort_key == "recent":
                    matched.sort(
                        key=lambda r: r.last_used_at or 0.0, reverse=direction == "desc"
                    )
                elif direction == "desc":
                    matched.reverse()
            if limit == 0:
                return []
            return [self.adapter.hydrate(r) for r in matched[offset : offset + limit]]
        resources = await self.storage.query(
            self.name, states=states, tags=tags, limit=limit, offset=offset
        )
        return [self.adapter.hydrate(resource) for resource in resources]

    async def iter_resources(
        self,
        *,
        states: set[ResourceState] | None = None,
        tags: Iterable[str] | None = None,
        search: str | None = None,
        batch_size: int = 100,
    ) -> AsyncGenerator[Resource, None]:
        """Stream matching resources without building a full result list.

        Ordering and filters match ``query()``; there is no offset window, so
        writes during iteration cannot shift the caller's position.
        """

        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size < 1:
            raise ConfigError("batch_size must be an integer >= 1")
        tag_filter: frozenset[str] | None = None
        if tags is not None:
            if isinstance(tags, str):
                raise TypeError("tags must be an iterable of strings, not a str")
            tag_filter = frozenset(tags)
            if any(not isinstance(tag, str) or not tag for tag in tag_filter):
                raise ValueError("tags must contain non-empty strings")
        needle = (search or "").casefold()
        async for resource in self.storage.scan(
            self.name, states=states, batch=batch_size
        ):
            if tag_filter and not tag_filter.issubset(resource.tags):
                continue
            if needle and not self._matches(resource, needle):
                continue
            yield self.adapter.hydrate(resource)

    async def count_matching(
        self,
        *,
        states: set[ResourceState] | None = None,
        tags: frozenset[str] | None = None,
        search: str | None = None,
    ) -> int:
        """Number of resources matching the filters, ignoring pagination."""

        if not search and not tags and not states:
            return sum((await self.counts()).values())
        return len(await self._search_resources(states=states, tags=tags, search=search))

    async def _search_resources(
        self,
        *,
        states: set[ResourceState] | None = None,
        tags: frozenset[str] | None = None,
        search: str | None = None,
    ) -> list[Resource]:
        needle = (search or "").casefold()
        matched: list[Resource] = []
        async for resource in self.storage.scan(self.name, states=states):
            if tags and not tags.issubset(resource.tags):
                continue
            if needle and not self._matches(resource, needle):
                continue
            matched.append(resource)
        matched.sort(key=lambda r: r.id)
        return matched

    @staticmethod
    def _matches(resource: Resource, needle: str) -> bool:
        fields = [resource.id, resource.kind, *resource.tags]
        for key in ("batch_id", "source"):
            value = resource.meta.get(key)
            if value is not None:
                fields.append(str(value))
        return needle in " ".join(fields).casefold()

    async def batches(self) -> list[dict[str, Any]]:
        """Aggregate resources by their import/generation batch."""

        grouped: dict[str, dict[str, Any]] = {}
        async for resource in self.storage.scan(self.name):
            batch_id = str(resource.meta.get("batch_id") or "").strip()
            if not batch_id:
                continue
            item = grouped.setdefault(
                batch_id,
                {
                    "batch_id": batch_id,
                    "total": 0,
                    "states": {},
                    "source": resource.meta.get("source"),
                    "created_at": None,
                },
            )
            item["total"] += 1
            states = item["states"]
            states[resource.state.value] = states.get(resource.state.value, 0) + 1
            created = resource.meta.get("created_at")
            if isinstance(created, (int, float)) and (
                item["created_at"] is None or created < item["created_at"]
            ):
                item["created_at"] = created
        return sorted(grouped.values(), key=lambda item: item["batch_id"])

    @overload
    async def counts(
        self, *, by_name: Literal[False] = False
    ) -> dict[ResourceState, int]: ...

    @overload
    async def counts(self, *, by_name: Literal[True]) -> dict[str, int]: ...

    async def counts(self, *, by_name: bool = False) -> dict[Any, int]:
        """State distribution.

        The default returns enum keys; ``by_name=True`` returns the same data
        with ``ResourceState.value`` string keys, matching ``stats()``.
        """

        counts = await self.storage.counts(self.name)
        if by_name:
            return {state.value: n for state, n in counts.items()}
        return counts

    async def stats(self) -> PoolStats:
        counts = await self.counts(by_name=True)
        health_scores = await self.storage.health_scores(self.name)
        return {
            "name": self.name,
            "kind": self.cfg.kind,
            "total": sum(counts.values()),
            "counts": counts,
            "allocatable": counts[ResourceState.IDLE.value]
            + counts[ResourceState.ACTIVE.value],
            "active_leases": len(await self._coordination.active(self.name)),
            "waiting": len(self._waiters),
            "avg_health": round(sum(health_scores) / len(health_scores), 3) if health_scores else None,
            "config": self.cfg.to_dict(),
        }

    def describe(self, resource: Resource, *, secret: bool = False) -> dict[str, Any]:
        return describe_resource(resource, secret=secret)

    async def leases(self, resource_id: str | None = None) -> list[Lease]:
        """Active leases from the coordination plane.

        ``resource_id=None`` returns every active lease in the pool.  Leases
        issued by other instances sharing the same coordination backend are
        included; their ``resource`` snapshot is ``None``.
        """

        if resource_id is None:
            return await self._coordination.active(self.name)
        return await self._coordination.leases_for(self.name, resource_id)

    # -- import / generate ------------------------------------------------

    async def import_resources(
        self,
        records: Sequence[dict[str, Any] | Resource],
        *,
        dedup_key: str = "id",
        source: str = "manual",
        operator: str = "system",
        fail_fast: bool = False,
        import_validate: str | None = None,
        batch_id: str | None = None,
    ) -> ImportReport:
        """Bulk import dict specs or pre-built Resources with dedup (O4)."""

        report = ImportReport(batch_id=batch_id or uuid.uuid4().hex)
        mode = import_validate or self.cfg.import_validate
        pending_validate: list[str] = []
        for index, record in enumerate(records):
            try:
                explicit_slots = False
                if isinstance(record, Resource):
                    resource = self.adapter.adopt(copy.deepcopy(record))
                elif isinstance(record, dict):
                    spec = dict(record)
                    reserved = {
                        key: spec.pop(key) for key in _IMPORT_RESERVED if key in spec
                    }
                    resource = await self.adapter.generate(spec)
                    _apply_import_fields(resource, reserved)
                    explicit_slots = "max_slots" in reserved
                else:
                    raise TypeError("import record must be a dict or Resource")
            except Exception as exc:
                self._guard_error("import.generate", exc)
                report.errors.append({"line": index, "error": str(exc)})
                if fail_fast:
                    break
                continue
            value = _record_value(record, dedup_key) if dedup_key else _MISSING
            if value is not _MISSING:
                resource.id = str(value)
            resource.meta.setdefault("source", source)
            resource.meta["batch_id"] = report.batch_id
            if not explicit_slots and self.cfg.max_slots:
                resource.concurrency.max_slots = self.cfg.max_slots
            if resource.expires_at is None and self.cfg.expires_in:
                resource.expires_at = _now() + self.cfg.expires_in
            if self.cfg.usage_quota is not None and resource.usage_quota is None:
                resource.usage_quota = self.cfg.usage_quota
            if self.cfg.quota_limit is not None and resource.quota is None:
                resource.quota = Quota(limit=int(self.cfg.quota_limit))
            stored = False
            for attempt in range(3):
                existing = await self.storage.get(self.name, resource.id)
                if existing is not None and existing.state is ResourceState.ARCHIVED:
                    report.skipped += 1
                    break
                if existing is not None:
                    resource.state = existing.state
                    resource.concurrency = existing.concurrency
                    resource.health = existing.health
                    resource.quota = existing.quota
                    resource.site_failures = existing.site_failures
                    resource.used_count = existing.used_count
                    resource.next_available_at = existing.next_available_at
                    resource.meta = {**existing.meta, **resource.meta}
                    resource.meta["updated_at"] = _now()
                    expected = existing.version
                else:
                    resource.meta["created_at"] = _now()
                    expected = 0
                try:
                    await self.storage.put(
                        self.name, resource, expected_version=expected
                    )
                except VersionConflict:
                    if attempt == 2:
                        raise
                    continue
                if existing is not None:
                    report.updated += 1
                else:
                    report.imported += 1
                stored = True
                break
            if not stored:
                continue
            if mode == "on_import":
                verdict = await self.validate_now(resource.id, operator=operator)
                report.validation.append(
                    {
                        "id": resource.id,
                        "alive": verdict.alive,
                        "latency_ms": verdict.latency_ms,
                        "error": verdict.error,
                        "action": verdict.action.value,
                    }
                )
            elif mode == "batch":
                pending_validate.append(resource.id)
        if mode == "batch" and pending_validate:
            self._start_validate_job(
                report.batch_id or uuid.uuid4().hex, pending_validate, operator
            )
        self.purser.bus.publish(
            "import_finished",
            self.name,
            data={"report": report.to_dict()},
        )
        self.purser.audit.record(
            operator=operator,
            action="import_resources",
            pool=self.name,
            detail={"report": report.to_dict(), "batch_id": report.batch_id},
        )
        return report

    async def generate(self, specs: Sequence[dict[str, Any] | Resource], *, source: str = "builtin", operator: str = "system") -> ImportReport:
        return await self.import_resources(
            specs, source=source, operator=operator, import_validate="on_import"
        )

    async def apply_defaults(
        self,
        *,
        operator: str = "system",
        reason: str = "apply pool defaults",
    ) -> dict[str, int]:
        """Backfill pool-level defaults onto resources that lack them.

        Mirrors the import-time defaults (``expires_in``, ``usage_quota``,
        ``quota_limit``) but only ever fills a missing value; existing
        resource-level settings are never overwritten, so this is safe to run
        on a live pool.  ``max_slots`` cannot be detected as "unset"
        retroactively and is left alone.
        """

        updated = 0
        now = _now()
        expires_in = self.cfg.expires_in
        usage_quota = self.cfg.usage_quota
        quota_limit = self.cfg.quota_limit
        if expires_in is None and usage_quota is None and quota_limit is None:
            return {"updated": 0}
        async for resource in self.storage.scan(self.name):
            extra: dict[str, Any] = {}
            clear: set[str] = set()
            if (
                expires_in is not None
                and resource.expires_at is None
            ):
                extra["expires_at"] = now + expires_in
            if usage_quota is not None and resource.usage_quota is None:
                extra["usage_quota"] = int(usage_quota)
            if quota_limit is not None and resource.quota is None:
                extra["quota"] = Quota(limit=int(quota_limit))
            if not extra:
                continue
            if await self._cas(
                resource.id,
                ResourcePatch(
                    meta_patch={**resource.meta, "updated_by": operator, "update_reason": reason},
                    clear_fields=frozenset(clear),
                    **extra,
                ),
            ):
                updated += 1
        if updated:
            self.purser.audit.record(
                operator=operator,
                action="apply_defaults",
                pool=self.name,
                reason=reason,
                detail={"updated": updated},
            )
        return {"updated": updated}

    async def _restore_resources(
        self, records: Sequence[Any], *, conflict: str, operator: str
    ) -> dict[str, Any]:
        stats: dict[str, Any] = {
            "imported": 0,
            "updated": 0,
            "skipped": 0,
            "normalized": 0,
            "errors": [],
        }
        for index, entry in enumerate(records):
            try:
                resource, normalized = self._restore_resource(entry)
            except Exception as exc:
                stats["errors"].append({"line": index, "error": str(exc)})
                continue
            existing = await self.storage.get(self.name, resource.id)
            if existing is not None and conflict == "skip":
                stats["skipped"] += 1
                continue
            if existing is not None:
                resource.version = existing.version
                resource.meta.setdefault("created_at", existing.meta.get("created_at", _now()))
                expected = existing.version
            else:
                resource.meta["created_at"] = resource.meta.get("created_at") or _now()
                expected = 0
            resource.meta["updated_at"] = _now()
            try:
                for attempt in range(3):
                    try:
                        await self.storage.put(
                            self.name, resource, expected_version=expected
                        )
                        break
                    except VersionConflict:
                        if attempt == 2:
                            raise
                        current = await self.storage.get(self.name, resource.id)
                        expected = current.version if current is not None else 0
            except Exception as exc:
                stats["errors"].append(
                    {"line": index, "id": resource.id, "error": str(exc)}
                )
                continue
            if existing is not None:
                stats["updated"] += 1
            else:
                stats["imported"] += 1
            if normalized:
                stats["normalized"] += 1
            self.purser.bus.publish(
                "resource_restored",
                self.name,
                rid=resource.id,
                data={"state": resource.state.value, "conflict": conflict},
            )
        return stats

    def _restore_resource(self, entry: Any) -> tuple[Resource, bool]:
        if not isinstance(entry, dict):
            raise ValueError("resource entry must be an object")
        rid = str(entry.get("id") or "").strip()
        if not rid:
            raise ValueError("resource id must be non-empty")
        payload = entry.get("payload")
        if payload is None:
            payload = {}
        if not isinstance(payload, dict):
            raise ValueError("payload must be an object")
        tags_raw = entry.get("tags") or ()
        if not isinstance(tags_raw, (list, tuple, set, frozenset)):
            raise ValueError("tags must be a list")
        raw_state = str(entry.get("state") or "idle")
        try:
            state = ResourceState(raw_state)
        except ValueError:
            state = ResourceState.IDLE
            normalized = True
        else:
            normalized = state in _RESTORE_IDLE_STATES
            if normalized:
                state = ResourceState.IDLE
        meta_raw = entry.get("meta")
        meta = dict(meta_raw) if isinstance(meta_raw, dict) else {}
        if normalized:
            for key in _RESTORE_TRANSIENT_META:
                meta.pop(key, None)
        concurrency_raw = entry.get("concurrency")
        concurrency_raw = concurrency_raw if isinstance(concurrency_raw, dict) else {}
        slots_raw = concurrency_raw.get("max_slots", entry.get("max_slots"))
        max_slots = (
            int(slots_raw)
            if slots_raw is not None
            else (self.cfg.max_slots or self.adapter.default_concurrency)
        )
        if max_slots <= 0:
            raise ValueError("max_slots must be positive")
        health_raw = entry.get("health")
        health_raw = health_raw if isinstance(health_raw, dict) else {}
        health = HealthStats(
            score=float(health_raw.get("score", 0.5)),
            samples=int(health_raw.get("samples", 0)),
            last_latency_ms=_optional_float(health_raw.get("last_latency_ms")),
            last_checked_at=_optional_float(health_raw.get("last_checked_at")),
            last_error=health_raw.get("last_error"),
            consecutive_failures=int(health_raw.get("consecutive_failures", 0)),
        )
        quota_raw = entry.get("quota")
        quota = None
        if isinstance(quota_raw, dict):
            quota = Quota(
                limit=int(quota_raw.get("limit", 0)),
                used=int(quota_raw.get("used", 0)),
                reset_at=_optional_float(quota_raw.get("reset_at")),
            )
        site_failures: dict[str, SiteFailure] = {}
        failures_raw = entry.get("site_failures")
        if isinstance(failures_raw, dict):
            now = _now()
            for site, data in failures_raw.items():
                data = data if isinstance(data, dict) else {}
                site_failures[str(site)] = SiteFailure(
                    site=str(site),
                    count=int(data.get("count", 1)),
                    since=_optional_float(data.get("since")) or now,
                    until=_optional_float(data.get("until")) or now,
                )
        resource = Resource(
            id=rid,
            kind=self.adapter.kind,
            payload=payload,
            state=state,
            tags=frozenset(str(tag) for tag in tags_raw),
            concurrency=Concurrency(max_slots=max_slots),
            health=health,
            site_failures=site_failures,
            quota=quota,
            usage_quota=_optional_int(entry.get("usage_quota"), "usage_quota"),
            used_count=max(0, int(entry.get("used_count") or 0)),
            expires_at=_optional_float(entry.get("expires_at")),
            next_available_at=_optional_float(entry.get("next_available_at")),
            mutex_group=str(entry["mutex_group"]) if entry.get("mutex_group") else None,
            schedule_window=_schedule_window(entry.get("schedule_window")),
            meta=meta,
        )
        typed = self.adapter.adopt(resource)
        return typed, normalized

    # -- acquisition -------------------------------------------------------

    async def acquire(self, req: AcquireRequest | None = None, **kwargs: Any) -> Lease:
        await self._ensure_started("acquire")
        # Work on a copy: kwargs and the generated trace id are per-call, so a
        # reusable request must not accumulate them between acquisitions.
        request = replace(req) if req is not None else AcquireRequest()
        unknown = set(kwargs) - set(AcquireRequest.__dataclass_fields__)
        if unknown:
            fields = ", ".join(sorted(unknown))
            raise TypeError(f"acquire() got unexpected keyword argument(s): {fields}")
        for key, value in kwargs.items():
            setattr(request, key, value)
        timeout = request.timeout if request.timeout is not None else self.cfg.wait_timeout
        request.ensure_trace()
        self.purser.metrics.inc(f"pool.{self.name}.acquire_total")
        if not isinstance(request.slots, int) or request.slots <= 0:
            raise ConfigError("slots must be a positive integer")
        if request.use_limit is not None and request.use_limit < 0:
            raise ConfigError("use_limit must be >= 0")

        if request.resource_id is not None:
            return await self._acquire_directed(request, timeout)

        lease = await self._try_allocate(request)
        if lease is not None:
            return lease

        counts = await self.counts()
        total = sum(counts.values())
        if total == 0:
            raise PoolEmpty(f"pool {self.name} has no allocatable resources")
        if counts.get(ResourceState.NEW, 0) == total:
            raise PoolExhausted(
                f"pool {self.name} has {total} resource(s) but all are in NEW "
                "(not validated); import with import_validate='on_import'|'batch' "
                "or call validate_now()"
            )
        if timeout <= 0:
            raise PoolExhausted(f"pool {self.name} could not satisfy request immediately")

        handle = self._enqueue_waiter(request)
        current = asyncio.current_task()
        if current is not None:
            current.add_done_callback(lambda _task: handle.cancel_pending())
        try:
            return await self._await_slot(handle, request, timeout, counts)
        except asyncio.CancelledError:
            await self._abandon_wait_handle(handle)
            raise

    async def _await_slot(
        self,
        handle: _WaitHandle,
        request: AcquireRequest,
        timeout: float,
        counts: dict[ResourceState, int],
    ) -> Lease:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        # Redis has no push channel: the waiter itself re-checks shared
        # storage so a release on another instance can wake it (V03).
        poll = self.cfg.coordination == "redis"
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            window = min(WAITER_POLL_INTERVAL, remaining) if poll else remaining
            try:
                return await asyncio.wait_for(
                    asyncio.shield(handle.future), window
                )
            except asyncio.TimeoutError:
                if handle.future.done():
                    return cast(Lease, handle.future.result())
                if not poll or loop.time() >= deadline:
                    break
                lease = await self._poll_wait_handle(handle, request)
                if lease is not None:
                    return lease

        handle.cancel_pending()
        state_hint = ", ".join(
            f"{state.value}={n}" for state, n in counts.items() if n
        )
        raise PoolExhausted(
            f"pool {self.name} exhausted after {timeout:.1f}s"
            f" (consumer={request.consumer!r}, slots={request.slots},"
            f" states: {state_hint})"
        )

    async def _poll_wait_handle(
        self, handle: _WaitHandle, request: AcquireRequest
    ) -> Lease | None:
        if handle.cancelled or handle.future.done() or handle.serving:
            return None
        handle.serving = True
        if self._active_polls == 0:
            self._polls_idle.clear()
        self._active_polls += 1
        try:
            try:
                lease = await self._try_allocate(request)
            except (NoSuitableResource, PoolEmpty, BadState):
                lease = None
            except Exception:
                # A close/cancel may win while the poll is in flight; let the
                # completed future surface that outcome to the caller.
                if handle.cancelled or handle.future.done():
                    return None
                raise
            if lease is None:
                return None
            if handle.cancelled or handle.future.done():
                await self._abandon_lease(lease)
                return None
            handle.future.set_result(lease)
            return lease
        finally:
            self._active_polls -= 1
            if self._active_polls == 0:
                self._polls_idle.set()
            handle.serving = False

    def _enqueue_waiter(self, req: AcquireRequest) -> _WaitHandle:
        loop = asyncio.get_running_loop()
        handle = _WaitHandle(
            seq=self._next_seq(),
            priority=req.priority,
            future=loop.create_future(),
            req=req,
        )
        heapq.heappush(self._waiters, handle)
        return handle

    async def _serve_waiters(self, limit: int = 64) -> int:
        served = 0
        pending: list[_WaitHandle] = []
        current: _WaitHandle | None = None
        try:
            while self._waiters and served < limit:
                handle = heapq.heappop(self._waiters)
                if handle.cancelled or handle.future.done():
                    continue
                if handle.serving:
                    pending.append(handle)
                    continue
                current = handle
                handle.serving = True
                try:
                    try:
                        lease = await self._try_allocate(handle.req)
                    except (NoSuitableResource, PoolEmpty, BadState):
                        lease = None
                finally:
                    handle.serving = False
                current = None
                if handle.cancelled or handle.future.done():
                    if lease is not None:
                        # The waiter timed out or was cancelled while we were
                        # allocating; give the freshly acquired slot back.
                        await self._abandon_lease(lease)
                        continue
                if lease is not None:
                    handle.future.set_result(lease)
                    served += 1
                else:
                    pending.append(handle)
        finally:
            if (
                current is not None
                and not current.cancelled
                and not current.future.done()
            ):
                heapq.heappush(self._waiters, current)
            for handle in pending:
                if not handle.cancelled and not handle.future.done():
                    heapq.heappush(self._waiters, handle)
        return served

    async def _abandon_lease(self, lease: Lease) -> None:
        """Return a freshly acquired slot whose waiter vanished before delivery."""

        await self._coordination.unregister_lease(self.name, lease.token)
        await self.storage.transition_release(
            self.name, lease.resource_id, lease.slots
        )
        self._leases.unregister(lease.token)
        if lease.consumer:
            self._consumer_holds.get(lease.consumer, set()).discard(
                lease.resource_id
            )

    async def _abandon_wait_handle(self, handle: _WaitHandle) -> None:
        """Undo a lease delivered to a waiter whose task was cancelled."""

        handle.cancel_pending()
        if not handle.future.done() or handle.future.cancelled():
            return
        with contextlib.suppress(Exception):
            await self._abandon_lease(handle.future.result())

    async def _acquire_directed(self, req: AcquireRequest, timeout: float) -> Lease:
        rid = req.resource_id
        assert rid is not None
        deadline = time.monotonic() + timeout
        while True:
            resource = await self.storage.get(self.name, rid)
            if resource is None:
                raise ResourceNotFound(f"{self.name}:{rid}")
            if req.consumer and not req.allow_duplicate:
                held = await self._consumer_hold(rid, req.consumer)
                if held is not None:
                    held.payload = dict(resource.payload)
                    held.resource = resource
                    return held
            if resource.allocatable and resource.concurrency.available >= req.slots:
                lease = await self._commit(req, rid)
                if lease is not None:
                    return lease
            elif resource.state not in (ResourceState.ACTIVE, ResourceState.FULL, ResourceState.IDLE):
                raise BadState(f"resource {rid} is {resource.state.value}")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ResourceBusy(f"resource {rid} busy after {timeout:.1f}s")
            await asyncio.sleep(min(0.1, remaining))

    async def _consumer_hold(self, rid: str, consumer: str) -> Lease | None:
        for lease in await self._coordination.leases_for(self.name, rid):
            if lease.consumer == consumer:
                return lease
        return None

    async def _try_allocate(self, req: AcquireRequest) -> Lease | None:
        if req.resource_id is not None:
            resource = await self.storage.get(self.name, req.resource_id)
            if resource is None or not (
                resource.allocatable and resource.concurrency.available >= req.slots
            ):
                return None
            return await self._commit(req, req.resource_id)

        candidates = await self._hard_filtered_candidates(req)
        if not candidates:
            return None

        ctx = AllocationContext(
            now=_now(),
            request=req,
            weights=self.cfg.weights,
            affinity_scores=(
                await self._affinity_scores(req.consumer, candidates)
                if self.cfg.affinity_weight > 0
                else None
            ),
            top_k=self.cfg.top_k,
            cursor=self._next_rr_cursor(),
            rng=random.Random(f"{req.trace_id}:{self._seq}"),
        )
        ranked = self._allocator.rank(candidates, ctx)
        if not ranked:
            raise NoSuitableResource(req.target, checked=0)
        # First try the allocator's weighted pick (top-K anti-herd); fall back
        # to the remaining candidates in ranked order if it is busy or rejected.
        first = self._allocator.pick(candidates, ctx)
        ordered: list[Resource] = []
        if first is not None:
            ordered.append(first)
            ordered.extend(r for r, _ in ranked if r.id != first.id)
        else:
            ordered.extend(r for r, _ in ranked)
        limit = max(self.cfg.top_k, 1)
        checked = 0
        check_rejected = 0
        for resource in ordered[: max(limit * 3, 1)]:
            checked += 1
            verdict = await self._request_check(resource, req)
            if verdict is CheckVerdict.BAN:
                await self._on_ban_verdict(resource, req)
                check_rejected += 1
                continue
            if verdict is CheckVerdict.FAIL:
                check_rejected += 1
                continue
            lease = await self._commit(req, resource.id)
            if lease is not None:
                return lease
        if checked > 0 and check_rejected == checked:
            raise NoSuitableResource(req.target, checked=checked)
        # Candidates existed but every commit lost to contention: let the
        # caller enter the fair wait queue instead of failing immediately.
        return None

    def _next_rr_cursor(self) -> int:
        self._rr_cursor += 1
        return self._rr_cursor

    async def _affinity_scores(
        self, consumer: str | None, candidates: list[Resource]
    ) -> dict[str, float]:
        if consumer is None:
            return {}
        history = self._affinity.get(consumer, {})
        if not history:
            return {}
        max_score = max(history.values()) or 1.0
        return {
            r.id: history.get(r.id, 0.0) / max_score for r in candidates
        }

    async def _hard_filtered_candidates(self, req: AcquireRequest) -> list[Resource]:
        now = _now()
        out: list[Resource] = []
        async for resource in self.storage.scan(
            self.name, states=set(ALLOCATABLE_STATES)
        ):
            if resource.concurrency.available < req.slots:
                continue
            if req.tags and not req.tags <= resource.tags:
                continue
            if req.required_tag and req.required_tag not in resource.tags:
                continue
            if not self._schedule_open(resource, now):
                continue
            if resource.expires_at is not None and resource.expires_at <= now:
                await self._on_expiration(resource)
                continue
            if not req.ignore_cooldown:
                if resource.next_usable_at(now, req.consumer) > now:
                    continue
            if req.target and resource.site_failures.get(req.target, SiteFailure(req.target, until=0)).active(now):
                continue
            if resource.usage_quota is not None and resource.used_count >= resource.usage_quota:
                continue
            if resource.quota is not None and resource.quota.exhausted:
                continue
            if resource.mutex_group and req.consumer and await self._holds_group(
                req.consumer, resource.mutex_group, resource.id
            ):
                continue
            if req.consumer and not req.allow_duplicate:
                if self._consumer_holds.get(req.consumer, set()) & {resource.id}:
                    continue
            out.append(resource)
        return out

    async def _holds_group(self, consumer: str, group: str, except_rid: str) -> bool:
        for rid in self._consumer_holds.get(consumer, set()):
            if rid == except_rid:
                continue
            resource = await self.storage.get(self.name, rid)
            if resource is not None and resource.mutex_group == group:
                return True
        return False

    @staticmethod
    def _schedule_open(resource: Resource, now: float) -> bool:
        if resource.schedule_window is None:
            return True
        start, end = resource.schedule_window
        current = time.strftime("%H:%M", time.localtime(now))
        if start <= end:
            return start <= current <= end
        return current >= start or current <= end  # overnight window

    async def _request_check(self, resource: Resource, req: AcquireRequest) -> CheckVerdict:
        check_target = req.probe or req.target
        if not check_target:
            return CheckVerdict.PASS
        key = (req.consumer or "", check_target, resource.id)
        now = _now()
        cached = self._check_cache.get(key)
        if cached and cached[1] > now:
            return cached[0]
        check_req = req if req.probe is None else replace(req, target=req.probe)
        try:
            verdict = await self.adapter.on_request_check(resource, check_req)
        except Exception as exc:
            self._guard_error("on_request_check", exc)
            verdict = CheckVerdict.FAIL
        self._check_cache[key] = (verdict, now + self.cfg.check_cache_ttl)
        if len(self._check_cache) > 2048:
            # Cheap TTL sweep keeps the cache bounded.
            self._check_cache = {
                k: v for k, v in self._check_cache.items() if v[1] > now
            }
        return verdict

    async def _commit(self, req: AcquireRequest, rid: str) -> Lease | None:
        resource = await self.storage.get(self.name, rid)
        if resource is None or not (
            resource.allocatable and resource.concurrency.available >= req.slots
        ):
            return None
        if resource.mutex_group and req.consumer and await self._holds_group(
            req.consumer, resource.mutex_group, rid
        ):
            return None
        lease = Lease.new(
            self.name,
            rid,
            consumer=req.consumer,
            slots=req.slots,
            ttl=self.cfg.lease_ttl,
            priority=req.priority,
            use_limit=req.use_limit,
            trace_id=req.trace_id,
        )
        lease.target = req.target
        old_state = resource.state
        result = await self.storage.transition_acquire(self.name, rid, lease.slots)
        if not result.ok or result.resource is None:
            return None
        if old_state != result.resource.state:
            await self._notify_state_changed(
                rid, old_state, result.resource.state, "acquire"
            )
        lease.payload = dict(result.resource.payload)
        lease.resource = result.resource
        registered = False
        try:
            try:
                await self._coordination.register_lease(self.name, lease)
            except asyncio.CancelledError:
                # A cancelled acquire must not leak the slot we just took.
                await self.storage.transition_release(
                    self.name, rid, lease.slots
                )
                raise
            except Exception:
                # Coordination is transient; roll the slot back so the resource
                # does not stay busy with an untracked lease.
                logger.exception("register lease failed for %s", rid)
                await self.storage.transition_release(self.name, rid, lease.slots)
                return None
            registered = True
            self._leases.register(lease, self._renew)
            if req.consumer:
                self._consumer_holds[req.consumer].add(rid)
                self._record_affinity(req.consumer, rid)
            self.purser.bus.publish(
                "lease_acquired",
                self.name,
                rid=rid,
                data={
                    "consumer": req.consumer,
                    "slots": lease.slots,
                    "target": lease.target,
                },
                trace_id=req.trace_id,
            )
            self.purser.metrics.inc(f"pool.{self.name}.lease_total")
        except asyncio.CancelledError:
            if registered:
                await self._coordination.unregister_lease(self.name, lease.token)
            await self.storage.transition_release(self.name, rid, lease.slots)
            raise
        try:
            await self.adapter.on_acquire(result.resource, lease)
        except Exception as exc:
            self._guard_error("on_acquire", exc)
        return lease

    async def _renew(self, lease: Lease) -> bool:
        if lease.use_limit is not None and lease.used >= lease.use_limit:
            return False
        if self.cfg.max_lease_duration is not None:
            hard_cap = lease.started_at + self.cfg.max_lease_duration
            if hard_cap <= _now():
                return False  # total lease lifetime is over
        expires_at = _now() + self.cfg.lease_ttl
        if self.cfg.max_lease_duration is not None:
            expires_at = min(expires_at, lease.started_at + self.cfg.max_lease_duration)
        ok = await self._coordination.renew_lease(
            self.name, lease.resource_id, lease.token, expires_at
        )
        if ok:
            lease.expires_at = expires_at
            lease.heartbeat_at = _now()
            try:
                await self.adapter.on_renew(lease.resource, lease)
            except Exception as exc:
                self._guard_error("on_renew", exc)
        return ok

    def _record_affinity(self, consumer: str, rid: str) -> None:
        history = self._affinity[consumer]
        history[rid] = float(history.get(rid, 0.0)) + 1.0
        if len(history) > 64:
            oldest = next(iter(history))
            history.pop(oldest, None)

    # -- lease() context manager ------------------------------------------

    @contextlib.asynccontextmanager
    async def lease(
        self, req: AcquireRequest | None = None, **kwargs: Any
    ) -> AsyncIterator[Lease]:
        """Recommended path: acquire now, always release at block exit (S5)."""

        lease = await self.acquire(req, **kwargs)
        try:
            yield lease
        finally:
            await self.release(lease, outcome=lease.outcome, used=lease.used)

    # -- release and usage feedback ---------------------------------------

    async def release(
        self,
        lease: Lease,
        *,
        outcome: UsageOutcome | None = None,
        used: int | None = None,
    ) -> bool:
        if used is not None and used < 0:
            raise ConfigError("used must be >= 0")
        if outcome is not None and not isinstance(outcome, UsageOutcome):
            raise ConfigError("outcome must be a UsageOutcome")
        before = await self.storage.get(self.name, lease.resource_id)
        before_state = before.state if before is not None else None
        if before is not None:
            try:
                await self.adapter.before_release(before, lease)
            except Exception as exc:
                self._guard_error("before_release", exc)
        removed = await self._coordination.unregister_lease(self.name, lease.token)
        if removed is None:
            # A revoke, expiry, concurrent retry or coordination wipe already
            # removed our token.  Never decrement storage here: the busy slot
            # may now belong to a fresh lease from another instance, and a
            # blind decrement would steal it.  Orphaned slots are recovered by
            # _reconcile_leases() on the next start(), which is the designated
            # restart path.
            self._leases.unregister(lease.token)
            if lease.consumer:
                self._consumer_holds.get(lease.consumer, set()).discard(
                    lease.resource_id
                )
            self.purser.metrics.inc(f"pool.{self.name}.release_total")
            await self._serve_waiters()
            return True
        try:
            result = await self.storage.transition_release(
                self.name, lease.resource_id, lease.slots
            )
        except BaseException:
            # The token is already gone from coordination.  Put it back when
            # storage still counts our slots, otherwise the retry path would
            # see no token and leave the slot orphaned until reconcile.
            try:
                await self._restore_release_claim(lease, removed)
            except BaseException:
                logger.exception(
                    "failed to restore lease %s after storage release error",
                    lease.token,
                )
            raise
        if not result.ok:
            # Put the token back only if the resource still counts our slots.
            # If used_slots already dropped to zero, a concurrent revoke or
            # reconcile returned them; re-registering would leave a ghost lease
            # in ``active()`` that the storage plane no longer backs.
            await self._restore_release_claim(lease, removed)
            raise LeaseConflict(
                f"token {lease.token} could not return {lease.resource_id} slots"
            )
        self._leases.unregister(lease.token)
        if lease.consumer:
            self._consumer_holds.get(lease.consumer, set()).discard(lease.resource_id)
        if before_state is not None and result.state is not None and before_state != result.state:
            await self._notify_state_changed(
                lease.resource_id, before_state, result.state, "release"
            )
        outcome = outcome or lease.outcome or UsageOutcome.OK
        lease.outcome = outcome
        self.purser.bus.publish(
            "lease_released",
            self.name,
            rid=lease.resource_id,
            data={
                "outcome": outcome.value,
                "consumer": lease.consumer,
            },
            trace_id=lease.trace_id,
        )
        await self._apply_usage_outcome(lease.resource_id, outcome, used, lease)
        self.purser.usage.record(
            ts=_now(),
            pool=self.name,
            kind=self.cfg.kind,
            rid=lease.resource_id,
            consumer=lease.consumer,
            target=lease.target,
            outcome=outcome.value,
            used=used if used is not None else (lease.used or 1),
            slots=lease.slots,
        )
        await self._serve_waiters()
        self.purser.metrics.inc(f"pool.{self.name}.release_total")
        return True

    async def _restore_release_claim(self, lease: Lease, removed: Lease) -> None:
        """Put a failed release's token back without resurrecting a freed slot."""

        try:
            current = await self.storage.get(self.name, lease.resource_id)
        except Exception:
            # Storage state is unknown: keep the coordination claim rather than
            # letting another instance treat a possibly-live slot as free.
            logger.exception(
                "could not verify %s after failed release; restoring token",
                lease.resource_id,
            )
            await self._coordination.register_lease(self.name, removed)
            return
        if (
            current is not None
            and current.concurrency.used_slots >= lease.slots
        ):
            await self._coordination.register_lease(self.name, removed)

    async def release_consumer(
        self,
        consumer: str,
        *,
        outcome: UsageOutcome = UsageOutcome.OK,
        operator: str = "system",
    ) -> list[Lease]:
        """Return every live lease held by ``consumer`` (logout / teardown)."""

        if not consumer:
            raise ConfigError("consumer must be non-empty")
        if not isinstance(outcome, UsageOutcome):
            raise ConfigError("outcome must be a UsageOutcome")
        released: list[Lease] = []
        for lease in await self._coordination.active(self.name):
            if lease.consumer != consumer:
                continue
            await self.release(lease, outcome=outcome)
            released.append(lease)
        if released:
            self.purser.audit.record(
                operator=operator,
                action="release_consumer",
                pool=self.name,
                reason=f"consumer {consumer}",
                detail={
                    "consumer": consumer,
                    "outcome": outcome.value,
                    "tokens": [lease.token for lease in released],
                },
            )
        return released

    async def _apply_usage_outcome(
        self, rid: str, outcome: UsageOutcome, used: int | None, lease: Lease
    ) -> None:
        for _attempt in range(2):
            resource = await self.storage.get(self.name, rid)
            if resource is None or resource.state in (
                ResourceState.ARCHIVED,
                ResourceState.DRAINING,
            ):
                return
            now = _now()
            health = resource.health
            site_failures = resource.site_failures
            meta = dict(resource.meta)
            consume = used if used is not None else (lease.used or 1)
            used_count = resource.used_count + consume
            next_available_at = resource.next_available_at

            if outcome in (UsageOutcome.RATE_LIMITED, UsageOutcome.CAPTCHA, UsageOutcome.NETWORK_ERROR):
                if lease.target:
                    self._record_site_failure(site_failures, lease.target, now)
            if outcome in (UsageOutcome.TIMEOUT, UsageOutcome.NETWORK_ERROR, UsageOutcome.BANNED):
                health.update(alive=False, error=outcome.value, now=now)
                meta["needs_validate"] = now
            if outcome is UsageOutcome.RATE_LIMITED:
                next_available_at = now + max(self.cfg.cooldown_after_release, 5.0)
            if outcome is UsageOutcome.PREEMPTED:
                if lease.consumer:
                    meta[f"cooldown:{lease.consumer}"] = now + 10.0

            # Usage metering (S19) and period quota (S16).
            quota = resource.quota
            if quota is not None:
                quota.used += consume

            target_state = resource.state
            if self.cfg.cooldown_after_release > 0 and not self.cfg.cooldown_consumer_only:
                next_available_at = now + self.cfg.cooldown_after_release
            elif (
                self.cfg.cooldown_after_release > 0
                and self.cfg.cooldown_consumer_only
                and lease.consumer
            ):
                meta[f"cooldown:{lease.consumer}"] = now + self.cfg.cooldown_after_release

            if outcome is not UsageOutcome.BANNED:
                if (quota is not None and quota.exhausted) or (
                    resource.usage_quota is not None
                    and used_count >= resource.usage_quota
                ):
                    if resource.state in ALLOCATABLE_STATES:
                        target_state = ResourceState.QUOTA_EXHAUSTED
                        if (
                            self.cfg.quota_cooldown > 0
                            and resource.usage_quota is not None
                            and used_count >= resource.usage_quota
                        ):
                            next_available_at = now + self.cfg.quota_cooldown
                        self.purser.bus.publish(
                            "quota_exhausted", self.name, rid=rid, trace_id=lease.trace_id
                        )

            patch = ResourcePatch(
                state=target_state,
                health=health,
                site_failures=site_failures,
                quota=quota,
                used_count=used_count,
                next_available_at=next_available_at,
                meta_patch=meta,
            )
            ok = await self.storage.set_state(
                self.name,
                rid,
                resource.version,
                expect_state=resource.state,
                patch=patch,
            )
            if ok:
                try:
                    await self.adapter.on_release(resource, outcome)
                except Exception as exc:
                    self._guard_error("on_release", exc)
                if outcome is UsageOutcome.BANNED:
                    await self._on_validation_fail(
                        rid,
                        f"usage banned for {lease.target or 'target'}",
                        operator="system",
                    )
                return

    def _record_site_failure(
        self, site_failures: dict[str, SiteFailure], site: str, now: float
    ) -> None:
        current = site_failures.get(site)
        if current is not None and current.active(now):
            current.count += 1
            current.until = now + self.cfg.site_failure_ttl
        else:
            site_failures[site] = SiteFailure(site=site, count=1, since=now, until=now + self.cfg.site_failure_ttl)
        if len(site_failures) > self.cfg.site_failure_max:
            oldest = min(site_failures, key=lambda k: site_failures[k].since)
            site_failures.pop(oldest, None)

    # -- hooks / callbacks ------------------------------------------------

    async def _on_lease_expired(self, lease: Lease) -> None:
        """Watchdog path (5.2): a lease passed its deadline while still held."""

        removed = await self._coordination.unregister_lease(self.name, lease.token)
        self._leases.unregister(lease.token)
        if lease.consumer:
            self._consumer_holds.get(lease.consumer, set()).discard(lease.resource_id)
        if removed is None:
            # The token was already gone (revoked elsewhere or Redis TTL fired).
            # Its slots may now belong to a fresh holder, so never decrement
            # storage here; reconcile on the next start() cleans up orphans.
            self.purser.bus.publish(
                "lease_lost",
                self.name,
                rid=lease.resource_id,
                data={"reason": "token already gone"},
                trace_id=lease.trace_id,
            )
            self.purser.metrics.inc(f"pool.{self.name}.lease_lost_total")
            await self._serve_waiters()
            return
        result = await self.storage.transition_release(
            self.name, lease.resource_id, lease.slots
        )
        if result.ok:
            self.purser.bus.publish(
                "lease_expired",
                self.name,
                rid=lease.resource_id,
                data={"consumer": lease.consumer},
                trace_id=lease.trace_id,
            )
        else:
            self.purser.bus.publish(
                "lease_lost",
                self.name,
                rid=lease.resource_id,
                data={},
                trace_id=lease.trace_id,
            )
        await self._serve_waiters()

    async def _on_lease_lost(self, lease: Lease) -> None:
        # The heartbeat gave up on this lease (e.g. use_limit reached or the
        # coordination token vanished).  Stop tracking it in the manager and
        # return the slots if we still own the token; a token that is already
        # gone may now belong to a fresh holder, so leave those slots to
        # _reconcile_leases() instead of stealing them.
        removed = await self._coordination.unregister_lease(self.name, lease.token)
        self._leases.unregister(lease.token)
        if lease.consumer:
            self._consumer_holds.get(lease.consumer, set()).discard(lease.resource_id)
        if removed is not None:
            await self.storage.transition_release(
                self.name, lease.resource_id, lease.slots
            )
        self.purser.bus.publish(
            "lease_lost",
            self.name,
            rid=lease.resource_id,
            data={"reason": "heartbeat rejected"},
            trace_id=lease.trace_id,
        )
        self.purser.metrics.inc(f"pool.{self.name}.lease_lost_total")
        try:
            resource = await self.storage.get(self.name, lease.resource_id)
            if resource is not None:
                await self.adapter.on_lease_lost(resource, lease)
        except Exception as exc:
            self._guard_error("on_lease_lost", exc)
        await self._serve_waiters()

    def _guard_error(self, context: str, error: Exception) -> None:
        logger.warning("%s hook failed for pool %s: %s", context, self.name, error)
        try:
            asyncio.get_running_loop().create_task(
                self.adapter.on_error(context, error)
            )
        except Exception:
            pass

    # -- validation --------------------------------------------------------

    async def validate_now(
        self,
        rid: str,
        *,
        operator: str = "system",
    ) -> HealthReport:
        resource = await self.require(rid)
        now = _now()
        state = resource.state
        if state is ResourceState.BANNED:
            raise BadState("banned resources must go through unban()")
        if state in (ResourceState.DRAINING, ResourceState.ARCHIVED, ResourceState.SUSPENDED, ResourceState.QUOTA_EXHAUSTED):
            raise BadState(f"cannot validate {state.value} resource")
        if state is ResourceState.ACTIVE:
            pass  # validate in place, no state change
        elif state in (ResourceState.NEW, ResourceState.IDLE):
            entered = await self.storage.set_state(
                self.name,
                rid,
                resource.version,
                expect_state=state,
                patch=ResourcePatch(
                    state=ResourceState.VALIDATING,
                    meta_patch={**resource.meta, "validate_started_at": now},
                ),
            )
            if not entered:
                # Raced with another validator; validate against current state.
                resource = await self.require(rid)
            else:
                resource.state = ResourceState.VALIDATING
                await self._notify_state_changed(
                    rid, state, ResourceState.VALIDATING, "validate"
                )

        report: HealthReport
        async with self._validate_sem:
            try:
                report = await self.adapter.validate_with_checks(resource)
            except Exception as exc:
                self._guard_error("validate", exc)
                report = HealthReport.fail(str(exc))
        self.purser.bus.publish(
            "health_reported",
            self.name,
            rid=rid,
            data={"alive": report.alive, "latency_ms": report.latency_ms, "error": report.error},
        )
        if report.alive:
            await self._on_validation_ok(rid, report)
        else:
            await self._on_validation_fail(
                rid,
                report.error or "validation failed",
                operator=operator,
                from_states={ResourceState.VALIDATING, ResourceState.ACTIVE, ResourceState.IDLE, ResourceState.NEW},
            )
        return report

    async def validate_check(self, rid: str, check: str) -> HealthReport:
        """Run one named check in isolation; never writes health or state."""

        resource = await self.require(rid)
        try:
            return await self.adapter.run_validator(resource, check)
        except ConfigError:
            raise
        except Exception as exc:
            self._guard_error("validate_check", exc)
            return HealthReport.fail(str(exc))

    async def _on_validation_ok(self, rid: str, report: HealthReport) -> None:
        for _attempt in range(2):
            resource = await self.storage.get(self.name, rid)
            if resource is None or resource.state is ResourceState.DRAINING:
                return
            health = resource.health
            health.update(alive=True, latency_ms=report.latency_ms, now=_now())
            meta = dict(resource.meta)
            meta["last_validated_at"] = health.last_checked_at
            meta["needs_validate"] = 0
            patch = ResourcePatch(
                state=ResourceState.IDLE if resource.state is ResourceState.VALIDATING else None,
                health=health,
                meta_patch=meta,
            )
            ok = await self._apply_state(
                rid, patch, expect_state=resource.state, reason="validate_ok"
            )
            if ok:
                self.purser.bus.publish("validate_ok", self.name, rid=rid)
                await self._serve_waiters()
                return

    async def _on_validation_fail(
        self,
        rid: str,
        reason: str,
        *,
        operator: str = "system",
        from_states: set[ResourceState] | None = None,
        site_failures: dict[str, SiteFailure] | None = None,
    ) -> None:
        for _attempt in range(3):
            resource = await self.storage.get(self.name, rid)
            if resource is None:
                return
            if from_states and resource.state not in from_states:
                return
            now = _now()
            health = resource.health
            health.update(alive=False, error=reason, now=now)
            policy = self.cfg.quarantine
            created = resource.created_at or now
            fail_count = health.consecutive_failures
            threshold = policy.threshold_for(created, now)
            meta = dict(resource.meta)
            meta["last_failure_reason"] = reason
            if fail_count >= threshold:
                target = ResourceState.BANNED
                meta["banned_at"] = now
                meta["banned_reason"] = reason
                next_available_at = None
            else:
                target = ResourceState.QUARANTINED
                duration = policy.duration(fail_count)
                meta["quarantine_until"] = now + duration
                next_available_at = now + duration
            patch = ResourcePatch(
                state=target,
                health=health,
                next_available_at=next_available_at,
                meta_patch=meta,
                site_failures=site_failures,
            )
            ok = await self._apply_state(
                rid,
                patch,
                expect_state=resource.state,
                reason=f"validation failed: {reason}",
            )
            if ok:
                event = "banned" if target is ResourceState.BANNED else "quarantined"
                self.purser.bus.publish(event, self.name, rid=rid, data={"reason": reason})
                if target is ResourceState.BANNED:
                    self.purser.audit.record(
                        operator=operator,
                        action="ban",
                        pool=self.name,
                        rid=rid,
                        reason=reason,
                    )
                return

    async def _on_ban_verdict(self, resource: Resource, req: AcquireRequest) -> None:
        site_failures = dict(resource.site_failures)
        if req.target:
            self._record_site_failure(site_failures, req.target, _now())
        await self._on_validation_fail(
            resource.id,
            f"request-level BAN for {req.target or 'target'}",
            from_states={resource.state},
            site_failures=site_failures if req.target else None,
        )

    async def _on_expiration(self, resource: Resource) -> None:
        """S18: absolute expiry flips the resource off."""

        policy = self.cfg.expire_policy
        target = (
            ResourceState.QUOTA_EXHAUSTED
            if policy == "quota"
            else ResourceState.SUSPENDED
        )
        patch = ResourcePatch(
            state=target,
            meta_patch={**resource.meta, "expired_at": _now()},
        )
        await self._apply_state(resource.id, patch, expect_state=resource.state, reason="expired")
        self.purser.bus.publish("expired", self.name, rid=resource.id)

    # -- operations --------------------------------------------------------

    async def _apply_state(
        self,
        rid: str,
        patch: ResourcePatch,
        *,
        expect_state: ResourceState | None = None,
        attempts: int = 3,
        reason: str = "op",
    ) -> bool:
        """Optimistic CAS plus event/hook fan-out on successful transitions."""

        for _ in range(attempts):
            resource = await self.storage.get(self.name, rid)
            if resource is None:
                return False
            old_state = resource.state
            ok = await self.storage.set_state(
                self.name,
                rid,
                resource.version,
                expect_state=expect_state or resource.state,
                patch=patch,
            )
            if ok:
                new_state = patch.state if patch.state is not None else old_state
                if new_state != old_state:
                    await self._notify_state_changed(rid, old_state, new_state, reason)
                return True
            if expect_state is not None:
                return False
        return False

    async def _notify_state_changed(
        self, rid: str, old: ResourceState, new: ResourceState, reason: str
    ) -> None:
        """Fan out one state transition to the bus and the plugin observer."""

        self.purser.bus.publish(
            "state_changed",
            self.name,
            rid=rid,
            data={"old": old.value, "new": new.value, "reason": reason},
        )
        try:
            updated = await self.storage.get(self.name, rid)
            if updated is not None:
                await self.adapter.on_state_changed(updated, old, new, reason)
        except Exception as exc:
            self._guard_error("on_state_changed", exc)

    async def _cas(
        self,
        rid: str,
        patch: ResourcePatch,
        *,
        expect_state: ResourceState | None = None,
        attempts: int = 3,
    ) -> bool:
        return await self._apply_state(
            rid, patch, expect_state=expect_state, attempts=attempts, reason="op"
        )

    async def ban(self, rid: str, *, reason: str, operator: str = "system") -> None:
        resource = await self.require(rid)
        if resource.state in (ResourceState.BANNED, ResourceState.ARCHIVED):
            raise BadState(f"resource {rid} is already {resource.state.value}")
        # Revoke every live lease first: forcing BANNED while slots are busy
        # would leave the state machine in a state a later release cannot leave.
        while True:
            leases = await self._coordination.leases_for(self.name, rid)
            if not leases:
                resource = await self.require(rid)
                if resource.concurrency.used_slots > 0:
                    await self.storage.reset_usage(self.name, rid)
                break
            lease = leases[0]
            await self._coordination.unregister_lease(self.name, lease.token)
            self._leases.unregister(lease.token)
            if lease.consumer:
                self._consumer_holds.get(lease.consumer, set()).discard(rid)
            await self.storage.transition_release(self.name, rid, lease.slots)
        resource = await self.require(rid)
        patch = ResourcePatch(
            state=ResourceState.BANNED,
            meta_patch={**resource.meta, "banned_at": _now(), "banned_reason": reason},
        )
        await self._cas(rid, patch)
        self.purser.audit.record(
            operator=operator, action="ban", pool=self.name, rid=rid, reason=reason
        )
        self.purser.bus.publish("banned", self.name, rid=rid, data={"reason": reason})

    async def unban(
        self, rid: str, *, operator: str = "system", reason: str = "manual unban", revalidate: bool = True
    ) -> None:
        resource = await self.require(rid)
        if resource.state is not ResourceState.BANNED:
            raise BadState(f"resource {rid} is not banned")
        patch = ResourcePatch(
            state=ResourceState.VALIDATING,
            meta_patch={
                **resource.meta,
                "unbanned_at": _now(),
                "unban_reason": reason,
            },
        )
        await self._apply_state(rid, patch, expect_state=ResourceState.BANNED, reason="unban")
        self.purser.audit.record(
            operator=operator, action="unban", pool=self.name, rid=rid, reason=reason
        )
        self.purser.bus.publish("unbanned", self.name, rid=rid, data={"reason": reason})
        if revalidate:
            await self.validate_now(rid, operator=operator)

    async def suspend(self, rid: str, *, operator: str = "system", reason: str) -> None:
        resource = await self.require(rid)
        if resource.state is not ResourceState.IDLE:
            raise BadState(f"only IDLE resources can be suspended, got {resource.state.value}")
        await self._cas(
            rid,
            ResourcePatch(
                state=ResourceState.SUSPENDED,
                meta_patch={**resource.meta, "suspended_at": _now(), "suspend_reason": reason},
            ),
        )
        self.purser.audit.record(
            operator=operator, action="suspend", pool=self.name, rid=rid, reason=reason
        )

    async def resume(
        self, rid: str, *, operator: str = "system", reason: str = "manual resume", revalidate: bool = False
    ) -> None:
        resource = await self.require(rid)
        if resource.state is not ResourceState.SUSPENDED:
            raise BadState(f"resource {rid} is not suspended")
        target = ResourceState.VALIDATING if revalidate else ResourceState.IDLE
        await self._apply_state(
            rid,
            ResourcePatch(
                state=target,
                meta_patch={**resource.meta, "resumed_at": _now(), "resume_reason": reason},
            ),
            expect_state=ResourceState.SUSPENDED,
            reason="resume",
        )
        self.purser.audit.record(
            operator=operator, action="resume", pool=self.name, rid=rid, reason=reason
        )
        await self._serve_waiters()

    async def top_up(self, rid: str, *, operator: str = "system", amount: int | None = None, reason: str = "manual top_up") -> None:
        resource = await self.require(rid)
        quota = resource.quota
        if quota is None and resource.usage_quota is None:
            raise BadState(f"resource {rid} has no quota to top up")
        if amount is not None and amount <= 0:
            raise ConfigError("amount must be > 0")
        if quota is not None:
            quota.top_up(amount)
        meta = dict(resource.meta)
        meta["top_ups"] = meta.get("top_ups", 0) + 1
        new_used_count = resource.used_count
        if amount is not None and resource.usage_quota is not None:
            new_used_count = max(0, resource.used_count - amount)
        patch = ResourcePatch(
            state=ResourceState.IDLE if resource.state is ResourceState.QUOTA_EXHAUSTED else resource.state,
            quota=quota,
            used_count=new_used_count,
            meta_patch=meta,
        )
        await self._cas(rid, patch)
        self.purser.audit.record(
            operator=operator, action="top_up", pool=self.name, rid=rid, reason=reason
        )
        self.purser.bus.publish("quota_reset", self.name, rid=rid)
        await self._serve_waiters()

    async def drain(self, rid: str, *, operator: str = "system", reason: str) -> None:
        """Retire a resource: no new leases, archived once leases return (O10)."""

        resource = await self.require(rid)
        if resource.state is ResourceState.DRAINING:
            return
        await self._cas(
            rid,
            ResourcePatch(
                state=ResourceState.DRAINING,
                meta_patch={**resource.meta, "drained_at": _now(), "drain_reason": reason},
            ),
        )
        self.purser.audit.record(
            operator=operator, action="drain", pool=self.name, rid=rid, reason=reason
        )

    async def cancel_drain(
        self, rid: str, *, operator: str = "system", reason: str = "cancel drain"
    ) -> None:
        """Undo a drain before the resource is archived (back online)."""

        resource = await self.require(rid)
        if resource.state is not ResourceState.DRAINING:
            raise BadState(f"resource {rid} is not draining")
        target = StateMachine.trigger(
            resource.state,
            Event.UNDRAIN,
            ctx=StateContext(
                used_slots=resource.concurrency.used_slots,
                slots=0,
                max_slots=resource.concurrency.max_slots,
            ),
        )
        meta = {
            key: value
            for key, value in resource.meta.items()
            if key not in ("drained_at", "drain_reason")
        }
        meta["drain_cancelled_at"] = _now()
        await self._apply_state(
            rid,
            ResourcePatch(state=target, meta_patch=meta),
            expect_state=ResourceState.DRAINING,
            reason="cancel_drain",
        )
        self.purser.audit.record(
            operator=operator,
            action="cancel_drain",
            pool=self.name,
            rid=rid,
            reason=reason,
            detail={"target": target.value},
        )
        await self._serve_waiters()

    async def restore(self, rid: str, *, operator: str = "system", reason: str = "manual restore") -> None:
        resource = await self.require(rid)
        if resource.state is not ResourceState.ARCHIVED:
            raise BadState(f"resource {rid} is not archived")
        await self._apply_state(
            rid,
            ResourcePatch(
                state=ResourceState.VALIDATING,
                meta_patch={**resource.meta, "restored_at": _now(), "restore_reason": reason},
            ),
            expect_state=ResourceState.ARCHIVED,
            reason="restore",
        )
        self.purser.audit.record(
            operator=operator, action="restore", pool=self.name, rid=rid, reason=reason
        )
        await self.validate_now(rid, operator=operator)

    async def admin_set_state(
        self, rid: str, *, target: ResourceState, operator: str = "system", reason: str
    ) -> None:
        if target not in ADMIN_ALLOWED:
            raise BadState(f"admin_set_state disallows {target.value}") from None
        resource = await self.require(rid)
        if resource.concurrency.used_slots > 0:
            raise BadState(f"cannot force-set state while {rid} is busy")
        StateMachine.trigger(
            resource.state,
            Event.ADMIN_SET_STATE,
            ctx=StateContext(target_state=target),
        )
        await self._apply_state(
            rid,
            ResourcePatch(
                state=target,
                meta_patch={**resource.meta, "admin_set_at": _now(), "admin_reason": reason, "admin_operator": operator},
            ),
            reason="admin_set_state",
        )
        self.purser.audit.record(
            operator=operator, action="admin_set_state", pool=self.name, rid=rid, reason=reason,
            detail={"target": target.value},
        )
        await self._serve_waiters()

    async def force_revoke(
        self, rid: str, *, operator: str = "system", reason: str, token: str | None = None
    ) -> Lease:
        """Administrative kick (O1): returns and releases the lease."""

        leases = await self._coordination.leases_for(self.name, rid)
        if not leases:
            raise BadState(f"no active lease on {rid}")
        lease = (
            next((item for item in leases if item.token == token), None)
            if token
            else next(iter(leases))
        )
        if lease is None:
            raise LeaseConflict(f"lease token {token} not found on {rid}")
        removed = await self._coordination.unregister_lease(self.name, lease.token)
        if removed is None:
            # A concurrent release/reaper removed it first and owns the storage
            # decrement.  Revoking again would return the same slot twice.
            raise LeaseConflict(
                f"lease token {lease.token} is no longer active on {rid}"
            )
        self._leases.unregister(lease.token)
        result = await self.storage.transition_release(self.name, rid, lease.slots)
        if not result.ok:
            raise LeaseConflict(f"failed to revoke {rid}:{lease.token}")
        if lease.consumer:
            self._consumer_holds.get(lease.consumer, set()).discard(rid)
            await self._kick_cooldown(rid, lease.consumer)
        try:
            resource = await self.storage.get(self.name, rid)
            if resource is not None:
                await self.adapter.on_revoke(resource, lease, reason)
        except Exception as exc:
            self._guard_error("on_revoke", exc)
        self.purser.audit.record(
            operator=operator, action="force_revoke", pool=self.name, rid=rid, reason=reason,
            detail={"token": lease.token, "consumer": lease.consumer},
        )
        self.purser.bus.publish(
            "lease_revoked", self.name, rid=rid,
            data={"reason": reason},
            trace_id=lease.trace_id,
        )
        self.purser.usage.record(
            ts=_now(),
            pool=self.name,
            kind=self.cfg.kind,
            rid=rid,
            consumer=lease.consumer,
            target=lease.target,
            outcome=UsageOutcome.PREEMPTED.value,
            used=0,
            slots=lease.slots,
        )
        await self._serve_waiters()
        return lease

    async def _kick_cooldown(self, rid: str, consumer: str) -> None:
        """F5: the kicked consumer cools down on this resource."""

        resource = await self.storage.get(self.name, rid)
        if resource is None:
            return
        meta = dict(resource.meta)
        meta[f"cooldown:{consumer}"] = _now() + 10.0
        await self._apply_state(
            rid, ResourcePatch(meta_patch=meta), expect_state=resource.state, reason="kick_cooldown"
        )

    async def update_resource(
        self,
        rid: str,
        *,
        payload: dict[str, Any] | None = None,
        tags: frozenset[str] | None = None,
        max_slots: int | None = None,
        usage_quota: Any = _MISSING,
        quota_limit: Any = _MISSING,
        expires_at: Any = _MISSING,
        next_available_at: Any = _MISSING,
        mutex_group: Any = _MISSING,
        schedule_window: Any = _MISSING,
        operator: str = "system",
        reason: str,
    ) -> Resource:
        """O2: mutate metadata/payload/slots; busy resources are rejected.

        ``usage_quota`` / ``quota_limit`` / ``expires_at`` /
        ``next_available_at`` / ``mutex_group`` / ``schedule_window`` accept
        ``None`` to clear the value and ``_MISSING`` (omitted) to keep it.
        """

        resource = await self.require(rid)
        if resource.concurrency.used_slots > 0:
            raise BadState("cannot update a resource with active leases")
        if max_slots is not None and max_slots <= 0:
            raise ConfigError("max_slots must be > 0")
        clear: set[str] = set()
        extra: dict[str, Any] = {}
        if usage_quota is not _MISSING:
            if usage_quota is None:
                clear.add("usage_quota")
            else:
                extra["usage_quota"] = _optional_int(usage_quota, "usage_quota")
        if quota_limit is not _MISSING:
            if quota_limit is None:
                clear.add("quota")
            else:
                limit = _optional_int(quota_limit, "quota_limit", minimum=1)
                if limit is not None:
                    extra["quota"] = Quota(limit=limit)
        if expires_at is not _MISSING:
            if expires_at is None:
                clear.add("expires_at")
            else:
                if isinstance(expires_at, bool) or not isinstance(expires_at, (int, float)):
                    raise ConfigError("expires_at must be an epoch number or null")
                extra["expires_at"] = float(expires_at)
        if next_available_at is not _MISSING:
            if next_available_at is None:
                clear.add("next_available_at")
            else:
                if isinstance(next_available_at, bool) or not isinstance(
                    next_available_at, (int, float)
                ):
                    raise ConfigError("next_available_at must be an epoch number or null")
                extra["next_available_at"] = float(next_available_at)
        if mutex_group is not _MISSING:
            if mutex_group in (None, ""):
                clear.add("mutex_group")
            else:
                extra["mutex_group"] = str(mutex_group)
        if schedule_window is not _MISSING:
            if schedule_window is None:
                clear.add("schedule_window")
            else:
                extra["schedule_window"] = _schedule_window(schedule_window)
        patch = ResourcePatch(
            payload=payload if payload is not None else dict(resource.payload),
            tags=tags if tags is not None else resource.tags,
            concurrency=(
                Concurrency(max_slots=max_slots)
                if max_slots is not None
                else resource.concurrency
            ),
            meta_patch={**resource.meta, "updated_by": operator, "update_reason": reason},
            clear_fields=frozenset(clear),
            **extra,
        )
        await self._cas(rid, patch)
        self.purser.audit.record(
            operator=operator, action="update_resource", pool=self.name, rid=rid, reason=reason
        )
        updated = await self.require(rid)
        return updated

    async def switch_resource(
        self,
        rid: str,
        *,
        operator: str = "system",
        reason: str = "manual switch",
    ) -> Resource | None:
        """Type-driven switch (reserved extension point).

        Calls ``ResourceAdapter.switch`` and persists whatever it returns: a
        same-id result (typically the mutated old resource) updates the row in
        place; a new-id result is imported and the old row is archived.
        ``None`` leaves the resource untouched and returns ``None``.
        """

        resource = await self.require(rid)
        if resource.concurrency.used_slots > 0:
            raise BadState("cannot switch a resource with active leases")
        if resource.state in (
            ResourceState.ARCHIVED,
            ResourceState.DRAINING,
            ResourceState.BANNED,
            ResourceState.LOST,
        ):
            raise BadState(f"cannot switch {rid} in state {resource.state.value}")
        replacement = await self.adapter.switch(resource)
        if replacement is None:
            return None
        replacement = self.adapter.adopt(replacement)
        now = _now()
        if replacement.id == rid:
            ok = await self._apply_state(
                rid,
                ResourcePatch(
                    payload=dict(replacement.payload),
                    tags=replacement.tags,
                    meta_patch={
                        **resource.meta,
                        **replacement.meta,
                        "switched_at": now,
                        "updated_at": now,
                    },
                ),
                expect_state=resource.state,
                reason="switch",
            )
            if not ok:
                raise VersionConflict(f"switch lost the race on {rid}")
            new_id = rid
        else:
            replacement.version = 0
            replacement.meta = {
                **replacement.meta,
                "switched_from": rid,
                "switched_at": now,
                "created_at": now,
                "updated_at": now,
            }
            try:
                await self.storage.put(self.name, replacement, expected_version=0)
            except VersionConflict:
                raise VersionConflict(
                    f"switch target {replacement.id!r} already exists"
                ) from None
            await self._apply_state(
                rid,
                ResourcePatch(
                    state=ResourceState.ARCHIVED,
                    meta_patch={
                        **resource.meta,
                        "switched_to": replacement.id,
                        "switched_at": now,
                    },
                ),
                expect_state=resource.state,
                reason="switch",
            )
            new_id = replacement.id
        self.purser.audit.record(
            operator=operator,
            action="switch",
            pool=self.name,
            rid=rid,
            reason=reason,
            detail={"old_id": rid, "new_id": new_id},
        )
        self.purser.bus.publish(
            "resource_switched",
            self.name,
            rid=new_id,
            data={"old_id": rid, "new_id": new_id, "reason": reason},
        )
        return await self.require(new_id)

    async def move_resource(
        self, rid: str, target_pool: ResourcePool, *, operator: str = "system", reason: str
    ) -> Resource:
        """O3: cross-pool migration; busy resources are rejected."""

        resource = await self.require(rid)
        if resource.concurrency.used_slots > 0:
            raise BadState("cannot move a resource with active leases")
        if target_pool.cfg.kind != self.cfg.kind:
            raise BadState(
                f"cannot move {self.cfg.kind} resource into "
                f"{target_pool.cfg.kind} pool {target_pool.name!r}"
            )
        existing = await target_pool.storage.get(target_pool.name, rid)
        if existing is not None:
            raise BadState(f"target pool {target_pool.name} already contains {rid}")
        await target_pool.storage.put(target_pool.name, resource)
        await self.storage.delete(self.name, rid)
        self.purser.audit.record(
            operator=operator, action="move_resource", pool=self.name, rid=rid, reason=reason,
            detail={"target": target_pool.name},
        )
        return resource

    async def recall_batch(
        self,
        batch_id: str,
        *,
        action: str = "requalify",
        operator: str = "system",
        reason: str,
    ) -> dict[str, Any]:
        """B1-lite: requalify / isolate / retire every resource of a batch."""

        if not isinstance(batch_id, str) or not batch_id:
            raise ConfigError("batch_id must be a non-empty string")
        if action not in ("requalify", "isolate", "retire"):
            raise ConfigError(f"unknown recall action {action!r}")
        report: dict[str, Any] = {
            "action": action,
            "batch_id": batch_id,
            "processed": 0,
            "errors": [],
        }
        async for resource in self.storage.scan(self.name):
            if resource.meta.get("batch_id") != batch_id:
                continue
            try:
                if action == "requalify":
                    await self.validate_now(resource.id, operator=operator)
                elif action == "isolate":
                    await self._on_validation_fail(
                        resource.id, reason, operator=operator,
                        from_states={resource.state},
                    )
                elif action == "retire":
                    await self.drain(resource.id, operator=operator, reason=reason)
                report["processed"] += 1
            except Exception as exc:
                report["errors"].append({"rid": resource.id, "error": str(exc)})
        return report

    # -- periodic maintenance ---------------------------------------------

    async def run_maintenance(self, name: str) -> dict[str, int]:
        """Execute one of the maintenance sweeps (5.9 / O19 / X3)."""

        runner = {
            "quota_reset": self._maintenance_quota_reset,
            "periodic_unban": self._maintenance_periodic_unban,
            "lost_gc": self._maintenance_lost,
            "validating_gc": self._maintenance_validating,
            "quarantine_gc": self._maintenance_quarantine,
            "archive_gc": self._maintenance_archive,
            "expire_gc": self._maintenance_expire,
        }.get(name)
        if runner is None:
            raise ConfigError(f"unknown maintenance task {name!r}")
        return await runner()

    async def _maintenance_quota_reset(self) -> dict[str, int]:
        moved = 0
        now = _now()
        async for resource in self.storage.scan(self.name, states={ResourceState.QUOTA_EXHAUSTED}):
            quota = resource.quota
            if quota is not None and not quota.reset_daily(timezone=self.cfg.timezone):
                continue
            if (
                self.cfg.quota_cooldown > 0
                and resource.next_available_at is not None
                and now < resource.next_available_at
            ):
                continue  # still cooling down
            patch = ResourcePatch(
                state=ResourceState.IDLE,
                quota=quota,
                used_count=0,
                clear_fields=frozenset({"next_available_at"}),
                meta_patch={**resource.meta, "quota_reset_at": _now()},
            )
            if await self._cas(resource.id, patch):
                moved += 1
                self.purser.bus.publish("quota_reset", self.name, rid=resource.id)
        await self._serve_waiters()
        return {"quota_reset": moved}

    async def _maintenance_periodic_unban(self) -> dict[str, int]:
        moved = 0
        if self.cfg.unban_mode == "manual_only":
            return {"periodic_unban": 0}
        now = _now()
        async for resource in self.storage.scan(self.name, states={ResourceState.BANNED}):
            banned_at = float(resource.meta.get("banned_at") or 0.0)
            if now - banned_at >= self.cfg.unban_after:
                await self.unban(resource.id, operator="system", reason="auto unban after ttl")
                moved += 1
        return {"periodic_unban": moved}

    async def _maintenance_lost(self) -> dict[str, int]:
        moved = 0
        async for resource in self.storage.scan(self.name, states={ResourceState.LOST}):
            if _now() - float(resource.meta.get("lost_at") or _now()) >= self.cfg.lost_retention:
                if await self._cas(
                    resource.id,
                    ResourcePatch(
                        state=ResourceState.ARCHIVED,
                        meta_patch={**resource.meta, "archived_at": _now(), "archived_reason": "lost_gc"},
                    ),
                ):
                    moved += 1
        return {"lost_gc": moved}

    async def _maintenance_validating(self) -> dict[str, int]:
        moved = 0
        async for resource in self.storage.scan(self.name, states={ResourceState.VALIDATING}):
            started = float(resource.meta.get("validate_started_at") or _now())
            if _now() - started >= self.cfg.validate_timeout:
                await self._on_validation_fail(
                    resource.id, "validation timed out", from_states={ResourceState.VALIDATING}
                )
                moved += 1
        return {"validating_gc": moved}

    async def _maintenance_quarantine(self) -> dict[str, int]:
        moved = 0
        now = _now()
        async for resource in self.storage.scan(self.name, states={ResourceState.QUARANTINED}):
            until = float(resource.meta.get("quarantine_until") or resource.next_available_at or 0.0)
            if until > now:
                continue
            policy = self.cfg.requalify_on_release
            target = ResourceState.VALIDATING if policy else ResourceState.IDLE
            if await self._cas(
                resource.id,
                ResourcePatch(
                    state=target,
                    meta_patch={**resource.meta, "quarantine_released_at": now},
                ),
            ):
                moved += 1
                self.purser.bus.publish("quarantine_expired", self.name, rid=resource.id)
        await self._serve_waiters()
        return {"quarantine_gc": moved}

    async def _maintenance_archive(self) -> dict[str, int]:
        deleted = 0
        async for resource in self.storage.scan(self.name, states={ResourceState.ARCHIVED}):
            archived_at = float(resource.meta.get("archived_at") or _now())
            if _now() - archived_at >= self.cfg.archive_retention:
                resource.payload = {"erased": True}  # secure erase before delete (Y3)
                await self.storage.delete(self.name, resource.id)
                deleted += 1
                self.purser.audit.record(
                    operator="system", action="archive_maintenance_delete",
                    pool=self.name, rid=resource.id, reason="archive retention reached",
                )
        return {"archive_gc": deleted}

    async def _maintenance_expire(self) -> dict[str, int]:
        """Archive resources that expired long ago (opt-in via config).

        Only resources flipped off by ``expires_at`` (they carry
        ``meta["expired_at"]``) are considered, so a manually suspended
        resource is never touched.  Disabled by default: ``cfg.expired_retention``
        must be set to an age in seconds.
        """

        retention = self.cfg.expired_retention
        if retention is None:
            return {"expire_gc": 0}
        moved = 0
        now = _now()
        async for resource in self.storage.scan(
            self.name, states={ResourceState.SUSPENDED}
        ):
            expired_at = resource.meta.get("expired_at")
            if expired_at is None:
                continue
            if resource.concurrency.used_slots > 0:
                continue
            if now - float(expired_at) < retention:
                continue
            if await self._cas(
                resource.id,
                ResourcePatch(
                    state=ResourceState.ARCHIVED,
                    meta_patch={
                        **resource.meta,
                        "archived_at": now,
                        "archived_reason": "expire_gc",
                    },
                ),
            ):
                moved += 1
                self.purser.audit.record(
                    operator="system",
                    action="expire_gc_archive",
                    pool=self.name,
                    rid=resource.id,
                    reason="expired resource retention reached",
                )
        await self._serve_waiters()
        return {"expire_gc": moved}

    # -- misc --------------------------------------------------------------

    async def acquire_many(
        self,
        n: int,
        *,
        req: AcquireRequest | None = None,
        mode: str = "atomic",
        n_min: int | None = None,
        timeout: float | None = None,
    ) -> BatchLease:
        """O13: acquire ``n`` leases; ``atomic`` rolls back on partial failure,
        ``best_effort`` returns what it got (below ``n_min`` it raises)."""

        await self._ensure_started("acquire_many")
        if n <= 0:
            raise ConfigError("n must be positive")
        basis = req or AcquireRequest()
        if not isinstance(basis.slots, int) or basis.slots <= 0:
            raise ConfigError("slots must be a positive integer")
        timeout = timeout if timeout is not None else basis.timeout
        basis.timeout = timeout
        leased: list[Lease] = []
        try:
            for _ in range(n):
                try:
                    leased.append(await self.acquire(basis))
                except (PoolExhausted, PoolEmpty):
                    if mode != "best_effort":
                        raise
                    break
            if mode == "best_effort" and len(leased) < (n_min or n):
                raise PoolExhausted(
                    f"only got {len(leased)}/{n} (n_min={n_min or n})"
                )
            return BatchLease(leased)
        except Exception:
            for lease in reversed(leased):
                try:
                    await self.release(lease)
                except Exception:
                    pass
            raise

    async def renew_manual(self, lease: Lease) -> bool:
        """Manual renewal for leases created outside the context manager."""

        return await self._renew(lease)


class Purser:
    """Top-level entry point: owns pools, plugin registry, events, audit."""

    def __init__(
        self,
        *,
        maintenance_enabled: bool = True,
        event_queue_size: int = 10_000,
        usage_ledger_size: int = 50_000,
        storage: str = "memory",
        coordination: str = "memory",
        mysql_url: str | None = None,
        mysql_connection: Any = None,
        redis_url: str | None = None,
        redis_client: Any = None,
        sqlite_path: str | None = None,
        audit_path: str | None = None,
        secret_key: bytes | None = None,
        secret_key_id: str = "1",
        previous_secret_keys: dict[str, bytes | str] | None = None,
        use_globals: bool = True,
        bundle_mode: str = "rollback",
        bundle_fairness: BundleFairness | None = None,
        reservation_ttl: float = 30.0,
        bundle_gc_interval: float = 30.0,
        fairness_ttl: float = 300.0,
    ) -> None:
        if bundle_mode not in ("rollback", "two_phase"):
            raise ConfigError(
                "bundle_mode must be 'rollback' or 'two_phase'"
            )
        if reservation_ttl <= 0 or bundle_gc_interval <= 0 or fairness_ttl <= 0:
            raise ConfigError(
                "reservation_ttl / bundle_gc_interval / fairness_ttl must be > 0"
            )
        self._isolation = IsolationGuard(self)
        self._isolation.claim_many(
            [
                redis_client_identity(redis_client)
                if redis_client is not None
                else redis_identity(redis_url)
            ]
        )
        self.adapters = ResourceAdapterRegistry()
        self.bus = EventBus(maxsize=event_queue_size)
        self.usage = UsageLedger(capacity=usage_ledger_size)
        self.audit = AuditLog(sqlite_path=audit_path)
        self.metrics = Metrics()
        self.idempotency = (
            RedisIdempotencyStore(url=redis_url, client=redis_client)
            if redis_url or redis_client is not None
            else IdempotencyStore()
        )
        self.probe_guard = ProbeGuard()
        self.maintenance_enabled = maintenance_enabled
        self.bundle_mode = bundle_mode
        self.bundle_fairness = bundle_fairness
        self.reservation_ttl = reservation_ttl
        self.bundle_gc_interval = bundle_gc_interval
        self.fairness_ttl = fairness_ttl
        self.storage_kind = storage
        self.coordination_kind = coordination
        self.mysql_url = mysql_url
        self.mysql_connection = mysql_connection
        self.redis_url = redis_url
        self.redis_client = redis_client
        self.sqlite_path = sqlite_path
        self.audit_path = audit_path
        self.secret_key = secret_key
        key = secret_key
        if key is None and sqlite_path:
            key = load_or_create_key(sqlite_path)
        self._codec = SecretCodec(
            key,
            key_id=secret_key_id,
            previous_keys=dict(previous_secret_keys or {}),
        )
        self._pools: dict[str, ResourcePool] = {}
        self._bundles: dict[str, BundleLease] = {}
        self._bundle_store: BundleStore | None = None
        self._fair_heap: list[tuple[int, int, _FairWaiter]] = []
        self._fair_seq = 0
        self._bundle_gc_task: asyncio.Task[Any] | None = None
        self._running = False

        from purser.adapters import AccountAdapter, ProxyAdapter

        self.adapters.register(ProxyAdapter(probe_guard=self.probe_guard))
        self.adapters.register(AccountAdapter())
        builtin_kinds = {ProxyAdapter.kind, AccountAdapter.kind}
        if use_globals:
            for kind, build in default_adapter_factories().items():
                if kind in builtin_kinds or kind in self.adapters.names():
                    raise ConfigError(
                        f"default resource adapter {kind!r} conflicts with a builtin adapter"
                    )
                self.adapters.register(build())

    def __getstate__(self) -> Any:
        raise IsolationConflict(PICKLE_MESSAGE)

    def _ensure_process(self) -> None:
        self._isolation.ensure_process()

    # -- plugins / pools ---------------------------------------------------

    def register_adapter(self, adapter: ResourceAdapter) -> None:
        self._ensure_process()
        identity = resource_adapter_identity(adapter)
        self._isolation.claim_many([identity])
        try:
            self.adapters.register(adapter)
        except BaseException:
            self._isolation.release_many([identity[0]])
            raise

    def _pool_isolation_entries(
        self, cfg: PoolConfig, adapter: ResourceAdapter
    ) -> list[Any]:
        """Scheduling-plane identities this pool would actually use."""

        entries: list[Any] = []
        if cfg.storage == "sqlite":
            entries.append(sqlite_identity(self.sqlite_path))
        elif cfg.storage == "mysql":
            entries.append(
                mysql_connection_identity(self.mysql_connection)
                if self.mysql_connection is not None
                else mysql_identity(self.mysql_url)
            )
        if cfg.coordination == "redis":
            entries.append(
                redis_client_identity(self.redis_client)
                if self.redis_client is not None
                else redis_identity(self.redis_url)
            )
        entries.append(resource_adapter_identity(adapter))
        return entries

    def _release_pool_claims(self, pool: ResourcePool) -> None:
        keys = pool._isolation_keys
        if keys:
            pool._isolation_keys = []
            self._isolation.release_many(keys)

    def _make_storage(self, cfg: PoolConfig) -> StorageBackend:
        if cfg.storage == "memory":
            return MemoryStorage()
        if cfg.storage == "sqlite":
            if not self.sqlite_path:
                raise ConfigError(
                    "storage='sqlite' requires sqlite_path; refusing to fall "
                    "back to a silent in-memory database"
                )
            return SqliteStorage(
                path=self.sqlite_path,
                codec=self._codec,
                secret_fields=cfg.secret_fields,
            )
        if cfg.storage == "mysql":
            from purser.backends.mysql import MySqlStorage

            if cfg.secret_fields and self._codec.ephemeral:
                raise ConfigError(
                    "storage='mysql' with secret_fields requires an explicit "
                    "secret_key (or PURSER_SECRET_KEY); an ephemeral key would "
                    "make encrypted payloads unreadable by other instances"
                )
            return MySqlStorage(
                url=self.mysql_url,
                connection=self.mysql_connection,
                codec=self._codec,
                secret_fields=cfg.secret_fields,
            )
        raise ConfigError(f"unknown storage {cfg.storage!r}")

    def _make_coordination(self, cfg: PoolConfig) -> Coordination:
        if cfg.coordination == "memory":
            return MemoryCoordination()
        if cfg.coordination == "redis":
            return RedisCoordination(
                url=self.redis_url,
                client=self.redis_client,
            )
        raise ConfigError(f"unknown coordination {cfg.coordination!r}")

    def create_pool(
        self,
        name: str,
        kind: str,
        *,
        config: PoolConfig | dict[str, Any] | None = None,
        **overrides: Any,
    ) -> ResourcePool:
        self._ensure_process()
        if name in self._pools:
            raise PoolExists(f"pool {name!r} already exists")
        adapter = self.adapters.get(kind)
        if isinstance(config, PoolConfig):
            cfg = config
        else:
            defaults = {
                "storage": self.storage_kind,
                "coordination": self.coordination_kind,
            }
            cfg = PoolConfig(kind=kind, **{**defaults, **(config or {})})
        if overrides:
            cfg = cfg.with_overrides(**overrides)
        cfg = apply_adapter_defaults(cfg, adapter)
        entries = self._pool_isolation_entries(cfg, adapter)
        self._isolation.claim_many(entries)
        keys = list(
            dict.fromkeys(entry[0] for entry in entries if entry is not None)
        )
        try:
            storage = self._make_storage(cfg)
            coordination = self._make_coordination(cfg)
            pool = ResourcePool(name, self, adapter, cfg, storage, coordination)
        except BaseException:
            self._isolation.release_many(keys)
            raise
        pool._isolation_keys = keys
        self._pools[name] = pool
        if self._running:
            pool._start_task = asyncio.create_task(pool.start())
        self.audit.record(
            operator="system", action="create_pool", pool=name,
            detail={
                "kind": kind,
                "storage": cfg.storage,
                "coordination": cfg.coordination,
            },
        )
        self.bus.publish("pool_created", name)
        return pool

    def pool(
        self,
        name: str,
        kind: str | None = None,
        *,
        config: PoolConfig | dict[str, Any] | None = None,
        **overrides: Any,
    ) -> ResourcePool:
        """Get an existing pool, or create it (convenience for quick starts)."""

        self._ensure_process()
        existing = self._pools.get(name)
        if existing is not None:
            return existing
        if kind is None:
            raise PoolNotFound(f"pool {name!r} does not exist and no kind given")
        return self.create_pool(name, kind, config=config, **overrides)

    def get_pool(self, name: str) -> ResourcePool | None:
        return self._pools.get(name)

    def require_pool(self, name: str) -> ResourcePool:
        self._ensure_process()
        pool = self._pools.get(name)
        if pool is None:
            raise PoolNotFound(f"pool {name!r} does not exist")
        return pool

    def pool_names(self) -> list[str]:
        return list(self._pools)

    # -- cross-pool bundles (O8) -----------------------------------------

    @staticmethod
    def _coerce_bundle(request: Any) -> BundleRequest:
        req = bundle_request_from_dict(request) if isinstance(request, dict) else request
        if not isinstance(req, BundleRequest):
            raise TypeError("acquire_bundle expects a BundleRequest or dict")
        return req

    def _register_bundle(
        self, req: BundleRequest, acquired: list[tuple[str, Lease]]
    ) -> BundleLease:
        bundle_id = req.bundle_id or f"bundle-{uuid.uuid4().hex[:12]}"
        bundle = BundleLease(
            bundle_id=bundle_id,
            consumer=req.consumer,
            members={name: lease for name, lease in acquired},
        )
        self._bundles[bundle_id] = bundle
        return bundle

    async def _release_bundle_members(self, acquired: list[tuple[str, Lease]]) -> None:
        for pool_name, lease in reversed(acquired):
            pool = self._pools.get(pool_name)
            if pool is None:
                continue
            try:
                await pool.release(lease)
            except Exception:
                continue

    async def _order_bundle_slots(
        self, req: BundleRequest, *, deterministic: bool = False
    ) -> list[BundleSlot]:
        slots = list(req.slots)
        if deterministic:
            # Global deterministic order across instances: no ABBA cycle.
            ordered_slots = sorted(
                slots,
                key=lambda slot: (slot.pool, slot.resource_id or ""),
            )
        elif req.order == "scarce_first":
            idle: dict[str, int] = {}
            for slot in slots:
                pool = self.require_pool(slot.pool)
                counts = await pool.counts()
                idle[slot.pool] = counts.get(ResourceState.IDLE, 0)
            ordered_slots = sorted(slots, key=lambda slot: idle[slot.pool])
        else:
            return slots
        ordered: list[BundleSlot] = []
        placed: set[str] = set()
        for slot in ordered_slots:
            # A match slot depends on its anchor: emit the whole anchor
            # chain first even when the anchor pool is not the scarcer one.
            chain: list[BundleSlot] = []
            seen: set[str] = set()
            cur = slot
            while (
                cur.match is not None
                and cur.match.anchor_pool not in placed
                and cur.match.anchor_pool not in seen
            ):
                seen.add(cur.match.anchor_pool)
                anchor = next(
                    (candidate for candidate in slots if candidate.pool == cur.match.anchor_pool),
                    None,
                )
                if anchor is None or anchor.pool in placed:
                    break
                chain.append(anchor)
                cur = anchor
            for ancestor in reversed(chain):
                if ancestor.pool not in placed:
                    ordered.append(ancestor)
                    placed.add(ancestor.pool)
            if slot.pool not in placed:
                ordered.append(slot)
                placed.add(slot.pool)
        return ordered

    async def _store(self) -> BundleStore:
        if self._bundle_store is None:
            if self.coordination_kind == "redis":
                self._bundle_store = RedisBundleStore(
                    client=self.redis_client,
                    url=self.redis_url,
                )
            else:
                self._bundle_store = MemoryBundleStore()
        return self._bundle_store

    async def _fairness_counts(
        self, store: BundleStore
    ) -> tuple[int, dict[str, int], dict[str, int]]:
        total = 0
        by_consumer: dict[str, int] = {}
        by_pool: dict[str, int] = {}
        for res in await store.active(_now()):
            total += 1
            if res.consumer:
                by_consumer[res.consumer] = by_consumer.get(res.consumer, 0) + 1
            for pool in res.pools:
                by_pool[pool] = by_pool.get(pool, 0) + 1
        return total, by_consumer, by_pool

    def _fairness_reject(
        self,
        req: BundleRequest,
        total: int,
        by_consumer: dict[str, int],
        by_pool: dict[str, int],
        idle: dict[str, int],
    ) -> str | None:
        fairness = self.bundle_fairness
        if fairness is None:
            return None
        if fairness.max_total is not None and total + 1 > fairness.max_total:
            return "max_total"
        if (
            fairness.max_per_consumer is not None
            and req.consumer
            and by_consumer.get(req.consumer, 0) + 1 > fairness.max_per_consumer
        ):
            return "max_per_consumer"
        for slot in req.slots:
            reservable = max(
                0, idle.get(slot.pool, 0) - fairness.per_pool_headroom
            )
            if by_pool.get(slot.pool, 0) + 1 > reservable:
                return f"per_pool_headroom:{slot.pool}"
        return None

    def _fairness_release_notify(self) -> None:
        """Wake the highest-priority waiter; it re-checks admission."""

        while self._fair_heap:
            _neg, _seq, waiter = heapq.heappop(self._fair_heap)
            if waiter.cancelled:
                continue
            waiter.event.set()
            return

    async def _fairness_gate(self, req: BundleRequest, store: BundleStore) -> None:
        fairness = self.bundle_fairness
        if fairness is None:
            return
        loop = asyncio.get_running_loop()
        deadline = (
            loop.time() + fairness.timeout if fairness.timeout is not None else None
        )
        waiter: _FairWaiter | None = None
        try:
            while True:
                total, by_consumer, by_pool = await self._fairness_counts(store)
                idle: dict[str, int] = {}
                for slot in req.slots:
                    if slot.pool in idle:
                        continue
                    counts = await self.require_pool(slot.pool).counts()
                    idle[slot.pool] = counts.get(ResourceState.IDLE, 0)
                reason = self._fairness_reject(
                    req, total, by_consumer, by_pool, idle
                )
                if reason is None:
                    return
                if not fairness.wait:
                    raise BundleQuotaExceeded(
                        f"bundle fairness limit: {reason}"
                    )
                if deadline is not None and loop.time() >= deadline:
                    raise BundleQuotaExceeded(
                        f"bundle fairness limit {reason}: wait timed out"
                    )
                if waiter is None:
                    waiter = _FairWaiter(
                        seq=self._fair_seq,
                        priority=req.priority,
                        event=asyncio.Event(),
                    )
                    self._fair_seq += 1
                    heapq.heappush(
                        self._fair_heap, (-waiter.priority, waiter.seq, waiter)
                    )
                if store.distributed:
                    # No push channel cross-instance: poll the shared store
                    # like pool waiters do, so a release on another instance
                    # wakes us within WAITER_POLL_INTERVAL.
                    remaining = (
                        None if deadline is None else deadline - loop.time()
                    )
                    window = (
                        min(WAITER_POLL_INTERVAL, remaining)
                        if remaining is not None and remaining > 0
                        else WAITER_POLL_INTERVAL
                    )
                    try:
                        await asyncio.wait_for(waiter.event.wait(), window)
                    except asyncio.TimeoutError:
                        waiter.event.clear()
                        continue  # poll tick: re-check shared admission
                    waiter.event.clear()
                    waiter = None  # poke consumed; re-register before waiting
                    continue
                try:
                    await asyncio.wait_for(waiter.event.wait(), fairness.timeout)
                except (asyncio.TimeoutError, TimeoutError) as exc:
                    raise BundleQuotaExceeded(
                        f"bundle fairness limit {reason}: wait timed out"
                    ) from exc
                waiter.event.clear()
                waiter = None
        except asyncio.CancelledError:
            if waiter is not None:
                waiter.cancelled = True
            raise
        finally:
            if waiter is not None:
                waiter.cancelled = True

    async def acquire_bundle(
        self,
        request: BundleRequest | dict[str, Any],
        **overrides: Any,
    ) -> BundleLease:
        """Acquire one lease per bundle slot across pools (O8, opt-in v2).

        Default ``rollback`` mode: scarce/declared order, rollback + jittered
        retry, ``best_effort`` partials.  With ``bundle_mode="two_phase"`` (or
        ``strict=True``) members are assembled in a deterministic global order
        and confirmed atomically on the reservation store.  With
        ``bundle_fairness`` configured, admission is gated by quota and
        per-pool headroom before any member is taken.  All advanced paths are
        off by default.
        """

        req = self._coerce_bundle(request)
        keys = (
            "consumer",
            "mode",
            "order",
            "timeout",
            "bundle_id",
            "max_retries",
            "strict",
            "priority",
        )
        unknown = set(overrides) - set(keys)
        if unknown:
            fields = ", ".join(sorted(unknown))
            raise TypeError(
                f"acquire_bundle() got unexpected keyword argument(s): {fields}"
            )
        if overrides:
            kwargs = {key: value for key, value in overrides.items() if value is not None}
            req = replace(req, **kwargs)
        strict = (
            req.strict if req.strict is not None else self.bundle_mode == "two_phase"
        )
        if strict and req.mode == "best_effort":
            raise ConfigError("strict (two-phase) bundles cannot use best_effort")
        store: BundleStore | None = None
        if strict or self.bundle_fairness is not None:
            store = await self._store()
            if req.bundle_id is None:
                req = replace(req, bundle_id=f"bundle-{await store.next_seq():012d}")
        bundle_id = req.bundle_id or ""
        if req.bundle_id and req.bundle_id in self._bundles:
            existing = self._bundles[req.bundle_id]
            if existing.consumer == req.consumer:
                return existing
            raise ConfigError(f"bundle id {req.bundle_id!r} is already in use")
        pools = tuple(sorted(slot.pool for slot in req.slots))
        marker_ttl = (
            self.fairness_ttl
            if self.bundle_fairness is not None
            else self.reservation_ttl
        )
        try:
            if self.bundle_fairness is not None and store is not None:
                await self._fairness_gate(req, store)
            if store is not None:
                await store.admit(bundle_id, req.consumer, pools, marker_ttl)
            last_error: Exception | None = None
            for attempt in range(req.max_retries + 1):
                slots = await self._order_bundle_slots(
                    req, deterministic=strict
                )
                acquired: list[tuple[str, Lease]] = []
                try:
                    if store is not None:
                        await store.begin_members(bundle_id, self.reservation_ttl)
                    for slot in slots:
                        pool = self.require_pool(slot.pool)
                        areq = (
                            replace(slot.req)
                            if slot.req is not None
                            else AcquireRequest()
                        )
                        consumer = (
                            req.consumer if req.consumer is not None else areq.consumer
                        )
                        areq = replace(areq, consumer=consumer)
                        if slot.resource_id is not None:
                            areq = replace(areq, resource_id=slot.resource_id)
                        if slot.match is not None:
                            anchor = next(
                                (
                                    lease
                                    for name, lease in acquired
                                    if name == slot.match.anchor_pool
                                ),
                                None,
                            )
                            if anchor is None:
                                raise ConfigError(
                                    "bundle match anchor pool "
                                    f"{slot.match.anchor_pool!r} must be acquired "
                                    "before the matched slot"
                                )
                            value = self._bundle_match_value(
                                anchor.resource, slot.match
                            ) if anchor.resource is not None else None
                            if value is None:
                                raise ConfigError(
                                    "bundle match field "
                                    f"{slot.match.from_field!r} missing on "
                                    f"resource {anchor.resource_id!r}"
                                )
                            areq = replace(
                                areq, required_tag=f"{slot.match.to_tag}:{value}"
                            )
                        if req.timeout is not None:
                            areq = replace(areq, timeout=req.timeout)
                        lease = await pool.acquire(areq)
                        acquired.append((slot.pool, lease))
                        if store is not None:
                            await store.add_member(
                                bundle_id,
                                ReservationMember(
                                    pool=slot.pool,
                                    rid=lease.resource_id,
                                    token=lease.token,
                                ),
                            )
                    if store is not None:
                        await store.confirm(bundle_id, marker_ttl)
                    return self._register_bundle(req, acquired)
                except asyncio.CancelledError:
                    await self._release_bundle_members(acquired)
                    raise
                except Exception as exc:
                    await self._release_bundle_members(acquired)
                    last_error = exc
                    if req.mode == "best_effort" and acquired:
                        if store is not None:
                            await store.confirm(bundle_id, marker_ttl)
                        return self._register_bundle(req, acquired)
                    transient = isinstance(
                        exc, (PoolEmpty, PoolExhausted, ResourceBusy)
                    )
                    if attempt < req.max_retries and transient:
                        # Deadlock mitigation: rollback + jittered backoff.
                        await asyncio.sleep(
                            random.uniform(0.05, 0.2) * (attempt + 1)
                        )
                        continue
                    raise BundleAcquireFailed(
                        f"bundle {req.bundle_id or '-'} could not be completed: {exc}"
                    ) from exc
            raise BundleAcquireFailed(
                f"bundle {req.bundle_id or '-'} could not be completed: {last_error}"
            ) from last_error
        except BaseException:
            if store is not None:
                await store.release(bundle_id)
            if self.bundle_fairness is not None:
                self._fairness_release_notify()
            raise

    async def release_bundle(
        self,
        bundle: BundleLease,
        *,
        outcome_by_pool: dict[str, UsageOutcome] | None = None,
        used_by_pool: dict[str, int] | None = None,
    ) -> None:
        """Release every member lease of a bundle (reversed acquisition order)."""

        outcomes = outcome_by_pool or {}
        used = used_by_pool or {}
        for pool_name, lease in reversed(list(bundle.members.items())):
            pool = self._pools.get(pool_name)
            if pool is None:
                continue
            await pool.release(
                lease,
                outcome=outcomes.get(pool_name),
                used=used.get(pool_name),
            )
        self._bundles.pop(bundle.bundle_id, None)
        if self._bundle_store is not None:
            await self._bundle_store.release(bundle.bundle_id)
            if self.bundle_fairness is not None:
                self._fairness_release_notify()

    async def bundle_gc(self) -> dict[str, int]:
        """Keep live bundle markers fresh, then sweep expired reservations."""

        store = self._bundle_store
        if store is None:
            return {"revoked": 0, "pruned": 0, "revived": 0}
        ttl = (
            self.fairness_ttl
            if self.bundle_fairness is not None
            else self.reservation_ttl
        )
        revived = 0
        # Bundles this process still holds must never drop out of the active
        # set, otherwise fairness under-counts long-held combinations.
        for bundle in list(self._bundles.values()):
            await store.refresh(bundle.bundle_id, ttl)
            revived += 1
        revoked = 0
        pruned = 0
        for res in await store.expired(_now()):
            # A committed bundle whose member leases are still alive was
            # assembled by a live (possibly other-instance) process: refresh
            # instead of pruning so fairness keeps counting it.
            if (
                res.status == "committed"
                and res.members
                and await self._bundle_members_alive(res)
            ):
                await store.refresh(res.bundle_id, ttl)
                revived += 1
                continue
            if res.status == "assembling":
                for member in res.members.values():
                    pool = self._pools.get(member.pool)
                    if pool is None:
                        continue
                    try:
                        await pool.force_revoke(
                            member.rid,
                            operator="system",
                            reason="bundle_gc",
                            token=member.token,
                        )
                        revoked += 1
                    except Exception:
                        continue
            await store.release(res.bundle_id)
            pruned += 1
        if self.bundle_fairness is not None:
            self._fairness_release_notify()
        return {"revoked": revoked, "pruned": pruned, "revived": revived}

    async def _bundle_members_alive(self, res: Any) -> bool:
        for member in res.members.values():
            pool = self._pools.get(member.pool)
            if pool is None:
                return False
            leases = await pool.leases(member.rid)
            if not any(lease.token == member.token for lease in leases):
                return False
        return True

    async def _bundle_gc_loop(self) -> None:
        while True:
            await asyncio.sleep(self.bundle_gc_interval)
            try:
                await self.bundle_gc()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("bundle_gc sweep failed")

    @contextlib.asynccontextmanager
    async def bundle(
        self,
        request: BundleRequest | dict[str, Any],
        **overrides: Any,
    ) -> AsyncIterator[BundleLease]:
        """Atomic bundle acquire/release context manager (O8)."""

        bundle_lease = await self.acquire_bundle(request, **overrides)
        try:
            yield bundle_lease
        finally:
            await self.release_bundle(bundle_lease)

    def _bundle_match_value(self, resource: Resource, rule: Any) -> str | None:
        value = getattr(resource, rule.from_field, None)
        if value is None:
            value = (getattr(resource, "payload", None) or {}).get(rule.from_field)
        if value is None or value == "":
            return None
        return str(value)

    # -- usage report (R1) -------------------------------------------------

    async def report_usage(
        self,
        *,
        since: float | None = None,
        until: float | None = None,
        group_by: str = "consumer",
        pool: str | None = None,
        limit: int = 20,
    ) -> UsageReport:
        """Aggregate completed leases plus the current arena snapshot.

        History comes from the bounded in-memory usage ledger: one row per
        returned (or force-revoked) lease, evicted oldest-first at capacity.
        Active leases and pool load are read live, so the report mixes
        historical usage with the present bottleneck view.
        """

        self._ensure_process()
        if group_by not in ("consumer", "target", "pool", "kind"):
            raise ConfigError(
                f"unknown group_by {group_by!r}; use consumer/target/pool/kind"
            )
        if not isinstance(limit, int) or limit < 1:
            raise ConfigError("limit must be an integer >= 1")
        for name, value in (("since", since), ("until", until)):
            if value is not None and (
                not isinstance(value, (int, float)) or not math.isfinite(value)
            ):
                raise ConfigError(f"{name} must be a finite epoch")
        if since is not None and until is not None and since > until:
            raise ConfigError("since must be <= until")
        names = [pool] if pool else list(self._pools)
        for name in names:
            if name not in self._pools:
                raise PoolNotFound(name)

        snapshot = self.usage.snapshot(
            since=since, until=until, group_by=group_by, pool=pool, limit=limit
        )
        active_groups: dict[str, dict[str, Any]] = {}
        pools: list[PoolUsage] = []
        for name in names:
            item = self._pools[name]
            active = await item._coordination.active(name)
            for lease in active:
                if group_by == "consumer":
                    key = lease.consumer or "(anonymous)"
                elif group_by == "target":
                    key = lease.target or "(no target)"
                elif group_by == "pool":
                    key = name
                else:
                    key = item.cfg.kind
                row = active_groups.setdefault(
                    key, {"key": key, "leases": 0, "slots": 0}
                )
                row["leases"] += 1
                row["slots"] += lease.slots
            stats = await item.stats()
            pools.append(
                {
                    "name": name,
                    "kind": item.cfg.kind,
                    "total": sum(stats["counts"].values()),
                    "counts": stats["counts"],
                    "allocatable": stats["allocatable"],
                    "active_leases": stats["active_leases"],
                    "waiting": stats["waiting"],
                    "avg_health": stats["avg_health"],
                }
            )
        groups: list[ActiveUsageGroup] = [
            {
                "key": str(row["key"]),
                "leases": int(row["leases"]),
                "slots": int(row["slots"]),
            }
            for row in sorted(
                active_groups.values(), key=lambda row: (-row["slots"], str(row["key"]))
            )
        ]
        return {
            "window": snapshot["window"],
            "group_by": snapshot["group_by"],
            "totals": snapshot["totals"],
            "groups": snapshot["groups"],
            "groups_total": snapshot["groups_total"],
            "truncated": snapshot["truncated"],
            "active": {"group_by": group_by, "groups": groups},
            "pools": pools,
        }

    # -- backup / restore --------------------------------------------------

    async def backup(self, *, operator: str = "system") -> dict[str, Any]:
        """Snapshot pool configs and resource state, secrets included (O5).

        Callers must be trusted: the document carries plaintext payloads so a
        restore can reproduce resources exactly.  Leases live on the
        coordination plane and are deliberately not exported.
        """

        self._ensure_process()
        pools: list[dict[str, Any]] = []
        resources_total = 0
        for name in self._pools:
            pool = self._pools[name]
            records = []
            async for resource in pool.storage.scan(name):
                item = resource.to_dict()
                item.pop("leases", None)
                records.append(item)
            records.sort(key=lambda item: str(item.get("id", "")))
            resources_total += len(records)
            active = await pool._coordination.active(name)
            pools.append(
                {
                    "name": name,
                    "kind": pool.cfg.kind,
                    "config": pool.cfg.to_dict(),
                    "resources": records,
                    "leases": [
                        {
                            "token": lease.token,
                            "resource_id": lease.resource_id,
                            "consumer": lease.consumer,
                            "slots": lease.slots,
                            "started_at": lease.started_at,
                            "expires_at": lease.expires_at,
                            "priority": lease.priority,
                            "use_limit": lease.use_limit,
                            "used": lease.used,
                            "target": lease.target,
                            "trace_id": lease.trace_id,
                            "outcome": lease.outcome.value,
                        }
                        for lease in active
                    ],
                }
            )
        document = {
            "format": BACKUP_FORMAT,
            "version": BACKUP_VERSION,
            "schema_version": 1,
            "created_at": _now(),
            "pools": pools,
        }
        self.audit.record(
            operator=operator,
            action="backup",
            pool="*",
            detail={"pools": len(pools), "resources": resources_total},
        )
        return document

    async def restore(
        self,
        document: dict[str, Any],
        *,
        conflict: str = "skip",
        operator: str = "system",
    ) -> dict[str, Any]:
        """Recreate pools/resources from :meth:`backup` output (O5 / F2).

        Occupancy states (new/validating/active/full/draining/lost) return as
        IDLE because leases are not migrated; banned/quarantined/suspended/
        quota-exhausted/archived states are preserved.  ``conflict="skip"``
        leaves existing resources untouched, ``"overwrite"`` applies backup
        fields.  Pool storage/coordination are deployment choices, so they are
        taken from this service rather than the backup.
        """

        self._ensure_process()
        if conflict not in ("skip", "overwrite"):
            raise ConfigError("conflict must be 'skip' or 'overwrite'")
        if not isinstance(document, dict):
            raise ConfigError("backup document must be an object")
        if document.get("format") != BACKUP_FORMAT:
            raise ConfigError("backup document has an unknown format")
        if document.get("version") != BACKUP_VERSION:
            raise ConfigError(
                f"unsupported backup version {document.get('version')!r}"
            )
        entries = document.get("pools")
        if not isinstance(entries, list):
            raise ConfigError("backup.pools must be a list")

        created_pools: list[str] = []
        reused_pools: list[str] = []
        errors: list[dict[str, Any]] = []
        imported = updated = skipped = normalized = leases_ignored = 0
        for item in entries:
            if not isinstance(item, dict):
                errors.append({"pool": None, "error": "pool entry must be an object"})
                continue
            name = str(item.get("name") or "").strip()
            kind = str(item.get("kind") or "").strip()
            records = item.get("resources", [])
            if not name or not kind or not isinstance(records, list):
                errors.append(
                    {
                        "pool": name or None,
                        "error": "pool entry needs name, kind and resources[]",
                    }
                )
                continue
            pool = self._pools.get(name)
            if pool is not None:
                if pool.cfg.kind != kind:
                    errors.append(
                        {
                            "pool": name,
                            "error": f"pool already exists with kind {pool.cfg.kind!r}",
                        }
                    )
                    continue
                reused_pools.append(name)
            else:
                raw = item.get("config")
                config = raw if isinstance(raw, dict) else {}
                config = {
                    key: value
                    for key, value in config.items()
                    if key not in ("kind", "storage", "coordination", "schema_version")
                }
                if isinstance(config.get("weights"), dict):
                    config["weights"] = Weights.from_dict(config["weights"])
                if isinstance(config.get("quarantine"), dict):
                    quarantine = config["quarantine"]
                    allowed = set(QuarantinePolicy.__dataclass_fields__) - {"rng"}
                    config["quarantine"] = QuarantinePolicy(
                        **{k: v for k, v in quarantine.items() if k in allowed}
                    )
                if isinstance(config.get("secret_fields"), list):
                    config["secret_fields"] = tuple(config["secret_fields"])
                try:
                    pool = self.create_pool(name, kind, config=config)
                except Exception as exc:
                    errors.append({"pool": name, "error": str(exc)})
                    continue
                created_pools.append(name)
            if self._running:
                await pool._ensure_started("restore")
            stats = await pool._restore_resources(
                records, conflict=conflict, operator=operator
            )
            pool_leases = item.get("leases")
            if isinstance(pool_leases, list):
                leases_ignored += len(pool_leases)
            imported += stats["imported"]
            updated += stats["updated"]
            skipped += stats["skipped"]
            normalized += stats["normalized"]
            if stats["errors"]:
                errors.extend({"pool": name, **err} for err in stats["errors"])
            self.audit.record(
                operator=operator,
                action="restore",
                pool=name,
                detail={**stats, "conflict": conflict},
            )
        return {
            "ok": True,
            "format": BACKUP_FORMAT,
            "conflict": conflict,
            "created_pools": created_pools,
            "reused_pools": reused_pools,
            "imported": imported,
            "updated": updated,
            "skipped": skipped,
            "normalized": normalized,
            "leases_ignored": leases_ignored,
            "errors": errors,
        }

    async def delete_pool(
        self, name: str, *, force: bool = False, operator: str = "system"
    ) -> dict[str, Any]:
        """Remove a pool from the registry.

        A pool with live leases is never deleted, even with ``force``: release
        or revoke them first.  ``force`` allows deleting a pool that still has
        stored resources, and flushes the backing stores before closing.
        """

        self._ensure_process()
        pool = self.require_pool(name)
        active = await pool._coordination.active(name)
        if active:
            raise BadState(
                f"pool {name!r} still has {len(active)} active lease(s); "
                "release or revoke them before deleting"
            )
        counts = await pool.counts()
        total = sum(counts.values())
        if total and not force:
            raise BadState(
                f"pool {name!r} still contains {total} resource(s); "
                "pass force to delete it"
            )
        if force:
            await pool.storage.flush(name)
            await pool._coordination.flush(name)
        await pool._binding.forget()
        await pool.close()
        self._pools.pop(name, None)
        self.audit.record(
            operator=operator,
            action="delete_pool",
            pool=name,
            detail={"removed": total, "forced": force},
        )
        self.bus.publish(
            "pool_deleted", name, data={"removed": total, "forced": force}
        )
        return {"pool": name, "removed": total, "forced": force}

    # -- runtimes ----------------------------------------------------------

    @property
    def started(self) -> bool:
        """Whether ``start()`` has been called and ``close()`` has not."""

        return self._running

    async def start(self) -> None:
        self._ensure_process()
        if self._running:
            return
        self._running = True
        await self.bus.start()
        for pool in self._pools.values():
            await pool.start()
        if self.bundle_mode == "two_phase" or self.bundle_fairness is not None:
            self._bundle_gc_task = asyncio.create_task(self._bundle_gc_loop())

    async def close(self) -> None:
        self._ensure_process()
        self._running = False
        try:
            task, self._bundle_gc_task = self._bundle_gc_task, None
            if task is not None:
                task.cancel()
                with contextlib.suppress(BaseException):
                    await task
            for pool in self._pools.values():
                await pool.close()
            self._bundles.clear()
            await self.bus.stop()
            self.audit.close()
        finally:
            self._isolation.release_all()

    async def __aenter__(self) -> Purser:
        await self.start()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()
