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
import heapq
import inspect
import logging
import random
import time
import uuid
from collections import defaultdict
from collections.abc import AsyncIterator, Callable, Iterator
from dataclasses import dataclass, field
from typing import Any, cast

from purser.backends.base import Coordination, ResourcePatch, StorageBackend
from purser.backends.coordination import MemoryCoordination, RedisCoordination
from purser.backends.memory import MemoryStorage
from purser.backends.sqlite import SqliteStorage
from purser.config import PoolConfig
from purser.core.allocator import AllocationContext, Allocator, make_allocator
from purser.core.audit import AuditLog
from purser.core.events import EventBus
from purser.core.health import HealthReport, ProbeGuard, ValidationGate
from purser.core.lease import LeaseManager
from purser.core.resource import (
    AcquireRequest,
    CheckVerdict,
    Concurrency,
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
    ResourceState,
)
from purser.crypto import SecretCodec, load_or_create_key
from purser.errors import (
    BadState,
    ConfigError,
    LeaseConflict,
    NoSuitableResource,
    PoolEmpty,
    PoolExhausted,
    PoolExists,
    PoolNotFound,
    ResourceBusy,
    ResourceNotFound,
)
from purser.idempotency import IdempotencyStore
from purser.metrics import Metrics
from purser.types.base import ResourceType, ResourceTypeRegistry

logger = logging.getLogger("purser")

MAINTENANCE_NAMES = (
    "quota_reset",
    "periodic_unban",
    "lost_gc",
    "validating_gc",
    "quarantine_gc",
    "archive_gc",
)


def _now() -> float:
    return time.time()


@dataclass(slots=True)
class ImportReport:
    imported: int = 0
    updated: int = 0
    skipped: int = 0
    errors: list[dict[str, Any]] = field(default_factory=list)
    batch_id: str | None = None

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
            "total": self.total,
        }


@dataclass(slots=True)
class _WaitHandle:
    seq: int
    priority: int
    future: asyncio.Future[Any]
    req: AcquireRequest
    cancelled: bool = False

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


@dataclass(slots=True)
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


class ResourcePool:
    """One named pool: a resource type, a storage plane and coordination."""

    def __init__(
        self,
        name: str,
        purser: Purser,
        rtype: ResourceType,
        cfg: PoolConfig,
        storage: StorageBackend,
        coordination: Coordination,
    ) -> None:
        self.name = name
        self.purser = purser
        self.type = rtype
        self.cfg = cfg
        self.storage = storage
        self._coordination = coordination
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
        self._seq = 0
        self._consumer_holds: dict[str, set[str]] = defaultdict(set)
        self._affinity: dict[str, dict[str, float]] = defaultdict(dict)
        self._check_cache: dict[tuple[str, str, str], tuple[CheckVerdict, float]] = {}
        self._rr_cursor = 0
        self._scheduler = PeriodicScheduler()
        self._tasks: list[asyncio.Task[Any]] = []
        self._bg_tasks: set[asyncio.Task[Any]] = set()
        self._started = False
        self._register_scheduler()

    # -- lifecycle ---------------------------------------------------------

    def _register_scheduler(self) -> None:
        intervals = self.cfg.meta.get("scheduler_intervals", {})
        defaults = {
            "quota_reset": 300.0,
            "periodic_unban": 60.0,
            "lost_gc": 300.0,
            "validating_gc": 30.0,
            "quarantine_gc": 15.0,
            "archive_gc": 3600.0,
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
        if self._started:
            return
        self._started = True
        if self.purser.maintenance_enabled:
            await self._scheduler.start()
        self._tasks.append(asyncio.create_task(self._reaper_loop()))
        if self.purser.maintenance_enabled:
            self._tasks.append(asyncio.create_task(self._background_validate_loop()))
        await self._reconcile_leases()

    async def close(self) -> None:
        self._started = False
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
        closer = getattr(self.storage, "close", None)
        if closer is not None:
            result = closer()
            if inspect.isawaitable(result):
                await result
        await self._coordination.close()

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

    # -- reads -------------------------------------------------------------

    async def get(self, rid: str) -> Resource | None:
        return await self.storage.get(self.name, rid)

    async def require(self, rid: str) -> Resource:
        resource = await self.storage.get(self.name, rid)
        if resource is None:
            raise ResourceNotFound(f"{self.name}:{rid}")
        return resource

    async def query(
        self,
        *,
        states: set[ResourceState] | None = None,
        tags: frozenset[str] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Resource]:
        if limit < 0 or offset < 0:
            raise ConfigError("limit and offset must be >= 0")
        return await self.storage.query(
            self.name, states=states, tags=tags, limit=limit, offset=offset
        )

    async def counts(self) -> dict[ResourceState, int]:
        return await self.storage.counts(self.name)

    async def stats(self) -> dict[str, Any]:
        counts = await self.counts()
        resources = await self.query(limit=10_000)
        health_scores = [r.health.score for r in resources if r.health.samples]
        return {
            "name": self.name,
            "kind": self.cfg.kind,
            "total": sum(counts.values()),
            "counts": {s.value: n for s, n in counts.items()},
            "allocatable": counts[ResourceState.IDLE] + counts[ResourceState.ACTIVE],
            "active_leases": len(await self._coordination.active(self.name)),
            "waiting": len(self._waiters),
            "avg_health": round(sum(health_scores) / len(health_scores), 3) if health_scores else None,
            "config": self.cfg.to_dict(),
        }

    def describe(self, resource: Resource, *, secret: bool = False) -> dict[str, Any]:
        return describe_resource(resource, secret=secret)

    # -- import / generate ------------------------------------------------

    async def import_resources(
        self,
        records: list[dict[str, Any]],
        *,
        dedup_key: str = "id",
        source: str = "manual",
        operator: str = "system",
        fail_fast: bool = False,
        import_validate: str | None = None,
        batch_id: str | None = None,
    ) -> ImportReport:
        """Bulk import with dedup (O4), dirty-row skipping and a report (O15)."""

        report = ImportReport(batch_id=batch_id or uuid.uuid4().hex)
        mode = import_validate or self.cfg.import_validate
        for index, record in enumerate(records):
            try:
                resource = await self.type.generate(record)
            except Exception as exc:
                self._guard_error("import.generate", exc)
                report.errors.append({"line": index, "error": str(exc)})
                if fail_fast:
                    break
                continue
            if dedup_key and dedup_key in record:
                resource.id = str(record[dedup_key])
            resource.meta.setdefault("source", source)
            resource.meta["batch_id"] = report.batch_id
            resource.concurrency.max_slots = self.cfg.max_slots or resource.concurrency.max_slots
            if resource.expires_at is None and self.cfg.expires_in:
                resource.expires_at = _now() + self.cfg.expires_in
            if self.cfg.usage_quota is not None and resource.usage_quota is None:
                resource.usage_quota = self.cfg.usage_quota
            if self.cfg.target_quota and resource.quota is None:
                resource.quota = Quota(limit=int(sum(self.cfg.target_quota.values())))
            existing = await self.storage.get(self.name, resource.id)
            if existing is not None:
                if existing.state is ResourceState.ARCHIVED:
                    report.skipped += 1
                    continue
                resource.state = existing.state
                resource.concurrency = existing.concurrency
                resource.health = existing.health
                resource.quota = existing.quota
                resource.site_failures = existing.site_failures
                resource.used_count = existing.used_count
                resource.next_available_at = existing.next_available_at
                resource.meta = {**existing.meta, **resource.meta}
                resource.meta["updated_at"] = _now()
                report.updated += 1
            else:
                resource.meta["created_at"] = _now()
                report.imported += 1
            await self.storage.put(self.name, resource)
            if mode == "on_import":
                await self.validate_now(resource.id, operator=operator)
            elif mode == "batch":
                self._spawn(self._validate_in_background(resource.id))
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

    async def generate(self, specs: list[dict[str, Any]], *, source: str = "builtin", operator: str = "system") -> ImportReport:
        return await self.import_resources(
            specs, source=source, operator=operator, import_validate="on_import"
        )

    # -- acquisition -------------------------------------------------------

    async def acquire(self, req: AcquireRequest | None = None, **kwargs: Any) -> Lease:
        request = req or AcquireRequest()
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

        if not await self._has_any_resources():
            raise PoolEmpty(f"pool {self.name} has no allocatable resources")
        if timeout <= 0:
            raise PoolExhausted(f"pool {self.name} could not satisfy request immediately")

        handle = self._enqueue_waiter(request)
        current = asyncio.current_task()
        if current is not None:
            current.add_done_callback(lambda _task: handle.cancel_pending())
        try:
            await asyncio.wait_for(asyncio.shield(handle.future), timeout)
        except asyncio.TimeoutError:
            if handle.future.done():
                return cast(Lease, handle.future.result())
            handle.cancel_pending()
            raise PoolExhausted(
                f"pool {self.name} exhausted after {timeout:.1f}s"
                f" (consumer={request.consumer!r}, slots={request.slots})"
            ) from None
        return cast(Lease, handle.future.result())

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

    async def _has_any_resources(self) -> bool:
        counts = await self.counts()
        return sum(counts.values()) > 0

    async def _serve_waiters(self, limit: int = 64) -> int:
        served = 0
        pending: list[_WaitHandle] = []
        while self._waiters and served < limit:
            handle = heapq.heappop(self._waiters)
            if handle.cancelled or handle.future.done():
                continue
            try:
                lease = await self._try_allocate(handle.req)
            except (NoSuitableResource, PoolEmpty, BadState):
                lease = None
            if handle.cancelled or handle.future.done():
                if lease is not None:
                    # The waiter timed out or was cancelled while we were
                    # allocating; give the freshly acquired slot straight back.
                    await self._abandon_lease(lease)
                    continue
            if lease is not None:
                handle.future.set_result(lease)
                served += 1
            else:
                pending.append(handle)
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
        if not req.target:
            return CheckVerdict.PASS
        key = (req.consumer or "", req.target, resource.id)
        now = _now()
        cached = self._check_cache.get(key)
        if cached and cached[1] > now:
            return cached[0]
        try:
            verdict = await self.type.on_request_check(resource, req)
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
                    "token": lease.token,
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
        held = await self._coordination.get_lease(self.name, lease.token)
        if held is None:
            # A revoke, expiry or coordination wipe already removed our token.
            # Never decrement storage here: the busy slot may now belong to a
            # fresh lease from another instance, and a blind decrement would
            # steal it.  Orphaned slots are recovered by _reconcile_leases()
            # on the next start(), which is the designated restart path.
            self._leases.unregister(lease.token)
            if lease.consumer:
                self._consumer_holds.get(lease.consumer, set()).discard(
                    lease.resource_id
                )
            self.purser.metrics.inc(f"pool.{self.name}.release_total")
            await self._serve_waiters()
            return True
        result = await self.storage.transition_release(
            self.name, lease.resource_id, lease.slots
        )
        if not result.ok:
            raise LeaseConflict(
                f"token {lease.token} could not return {lease.resource_id} slots"
            )
        await self._coordination.unregister_lease(self.name, lease.token)
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
                "token": lease.token,
                "outcome": outcome.value,
                "consumer": lease.consumer,
            },
            trace_id=lease.trace_id,
        )
        await self._apply_usage_outcome(lease.resource_id, outcome, used, lease)
        await self._serve_waiters()
        self.purser.metrics.inc(f"pool.{self.name}.release_total")
        return True

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
                    await self.type.on_release(resource, outcome)
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
                data={"token": lease.token, "reason": "token already gone"},
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
                data={"token": lease.token, "consumer": lease.consumer},
                trace_id=lease.trace_id,
            )
        else:
            self.purser.bus.publish(
                "lease_lost",
                self.name,
                rid=lease.resource_id,
                data={"token": lease.token},
                trace_id=lease.trace_id,
            )
        await self._serve_waiters()

    async def _on_lease_lost(self, lease: Lease) -> None:
        # The heartbeat gave up on this lease (e.g. use_limit reached or the
        # coordination token vanished).  Stop tracking it in the manager so a
        # non-expired lost lease does not linger in `stats.active_leases`.
        self._leases.unregister(lease.token)
        self.purser.bus.publish(
            "lease_lost",
            self.name,
            rid=lease.resource_id,
            data={"token": lease.token, "reason": "heartbeat rejected"},
            trace_id=lease.trace_id,
        )
        self.purser.metrics.inc(f"pool.{self.name}.lease_lost_total")

    def _guard_error(self, context: str, error: Exception) -> None:
        logger.warning("%s hook failed for pool %s: %s", context, self.name, error)
        try:
            asyncio.get_running_loop().create_task(
                self.type.on_error(context, error)
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
                report = await self.type.validate(resource)
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

        policy = self.cfg.meta.get("expire_policy", "suspended")
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
                await self.type.on_state_changed(updated, old, new, reason)
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

    async def ban(self, rid: str, *, reason: str, operator: str) -> None:
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
        self, rid: str, *, operator: str, reason: str = "manual unban", revalidate: bool = True
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

    async def suspend(self, rid: str, *, operator: str, reason: str) -> None:
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
        self, rid: str, *, operator: str, reason: str = "manual resume", revalidate: bool = False
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

    async def top_up(self, rid: str, *, operator: str, amount: int | None = None, reason: str = "manual top_up") -> None:
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

    async def drain(self, rid: str, *, operator: str, reason: str) -> None:
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

    async def restore(self, rid: str, *, operator: str, reason: str = "manual restore") -> None:
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
        self, rid: str, *, target: ResourceState, operator: str, reason: str
    ) -> None:
        if target not in ADMIN_ALLOWED:
            raise BadState(f"admin_set_state disallows {target.value}") from None
        resource = await self.require(rid)
        if resource.concurrency.used_slots > 0:
            raise BadState(f"cannot force-set state while {rid} is busy")
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
        self, rid: str, *, operator: str, reason: str, token: str | None = None
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
        await self._coordination.unregister_lease(self.name, lease.token)
        self._leases.unregister(lease.token)
        result = await self.storage.transition_release(self.name, rid, lease.slots)
        if not result.ok:
            raise LeaseConflict(f"failed to revoke {rid}:{lease.token}")
        if lease.consumer:
            self._consumer_holds.get(lease.consumer, set()).discard(rid)
            await self._kick_cooldown(rid, lease.consumer)
        self.purser.audit.record(
            operator=operator, action="force_revoke", pool=self.name, rid=rid, reason=reason,
            detail={"token": lease.token, "consumer": lease.consumer},
        )
        self.purser.bus.publish(
            "lease_revoked", self.name, rid=rid,
            data={"token": lease.token, "reason": reason},
            trace_id=lease.trace_id,
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
        operator: str,
        reason: str,
    ) -> Resource:
        """O2: mutate metadata/payload/slots; busy resources are rejected."""

        resource = await self.require(rid)
        if resource.concurrency.used_slots > 0:
            raise BadState("cannot update a resource with active leases")
        if max_slots is not None and max_slots <= 0:
            raise ConfigError("max_slots must be > 0")
        patch = ResourcePatch(
            payload=payload if payload is not None else dict(resource.payload),
            tags=tags if tags is not None else resource.tags,
            concurrency=(
                Concurrency(max_slots=max_slots)
                if max_slots is not None
                else resource.concurrency
            ),
            meta_patch={**resource.meta, "updated_by": operator, "update_reason": reason},
        )
        await self._cas(rid, patch)
        self.purser.audit.record(
            operator=operator, action="update_resource", pool=self.name, rid=rid, reason=reason
        )
        updated = await self.require(rid)
        return updated

    async def move_resource(
        self, rid: str, target_pool: ResourcePool, *, operator: str, reason: str
    ) -> Resource:
        """O3: cross-pool migration; busy resources are rejected."""

        resource = await self.require(rid)
        if resource.concurrency.used_slots > 0:
            raise BadState("cannot move a resource with active leases")
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
        operator: str,
        reason: str,
    ) -> dict[str, Any]:
        """B1-lite: requalify / isolate / retire every resource of a batch."""

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
        """Execute one of the six maintenance sweeps (5.9 / O19 / X3)."""

        runner = {
            "quota_reset": self._maintenance_quota_reset,
            "periodic_unban": self._maintenance_periodic_unban,
            "lost_gc": self._maintenance_lost,
            "validating_gc": self._maintenance_validating,
            "quarantine_gc": self._maintenance_quarantine,
            "archive_gc": self._maintenance_archive,
        }.get(name)
        if runner is None:
            raise ConfigError(f"unknown maintenance task {name!r}")
        return await runner()

    async def _maintenance_quota_reset(self) -> dict[str, int]:
        moved = 0
        async for resource in self.storage.scan(self.name, states={ResourceState.QUOTA_EXHAUSTED}):
            quota = resource.quota
            if quota is not None:
                quota.reset_daily(timezone=self.cfg.timezone)
            patch = ResourcePatch(
                state=ResourceState.IDLE,
                quota=quota,
                used_count=0,
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
            policy = self.cfg.meta.get("requalify_on_release", False)
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
                leased.append(await self.acquire(basis))
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
        storage: str = "memory",
        coordination: str = "memory",
        mysql_url: str | None = None,
        mysql_connection: Any = None,
        redis_url: str | None = None,
        redis_client: Any = None,
        sqlite_path: str | None = None,
        audit_path: str | None = None,
        secret_key: bytes | None = None,
    ) -> None:
        self.types = ResourceTypeRegistry()
        self.bus = EventBus(maxsize=event_queue_size)
        self.audit = AuditLog(sqlite_path=audit_path)
        self.metrics = Metrics()
        self.idempotency = IdempotencyStore()
        self.probe_guard = ProbeGuard()
        self.maintenance_enabled = maintenance_enabled
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
        self._codec = SecretCodec(key)
        self._pools: dict[str, ResourcePool] = {}
        self._running = False

        from purser.types import AccountType, ProxyType

        self.types.register(ProxyType(probe_guard=self.probe_guard))
        self.types.register(AccountType())

    # -- plugins / pools ---------------------------------------------------

    def register_type(self, rtype: ResourceType) -> None:
        self.types.register(rtype)

    def _make_storage(self, cfg: PoolConfig) -> StorageBackend:
        if cfg.storage == "memory":
            return MemoryStorage()
        if cfg.storage == "sqlite":
            return SqliteStorage(
                path=self.sqlite_path,
                codec=self._codec,
                secret_fields=cfg.secret_fields,
            )
        if cfg.storage == "mysql":
            from purser.backends.mysql import MySqlStorage

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
        if name in self._pools:
            raise PoolExists(f"pool {name!r} already exists")
        rtype = self.types.get(kind)
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
        if cfg.max_slots is None:
            cfg = cfg.with_overrides(max_slots=rtype.default_concurrency)
        if not cfg.secret_fields:
            cfg = cfg.with_overrides(secret_fields=rtype.secret_fields)
        storage = self._make_storage(cfg)
        coordination = self._make_coordination(cfg)
        pool = ResourcePool(name, self, rtype, cfg, storage, coordination)
        self._pools[name] = pool
        if self._running:
            asyncio.create_task(pool.start())
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

        existing = self._pools.get(name)
        if existing is not None:
            return existing
        if kind is None:
            raise PoolNotFound(f"pool {name!r} does not exist and no kind given")
        return self.create_pool(name, kind, config=config, **overrides)

    def get_pool(self, name: str) -> ResourcePool | None:
        return self._pools.get(name)

    def require_pool(self, name: str) -> ResourcePool:
        pool = self._pools.get(name)
        if pool is None:
            raise PoolNotFound(f"pool {name!r} does not exist")
        return pool

    def pool_names(self) -> list[str]:
        return list(self._pools)

    # -- runtimes ----------------------------------------------------------

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        await self.bus.start()
        for pool in self._pools.values():
            await pool.start()

    async def close(self) -> None:
        self._running = False
        for pool in self._pools.values():
            await pool.close()
        await self.bus.stop()
        self.audit.close()

    async def __aenter__(self) -> Purser:
        await self.start()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()
