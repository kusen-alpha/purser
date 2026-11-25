"""The async :class:`ResourcePool` data plane.

Composes the pool services (leases, catalog, validation, admin, maintenance)
over one adapter, storage plane and coordination plane, and exposes the
public data-plane API.  The state machine in ``core.state`` remains the
single source of truth for transitions; the storage plane provides the
atomic read-check-write primitives and the coordination plane tracks
in-flight leases.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import (
    AsyncGenerator,
    AsyncIterator,
    Callable,
    Iterable,
    Sequence,
)
from dataclasses import replace
from typing import TYPE_CHECKING, Any, Literal, overload

from purser.adapters.base import (
    ResourceAdapter,
)
from purser.authority import make_authority
from purser.backends.base import Coordination, ResourcePatch, StorageBackend
from purser.binding import make_binding
from purser.config import (
    IMMUTABLE_POOL_FIELDS,
    MAINTENANCE_NAMES,
    ImportValidate,
    PoolConfig,
)
from purser.core.allocator import Allocator, make_allocator
from purser.core.health import HealthReport, ValidationGate
from purser.core.lease import LeaseManager
from purser.core.resource import (
    AcquireRequest,
    Lease,
    Resource,
    SiteFailure,
    UsageOutcome,
)
from purser.core.scheduler import PeriodicScheduler
from purser.core.state import (
    ALLOCATABLE_STATES,
    ResourceState,
)
from purser.core.views import (
    PoolStats,
    ValidateJobCancelView,
    ValidateJobView,
)
from purser.errors import (
    ConfigError,
    IsolationConflict,
    PoolEmpty,
    PoolExhausted,
    ResourceNotFound,
)
from purser.facade._helpers import (
    _MISSING,
    BatchLease,
    _now,
)
from purser.isolation import (
    PICKLE_MESSAGE,
)
from purser.pool.admin import AdminService
from purser.pool.catalog import CatalogService
from purser.pool.context import PoolContext
from purser.pool.leases import LeaseService
from purser.pool.lifecycle import LifecycleService
from purser.pool.maintenance import MaintenanceService
from purser.pool.state_ops import StateOps
from purser.pool.tasks import TaskRegistry
from purser.pool.usage import UsageService
from purser.pool.validation import ValidationService
from purser.pool.validation_jobs import ValidationJobs
from purser.pool.wait_queue import WaitQueue
from purser.results import ImportReport as ImportReport

if TYPE_CHECKING:
    from purser.facade.purser import Purser

logger = logging.getLogger("purser")


class ResourcePool:
    """One named pool: a resource adapter, a storage plane and coordination."""

    @property
    def kind(self) -> str:
        """Resource adapter kind served by this pool."""

        return self.cfg.kind

    @property
    def storage(self) -> StorageBackend:
        """Storage backend used by every pool service."""

        ctx = getattr(self, "_ctx", None)
        return ctx.storage if ctx is not None else self._storage

    @storage.setter
    def storage(self, value: StorageBackend) -> None:
        # Keep the services' shared holder in sync: callers (and fault-
        # injection tests) may swap the backend after construction.
        self._storage = value
        ctx = getattr(self, "_ctx", None)
        if ctx is not None:
            ctx.storage = value

    def __init__(
        self,
        name: str,
        purser: Purser,
        adapter: ResourceAdapter,
        cfg: PoolConfig,
        storage: StorageBackend,
        coordination: Coordination,
        profile: str | None = None,
    ) -> None:
        self.name = name
        self.purser = purser
        self.adapter = adapter
        self.cfg = cfg
        # The profile is creation-time provenance only; ``cfg`` is the truth.
        self.profile = profile
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
            heartbeat_interval=cfg.heartbeat_interval
            or max(0.01, cfg.lease_ttl / 3.0),
        )
        self._wait_queue = WaitQueue()
        self._waiters = self._wait_queue.heap
        self._scheduler = PeriodicScheduler()
        self._task_registry = TaskRegistry()
        self._validation_jobs = ValidationJobs()
        self._validate_job_queues: dict[str, asyncio.Queue[str]] = {}
        self._start_task: asyncio.Task[None] | None = None
        self._started = False
        self._ctx = PoolContext(
            name=name,
            purser=purser,
            adapter=adapter,
            cfg=cfg,
            storage=storage,
            coordination=coordination,
            authority=self._authority,
            binding=self._binding,
            leases=self._leases,
            scheduler=self._scheduler,
            allocator=self._allocator,
            profile=profile,
            guard_error=self._guard_error,
        )
        self._state = StateOps(self._ctx)
        self._validation = ValidationService(
            self._ctx,
            self._state,
            gate=ValidationGate(
                stale_after=cfg.stale_after,
                background_interval=cfg.background_interval,
                sample=cfg.background_validate_sample,
            ),
            semaphore=asyncio.Semaphore(cfg.validate_concurrency),
            jobs=self._validation_jobs,
            spawn=self._spawn,
        )
        self._usage = UsageService(
            self._ctx, self._state, on_validation_fail=self._validation.on_failure
        )
        self._lease = LeaseService(
            self._ctx,
            self._state,
            self._usage,
            self._validation,
            wait_queue=self._wait_queue,
            counts=self.counts,
            schedule_open=self._schedule_open,
        )
        self._admin = AdminService(
            self._ctx,
            self._state,
            consumer_holds=self._lease.consumer_holds,
            validate_now=self._validation.validate_now,
            validation_fail=self._validation.on_failure,
            serve_waiters=self._lease.serve_waiters,
        )
        self._maintenance = MaintenanceService(
            self._ctx,
            self._state,
            admin=self._admin,
            validation=self._validation,
            serve_waiters=self._lease.serve_waiters,
            active_leases=self._leases.active,
            release=self.release,
            revoke=self.force_revoke,
            is_degraded=self._lease.is_degraded,
        )
        self._lifecycle = LifecycleService(self)
        self._validation.bind_waker(self._lease.serve_waiters)
        self._lease.bind(renew=self._renew, revoker=self._admin.force_revoke)
        self._leases.on_expire = self._lease.on_expired
        self._leases.on_lost = self._lease.on_lost
        # Migration aliases: tests and out-of-package code still reach these.
        self._consumer_holds = self._lease.consumer_holds
        self._affinity = self._lease.affinity
        self._catalog = CatalogService(
            self._ctx,
            self._state,
            waiters=self._waiters,
            validate_now=self.validate_now,
            start_validate_job=self._start_validate_job,
        )
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
        """Start the pool runtime (reaper, validation sampling, wake-ups)."""

        await self._lifecycle.start()

    @property
    def started(self) -> bool:
        """Whether the pool runtime (reaper, validation, wake-ups) is running."""

        return self._started

    async def _ensure_started(self, operation: str) -> None:
        await self._lifecycle.ensure_started(operation)

    async def close(self) -> None:
        """Stop background work, fail waiters and release claims."""

        await self._lifecycle.close()

    async def _drain(self) -> None:
        await self._maintenance.drain()

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
            # The memory backend mutates the shared Concurrency in place, so
            # read the pre-reset count before reconciliation runs.
            used_before = resource.concurrency.used_slots
            if await self.storage.reset_usage(self.name, resource.id, slots=target):
                dropped = used_before - target
                if dropped > 0 and self.cfg.coordination == "memory":
                    logger.warning(
                        "reconcile on pool %r reset %s from %s to %s used slots: "
                        "memory coordination is instance-private, so a storage "
                        "shard must be owned by one live instance at a time; if "
                        "another instance still holds leases here, those slots are "
                        "now double-bookable",
                        self.name,
                        resource.id,
                        used_before,
                        target,
                    )
                self.purser.bus.publish(
                    "lease_reconciled",
                    self.name,
                    rid=resource.id,
                    data={"reason": "orphaned leases reset", "used_slots": target},
                )

    def _spawn(self, coro: Any) -> asyncio.Task[Any]:
        """Track a fire-and-forget task so :meth:`close` can drain it."""

        return self._task_registry.spawn(coro)

    def reconfigure(self, **updates: Any) -> PoolConfig:
        """Live-update pool knobs: rebuilds the allocator, validation
        gate, heartbeat cadence and scheduler intervals from the new config.
        """

        immutable = IMMUTABLE_POOL_FIELDS & set(updates)
        if immutable:
            fields = ", ".join(sorted(immutable))
            raise ConfigError(f"cannot change {fields} at runtime")
        self._ensure_process()
        try:
            cfg = self.cfg.with_overrides(**updates)
        except ConfigError:
            raise
        except (TypeError, ValueError) as exc:
            raise ConfigError(f"invalid pool config override: {exc}") from exc
        self.cfg = cfg
        self._ctx.cfg = cfg
        self._allocator = make_allocator(cfg.allocator)
        self._ctx.allocator = self._allocator
        self._validation.reload(cfg)
        self._leases.heartbeat_interval = cfg.heartbeat_interval or max(
            0.01, cfg.lease_ttl / 3.0
        )
        self._lease.reset_rate_limits()
        self._register_scheduler()
        return cfg

    def _next_seq(self) -> int:
        return self._wait_queue.next_seq()

    # -- maintenance loops -------------------------------------------------

    def _start_validate_job(
        self, job_id: str, rids: Sequence[str], operator: str
    ) -> ValidateJobView:
        return self._validation.start_job(job_id, rids, operator)

    def validate_jobs(self) -> list[ValidateJobView]:
        """List background validation jobs."""

        return self._validation.jobs()

    def validate_job(self, job_id: str) -> ValidateJobView | None:
        """Return one validation job, or ``None`` when unknown."""

        return self._validation.job(job_id)

    def cancel_validate_job(self, job_id: str) -> ValidateJobCancelView | None:
        """Request cancellation of a running validation job."""

        return self._validation.cancel_job(job_id)

    # -- reads -------------------------------------------------------------

    async def get(self, resource_id: str) -> Resource | None:
        """Return one resource snapshot, or ``None`` when it does not exist."""

        resource = await self.storage.get(self.name, resource_id)
        return None if resource is None else self.adapter.hydrate(resource)

    async def require(self, resource_id: str) -> Resource:
        """Return one resource snapshot or raise :class:`ResourceNotFound`."""

        resource = await self.storage.get(self.name, resource_id)
        if resource is None:
            raise ResourceNotFound(f"{self.name}:{resource_id}")
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
        """List resources matching ``states``/``tags``/``search`` filters."""

        return await self._catalog.query(
            states=states,
            tags=tags,
            limit=limit,
            offset=offset,
            search=search,
            sort=sort,
            order=order,
        )

    def iter_resources(
        self,
        *,
        states: set[ResourceState] | None = None,
        tags: Iterable[str] | None = None,
        search: str | None = None,
        batch_size: int = 100,
    ) -> AsyncGenerator[Resource, None]:
        """Stream matching resources in storage-friendly batches."""

        return self._catalog.iter_resources(
            states=states, tags=tags, search=search, batch_size=batch_size
        )

    async def count_matching(
        self,
        *,
        states: set[ResourceState] | None = None,
        tags: frozenset[str] | None = None,
        search: str | None = None,
    ) -> int:
        """Count resources matching the same filters as :meth:`query`."""

        return await self._catalog.count_matching(
            states=states, tags=tags, search=search
        )

    async def _search_resources(
        self,
        *,
        states: set[ResourceState] | None = None,
        tags: frozenset[str] | None = None,
        search: str | None = None,
    ) -> list[Resource]:
        return await self._catalog._search_resources(
            states=states, tags=tags, search=search
        )

    @staticmethod
    def _matches(resource: Resource, needle: str) -> bool:
        return CatalogService._matches(resource, needle)

    async def batches(self) -> list[dict[str, Any]]:
        """Import batches recorded by this pool."""

        return await self._catalog.batches()

    @overload
    async def counts(self, *, by_name: Literal[False] = False) -> dict[ResourceState, int]: ...

    @overload
    async def counts(self, *, by_name: Literal[True]) -> dict[str, int]: ...

    async def counts(self, *, by_name: bool = False) -> dict[Any, int]:
        """State histogram (or per-state-name counts when ``by_name``)."""

        if by_name:
            return await self._catalog.counts(by_name=True)
        return await self._catalog.counts(by_name=False)

    async def stats(self) -> PoolStats:
        """Totals, state counts, allocatable slots and health summary."""

        return await self._catalog.stats()

    def describe(self, resource: Resource, *, secret: bool = False) -> dict[str, Any]:
        """Public dict view of ``resource``; payload fields are masked by default."""

        return self._catalog.describe(resource, secret=secret)

    async def leases(self, resource_id: str | None = None) -> list[Lease]:
        """Live leases, optionally restricted to one resource."""

        return await self._catalog.leases(resource_id)

    async def lease_by_token(self, token: str) -> Lease | None:
        """Local live lease first, then the coordination plane.

        Returning the local object matters for ``renew``/``release``: they
        mutate it.  Leases issued by other instances only exist in the
        coordination plane and come back as detached copies.
        """

        lease = self._leases.get(token)
        if lease is not None:
            return lease
        return await self._coordination.get_lease(self.name, token)

    async def import_resources(
        self,
        records: Sequence[dict[str, Any] | Resource],
        *,
        dedup_key: str = "id",
        source: str = "manual",
        operator: str = "system",
        fail_fast: bool = False,
        import_validate: ImportValidate | str | None = None,
        batch_id: str | None = None,
    ) -> ImportReport:
        """Bulk import dict specs or Resources with dedup and optional validation."""

        return await self._catalog.import_resources(
            records,
            dedup_key=dedup_key,
            source=source,
            operator=operator,
            fail_fast=fail_fast,
            import_validate=import_validate,
            batch_id=batch_id,
        )

    async def generate(
        self,
        specs: Sequence[dict[str, Any] | Resource],
        *,
        source: str = "builtin",
        operator: str = "system",
    ) -> ImportReport:
        """Build resources from adapter specs and import them with validation."""

        return await self._catalog.generate(
            specs, source=source, operator=operator
        )

    async def apply_defaults(
        self,
        *,
        operator: str = "system",
        reason: str = "apply pool defaults",
    ) -> dict[str, int]:
        """Backfill pool defaults onto resources that lack them."""

        return await self._catalog.apply_defaults(operator=operator, reason=reason)

    async def _restore_resources(
        self, records: Sequence[Any], *, conflict: str, operator: str
    ) -> dict[str, Any]:
        return await self._catalog._restore_resources(
            records, conflict=conflict, operator=operator
        )

    def _restore_resource(self, entry: Any) -> tuple[Resource, bool]:
        return self._catalog._restore_resource(entry)

    async def acquire(self, req: AcquireRequest | None = None, **kwargs: Any) -> Lease:
        """Acquire one lease; ``req`` is never mutated.

        Keyword arguments build an :class:`AcquireRequest` when ``req`` is
        omitted; both paths run the same ``__post_init__`` validation.
        """

        await self._ensure_started("acquire")
        unknown = set(kwargs) - set(AcquireRequest.__dataclass_fields__)
        if unknown:
            fields = ", ".join(sorted(unknown))
            raise TypeError(f"acquire() got unexpected keyword argument(s): {fields}")
        # Rebuild through the constructor so __post_init__ validation applies to
        # keyword fields too, and a reusable request is never mutated.
        request = replace(req, **kwargs) if req is not None else AcquireRequest(**kwargs)
        request.ensure_trace()
        self.purser.metrics.inc(f"pool.{self.name}.acquire_total")
        if not isinstance(request.slots, int) or request.slots <= 0:
            raise ConfigError("slots must be a positive integer")
        if request.use_limit is not None and request.use_limit < 0:
            raise ConfigError("use_limit must be >= 0")
        return await self._lease.acquire(request)

    async def _serve_waiters(self, limit: int = 64) -> int:
        return await self._lease.serve_waiters(limit)

    async def _affinity_scores(
        self, consumer: str | None, candidates: list[Resource]
    ) -> dict[str, float]:
        return await self._lease._affinity_scores(consumer, candidates)

    async def _renew(self, lease: Lease, used: int | None = None) -> bool:
        return await self._lease.renew(lease, used)

    # -- lease() context manager ------------------------------------------

    @contextlib.asynccontextmanager
    async def lease(
        self, req: AcquireRequest | None = None, **kwargs: Any
    ) -> AsyncIterator[Lease]:
        """Recommended path: acquire now, always release at block exit."""

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
        outcome: UsageOutcome | str | None = None,
        used: int | None = None,
    ) -> bool:
        """Return a lease and optionally report its outcome and usage."""

        return await self._lease.release(lease, outcome=outcome, used=used)

    async def release_consumer(
        self,
        consumer: str,
        *,
        outcome: UsageOutcome | str = UsageOutcome.OK,
        operator: str = "system",
    ) -> list[Lease]:
        """Release every live lease held by ``consumer``."""

        return await self._lease.release_consumer(
            consumer, outcome=outcome, operator=operator
        )

    async def _apply_usage_outcome(
        self, resource_id: str, outcome: UsageOutcome, used: int | None, lease: Lease
    ) -> None:
        await self._usage.apply_outcome(resource_id, outcome, used, lease)

    def _record_site_failure(
        self, site_failures: dict[str, SiteFailure], site: str, now: float
    ) -> None:
        self._state.record_site_failure(site_failures, site, now)

    # -- hooks / callbacks ------------------------------------------------

    async def _on_lease_expired(self, lease: Lease) -> None:
        await self._lease.on_expired(lease)

    async def _on_lease_lost(self, lease: Lease) -> None:
        await self._lease.on_lost(lease)

    def _guard_error(self, context: str, error: Exception) -> None:
        logger.warning("%s hook failed for pool %s: %s", context, self.name, error)
        try:
            asyncio.get_running_loop().create_task(
                self.adapter.on_error(context, error)
            )
        except Exception as exc:
            logger.debug("on_error hook not scheduled: %s", exc)

    # -- validation --------------------------------------------------------

    async def validate_now(
        self,
        resource_id: str,
        *,
        operator: str = "system",
    ) -> HealthReport:
        """Validate one resource immediately and apply the verdict."""

        return await self._validation.validate_now(resource_id, operator=operator)

    async def validate_check(self, resource_id: str, check: str) -> HealthReport:
        """Run one named check without changing the resource state."""

        return await self._validation.validate_check(resource_id, check)

    async def _on_validation_ok(self, resource_id: str, report: HealthReport) -> None:
        await self._validation.on_ok(resource_id, report)

    async def _on_validation_fail(
        self,
        resource_id: str,
        reason: str,
        *,
        operator: str = "system",
        from_states: set[ResourceState] | None = None,
        site_failures: dict[str, SiteFailure] | None = None,
    ) -> None:
        await self._validation.on_failure(
            resource_id,
            reason,
            operator=operator,
            from_states=from_states,
            site_failures=site_failures,
        )

    # -- operations --------------------------------------------------------

    async def _apply_state(
        self,
        resource_id: str,
        patch: ResourcePatch,
        *,
        expect_state: ResourceState | None = None,
        attempts: int = 3,
        reason: str = "op",
    ) -> bool:
        return await self._state.apply_state(
            resource_id, patch, expect_state=expect_state, attempts=attempts, reason=reason
        )

    async def _notify_state_changed(
        self, resource_id: str, old: ResourceState, new: ResourceState, reason: str
    ) -> None:
        await self._state.notify_state_changed(resource_id, old, new, reason)

    async def _cas(
        self,
        resource_id: str,
        patch: ResourcePatch,
        *,
        expect_state: ResourceState | None = None,
        attempts: int = 3,
    ) -> bool:
        return await self._state.cas(
            resource_id, patch, expect_state=expect_state, attempts=attempts
        )

    async def ban(self, resource_id: str, *, reason: str, operator: str = "system") -> None:
        """Ban one resource after a hard failure or operator decision."""

        await self._admin.ban(resource_id, reason=reason, operator=operator)

    async def unban(
        self,
        resource_id: str,
        *,
        operator: str = "system",
        reason: str = "manual unban",
        revalidate: bool = True,
    ) -> None:
        """Lift a ban; ``revalidate=True`` parks it in VALIDATING first."""

        await self._admin.unban(
            resource_id, operator=operator, reason=reason, revalidate=revalidate
        )

    async def suspend(
        self, resource_id: str, *, operator: str = "system", reason: str
    ) -> None:
        """Temporarily remove a resource from allocation without banning it."""

        await self._admin.suspend(resource_id, operator=operator, reason=reason)

    async def resume(
        self,
        resource_id: str,
        *,
        operator: str = "system",
        reason: str = "manual resume",
        revalidate: bool = False,
    ) -> None:
        """Return a suspended resource to allocation (optionally re-validating)."""

        await self._admin.resume(
            resource_id, operator=operator, reason=reason, revalidate=revalidate
        )

    async def top_up(
        self,
        resource_id: str,
        *,
        operator: str = "system",
        amount: int | None = None,
        reason: str = "manual top_up",
    ) -> None:
        """Replenish a quota-exhausted resource and return it to IDLE."""

        await self._admin.top_up(resource_id, operator=operator, amount=amount, reason=reason)

    async def drain(
        self, resource_id: str, *, operator: str = "system", reason: str
    ) -> None:
        """Stop new acquires; the resource archives once its leases drain."""

        await self._admin.drain(resource_id, operator=operator, reason=reason)

    async def cancel_drain(
        self,
        resource_id: str,
        *,
        operator: str = "system",
        reason: str = "cancel drain",
    ) -> None:
        """Undo a drain and return the resource to allocation."""

        await self._admin.cancel_drain(resource_id, operator=operator, reason=reason)

    async def restore_resource(
        self,
        resource_id: str,
        *,
        operator: str = "system",
        reason: str = "manual restore",
    ) -> None:
        """Restore an archived resource through VALIDATING."""

        await self._admin.restore_resource(resource_id, operator=operator, reason=reason)

    async def admin_set_state(
        self,
        resource_id: str,
        *,
        target: ResourceState,
        operator: str = "system",
        reason: str,
    ) -> None:
        """Force a state the operator chooses; only safe targets are accepted."""

        await self._admin.admin_set_state(
            resource_id, target=target, operator=operator, reason=reason
        )

    async def force_revoke(
        self,
        resource_id: str,
        *,
        operator: str = "system",
        reason: str,
        token: str | None = None,
    ) -> Lease:
        """Revoke one live lease immediately and return the released snapshot."""

        return await self._admin.force_revoke(
            resource_id, operator=operator, reason=reason, token=token
        )

    async def update_resource(
        self,
        resource_id: str,
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
        priority: Any = _MISSING,
        preference: Any = _MISSING,
        operator: str = "system",
        reason: str,
    ) -> Resource:
        """Patch scheduling fields; omitted fields keep their value."""

        return await self._admin.update_resource(
            resource_id,
            payload=payload,
            tags=tags,
            max_slots=max_slots,
            usage_quota=usage_quota,
            quota_limit=quota_limit,
            expires_at=expires_at,
            next_available_at=next_available_at,
            mutex_group=mutex_group,
            schedule_window=schedule_window,
            priority=priority,
            preference=preference,
            operator=operator,
            reason=reason,
        )

    async def switch_resource(
        self,
        resource_id: str,
        *,
        operator: str = "system",
        reason: str = "manual switch",
    ) -> Resource | None:
        """Switch an ACTIVE resource to its alternate endpoint/payload."""

        return await self._admin.switch_resource(
            resource_id, operator=operator, reason=reason
        )

    async def move_resource(
        self,
        resource_id: str,
        target_pool: ResourcePool,
        *,
        operator: str = "system",
        reason: str,
    ) -> Resource:
        """Move a resource to another pool owned by the same Purser."""

        return await self._admin.move_resource(
            resource_id, target_pool, operator=operator, reason=reason
        )

    async def recall_batch(
        self,
        batch_id: str,
        *,
        action: str = "requalify",
        operator: str = "system",
        reason: str,
    ) -> dict[str, Any]:
        """Re-quarantine or archive resources from one import batch."""

        return await self._admin.recall_batch(
            batch_id, action=action, operator=operator, reason=reason
        )

    # -- periodic maintenance ---------------------------------------------

    async def run_maintenance(self, name: str) -> dict[str, int]:
        """Run one named maintenance sweep now and return its counters."""

        return await self._maintenance.run(name)

    # -- misc --------------------------------------------------------------

    @staticmethod
    def _schedule_open(resource: Resource, now: float) -> bool:
        if resource.schedule_window is None:
            return True
        start, end = resource.schedule_window
        current = time.strftime("%H:%M", time.localtime(now))
        if start <= end:
            return bool(start <= current <= end)
        return bool(current >= start or current <= end)  # overnight window


    async def acquire_many(
        self,
        n: int,
        *,
        req: AcquireRequest | None = None,
        mode: Literal["atomic", "best_effort"] = "atomic",
        n_min: int | None = None,
        timeout: float | None = None,
    ) -> BatchLease:
        """Acquire up to ``n`` leases without mutating ``req``.

        ``atomic`` releases everything when a step fails; ``best_effort`` keeps
        what it got and raises :class:`PoolExhausted` below ``n_min``
        (defaults to ``n``).  ``timeout`` overrides ``req.timeout``.
        """

        await self._ensure_started("acquire_many")
        if not isinstance(n, int) or isinstance(n, bool) or n <= 0:
            raise ConfigError("n must be a positive integer")
        if mode not in ("atomic", "best_effort"):
            raise ConfigError(
                f"unknown acquire mode {mode!r}; expected 'atomic' or 'best_effort'"
            )
        minimum = n if n_min is None else n_min
        if not isinstance(minimum, int) or isinstance(minimum, bool) or minimum <= 0:
            raise ConfigError("n_min must be a positive integer")
        if minimum > n:
            raise ConfigError(f"n_min ({minimum}) cannot exceed n ({n})")
        basis = req if req is not None else AcquireRequest()
        if not isinstance(basis.slots, int) or basis.slots <= 0:
            raise ConfigError("slots must be a positive integer")
        if timeout is not None:
            basis = replace(basis, timeout=timeout)
        leased: list[Lease] = []
        try:
            for _ in range(n):
                try:
                    leased.append(await self.acquire(basis))
                except (PoolExhausted, PoolEmpty):
                    if mode != "best_effort":
                        raise
                    break
            if len(leased) < minimum:
                raise PoolExhausted(
                    f"only got {len(leased)}/{n} (n_min={minimum})"
                )
            return BatchLease(leased)
        except Exception:
            for lease in reversed(leased):
                try:
                    await self.release(lease)
                except Exception as exc:
                    logger.warning(
                        "acquire_many rollback failed for pool %s: %s",
                        self.name,
                        exc,
                    )
            raise

    async def renew(self, lease: Lease, *, used: int | None = None) -> float:
        """Extend one lease and return its new ``expires_at``.

        ``used`` reports the holder's usage count; the lease keeps the higher
        of the two so retries and out-of-order reports cannot lower it.

        Raises :class:`LeaseLimitReached` when ``use_limit`` or
        ``max_lease_duration`` is exhausted and :class:`LeaseLost` when the
        coordination plane no longer knows the token.
        """

        await self._renew(lease, used)
        return float(lease.expires_at)

