"""Synchronous wrapper around a loop-owned ``ResourcePool``.

:class:`SyncPool` mirrors the async pool API for callers on any thread; every
call is marshalled through its owning :class:`~purser.sync.SyncPurser`.
"""

from __future__ import annotations

import copy
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Literal

from purser.core.resource import Lease, Resource, UsageOutcome
from purser.core.views import PoolStats
from purser.errors import IsolationConflict
from purser.facade import ImportReport, ResourcePool, coerce_usage_outcome
from purser.isolation import PICKLE_MESSAGE
from purser.sync_primitives import (
    SyncBatchLease,
    SyncLease,
    SyncResourceIterator,
    T,
    logger,
)

if TYPE_CHECKING:
    from purser.sync import SyncPurser


class SyncPool:
    """Synchronous wrapper around a loop-owned :class:`ResourcePool`."""

    def __init__(self, owner: SyncPurser, pool: ResourcePool) -> None:
        self._owner = owner
        self._pool = pool
        self.admin = SyncPoolAdminView(self)

    def __getstate__(self) -> Any:
        raise IsolationConflict(PICKLE_MESSAGE)

    # -- identity ---------------------------------------------------------
    @property
    def name(self) -> str:
        """Pool name as registered with the owning ``SyncPurser``."""

        return self._pool.name

    @property
    def kind(self) -> str:
        """Adapter/resource kind this pool stores."""

        return self._pool.cfg.kind

    @property
    def profile(self) -> str | None:
        """Optional profile name shared by this pool's resources."""

        return self._pool.profile

    @property
    def config(self) -> Any:
        return self._owner._call_sync(lambda: self._owner._snapshot(self._pool.cfg))

    @property
    def started(self) -> bool:
        if self._owner.closed:
            return False
        return self._owner._call_sync(lambda: self._pool.started)

    def __repr__(self) -> str:
        return f"SyncPool({self.name!r}, kind={self.kind!r})"

    # -- helpers ----------------------------------------------------------
    def _snapshot_call(
        self,
        factory: Callable[[], Awaitable[T]],
        timeout: float | None = None,
        *,
        allow_closing: bool = False,
    ) -> T:
        owner = self._owner

        async def run() -> T:
            return owner._snapshot(await factory())

        return owner._submit(run, timeout=timeout, allow_closing=allow_closing)

    def _sync_snapshot(self, factory: Callable[[], T]) -> T:
        owner = self._owner
        return owner._call_sync(lambda: owner._snapshot(factory()))

    def _proxy_for(self, lease: Lease) -> SyncLease:
        owner = self._owner
        return SyncLease(
            owner,
            self,
            lease,
            owner._snapshot(lease.payload),
            owner._snapshot(lease.resource),
        )

    def _acquire_wrapped(self, factory: Callable[[], Any], timeout: float | None = None) -> SyncLease:
        owner = self._owner
        pool = self._pool

        async def run() -> SyncLease:
            lease = await factory()
            try:
                return self._proxy_for(lease)
            except BaseException:
                try:
                    await pool.release(lease, outcome=lease.outcome, used=lease.used)
                except BaseException:
                    logger.exception("failed to compensate undeliverable lease %s", lease.token)
                raise

        return owner._submit(run, timeout=timeout)

    def _resolve(self, lease: SyncLease | Lease) -> Lease:
        if isinstance(lease, SyncLease):
            if lease._owner is not self._owner or lease._sync_pool is not self:
                raise ValueError("lease belongs to a different SyncPool")
            return lease._lease
        if isinstance(lease, Lease):
            token = lease.token
            return self._owner._call_sync(
                lambda: self._pool._leases.get(token) or lease
            )
        raise TypeError("release requires a SyncLease or Lease")

    # -- lifecycle --------------------------------------------------------
    def start(self) -> None:
        """Start the underlying pool tasks if they are not running."""

        self._owner._submit(self._pool.start)

    def close(self) -> None:
        """Stop the underlying pool and forget this handle."""

        self._owner._submit(self._pool.close)
        self._owner._forget_pool(self)

    def reconfigure(self, **updates: Any) -> Any:
        """Apply mutable pool-config fields and return the new config."""

        copied = {key: copy.deepcopy(value) for key, value in updates.items()}
        return self._sync_snapshot(lambda: self._pool.reconfigure(**copied))

    # -- validation jobs / describe --------------------------------------
    def validate_jobs(self) -> Any:
        return self._sync_snapshot(self._pool.validate_jobs)

    def validate_job(self, job_id: str) -> Any:
        return self._sync_snapshot(lambda: self._pool.validate_job(job_id))

    def cancel_validate_job(self, job_id: str) -> Any:
        return self._sync_snapshot(lambda: self._pool.cancel_validate_job(job_id))

    def describe(self, resource: Resource, *, secret: bool = False) -> dict[str, Any]:
        """Public dict view of ``resource``; payload fields are masked by default."""

        copied = copy.deepcopy(resource)
        return self._sync_snapshot(
            lambda: self._pool.describe(copied, secret=secret)
        )

    # -- reads ------------------------------------------------------------
    def get(self, rid: str) -> Resource | None:
        """Return one resource snapshot, or ``None`` when it does not exist."""

        return self._snapshot_call(lambda: self._pool.get(rid))

    def require(self, rid: str) -> Resource:
        """Return one resource snapshot or raise :class:`ResourceNotFound`."""

        return self._snapshot_call(lambda: self._pool.require(rid))

    def query(self, **kwargs: Any) -> list[Resource]:
        """List resources matching ``states``/``tags``/``search`` filters."""

        copied = copy.deepcopy(kwargs)
        return self._snapshot_call(lambda: self._pool.query(**copied))

    def iter_resources(self, **kwargs: Any) -> SyncResourceIterator:
        """Stream matching resources in storage-friendly batches."""

        copied = copy.deepcopy(kwargs)
        agen = self._owner._call_sync(
            lambda: self._pool.iter_resources(**copied)
        )
        return SyncResourceIterator(self._owner, agen)

    def count_matching(self, **kwargs: Any) -> int:
        """Count resources matching the same filters as :meth:`query`."""

        copied = copy.deepcopy(kwargs)
        return self._snapshot_call(lambda: self._pool.count_matching(**copied))

    def batches(self) -> list[dict[str, Any]]:
        """Import batches recorded by this pool."""

        return self._snapshot_call(self._pool.batches)

    def counts(self, *, by_name: bool = False) -> dict[Any, int]:
        """State histogram (or per-state-name counts when ``by_name``)."""

        if by_name:
            return self._snapshot_call(lambda: self._pool.counts(by_name=True))
        return self._snapshot_call(lambda: self._pool.counts())

    def stats(self) -> PoolStats:
        """Totals, state counts, allocatable slots and health summary."""

        return self._snapshot_call(self._pool.stats)

    def leases(self, rid: str | None = None) -> list[SyncLease]:
        """Live leases, optionally restricted to one resource."""

        owner = self._owner
        pool = self._pool

        async def run() -> list[SyncLease]:
            leases = await pool.leases(rid)
            return [self._proxy_for(lease) for lease in leases]

        return owner._submit(run)

    # -- import / generate / validation ----------------------------------
    def import_resources(
        self,
        records: Any,
        *,
        dedup_key: str = "id",
        source: str = "manual",
        operator: str = "system",
        fail_fast: bool = False,
        import_validate: str | None = None,
        batch_id: str | None = None,
    ) -> ImportReport:
        """Bulk import dict specs or Resources with dedup and optional validation."""

        copied = copy.deepcopy(records)
        return self._snapshot_call(
            lambda: self._pool.import_resources(
                copied,
                dedup_key=dedup_key,
                source=source,
                operator=operator,
                fail_fast=fail_fast,
                import_validate=import_validate,
                batch_id=batch_id,
            )
        )

    def generate(
        self,
        specs: Any,
        *,
        source: str = "builtin",
        operator: str = "system",
    ) -> ImportReport:
        """Build resources from adapter specs and import them with validation."""

        copied = copy.deepcopy(specs)
        return self._snapshot_call(
            lambda: self._pool.generate(copied, source=source, operator=operator)
        )

    # -- acquire / release ------------------------------------------------
    def acquire(self, req: Any = None, **kwargs: Any) -> SyncLease:
        """Acquire one lease and return a blocking handle."""

        copied = copy.deepcopy(req) if req is not None else None
        copied_kwargs = copy.deepcopy(kwargs)
        return self._acquire_wrapped(
            lambda: self._pool.acquire(copied, **copied_kwargs)
        )

    def acquire_many(
        self,
        n: int,
        *,
        req: Any = None,
        mode: Literal["atomic", "best_effort"] = "atomic",
        n_min: int | None = None,
        timeout: float | None = None,
    ) -> SyncBatchLease:
        """Acquire up to ``n`` leases without mutating ``req``."""

        copied = copy.deepcopy(req) if req is not None else None
        owner = self._owner
        pool = self._pool

        async def run() -> SyncBatchLease:
            batch = await pool.acquire_many(
                n, req=copied, mode=mode, n_min=n_min, timeout=timeout
            )
            try:
                leases = [self._proxy_for(lease) for lease in batch.leases]
            except BaseException:
                try:
                    await batch.release_all(pool)
                except BaseException:
                    logger.exception("failed to compensate undeliverable batch leases")
                raise
            return SyncBatchLease(owner, self, leases)

        return owner._submit(run)

    def release(
        self,
        lease: SyncLease | Lease,
        *,
        outcome: UsageOutcome | str | None = None,
        used: int | None = None,
        latency_ms: float | None = None,
    ) -> bool:
        """Return a lease and optionally report its outcome, usage and latency."""

        outcome = coerce_usage_outcome(outcome)
        authoritative = self._resolve(lease)
        result = self._owner._submit(
            lambda: self._pool.release(
                authoritative, outcome=outcome, used=used, latency_ms=latency_ms
            ),
            allow_closing=True,
        )
        if isinstance(lease, SyncLease):
            lease._mark_released()
        return result

    def release_consumer(
        self,
        consumer: str,
        *,
        outcome: UsageOutcome | str = UsageOutcome.OK,
        operator: str = "system",
    ) -> list[Lease]:
        """Release every live lease held by ``consumer``."""

        outcome = coerce_usage_outcome(outcome) or UsageOutcome.OK
        return self._snapshot_call(
            lambda: self._pool.release_consumer(
                consumer, outcome=outcome, operator=operator
            ),
            allow_closing=True,
        )

    @contextmanager
    def lease(self, req: Any = None, **kwargs: Any) -> Iterator[SyncLease]:
        """Acquire now and always release at block exit."""

        lease = self.acquire(req, **kwargs)
        try:
            yield lease
        finally:
            self.release(lease, outcome=lease.outcome, used=lease.used)

    def renew(self, lease: SyncLease | Lease, *, used: int | None = None) -> float:
        """Extend one lease and return its new ``expires_at``."""

        authoritative = self._resolve(lease)
        return self._owner._submit(
            lambda: self._pool.renew(authoritative, used=used)
        )


class SyncPoolAdminView:
    """Operator namespace mirroring ``ResourcePool.admin`` for sync callers."""

    def __init__(self, sync_pool: SyncPool) -> None:
        self._sp = sync_pool

    # -- validation --------------------------------------------------------

    def apply_defaults(
        self,
        *,
        operator: str = "system",
        reason: str = "apply pool defaults",
    ) -> dict[str, int]:
        """Backfill pool defaults onto resources that lack them."""

        return self._sp._snapshot_call(
            lambda: self._sp._pool.admin.apply_defaults(operator=operator, reason=reason)
        )

    def validate_now(self, rid: str, *, operator: str = "system") -> Any:
        """Validate one resource immediately and apply the verdict."""

        return self._sp._snapshot_call(
            lambda: self._sp._pool.admin.validate_now(rid, operator=operator)
        )

    def validate_check(self, rid: str, check: str) -> Any:
        """Run one named check without changing the resource state."""

        return self._sp._snapshot_call(
            lambda: self._sp._pool.admin.validate_check(rid, check)
        )

    # -- state / resource management --------------------------------------

    def ban(self, rid: str, *, reason: str, operator: str = "system") -> None:
        """Ban one resource after a hard failure or operator decision."""

        self._sp._owner._submit(
            lambda: self._sp._pool.admin.ban(rid, reason=reason, operator=operator)
        )

    def unban(
        self,
        rid: str,
        *,
        operator: str = "system",
        reason: str = "manual unban",
        revalidate: bool = True,
    ) -> None:
        """Lift a ban; ``revalidate=True`` parks it in VALIDATING first."""

        self._sp._owner._submit(
            lambda: self._sp._pool.admin.unban(
                rid, operator=operator, reason=reason, revalidate=revalidate
            )
        )

    def suspend(
        self, rid: str, *, operator: str = "system", reason: str
    ) -> None:
        """Temporarily remove a resource from allocation without banning it."""

        self._sp._owner._submit(
            lambda: self._sp._pool.admin.suspend(rid, operator=operator, reason=reason)
        )

    def resume(
        self,
        rid: str,
        *,
        operator: str = "system",
        reason: str = "manual resume",
        revalidate: bool = False,
    ) -> None:
        """Return a suspended resource to allocation (optionally re-validating)."""

        self._sp._owner._submit(
            lambda: self._sp._pool.admin.resume(
                rid, operator=operator, reason=reason, revalidate=revalidate
            )
        )

    def top_up(
        self,
        rid: str,
        *,
        operator: str = "system",
        amount: int | None = None,
        reason: str = "manual top_up",
    ) -> None:
        """Replenish a quota-exhausted resource and return it to IDLE."""

        self._sp._owner._submit(
            lambda: self._sp._pool.admin.top_up(
                rid, operator=operator, amount=amount, reason=reason
            )
        )

    def drain(self, rid: str, *, operator: str = "system", reason: str) -> None:
        """Stop new acquires; the resource archives once its leases drain."""

        self._sp._owner._submit(
            lambda: self._sp._pool.admin.drain(rid, operator=operator, reason=reason)
        )

    def cancel_drain(
        self,
        rid: str,
        *,
        operator: str = "system",
        reason: str = "cancel drain",
    ) -> None:
        """Undo a drain and return the resource to allocation."""

        self._sp._owner._submit(
            lambda: self._sp._pool.admin.cancel_drain(
                rid, operator=operator, reason=reason
            )
        )

    def restore_resource(
        self, rid: str, *, operator: str = "system", reason: str = "manual restore"
    ) -> None:
        """Restore an archived resource through VALIDATING."""

        self._sp._owner._submit(
            lambda: self._sp._pool.admin.restore_resource(
                rid, operator=operator, reason=reason
            )
        )

    def admin_set_state(
        self,
        rid: str,
        *,
        target: Any,
        operator: str = "system",
        reason: str,
    ) -> None:
        """Force a state the operator chooses; only safe targets are accepted."""

        self._sp._owner._submit(
            lambda: self._sp._pool.admin.admin_set_state(
                rid, target=target, operator=operator, reason=reason
            )
        )

    def force_revoke(
        self,
        rid: str,
        *,
        operator: str = "system",
        reason: str,
        token: str | None = None,
    ) -> Lease:
        """Revoke one live lease immediately and return the released snapshot."""

        return self._sp._snapshot_call(
            lambda: self._sp._pool.admin.force_revoke(
                rid, operator=operator, reason=reason, token=token
            )
        )

    def update_resource(
        self,
        rid: str,
        *,
        payload: dict[str, Any] | None = None,
        tags: Any = None,
        max_slots: int | None = None,
        operator: str = "system",
        reason: str,
        **optional: Any,
    ) -> Resource:
        """Patch scheduling fields; omitted fields keep their value."""

        kwargs: dict[str, Any] = {
            "payload": copy.deepcopy(payload),
            "tags": copy.deepcopy(tags),
            "max_slots": max_slots,
            "operator": operator,
            "reason": reason,
        }
        kwargs.update(copy.deepcopy(optional))
        return self._sp._snapshot_call(
            lambda: self._sp._pool.admin.update_resource(rid, **kwargs)
        )

    def switch_resource(
        self,
        rid: str,
        *,
        operator: str = "system",
        reason: str,
    ) -> Resource | None:
        """Switch an ACTIVE resource to its alternate endpoint/payload."""

        return self._sp._snapshot_call(
            lambda: self._sp._pool.admin.switch_resource(
                rid, operator=operator, reason=reason
            )
        )

    def move_resource(
        self,
        rid: str,
        target_pool: SyncPool,
        *,
        operator: str = "system",
        reason: str,
    ) -> Resource:
        """Move a resource to another pool owned by the same SyncPurser."""

        if not isinstance(target_pool, SyncPool):
            raise TypeError("target_pool must be a SyncPool")
        if target_pool._owner is not self._sp._owner:
            raise ValueError("target_pool belongs to a different SyncPurser")
        return self._sp._snapshot_call(
            lambda: self._sp._pool.admin.move_resource(
                rid, target_pool._pool, operator=operator, reason=reason
            )
        )

    def recall_batch(
        self,
        batch_id: str,
        *,
        action: str = "requalify",
        operator: str = "system",
        reason: str,
    ) -> dict[str, Any]:
        """Re-quarantine or archive resources from one import batch."""

        return self._sp._snapshot_call(
            lambda: self._sp._pool.admin.recall_batch(
                batch_id, action=action, operator=operator, reason=reason
            )
        )

    # -- maintenance ------------------------------------------------------

    def run_maintenance(self, name: str) -> dict[str, int]:
        """Run one named maintenance sweep now and return its counters."""

        return self._sp._snapshot_call(lambda: self._sp._pool.admin.run_maintenance(name))
