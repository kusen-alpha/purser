"""Health validation: single probes, background sampling and batch jobs."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, cast

from purser.backends.base import ResourcePatch
from purser.config import PoolConfig
from purser.core.health import HealthReport, ValidationGate
from purser.core.resource import SiteFailure
from purser.core.state import ResourceState
from purser.core.views import ValidateJobCancelView, ValidateJobView
from purser.errors import BadState, ConfigError
from purser.facade._helpers import _now
from purser.pool.context import PoolContext, require_resource
from purser.pool.state_ops import StateOps
from purser.pool.validation_jobs import ValidationJobs

logger = logging.getLogger("purser")


class ValidationService:
    def __init__(
        self,
        ctx: PoolContext,
        state: StateOps,
        *,
        gate: ValidationGate,
        semaphore: asyncio.Semaphore,
        jobs: ValidationJobs,
        spawn: Callable[[Any], asyncio.Task[Any]],
        serve_waiters: Callable[[], Awaitable[Any]] | None = None,
    ) -> None:
        self.ctx = ctx
        self.state = state
        self._gate = gate
        self._sem = semaphore
        self._jobs = jobs
        self._queues: dict[str, asyncio.Queue[str]] = {}
        self._spawn = spawn
        self._serve_waiters = serve_waiters

    def bind_waker(self, waker: Callable[[], Awaitable[Any]]) -> None:
        self._serve_waiters = waker

    def reload(self, cfg: PoolConfig) -> None:
        self._gate = ValidationGate(
            stale_after=cfg.stale_after,
            background_interval=cfg.background_interval,
            sample=cfg.background_validate_sample,
        )
        self._sem = asyncio.Semaphore(cfg.validate_concurrency)

    # -- background sampling ----------------------------------------------

    async def background_loop(self) -> None:
        while True:
            await asyncio.sleep(max(1.0, self.ctx.cfg.background_interval))
            try:
                await self.background_stale()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "background validation failed for pool %s", self.ctx.name
                )

    async def background_stale(self, budget: int = 32) -> int:
        """Sample stale IDLE resources and refresh them proactively."""

        checked = 0
        async for resource in self.ctx.storage.scan(
            self.ctx.name, states={ResourceState.IDLE}
        ):
            if checked >= budget:
                break
            if not self._gate.due_background(resource):
                continue
            checked += 1
            await self._validate_in_background(resource.id)
        return checked

    async def _validate_in_background(self, rid: str) -> None:
        try:
            await self.validate_now(rid)
        except Exception:
            logger.exception("background validate %s failed", rid)

    # -- batch validate jobs ----------------------------------------------

    def start_job(
        self, job_id: str, rids: Sequence[str], operator: str
    ) -> ValidateJobView:
        """Track a batch validation run under ``job_id`` (the import batch id).

        Workers pull from a shared queue so a cancellation stops pending
        items while in-flight probes finish normally.
        """

        now = _now()
        existing = self._jobs.get(job_id)
        queue = self._queues.get(job_id)
        if existing is not None and queue is not None and existing["status"] in (
            "running",
            "cancelling",
        ):
            for rid in rids:
                queue.put_nowait(rid)
            existing["total"] += len(rids)
            existing["updated_at"] = now
            return self._view(existing)
        self._prune()
        job = {
            "id": job_id,
            "pool": self.ctx.name,
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
        self._jobs.put(job_id, job)
        queue = asyncio.Queue()
        self._queues[job_id] = queue
        for rid in rids:
            queue.put_nowait(rid)
        workers = max(1, min(self.ctx.cfg.validate_concurrency, len(rids)))
        for _ in range(workers):
            self._spawn(self._worker(job_id))
        self.ctx.bus.publish(
            "validate_job_started",
            self.ctx.name,
            data={"job_id": job_id, "total": job["total"]},
        )
        return self._view(job)

    async def _worker(self, job_id: str) -> None:
        queue = self._queues.get(job_id)
        job = self._jobs.get(job_id)
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
                self._finish_job(job_id)
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
                self._finish_job(job_id)

    def _finish_job(self, job_id: str) -> None:
        job = self._jobs.get(job_id)
        if job is None or job["done"] < job["total"]:
            return
        if job["status"] == "running" or job["status"] == "cancelling":
            job["status"] = "cancelled" if job["cancelled"] else "completed"
            job["updated_at"] = _now()
            self.ctx.bus.publish(
                "validate_job_finished",
                self.ctx.name,
                data={
                    "job_id": job_id,
                    "status": job["status"],
                    "alive": job["alive"],
                    "failed": job["failed"],
                    "skipped": job["skipped"],
                },
            )

    def _view(self, job: dict[str, Any]) -> ValidateJobView:
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

    def jobs(self) -> list[ValidateJobView]:
        return [
            self._view(job)
            for job in sorted(
                self._jobs.values(),
                key=lambda item: item["created_at"],
                reverse=True,
            )
        ]

    def job(self, job_id: str) -> ValidateJobView | None:
        job = self._jobs.get(job_id)
        return None if job is None else self._view(job)

    def cancel_job(self, job_id: str) -> ValidateJobCancelView | None:
        job = self._jobs.get(job_id)
        if job is None:
            return None
        if job["status"] not in ("running", "cancelling"):
            view = self._view(job)
            return {**view, "cancelled": False}
        job["cancelled"] = True
        job["status"] = "cancelling" if job["done"] < job["total"] else "cancelled"
        job["updated_at"] = _now()
        queue = self._queues.get(job_id)
        if queue is not None:
            while not queue.empty():
                queue.get_nowait()
                job["skipped"] += 1
                job["done"] += 1
        self._finish_job(job_id)
        return {**self._view(job), "cancelled": True}

    def _prune(self, keep: int = 100) -> None:
        terminal = self._jobs.terminal_ids()
        for job_id in terminal[:-keep]:
            self._jobs.discard(job_id)
            self._queues.pop(job_id, None)

    # -- probes -----------------------------------------------------------

    async def validate_now(
        self,
        rid: str,
        *,
        operator: str = "system",
    ) -> HealthReport:
        resource = await require_resource(self.ctx, rid)
        now = _now()
        state = resource.state
        if state is ResourceState.BANNED:
            raise BadState("banned resources must go through unban()")
        if state in (
            ResourceState.DRAINING,
            ResourceState.ARCHIVED,
            ResourceState.SUSPENDED,
            ResourceState.QUOTA_EXHAUSTED,
        ):
            raise BadState(f"cannot validate {state.value} resource")
        if state is ResourceState.ACTIVE:
            pass  # validate in place, no state change
        elif state in (ResourceState.NEW, ResourceState.IDLE):
            entered = await self.ctx.storage.set_state(
                self.ctx.name,
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
                resource = await require_resource(self.ctx, rid)
            else:
                resource.state = ResourceState.VALIDATING
                await self.state.notify_state_changed(
                    rid, state, ResourceState.VALIDATING, "validate"
                )

        report: HealthReport
        async with self._sem:
            try:
                report = await self.ctx.adapter.validate_with_checks(resource)
            except Exception as exc:
                self.ctx.guard_error("validate", exc)
                report = HealthReport.fail(str(exc))
        self.ctx.bus.publish(
            "health_reported",
            self.ctx.name,
            rid=rid,
            data={
                "alive": report.alive,
                "latency_ms": report.latency_ms,
                "error": report.error,
            },
        )
        if report.alive:
            await self.on_ok(rid, report)
        else:
            await self.on_failure(
                rid,
                report.error or "validation failed",
                operator=operator,
                from_states={
                    ResourceState.VALIDATING,
                    ResourceState.ACTIVE,
                    ResourceState.IDLE,
                    ResourceState.NEW,
                },
            )
        return report

    async def validate_check(self, rid: str, check: str) -> HealthReport:
        """Run one named check in isolation; never writes health or state."""

        resource = await require_resource(self.ctx, rid)
        try:
            return cast(HealthReport, await self.ctx.adapter.run_validator(resource, check))
        except ConfigError:
            raise
        except Exception as exc:
            self.ctx.guard_error("validate_check", exc)
            return HealthReport.fail(str(exc))

    async def on_ok(self, rid: str, report: HealthReport) -> None:
        for _attempt in range(2):
            resource = await self.ctx.storage.get(self.ctx.name, rid)
            if resource is None or resource.state is ResourceState.DRAINING:
                return
            health = resource.health
            health.update(alive=True, latency_ms=report.latency_ms, now=_now())
            meta = dict(resource.meta)
            meta["last_validated_at"] = health.last_checked_at
            meta["needs_validate"] = 0
            patch = ResourcePatch(
                state=(
                    ResourceState.IDLE
                    if resource.state is ResourceState.VALIDATING
                    else None
                ),
                health=health,
                meta_patch=meta,
            )
            ok = await self.state.apply_state(
                rid, patch, expect_state=resource.state, reason="validate_ok"
            )
            if ok:
                self.ctx.bus.publish("validate_ok", self.ctx.name, rid=rid)
                if self._serve_waiters is not None:
                    await self._serve_waiters()
                return

    async def on_failure(
        self,
        rid: str,
        reason: str,
        *,
        operator: str = "system",
        from_states: set[ResourceState] | None = None,
        site_failures: dict[str, SiteFailure] | None = None,
    ) -> None:
        for _attempt in range(3):
            resource = await self.ctx.storage.get(self.ctx.name, rid)
            if resource is None:
                return
            if from_states and resource.state not in from_states:
                return
            now = _now()
            health = resource.health
            health.update(alive=False, error=reason, now=now)
            policy = self.ctx.cfg.quarantine
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
            ok = await self.state.apply_state(
                rid,
                patch,
                expect_state=resource.state,
                reason=f"validation failed: {reason}",
            )
            if ok:
                event = "banned" if target is ResourceState.BANNED else "quarantined"
                self.ctx.bus.publish(
                    event, self.ctx.name, rid=rid, data={"reason": reason}
                )
                if target is ResourceState.BANNED:
                    self.ctx.audit.record(
                        operator=operator,
                        action="ban",
                        pool=self.ctx.name,
                        rid=rid,
                        reason=reason,
                    )
                return
