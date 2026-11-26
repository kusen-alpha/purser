"""Lease acquisition, waiting, renewal and release."""

from __future__ import annotations

import asyncio
import contextlib
import heapq
import logging
import math
import random
import time
from collections import OrderedDict, defaultdict
from collections.abc import Awaitable, Callable
from copy import deepcopy
from dataclasses import replace
from typing import Any, cast

from purser.core.allocator import AllocationContext
from purser.core.resource import (
    AcquireRequest,
    CheckVerdict,
    Lease,
    Resource,
    SiteFailure,
    UsageOutcome,
)
from purser.core.state import ALLOCATABLE_STATES, ResourceState
from purser.core.views import ValidateJobView
from purser.errors import (
    BackendUnavailable,
    BadState,
    NoSuitableResource,
    PoolEmpty,
    PoolExhausted,
    ResourceBusy,
    ResourceNotFound,
)
from purser.facade._helpers import (
    _now,
    _TokenBucket,
    _WaitHandle,
)
from purser.pool.context import PoolContext, coordination_degraded
from purser.pool.release import ReleaseService
from purser.pool.state_ops import StateOps
from purser.pool.usage import UsageService
from purser.pool.validation import ValidationService
from purser.pool.wait_queue import WaitQueue

logger = logging.getLogger("purser")

RenewFn = Callable[[Lease, int | None], Awaitable[Any]]
RevokeFn = Callable[..., Awaitable[Any]]


class LeaseService:
    def __init__(
        self,
        ctx: PoolContext,
        state: StateOps,
        usage: UsageService,
        validation: ValidationService,
        *,
        wait_queue: WaitQueue,
        counts: Callable[[], Awaitable[dict[Any, int]]],
        schedule_open: Callable[[Resource, float], bool],
    ) -> None:
        self.ctx = ctx
        self.state = state
        self.usage = usage
        self.validation = validation
        self.wait_queue = wait_queue
        self._counts = counts
        self._is_schedule_open = schedule_open
        self.consumer_holds: dict[str, set[str]] = defaultdict(set)
        self.affinity: dict[str, dict[str, float]] = defaultdict(dict)
        self._check_cache: dict[tuple[str, str, str], tuple[CheckVerdict, float]] = {}
        self._stateless_leases: OrderedDict[str, Lease] = OrderedDict()
        self._idem_inflight: dict[str, asyncio.Future[Lease]] = {}
        self._rate_buckets: dict[str, _TokenBucket] = {}
        # Tokens granted while coordination was degraded (fail_open): they were
        # never registered remotely, so their teardown must stay local too.
        self._degraded_tokens: set[str] = set()
        self._renew_cb: RenewFn | None = None
        self._revoker: RevokeFn | None = None
        # One serving pass at a time.  The tick and the inline release path
        # both drain the heap; without this lock a release could hand the heap
        # head's slot to a later waiter while the head is mid-allocation.
        self._serve_lock = asyncio.Lock()
        self.releases = ReleaseService(self)

    def bind(self, *, renew: RenewFn, revoker: RevokeFn) -> None:
        self._renew_cb = renew
        self._revoker = revoker

    def is_degraded(self, token: str) -> bool:
        return token in self._degraded_tokens

    def reset_rate_limits(self) -> None:
        self._rate_buckets.clear()

    async def _revoke(
        self, rid: str, *, operator: str, reason: str, token: str | None = None
    ) -> Any:
        assert self._revoker is not None
        return await self._revoker(
            rid, operator=operator, reason=reason, token=token
        )

    async def reap_loop(self) -> None:
        interval = max(1.0, min(self.ctx.cfg.lease_ttl / 2.0, 5.0))
        while True:
            await asyncio.sleep(interval)
            try:
                await self.ctx.leases.reap()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("reaper loop failed for pool %s", self.ctx.name)

    _SERVE_INTERVAL = 0.1

    async def serve_loop(self) -> None:
        """One serving loop per pool; every waiter is granted here.

        A merged loop means an acquire never allocates on its own tick: the
        heap is drained in priority order by this task, so a burst of waiters
        cannot stampede the storage plane, and a release on another instance
        is picked up on the next tick instead of by a per-waiter full scan.
        """

        while True:
            await asyncio.sleep(self._SERVE_INTERVAL)
            try:
                await self.serve_waiters()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("wake serving failed for pool %s", self.ctx.name)

    async def acquire(self, request: AcquireRequest) -> Lease:
        if request.idempotency_key is not None:
            return await self._acquire_idempotent(request)
        await self._admit_rate(request)
        return await self._acquire_request(request)

    async def _acquire_idempotent(self, request: AcquireRequest) -> Lease:
        key = f"acquire:{self.ctx.name}:{request.idempotency_key}"
        cached = await self.ctx.purser.idempotency.aget(key)
        token = cached.get("token") if isinstance(cached, dict) else None
        if isinstance(token, str):
            replayed = self._lease_by_token(token)
            if replayed is not None:
                self.ctx.purser.metrics.inc(f"pool.{self.ctx.name}.acquire_replayed")
                return replayed
        inflight = self._idem_inflight.get(key)
        if inflight is not None:
            return await asyncio.shield(inflight)
        future: asyncio.Future[Lease] = asyncio.get_running_loop().create_future()
        self._idem_inflight[key] = future
        try:
            await self._admit_rate(request)
            lease = await self._acquire_request(request)
        except BaseException as exc:
            future.set_exception(exc)
            future.exception()  # mark retrieved; a cancelled waiter must not warn
            raise
        else:
            await self.ctx.purser.idempotency.aput(key, {"token": lease.token})
            future.set_result(lease)
            return lease
        finally:
            self._idem_inflight.pop(key, None)

    def _lease_by_token(self, token: str) -> Lease | None:
        internal = self.ctx.leases.get(token)
        if internal is not None:
            return cast("Lease", internal)
        return self._stateless_leases.get(token)

    def _rate_bucket(self, consumer: str | None) -> _TokenBucket:
        key = consumer or ""
        bucket = self._rate_buckets.get(key)
        if bucket is None:
            rate = float(self.ctx.cfg.rate_limit or 1.0)
            burst = self.ctx.cfg.rate_burst or max(1, math.ceil(rate))
            if len(self._rate_buckets) >= 4096:
                self._rate_buckets.pop(next(iter(self._rate_buckets)))
            bucket = _TokenBucket(rate, burst)
            self._rate_buckets[key] = bucket
        return bucket

    async def _admit_rate(self, request: AcquireRequest) -> None:
        """Token-bucket admission for ``scheduler="token_bucket"``."""

        if self.ctx.cfg.scheduler != "token_bucket" or self.ctx.cfg.rate_limit is None:
            return
        timeout = request.timeout if request.timeout is not None else self.ctx.cfg.wait_timeout
        bucket = self._rate_bucket(request.consumer)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + max(0.0, timeout)
        while True:
            now = loop.time()
            if bucket.ready(now):
                bucket.consume(now)
                return
            wait = bucket.wait_time(now)
            remaining = deadline - now
            if remaining <= 0:
                raise PoolExhausted(
                    f"pool {self.ctx.name} rate limited for consumer "
                    f"{request.consumer!r} (rate_limit={self.ctx.cfg.rate_limit}/s, "
                    f"retry after {wait:.2f}s)"
                )
            await asyncio.sleep(min(wait, remaining))

    async def _acquire_request(self, request: AcquireRequest) -> Lease:
        timeout = request.timeout if request.timeout is not None else self.ctx.cfg.wait_timeout
        if request.resource_id is not None:
            return await self._acquire_directed(request, timeout)

        lease = await self._try_allocate(request)
        if lease is not None:
            return lease

        if await self._try_preempt(request):
            lease = await self._try_allocate(request)
            if lease is not None:
                return lease

        counts = await self._counts()
        total = sum(counts.values())
        if total == 0:
            raise PoolEmpty(f"pool {self.ctx.name} has no allocatable resources")
        new = counts.get(ResourceState.NEW, 0)
        validating = counts.get(ResourceState.VALIDATING, 0)
        if new + validating == total:
            job = self._active_validate_job()
            if validating == 0 and job is None:
                raise PoolExhausted(
                    f"pool {self.ctx.name} has {total} resource(s) but none is validated "
                    "(all in NEW); the pool was imported with import_validate='none'. "
                    "Call await pool.admin.validate_now(<id>) per resource, or re-import "
                    "with import_validate='batch'."
                )
            if timeout <= 0:
                raise PoolExhausted(self._validation_wait_message(counts, job))
            # Validation is in progress; _on_validation_ok() wakes waiters.
        elif timeout <= 0:
            raise PoolExhausted(await self._exhausted_message(request, 0.0))

        handle = self._enqueue_waiter(request)
        current = asyncio.current_task()
        if current is not None:
            current.add_done_callback(lambda _task: handle.cancel_pending())
        try:
            return await self._await_slot(handle, request, timeout)
        except asyncio.CancelledError:
            await self._abandon_wait_handle(handle)
            raise

    def _active_validate_job(self) -> ValidateJobView | None:
        for job in self.validation.jobs():
            if job["status"] in ("running", "cancelling"):
                return job
        return None

    def _validation_wait_message(
        self, counts: dict[ResourceState, int], job: ValidateJobView | None
    ) -> str:
        new = counts.get(ResourceState.NEW, 0)
        validating = counts.get(ResourceState.VALIDATING, 0)
        message = (
            f"pool {self.ctx.name} has {new + validating} unvalidated resource(s)"
            f" (new={new}, validating={validating})"
        )
        if job is not None:
            message += (
                f"; validate job {job['id']} is {job['done']}/{job['total']} done"
                f" ({job['status']})"
            )
        message += (
            "; wait for validation, await pool.validate_jobs(), or raise"
            " AcquireRequest.timeout / wait_timeout"
        )
        return message

    async def _exhausted_message(
        self, request: AcquireRequest, timeout: float
    ) -> str:
        counts = await self._counts()
        state_hint = ", ".join(
            f"{state.value}={n}" for state, n in counts.items() if n
        )
        filters = []
        if request.tags:
            filters.append(f"tags={sorted(request.tags)}")
        if request.required_tag:
            filters.append(f"required_tag={request.required_tag!r}")
        filter_hint = f", {', '.join(filters)}" if filters else ""
        message = (
            f"pool {self.ctx.name} exhausted after {timeout:.1f}s"
            f" (consumer={request.consumer!r}, slots={request.slots}{filter_hint},"
            f" states: {state_hint})"
        )
        new = counts.get(ResourceState.NEW, 0)
        validating = counts.get(ResourceState.VALIDATING, 0)
        if new or validating:
            message += (
                f"; {new + validating} resource(s) not validated yet"
                f" (new={new}, validating={validating})"
            )
        reasons, truncated = await self._skip_reasons(request)
        if reasons:
            detail = ", ".join(f"{reason}={n}" for reason, n in reasons.items())
            message += f"; allocatable resources skipped: {detail}"
            if truncated:
                message += f" (first {self._SKIP_SCAN_LIMIT} scanned)"
        return message

    _SKIP_SCAN_LIMIT = 2000
    _STATELESS_MEMORY = 2048

    async def _skip_reasons(
        self, request: AcquireRequest
    ) -> tuple[dict[str, int], bool]:
        """Explain why allocatable resources did not match this request.

        Mirrors ``_hard_filtered_candidates`` without side effects; only used
        on the failure path so the extra scan never sits on the hot path.
        """

        now = _now()
        reasons: dict[str, int] = {}
        if self._consumer_at_limit(request.consumer):
            return {"consumer_limit": 1}, False
        scanned = 0
        truncated = False
        async for resource in self.ctx.storage.scan(
            self.ctx.name, states=set(ALLOCATABLE_STATES)
        ):
            if scanned >= self._SKIP_SCAN_LIMIT:
                truncated = True
                break
            scanned += 1
            reason = await self._skip_reason(resource, request, now)
            reasons[reason] = reasons.get(reason, 0) + 1
        return reasons, truncated

    async def _skip_reason(
        self, resource: Resource, request: AcquireRequest, now: float
    ) -> str:
        stateless = self.ctx.cfg.acquire_mode == "stateless"
        if not stateless and resource.concurrency.available < request.slots:
            return "slots"
        if request.tags and not request.tags <= resource.tags:
            return "tags"
        if request.required_tag and request.required_tag not in resource.tags:
            return "required_tag"
        if not self._is_schedule_open(resource, now):
            return "schedule_window"
        if resource.expires_at is not None and resource.expires_at <= now:
            return "expired"
        if (
            not request.ignore_cooldown
            and resource.usable_at(now, request.consumer) > now
        ):
            return "cooldown"
        if request.target and resource.site_failures.get(
            request.target, SiteFailure(request.target, until=0)
        ).active(now):
            return "site_failure"
        if (
            resource.usage_quota is not None
            and resource.used_count >= resource.usage_quota
        ):
            return "usage_quota"
        if resource.quota is not None and resource.quota.exhausted:
            return "quota"
        if (
            not stateless
            and resource.mutex_group
            and request.consumer
            and await self._holds_group(
                request.consumer, resource.mutex_group, resource.id
            )
        ):
            return "mutex_group"
        if (
            not stateless
            and request.consumer
            and not request.allow_duplicate
            and resource.id in self.consumer_holds.get(request.consumer, set())
        ):
            return "consumer_hold"
        # Passed every filter but was not leased: lost a race or was rejected
        # by the request-level check.
        return "contention"

    async def _await_slot(
        self,
        handle: _WaitHandle,
        request: AcquireRequest,
        timeout: float,
    ) -> Lease:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        # The pool's merged serve loop grants every waiter; a waiter only
        # sleeps on its own future, so queue pressure stays O(waiters) here
        # and O(1) storage work per serving tick.
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            try:
                return await asyncio.wait_for(
                    asyncio.shield(handle.future), remaining
                )
            except asyncio.TimeoutError:
                if handle.future.done():  # delivered right at the deadline
                    return cast(Lease, handle.future.result())
                break

        handle.cancel_pending()
        raise PoolExhausted(await self._exhausted_message(request, timeout))

    def _enqueue_waiter(self, req: AcquireRequest) -> _WaitHandle:
        loop = asyncio.get_running_loop()
        handle = _WaitHandle(
            seq=self.wait_queue.next_seq(),
            # ``fifo`` keeps arrival order; every other scheduler honours the
            # request priority (``priority_quota`` and ``token_bucket``).
            priority=0 if self.ctx.cfg.scheduler == "fifo" else req.priority,
            future=loop.create_future(),
            req=req,
            enqueued_at=loop.time(),
        )
        heapq.heappush(self.wait_queue.heap, handle)
        return handle

    async def serve_waiters(self, limit: int = 64) -> int:
        async with self._serve_lock:
            return await self._serve_waiters_locked(limit)

    # Waiting this many seconds is worth one priority point, capped so a
    # starved waiter cannot outrank explicit priorities forever.
    _AGING_STEP = 1.0
    _AGING_CAP = 5

    async def _serve_waiters_locked(self, limit: int) -> int:
        served = 0
        pending: list[_WaitHandle] = []
        current: _WaitHandle | None = None
        self.wait_queue.begin_poll()  # close() waits for in-flight allocations
        now = asyncio.get_running_loop().time()
        batch: list[_WaitHandle] = []
        while self.wait_queue.heap and len(batch) < limit:
            batch.append(heapq.heappop(self.wait_queue.heap))
        batch.sort(
            key=lambda h: (-(h.priority + h.age_boost(
                now, step=self._AGING_STEP, cap=self._AGING_CAP
            )), h.seq)
        )
        try:
            for handle in batch:
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
                heapq.heappush(self.wait_queue.heap, current)
            for handle in pending:
                if not handle.cancelled and not handle.future.done():
                    heapq.heappush(self.wait_queue.heap, handle)
            self.wait_queue.end_poll()
        return served

    async def _abandon_lease(self, lease: Lease) -> None:
        """Return a freshly acquired slot whose waiter vanished before delivery."""

        if self._stateless_leases.pop(lease.token, None) is not None:
            return
        if lease.token in self._degraded_tokens:
            self._degraded_tokens.discard(lease.token)
            await self.ctx.storage.transition_release(
                self.ctx.name, lease.resource_id, lease.slots
            )
            self.ctx.leases.unregister(lease.token)
            return
        await self.ctx.coordination.unregister_lease(self.ctx.name, lease.token)
        await self.ctx.storage.transition_release(
            self.ctx.name, lease.resource_id, lease.slots
        )
        self.ctx.leases.unregister(lease.token)
        if lease.consumer:
            self.consumer_holds.get(lease.consumer, set()).discard(
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
            resource = await self.ctx.storage.get(self.ctx.name, rid)
            if resource is None:
                raise ResourceNotFound(f"{self.ctx.name}:{rid}")
            if req.consumer and not req.allow_duplicate:
                held = await self._consumer_hold(rid, req.consumer)
                if held is not None:
                    held.payload = deepcopy(resource._payload_view())
                    held.resource = resource
                    return held
            if resource.allocatable and resource.concurrency.available >= req.slots:
                lease = await self._commit(req, rid)
                if lease is not None:
                    return lease
            elif resource.state in (ResourceState.NEW, ResourceState.VALIDATING):
                job = self._active_validate_job()
                if resource.state is ResourceState.NEW and job is None:
                    raise PoolExhausted(
                        f"resource {rid} is NEW (never validated); call "
                        f"await pool.admin.validate_now({rid!r}) or re-import with "
                        "import_validate='batch'"
                    )
                if deadline - time.monotonic() <= 0:
                    raise PoolExhausted(
                        f"resource {rid} is still {resource.state.value} after"
                        f" {timeout:.1f}s; validation is in progress"
                    )
            elif resource.state not in (ResourceState.ACTIVE, ResourceState.FULL, ResourceState.IDLE):
                raise BadState(f"resource {rid} is {resource.state.value}")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ResourceBusy(f"resource {rid} busy after {timeout:.1f}s")
            await asyncio.sleep(min(0.1, remaining))

    async def _consumer_hold(self, rid: str, consumer: str) -> Lease | None:
        for lease in await self.ctx.coordination.leases_for(self.ctx.name, rid):
            if lease.consumer == consumer:
                return cast("Lease", lease)
        return None

    async def _try_allocate(self, req: AcquireRequest) -> Lease | None:
        stateless = self.ctx.cfg.acquire_mode == "stateless"
        if req.resource_id is not None:
            if stateless:
                return await self._commit(req, req.resource_id)
            resource = await self.ctx.storage.get(self.ctx.name, req.resource_id)
            if resource is None or not (
                resource.allocatable and resource.concurrency.available >= req.slots
            ):
                return None
            return await self._commit(req, req.resource_id)

        candidates = await self._hard_filtered_candidates(req)
        if not candidates:
            return None

        affinity_key = req.affinity or req.consumer
        ctx = AllocationContext(
            now=_now(),
            request=req,
            weights=self.ctx.cfg.weights,
            affinity_scores=(
                await self._affinity_scores(affinity_key, candidates)
                if self.ctx.cfg.affinity_weight > 0
                else None
            ),
            top_k=self.ctx.cfg.top_k,
            rng=random.Random(f"{req.trace_id}:{self.wait_queue.seq}"),
        )
        # Score once: the ranked list feeds both the pick and the fallback
        # order, so allocators are never invoked twice per acquire.
        ranked, first = self.ctx.allocator.rank_and_pick(candidates, ctx)
        if not ranked:
            raise NoSuitableResource(req.target, checked=0)
        # First try the allocator's weighted pick (top-K anti-herd); fall back
        # to the remaining candidates in ranked order if it is busy or rejected.
        ordered: list[Resource] = []
        if first is not None:
            ordered.append(first)
            ordered.extend(r for r, _ in ranked if r.id != first.id)
        else:
            ordered.extend(r for r, _ in ranked)
        limit = max(self.ctx.cfg.top_k, 1)
        checked = 0
        check_rejected = 0
        for resource in ordered[: max(limit * 3, 1)]:
            checked += 1
            verdict = await self._request_check(resource, req)
            if verdict is CheckVerdict.BAN:
                await self.on_ban_verdict(resource, req)
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

    async def _try_preempt(self, req: AcquireRequest) -> bool:
        """§5.14: kick the cheapest holder whose priority trails far enough."""

        if not self.ctx.cfg.preemptible or self.ctx.cfg.acquire_mode == "stateless":
            return False
        threshold = self.ctx.cfg.preempt_threshold
        victims = [
            lease
            for lease in self.ctx.leases.active()
            if lease.consumer != req.consumer
            and req.priority - lease.priority >= threshold
        ]
        if not victims:
            return False
        victims.sort(key=lambda lease: (lease.priority, lease.started_at))
        victim: Lease | None = None
        for candidate in victims:
            resource = await self.ctx.storage.get(self.ctx.name, candidate.resource_id)
            if resource is None or resource.state not in (
                ResourceState.IDLE,
                ResourceState.ACTIVE,
                ResourceState.FULL,
            ):
                continue
            if req.tags and not req.tags <= resource.tags:
                continue
            if req.required_tag and req.required_tag not in resource.tags:
                continue
            victim = candidate
            break
        if victim is None:
            return False
        try:
            await self._revoke(
                victim.resource_id,
                operator="purser",
                reason="preempted",
                token=victim.token,
            )
        except Exception as exc:
            logger.warning(
                "preemption of %s failed on pool %s: %s",
                victim.token,
                self.ctx.name,
                exc,
            )
            return False
        self.ctx.purser.bus.publish(
            "lease_preempted",
            self.ctx.name,
            rid=victim.resource_id,
            data={
                "by": req.consumer,
                "priority": req.priority,
                "victim_priority": victim.priority,
            },
            trace_id=req.trace_id,
        )
        self.ctx.purser.metrics.inc(f"pool.{self.ctx.name}.preempt_total")
        return True

    async def _affinity_scores(
        self, consumer: str | None, candidates: list[Resource]
    ) -> dict[str, float]:
        if consumer is None:
            return {}
        history = self.affinity.get(consumer, {})
        if not history:
            return {}
        max_score = max(history.values()) or 1.0
        return {
            r.id: history.get(r.id, 0.0) / max_score for r in candidates
        }

    _CANDIDATE_BATCH = 64
    _CANDIDATE_ROUNDS = 3

    async def _hard_filtered_candidates(self, req: AcquireRequest) -> list[Resource]:
        stateless = self.ctx.cfg.acquire_mode == "stateless"
        if not stateless and self._consumer_at_limit(req.consumer):
            return []
        now = _now()
        limit = max(self._CANDIDATE_BATCH, self.ctx.cfg.top_k * 8)
        # The same seed keeps one acquire deterministic while the sampling
        # rounds walk different parts of a large allocatable index.
        rng = random.Random(f"{req.trace_id}:{self.wait_queue.seq}")
        out: list[Resource] = []
        seen: set[str] = set()
        exhaustive = False
        for _ in range(self._CANDIDATE_ROUNDS):
            batch = await self.ctx.storage.sample_allocatable(
                self.ctx.name,
                states=set(ALLOCATABLE_STATES),
                slots=req.slots,
                tags=req.tags or None,
                required_tag=req.required_tag,
                limit=limit,
                rng=rng,
            )
            fresh = 0
            for resource in batch:
                if resource.id in seen:
                    continue
                seen.add(resource.id)
                fresh += 1
                if await self._candidate_passes(resource, req, now, stateless):
                    out.append(resource)
            if len(batch) < limit:
                # The backend returned everything it has: filters are exact.
                exhaustive = True
                break
            if len(out) >= max(self.ctx.cfg.top_k * 4, 16):
                break
            if fresh == 0:
                break
        if not out and not exhaustive:
            # A large pool where sampling happened to miss every eligible
            # resource: fall back to an exact scan on this slow path so we
            # never report false exhaustion.
            async for resource in self.ctx.storage.scan(
                self.ctx.name, states=set(ALLOCATABLE_STATES)
            ):
                if resource.id in seen:
                    continue
                seen.add(resource.id)
                if await self._candidate_passes(resource, req, now, stateless):
                    out.append(resource)
                    if len(out) >= limit:
                        break
        return out

    async def _candidate_passes(
        self,
        resource: Resource,
        req: AcquireRequest,
        now: float,
        stateless: bool,
    ) -> bool:
        if not stateless and resource.concurrency.available < req.slots:
            return False
        if req.tags and not req.tags <= resource.tags:
            return False
        if req.required_tag and req.required_tag not in resource.tags:
            return False
        if not self._is_schedule_open(resource, now):
            return False
        if resource.expires_at is not None and resource.expires_at <= now:
            await self.on_expiration(resource)
            return False
        if not req.ignore_cooldown and resource.usable_at(now, req.consumer) > now:
            return False
        if req.target and resource.site_failures.get(
            req.target, SiteFailure(req.target, until=0)
        ).active(now):
            return False
        if (
            resource.usage_quota is not None
            and resource.used_count >= resource.usage_quota
        ):
            return False
        if resource.quota is not None and resource.quota.exhausted:
            return False
        if (
            not stateless
            and resource.mutex_group
            and req.consumer
            and await self._holds_group(
                req.consumer, resource.mutex_group, resource.id
            )
        ):
            return False
        if (
            not stateless
            and req.consumer
            and not req.allow_duplicate
            and resource.id in self.consumer_holds.get(req.consumer, set())
        ):
            return False
        return True

    async def _holds_group(self, consumer: str, group: str, except_rid: str) -> bool:
        for rid in self.consumer_holds.get(consumer, set()):
            if rid == except_rid:
                continue
            resource = await self.ctx.storage.get(self.ctx.name, rid)
            if resource is not None and resource.mutex_group == group:
                return True
        return False

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
            verdict = cast(
                CheckVerdict,
                await self.ctx.adapter.on_request_check(resource, check_req),
            )
        except Exception as exc:
            self.ctx.guard_error("on_request_check", exc)
            verdict = CheckVerdict.FAIL
        self._check_cache[key] = (verdict, now + self.ctx.cfg.check_cache_ttl)
        if len(self._check_cache) > 2048:
            # Cheap TTL sweep keeps the cache bounded.
            self._check_cache = {
                k: v for k, v in self._check_cache.items() if v[1] > now
            }
        return verdict

    async def _commit(self, req: AcquireRequest, rid: str) -> Lease | None:
        resource = await self.ctx.storage.get(self.ctx.name, rid)
        stateless = self.ctx.cfg.acquire_mode == "stateless"
        if resource is None:
            return None
        if stateless:
            if resource.state not in ALLOCATABLE_STATES:
                return None
            if req.tags and not req.tags <= resource.tags:
                return None
            if req.required_tag and req.required_tag not in resource.tags:
                return None
        else:
            if not (
                resource.allocatable and resource.concurrency.available >= req.slots
            ):
                return None
            if self._consumer_at_limit(req.consumer):
                return None
            if resource.mutex_group and req.consumer and await self._holds_group(
                req.consumer, resource.mutex_group, rid
            ):
                return None
        lease = Lease.new(
            self.ctx.name,
            rid,
            consumer=req.consumer,
            slots=req.slots,
            ttl=self.ctx.cfg.lease_ttl,
            priority=req.priority,
            use_limit=req.use_limit,
            trace_id=req.trace_id,
        )
        lease.target = req.target
        if stateless:
            return await self._commit_stateless(req, resource, lease)
        old_state = resource.state
        result = await self.ctx.storage.transition_acquire(self.ctx.name, rid, lease.slots)
        if not result.ok or result.resource is None:
            return None
        if old_state != result.resource.state:
            await self.state.notify_state_changed(
                rid, old_state, result.resource.state, "acquire"
            )
        lease.payload = deepcopy(result.resource._payload_view())
        lease.resource = result.resource
        registered = False
        try:
            try:
                await self.ctx.coordination.register_lease(self.ctx.name, lease)
            except asyncio.CancelledError:
                # A cancelled acquire must not leak the slot we just took.
                await self.ctx.storage.transition_release(
                    self.ctx.name, rid, lease.slots
                )
                raise
            except BackendUnavailable as exc:
                if not self._degraded("register_lease", exc):
                    await self.ctx.storage.transition_release(self.ctx.name, rid, lease.slots)
                    return None
                self._degraded_tokens.add(lease.token)
            except Exception:
                # Coordination is transient; roll the slot back so the resource
                # does not stay busy with an untracked lease.
                logger.exception("register lease failed for %s", rid)
                await self.ctx.storage.transition_release(self.ctx.name, rid, lease.slots)
                return None
            registered = True
            self.ctx.leases.register(lease, self._renew_cb)
            affinity_key = req.affinity or req.consumer
            if req.consumer:
                self.consumer_holds[req.consumer].add(rid)
            if affinity_key:
                self._record_affinity(affinity_key, rid)
            self.ctx.purser.bus.publish(
                "lease_acquired",
                self.ctx.name,
                rid=rid,
                data={
                    "consumer": req.consumer,
                    "slots": lease.slots,
                    "target": lease.target,
                },
                trace_id=req.trace_id,
            )
            self.ctx.purser.metrics.inc(f"pool.{self.ctx.name}.lease_total")
        except asyncio.CancelledError:
            if registered:
                await self.ctx.coordination.unregister_lease(self.ctx.name, lease.token)
            await self.ctx.storage.transition_release(self.ctx.name, rid, lease.slots)
            raise
        try:
            await self.ctx.adapter.on_acquire(result.resource, lease)
        except Exception as exc:
            self.ctx.guard_error("on_acquire", exc)
        return lease

    async def _commit_stateless(
        self, req: AcquireRequest, resource: Resource, lease: Lease
    ) -> Lease:
        """Hand out a borrow without occupying a slot or registering a lease.

        The returned ``Lease`` is only remembered pool-side so an explicit
        ``release()`` can still deliver usage/health feedback.
        """

        lease.payload = deepcopy(resource._payload_view())
        lease.resource = resource
        self._remember_stateless(lease)
        affinity_key = req.affinity or req.consumer
        if affinity_key:
            self._record_affinity(affinity_key, resource.id)
        self.ctx.purser.bus.publish(
            "lease_acquired",
            self.ctx.name,
            rid=resource.id,
            data={
                "consumer": req.consumer,
                "slots": lease.slots,
                "target": lease.target,
                "tracked": False,
            },
            trace_id=req.trace_id,
        )
        self.ctx.purser.metrics.inc(f"pool.{self.ctx.name}.lease_untracked_total")
        try:
            await self.ctx.adapter.on_acquire(resource, lease)
        except Exception as exc:
            self.ctx.guard_error("on_acquire", exc)
        return lease

    def _remember_stateless(self, lease: Lease) -> None:
        self._stateless_leases[lease.token] = lease
        while len(self._stateless_leases) > self._STATELESS_MEMORY:
            self._stateless_leases.popitem(last=False)

    def _consumer_lease_count(self, consumer: str) -> int:
        return self.ctx.leases.consumer_count(consumer)

    def _consumer_at_limit(self, consumer: str | None) -> bool:
        cap = self.ctx.cfg.max_leases_per_consumer
        if cap is None or consumer is None or self.ctx.cfg.acquire_mode == "stateless":
            return False
        return cast(bool, self._consumer_lease_count(consumer) >= cap)

    def _record_affinity(self, consumer: str, rid: str) -> None:
        history = self.affinity[consumer]
        history[rid] = float(history.get(rid, 0.0)) + 1.0
        if len(history) > 64:
            oldest = next(iter(history))
            history.pop(oldest, None)

    async def renew(self, lease: Lease, used: int | None = None) -> bool:
        return await self.releases.renew(lease, used)

    async def release(
        self,
        lease: Lease,
        *,
        outcome: UsageOutcome | str | None = None,
        used: int | None = None,
        latency_ms: float | None = None,
    ) -> bool:
        return await self.releases.release(
            lease, outcome=outcome, used=used, latency_ms=latency_ms
        )

    async def release_consumer(
        self,
        consumer: str,
        *,
        outcome: UsageOutcome | str = UsageOutcome.OK,
        operator: str = "system",
    ) -> list[Lease]:
        return await self.releases.release_consumer(
            consumer, outcome=outcome, operator=operator
        )

    async def on_expired(self, lease: Lease) -> None:
        await self.releases.on_expired(lease)

    async def on_lost(self, lease: Lease) -> None:
        await self.releases.on_lost(lease)

    async def on_ban_verdict(self, resource: Resource, req: AcquireRequest) -> None:
        await self.releases.on_ban_verdict(resource, req)

    async def on_expiration(self, resource: Resource) -> None:
        await self.releases.on_expiration(resource)

    def _degraded(self, action: str, error: Exception) -> bool:
        return coordination_degraded(self.ctx, action, error)
