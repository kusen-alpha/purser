"""Cross-pool bundle orchestration used by the Purser facade."""

from __future__ import annotations

import asyncio
import heapq
import logging
import random
import uuid
from dataclasses import replace
from typing import Any

from purser.core.bundle import (
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
from purser.core.resource import (
    AcquireRequest,
    Lease,
    Resource,
    UsageOutcome,
)
from purser.core.state import ResourceState
from purser.errors import (
    BundleAcquireFailed,
    BundleQuotaExceeded,
    ConfigError,
    PoolEmpty,
    PoolExhausted,
    ResourceBusy,
)
from purser.facade._helpers import (
    WAITER_POLL_INTERVAL,
    _FairWaiter,
    _now,
)

logger = logging.getLogger("purser")


class BundleService:
    """Cross-pool bundle acquisition, fairness gating and reservation GC."""

    def __init__(self, purser: Any) -> None:
        self.ps = purser

    # -- cross-pool bundles -----------------------------------------

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
        self.ps._bundles[bundle_id] = bundle
        return bundle

    async def _release_bundle_members(self, acquired: list[tuple[str, Lease]]) -> None:
        for pool_name, lease in reversed(acquired):
            pool = self.ps._pools.get(pool_name)
            if pool is None:
                continue
            try:
                await pool.release(lease)
            except Exception as exc:
                logger.warning("bundle member release failed: %s", exc)
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
                pool = self.ps.get_pool(slot.pool)
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

    async def ensure_store(self) -> BundleStore:
        store: BundleStore | None = self.ps._bundle_store
        if store is None:
            if self.ps.coordination_kind == "redis":
                store = RedisBundleStore(
                    client=self.ps.redis_client,
                    url=self.ps.redis_url,
                )
            else:
                store = MemoryBundleStore()
            self.ps._bundle_store = store
        return store

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
        fairness = self.ps.bundle_fairness
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

        while self.ps._fair_heap:
            _neg, _seq, waiter = heapq.heappop(self.ps._fair_heap)
            if waiter.cancelled:
                continue
            waiter.event.set()
            return

    async def _fairness_gate(self, req: BundleRequest, store: BundleStore) -> None:
        fairness = self.ps.bundle_fairness
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
                    counts = await self.ps.get_pool(slot.pool).counts()
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
                        seq=self.ps._fair_seq,
                        priority=req.priority,
                        event=asyncio.Event(),
                    )
                    self.ps._fair_seq += 1
                    heapq.heappush(
                        self.ps._fair_heap, (-waiter.priority, waiter.seq, waiter)
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

    async def acquire(
        self,
        request: BundleRequest | dict[str, Any],
        **overrides: Any,
    ) -> BundleLease:
        """Acquire one lease per bundle slot across pools (opt-in v2).

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
            req.strict if req.strict is not None else self.ps.bundle_mode == "two_phase"
        )
        if strict and req.mode == "best_effort":
            raise ConfigError("strict (two-phase) bundles cannot use best_effort")
        store: BundleStore | None = None
        if strict or self.ps.bundle_fairness is not None:
            store = await self.ensure_store()
            if req.bundle_id is None:
                req = replace(req, bundle_id=f"bundle-{await store.next_seq():012d}")
        bundle_id = req.bundle_id or ""
        if req.bundle_id and req.bundle_id in self.ps._bundles:
            existing: BundleLease = self.ps._bundles[req.bundle_id]
            if existing.consumer == req.consumer:
                return existing
            raise ConfigError(f"bundle id {req.bundle_id!r} is already in use")
        pools = tuple(sorted(slot.pool for slot in req.slots))
        marker_ttl = (
            self.ps.fairness_ttl
            if self.ps.bundle_fairness is not None
            else self.ps.reservation_ttl
        )
        try:
            if self.ps.bundle_fairness is not None and store is not None:
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
                        await store.begin_members(bundle_id, self.ps.reservation_ttl)
                    for slot in slots:
                        pool = self.ps.get_pool(slot.pool)
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
            if self.ps.bundle_fairness is not None:
                self._fairness_release_notify()
            raise

    async def release(
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
            pool = self.ps._pools.get(pool_name)
            if pool is None:
                continue
            await pool.release(
                lease,
                outcome=outcomes.get(pool_name),
                used=used.get(pool_name),
            )
        self.ps._bundles.pop(bundle.bundle_id, None)
        if self.ps._bundle_store is not None:
            await self.ps._bundle_store.release(bundle.bundle_id)
            if self.ps.bundle_fairness is not None:
                self._fairness_release_notify()

    async def gc(self) -> dict[str, int]:
        """Keep live bundle markers fresh, then sweep expired reservations."""

        store = self.ps._bundle_store
        if store is None:
            return {"revoked": 0, "pruned": 0, "revived": 0}
        ttl = (
            self.ps.fairness_ttl
            if self.ps.bundle_fairness is not None
            else self.ps.reservation_ttl
        )
        revived = 0
        # Bundles this process still holds must never drop out of the active
        # set, otherwise fairness under-counts long-held combinations.
        for bundle in list(self.ps._bundles.values()):
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
                    pool = self.ps._pools.get(member.pool)
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
                    except Exception as exc:
                        logger.warning("bundle lease force-revoke failed: %s", exc)
                        continue
            await store.release(res.bundle_id)
            pruned += 1
        if self.ps.bundle_fairness is not None:
            self._fairness_release_notify()
        return {"revoked": revoked, "pruned": pruned, "revived": revived}

    async def _bundle_members_alive(self, res: Any) -> bool:
        for member in res.members.values():
            pool = self.ps._pools.get(member.pool)
            if pool is None:
                return False
            leases = await pool.leases(member.rid)
            if not any(lease.token == member.token for lease in leases):
                return False
        return True

    async def gc_loop(self) -> None:
        while True:
            await asyncio.sleep(self.ps.bundle_gc_interval)
            try:
                await self.gc()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("bundle_gc sweep failed")

    def _bundle_match_value(self, resource: Resource, rule: Any) -> str | None:
        value = getattr(resource, rule.from_field, None)
        if value is None:
            value = (getattr(resource, "payload", None) or {}).get(rule.from_field)
        if value is None or value == "":
            return None
        return str(value)
