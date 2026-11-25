"""Lease renewal, release and end-of-life callbacks."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from purser.backends.base import ResourcePatch
from purser.core.resource import (
    AcquireRequest,
    Lease,
    Resource,
    UsageOutcome,
)
from purser.core.state import ResourceState
from purser.errors import (
    BackendUnavailable,
    ConfigError,
    LeaseConflict,
    LeaseLimitReached,
    LeaseLost,
)
from purser.facade._helpers import _now, coerce_usage_outcome
from purser.pool.context import coordination_degraded

if TYPE_CHECKING:
    from purser.pool.leases import LeaseService

logger = logging.getLogger("purser")


class ReleaseService:
    """Returns slots, settles usage and handles expired/lost leases."""

    def __init__(self, leases: LeaseService) -> None:
        self.leases = leases
        self.ctx = leases.ctx
        self.state = leases.state
        self.usage = leases.usage
        self.validation = leases.validation
        self.consumer_holds = leases.consumer_holds

    @property
    def _stateless_leases(self) -> Any:
        return self.leases._stateless_leases

    @property
    def _degraded_tokens(self) -> Any:
        return self.leases._degraded_tokens

    async def serve_waiters(self, limit: int = 64) -> int:
        return await self.leases.serve_waiters(limit)

    def _degraded(self, action: str, error: Exception) -> bool:
        return coordination_degraded(self.ctx, action, error)

    async def renew(self, lease: Lease, used: int | None = None) -> bool:
        if used is not None:
            if used < 0:
                raise ConfigError("used must be >= 0")
            # Usage counters only grow; a stale retry must not roll them back.
            if used > lease.used:
                lease.used = used
        if lease.use_limit is not None and lease.used >= lease.use_limit:
            raise LeaseLimitReached(
                f"lease {lease.token} reached use_limit={lease.use_limit}"
            )
        if self.ctx.cfg.max_lease_duration is not None:
            hard_cap = lease.started_at + self.ctx.cfg.max_lease_duration
            if hard_cap <= _now():
                raise LeaseLimitReached(
                    f"lease {lease.token} reached max_lease_duration="
                    f"{self.ctx.cfg.max_lease_duration}"
                )
        expires_at = _now() + self.ctx.cfg.lease_ttl
        if self.ctx.cfg.max_lease_duration is not None:
            expires_at = min(expires_at, lease.started_at + self.ctx.cfg.max_lease_duration)
        try:
            ok = await self.ctx.coordination.renew_lease(
                self.ctx.name, lease.resource_id, lease.token, expires_at
            )
        except BackendUnavailable as exc:
            if not self._degraded("renew_lease", exc):
                raise
            ok = True
        if not ok:
            raise LeaseLost(
                f"lease {lease.token} is no longer registered in coordination"
            )
        lease.expires_at = expires_at
        lease.heartbeat_at = _now()
        try:
            await self.ctx.adapter.on_renew(lease.resource, lease)
        except Exception as exc:
            self.ctx.guard_error("on_renew", exc)
        return True

    async def release(
        self,
        lease: Lease,
        *,
        outcome: UsageOutcome | str | None = None,
        used: int | None = None,
        latency_ms: float | None = None,
    ) -> bool:
        if used is not None and used < 0:
            raise ConfigError("used must be >= 0")
        if latency_ms is not None and latency_ms < 0:
            raise ConfigError("latency_ms must be >= 0")
        outcome = coerce_usage_outcome(outcome)
        if lease.token in self._stateless_leases:
            return await self._release_stateless(
                lease, outcome, used, latency_ms=latency_ms
            )
        before = await self.ctx.storage.get(self.ctx.name, lease.resource_id)
        before_state = before.state if before is not None else None
        if before is not None:
            try:
                await self.ctx.adapter.before_release(before, lease)
            except Exception as exc:
                self.ctx.guard_error("before_release", exc)
        if lease.token in self._degraded_tokens:
            self._degraded_tokens.discard(lease.token)
            removed: Lease | None = lease
        else:
            try:
                removed = await self.ctx.coordination.unregister_lease(
                    self.ctx.name, lease.token
                )
            except BackendUnavailable as exc:
                if not self._degraded("unregister_lease", exc):
                    raise
                removed = lease
        if removed is None:
            # A revoke, expiry, concurrent retry or coordination wipe already
            # removed our token.  Never decrement storage here: the busy slot
            # may now belong to a fresh lease from another instance, and a
            # blind decrement would steal it.  Orphaned slots are recovered by
            # _reconcile_leases() on the next start(), which is the designated
            # restart path.
            self.ctx.leases.unregister(lease.token)
            if lease.consumer:
                self.consumer_holds.get(lease.consumer, set()).discard(
                    lease.resource_id
                )
            self.ctx.purser.metrics.inc(f"pool.{self.ctx.name}.release_total")
            await self.serve_waiters()
            return True
        try:
            result = await self.ctx.storage.transition_release(
                self.ctx.name, lease.resource_id, lease.slots
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
        self.ctx.leases.unregister(lease.token)
        if lease.consumer:
            self.consumer_holds.get(lease.consumer, set()).discard(lease.resource_id)
        if before_state is not None and result.state is not None and before_state != result.state:
            await self.state.notify_state_changed(
                lease.resource_id, before_state, result.state, "release"
            )
        outcome = outcome or lease.outcome or UsageOutcome.OK
        lease.outcome = outcome
        self.ctx.purser.bus.publish(
            "lease_released",
            self.ctx.name,
            rid=lease.resource_id,
            data={
                "outcome": outcome.value,
                "consumer": lease.consumer,
            },
            trace_id=lease.trace_id,
        )
        await self.usage.apply_outcome(
            lease.resource_id, outcome, used, lease, latency_ms=latency_ms
        )
        self.ctx.purser.usage.record(
            ts=_now(),
            pool=self.ctx.name,
            kind=self.ctx.cfg.kind,
            rid=lease.resource_id,
            consumer=lease.consumer,
            target=lease.target,
            outcome=outcome.value,
            used=used if used is not None else (lease.used or 1),
            slots=lease.slots,
        )
        await self.serve_waiters()
        self.ctx.purser.metrics.inc(f"pool.{self.ctx.name}.release_total")
        return True

    async def _release_stateless(
        self,
        lease: Lease,
        outcome: UsageOutcome | None,
        used: int | None,
        *,
        latency_ms: float | None = None,
    ) -> bool:
        """Finish a stateless borrow: feedback only, no slot to return."""

        self._stateless_leases.pop(lease.token, None)
        resource = await self.ctx.storage.get(self.ctx.name, lease.resource_id)
        if resource is not None:
            try:
                await self.ctx.adapter.before_release(resource, lease)
            except Exception as exc:
                self.ctx.guard_error("before_release", exc)
        outcome = outcome or lease.outcome or UsageOutcome.OK
        lease.outcome = outcome
        self.ctx.purser.bus.publish(
            "lease_released",
            self.ctx.name,
            rid=lease.resource_id,
            data={
                "outcome": outcome.value,
                "consumer": lease.consumer,
                "tracked": False,
            },
            trace_id=lease.trace_id,
        )
        await self.usage.apply_outcome(
            lease.resource_id, outcome, used, lease, latency_ms=latency_ms
        )
        self.ctx.purser.usage.record(
            ts=_now(),
            pool=self.ctx.name,
            kind=self.ctx.cfg.kind,
            rid=lease.resource_id,
            consumer=lease.consumer,
            target=lease.target,
            outcome=outcome.value,
            used=used if used is not None else (lease.used or 1),
            slots=lease.slots,
        )
        await self.serve_waiters()
        self.ctx.purser.metrics.inc(f"pool.{self.ctx.name}.release_total")
        return True

    async def _restore_release_claim(self, lease: Lease, removed: Lease) -> None:
        """Put a failed release's token back without resurrecting a freed slot."""

        try:
            current = await self.ctx.storage.get(self.ctx.name, lease.resource_id)
        except Exception:
            # Storage state is unknown: keep the coordination claim rather than
            # letting another instance treat a possibly-live slot as free.
            logger.exception(
                "could not verify %s after failed release; restoring token",
                lease.resource_id,
            )
            await self.ctx.coordination.register_lease(self.ctx.name, removed)
            return
        if (
            current is not None
            and current.concurrency.used_slots >= lease.slots
        ):
            await self.ctx.coordination.register_lease(self.ctx.name, removed)

    async def release_consumer(
        self,
        consumer: str,
        *,
        outcome: UsageOutcome | str = UsageOutcome.OK,
        operator: str = "system",
    ) -> list[Lease]:
        """Return every live lease held by ``consumer`` (logout / teardown)."""

        if not consumer:
            raise ConfigError("consumer must be non-empty")
        outcome = coerce_usage_outcome(outcome) or UsageOutcome.OK
        released: list[Lease] = []
        for lease in await self.ctx.coordination.active(self.ctx.name):
            if lease.consumer != consumer:
                continue
            await self.release(lease, outcome=outcome)
            released.append(lease)
        if released:
            self.ctx.purser.audit.record(
                operator=operator,
                action="release_consumer",
                pool=self.ctx.name,
                reason=f"consumer {consumer}",
                detail={
                    "consumer": consumer,
                    "outcome": outcome.value,
                    "tokens": [lease.token for lease in released],
                },
            )
        return released

    async def on_expired(self, lease: Lease) -> None:
        """Watchdog path: a lease passed its deadline while still held."""

        if lease.token in self._degraded_tokens:
            self._degraded_tokens.discard(lease.token)
            removed: Lease | None = lease
        else:
            removed = await self.ctx.coordination.unregister_lease(
                self.ctx.name, lease.token
            )
        self.ctx.leases.unregister(lease.token)
        if lease.consumer:
            self.consumer_holds.get(lease.consumer, set()).discard(lease.resource_id)
        if removed is None:
            # The token was already gone (revoked elsewhere or Redis TTL fired).
            # Its slots may now belong to a fresh holder, so never decrement
            # storage here; reconcile on the next start() cleans up orphans.
            self.ctx.purser.bus.publish(
                "lease_lost",
                self.ctx.name,
                rid=lease.resource_id,
                data={"reason": "token already gone"},
                trace_id=lease.trace_id,
            )
            self.ctx.purser.metrics.inc(f"pool.{self.ctx.name}.lease_lost_total")
            await self.serve_waiters()
            return
        result = await self.ctx.storage.transition_release(
            self.ctx.name, lease.resource_id, lease.slots
        )
        if result.ok:
            self.ctx.purser.bus.publish(
                "lease_expired",
                self.ctx.name,
                rid=lease.resource_id,
                data={"consumer": lease.consumer},
                trace_id=lease.trace_id,
            )
        else:
            self.ctx.purser.bus.publish(
                "lease_lost",
                self.ctx.name,
                rid=lease.resource_id,
                data={},
                trace_id=lease.trace_id,
            )
        await self.serve_waiters()

    async def on_lost(self, lease: Lease) -> None:
        # The heartbeat gave up on this lease (e.g. use_limit reached or the
        # coordination token vanished).  Stop tracking it in the manager and
        # return the slots if we still own the token; a token that is already
        # gone may now belong to a fresh holder, so leave those slots to
        # _reconcile_leases() instead of stealing them.
        if lease.token in self._degraded_tokens:
            self._degraded_tokens.discard(lease.token)
            removed: Lease | None = lease
        else:
            removed = await self.ctx.coordination.unregister_lease(
                self.ctx.name, lease.token
            )
        self.ctx.leases.unregister(lease.token)
        if lease.consumer:
            self.consumer_holds.get(lease.consumer, set()).discard(lease.resource_id)
        if removed is not None:
            await self.ctx.storage.transition_release(
                self.ctx.name, lease.resource_id, lease.slots
            )
        self.ctx.purser.bus.publish(
            "lease_lost",
            self.ctx.name,
            rid=lease.resource_id,
            data={"reason": "heartbeat rejected"},
            trace_id=lease.trace_id,
        )
        self.ctx.purser.metrics.inc(f"pool.{self.ctx.name}.lease_lost_total")
        try:
            resource = await self.ctx.storage.get(self.ctx.name, lease.resource_id)
            if resource is not None:
                await self.ctx.adapter.on_lease_lost(resource, lease)
        except Exception as exc:
            self.ctx.guard_error("on_lease_lost", exc)
        await self.serve_waiters()

    async def on_ban_verdict(self, resource: Resource, req: AcquireRequest) -> None:
        site_failures = dict(resource.site_failures)
        if req.target:
            self.state.record_site_failure(site_failures, req.target, _now())
        await self.validation.on_failure(
            resource.id,
            f"request-level BAN for {req.target or 'target'}",
            from_states={resource.state},
            site_failures=site_failures if req.target else None,
        )

    async def on_expiration(self, resource: Resource) -> None:
        """S18: absolute expiry flips the resource off."""

        policy = self.ctx.cfg.expire_policy
        target = (
            ResourceState.QUOTA_EXHAUSTED
            if policy == "quota"
            else ResourceState.SUSPENDED
        )
        patch = ResourcePatch(
            state=target,
            meta_patch={**resource.meta, "expired_at": _now()},
        )
        await self.state.apply_state(resource.id, patch, expect_state=resource.state, reason="expired")
        self.ctx.purser.bus.publish("expired", self.ctx.name, rid=resource.id)
