"""Usage-outcome side effects applied when a lease is released."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from purser.backends.base import ResourcePatch
from purser.core.resource import Lease, UsageOutcome
from purser.core.state import ALLOCATABLE_STATES, ResourceState
from purser.facade._helpers import _now

if TYPE_CHECKING:
    from purser.pool.context import PoolContext
    from purser.pool.state_ops import StateOps


class UsageService:
    """Applies one release outcome; deliberately not idempotent."""

    def __init__(
        self,
        ctx: PoolContext,
        state: StateOps,
        on_validation_fail: Callable[..., Awaitable[None]],
    ) -> None:
        self.ctx = ctx
        self.state = state
        self.on_validation_fail = on_validation_fail

    async def apply_outcome(
        self, rid: str, outcome: UsageOutcome, used: int | None, lease: Lease
    ) -> None:
        for _attempt in range(2):
            resource = await self.ctx.storage.get(self.ctx.name, rid)
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
                    self.state.record_site_failure(site_failures, lease.target, now)
            if outcome in (UsageOutcome.TIMEOUT, UsageOutcome.NETWORK_ERROR, UsageOutcome.BANNED):
                health.update(alive=False, error=outcome.value, now=now)
                meta["needs_validate"] = now
            if outcome is UsageOutcome.RATE_LIMITED:
                next_available_at = now + max(self.ctx.cfg.cooldown_after_release, 5.0)
            if outcome is UsageOutcome.PREEMPTED:
                if lease.consumer:
                    meta[f"cooldown:{lease.consumer}"] = now + 10.0

            # Usage metering and period quota.
            quota = resource.quota
            if quota is not None:
                quota.used += consume

            target_state = resource.state
            if self.ctx.cfg.cooldown_after_release > 0 and not self.ctx.cfg.cooldown_consumer_only:
                next_available_at = now + self.ctx.cfg.cooldown_after_release
            elif (
                self.ctx.cfg.cooldown_after_release > 0
                and self.ctx.cfg.cooldown_consumer_only
                and lease.consumer
            ):
                meta[f"cooldown:{lease.consumer}"] = now + self.ctx.cfg.cooldown_after_release

            if outcome is not UsageOutcome.BANNED:
                if (quota is not None and quota.exhausted) or (
                    resource.usage_quota is not None
                    and used_count >= resource.usage_quota
                ):
                    if resource.state in ALLOCATABLE_STATES:
                        target_state = ResourceState.QUOTA_EXHAUSTED
                        if (
                            self.ctx.cfg.quota_cooldown > 0
                            and resource.usage_quota is not None
                            and used_count >= resource.usage_quota
                        ):
                            next_available_at = now + self.ctx.cfg.quota_cooldown
                        self.ctx.purser.bus.publish(
                            "quota_exhausted", self.ctx.name, rid=rid, trace_id=lease.trace_id
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
            ok = await self.ctx.storage.set_state(
                self.ctx.name,
                rid,
                resource.version,
                expect_state=resource.state,
                patch=patch,
            )
            if ok:
                try:
                    await self.ctx.adapter.on_release(resource, outcome)
                except Exception as exc:
                    self.ctx.guard_error("on_release", exc)
                if outcome is UsageOutcome.BANNED:
                    await self.on_validation_fail(
                        rid,
                        f"usage banned for {lease.target or 'target'}",
                        operator="system",
                    )
                return
