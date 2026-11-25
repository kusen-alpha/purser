"""Optimistic state transitions, CAS and site-failure bookkeeping."""

from __future__ import annotations

from typing import TYPE_CHECKING

from purser.backends.base import ResourcePatch
from purser.core.resource import SiteFailure
from purser.core.state import ResourceState

if TYPE_CHECKING:
    from purser.pool.context import PoolContext


class StateOps:
    """State machine writes shared by the pool services."""

    def __init__(self, ctx: PoolContext) -> None:
        self.ctx = ctx

    async def apply_state(
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
            resource = await self.ctx.storage.get(self.ctx.name, rid)
            if resource is None:
                return False
            old_state = resource.state
            ok = await self.ctx.storage.set_state(
                self.ctx.name,
                rid,
                resource.version,
                expect_state=expect_state or resource.state,
                patch=patch,
            )
            if ok:
                new_state = patch.state if patch.state is not None else old_state
                if new_state != old_state:
                    await self.notify_state_changed(
                        rid, old_state, new_state, reason
                    )
                return True
            if expect_state is not None:
                return False
        return False

    async def notify_state_changed(
        self, rid: str, old: ResourceState, new: ResourceState, reason: str
    ) -> None:
        """Fan out one state transition to the bus and the plugin observer."""

        self.ctx.bus.publish(
            "state_changed",
            self.ctx.name,
            rid=rid,
            data={"old": old.value, "new": new.value, "reason": reason},
        )
        try:
            updated = await self.ctx.storage.get(self.ctx.name, rid)
            if updated is not None:
                await self.ctx.adapter.on_state_changed(updated, old, new, reason)
        except Exception as exc:
            self.ctx.guard_error("on_state_changed", exc)

    async def cas(
        self,
        rid: str,
        patch: ResourcePatch,
        *,
        expect_state: ResourceState | None = None,
        attempts: int = 3,
    ) -> bool:
        return await self.apply_state(
            rid, patch, expect_state=expect_state, attempts=attempts, reason="op"
        )

    def record_site_failure(
        self, site_failures: dict[str, SiteFailure], site: str, now: float
    ) -> None:
        cfg = self.ctx.cfg
        current = site_failures.get(site)
        if current is not None and current.active(now):
            current.count += 1
            current.until = now + cfg.site_failure_ttl
        else:
            site_failures[site] = SiteFailure(
                site=site, count=1, since=now, until=now + cfg.site_failure_ttl
            )
        if len(site_failures) > cfg.site_failure_max:
            oldest = min(site_failures, key=lambda k: site_failures[k].since)
            site_failures.pop(oldest, None)
