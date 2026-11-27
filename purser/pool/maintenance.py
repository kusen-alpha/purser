"""Periodic maintenance sweeps (quota reset, GC and unban) and shutdown drain."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any, cast

from purser.backends.base import ResourcePatch
from purser.core.meta import ResourceMeta
from purser.core.resource import Lease
from purser.core.state import ResourceState
from purser.errors import ConfigError
from purser.facade._helpers import _now
from purser.pool.context import PoolContext
from purser.pool.state_ops import StateOps

logger = logging.getLogger("purser")


class MaintenanceService:
    def __init__(
        self,
        ctx: PoolContext,
        state: StateOps,
        *,
        admin: Any,
        validation: Any,
        serve_waiters: Callable[[], Awaitable[Any]],
        active_leases: Callable[[], list[Lease]],
        release: Callable[..., Awaitable[Any]],
        revoke: Callable[..., Awaitable[Any]],
        is_degraded: Callable[[str], bool],
        reconcile: Callable[[], Awaitable[int]] | None = None,
    ) -> None:
        self.ctx = ctx
        self.state = state
        self.admin = admin
        self.validation = validation
        self._serve_waiters = serve_waiters
        self._active_leases = active_leases
        self._release = release
        self._revoke = revoke
        self._is_degraded = is_degraded
        self._reconcile = reconcile

    async def drain(self) -> None:
        """O17: wait for in-flight leases, then kick whatever is left."""

        timeout = float(self.ctx.cfg.drain_timeout or 0.0)
        if timeout <= 0 or not self._active_leases():
            return
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        self.ctx.bus.publish(
            "pool_draining", self.ctx.name, data={"timeout": timeout}
        )
        while self._active_leases() and loop.time() < deadline:
            await asyncio.sleep(0.05)
        for lease in list(self._active_leases()):
            if self._is_degraded(lease.token):
                await self._release(lease)
                continue
            try:
                await self._revoke(
                    lease.rid,
                    operator="purser",
                    reason="drain_timeout",
                    token=lease.token,
                )
            except Exception as exc:
                logger.warning(
                    "drain could not revoke %s on pool %s: %s",
                    lease.token,
                    self.ctx.name,
                    exc,
                )

    async def run(self, name: str) -> dict[str, int]:
        """Execute one of the maintenance sweeps."""

        runner = {
            "quota_reset": self._maintenance_quota_reset,
            "periodic_unban": self._maintenance_periodic_unban,
            "lost_gc": self._maintenance_lost,
            "validating_gc": self._maintenance_validating,
            "quarantine_gc": self._maintenance_quarantine,
            "archive_gc": self._maintenance_archive,
            "expire_gc": self._maintenance_expire,
            "reconcile": self._maintenance_reconcile,
        }.get(name)
        if runner is None:
            raise ConfigError(f"unknown maintenance task {name!r}")
        return await runner()

    async def _maintenance_reconcile(self) -> dict[str, int]:
        """Leader-elected slot reconciliation; see ``ResourcePool._reconcile_sweep``."""

        if self._reconcile is None:
            return {"reconcile": 0}
        return {"reconcile": await self._reconcile()}

    async def _maintenance_quota_reset(self) -> dict[str, int]:
        moved = 0
        now = _now()
        async for resource in self.ctx.storage.scan(self.ctx.name, states={ResourceState.QUOTA_EXHAUSTED}):
            quota = resource.quota
            if quota is not None and not quota.reset_daily(timezone=self.ctx.cfg.timezone):
                continue
            if (
                self.ctx.cfg.quota_cooldown > 0
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
            if await self.state.cas(resource.id, patch):
                moved += 1
                self.ctx.purser.bus.publish("quota_reset", self.ctx.name, rid=resource.id)
        await self._serve_waiters()
        return {"quota_reset": moved}

    async def _maintenance_periodic_unban(self) -> dict[str, int]:
        moved = 0
        if self.ctx.cfg.unban_mode == "manual_only":
            return {"periodic_unban": 0}
        now = _now()
        async for resource in self.ctx.storage.scan(self.ctx.name, states={ResourceState.BANNED}):
            banned_at = float(resource.meta.get("banned_at") or 0.0)
            if now - banned_at >= self.ctx.cfg.unban_after:
                await self.admin.unban(resource.id, operator="system", reason="auto unban after ttl")
                moved += 1
        return {"periodic_unban": moved}

    async def _maintenance_lost(self) -> dict[str, int]:
        moved = 0
        async for resource in self.ctx.storage.scan(self.ctx.name, states={ResourceState.LOST}):
            if _now() - float(resource.meta.get("lost_at") or _now()) >= self.ctx.cfg.lost_retention:
                if await self.state.cas(
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
        async for resource in self.ctx.storage.scan(self.ctx.name, states={ResourceState.VALIDATING}):
            started = float(resource.meta.get("validate_started_at") or _now())
            if _now() - started >= self.ctx.cfg.validate_timeout:
                await self.validation.on_failure(
                    resource.id, "validation timed out", from_states={ResourceState.VALIDATING}
                )
                moved += 1
        return {"validating_gc": moved}

    async def _maintenance_quarantine(self) -> dict[str, int]:
        moved = 0
        now = _now()
        async for resource in self.ctx.storage.scan(self.ctx.name, states={ResourceState.QUARANTINED}):
            until = float(
                cast(ResourceMeta, resource.meta).quarantine_until
                or resource.next_available_at
                or 0.0
            )
            if until > now:
                continue
            policy = self.ctx.cfg.requalify_on_release
            target = ResourceState.VALIDATING if policy else ResourceState.IDLE
            if await self.state.cas(
                resource.id,
                ResourcePatch(
                    state=target,
                    meta_patch={**resource.meta, "quarantine_released_at": now},
                ),
            ):
                moved += 1
                self.ctx.purser.bus.publish("quarantine_expired", self.ctx.name, rid=resource.id)
        await self._serve_waiters()
        return {"quarantine_gc": moved}

    async def _maintenance_archive(self) -> dict[str, int]:
        deleted = 0
        async for resource in self.ctx.storage.scan(self.ctx.name, states={ResourceState.ARCHIVED}):
            archived_at = float(resource.meta.get("archived_at") or _now())
            if _now() - archived_at >= self.ctx.cfg.archive_retention:
                resource.payload = {"erased": True}  # secure erase before delete
                await self.ctx.storage.delete(self.ctx.name, resource.id)
                deleted += 1
                self.ctx.purser.audit.record(
                    operator="system", action="archive_maintenance_delete",
                    pool=self.ctx.name, rid=resource.id, reason="archive retention reached",
                )
        return {"archive_gc": deleted}

    async def _maintenance_expire(self) -> dict[str, int]:
        """Archive resources that expired long ago (opt-in via config).

        Only resources flipped off by ``expires_at`` (they carry
        ``meta["expired_at"]``) are considered, so a manually suspended
        resource is never touched.  Disabled by default: ``cfg.expired_retention``
        must be set to an age in seconds.
        """

        retention = self.ctx.cfg.expired_retention
        if retention is None:
            return {"expire_gc": 0}
        moved = 0
        now = _now()
        async for resource in self.ctx.storage.scan(
            self.ctx.name, states={ResourceState.SUSPENDED}
        ):
            expired_at = resource.meta.get("expired_at")
            if expired_at is None:
                continue
            if resource.slots.used_slots > 0:
                continue
            if now - float(expired_at) < retention:
                continue
            if await self.state.cas(
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
                self.ctx.purser.audit.record(
                    operator="system",
                    action="expire_gc_archive",
                    pool=self.ctx.name,
                    rid=resource.id,
                    reason="expired resource retention reached",
                )
        await self._serve_waiters()
        return {"expire_gc": moved}

