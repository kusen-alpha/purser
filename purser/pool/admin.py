"""Administrative operations: state overrides, bans, migrations, recall."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, cast

from purser.backends.base import ResourcePatch
from purser.core.health import HealthReport
from purser.core.meta import ResourceMeta
from purser.core.resource import (
    Lease,
    Quota,
    Resource,
    Slots,
    UsageOutcome,
)
from purser.core.state import (
    ADMIN_ALLOWED,
    Event,
    ResourceState,
    StateContext,
    StateMachine,
)
from purser.errors import (
    BadState,
    ConfigError,
    LeaseConflict,
    VersionConflict,
)
from purser.facade._helpers import (
    _MISSING,
    _now,
    _optional_int,
    _schedule_window,
)
from purser.pool.context import PoolContext, require_resource
from purser.pool.state_ops import StateOps

if TYPE_CHECKING:
    from purser.pool.pool import ResourcePool

logger = logging.getLogger("purser")


class AdminService:
    def __init__(
        self,
        ctx: PoolContext,
        state: StateOps,
        *,
        consumer_holds: dict[str, set[str]],
        validate_now: Callable[..., Awaitable[Any]],
        validation_fail: Callable[..., Awaitable[None]],
        serve_waiters: Callable[[], Awaitable[Any]],
    ) -> None:
        self.ctx = ctx
        self.state = state
        self._consumer_holds = consumer_holds
        self._validate_now = validate_now
        self._validation_fail = validation_fail
        self._serve_waiters = serve_waiters

    async def ban(
        self, rid: str, *, reason: str, operator: str = "system"
    ) -> None:
        resource = await require_resource(self.ctx, rid)
        if resource.state in (ResourceState.BANNED, ResourceState.ARCHIVED):
            raise BadState(f"resource {rid} is already {resource.state.value}")
        # Revoke every live lease first: forcing BANNED while slots are busy
        # would leave the state machine in a state a later release cannot leave.
        while True:
            leases = await self.ctx.coordination.leases_for(self.ctx.name, rid)
            if not leases:
                resource = await require_resource(self.ctx, rid)
                if resource.slots.used_slots > 0:
                    await self.ctx.storage.reset_usage(self.ctx.name, rid)
                break
            lease = leases[0]
            await self.ctx.coordination.unregister_lease(self.ctx.name, lease.token)
            self.ctx.leases.unregister(lease.token)
            if lease.consumer:
                self._consumer_holds.get(lease.consumer, set()).discard(rid)
            await self.ctx.storage.transition_release(self.ctx.name, rid, lease.slots)
        resource = await require_resource(self.ctx, rid)
        patch = ResourcePatch(
            state=ResourceState.BANNED,
            meta_patch={**resource.meta, "banned_at": _now(), "banned_reason": reason},
        )
        await self.state.cas(rid, patch)
        self.ctx.audit.record(
            operator=operator, action="ban", pool=self.ctx.name, rid=rid, reason=reason
        )
        self.ctx.bus.publish(
            "banned", self.ctx.name, rid=rid, data={"reason": reason}
        )

    async def unban(
        self,
        rid: str,
        *,
        operator: str = "system",
        reason: str = "manual unban",
        revalidate: bool = True,
    ) -> None:
        resource = await require_resource(self.ctx, rid)
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
        await self.state.apply_state(
            rid, patch, expect_state=ResourceState.BANNED, reason="unban"
        )
        self.ctx.audit.record(
            operator=operator, action="unban", pool=self.ctx.name, rid=rid, reason=reason
        )
        self.ctx.bus.publish(
            "unbanned", self.ctx.name, rid=rid, data={"reason": reason}
        )
        if revalidate:
            await self._validate_now(rid, operator=operator)

    async def suspend(
        self, rid: str, *, operator: str = "system", reason: str
    ) -> None:
        resource = await require_resource(self.ctx, rid)
        if resource.state is not ResourceState.IDLE:
            raise BadState(
                f"only IDLE resources can be suspended, got {resource.state.value}"
            )
        await self.state.cas(
            rid,
            ResourcePatch(
                state=ResourceState.SUSPENDED,
                meta_patch={
                    **resource.meta,
                    "suspended_at": _now(),
                    "suspend_reason": reason,
                },
            ),
        )
        self.ctx.audit.record(
            operator=operator,
            action="suspend",
            pool=self.ctx.name,
            rid=rid,
            reason=reason,
        )

    async def resume(
        self,
        rid: str,
        *,
        operator: str = "system",
        reason: str = "manual resume",
        revalidate: bool = False,
    ) -> None:
        resource = await require_resource(self.ctx, rid)
        if resource.state is not ResourceState.SUSPENDED:
            raise BadState(f"resource {rid} is not suspended")
        target = ResourceState.VALIDATING if revalidate else ResourceState.IDLE
        await self.state.apply_state(
            rid,
            ResourcePatch(
                state=target,
                meta_patch={
                    **resource.meta,
                    "resumed_at": _now(),
                    "resume_reason": reason,
                },
            ),
            expect_state=ResourceState.SUSPENDED,
            reason="resume",
        )
        self.ctx.audit.record(
            operator=operator, action="resume", pool=self.ctx.name, rid=rid, reason=reason
        )
        await self._serve_waiters()

    async def top_up(
        self,
        rid: str,
        *,
        operator: str = "system",
        amount: int | None = None,
        reason: str = "manual top_up",
    ) -> None:
        resource = await require_resource(self.ctx, rid)
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
            state=(
                ResourceState.IDLE
                if resource.state is ResourceState.QUOTA_EXHAUSTED
                else resource.state
            ),
            quota=quota,
            used_count=new_used_count,
            meta_patch=meta,
        )
        await self.state.cas(rid, patch)
        self.ctx.audit.record(
            operator=operator, action="top_up", pool=self.ctx.name, rid=rid, reason=reason
        )
        self.ctx.bus.publish("quota_reset", self.ctx.name, rid=rid)
        await self._serve_waiters()

    async def drain(
        self, rid: str, *, operator: str = "system", reason: str
    ) -> None:
        """Retire a resource: no new leases, archived once leases return."""

        resource = await require_resource(self.ctx, rid)
        if resource.state is ResourceState.DRAINING:
            return
        await self.state.cas(
            rid,
            ResourcePatch(
                state=ResourceState.DRAINING,
                meta_patch={
                    **resource.meta,
                    "drained_at": _now(),
                    "drain_reason": reason,
                },
            ),
        )
        self.ctx.audit.record(
            operator=operator, action="drain", pool=self.ctx.name, rid=rid, reason=reason
        )

    async def cancel_drain(
        self,
        rid: str,
        *,
        operator: str = "system",
        reason: str = "cancel drain",
    ) -> None:
        """Undo a drain before the resource is archived (back online)."""

        resource = await require_resource(self.ctx, rid)
        if resource.state is not ResourceState.DRAINING:
            raise BadState(f"resource {rid} is not draining")
        target = StateMachine.trigger(
            resource.state,
            Event.UNDRAIN,
            ctx=StateContext(
                used_slots=resource.slots.used_slots,
                slots=0,
                max_slots=resource.slots.max_slots,
            ),
        )
        meta = {
            key: value
            for key, value in resource.meta.items()
            if key not in ("drained_at", "drain_reason")
        }
        meta["drain_cancelled_at"] = _now()
        await self.state.apply_state(
            rid,
            ResourcePatch(state=target, meta_patch=meta),
            expect_state=ResourceState.DRAINING,
            reason="cancel_drain",
        )
        self.ctx.audit.record(
            operator=operator,
            action="cancel_drain",
            pool=self.ctx.name,
            rid=rid,
            reason=reason,
            detail={"target": target.value},
        )
        await self._serve_waiters()

    async def restore_resource(
        self,
        rid: str,
        *,
        operator: str = "system",
        reason: str = "manual restore",
    ) -> None:
        resource = await require_resource(self.ctx, rid)
        if resource.state is not ResourceState.ARCHIVED:
            raise BadState(f"resource {rid} is not archived")
        await self.state.apply_state(
            rid,
            ResourcePatch(
                state=ResourceState.VALIDATING,
                meta_patch={
                    **resource.meta,
                    "restored_at": _now(),
                    "restore_reason": reason,
                },
            ),
            expect_state=ResourceState.ARCHIVED,
            reason="restore",
        )
        self.ctx.audit.record(
            operator=operator, action="restore", pool=self.ctx.name, rid=rid, reason=reason
        )
        await self._validate_now(rid, operator=operator)

    async def admin_set_state(
        self,
        rid: str,
        *,
        target: ResourceState,
        operator: str = "system",
        reason: str,
    ) -> None:
        if target not in ADMIN_ALLOWED:
            raise BadState(f"admin_set_state disallows {target.value}") from None
        resource = await require_resource(self.ctx, rid)
        if resource.slots.used_slots > 0:
            raise BadState(f"cannot force-set state while {rid} is busy")
        StateMachine.trigger(
            resource.state,
            Event.ADMIN_SET_STATE,
            ctx=StateContext(target_state=target),
        )
        await self.state.apply_state(
            rid,
            ResourcePatch(
                state=target,
                meta_patch={
                    **resource.meta,
                    "admin_set_at": _now(),
                    "admin_reason": reason,
                    "admin_operator": operator,
                },
            ),
            reason="admin_set_state",
        )
        self.ctx.audit.record(
            operator=operator,
            action="admin_set_state",
            pool=self.ctx.name,
            rid=rid,
            reason=reason,
            detail={"target": target.value},
        )
        await self._serve_waiters()

    async def force_revoke(
        self,
        rid: str,
        *,
        operator: str = "system",
        reason: str,
        token: str | None = None,
    ) -> Lease:
        """Administrative kick: returns and releases the lease."""

        leases = await self.ctx.coordination.leases_for(self.ctx.name, rid)
        if not leases:
            raise BadState(f"no active lease on {rid}")
        lease = cast(
            "Lease | None",
            (
                next((item for item in leases if item.token == token), None)
                if token
                else next(iter(leases))
            ),
        )
        if lease is None:
            raise LeaseConflict(f"lease token {token} not found on {rid}")
        removed = await self.ctx.coordination.unregister_lease(
            self.ctx.name, lease.token
        )
        if removed is None:
            # A concurrent release/reaper removed it first and owns the storage
            # decrement.  Revoking again would return the same slot twice.
            raise LeaseConflict(
                f"lease token {lease.token} is no longer active on {rid}"
            )
        self.ctx.leases.unregister(lease.token)
        result = await self.ctx.storage.transition_release(
            self.ctx.name, rid, lease.slots
        )
        if not result.ok:
            raise LeaseConflict(f"failed to revoke {rid}:{lease.token}")
        if lease.consumer:
            self._consumer_holds.get(lease.consumer, set()).discard(rid)
            await self._kick_cooldown(rid, lease.consumer)
        try:
            resource = await self.ctx.storage.get(self.ctx.name, rid)
            if resource is not None:
                await self.ctx.adapter.on_revoke(resource, lease, reason)
        except Exception as exc:
            self.ctx.guard_error("on_revoke", exc)
        self.ctx.audit.record(
            operator=operator,
            action="force_revoke",
            pool=self.ctx.name,
            rid=rid,
            reason=reason,
            detail={"token": lease.token, "consumer": lease.consumer},
        )
        self.ctx.bus.publish(
            "lease_revoked",
            self.ctx.name,
            rid=rid,
            data={"reason": reason},
            trace_id=lease.trace_id,
        )
        self.ctx.purser.usage.record(
            ts=_now(),
            pool=self.ctx.name,
            kind=self.ctx.cfg.kind,
            rid=rid,
            consumer=lease.consumer,
            target=lease.target,
            outcome=UsageOutcome.PREEMPTED.value,
            used=0,
            slots=lease.slots,
        )
        await self._serve_waiters()
        return lease

    async def _kick_cooldown(self, rid: str, consumer: str) -> None:
        """The kicked consumer cools down on this resource."""

        resource = await self.ctx.storage.get(self.ctx.name, rid)
        if resource is None:
            return
        meta = dict(resource.meta)
        meta[f"cooldown:{consumer}"] = _now() + 10.0
        await self.state.apply_state(
            rid,
            ResourcePatch(meta_patch=meta),
            expect_state=resource.state,
            reason="kick_cooldown",
        )

    async def update_resource(
        self,
        rid: str,
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
        """Mutate metadata/payload/slots; busy resources are rejected.

        ``usage_quota`` / ``quota_limit`` / ``expires_at`` /
        ``next_available_at`` / ``mutex_group`` / ``schedule_window`` /
        ``priority`` / ``preference`` accept ``None`` to clear the value and
        ``_MISSING`` (omitted) to keep it.
        """

        resource = await require_resource(self.ctx, rid)
        if resource.slots.used_slots > 0:
            raise BadState("cannot update a resource with active leases")
        if max_slots is not None and max_slots <= 0:
            raise ConfigError("max_slots must be > 0")
        clear: set[str] = set()
        extra: dict[str, Any] = {}
        if usage_quota is not _MISSING:
            if usage_quota is None:
                clear.add("usage_quota")
            else:
                extra["usage_quota"] = _optional_int(usage_quota, "usage_quota")
        if quota_limit is not _MISSING:
            if quota_limit is None:
                clear.add("quota")
            else:
                limit = _optional_int(quota_limit, "quota_limit", minimum=1)
                if limit is not None:
                    extra["quota"] = Quota(limit=limit)
        if expires_at is not _MISSING:
            if expires_at is None:
                clear.add("expires_at")
            else:
                if isinstance(expires_at, bool) or not isinstance(
                    expires_at, (int, float)
                ):
                    raise ConfigError("expires_at must be an epoch number or null")
                extra["expires_at"] = float(expires_at)
        if next_available_at is not _MISSING:
            if next_available_at is None:
                clear.add("next_available_at")
            else:
                if isinstance(next_available_at, bool) or not isinstance(
                    next_available_at, (int, float)
                ):
                    raise ConfigError("next_available_at must be an epoch number or null")
                extra["next_available_at"] = float(next_available_at)
        if mutex_group is not _MISSING:
            if mutex_group in (None, ""):
                clear.add("mutex_group")
            else:
                extra["mutex_group"] = str(mutex_group)
        if schedule_window is not _MISSING:
            if schedule_window is None:
                clear.add("schedule_window")
            else:
                extra["schedule_window"] = _schedule_window(schedule_window)
        meta_patch = {
            **resource.meta,
            "updated_by": operator,
            "update_reason": reason,
        }
        meta_clear: set[str] = set()
        if priority is not _MISSING:
            if priority is None:
                meta_patch.pop("priority", None)
                meta_clear.add("priority")
            else:
                if isinstance(priority, bool) or not isinstance(priority, int):
                    raise ConfigError("priority must be an integer or null")
                meta_patch["priority"] = priority
        if preference is not _MISSING:
            if preference is None:
                meta_patch.pop("preference", None)
                meta_clear.add("preference")
            else:
                if isinstance(preference, bool) or not isinstance(
                    preference, (int, float)
                ):
                    raise ConfigError("preference must be a number or null")
                meta_patch["preference"] = float(preference)
        patch = ResourcePatch(
            payload=payload,
            tags=tags if tags is not None else resource.tags,
            slots=(
                Slots(max_slots=max_slots)
                if max_slots is not None
                else resource.slots
            ),
            meta_patch=meta_patch,
            meta_clear=frozenset(meta_clear),
            clear_fields=frozenset(clear),
            **extra,
        )
        await self.state.cas(rid, patch)
        self.ctx.audit.record(
            operator=operator,
            action="update_resource",
            pool=self.ctx.name,
            rid=rid,
            reason=reason,
        )
        return await require_resource(self.ctx, rid)

    async def switch_resource(
        self,
        rid: str,
        *,
        operator: str = "system",
        reason: str = "manual switch",
    ) -> Resource | None:
        """Type-driven switch (reserved extension point).

        Calls ``ResourceAdapter.switch`` and persists whatever it returns: a
        same-id result (typically the mutated old resource) updates the row in
        place; a new-id result is imported and the old row is archived.
        ``None`` leaves the resource untouched and returns ``None``.
        """

        resource = await require_resource(self.ctx, rid)
        if resource.slots.used_slots > 0:
            raise BadState("cannot switch a resource with active leases")
        if resource.state in (
            ResourceState.ARCHIVED,
            ResourceState.DRAINING,
            ResourceState.BANNED,
            ResourceState.LOST,
        ):
            raise BadState(f"cannot switch {rid} in state {resource.state.value}")
        replacement = await self.ctx.adapter.switch(resource)
        if replacement is None:
            return None
        replacement = self.ctx.adapter.adopt(replacement)
        now = _now()
        if replacement.id == rid:
            ok = await self.state.apply_state(
                rid,
                ResourcePatch(
                    payload=dict(replacement.payload),
                    tags=replacement.tags,
                    meta_patch={
                        **resource.meta,
                        **replacement.meta,
                        "switched_at": now,
                        "updated_at": now,
                    },
                ),
                expect_state=resource.state,
                reason="switch",
            )
            if not ok:
                raise VersionConflict(f"switch lost the race on {rid}")
            new_id = rid
        else:
            replacement.version = 0
            replacement.meta = ResourceMeta(
                {
                    **replacement.meta,
                    "switched_from": rid,
                    "switched_at": now,
                    "created_at": now,
                    "updated_at": now,
                }
            )
            try:
                await self.ctx.storage.put(
                    self.ctx.name, replacement, expected_version=0
                )
            except VersionConflict:
                raise VersionConflict(
                    f"switch target {replacement.id!r} already exists"
                ) from None
            await self.state.apply_state(
                rid,
                ResourcePatch(
                    state=ResourceState.ARCHIVED,
                    meta_patch={
                        **resource.meta,
                        "switched_to": replacement.id,
                        "switched_at": now,
                    },
                ),
                expect_state=resource.state,
                reason="switch",
            )
            new_id = replacement.id
        self.ctx.audit.record(
            operator=operator,
            action="switch",
            pool=self.ctx.name,
            rid=rid,
            reason=reason,
            detail={"old_id": rid, "new_id": new_id},
        )
        self.ctx.bus.publish(
            "resource_switched",
            self.ctx.name,
            rid=new_id,
            data={"old_id": rid, "new_id": new_id, "reason": reason},
        )
        return await require_resource(self.ctx, new_id)

    async def move_resource(
        self,
        rid: str,
        target_pool: ResourcePool,
        *,
        operator: str = "system",
        reason: str,
    ) -> Resource:
        """Cross-pool migration; busy resources are rejected."""

        resource = await require_resource(self.ctx, rid)
        if resource.slots.used_slots > 0:
            raise BadState("cannot move a resource with active leases")
        if target_pool.cfg.kind != self.ctx.cfg.kind:
            raise BadState(
                f"cannot move {self.ctx.cfg.kind} resource into "
                f"{target_pool.cfg.kind} pool {target_pool.name!r}"
            )
        existing = await target_pool.storage.get(target_pool.name, rid)
        if existing is not None:
            raise BadState(f"target pool {target_pool.name} already contains {rid}")
        await target_pool.storage.put(target_pool.name, resource)
        await self.ctx.storage.delete(self.ctx.name, rid)
        self.ctx.audit.record(
            operator=operator,
            action="move_resource",
            pool=self.ctx.name,
            rid=rid,
            reason=reason,
            detail={"target": target_pool.name},
        )
        return resource

    async def recall_batch(
        self,
        batch_id: str,
        *,
        action: str = "requalify",
        operator: str = "system",
        reason: str,
    ) -> dict[str, Any]:
        """Requalify / isolate / retire every resource of a batch."""

        if not isinstance(batch_id, str) or not batch_id:
            raise ConfigError("batch_id must be a non-empty string")
        if action not in ("requalify", "isolate", "retire"):
            raise ConfigError(f"unknown recall action {action!r}")
        report: dict[str, Any] = {
            "action": action,
            "batch_id": batch_id,
            "processed": 0,
            "errors": [],
        }
        async for resource in self.ctx.storage.scan(self.ctx.name):
            if resource.meta.get("batch_id") != batch_id:
                continue
            try:
                if action == "requalify":
                    await self._validate_now(resource.id, operator=operator)
                elif action == "isolate":
                    await self._validation_fail(
                        resource.id,
                        reason,
                        operator=operator,
                        from_states={resource.state},
                    )
                elif action == "retire":
                    await self.drain(resource.id, operator=operator, reason=reason)
                report["processed"] += 1
            except Exception as exc:
                report["errors"].append({"rid": resource.id, "error": str(exc)})
        return report


class PoolAdminView:
    """Operator namespace exposed as ``pool.admin``.

    Lifecycle, repair and maintenance actions live here so the pool's
    data-plane surface (acquire / lease / release / renew) stays small.
    """

    def __init__(self, pool: ResourcePool) -> None:
        self._pool = pool

    # -- validation --------------------------------------------------------

    async def validate_now(
        self,
        rid: str,
        *,
        operator: str = "system",
    ) -> HealthReport:
        """Validate one resource immediately and apply the verdict."""

        return await self._pool._validation.validate_now(rid, operator=operator)

    async def validate_check(self, rid: str, check: str) -> HealthReport:
        """Run one named check without changing the resource state."""

        return await self._pool._validation.validate_check(rid, check)

    async def apply_defaults(
        self,
        *,
        operator: str = "system",
        reason: str = "apply pool defaults",
    ) -> dict[str, int]:
        """Backfill pool defaults onto resources that lack them."""

        return await self._pool._catalog.apply_defaults(operator=operator, reason=reason)

    # -- lifecycle operations ----------------------------------------------

    async def ban(self, rid: str, *, reason: str, operator: str = "system") -> None:
        """Ban one resource after a hard failure or operator decision."""

        await self._pool._admin.ban(rid, reason=reason, operator=operator)

    async def unban(
        self,
        rid: str,
        *,
        operator: str = "system",
        reason: str = "manual unban",
        revalidate: bool = True,
    ) -> None:
        """Lift a ban; ``revalidate=True`` parks it in VALIDATING first."""

        await self._pool._admin.unban(
            rid, operator=operator, reason=reason, revalidate=revalidate
        )

    async def suspend(
        self, rid: str, *, operator: str = "system", reason: str
    ) -> None:
        """Temporarily remove a resource from allocation without banning it."""

        await self._pool._admin.suspend(rid, operator=operator, reason=reason)

    async def resume(
        self,
        rid: str,
        *,
        operator: str = "system",
        reason: str = "manual resume",
        revalidate: bool = False,
    ) -> None:
        """Return a suspended resource to allocation (optionally re-validating)."""

        await self._pool._admin.resume(
            rid, operator=operator, reason=reason, revalidate=revalidate
        )

    async def top_up(
        self,
        rid: str,
        *,
        operator: str = "system",
        amount: int | None = None,
        reason: str = "manual top_up",
    ) -> None:
        """Replenish a quota-exhausted resource and return it to IDLE."""

        await self._pool._admin.top_up(rid, operator=operator, amount=amount, reason=reason)

    async def drain(
        self, rid: str, *, operator: str = "system", reason: str
    ) -> None:
        """Stop new acquires; the resource archives once its leases drain."""

        await self._pool._admin.drain(rid, operator=operator, reason=reason)

    async def cancel_drain(
        self,
        rid: str,
        *,
        operator: str = "system",
        reason: str = "cancel drain",
    ) -> None:
        """Undo a drain and return the resource to allocation."""

        await self._pool._admin.cancel_drain(rid, operator=operator, reason=reason)

    async def restore_resource(
        self,
        rid: str,
        *,
        operator: str = "system",
        reason: str = "manual restore",
    ) -> None:
        """Restore an archived resource through VALIDATING."""

        await self._pool._admin.restore_resource(rid, operator=operator, reason=reason)

    async def admin_set_state(
        self,
        rid: str,
        *,
        target: ResourceState,
        operator: str = "system",
        reason: str,
    ) -> None:
        """Force a state the operator chooses; only safe targets are accepted."""

        await self._pool._admin.admin_set_state(
            rid, target=target, operator=operator, reason=reason
        )

    async def force_revoke(
        self,
        rid: str,
        *,
        operator: str = "system",
        reason: str,
        token: str | None = None,
    ) -> Lease:
        """Revoke one live lease immediately and return the released snapshot."""

        return await self._pool._admin.force_revoke(
            rid, operator=operator, reason=reason, token=token
        )

    async def update_resource(
        self,
        rid: str,
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

        return await self._pool._admin.update_resource(
            rid,
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
        rid: str,
        *,
        operator: str = "system",
        reason: str = "manual switch",
    ) -> Resource | None:
        """Switch an ACTIVE resource to its alternate endpoint/payload."""

        return await self._pool._admin.switch_resource(
            rid, operator=operator, reason=reason
        )

    async def move_resource(
        self,
        rid: str,
        target_pool: ResourcePool,
        *,
        operator: str = "system",
        reason: str,
    ) -> Resource:
        """Move a resource to another pool owned by the same Purser."""

        return await self._pool._admin.move_resource(
            rid, target_pool, operator=operator, reason=reason
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

        return await self._pool._admin.recall_batch(
            batch_id, action=action, operator=operator, reason=reason
        )

    # -- maintenance ---------------------------------------------------------

    async def run_maintenance(self, name: str) -> dict[str, int]:
        """Run one named maintenance sweep now and return its counters."""

        return await self._pool._maintenance.run(name)
