"""The state machine is the single source of truth for resource lifecycles.

The transition table below is the only place transitions are declared; both the
runtime orchestrator and the exhaustive tests are generated from it.  A
transition target may be a fixed state or a callable that resolves the target
from runtime context (e.g. ``ACTIVE`` vs ``FULL`` depending on slot counts).
"""

from __future__ import annotations

import enum
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from purser.errors import InvalidTransition


class ResourceState(str, enum.Enum):
    NEW = "new"
    VALIDATING = "validating"
    IDLE = "idle"
    ACTIVE = "active"
    FULL = "full"
    QUARANTINED = "quarantined"
    SUSPENDED = "suspended"
    QUOTA_EXHAUSTED = "quota_exhausted"
    BANNED = "banned"
    LOST = "lost"
    DRAINING = "draining"
    ARCHIVED = "archived"


class Event(str, enum.Enum):
    VALIDATE_OK = "validate_ok"
    VALIDATE_FAIL = "validate_fail"
    ACQUIRE = "acquire"
    RELEASE = "release"
    LEASE_EXPIRED = "lease_expired"
    FORCE_REVOKE = "force_revoke"
    LOST = "lost"
    BANNED = "banned"
    QUARANTINE_EXPIRED = "quarantine_expired"
    SUSPEND = "suspend"
    RESUME = "resume"
    QUOTA_EXHAUSTED = "quota_exhausted"
    TOP_UP = "top_up"
    QUOTA_RESET = "quota_reset"
    UNBAN = "unban"
    PROBE_OK = "probe_ok"
    DISCARD = "discard"
    DRAIN = "drain"
    REAPED = "reaped"
    RESTORE = "restore"
    ADMIN_SET_STATE = "admin_set_state"


ALL_STATES: tuple[ResourceState, ...] = tuple(ResourceState)
ALL_EVENTS: tuple[Event, ...] = tuple(Event)

# States from which an acquire may draw capacity.  ACTIVE is only eligible when
# the resource still has free slots; that check happens in the allocator.
ALLOCATABLE_STATES: frozenset[ResourceState] = frozenset(
    {ResourceState.IDLE, ResourceState.ACTIVE}
)

if TYPE_CHECKING:
    _Target = ResourceState | Callable[["ResourceState", "StateContext"], ResourceState]


@dataclass(frozen=True)
class StateContext:
    """Runtime values consulted by guard callables inside the table."""

    used_slots: int = 0
    slots: int = 1
    max_slots: int = 1
    fail_count: int = 0
    ban_threshold: int = 1
    revalidate: bool = False
    target_state: ResourceState | None = None


def _acquire_target(_: ResourceState, ctx: StateContext) -> ResourceState:
    nxt = ctx.used_slots + ctx.slots
    if nxt >= ctx.max_slots:
        return ResourceState.FULL
    return ResourceState.ACTIVE


def _release_target(_: ResourceState, ctx: StateContext) -> ResourceState:
    used = ctx.used_slots - ctx.slots
    if used <= 0:
        return ResourceState.IDLE
    if used >= ctx.max_slots:
        return ResourceState.FULL
    return ResourceState.ACTIVE


def _validate_fail_target(_: ResourceState, ctx: StateContext) -> ResourceState:
    if ctx.fail_count >= ctx.ban_threshold:
        return ResourceState.BANNED
    return ResourceState.QUARANTINED


def _resume_target(_: ResourceState, ctx: StateContext) -> ResourceState:
    return ResourceState.VALIDATING if ctx.revalidate else ResourceState.IDLE


def _admin_target(_: ResourceState, ctx: StateContext) -> ResourceState:
    target = ctx.target_state
    if target is None or target not in ADMIN_ALLOWED:
        raise InvalidTransition(f"admin_set_state disallows target {target!r}")
    return target


ADMIN_ALLOWED: frozenset[ResourceState] = frozenset(
    {
        ResourceState.IDLE,
        ResourceState.SUSPENDED,
        ResourceState.QUOTA_EXHAUSTED,
        ResourceState.VALIDATING,
    }
)


@dataclass(frozen=True)
class _TransitionSpec:
    event: Event
    target: _Target


# (from_state, event) -> spec.  The union of all rows is the executable truth
# for the lifecycle; UI, docs and tests all derive from it.
TRANSITIONS: dict[ResourceState, dict[Event, _TransitionSpec]] = {
    ResourceState.NEW: {
        Event.VALIDATE_OK: _TransitionSpec(Event.VALIDATE_OK, ResourceState.IDLE),
        Event.VALIDATE_FAIL: _TransitionSpec(Event.VALIDATE_FAIL, _validate_fail_target),
    },
    ResourceState.VALIDATING: {
        Event.VALIDATE_OK: _TransitionSpec(Event.VALIDATE_OK, ResourceState.IDLE),
        Event.VALIDATE_FAIL: _TransitionSpec(Event.VALIDATE_FAIL, _validate_fail_target),
    },
    ResourceState.IDLE: {
        Event.ACQUIRE: _TransitionSpec(Event.ACQUIRE, _acquire_target),
        Event.VALIDATE_OK: _TransitionSpec(Event.VALIDATE_OK, ResourceState.IDLE),
        Event.VALIDATE_FAIL: _TransitionSpec(
            Event.VALIDATE_FAIL, ResourceState.QUARANTINED
        ),
        Event.SUSPEND: _TransitionSpec(Event.SUSPEND, ResourceState.SUSPENDED),
        Event.QUOTA_EXHAUSTED: _TransitionSpec(
            Event.QUOTA_EXHAUSTED, ResourceState.QUOTA_EXHAUSTED
        ),
        Event.DRAIN: _TransitionSpec(Event.DRAIN, ResourceState.DRAINING),
        Event.ADMIN_SET_STATE: _TransitionSpec(Event.ADMIN_SET_STATE, _admin_target),
    },
    ResourceState.ACTIVE: {
        Event.ACQUIRE: _TransitionSpec(Event.ACQUIRE, _acquire_target),
        Event.VALIDATE_OK: _TransitionSpec(Event.VALIDATE_OK, ResourceState.ACTIVE),
        Event.VALIDATE_FAIL: _TransitionSpec(
            Event.VALIDATE_FAIL, ResourceState.QUARANTINED
        ),
        Event.RELEASE: _TransitionSpec(Event.RELEASE, _release_target),
        Event.LEASE_EXPIRED: _TransitionSpec(Event.LEASE_EXPIRED, _release_target),
        Event.FORCE_REVOKE: _TransitionSpec(Event.FORCE_REVOKE, _release_target),
        Event.LOST: _TransitionSpec(Event.LOST, ResourceState.LOST),
        Event.BANNED: _TransitionSpec(Event.BANNED, ResourceState.BANNED),
        Event.DRAIN: _TransitionSpec(Event.DRAIN, ResourceState.DRAINING),
        Event.ADMIN_SET_STATE: _TransitionSpec(Event.ADMIN_SET_STATE, _admin_target),
    },
    ResourceState.FULL: {
        Event.RELEASE: _TransitionSpec(Event.RELEASE, _release_target),
        Event.LEASE_EXPIRED: _TransitionSpec(Event.LEASE_EXPIRED, _release_target),
        Event.FORCE_REVOKE: _TransitionSpec(Event.FORCE_REVOKE, _release_target),
        Event.LOST: _TransitionSpec(Event.LOST, ResourceState.LOST),
        Event.BANNED: _TransitionSpec(Event.BANNED, ResourceState.BANNED),
        Event.ADMIN_SET_STATE: _TransitionSpec(Event.ADMIN_SET_STATE, _admin_target),
    },
    ResourceState.QUARANTINED: {
        Event.QUARANTINE_EXPIRED: _TransitionSpec(
            Event.QUARANTINE_EXPIRED, ResourceState.IDLE
        ),
        Event.VALIDATE_FAIL: _TransitionSpec(
            Event.VALIDATE_FAIL, ResourceState.BANNED
        ),
        Event.VALIDATE_OK: _TransitionSpec(
            Event.VALIDATE_OK, ResourceState.IDLE
        ),
        Event.ADMIN_SET_STATE: _TransitionSpec(Event.ADMIN_SET_STATE, _admin_target),
    },
    ResourceState.SUSPENDED: {
        Event.RESUME: _TransitionSpec(Event.RESUME, _resume_target),
        Event.ADMIN_SET_STATE: _TransitionSpec(Event.ADMIN_SET_STATE, _admin_target),
    },
    ResourceState.QUOTA_EXHAUSTED: {
        Event.TOP_UP: _TransitionSpec(Event.TOP_UP, ResourceState.IDLE),
        Event.QUOTA_RESET: _TransitionSpec(
            Event.QUOTA_RESET, ResourceState.IDLE
        ),
        Event.ADMIN_SET_STATE: _TransitionSpec(Event.ADMIN_SET_STATE, _admin_target),
    },
    ResourceState.BANNED: {
        Event.UNBAN: _TransitionSpec(Event.UNBAN, ResourceState.VALIDATING),
        Event.ADMIN_SET_STATE: _TransitionSpec(Event.ADMIN_SET_STATE, _admin_target),
    },
    ResourceState.LOST: {
        Event.PROBE_OK: _TransitionSpec(Event.PROBE_OK, ResourceState.IDLE),
        Event.DISCARD: _TransitionSpec(Event.DISCARD, ResourceState.ARCHIVED),
        Event.ADMIN_SET_STATE: _TransitionSpec(Event.ADMIN_SET_STATE, _admin_target),
    },
    ResourceState.DRAINING: {
        Event.RELEASE: _TransitionSpec(Event.RELEASE, ResourceState.ARCHIVED),
        Event.REAPED: _TransitionSpec(Event.REAPED, ResourceState.ARCHIVED),
        Event.ADMIN_SET_STATE: _TransitionSpec(Event.ADMIN_SET_STATE, _admin_target),
    },
    ResourceState.ARCHIVED: {
        Event.RESTORE: _TransitionSpec(Event.RESTORE, ResourceState.VALIDATING),
        Event.ADMIN_SET_STATE: _TransitionSpec(Event.ADMIN_SET_STATE, _admin_target),
    },
}

# REAPED is a reaper-only convenience alias for DRAINING -> ARCHIVED.
TRANSITIONS[ResourceState.DRAINING][Event.REAPED] = _TransitionSpec(
    Event.REAPED, ResourceState.ARCHIVED
)


class StateMachine:
    """Declarative executor over :data:`TRANSITIONS`."""

    @classmethod
    def allowed_events(cls, state: ResourceState) -> tuple[Event, ...]:
        return tuple(TRANSITIONS.get(state, {}))

    @classmethod
    def can_trigger(cls, state: ResourceState, event: Event) -> bool:
        return event in TRANSITIONS.get(state, {})

    @classmethod
    def trigger(
        cls,
        state: ResourceState,
        event: Event,
        *,
        ctx: StateContext | None = None,
    ) -> ResourceState:
        """Resolve the target state for ``(state, event)``.

        Raises :class:`InvalidTransition` when the pair is illegal.  Guard
        callables receive ``ctx`` (defaults to an empty :class:`StateContext`).
        """
        spec = TRANSITIONS.get(state, {}).get(event)
        if spec is None:
            raise InvalidTransition(f"illegal transition {state.value} --{event.value}-->")
        ctx = ctx or StateContext()
        target = spec.target(state, ctx) if callable(spec.target) else spec.target
        return target
