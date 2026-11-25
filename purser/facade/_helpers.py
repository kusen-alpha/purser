"""Shared helpers and lightweight value types for the facade layer.

.. note::

   Formerly the private top half of :mod:`purser.facade`.

This is the orchestration layer.  It owns the acquisition pipeline
(filter -> score -> request-level check -> atomic commit), the fair wait
queue, lease heartbeat/reaping, validation scheduling, quarantine escalation,
periodic GC and all lifecycle operations.  The state machine in
``core.state`` remains the single source of truth for transitions; the
storage plane provides the atomic read-check-write primitives and the
coordination plane tracks in-flight leases.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import (
    Iterator,
)
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, TypeVar

from purser.core.resource import (
    AcquireRequest,
    Concurrency,
    Lease,
    Quota,
    Resource,
    UsageOutcome,
)
from purser.core.state import (
    ResourceState,
)
from purser.errors import (
    ConfigError,
)
from purser.results import ImportReport as ImportReport

if TYPE_CHECKING:
    from purser.pool.pool import ResourcePool

logger = logging.getLogger("purser")

_T = TypeVar("_T")

# Shared coordination has no push channel; general waiters poll storage on this
# interval so a release on another instance can wake them (V03).
WAITER_POLL_INTERVAL = 0.1

BACKUP_FORMAT = "purser-backup"
BACKUP_VERSION = 1

# Operational states cannot survive a restore: leases are not migrated, so
# anything that implies occupancy comes back as IDLE and reconciles normally.
_RESTORE_IDLE_STATES = frozenset(
    {
        ResourceState.NEW,
        ResourceState.VALIDATING,
        ResourceState.ACTIVE,
        ResourceState.FULL,
        ResourceState.DRAINING,
        ResourceState.LOST,
    }
)
_RESTORE_TRANSIENT_META = ("drain_reason", "drained_at", "drain_cancelled_at")


def _now() -> float:
    return time.time()


def coerce_usage_outcome(
    value: UsageOutcome | str | None,
) -> UsageOutcome | None:
    """Accept the enum or its wire value; reject everything else."""

    if value is None or isinstance(value, UsageOutcome):
        return value
    try:
        return UsageOutcome(value)
    except ValueError as exc:
        raise ConfigError(f"unknown usage outcome {value!r}") from exc


_MISSING = object()


def _record_value(record: dict[str, Any] | Resource, key: str) -> Any:
    """Look up a dedup key on an import record (dict spec or Resource)."""

    if isinstance(record, dict):
        return record[key] if key in record else _MISSING
    if key == "id":
        return record.id
    if key in record.payload:
        return record.payload[key]
    return record.meta.get(key, _MISSING)


# Scheduling fields accepted next to resource payload keys on the import path.
_IMPORT_RESERVED = (
    "max_slots",
    "usage_quota",
    "quota_limit",
    "expires_at",
    "next_available_at",
    "mutex_group",
    "schedule_window",
    "priority",
    "preference",
    "source",
)


def _optional_int(value: Any, name: str, *, minimum: int = 0) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ConfigError(f"{name} must be an integer >= {minimum} or null")
    return int(value)


def _optional_float(value: Any, name: str = "value") -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigError(f"{name} must be a number or null")
    return float(value)


def _schedule_window(value: Any) -> tuple[str, str] | None:
    if value is None:
        return None
    if not isinstance(value, (list, tuple)) or len(value) != 2:
        raise ConfigError("schedule_window must be [start, end] in HH:MM")
    parts = []
    for raw in value:
        try:
            parsed = time.strptime(str(raw), "%H:%M")
        except ValueError:
            raise ConfigError(f"bad schedule_window time {raw!r}; expected HH:MM") from None
        parts.append(time.strftime("%H:%M", parsed))
    return (parts[0], parts[1])


def _apply_import_fields(resource: Resource, data: dict[str, Any]) -> None:
    """Apply scheduling fields carried alongside an import spec."""

    if "max_slots" in data:
        value = data["max_slots"]
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ConfigError("max_slots must be an integer > 0")
        resource.concurrency = Concurrency(max_slots=value)
    if "usage_quota" in data:
        resource.usage_quota = _optional_int(data["usage_quota"], "usage_quota")
    if "quota_limit" in data:
        raw = data["quota_limit"]
        if raw is None:
            resource.quota = None
        else:
            limit = _optional_int(raw, "quota_limit", minimum=1)
            if limit is not None:
                resource.quota = Quota(limit=limit)
    if "expires_at" in data:
        raw = data["expires_at"]
        if raw is not None and (isinstance(raw, bool) or not isinstance(raw, (int, float))):
            raise ConfigError("expires_at must be an epoch number or null")
        resource.expires_at = float(raw) if raw is not None else None
    if "next_available_at" in data:
        raw = data["next_available_at"]
        if raw is not None and (isinstance(raw, bool) or not isinstance(raw, (int, float))):
            raise ConfigError("next_available_at must be an epoch number or null")
        resource.next_available_at = float(raw) if raw is not None else None
    if "mutex_group" in data:
        raw = data["mutex_group"]
        resource.mutex_group = str(raw) if raw else None
    if "schedule_window" in data:
        resource.schedule_window = _schedule_window(data["schedule_window"])
    if "priority" in data:
        raw = data["priority"]
        if raw is None:
            resource.meta.pop("priority", None)
        else:
            if isinstance(raw, bool) or not isinstance(raw, int):
                raise ConfigError("priority must be an integer or null")
            resource.meta["priority"] = raw
    if "preference" in data:
        raw = data["preference"]
        if raw is None:
            resource.meta.pop("preference", None)
        else:
            if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                raise ConfigError("preference must be a number or null")
            resource.meta["preference"] = float(raw)
    source = data.get("source")
    if source:
        resource.meta["source"] = str(source)


@dataclass(slots=True)
class _WaitHandle:
    seq: int
    priority: int
    future: asyncio.Future[Any]
    req: AcquireRequest
    cancelled: bool = False
    serving: bool = False

    def __lt__(self, other: _WaitHandle) -> bool:
        if self.priority != other.priority:
            return self.priority > other.priority  # higher priority first
        return self.seq < other.seq

    def cancel_pending(self) -> None:
        """Mark the waiter as gone and cancel its future so the fair queue
        sweeps it instead of handing a lease to a dead waiter."""

        if not self.future.done():
            self.future.cancel()
            self.cancelled = True


@dataclass(slots=True)
class BatchLease:
    """Result of ``acquire_many``: a list of leases released together."""

    leases: list[Lease] = field(default_factory=list)

    def __len__(self) -> int:
        return len(self.leases)

    def __iter__(self) -> Iterator[Lease]:
        return iter(self.leases)

    async def release_all(
        self,
        pool: ResourcePool,
        outcome: UsageOutcome | str = UsageOutcome.OK,
    ) -> None:
        for lease in self.leases:
            await pool.release(lease, outcome=outcome)
        self.leases.clear()


class _TokenBucket:
    """Per-consumer admission bucket that refills continuously."""

    __slots__ = ("rate", "burst", "tokens", "updated")

    def __init__(self, rate: float, burst: int) -> None:
        self.rate = rate
        self.burst = float(burst)
        self.tokens = float(burst)
        self.updated = time.monotonic()

    def _refresh(self, now: float) -> None:
        elapsed = max(0.0, now - self.updated)
        if elapsed:
            self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
            self.updated = now

    def ready(self, now: float) -> bool:
        self._refresh(now)
        return self.tokens >= 1.0

    def consume(self, now: float) -> None:
        self._refresh(now)
        self.tokens = max(0.0, self.tokens - 1.0)

    def wait_time(self, now: float) -> float:
        self._refresh(now)
        if self.tokens >= 1.0:
            return 0.0
        return (1.0 - self.tokens) / self.rate


@dataclass(slots=True)
class _FairWaiter:
    """One bundle fairness admission waiter (priority heap entry payload)."""

    seq: int
    priority: int
    event: Any = field(default=None, repr=False, compare=False)
    cancelled: bool = False

