"""Domain model: resources, leases, health, quotas and acquisition requests."""

from __future__ import annotations

import enum
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - Python < 3.9
    from backports.zoneinfo import ZoneInfo  # type: ignore[no-redef]

from purser.core.state import ResourceState


def _now() -> float:
    return time.time()


def new_trace_id() -> str:
    return uuid.uuid4().hex


class UsageOutcome(str, enum.Enum):
    """Structured classification of how a leased resource was used (U1)."""

    OK = "ok"
    CAPTCHA = "captcha"
    RATE_LIMITED = "rate_limited"
    BANNED = "banned"
    TIMEOUT = "timeout"
    NETWORK_ERROR = "network_error"
    QUOTA_EXHAUSTED = "quota_exhausted"
    PREEMPTED = "preempted"


class CheckVerdict(str, enum.Enum):
    """Decision returned by request-level hooks (4.5)."""

    PASS = "pass"
    FAIL = "fail"
    BAN = "ban"


@dataclass
class Concurrency:
    """Slot accounting for EXCLUSIVE (``max_slots == 1``) and SHARED resources."""

    max_slots: int = 1
    used_slots: int = 0

    @property
    def available(self) -> int:
        return self.max_slots - self.used_slots

    @property
    def load_ratio(self) -> float:
        if self.max_slots <= 0:
            return 1.0
        return min(1.0, self.used_slots / self.max_slots)

    def can_acquire(self, slots: int) -> bool:
        return slots > 0 and self.used_slots + slots <= self.max_slots

    def acquire(self, slots: int) -> None:
        if not self.can_acquire(slots):
            raise ValueError(f"over-commit: used={self.used_slots} +{slots} > max={self.max_slots}")
        self.used_slots += slots

    def release(self, slots: int) -> None:
        if self.used_slots - slots < 0 or slots <= 0:
            raise ValueError(f"invalid release: used={self.used_slots} -{slots}")
        self.used_slots -= slots


@dataclass
class SiteFailure:
    """Per-target-site failure marker (5.8)."""

    site: str
    count: int = 1
    since: float = field(default_factory=_now)
    until: float = field(default_factory=lambda: _now() + 300.0)

    def active(self, now: float | None = None) -> bool:
        return (now or _now()) < self.until


@dataclass
class HealthStats:
    """EWMA health score plus latency bookkeeping (5.3 / Y6)."""

    score: float = 0.5          # cold start defaults to the neutral midpoint
    samples: int = 0
    last_latency_ms: float | None = None
    last_checked_at: float | None = None
    last_error: str | None = None
    consecutive_failures: int = 0

    @property
    def degraded(self) -> bool:
        return self.score < 0.35

    def update(
        self,
        alive: bool,
        *,
        latency_ms: float | None = None,
        error: str | None = None,
        now: float | None = None,
    ) -> None:
        alpha = 0.6 if self.samples == 0 else 0.3  # Y6: faster cold-start converge
        outcome = 1.0 if alive else 0.0
        self.score = round(self.score * (1.0 - alpha) + outcome * alpha, 4)
        self.samples += 1
        self.last_latency_ms = latency_ms
        self.last_checked_at = now or _now()
        self.last_error = error
        self.consecutive_failures = (
            self.consecutive_failures + 1 if not alive else 0
        )


@dataclass
class Quota:
    """Period quota (S16): a daily budget consumed on release and reset daily."""

    limit: int
    used: int = 0
    reset_at: float | None = None

    @property
    def exhausted(self) -> bool:
        return self.used >= self.limit

    def consume(self, n: int = 1) -> bool:
        if self.used + n > self.limit:
            return False
        self.used += n
        return True

    def top_up(self, n: int | None = None) -> None:
        self.used = max(0, self.used - n) if n is not None else 0

    def reset_daily(self, now: float | None = None, timezone: str | None = None) -> bool:
        now = now or _now()
        if self.reset_at is not None and now < self.reset_at:
            return False
        self.used = 0

        self.reset_at = _next_reset(now, timezone)
        return True


def _next_reset(now: float, timezone: str | None = None) -> float:
    """Epoch of the next midnight boundary in the configured zone (defaults
    to the process local zone when no valid IANA name is given)."""

    import datetime

    tzinfo = None
    if timezone:
        try:
            tzinfo = ZoneInfo(timezone)
        except Exception:
            tzinfo = None
    dt = datetime.datetime.fromtimestamp(now, tzinfo)
    nxt = (dt + datetime.timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return nxt.timestamp()


@dataclass
class Lease:
    """A single borrow: the holder's credential and expiry bookkeeping (5.2)."""

    token: str
    pool: str
    resource_id: str
    consumer: str | None = None
    slots: int = 1
    started_at: float = field(default_factory=_now)
    expires_at: float = field(default_factory=lambda: _now() + 60.0)
    heartbeat_at: float = field(default_factory=_now)
    priority: int = 0
    use_limit: int | None = None
    used: int = 0
    trace_id: str | None = None
    outcome: UsageOutcome = UsageOutcome.OK
    target: str | None = None
    payload: dict[str, Any] | None = None  # populated by the pool for holder convenience

    @staticmethod
    def new(
        pool: str,
        resource_id: str,
        *,
        consumer: str | None = None,
        slots: int = 1,
        ttl: float = 60.0,
        priority: int = 0,
        use_limit: int | None = None,
        trace_id: str | None = None,
        now: float | None = None,
    ) -> Lease:
        now = now or _now()
        return Lease(
            token=uuid.uuid4().hex,
            pool=pool,
            resource_id=resource_id,
            consumer=consumer,
            slots=slots,
            started_at=now,
            expires_at=now + ttl,
            heartbeat_at=now,
            priority=priority,
            use_limit=use_limit,
            trace_id=trace_id,
        )

    def expired(self, now: float | None = None) -> bool:
        return (now or _now()) >= self.expires_at


@dataclass
class Resource:
    """One resource: a proxy, account or custom object (3.2)."""

    id: str
    kind: str
    payload: dict[str, Any]
    state: ResourceState = ResourceState.NEW
    version: int = 0
    tags: frozenset[str] = frozenset()
    concurrency: Concurrency = field(default_factory=Concurrency)
    leases: dict[str, Lease] = field(default_factory=dict)
    health: HealthStats = field(default_factory=HealthStats)
    site_failures: dict[str, SiteFailure] = field(default_factory=dict)
    quota: Quota | None = None
    usage_quota: int | None = None
    used_count: int = 0
    expires_at: float | None = None
    next_available_at: float | None = None
    mutex_group: str | None = None
    schedule_window: tuple[str, str] | None = None
    meta: dict[str, Any] = field(default_factory=dict)

    @property
    def available_slots(self) -> int:
        return self.concurrency.available

    @property
    def allocatable(self) -> bool:
        return (
            self.state in (ResourceState.IDLE, ResourceState.ACTIVE)
            and self.available_slots > 0
        )

    @property
    def created_at(self) -> float:
        return float(self.meta.get("created_at", 0.0))

    @property
    def updated_at(self) -> float:
        return float(self.meta.get("updated_at", 0.0))

    @property
    def last_used_at(self) -> float | None:
        v = self.meta.get("last_used_at")
        return float(v) if v is not None else None

    def mark_updated(self, now: float | None = None) -> None:
        self.meta["updated_at"] = now or _now()

    def next_usable_at(
        self, now: float | None = None, consumer: str | None = None
    ) -> float:
        """Earliest moment this resource may be allocated (F6: max() merge).

        Only "not usable before" constraints participate: cooldowns, a
        quarantine stay and consumer-level pauses.  Absolute ``expires_at`` is
        a deadline, not a wait-until, and is enforced separately by the
        allocation filter.
        """

        now = now or _now()
        candidates = [0.0]
        if self.next_available_at is not None:
            candidates.append(self.next_available_at)
        if consumer is not None:
            key = f"cooldown:{consumer}"
            v = self.meta.get(key)
            if v is not None:
                candidates.append(float(v))
        if self.meta.get("quarantine_until") is not None:
            candidates.append(float(self.meta["quarantine_until"]))
        return max(candidates)


@dataclass
class AcquireRequest:
    """Everything an acquire operation may need (4.5)."""

    consumer: str | None = None
    target: str | None = None
    priority: int = 0
    slots: int = 1
    tags: frozenset[str] = frozenset()
    timeout: float | None = None
    probe: str | None = None
    trace_id: str | None = None
    use_limit: int | None = None
    allow_duplicate: bool = False
    ignore_cooldown: bool = False
    resource_id: str | None = None
    required_tag: str | None = None  # tag that must match (hard constraint)
    affinity: str | None = None

    def ensure_trace(self) -> str:
        if self.trace_id is None:
            self.trace_id = new_trace_id()
        return self.trace_id


def describe_resource(resource: Resource, *, secret: bool = False) -> dict[str, Any]:
    """Public-facing (masked by default) view of a resource (A1)."""

    payload: dict[str, Any]
    if secret:
        payload = dict(resource.payload)
    else:
        payload = {
            k: "***" for k in resource.payload
        } if resource.payload else {}
    return {
        "id": resource.id,
        "kind": resource.kind,
        "state": resource.state.value,
        "tags": sorted(resource.tags),
        "payload": payload,
        "max_slots": resource.concurrency.max_slots,
        "used_slots": resource.concurrency.used_slots,
        "version": resource.version,
        "health": {
            "score": resource.health.score,
            "samples": resource.health.samples,
            "last_latency_ms": resource.health.last_latency_ms,
            "last_checked_at": resource.health.last_checked_at,
            "consecutive_failures": resource.health.consecutive_failures,
        },
        "used_count": resource.used_count,
        "usage_quota": resource.usage_quota,
        "expires_at": resource.expires_at,
        "next_available_at": resource.next_available_at,
        "leases": {
            t: {
                "consumer": lease.consumer,
                "slots": lease.slots,
                "expires_at": lease.expires_at,
            }
            for t, lease in resource.leases.items()
        },
        "site_failures": {
            site: {"count": f.count, "until": f.until}
            for site, f in resource.site_failures.items()
        },
        "meta": dict(resource.meta),
    }
