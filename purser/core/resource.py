"""Domain model: resources, leases, health, quotas and acquisition requests."""

from __future__ import annotations

import enum
import time
import uuid
from collections.abc import Callable
from copy import deepcopy
from dataclasses import InitVar, asdict, dataclass, field, replace
from typing import Any, ClassVar, TypeVar, get_origin, get_type_hints

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - Python < 3.9
    from backports.zoneinfo import ZoneInfo  # type: ignore[no-redef]

from purser.core.state import ResourceState
from purser.errors import ConfigError

_MISSING = object()
ResourceT = TypeVar("ResourceT", bound="Resource")


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
    resource: Resource | None = field(
        default=None, repr=False, compare=False
    )  # typed snapshot loaded by this process at acquire time

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


@dataclass(frozen=True)
class PayloadField:
    """Declarative metadata for one typed payload field.

    A subclass lists these in :attr:`Resource.payload_fields`; the field names
    become ``typed_payload_fields`` automatically.  One table therefore drives
    coercion, required checks, defaults and at-rest encryption.

    ``coerce`` must be pure and idempotent: hydration may run it again on an
    already-typed value.  ``required`` means present and non-empty (``None``
    and ``""`` are rejected; ``0`` and ``False`` are valid).
    """

    name: str
    coerce: Callable[[Any], Any] | None = None
    required: bool = False
    secret: bool = False
    default: Any = None
    doc: str = ""

    def normalize(self, value: Any) -> Any:
        return value if self.coerce is None else self.coerce(value)


@dataclass
class Resource:
    """One resource: a proxy, account or custom object (3.2).

    ``payload`` is a derived view: ``_payload`` only keeps custom keys while
    subclasses declare their common fields in ``payload_fields``.  The merge
    happens on read, so typed attributes never drift from the bag.  Writing
    ``payload`` replaces the typed view as well; unknown keys stay in
    :attr:`extra`.

    This is not a plain DTO: ``dataclasses.asdict`` exposes ``_payload`` and
    ``dataclasses.replace(..., payload=...)`` cannot apply typed payload keys.
    Use :meth:`to_dict` and :meth:`copy_with` instead.
    """

    id: str
    kind: str
    payload: InitVar[dict[str, Any] | None] = None
    _payload: dict[str, Any] = field(default_factory=dict, repr=False, init=False)
    state: ResourceState = ResourceState.NEW
    version: int = 0
    tags: frozenset[str] = frozenset()
    concurrency: Concurrency = field(default_factory=Concurrency)
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

    payload_fields: ClassVar[tuple[PayloadField, ...]] = ()
    typed_payload_fields: ClassVar[tuple[str, ...]] = ()
    _payload_field_map: ClassVar[dict[str, PayloadField]] = {}

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if "payload_fields" not in cls.__dict__:
            return
        declared = cls.payload_fields
        for spec in declared:
            if not isinstance(spec, PayloadField):
                raise TypeError(
                    f"{cls.__name__}.payload_fields must contain PayloadField"
                )
        names = [spec.name for spec in declared]
        if len(set(names)) != len(names):
            raise TypeError(f"{cls.__name__}.payload_fields has duplicate names")
        # Dataclass fields accumulate down the MRO, so declarations must too;
        # nearer bases override by name but never drop inherited fields.
        merged: dict[str, PayloadField] = {}
        for base in reversed(cls.__mro__[1:]):
            inherited = base.__dict__.get("payload_fields")
            if inherited is not None:
                merged.update({spec.name: spec for spec in inherited})
        merged.update({spec.name: spec for spec in declared})
        known: set[str] = set()
        for base in cls.__mro__:
            known.update(getattr(base, "__dataclass_fields__", {}))
            known.update(base.__dict__.get("__annotations__", {}))
        known -= {"payload", "_payload", "typed_payload_fields", "payload_fields"}
        try:
            hints = get_type_hints(cls)
        except Exception:
            hints = {}
        known -= {
            name for name, hint in hints.items() if get_origin(hint) is ClassVar
        }
        unknown = [name for name in merged if name not in known]
        if unknown:
            raise TypeError(
                f"{cls.__name__}.payload_fields names are not dataclass fields: "
                + ", ".join(unknown)
            )
        cls.payload_fields = tuple(merged.values())
        cls.typed_payload_fields = tuple(merged)
        cls._payload_field_map = merged

    def __post_init__(self, payload: dict[str, Any] | None) -> None:
        # The property shadows the InitVar default, so dataclasses passes the
        # property object itself when the caller omits ``payload``.
        if not isinstance(payload, dict):
            payload = None
        self._load_payload(payload, overwrite_typed=False)
        self._coerce_typed()

    def _load_payload(
        self, value: dict[str, Any] | None, *, overwrite_typed: bool
    ) -> None:
        data = dict(value or {})
        for name in self.typed_payload_fields:
            incoming = data.pop(name, _MISSING)
            if incoming is _MISSING:
                if overwrite_typed:
                    setattr(self, name, None)
                continue
            if overwrite_typed or getattr(self, name, None) is None:
                spec = self._payload_field_map.get(name)
                if incoming is not None and spec is not None:
                    incoming = self._coerce(spec, incoming)
                setattr(self, name, incoming)
        self._payload = data

    def _coerce(self, spec: PayloadField, value: Any) -> Any:
        try:
            return spec.normalize(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"invalid {spec.name!r} for {self.kind} resource: {value!r}"
            ) from exc

    def _coerce_typed(self) -> None:
        for name in self.typed_payload_fields:
            spec = self._payload_field_map.get(name)
            if spec is None or spec.coerce is None:
                continue
            value = getattr(self, name, None)
            if value is not None:
                setattr(self, name, self._coerce(spec, value))

    def normalize_payload(self, *, require: bool = False) -> None:
        """Coerce declared fields to their canonical types and fill defaults.

        ``adopt`` runs this with ``require=True`` on the write boundary, so
        missing required fields fail early; ``hydrate`` runs it without the
        flag so a stored record is typed but never rejected on read.

        Direct attribute assignment is not intercepted: typed attributes are
        canonical after construction and at adopt/hydrate boundaries, so call
        this after mutating them in bulk.  A default filled on read becomes
        part of the derived view but is only persisted by the next write.
        """

        for spec in self.payload_fields:
            value = getattr(self, spec.name, None)
            if value is None:
                if spec.default is None:
                    if require and spec.required:
                        raise ValueError(
                            f"{self.kind} resource requires {spec.name!r}"
                        )
                    continue
                value = deepcopy(spec.default)
                setattr(self, spec.name, value)
            elif require and spec.required and value == "":
                raise ValueError(f"{self.kind} resource requires {spec.name!r}")
            if spec.coerce is not None:
                setattr(self, spec.name, self._coerce(spec, value))

    @property
    def extra(self) -> dict[str, Any]:
        """Deep copy of the custom keys that have no declared typed field."""

        return deepcopy(self._payload)

    def to_dict(self) -> dict[str, Any]:
        """Deep-copy the resource in its storage shape.

        Typed fields are folded back into ``payload`` instead of appearing at
        the top level, which ``dataclasses.asdict`` cannot express.
        """

        data = asdict(self)
        custom = data.pop("_payload", {})
        typed: dict[str, Any] = {}
        for name in self.typed_payload_fields:
            value = data.pop(name, _MISSING)
            if value is not _MISSING and value is not None:
                typed[name] = value
        data["payload"] = {**custom, **typed}
        data["state"] = self.state.value
        data["tags"] = sorted(self.tags)
        return data

    def copy_with(
        self: ResourceT,
        *,
        payload: dict[str, Any] | None = None,
        **changes: Any,
    ) -> ResourceT:
        """Copy with base/typed changes, optionally replacing the whole payload.

        ``dataclasses.replace`` copies the merged payload but lets typed kwargs
        win, so a ``payload=`` change is silently ignored.  This is the
        supported replacement.
        """

        clone = replace(self, **changes)
        if payload is not None:
            clone.payload = dict(payload)
        return clone

    # mypy rejects an InitVar/property pair even though dataclasses allows it.
    @property  # type: ignore[misc]
    def payload(self) -> dict[str, Any]:  # noqa: F811
        """Detached view: mutating it never changes the resource."""

        data = deepcopy(self._payload)
        for name in self.typed_payload_fields:
            value = getattr(self, name)
            if value is None:
                data.pop(name, None)
            else:
                data[name] = deepcopy(value)
        return data

    @payload.setter
    def payload(self, value: dict[str, Any] | None) -> None:
        self._load_payload(value, overwrite_typed=True)

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

    def __post_init__(self) -> None:
        if isinstance(self.tags, str):
            raise TypeError(
                "AcquireRequest.tags must be an iterable of strings, not a str"
            )
        try:
            tags = frozenset(self.tags)
        except TypeError as exc:
            raise TypeError(
                "AcquireRequest.tags must be an iterable of strings"
            ) from exc
        if any(not isinstance(tag, str) or not tag for tag in tags):
            raise ValueError("AcquireRequest.tags must contain non-empty strings")
        self.tags = tags
        if self.probe is not None:
            raise ConfigError(
                "AcquireRequest.probe is not implemented; configure probe targets "
                "on the resource type: ProxyType(probe_targets=[...])"
            )
        if self.affinity is not None:
            raise ConfigError(
                "AcquireRequest.affinity is not implemented; enable affinity scoring "
                "with PoolConfig(affinity_weight=...)"
            )

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
            "last_error": resource.health.last_error,
            "consecutive_failures": resource.health.consecutive_failures,
        },
        "used_count": resource.used_count,
        "usage_quota": resource.usage_quota,
        "quota": (
            {
                "limit": resource.quota.limit,
                "used": resource.quota.used,
                "reset_at": resource.quota.reset_at,
            }
            if resource.quota is not None
            else None
        ),
        "expires_at": resource.expires_at,
        "next_available_at": resource.next_available_at,
        "mutex_group": resource.mutex_group,
        "schedule_window": (
            list(resource.schedule_window) if resource.schedule_window else None
        ),
        "site_failures": {
            site: {"count": f.count, "until": f.until}
            for site, f in resource.site_failures.items()
        },
        "meta": dict(resource.meta),
    }
