"""Pool configuration with schema versioning and validation."""

from __future__ import annotations

import enum
import math
from collections.abc import Mapping
from dataclasses import dataclass, field, fields, replace
from typing import TYPE_CHECKING, Any, cast

from purser.allocator_registry import (
    ALLOCATOR_NAMES as _ALLOCATORS,
    DEFAULT_ALLOCATOR,
    register_allocator_name as register_allocator_name,
)
from purser.config_kinds import (
    AcquireMode as AcquireMode,
    BundleMode as BundleMode,
    CoordinationKind as CoordinationKind,
    ExpirePolicy as ExpirePolicy,
    ImportValidate as ImportValidate,
    SchedulerKind as SchedulerKind,
    StorageKind as StorageKind,
    UnbanMode as UnbanMode,
    _coerce_kind as _coerce_kind,
    _EnumT as _EnumT,
    _Kind as _Kind,
    _values as _values,
)
from purser.config_schema import (
    _FIELD_META as _FIELD_META,
    ConfigField as ConfigField,
)
from purser.core.quarantine import QuarantinePolicy
from purser.errors import ConfigError

if TYPE_CHECKING:
    from purser.adapters.base import ResourceAdapter

# The periodic sweeps a pool can run; ``scheduler_intervals`` keys must come
# from this set.  Kept here so PoolConfig can validate them without importing
# the facade (which would be circular).
MAINTENANCE_NAMES: tuple[str, ...] = (
    "quota_reset",
    "periodic_unban",
    "lost_gc",
    "validating_gc",
    "quarantine_gc",
    "archive_gc",
    "expire_gc",
    "reconcile",
)

# With ``import_validate="auto"`` (the default), imports up to this many
# records validate inline; larger batches submit a background validate job so
# import_resources returns immediately.
IMPORT_VALIDATE_AUTO_MAX = 10


def apply_adapter_defaults(
    cfg: PoolConfig, adapter: ResourceAdapter
) -> PoolConfig:
    """Merge an adapter's pool-level defaults into a PoolConfig.

    ``create_pool`` and ``PoolConfig.default_for`` both go through here so the
    adapter stays the single default source for ``max_slots`` and
    ``secret_fields``.
    """

    defaults = adapter.defaults()
    if cfg.max_slots is None and defaults.max_slots is not None:
        cfg = cfg.with_overrides(max_slots=defaults.max_slots)
    if not cfg.secret_fields and defaults.secret_fields:
        cfg = cfg.with_overrides(secret_fields=defaults.secret_fields)
    return cfg


@dataclass(slots=True)
class Weights:
    """Scoring weights for the health-weighted allocator."""

    health: float = 1.0
    age: float = 0.15      # longer-idle resources get a boost (LRU rotation)
    load: float = 0.30     # penalize already-busy shared resources
    preference: float = 0.0
    affinity: float = 0.0  # fed from config.affinity_weight at runtime
    jitter: float = 0.05   # anti-thundering-herd randomness

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> Weights:
        if not data:
            return cls()
        known = {f: float(data[f]) for f in cls.__dataclass_fields__ if f in data}
        return cls(**known)


# Fields that identify the storage/coordination planes or change the wire
# shape of a live pool.  They may be supplied at create time, but a PATCH may
# only repeat the current value (stats() round-trips include them).
IMMUTABLE_POOL_FIELDS: frozenset[str] = frozenset(
    {"storage", "coordination", "secret_fields", "event_queue_size", "schema_version"}
)

# Named starting points for the common deployment shapes.  A profile is only a
# merge base: explicit config/overrides always win, and the resolved values are
# what the pool stores.  Keep these small and transparent; users can always
# pass fields directly instead of learning a profile vocabulary.
POOL_PROFILES: dict[str, dict[str, Any]] = {
    # Small scripts: validate on import so resources are immediately usable.
    "basic": {
        "import_validate": "on_import",
        "requalify_on_release": True,
    },
    # Keep background checks aggressive for high-value credentials/proxies.
    "validated": {
        "import_validate": "on_import",
        "requalify_on_release": True,
        "background_validate_sample": 1.0,
    },
    # Shared accounts/proxies: spread load and only cool down the consumer.
    "shared": {
        "allocator": "least_loaded",
        "max_slots": 4,
        "cooldown_consumer_only": True,
    },
    # Long-running jobs: fewer heartbeats, longer leases, lazier background sweeps.
    "long_running": {
        "lease_ttl": 600.0,
        "heartbeat_interval": 200.0,
        "max_lease_duration": 3600.0,
        "stale_after": 300.0,
        "background_interval": 300.0,
        "wait_timeout": 60.0,
    },
    # Period-quota resources: unban automatically and expire like quota exhaustion.
    "quota": {
        "unban_mode": "auto_after_ttl",
        "requalify_on_release": True,
        "expire_policy": "quota",
        "quota_cooldown": 60.0,
    },
    # Simplest shape: sample a random resource without occupying a lease.
    "one_shot": {
        "acquire_mode": "stateless",
        "allocator": "random",
        "import_validate": "on_import",
    },
    # Keep lease semantics but pick uniformly instead of scoring.
    "random": {
        "allocator": "random",
    },
}


@dataclass(slots=True)
class PoolConfig:
    """Per-pool knobs.  ResourceAdapter provides defaults; pool overrides them."""

    kind: str
    storage: StorageKind = StorageKind.MEMORY
    coordination: CoordinationKind = CoordinationKind.MEMORY
    allocator: str = DEFAULT_ALLOCATOR
    scheduler: SchedulerKind = SchedulerKind.PRIORITY_QUOTA
    max_slots: int | None = None
    # ``lease`` occupies slots and needs release (or reaping); ``stateless``
    # samples a resource without tracking it, so release is a safe no-op.
    acquire_mode: AcquireMode = AcquireMode.LEASE
    max_leases_per_consumer: int | None = None
    # ``scheduler="token_bucket"`` admits per-consumer acquire attempts at
    # ``rate_limit`` per second with a burst of ``rate_burst``.
    rate_limit: float | None = None
    rate_burst: int | None = None

    unban_mode: UnbanMode = UnbanMode.MIXED
    unban_after: float = 86400.0  # BANNED -> VALIDATING after this TTL (auto)
    quarantine: QuarantinePolicy = field(default_factory=QuarantinePolicy)
    # What an absolute expiry does to the resource: flip it to
    # ``suspended`` or treat it like quota exhaustion (``quota``).
    expire_policy: ExpirePolicy = ExpirePolicy.SUSPENDED
    # Whether quarantine release re-validates the resource instead of idling.
    requalify_on_release: bool = False

    lease_ttl: float = 60.0
    heartbeat_interval: float | None = None  # defaults to ttl / 3
    max_lease_duration: float | None = None

    stale_after: float = 30.0
    background_interval: float = 30.0
    background_validate_sample: float = 1.0
    validate_timeout: float = 20.0
    validate_concurrency: int = 8

    cooldown_after_release: float = 0.0
    cooldown_consumer_only: bool = False
    quota_cooldown: float = 0.0
    usage_quota: int | None = None
    expires_in: float | None = None
    # Per-resource default for the period quota, applied at import time when
    # the resource does not set one itself.
    quota_limit: int | None = None

    site_failure_ttl: float = 300.0
    site_failure_max: int = 16
    check_cache_ttl: float = 2.0

    wait_timeout: float = 30.0
    fail_open: bool = False
    # ``auto`` (the default) validates small imports inline so the pool is
    # usable the moment import_resources returns, and hands large imports to a
    # background job instead. Explicit ``none`` stays available for callers
    # that pre-load data offline and validate later.
    import_validate: ImportValidate = ImportValidate.AUTO
    secret_fields: tuple[str, ...] = ()
    timezone: str = "Asia/Shanghai"
    schema_version: int = 1

    affinity_weight: float = 0.0
    preemptible: bool = False
    preempt_threshold: int = 10
    drain_timeout: float = 0.0
    archive_retention: float = 30 * 86400.0
    lost_retention: float = 4 * 3600.0
    # How long an expired (suspended) resource is kept before ``expire_gc``
    # archives it.  ``None`` disables the sweep (current behaviour).
    expired_retention: float | None = None

    top_k: int = 8
    event_queue_size: int = 10_000
    weights: Weights = field(default_factory=Weights)
    # Overrides for maintenance sweep intervals (keys from MAINTENANCE_NAMES);
    # ``None`` keeps the defaults.
    scheduler_intervals: dict[str, float] | None = None
    meta: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.kind:
            raise ConfigError("pool kind must not be empty")
        self.storage = _coerce_kind(StorageKind, self.storage, "storage")
        self.coordination = _coerce_kind(
            CoordinationKind, self.coordination, "coordination"
        )
        self.scheduler = _coerce_kind(SchedulerKind, self.scheduler, "scheduler")
        self.acquire_mode = _coerce_kind(AcquireMode, self.acquire_mode, "acquire_mode")
        self.unban_mode = _coerce_kind(UnbanMode, self.unban_mode, "unban_mode")
        self.import_validate = _coerce_kind(
            ImportValidate, self.import_validate, "import_validate"
        )
        self.expire_policy = _coerce_kind(
            ExpirePolicy, self.expire_policy, "expire_policy"
        )
        if self.max_slots is not None and self.max_slots <= 0:
            raise ConfigError("max_slots must be > 0")
        if self.allocator not in _ALLOCATORS:
            raise ConfigError(f"unknown allocator {self.allocator!r}")
        if self.max_leases_per_consumer is not None and (
            isinstance(self.max_leases_per_consumer, bool)
            or not isinstance(self.max_leases_per_consumer, int)
            or self.max_leases_per_consumer < 1
        ):
            raise ConfigError("max_leases_per_consumer must be an integer >= 1")
        if self.rate_limit is not None and (
            isinstance(self.rate_limit, bool)
            or not math.isfinite(float(self.rate_limit))
            or self.rate_limit <= 0
        ):
            raise ConfigError("rate_limit must be a finite number > 0")
        if self.rate_burst is not None and (
            isinstance(self.rate_burst, bool)
            or not isinstance(self.rate_burst, int)
            or self.rate_burst < 1
        ):
            raise ConfigError("rate_burst must be an integer >= 1")
        if self.scheduler is SchedulerKind.TOKEN_BUCKET and self.rate_limit is None:
            raise ConfigError(
                "scheduler='token_bucket' requires rate_limit (requests/second)"
            )
        if not 0.0 < self.background_validate_sample <= 1.0:
            raise ConfigError("background_validate_sample must be in (0, 1]")
        if self.lease_ttl <= 0:
            raise ConfigError("lease_ttl must be > 0")
        for name in (
            "lease_ttl",
            "wait_timeout",
            "background_interval",
            "stale_after",
            "validate_timeout",
            "check_cache_ttl",
            "site_failure_ttl",
        ):
            value = getattr(self, name)
            if not math.isfinite(value) or value < 0:
                raise ConfigError(f"{name} must be a finite, non-negative number")
        if self.heartbeat_interval is not None and (
            not math.isfinite(self.heartbeat_interval) or self.heartbeat_interval <= 0
        ):
            raise ConfigError("heartbeat_interval must be finite and > 0")
        if self.heartbeat_interval is None:
            # Must stay below the TTL: the heartbeat loop stops renewing once
            # the lease looks expired, so a fixed 1s floor would make every
            # lease with lease_ttl < 1s unrenewable.
            self.heartbeat_interval = max(0.01, self.lease_ttl / 3.0)
        if self.top_k <= 0:
            raise ConfigError("top_k must be > 0")
        if self.usage_quota is not None and (
            not isinstance(self.usage_quota, int) or self.usage_quota < 0
        ):
            raise ConfigError("usage_quota must be a non-negative integer")
        if self.quota_limit is not None and (
            isinstance(self.quota_limit, bool)
            or not isinstance(self.quota_limit, int)
            or self.quota_limit <= 0
        ):
            raise ConfigError("quota_limit must be a positive integer")
        if not isinstance(self.requalify_on_release, bool):
            raise ConfigError("requalify_on_release must be a boolean")
        if self.scheduler_intervals is not None:
            if not isinstance(self.scheduler_intervals, dict):
                raise ConfigError("scheduler_intervals must be an object")
            for name, interval in self.scheduler_intervals.items():
                if name not in MAINTENANCE_NAMES:
                    raise ConfigError(
                        f"scheduler_intervals has unknown maintenance task {name!r}"
                    )
                if (
                    isinstance(interval, bool)
                    or not isinstance(interval, (int, float))
                    or not math.isfinite(float(interval))
                    or interval <= 0
                ):
                    raise ConfigError(
                        f"scheduler_intervals[{name!r}] must be a positive number"
                    )
        if self.expires_in is not None and (
            not math.isfinite(self.expires_in) or self.expires_in < 0
        ):
            raise ConfigError("expires_in must be finite and >= 0")
        if self.max_lease_duration is not None and (
            not math.isfinite(self.max_lease_duration) or self.max_lease_duration <= 0
        ):
            raise ConfigError("max_lease_duration must be finite and > 0")
        if not math.isfinite(self.quota_cooldown) or self.quota_cooldown < 0:
            raise ConfigError("quota_cooldown must be a finite number >= 0")
        if not math.isfinite(self.unban_after) or self.unban_after < 0:
            raise ConfigError("unban_after must be finite and >= 0")
        if not isinstance(self.validate_concurrency, int) or self.validate_concurrency < 1:
            raise ConfigError("validate_concurrency must be an integer >= 1")
        if not isinstance(self.site_failure_max, int) or self.site_failure_max < 1:
            raise ConfigError("site_failure_max must be an integer >= 1")
        if not isinstance(self.event_queue_size, int) or self.event_queue_size < 1:
            raise ConfigError("event_queue_size must be an integer >= 1")
        for name in ("drain_timeout", "archive_retention", "lost_retention"):
            value = getattr(self, name)
            if not math.isfinite(value) or value < 0:
                raise ConfigError(f"{name} must be a finite, non-negative number")
        if self.expired_retention is not None and (
            not math.isfinite(self.expired_retention) or self.expired_retention < 0
        ):
            raise ConfigError("expired_retention must be a finite number >= 0")
        if self.schema_version != 1:
            raise ConfigError(f"unsupported schema_version {self.schema_version!r}")
        if not isinstance(self.preempt_threshold, int) or isinstance(
            self.preempt_threshold, bool
        ):
            raise ConfigError("preempt_threshold must be an integer")
        if self.preempt_threshold < 0:
            raise ConfigError("preempt_threshold must be >= 0")
        if not isinstance(self.fail_open, bool):
            raise ConfigError("fail_open must be a boolean")
        if not isinstance(self.preemptible, bool):
            raise ConfigError("preemptible must be a boolean")
        if not isinstance(self.weights, Weights):
            self.weights = Weights.from_dict(self.weights)
        self.weights.affinity = self.affinity_weight
        if not isinstance(self.quarantine, QuarantinePolicy):
            data = dict(self.quarantine or {})
            data.pop("rng", None)
            self.quarantine = QuarantinePolicy(**data)
        for name in ("base", "cap", "jitter", "trial_hours"):
            value = getattr(self.quarantine, name)
            if not math.isfinite(value) or value < 0:
                raise ConfigError(
                    f"quarantine.{name} must be a finite, non-negative number"
                )
        if not isinstance(self.quarantine.max_failures, int) or self.quarantine.max_failures < 1:
            raise ConfigError("quarantine.max_failures must be an integer >= 1")

    def with_overrides(self, **updates: Any) -> PoolConfig:
        """Return a shallow copy with the given knobs replaced."""

        cfg = replace(self, **updates)
        cfg.__post_init__()
        return cfg

    @classmethod
    def default_for(
        cls,
        kind: str,
        *,
        adapter: ResourceAdapter | None = None,
        max_slots: int | None = None,
        **overrides: Any,
    ) -> PoolConfig:
        """Build a config from adapter defaults plus caller overrides.

        Pass the ``ResourceAdapter`` for ``kind`` (from ``Purser.adapters``)
        to apply the same pool-level defaults (``max_slots`` /
        ``secret_fields``) that ``create_pool`` uses; caller ``max_slots``
        and ``overrides`` win.  Without an adapter the config is built from
        ``kind`` alone.
        """

        base = cls(kind=kind)
        if adapter is not None:
            base = apply_adapter_defaults(base, adapter)
        if max_slots is not None:
            base = base.with_overrides(max_slots=max_slots)
        if overrides:
            base = base.with_overrides(**overrides)
        return base

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        kind: str | None = None,
    ) -> PoolConfig:
        """Build a config from a plain mapping.

        ``kind`` may be supplied separately or included in ``data``::

            PoolConfig.from_dict({"kind": "proxy", "lease_ttl": 120})
            PoolConfig.from_dict({"lease_ttl": 120}, kind="proxy")
        """

        if not isinstance(data, Mapping):
            raise ConfigError("config must be an object")
        raw = dict(data)
        resolved_kind = kind or raw.pop("kind", None)
        if not resolved_kind:
            raise ConfigError("pool kind must not be empty")
        profile = raw.pop("profile", None)
        return resolve_pool_config(
            str(resolved_kind), profile=profile, config=raw
        )

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {}
        for name in self.__dataclass_fields__:
            value = getattr(self, name)
            data[name] = value.value if isinstance(value, enum.Enum) else value
        data["quarantine"] = {
            "base": self.quarantine.base,
            "cap": self.quarantine.cap,
            "jitter": self.quarantine.jitter,
            "max_failures": self.quarantine.max_failures,
            "trial_hours": self.quarantine.trial_hours,
        }
        data["weights"] = {
            f: getattr(self.weights, f) for f in Weights.__dataclass_fields__
        }
        return data


def _kind_for(value: Any) -> str:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "string"
    if isinstance(value, (list, tuple, set, frozenset)):
        return "array"
    if isinstance(value, Mapping):
        return "object"
    return "string"


def _nested_fields(
    cls: type,
    *,
    group: str,
    labels: Mapping[str, str],
    hidden: frozenset[str] = frozenset(),
    integer_fields: frozenset[str] = frozenset(),
) -> dict[str, ConfigField]:
    defaults = cls()
    out: dict[str, ConfigField] = {}
    for item in fields(cast(Any, cls)):
        name = item.name
        if name == "rng":
            continue
        default = getattr(defaults, name)
        out[name] = ConfigField(
            name=name,
            kind="int" if name in integer_fields else "float",
            group=group,
            label=labels.get(name, name),
            minimum=1 if name == "max_failures" else 0.0,
            hidden=name in hidden,
            default=default,
        )
    return out


_WEIGHTS_FIELDS = _nested_fields(
    Weights,
    group="scheduling",
    labels={
        "health": "健康分",
        "age": "空闲时长",
        "load": "负载惩罚",
        "preference": "偏好",
        "affinity": "亲和（派生）",
        "jitter": "抖动",
    },
    hidden=frozenset({"affinity"}),
)
_QUARANTINE_FIELDS = _nested_fields(
    QuarantinePolicy,
    group="lifecycle",
    labels={
        "base": "基础时长（秒）",
        "cap": "封顶（秒）",
        "jitter": "抖动（秒）",
        "max_failures": "最大失败次数",
        "trial_hours": "试用期（小时）",
    },
    integer_fields=frozenset({"max_failures"}),
)


def _build_pool_config_schema() -> dict[str, ConfigField]:
    defaults = PoolConfig(kind="__schema__")
    schema: dict[str, ConfigField] = {}
    for name in PoolConfig.__dataclass_fields__:
        if name == "kind":
            continue
        meta = dict(_FIELD_META.get(name, {}))
        default = getattr(defaults, name)
        nested: Mapping[str, ConfigField] = {}
        if name == "weights":
            nested = _WEIGHTS_FIELDS
        elif name == "quarantine":
            nested = _QUARANTINE_FIELDS
        schema[name] = ConfigField(
            name=name,
            kind=str(meta.pop("kind", "") or _kind_for(default)),
            group=str(meta.pop("group", "advanced")),
            label=str(meta.pop("label", name)),
            choices=tuple(meta.pop("choices", ())),
            minimum=meta.pop("minimum", None),
            maximum=meta.pop("maximum", None),
            mutable=bool(meta.pop("mutable", True)),
            nullable=bool(meta.pop("nullable", default is None)),
            hidden=bool(meta.pop("hidden", False)),
            unit=str(meta.pop("unit", "")),
            doc=str(meta.pop("doc", "")),
            default=default,
            fields=nested,
        )
    return schema


POOL_CONFIG_SCHEMA: dict[str, ConfigField] = _build_pool_config_schema()

RESOURCE_SCHEDULING_SCHEMA: dict[str, ConfigField] = {
    "max_slots": ConfigField(
        "max_slots", "int", "scheduling", "最大槽位", minimum=1, nullable=True
    ),
    "usage_quota": ConfigField(
        "usage_quota", "int", "quota", "使用次数上限", minimum=0, nullable=True
    ),
    "quota_limit": ConfigField(
        "quota_limit", "int", "quota", "周期配额上限", minimum=1, nullable=True
    ),
    "expires_at": ConfigField(
        "expires_at", "epoch", "lifecycle", "过期时间", nullable=True
    ),
    "next_available_at": ConfigField(
        "next_available_at", "epoch", "lifecycle", "下次可用时间", nullable=True
    ),
    "mutex_group": ConfigField(
        "mutex_group", "string", "lifecycle", "互斥组", nullable=True
    ),
    "schedule_window": ConfigField(
        "schedule_window", "window", "lifecycle", "可用时段", nullable=True
    ),
    "priority": ConfigField(
        "priority", "int", "scheduling", "静态优先级", nullable=True
    ),
    "preference": ConfigField(
        "preference", "float", "scheduling", "偏好加分", nullable=True
    ),
}


def _json_value(value: Any) -> Any:
    """Convert schema defaults to JSON-safe values for the console."""

    if isinstance(value, enum.Enum):
        return value.value
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_json_value(item) for item in value]
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if hasattr(value, "__dataclass_fields__"):
        return {
            name: _json_value(getattr(value, name))
            for name in value.__dataclass_fields__
            if name != "rng"
        }
    return str(value)


def _field_payload(field: ConfigField) -> dict[str, Any]:
    choices = list(field.choices)
    if field.name == "allocator":
        # Custom allocators registered after import must show up in the console.
        choices = sorted(_ALLOCATORS)
    payload: dict[str, Any] = {
        "name": field.name,
        "type": field.kind,
        "group": field.group,
        "label": field.label,
        "choices": choices,
        "min": field.minimum,
        "max": field.maximum,
        "mutable": field.mutable,
        "nullable": field.nullable,
        "hidden": field.hidden,
        "unit": field.unit,
        "doc": field.doc,
        "default": _json_value(field.default),
    }
    if field.fields:
        payload["fields"] = [_field_payload(item) for item in field.fields.values()]
    return payload


def config_schema_payload() -> dict[str, Any]:
    """Single read model for code, REST and the console.

    ``fields`` describe ``PoolConfig`` knobs; ``resource_fields`` describe the
    per-resource scheduling attributes accepted by import/update.  Profiles are
    merge bases, never a second configuration source.
    """

    return {
        "profiles": [
            {"name": name, "defaults": _json_value(values)}
            for name, values in POOL_PROFILES.items()
        ],
        "immutable": sorted(IMMUTABLE_POOL_FIELDS),
        "fields": [_field_payload(field) for field in POOL_CONFIG_SCHEMA.values()],
        "resource_fields": [
            _field_payload(field) for field in RESOURCE_SCHEDULING_SCHEMA.values()
        ],
    }


def profile_defaults(profile: str | Mapping[str, Any] | None) -> dict[str, Any]:
    """Resolve a named or inline profile into a config merge base."""

    if profile is None:
        return {}
    if isinstance(profile, Mapping):
        return parse_pool_config(profile)
    if not isinstance(profile, str):
        raise ConfigError("profile must be a name or an object")
    try:
        return dict(POOL_PROFILES[profile])
    except KeyError:
        names = ", ".join(sorted(POOL_PROFILES))
        raise ConfigError(f"unknown profile {profile!r}; available: {names}") from None


def _normalise_nested(data: dict[str, Any]) -> dict[str, Any]:
    if "secret_fields" in data:
        raw = data["secret_fields"]
        if isinstance(raw, str) or not isinstance(
            raw, (list, tuple, set, frozenset)
        ):
            raise ConfigError("secret_fields must be a list of non-empty strings")
        fields = tuple(str(item).strip() for item in raw)
        if any(not item for item in fields):
            raise ConfigError("secret_fields must not contain empty names")
        data["secret_fields"] = fields

    for key in ("weights", "quarantine", "scheduler_intervals", "meta"):
        if key in data and data[key] is not None:
            if not isinstance(data[key], Mapping):
                raise ConfigError(f"{key} must be an object")
            data[key] = dict(data[key])

    if data.get("weights") is not None:
        unknown = set(data["weights"]) - set(Weights.__dataclass_fields__)
        if unknown:
            raise ConfigError(
                f"unknown weights field(s): {', '.join(sorted(unknown))}"
            )
        for name, value in data["weights"].items():
            if isinstance(value, bool):
                raise ConfigError(f"weights.{name} must be a number")
            try:
                number = float(value)
            except (TypeError, ValueError):
                raise ConfigError(f"weights.{name} must be a number") from None
            if not math.isfinite(number):
                raise ConfigError(f"weights.{name} must be finite")

    if data.get("quarantine") is not None:
        quarantine = data["quarantine"]
        unknown = set(quarantine) - set(QuarantinePolicy.__dataclass_fields__)
        unknown.discard("rng")
        if unknown:
            raise ConfigError(
                f"unknown quarantine field(s): {', '.join(sorted(unknown))}"
            )
        for name in ("base", "cap", "jitter", "trial_hours"):
            if name not in quarantine:
                continue
            value = quarantine[name]
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ConfigError(f"quarantine.{name} must be a number")
            if not math.isfinite(float(value)) or value < 0:
                raise ConfigError(
                    f"quarantine.{name} must be a finite, non-negative number"
                )
        if "max_failures" in quarantine:
            value = quarantine["max_failures"]
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ConfigError("quarantine.max_failures must be an integer >= 1")

    if "meta" in data and data["meta"] is None:
        data["meta"] = {}
    return data


def parse_pool_config(
    raw: PoolConfig | Mapping[str, Any] | None = None,
    *,
    mode: str = "create",
    base: PoolConfig | None = None,
) -> dict[str, Any]:
    """Validate and normalise a pool config payload.

    ``mode="create"`` accepts every declared field.  ``mode="patch"`` drops
    immutable fields that merely repeat ``base`` (so a stats() round-trip is
    accepted) and rejects real attempts to change them.  The return value is a
    keyword-argument mapping ready for ``PoolConfig`` / ``with_overrides``.
    """

    if mode not in ("create", "patch"):
        raise ConfigError("config mode must be 'create' or 'patch'")
    if raw is None:
        data: dict[str, Any] = {}
    elif isinstance(raw, PoolConfig):
        data = raw.to_dict()
    elif isinstance(raw, Mapping):
        data = dict(raw)
    else:
        raise ConfigError("config must be an object")

    data.pop("kind", None)
    if "profile" in data:
        raise ConfigError("profile is a create-time preset; pass it separately")

    unknown = set(data) - set(POOL_CONFIG_SCHEMA)
    if unknown:
        raise ConfigError(
            f"unknown pool config field(s): {', '.join(sorted(unknown))}"
        )

    data = _normalise_nested(data)
    for name, value in data.items():
        if value is None and not POOL_CONFIG_SCHEMA[name].nullable:
            raise ConfigError(f"{name} cannot be null")

    if mode == "patch":
        if not isinstance(base, PoolConfig):
            raise ConfigError("patch mode requires the current PoolConfig")
        for name in sorted(IMMUTABLE_POOL_FIELDS & set(data)):
            if getattr(base, name) == data[name]:
                data.pop(name)
            else:
                raise ConfigError(f"cannot change {name} at runtime")
    return data


def build_pool_config(kind: str, data: Mapping[str, Any]) -> PoolConfig:
    """Construct a PoolConfig and keep the error surface stable."""

    if not isinstance(data, Mapping):
        raise ConfigError("config must be an object")
    try:
        return PoolConfig(kind=kind, **dict(data))
    except ConfigError:
        raise
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"invalid pool config: {exc}") from exc


def resolve_pool_config(
    kind: str,
    *,
    profile: str | Mapping[str, Any] | None = None,
    config: PoolConfig | Mapping[str, Any] | None = None,
    overrides: Mapping[str, Any] | None = None,
    defaults: Mapping[str, Any] | None = None,
) -> PoolConfig:
    """Merge defaults → profile → config → keyword overrides.

    Precedence is deliberately familiar: later sources win.  ``profile`` is
    only a merge base; it never survives as a second source of truth.
    """

    if isinstance(config, PoolConfig):
        if profile is not None:
            raise ConfigError(
                "profile cannot be combined with a PoolConfig; pass overrides instead"
            )
        cfg = config
    else:
        raw: dict[str, Any] = {}
        if config is not None:
            if not isinstance(config, Mapping):
                raise ConfigError("config must be an object or PoolConfig")
            raw = dict(config)
        embedded = raw.pop("profile", None)
        selected = profile if profile is not None else embedded
        merged = dict(defaults or {})
        merged.update(profile_defaults(selected))
        merged.update(parse_pool_config(raw))
        cfg = build_pool_config(kind, merged)

    if overrides:
        normalised = parse_pool_config(overrides)
        try:
            cfg = cfg.with_overrides(**normalised)
        except (TypeError, ValueError) as exc:
            raise ConfigError(f"invalid pool config override: {exc}") from exc
    return cfg
