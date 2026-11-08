"""Pool configuration with schema versioning (Y4) and validation (X8)."""

from __future__ import annotations

import math
import warnings
from dataclasses import dataclass, field, replace
from typing import Any

from purser.core.quarantine import QuarantinePolicy
from purser.errors import ConfigError


@dataclass
class Weights:
    """Scoring weights for the health-weighted allocator (5.1)."""

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


_ALLOCATORS = ("health_weighted", "round_robin", "least_loaded", "static_priority")
_SCHEDULERS = ("priority_quota",)
_RESERVED_SCHEDULERS = ("fifo", "token_bucket")
_UNBAN_MODES = ("manual_only", "auto_after_ttl", "mixed")
_IMPORT_VALIDATE = ("none", "on_import", "batch")


@dataclass
class PoolConfig:
    """Per-pool knobs.  ResourceType provides defaults; pool overrides them."""

    kind: str
    storage: str = "memory"
    coordination: str = "memory"
    allocator: str = "health_weighted"
    scheduler: str = "priority_quota"
    max_slots: int | None = None

    unban_mode: str = "mixed"
    unban_after: float = 86400.0  # BANNED -> VALIDATING after this TTL (auto)
    quarantine: QuarantinePolicy = field(default_factory=QuarantinePolicy)

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
    usage_quota: int | None = None
    expires_in: float | None = None
    total_quota: int | None = None
    target_quota: dict[str, int] | None = None  # deprecated alias for total_quota

    probe_targets: list[str] | None = None
    site_failure_ttl: float = 300.0
    site_failure_max: int = 16
    check_cache_ttl: float = 2.0

    wait_timeout: float = 30.0
    fail_open: bool = False
    import_validate: str = "none"
    secret_fields: tuple[str, ...] = ()
    timezone: str = "Asia/Shanghai"
    schema_version: int = 1

    affinity_weight: float = 0.0
    preemptible: bool = False
    preempt_threshold: int = 10
    drain_timeout: float = 30.0
    archive_retention: float = 30 * 86400.0
    lost_retention: float = 4 * 3600.0

    top_k: int = 8
    event_queue_size: int = 10_000
    weights: Weights = field(default_factory=Weights)
    meta: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.kind:
            raise ConfigError("pool kind must not be empty")
        if self.max_slots is not None and self.max_slots <= 0:
            raise ConfigError("max_slots must be > 0 (X8)")
        if self.storage not in ("memory", "sqlite", "mysql"):
            raise ConfigError(f"unknown storage {self.storage!r}")
        if self.coordination not in ("memory", "redis"):
            raise ConfigError(f"unknown coordination {self.coordination!r}")
        if self.allocator not in _ALLOCATORS:
            raise ConfigError(f"unknown allocator {self.allocator!r}")
        if self.scheduler in _RESERVED_SCHEDULERS:
            raise ConfigError(
                f"scheduler {self.scheduler!r} is not implemented; only "
                "'priority_quota' (priority first, FIFO within a priority) is available"
            )
        if self.scheduler not in _SCHEDULERS:
            raise ConfigError(f"unknown scheduler {self.scheduler!r}")
        if self.unban_mode not in _UNBAN_MODES:
            raise ConfigError(f"unknown unban_mode {self.unban_mode!r}")
        if self.import_validate not in _IMPORT_VALIDATE:
            raise ConfigError(f"unknown import_validate {self.import_validate!r}")
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
            self.heartbeat_interval = max(1.0, self.lease_ttl / 3.0)
        if self.top_k <= 0:
            raise ConfigError("top_k must be > 0")
        if self.usage_quota is not None and (
            not isinstance(self.usage_quota, int) or self.usage_quota < 0
        ):
            raise ConfigError("usage_quota must be a non-negative integer")
        if self.total_quota is not None and (
            isinstance(self.total_quota, bool)
            or not isinstance(self.total_quota, int)
            or self.total_quota <= 0
        ):
            raise ConfigError("total_quota must be a positive integer")
        if self.target_quota is not None:
            if self.total_quota is not None:
                raise ConfigError(
                    "set only total_quota; target_quota is its deprecated alias"
                )
            if not isinstance(self.target_quota, dict):
                raise ConfigError("target_quota must be a mapping of target to limit")
            for target, limit in self.target_quota.items():
                if not isinstance(limit, int) or limit < 0:
                    raise ConfigError(
                        f"target_quota[{target!r}] must be a non-negative integer"
                    )
            if sum(self.target_quota.values()) <= 0:
                raise ConfigError("target_quota must have a positive total")
            warnings.warn(
                "target_quota is deprecated; use total_quota=<sum of limits>. "
                "target_quota was never applied per target, only as a total quota.",
                DeprecationWarning,
                stacklevel=2,
            )
            self.total_quota = int(sum(self.target_quota.values()))
            self.target_quota = None
        if self.expires_in is not None and (
            not math.isfinite(self.expires_in) or self.expires_in < 0
        ):
            raise ConfigError("expires_in must be finite and >= 0")
        if self.max_lease_duration is not None and (
            not math.isfinite(self.max_lease_duration) or self.max_lease_duration <= 0
        ):
            raise ConfigError("max_lease_duration must be finite and > 0")
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
        if self.schema_version != 1:
            raise ConfigError(f"unsupported schema_version {self.schema_version!r}")
        if self.drain_timeout != 30.0:
            raise ConfigError(
                "drain_timeout is reserved for preemptive drains, which are not "
                "implemented; leave it at the default 30.0"
            )
        if self.preempt_threshold != 10:
            raise ConfigError(
                "preempt_threshold is reserved for preemptible leases, which are not "
                "implemented; leave it at the default 10"
            )
        if self.fail_open:
            raise ConfigError("fail_open is not implemented; storage fails closed")
        if self.preemptible:
            raise ConfigError("preemptible leases are not implemented yet")
        if self.probe_targets is not None:
            raise ConfigError(
                "pool-level probe_targets is not implemented; configure "
                "ProxyType(probe_targets=...)"
            )
        if not isinstance(self.weights, Weights):
            self.weights = Weights.from_dict(self.weights)
        self.weights.affinity = self.affinity_weight

    def with_overrides(self, **updates: Any) -> PoolConfig:
        """Return a shallow copy with the given knobs replaced (O16)."""

        # The deprecated alias replaces the canonical quota rather than colliding
        # with the value it was normalised into at construction time.
        if "target_quota" in updates and "total_quota" not in updates:
            updates["total_quota"] = None
        cfg = replace(self, **updates)
        cfg.__post_init__()
        return cfg

    @classmethod
    def default_for(
        cls, kind: str, *, max_slots: int | None = None, **overrides: Any
    ) -> PoolConfig:
        """Build a config from ResourceType defaults plus caller overrides."""

        base = cls(kind=kind)
        if max_slots is not None:
            base.max_slots = max_slots
        if overrides:
            base = base.with_overrides(**overrides)
        return base

    def to_dict(self) -> dict[str, Any]:
        data = {
            f: getattr(self, f) for f in self.__dataclass_fields__
        }
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
