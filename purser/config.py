"""Pool configuration with schema versioning (Y4) and validation (X8)."""

from __future__ import annotations

import math
from dataclasses import dataclass, field, replace
from typing import Any

from purser.core.quarantine import QuarantinePolicy
from purser.errors import ConfigError


@dataclass(slots=True)
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
_SCHEDULERS = ("fifo", "priority_quota", "token_bucket")
_UNBAN_MODES = ("manual_only", "auto_after_ttl", "mixed")
_IMPORT_VALIDATE = ("none", "on_import", "batch")


@dataclass(slots=True)
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
    target_quota: dict[str, int] | None = None

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
            not math.isfinite(self.heartbeat_interval) or self.heartbeat_interval < 0
        ):
            raise ConfigError("heartbeat_interval must be finite and >= 0")
        if self.heartbeat_interval is None:
            self.heartbeat_interval = max(1.0, self.lease_ttl / 3.0)
        if self.top_k <= 0:
            raise ConfigError("top_k must be > 0")
        if not isinstance(self.weights, Weights):
            self.weights = Weights.from_dict(self.weights)
        self.weights.affinity = self.affinity_weight

    def with_overrides(self, **updates: Any) -> PoolConfig:
        """Return a shallow copy with the given knobs replaced (O16)."""

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
