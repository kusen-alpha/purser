"""String enums for pool configuration values.

Kept separate from :mod:`purser.config` so schema metadata can reference the
legal values without importing ``PoolConfig``.
"""

from __future__ import annotations

import enum
from typing import TypeVar

from purser.errors import ConfigError


class _Kind(str, enum.Enum):
    """String enum that renders as its value on 3.10 (no ``StrEnum``)."""

    def __str__(self) -> str:
        return str(self.value)


class StorageKind(_Kind):
    MEMORY = "memory"
    SQLITE = "sqlite"
    MYSQL = "mysql"


class CoordinationKind(_Kind):
    MEMORY = "memory"
    REDIS = "redis"


class SchedulerKind(_Kind):
    PRIORITY_QUOTA = "priority_quota"
    FIFO = "fifo"
    TOKEN_BUCKET = "token_bucket"


class AcquireMode(_Kind):
    LEASE = "lease"
    STATELESS = "stateless"


class UnbanMode(_Kind):
    MANUAL_ONLY = "manual_only"
    AUTO_AFTER_TTL = "auto_after_ttl"
    MIXED = "mixed"


class ImportValidate(_Kind):
    NONE = "none"
    ON_IMPORT = "on_import"
    BATCH = "batch"


class ExpirePolicy(_Kind):
    SUSPENDED = "suspended"
    QUOTA = "quota"


class BundleMode(_Kind):
    ROLLBACK = "rollback"
    TWO_PHASE = "two_phase"


_EnumT = TypeVar("_EnumT", bound=_Kind)


def _values(kind: type[_Kind]) -> tuple[str, ...]:
    return tuple(str(item.value) for item in kind)


def _coerce_kind(kind: type[_EnumT], value: object, name: str) -> _EnumT:
    if isinstance(value, kind):
        return value
    try:
        return kind(value)
    except ValueError:
        choices = ", ".join(_values(kind))
        raise ConfigError(
            f"unknown {name} {value!r}; expected one of: {choices}"
        ) from None
