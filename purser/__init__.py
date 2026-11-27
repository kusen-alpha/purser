"""Purser: a generic resource scheduling library for scraping workloads.

Manage proxies, accounts or any custom resource across their full lifecycle —
generate, validate, lease, quarantine, ban, unban, drain, archive — over two
orthogonal planes: a durable storage plane (memory / sqlite) for master data
and important state, plus a transient coordination plane (memory / redis) for
in-flight leases, heartbeats and wake-ups.

Quick start (the runtime auto-starts on first acquire)::

    from purser import Purser

    ps = Purser()
    pool = ps.create_pool("proxies", "proxy")
    await pool.import_resources([{"host": "1.2.3.4", "port": 8080}])
    async with pool.lease(consumer="crawler-a") as lease:
        print(lease.rid, lease.payload)

Low-frequency operator work (ban / drain / revoke / maintenance) lives on
``pool.admin`` so the data plane stays small::

    await pool.admin.ban("p2", reason="dead upstream")

Synchronous facade (blocking calls from any thread)::

    from purser import SyncPurser

    with SyncPurser() as ps:
        pool = ps.create_pool("accounts", "account")
        pool.generate([{"username": "u1"}])
        with pool.lease(consumer="worker") as lease:
            print(lease.payload)
"""

import warnings

from purser.adapter_spec import AdapterSpec, load_adapter
from purser.adapters import (
    AccountAdapter,
    AccountResource,
    ProxyAdapter,
    ProxyResource,
    ResourceAdapter,
    clear_default_adapters,
    register_default_adapter,
    validator,
)
from purser.backends.coordination import MemoryCoordination, RedisCoordination
from purser.backends.memory import MemoryStorage
from purser.backends.mysql import MySqlStorage
from purser.backends.sqlite import SqliteStorage
from purser.config import (
    POOL_PROFILES,
    AcquireMode,
    BundleMode,
    CoordinationKind,
    ExpirePolicy,
    ImportValidate,
    PoolConfig,
    SchedulerKind,
    StorageKind,
    UnbanMode,
    Weights,
)
from purser.core.allocator import Allocator, allocator_names, register_allocator
from purser.core.bundle import (
    BundleFairness,
    BundleLease,
    BundleRequest,
    BundleSlot,
    MatchRule,
)
from purser.core.health import HealthReport
from purser.core.resource import (
    AcquireRequest,
    CheckVerdict,
    Lease,
    LeaseView,
    PayloadField,
    Resource,
    UsageOutcome,
)
from purser.core.state import ResourceState
from purser.core.views import (
    ActiveUsage,
    ActiveUsageGroup,
    PoolStats,
    PoolUsage,
    UsageGroup,
    UsageReport,
    UsageTotals,
    UsageWindow,
    ValidateJobCancelView,
    ValidateJobError,
    ValidateJobView,
)
from purser.errors import (
    BackendUnavailable,
    BadPayload,
    BadState,
    BundleAcquireFailed,
    BundleQuotaExceeded,
    ConfigError,
    InvalidTransition,
    IsolationConflict,
    LeaseConflict,
    LeaseExpired,
    LeaseLimitReached,
    LeaseLost,
    LeaseRevoked,
    NoSuitableResource,
    NotStarted,
    PoolEmpty,
    PoolExhausted,
    PoolExists,
    PoolNotFound,
    PurserError,
    QuotaExceeded,
    ResourceBusy,
    ResourceNotFound,
    VersionConflict,
    error_by_code,
    is_retryable,
)
from purser.facade import (
    BatchLease,
    ImportReport,
    Purser,
    ResourcePool,
)
from purser.local import (
    LocalScheduler,
    SchedulerAddress,
    SchedulerProtocolError,
    SchedulerTimeout,
    SchedulerUnavailable,
)
from purser.process import ProcessPurser
from purser.protocols import PoolProtocol, PurserProtocol, SyncPoolProtocol
from purser.remote import RemotePool, connect
from purser.sync import (
    SyncBatchLease,
    SyncLease,
    SyncPool,
    SyncPurser,
    SyncResourceIterator,
    SyncSubscription,
)
from purser.thread import ThreadPool, ThreadPurser

EmbeddedPurser = Purser

__version__ = "0.6.0"

__all__ = [
    # Core path: embedded async facade + multi-process connect().
    "Purser",
    "ResourcePool",
    "connect",
    "RemotePool",
    "LocalScheduler",
    "AcquireRequest",
    "Lease",
    "LeaseView",
    "BatchLease",
    "Resource",
    "ProxyResource",
    "AccountResource",
    "ResourceAdapter",
    "UsageOutcome",
    "HealthReport",
    "ImportReport",
    "PurserError",
    "is_retryable",
    # Sync facade.
    "SyncPurser",
    "SyncPool",
    "SyncLease",
    "SyncBatchLease",
    "SyncResourceIterator",
    "SyncSubscription",
    # Alternative runtimes (thread / process facades, scheduler errors).
    "ThreadPurser",
    "ThreadPool",
    "ProcessPurser",
    "SchedulerAddress",
    "SchedulerProtocolError",
    "SchedulerTimeout",
    "SchedulerUnavailable",
    # Adapters and typed resources.
    "AdapterSpec",
    "load_adapter",
    "PayloadField",
    "register_default_adapter",
    "validator",
    "clear_default_adapters",
    "ProxyAdapter",
    "AccountAdapter",
    "CheckVerdict",
    # Error taxonomy.
    "ConfigError",
    "IsolationConflict",
    "NotStarted",
    "PoolNotFound",
    "PoolExists",
    "ResourceNotFound",
    "PoolEmpty",
    "PoolExhausted",
    "NoSuitableResource",
    "ResourceBusy",
    "LeaseConflict",
    "LeaseExpired",
    "LeaseRevoked",
    "LeaseLost",
    "LeaseLimitReached",
    "BadState",
    "InvalidTransition",
    "QuotaExceeded",
    "BadPayload",
    "VersionConflict",
    "BackendUnavailable",
    "error_by_code",
    # Pool configuration.
    "PoolConfig",
    "POOL_PROFILES",
    "Weights",
    "StorageKind",
    "CoordinationKind",
    "SchedulerKind",
    "AcquireMode",
    "UnbanMode",
    "ImportValidate",
    "ExpirePolicy",
    "BundleMode",
    # Allocators.
    "Allocator",
    "allocator_names",
    "register_allocator",
    # Storage / coordination backends.
    "MemoryStorage",
    "SqliteStorage",
    "MySqlStorage",
    "MemoryCoordination",
    "RedisCoordination",
    # Bundles (atomic cross-pool acquisition).
    "BundleLease",
    "BundleRequest",
    "BundleSlot",
    "MatchRule",
    "BundleFairness",
    "BundleAcquireFailed",
    "BundleQuotaExceeded",
    # Views, protocols and compatibility aliases.
    "PoolStats",
    "UsageReport",
    "UsageWindow",
    "UsageTotals",
    "UsageGroup",
    "ActiveUsage",
    "ActiveUsageGroup",
    "PoolUsage",
    "ValidateJobView",
    "ValidateJobCancelView",
    "ValidateJobError",
    "ResourceState",
    "PurserProtocol",
    "PoolProtocol",
    "SyncPoolProtocol",
    "EmbeddedPurser",
]


def __getattr__(name: str) -> object:
    """Resolve deprecated pre-0.6 spellings with a warning."""

    if name in ("LeaseRevokedError", "LeaseLostError"):
        replacement = name.removesuffix("Error")
        warnings.warn(
            f"purser.{name} is deprecated; use {replacement}",
            DeprecationWarning,
            stacklevel=2,
        )
        return globals()[replacement]
    if name == "register":
        warnings.warn(
            "purser.register is deprecated; use purser.register_default_adapter",
            DeprecationWarning,
            stacklevel=2,
        )
        from purser.adapters.base import register_default_adapter

        return register_default_adapter
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
