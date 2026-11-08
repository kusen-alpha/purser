"""Purser: a generic resource scheduling library for scraping workloads.

Manage proxies, accounts or any custom resource across their full lifecycle —
generate, validate, lease, quarantine, ban, unban, drain, archive — over two
orthogonal planes: a durable storage plane (memory / sqlite) for master data
and important state, plus a transient coordination plane (memory / redis) for
in-flight leases, heartbeats and wake-ups.

Quick start::

    from purser import AcquireRequest, Purser

    ps = Purser()
    pool = ps.create_pool("proxies", "proxy")
    await pool.import_resources([{"host": "1.2.3.4", "port": 8080}])
    async with ps:
        async with pool.lease(AcquireRequest(consumer="crawler-a")) as proxy:
            print(proxy.payload)

Synchronous facade (blocking calls from any thread)::

    from purser import SyncPurser

    with SyncPurser() as ps:
        pool = ps.create_pool("accounts", "account")
        pool.generate([{"username": "u1"}])
        with pool.lease(consumer="worker") as lease:
            print(lease.payload)
"""

from purser.backends.coordination import MemoryCoordination, RedisCoordination
from purser.backends.memory import MemoryStorage
from purser.backends.mysql import MySqlStorage
from purser.backends.sqlite import SqliteStorage
from purser.config import PoolConfig, Weights
from purser.core.health import HealthReport
from purser.core.resource import (
    AcquireRequest,
    CheckVerdict,
    Lease,
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
    ConfigError,
    InvalidTransition,
    IsolationConflict,
    LeaseConflict,
    LeaseExpired,
    LeaseLimitReached,
    LeaseLostError,
    LeaseRevokedError,
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
)
from purser.facade import (
    BatchLease,
    ImportReport,
    Purser,
    ResourcePool,
)
from purser.local import LocalScheduler, SchedulerAddress
from purser.sync import (
    SyncBatchLease,
    SyncLease,
    SyncPool,
    SyncPurser,
    SyncResourceIterator,
    SyncSubscription,
)
from purser.types import (
    AccountResource,
    AccountType,
    ProxyResource,
    ProxyType,
    ResourceType,
)

__version__ = "0.2.0"

__all__ = [
    "Purser",
    "ResourcePool",
    "BatchLease",
    "ImportReport",
    "SyncPurser",
    "SyncPool",
    "SyncLease",
    "SyncBatchLease",
    "SyncResourceIterator",
    "SyncSubscription",
    "LocalScheduler",
    "SchedulerAddress",
    "PoolConfig",
    "Weights",
    "MemoryStorage",
    "SqliteStorage",
    "MySqlStorage",
    "MemoryCoordination",
    "RedisCoordination",
    "Resource",
    "PayloadField",
    "Lease",
    "AcquireRequest",
    "CheckVerdict",
    "HealthReport",
    "UsageOutcome",
    "ResourceState",
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
    "ResourceType",
    "ProxyType",
    "ProxyResource",
    "AccountType",
    "AccountResource",
    "PurserError",
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
    "LeaseRevokedError",
    "LeaseLostError",
    "LeaseLimitReached",
    "BadState",
    "InvalidTransition",
    "QuotaExceeded",
    "BadPayload",
    "VersionConflict",
    "BackendUnavailable",
]
