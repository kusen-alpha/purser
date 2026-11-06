"""purSer: a generic resource scheduling library for scraping workloads.

Manage proxies, accounts or any custom resource across their full lifecycle —
generate, validate, lease, quarantine, ban, unban, drain, archive — over two
orthogonal planes: a durable storage plane (memory / sqlite) for master data
and important state, plus a transient coordination plane (memory / redis) for
in-flight leases, heartbeats and wake-ups.

Quick start::

    from purser import Purser
    from purser.core.resource import AcquireRequest

    ps = Purser()
    pool = ps.create_pool("proxies", "proxy")
    await pool.import_resources([{"host": "1.2.3.4", "port": 8080}])
    async with ps:
        async with pool.lease(AcquireRequest(consumer="crawler-a")) as proxy:
            print(proxy.payload)
"""

from purser.backends.coordination import MemoryCoordination, RedisCoordination
from purser.backends.memory import MemoryStorage
from purser.backends.mysql import MySqlStorage
from purser.backends.sqlite import SqliteStorage
from purser.config import PoolConfig, Weights
from purser.core.resource import (
    AcquireRequest,
    CheckVerdict,
    Lease,
    Resource,
    UsageOutcome,
)
from purser.core.state import ResourceState
from purser.facade import (
    BatchLease,
    ImportReport,
    Purser,
    ResourcePool,
)
from purser.types import AccountType, ProxyType, ResourceType

__version__ = "0.2.0"

__all__ = [
    "Purser",
    "ResourcePool",
    "BatchLease",
    "ImportReport",
    "PoolConfig",
    "Weights",
    "MemoryStorage",
    "SqliteStorage",
    "MySqlStorage",
    "MemoryCoordination",
    "RedisCoordination",
    "Resource",
    "Lease",
    "AcquireRequest",
    "CheckVerdict",
    "UsageOutcome",
    "ResourceState",
    "ResourceType",
    "ProxyType",
    "AccountType",
]
