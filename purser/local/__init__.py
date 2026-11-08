"""Same-host multiprocessing scheduler over ``multiprocessing.connection``.

The scheduler process owns the only ``SyncPurser``; worker processes talk to
it with a small JSON protocol.  This is separate from ``purser serve`` (HTTP),
which exists for the UI.
"""

from purser.local.client import (
    LocalSchedulerClient,
    RemoteLease,
    close_client,
    get_client,
    init_worker,
    set_default_address,
)
from purser.local.errors import (
    SchedulerProtocolError,
    SchedulerTimeout,
    SchedulerUnavailable,
)
from purser.local.protocol import ENV_VAR, SchedulerAddress
from purser.local.scheduler import LocalScheduler

__all__ = [
    "ENV_VAR",
    "LocalScheduler",
    "LocalSchedulerClient",
    "RemoteLease",
    "SchedulerAddress",
    "SchedulerProtocolError",
    "SchedulerTimeout",
    "SchedulerUnavailable",
    "close_client",
    "get_client",
    "init_worker",
    "set_default_address",
]
