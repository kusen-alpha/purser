"""Asynchronous facade over the local scheduler IPC.

``connect()`` turns a :class:`~purser.local.protocol.SchedulerAddress` plus a
pool name into an object shaped like the embedded
:class:`~purser.facade.ResourcePool` data plane: ``acquire``/``lease``/
``release``/``renew``/``stats``.  Every blocking IPC call runs in a worker
thread, so the calling event loop is never stalled.

The scheduler process remains the sole authority for slots; nothing here
creates a second ``Purser``.  Use :class:`~purser.local.LocalScheduler` to run
the authority in a child process, then hand its address to workers.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import Any

from purser.core.resource import AcquireRequest, Resource, UsageOutcome
from purser.errors import ConfigError
from purser.local.client import LocalSchedulerClient, RemoteLease
from purser.local.protocol import SchedulerAddress

__all__ = ["RemotePool", "connect"]


def _resolve_address(
    address: SchedulerAddress | dict[str, Any] | str | None,
) -> SchedulerAddress:
    if address is None:
        resolved = SchedulerAddress.from_env()
    elif isinstance(address, SchedulerAddress):
        resolved = address
    elif isinstance(address, dict):
        resolved = SchedulerAddress.from_dict(address)
    elif isinstance(address, str):
        resolved = SchedulerAddress.from_env(address)
    else:
        raise TypeError("address must be SchedulerAddress, dict, str or None")
    if resolved is None:
        raise ConfigError(
            "no scheduler address; pass one or set PURSER_LOCAL_SCHEDULER"
        )
    return resolved


class RemotePool:
    """Async, pool-scoped view of a scheduler process.

    Instances are cheap wrappers; share one per process (``connect`` caches
    nothing, but reusing the underlying client keeps one connection per
    thread).
    """

    def __init__(
        self,
        client: LocalSchedulerClient,
        name: str,
        *,
        resource_types: Mapping[str, type[Resource]] | None = None,
        owns_client: bool = False,
    ) -> None:
        if not name:
            raise ConfigError("RemotePool name cannot be empty")
        self._client = client
        self.name = name
        self._owns_client = owns_client
        self._closed = False
        for kind, cls in (resource_types or {}).items():
            client.register_resource_type(kind, cls)

    # -- identity ---------------------------------------------------------
    @property
    def address(self) -> SchedulerAddress:
        return self._client.address

    @property
    def closed(self) -> bool:
        return self._closed

    # -- data plane -------------------------------------------------------
    async def acquire(
        self,
        req: AcquireRequest | dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> RemoteLease:
        return await asyncio.to_thread(self._client.acquire, self.name, req, **kwargs)

    async def release(
        self,
        lease: RemoteLease | str,
        *,
        outcome: UsageOutcome | str | None = None,
        used: int | None = None,
    ) -> bool:
        return await asyncio.to_thread(
            self._client.release,
            self.name,
            lease,
            outcome=outcome,
            used=used,
        )

    async def renew(self, lease: RemoteLease | str) -> float:
        return await asyncio.to_thread(self._client.renew, self.name, lease)

    async def stats(self) -> dict[str, Any]:
        return await asyncio.to_thread(self._client.stats, self.name)

    async def import_resources(self, records: Any, **kwargs: Any) -> Any:
        return await asyncio.to_thread(
            self._client.import_resources,
            self.name,
            records,
            **kwargs,
        )

    async def generate(self, specs: Any, **kwargs: Any) -> Any:
        return await asyncio.to_thread(
            self._client.generate,
            self.name,
            specs,
            **kwargs,
        )

    @asynccontextmanager
    async def lease(
        self,
        req: AcquireRequest | dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> AsyncIterator[RemoteLease]:
        """Acquire and always release; renewal stays in the scheduler process."""

        lease = await self.acquire(req, **kwargs)
        try:
            yield lease
        finally:
            await asyncio.to_thread(lease.release)

    # -- lifecycle --------------------------------------------------------
    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._owns_client:
            self._client.close()

    async def __aenter__(self) -> RemotePool:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()

    def __repr__(self) -> str:
        return f"RemotePool(name={self.name!r}, address={self.address.address!r})"


def connect(
    pool: str,
    address: SchedulerAddress | dict[str, Any] | str | None = None,
    *,
    resource_types: Mapping[str, type[Resource]] | None = None,
    client: LocalSchedulerClient | None = None,
) -> RemotePool:
    """Return an async pool facade for ``pool`` on the scheduler at ``address``.

    ``address`` defaults to ``PURSER_LOCAL_SCHEDULER``.  Pass ``client`` to
    share connections with other code; otherwise the returned pool owns its
    client and closes it in :meth:`RemotePool.aclose`.
    """

    if client is None:
        resolved = _resolve_address(address)
        client = LocalSchedulerClient(resolved, resource_types=dict(resource_types or {}))
        owns_client = True
        resource_types = None
    else:
        owns_client = False
    return RemotePool(
        client,
        pool,
        resource_types=resource_types,
        owns_client=owns_client,
    )
