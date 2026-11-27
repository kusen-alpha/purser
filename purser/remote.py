"""Asynchronous facade over the local scheduler IPC.

``connect()`` turns a :class:`~purser.local.protocol.SchedulerAddress` plus a
pool name into an object shaped like the embedded
:class:`~purser.facade.ResourcePool` data plane plus the read-only catalog
surface (``query``/``counts``/``get``/``require``/``batches``/``leases``).
Every blocking IPC call runs in a worker thread, so the calling event loop is
never stalled.

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
from purser.core.state import ResourceState
from purser.errors import ConfigError, ResourceNotFound
from purser.local.client import (
    LocalSchedulerClient,
    RemoteLease,
    _resource_from_dict,
    resolve_address,
)
from purser.local.protocol import SchedulerAddress

__all__ = ["RemotePool", "connect"]


class _RemotePoolAdmin:
    """Operator namespace over the scheduler's admin RPC surface."""

    def __init__(self, pool: RemotePool) -> None:
        self._pool = pool

    async def ban(
        self, rid: str, *, reason: str, operator: str = "system"
    ) -> None:
        """Ban one resource after a hard failure or operator decision."""

        await asyncio.to_thread(
            self._pool._client.pool_call,
            self._pool.name,
            "ban",
            rid,
            reason=reason,
            operator=operator,
        )

    async def unban(
        self,
        rid: str,
        *,
        reason: str | None = None,
        operator: str = "system",
        **kwargs: Any,
    ) -> Any:
        """Lift a ban; extra keyword arguments are forwarded to the scheduler."""

        if reason is not None:
            kwargs["reason"] = reason
        return await asyncio.to_thread(
            self._pool._client.pool_call,
            self._pool.name,
            "unban",
            rid,
            operator=operator,
            **kwargs,
        )


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
        kind: str | None = None,
        profile: str | None = None,
    ) -> None:
        if not name:
            raise ConfigError("RemotePool name cannot be empty")
        self._client = client
        self.name = name
        self._owns_client = owns_client
        self._closed = False
        self._kind = kind
        self._profile = profile
        self.admin = _RemotePoolAdmin(self)
        for kind_name, cls in (resource_types or {}).items():
            client.register_resource_type(kind_name, cls)

    # -- identity ---------------------------------------------------------
    @property
    def address(self) -> SchedulerAddress:
        return self._client.address

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def kind(self) -> str | None:
        """Resource kind reported by the scheduler (``None`` until known)."""

        return self._kind

    @property
    def profile(self) -> str | None:
        """Profile the pool was created from, when the scheduler reports one."""

        return self._profile

    async def _call(self, method: str, *args: Any, **kwargs: Any) -> Any:
        return await asyncio.to_thread(
            self._client.pool_call, self.name, method, *args, **kwargs
        )

    def _resource(self, data: Any) -> Resource | None:
        if not isinstance(data, dict):
            return None
        cls = self._client._resource_types.get(str(data.get("kind", "")))
        return _resource_from_dict(data, cls)

    # -- data plane -------------------------------------------------------
    async def acquire(
        self,
        req: AcquireRequest | dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> RemoteLease:
        """Acquire one lease owned by the scheduler process."""

        return await asyncio.to_thread(self._client.acquire, self.name, req, **kwargs)

    async def release(
        self,
        lease: RemoteLease | str,
        *,
        outcome: UsageOutcome | str | None = None,
        used: int | None = None,
    ) -> bool:
        """Return a lease and optionally report its outcome and usage."""

        return await asyncio.to_thread(
            self._client.release,
            self.name,
            lease,
            outcome=outcome,
            used=used,
        )

    async def renew(
        self, lease: RemoteLease | str, *, used: int | None = None
    ) -> float:
        """Extend one lease and return its new ``expires_at``."""

        return await asyncio.to_thread(
            self._client.renew, self.name, lease, used=used
        )

    async def stats(self) -> dict[str, Any]:
        """Totals, state counts, allocatable slots and health summary."""

        return await asyncio.to_thread(self._client.stats, self.name)

    async def refresh_info(self) -> dict[str, Any]:
        """Refresh ``kind``/``profile`` from one scheduler round-trip."""

        stats = await self.stats()
        self._kind = stats.get("kind") or self._kind
        self._profile = stats.get("profile")
        return stats

    # -- read-only plane --------------------------------------------------
    async def get(self, rid: str) -> Resource | None:
        """Return one resource snapshot, or ``None`` when it does not exist."""

        return self._resource(await self._call("get", rid))

    async def require(self, rid: str) -> Resource:
        """Return one resource snapshot or raise :class:`ResourceNotFound`."""

        resource = self._resource(await self._call("get", rid))
        if resource is None:
            raise ResourceNotFound(f"{self.name}:{rid}")
        return resource

    async def query(self, **kwargs: Any) -> list[Resource]:
        """List resources matching ``states``/``tags``/``search`` filters."""

        data = await self._call("query", **kwargs)
        return [
            resource
            for resource in (self._resource(item) for item in data or [])
            if resource is not None
        ]

    async def count_matching(self, **kwargs: Any) -> int:
        """Count resources matching the same filters as :meth:`query`."""

        return int(await self._call("count_matching", **kwargs))

    async def counts(self, *, by_name: bool = False) -> dict[Any, int]:
        """State histogram (or per-resource counts when ``by_name``)."""

        data = await self._call("counts", by_name=by_name)
        if by_name:
            return {str(key): int(value) for key, value in (data or {}).items()}
        return {ResourceState(str(key)): int(value) for key, value in (data or {}).items()}

    async def batches(self) -> list[dict[str, Any]]:
        """Import batches recorded by the scheduler."""

        return list(await self._call("batches") or [])

    async def leases(self, rid: str | None = None) -> list[RemoteLease]:
        """Live leases known to the scheduler, optionally for one resource."""

        data = await self._call("leases", rid)
        return [
            RemoteLease(
                self._client,
                item,
                self._client._resource_types.get(
                    str((item.get("resource") or {}).get("kind", ""))
                ),
            )
            for item in data or []
        ]

    async def describe(self, resource: Resource | dict[str, Any], *, secret: bool = False) -> dict[str, Any]:
        """Render a resource (snapshot or live object) as a public dict."""

        payload = resource if isinstance(resource, dict) else resource.to_dict()
        return dict(await self._call("describe", payload, secret=secret))

    async def import_resources(self, records: Any, **kwargs: Any) -> Any:
        """Bulk import dict specs or Resources with dedup and optional validation."""

        return await asyncio.to_thread(
            self._client.import_resources,
            self.name,
            records,
            **kwargs,
        )

    async def generate(self, specs: Any, **kwargs: Any) -> Any:
        """Build resources from adapter specs and import them with validation."""

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
        """Drop this client view; the scheduler process keeps the pool alive."""

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
        resolved = resolve_address(address)
        client = LocalSchedulerClient(resolved, resource_types=dict(resource_types or {}))
        owns_client = True
        resource_types = None
    else:
        owns_client = False
    remote = RemotePool(
        client,
        pool,
        resource_types=resource_types,
        owns_client=owns_client,
    )
    try:
        # One round-trip validates that the pool exists and fills kind/profile.
        stats = client.stats(remote.name)
    except BaseException:
        if owns_client:
            client.close()
        raise
    remote._kind = stats.get("kind")
    remote._profile = stats.get("profile")
    return remote
