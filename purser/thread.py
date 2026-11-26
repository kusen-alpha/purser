"""Async facade over the thread-owned kernel (:class:`~purser.sync.SyncPurser`).

``ThreadPurser`` keeps the calling convention of the other facades — synchronous
management calls and an asynchronous pool data plane — while the kernel itself
runs in ``SyncPurser``'s dedicated event-loop thread.  Multiple caller threads
and event loops can share one ``ThreadPurser``; every call is marshalled to the
kernel thread, so no loop-bound object leaks out.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import Any, TypeVar, overload

from purser.errors import ConfigError, IsolationConflict
from purser.isolation import PICKLE_MESSAGE
from purser.protocols import MISSING
from purser.sync import SyncLease, SyncPool, SyncPurser

_T = TypeVar("_T")

__all__ = ["ThreadPool", "ThreadPurser"]


class _ThreadPoolAdmin:
    """Operator namespace over the loop-owned ``SyncPool.admin``."""

    def __init__(self, pool: ThreadPool) -> None:
        self._pool = pool

    async def ban(self, resource_id: str, *, reason: str, operator: str = "system") -> None:
        """Ban one resource after a hard failure or operator decision."""

        await asyncio.to_thread(
            self._pool._pool.admin.ban, resource_id, reason=reason, operator=operator
        )

    async def unban(
        self,
        resource_id: str,
        *,
        reason: str | None = None,
        operator: str = "system",
        **kwargs: Any,
    ) -> Any:
        """Lift a ban; ``reason`` is only forwarded when supplied."""

        if reason is not None:
            kwargs["reason"] = reason
        return await asyncio.to_thread(
            self._pool._pool.admin.unban,
            resource_id,
            operator=operator,
            **kwargs,
        )


class ThreadPool:
    """Async, awaitable view of a :class:`~purser.sync.SyncPool`."""

    def __init__(self, pool: SyncPool) -> None:
        self._pool = pool
        self.admin = _ThreadPoolAdmin(self)

    def __getstate__(self) -> Any:
        raise IsolationConflict(PICKLE_MESSAGE)

    @property
    def name(self) -> str:
        """Pool name as registered with the owning ``SyncPurser``."""

        return str(self._pool.name)

    @property
    def kind(self) -> str:
        """Adapter/resource kind this pool stores."""

        return str(self._pool.kind)

    @property
    def profile(self) -> str | None:
        """Optional profile name shared by this pool's resources."""

        value = self._pool.profile
        return None if value is None else str(value)

    async def import_resources(self, records: Any, **kwargs: Any) -> Any:
        """Bulk import dict specs or Resources with dedup and optional validation."""

        return await asyncio.to_thread(self._pool.import_resources, records, **kwargs)

    async def generate(self, specs: Any, **kwargs: Any) -> Any:
        """Build resources from adapter specs and import them with validation."""

        return await asyncio.to_thread(self._pool.generate, specs, **kwargs)

    async def stats(self) -> Any:
        """Totals, state counts, allocatable slots and health summary."""

        return await asyncio.to_thread(self._pool.stats)

    async def acquire(self, req: Any = None, **kwargs: Any) -> SyncLease:
        """Acquire one lease; the call blocks a worker thread, not the loop."""

        return await asyncio.to_thread(self._pool.acquire, req, **kwargs)

    async def release(
        self,
        lease: SyncLease,
        *,
        outcome: Any = None,
        used: int | None = None,
    ) -> bool:
        """Return a lease and optionally report its outcome and usage."""

        return await asyncio.to_thread(
            self._pool.release, lease, outcome=outcome, used=used
        )

    async def renew(self, lease: SyncLease, *, used: int | None = None) -> float:
        """Extend one lease and return its new ``expires_at``."""

        return await asyncio.to_thread(self._pool.renew, lease, used=used)

    @asynccontextmanager
    async def lease(self, req: Any = None, **kwargs: Any) -> AsyncIterator[SyncLease]:
        """Acquire now and always release at block exit."""

        lease = await self.acquire(req, **kwargs)
        try:
            yield lease
        finally:
            await self.release(lease, outcome=lease.outcome, used=lease.used)

    def __repr__(self) -> str:
        return f"ThreadPool({self.name!r}, kind={self.kind!r})"


class ThreadPurser:
    """Purser-shaped facade backed by one ``SyncPurser`` loop thread.

    The kernel is owned by default.  Pass ``sync=`` to attach to an existing
    ``SyncPurser``; in that case ``close()`` leaves the kernel running.
    """

    def __init__(self, *, sync: SyncPurser | None = None, **kwargs: Any) -> None:
        if sync is not None and kwargs:
            raise ConfigError("pass either sync= or SyncPurser kwargs, not both")
        self._sync = sync if sync is not None else SyncPurser(**kwargs)
        self._owned = sync is None
        self._closed = False

    def __getstate__(self) -> Any:
        raise IsolationConflict(PICKLE_MESSAGE)

    @property
    def sync(self) -> SyncPurser:
        return self._sync

    @property
    def owned(self) -> bool:
        return self._owned

    @property
    def started(self) -> bool:
        return bool(self._sync.started)

    @property
    def closed(self) -> bool:
        return self._closed or bool(self._sync.closed)

    def register_adapter(self, adapter: Any) -> None:
        self._sync.register_adapter(adapter)

    def create_pool(
        self,
        name: str,
        kind: str,
        *,
        config: Any = None,
        profile: Any = None,
        **overrides: Any,
    ) -> ThreadPool:
        return ThreadPool(
            self._sync.create_pool(
                name, kind, config=config, profile=profile, **overrides
            )
        )

    def ensure_pool(
        self,
        name: str,
        kind: str,
        *,
        config: Any = None,
        profile: Any = None,
        **overrides: Any,
    ) -> ThreadPool:
        """Return pool ``name``, creating it with ``kind`` when absent."""

        return ThreadPool(
            self._sync.ensure_pool(
                name, kind, config=config, profile=profile, **overrides
            )
        )

    @overload
    def get_pool(self, name: str, *, default: _T) -> ThreadPool | _T: ...

    @overload
    def get_pool(self, name: str, *, default: Any = MISSING) -> ThreadPool: ...

    def get_pool(self, name: str, *, default: Any = MISSING) -> Any:
        """Return an existing pool, or ``default`` / raise ``PoolNotFound``."""

        result = self._sync.get_pool(name, default=default)
        if not isinstance(result, SyncPool):
            return result
        return ThreadPool(result)

    @property
    def pools(self) -> Mapping[str, ThreadPool]:
        """Snapshot of locally registered pools; later changes are not reflected."""

        return {name: ThreadPool(pool) for name, pool in self._sync.pools.items()}

    def get_pool_names(self) -> list[str]:
        return [str(name) for name in self._sync.get_pool_names()]

    async def delete_pool(
        self, name: str, *, force: bool = False, operator: str = "system"
    ) -> dict[str, Any]:
        return await asyncio.to_thread(
            self._sync.delete_pool, name, force=force, operator=operator
        )

    async def start(self) -> None:
        if self.closed:
            raise RuntimeError("ThreadPurser is closed")
        await asyncio.to_thread(self._sync.start)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._owned:
            await asyncio.to_thread(self._sync.close)

    async def __aenter__(self) -> ThreadPurser:
        await self.start()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    def __repr__(self) -> str:
        return f"ThreadPurser(owned={self._owned}, started={self.started})"
