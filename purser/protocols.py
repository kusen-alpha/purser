"""Typed structural interfaces shared by purser's runtime facades.

``Purser`` (embedded, async), :class:`~purser.thread.ThreadPurser` (one kernel
in a dedicated loop thread) and :class:`~purser.process.ProcessPurser` (one
kernel in a scheduler process) expose the same management surface.
:class:`PoolProtocol` is the data plane shared by the async pools;
:class:`SyncPoolProtocol` is its blocking twin.

The protocols are the single declaration of that contract: if a facade drifts,
``tests/test_facade_contract.py`` fails before users notice.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from typing import Any, Protocol, TypeVar, overload, runtime_checkable

from purser.adapters.base import ResourceAdapter
from purser.config import PoolConfig
from purser.core.resource import (
    AcquireRequest,
    LeaseView,
    Resource,
    UsageOutcome,
)
from purser.core.views import PoolStats
from purser.results import ImportReport

__all__ = [
    "MISSING",
    "PoolProtocol",
    "PurserProtocol",
    "SyncPoolProtocol",
]

_T = TypeVar("_T")


class _Missing:
    """Type of :data:`MISSING`; never instantiated outside this module."""

    __slots__ = ()


MISSING: Any = _Missing()
"""Sentinel for an omitted ``get_pool(..., default=...)`` argument."""


@runtime_checkable
class PoolProtocol(Protocol):
    """Data plane shared by every runtime pool facade."""

    name: str

    async def acquire(
        self, req: AcquireRequest | None = None, **kwargs: Any
    ) -> LeaseView: ...

    def lease(
        self, req: AcquireRequest | None = None, **kwargs: Any
    ) -> AbstractAsyncContextManager[LeaseView]: ...

    async def release(
        self,
        lease: LeaseView,
        *,
        outcome: UsageOutcome | str | None = None,
        used: int | None = None,
    ) -> bool: ...

    async def renew(
        self, lease: LeaseView, *, used: int | None = None
    ) -> float: ...

    async def stats(self) -> PoolStats: ...

    async def import_resources(
        self,
        records: Sequence[dict[str, Any] | Resource],
        **kwargs: Any,
    ) -> ImportReport: ...

    async def generate(
        self,
        specs: Sequence[dict[str, Any] | Resource],
        **kwargs: Any,
    ) -> ImportReport: ...


@runtime_checkable
class SyncPoolProtocol(Protocol):
    """Blocking twin of :class:`PoolProtocol` (``SyncPurser`` pools)."""

    name: str

    def acquire(
        self, req: AcquireRequest | None = None, **kwargs: Any
    ) -> LeaseView: ...

    def lease(
        self, req: AcquireRequest | None = None, **kwargs: Any
    ) -> AbstractContextManager[LeaseView]: ...

    def release(
        self,
        lease: LeaseView,
        *,
        outcome: UsageOutcome | str | None = None,
        used: int | None = None,
    ) -> bool: ...

    def renew(self, lease: LeaseView, *, used: int | None = None) -> float: ...

    def stats(self) -> PoolStats: ...

    def import_resources(
        self,
        records: Sequence[dict[str, Any] | Resource],
        **kwargs: Any,
    ) -> ImportReport: ...

    def generate(
        self,
        specs: Sequence[dict[str, Any] | Resource],
        **kwargs: Any,
    ) -> ImportReport: ...


@runtime_checkable
class PurserProtocol(Protocol):
    """Management surface shared by the async and thread/process facades."""

    adapters: Any

    @property
    def pools(self) -> Mapping[str, PoolProtocol]: ...

    @property
    def started(self) -> bool: ...

    def register_adapter(self, adapter: ResourceAdapter) -> None: ...

    def create_pool(
        self,
        name: str,
        kind: str,
        *,
        config: PoolConfig | Mapping[str, Any] | None = None,
        profile: str | Mapping[str, Any] | None = None,
        **overrides: Any,
    ) -> PoolProtocol: ...

    def ensure_pool(
        self,
        name: str,
        kind: str,
        *,
        config: PoolConfig | Mapping[str, Any] | None = None,
        profile: str | Mapping[str, Any] | None = None,
        **overrides: Any,
    ) -> PoolProtocol: ...

    @overload
    def get_pool(self, name: str, *, default: _T) -> PoolProtocol | _T: ...

    @overload
    def get_pool(self, name: str, *, default: Any = MISSING) -> PoolProtocol: ...

    def get_pool_names(self) -> list[str]: ...

    async def delete_pool(
        self, name: str, *, force: bool = False, operator: str = "system"
    ) -> dict[str, Any]: ...

    async def start(self) -> None: ...

    async def close(self) -> None: ...
