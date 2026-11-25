"""Thread-facing proxy objects shared by the synchronous facade.

:class:`SyncLease`, :class:`SyncBatchLease`, :class:`SyncResourceIterator`,
:class:`SyncSubscription` and the internal callback / loop proxies translate
loop-owned objects for callers on other threads.  They only call back into the
owning :class:`~purser.sync.SyncPurser` and never import the pool wrapper or the
facade at runtime.
"""

from __future__ import annotations

import contextlib
import copy
import inspect
import logging
import threading
from collections.abc import AsyncGenerator, Callable, Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, TypeVar

from purser.core.resource import Lease, Resource, UsageOutcome
from purser.facade import coerce_usage_outcome

if TYPE_CHECKING:
    from purser.sync import SyncPurser
    from purser.sync_pool import SyncPool

logger = logging.getLogger("purser.sync")

T = TypeVar("T")


class SyncLease:
    """Thread-safe view of an authoritative :class:`~purser.core.resource.Lease`.

    ``token`` and the other identity fields are read-only.  ``used`` and
    ``outcome`` are written through to the kernel object on the loop thread;
    ``expires_at`` / ``heartbeat_at`` reflect heartbeat renewals instead of a
    stale copy.  Resource and payload are snapshots taken at acquire time.
    """

    __slots__ = ("_owner", "_sync_pool", "_lease", "_payload", "_resource", "_released")

    _owner: SyncPurser
    _sync_pool: SyncPool
    _lease: Lease
    _payload: dict[str, Any] | None
    _resource: Resource | None
    _released: bool

    def __init__(
        self,
        owner: SyncPurser,
        sync_pool: SyncPool,
        lease: Lease,
        payload: dict[str, Any] | None,
        resource: Resource | None,
    ) -> None:
        object.__setattr__(self, "_owner", owner)
        object.__setattr__(self, "_sync_pool", sync_pool)
        object.__setattr__(self, "_lease", lease)
        object.__setattr__(self, "_payload", payload)
        object.__setattr__(self, "_resource", resource)
        object.__setattr__(self, "_released", False)

    # -- live authoritative fields ---------------------------------------
    @property
    def token(self) -> str:
        return self._lease.token

    @property
    def pool(self) -> str:
        return self._lease.pool

    @property
    def resource_id(self) -> str:
        return self._lease.resource_id

    @property
    def consumer(self) -> str | None:
        return self._lease.consumer

    @property
    def slots(self) -> int:
        return self._lease.slots

    @property
    def started_at(self) -> float:
        return self._lease.started_at

    @property
    def expires_at(self) -> float:
        return self._lease.expires_at

    @property
    def heartbeat_at(self) -> float:
        return self._lease.heartbeat_at

    @property
    def priority(self) -> int:
        return self._lease.priority

    @property
    def use_limit(self) -> int | None:
        return self._lease.use_limit

    @property
    def used(self) -> int:
        return self._lease.used

    @used.setter
    def used(self, value: int) -> None:
        self._write("used", value)

    @property
    def trace_id(self) -> str | None:
        return self._lease.trace_id

    @property
    def outcome(self) -> UsageOutcome:
        return self._lease.outcome

    @outcome.setter
    def outcome(self, value: UsageOutcome | str) -> None:
        self._write("outcome", value)

    @property
    def target(self) -> str | None:
        return self._lease.target

    @target.setter
    def target(self, value: str | None) -> None:
        self._write("target", value)

    # -- snapshots --------------------------------------------------------
    @property
    def payload(self) -> dict[str, Any] | None:
        return self._payload

    @payload.setter
    def payload(self, value: dict[str, Any] | None) -> None:
        if self._released:
            raise RuntimeError("lease has already been released")
        self._payload = copy.deepcopy(value)

    @property
    def resource(self) -> Resource | None:
        return self._resource

    # -- helpers ----------------------------------------------------------
    @property
    def released(self) -> bool:
        return self._released

    def expired(self, now: float | None = None) -> bool:
        return self._lease.expired(now)

    def _write(self, name: str, value: Any) -> None:
        if self._released:
            raise RuntimeError("lease has already been released")
        self._owner._call_sync(lambda: setattr(self._lease, name, value))

    def _mark_released(self) -> None:
        object.__setattr__(self, "_released", True)

    def __repr__(self) -> str:
        return (
            f"SyncLease(token={self.token[:8]!r}, pool={self.pool!r}, "
            f"resource_id={self.resource_id!r}, slots={self.slots})"
        )


class SyncBatchLease:
    """Sync counterpart of ``BatchLease``; release does not need the raw pool."""

    def __init__(
        self,
        owner: SyncPurser,
        pool: SyncPool,
        leases: list[SyncLease],
    ) -> None:
        self._owner = owner
        self._pool = pool
        self._leases = leases

    @property
    def leases(self) -> list[SyncLease]:
        return list(self._leases)

    def __len__(self) -> int:
        return len(self._leases)

    def __iter__(self) -> Iterator[SyncLease]:
        return iter(self._leases)

    def release_all(self, outcome: UsageOutcome | str = UsageOutcome.OK) -> None:
        outcome = coerce_usage_outcome(outcome) or UsageOutcome.OK
        first_error: BaseException | None = None
        remaining: list[SyncLease] = []
        for lease in self._leases:
            try:
                self._pool.release(lease, outcome=outcome)
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
                remaining.append(lease)
        self._leases = remaining
        if first_error is not None:
            raise first_error

    def __enter__(self) -> SyncBatchLease:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.release_all()


class SyncResourceIterator:
    """Lazy synchronous iterator over ``ResourcePool.iter_resources``."""

    def __init__(self, owner: SyncPurser, agen: AsyncGenerator[Resource, None]) -> None:
        self._owner = owner
        self._agen = agen
        self._lock = threading.Lock()
        self._closed = False

    def __iter__(self) -> SyncResourceIterator:
        return self

    def __next__(self) -> Resource:
        with self._lock:
            if self._closed:
                raise StopIteration

            async def step() -> Resource:
                try:
                    resource = await self._agen.__anext__()
                except StopAsyncIteration:
                    raise
                return self._owner._snapshot(resource)

            try:
                return self._owner._submit(step)
            except StopAsyncIteration:
                self._closed = True
                raise StopIteration from None

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            if self._owner.closed:
                return
            self._owner._submit(self._agen.aclose)

    def __enter__(self) -> SyncResourceIterator:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def __del__(self) -> None:
        if not self._closed:
            # The owner may already be gone during interpreter shutdown.
            with contextlib.suppress(Exception):
                self.close()


class SyncSubscription:
    """Unsubscribe handle returned by :meth:`SyncPurser.subscribe`."""

    def __init__(self, owner: SyncPurser, unsubscribe: Callable[[], None]) -> None:
        self._owner = owner
        self._unsubscribe = unsubscribe
        self._closed = False
        self._lock = threading.Lock()

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
        if not self._owner.closed:
            with contextlib.suppress(Exception):
                self._owner._call_sync(self._unsubscribe)

    def __enter__(self) -> SyncSubscription:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


class _CallbackContext:
    def __init__(self) -> None:
        self._local = threading.local()

    @property
    def active(self) -> bool:
        return bool(getattr(self._local, "active", False))

    @contextmanager
    def mark(self) -> Iterator[None]:
        previous = getattr(self._local, "active", False)
        self._local.active = True
        try:
            yield
        finally:
            self._local.active = previous


class _LoopProxy:
    """Forward attribute access on a loop-owned public object."""

    def __init__(
        self,
        owner: SyncPurser,
        target: Any,
        *,
        copy_results: bool = True,
        copy_inputs: bool = True,
    ) -> None:
        self._owner = owner
        self._target = target
        self._copy_results = copy_results
        self._copy_inputs = copy_inputs

    def _result(self, value: Any) -> Any:
        return self._owner._snapshot(value) if self._copy_results else value

    def _isolate(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> tuple[Any, Any]:
        if not self._copy_inputs:
            return args, kwargs
        return copy.deepcopy(args), copy.deepcopy(kwargs)

    def __getattr__(self, name: str) -> Any:
        member = self._owner._call_sync(lambda: getattr(self._target, name))
        if not callable(member):
            return self._owner._call_sync(lambda: self._result(member))
        if inspect.iscoroutinefunction(member):

            def async_method(*args: Any, **kwargs: Any) -> Any:
                call_args, call_kwargs = self._isolate(args, kwargs)

                def factory() -> Any:
                    return member(*call_args, **call_kwargs)

                async def run() -> Any:
                    return self._result(await factory())

                return self._owner._submit(run)

            return async_method

        def sync_method(*args: Any, **kwargs: Any) -> Any:
            call_args, call_kwargs = self._isolate(args, kwargs)
            return self._owner._call_sync(
                lambda: self._result(member(*call_args, **call_kwargs))
            )

        return sync_method

    def __repr__(self) -> str:
        return f"<SyncProxy {type(self._target).__name__}>"
