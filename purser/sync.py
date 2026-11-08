"""Synchronous facade for the asyncio-first purser kernel.

A :class:`SyncPurser` owns one dedicated event-loop thread.  The kernel and
every pool are created and used on that loop.  Calls from other threads are
submitted to the loop and wait for their result; they never touch loop-bound
objects directly.

The facade deliberately keeps the same public method names as the async API.
Lease objects are returned as :class:`SyncLease` proxies so heartbeat and
``used`` updates remain visible across the thread boundary.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import copy
import inspect
import logging
import threading
import time
from collections.abc import (
    AsyncGenerator,
    Awaitable,
    Callable,
    Coroutine,
    Iterator,
)
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import Any, TypeVar, cast

from purser.core.resource import Lease, Resource, UsageOutcome
from purser.core.views import PoolStats, UsageReport
from purser.errors import IsolationConflict
from purser.facade import ImportReport, Purser, ResourcePool
from purser.isolation import PICKLE_MESSAGE
from purser.types.base import ResourceType

logger = logging.getLogger("purser.sync")

T = TypeVar("T")

_TERMINAL = (str, bytes, int, float, bool, type(None))


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
    def outcome(self, value: UsageOutcome) -> None:
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

    def release_all(self, outcome: UsageOutcome = UsageOutcome.OK) -> None:
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


class SyncPool:
    """Synchronous wrapper around a loop-owned :class:`ResourcePool`."""

    def __init__(self, owner: SyncPurser, pool: ResourcePool) -> None:
        self._owner = owner
        self._pool = pool

    def __getstate__(self) -> Any:
        raise IsolationConflict(PICKLE_MESSAGE)

    # -- identity ---------------------------------------------------------
    @property
    def name(self) -> str:
        return self._pool.name

    @property
    def kind(self) -> str:
        return self._pool.cfg.kind

    @property
    def config(self) -> Any:
        return self._owner._call_sync(lambda: self._owner._snapshot(self._pool.cfg))

    @property
    def started(self) -> bool:
        if self._owner.closed:
            return False
        return self._owner._call_sync(lambda: self._pool.started)

    def __repr__(self) -> str:
        return f"SyncPool({self.name!r}, kind={self.kind!r})"

    # -- helpers ----------------------------------------------------------
    def _snapshot_call(
        self,
        factory: Callable[[], Awaitable[T]],
        timeout: float | None = None,
        *,
        allow_closing: bool = False,
    ) -> T:
        owner = self._owner

        async def run() -> T:
            return owner._snapshot(await factory())

        return owner._submit(run, timeout=timeout, allow_closing=allow_closing)

    def _sync_snapshot(self, factory: Callable[[], T]) -> T:
        owner = self._owner
        return owner._call_sync(lambda: owner._snapshot(factory()))

    def _proxy_for(self, lease: Lease) -> SyncLease:
        owner = self._owner
        return SyncLease(
            owner,
            self,
            lease,
            owner._snapshot(lease.payload),
            owner._snapshot(lease.resource),
        )

    def _acquire_wrapped(self, factory: Callable[[], Any], timeout: float | None = None) -> SyncLease:
        owner = self._owner
        pool = self._pool

        async def run() -> SyncLease:
            lease = await factory()
            try:
                return self._proxy_for(lease)
            except BaseException:
                try:
                    await pool.release(lease, outcome=lease.outcome, used=lease.used)
                except BaseException:
                    logger.exception("failed to compensate undeliverable lease %s", lease.token)
                raise

        return owner._submit(run, timeout=timeout)

    def _resolve(self, lease: SyncLease | Lease) -> Lease:
        if isinstance(lease, SyncLease):
            if lease._owner is not self._owner or lease._sync_pool is not self:
                raise ValueError("lease belongs to a different SyncPool")
            return lease._lease
        if isinstance(lease, Lease):
            token = lease.token
            return self._owner._call_sync(
                lambda: self._pool._leases.get(token) or lease
            )
        raise TypeError("release requires a SyncLease or Lease")

    # -- lifecycle --------------------------------------------------------
    def start(self) -> None:
        self._owner._submit(self._pool.start)

    def close(self) -> None:
        self._owner._submit(self._pool.close)
        self._owner._forget_pool(self)

    def reconfigure(self, **updates: Any) -> Any:
        copied = {key: copy.deepcopy(value) for key, value in updates.items()}
        return self._sync_snapshot(lambda: self._pool.reconfigure(**copied))

    # -- validation jobs / describe --------------------------------------
    def validate_jobs(self) -> Any:
        return self._sync_snapshot(self._pool.validate_jobs)

    def validate_job(self, job_id: str) -> Any:
        return self._sync_snapshot(lambda: self._pool.validate_job(job_id))

    def cancel_validate_job(self, job_id: str) -> Any:
        return self._sync_snapshot(lambda: self._pool.cancel_validate_job(job_id))

    def describe(self, resource: Resource, *, secret: bool = False) -> dict[str, Any]:
        copied = copy.deepcopy(resource)
        return self._sync_snapshot(
            lambda: self._pool.describe(copied, secret=secret)
        )

    # -- reads ------------------------------------------------------------
    def get(self, rid: str) -> Resource | None:
        return self._snapshot_call(lambda: self._pool.get(rid))

    def require(self, rid: str) -> Resource:
        return self._snapshot_call(lambda: self._pool.require(rid))

    def query(self, **kwargs: Any) -> list[Resource]:
        copied = copy.deepcopy(kwargs)
        return self._snapshot_call(lambda: self._pool.query(**copied))

    def iter_resources(self, **kwargs: Any) -> SyncResourceIterator:
        copied = copy.deepcopy(kwargs)
        agen = self._owner._call_sync(
            lambda: self._pool.iter_resources(**copied)
        )
        return SyncResourceIterator(self._owner, agen)

    def count_matching(self, **kwargs: Any) -> int:
        copied = copy.deepcopy(kwargs)
        return self._snapshot_call(lambda: self._pool.count_matching(**copied))

    def batches(self) -> list[dict[str, Any]]:
        return self._snapshot_call(self._pool.batches)

    def counts(self, *, by_name: bool = False) -> dict[Any, int]:
        if by_name:
            return self._snapshot_call(lambda: self._pool.counts(by_name=True))
        return self._snapshot_call(lambda: self._pool.counts())

    def stats(self) -> PoolStats:
        return self._snapshot_call(self._pool.stats)

    def leases(self, resource_id: str | None = None) -> list[SyncLease]:
        owner = self._owner
        pool = self._pool

        async def run() -> list[SyncLease]:
            leases = await pool.leases(resource_id)
            return [self._proxy_for(lease) for lease in leases]

        return owner._submit(run)

    # -- import / generate / validation ----------------------------------
    def import_resources(
        self,
        records: Any,
        *,
        dedup_key: str = "id",
        source: str = "manual",
        operator: str = "system",
        fail_fast: bool = False,
        import_validate: str | None = None,
        batch_id: str | None = None,
    ) -> ImportReport:
        copied = copy.deepcopy(records)
        return self._snapshot_call(
            lambda: self._pool.import_resources(
                copied,
                dedup_key=dedup_key,
                source=source,
                operator=operator,
                fail_fast=fail_fast,
                import_validate=import_validate,
                batch_id=batch_id,
            )
        )

    def generate(
        self,
        specs: Any,
        *,
        source: str = "builtin",
        operator: str = "system",
    ) -> ImportReport:
        copied = copy.deepcopy(specs)
        return self._snapshot_call(
            lambda: self._pool.generate(copied, source=source, operator=operator)
        )

    def validate_now(self, rid: str, *, operator: str = "system") -> Any:
        return self._snapshot_call(
            lambda: self._pool.validate_now(rid, operator=operator)
        )

    # -- acquire / release ------------------------------------------------
    def acquire(self, req: Any = None, **kwargs: Any) -> SyncLease:
        copied = copy.deepcopy(req) if req is not None else None
        copied_kwargs = copy.deepcopy(kwargs)
        return self._acquire_wrapped(
            lambda: self._pool.acquire(copied, **copied_kwargs)
        )

    def acquire_many(
        self,
        n: int,
        *,
        req: Any = None,
        mode: str = "atomic",
        n_min: int | None = None,
        timeout: float | None = None,
    ) -> SyncBatchLease:
        copied = copy.deepcopy(req) if req is not None else None
        owner = self._owner
        pool = self._pool

        async def run() -> SyncBatchLease:
            batch = await pool.acquire_many(
                n, req=copied, mode=mode, n_min=n_min, timeout=timeout
            )
            try:
                leases = [self._proxy_for(lease) for lease in batch.leases]
            except BaseException:
                try:
                    await batch.release_all(pool)
                except BaseException:
                    logger.exception("failed to compensate undeliverable batch leases")
                raise
            return SyncBatchLease(owner, self, leases)

        return owner._submit(run)

    def release(
        self,
        lease: SyncLease | Lease,
        *,
        outcome: UsageOutcome | None = None,
        used: int | None = None,
    ) -> bool:
        authoritative = self._resolve(lease)
        result = self._owner._submit(
            lambda: self._pool.release(
                authoritative, outcome=outcome, used=used
            ),
            allow_closing=True,
        )
        if isinstance(lease, SyncLease):
            lease._mark_released()
        return result

    def release_consumer(
        self,
        consumer: str,
        *,
        outcome: UsageOutcome = UsageOutcome.OK,
        operator: str = "system",
    ) -> list[Lease]:
        return self._snapshot_call(
            lambda: self._pool.release_consumer(
                consumer, outcome=outcome, operator=operator
            ),
            allow_closing=True,
        )

    @contextmanager
    def lease(self, req: Any = None, **kwargs: Any) -> Iterator[SyncLease]:
        lease = self.acquire(req, **kwargs)
        try:
            yield lease
        finally:
            self.release(lease, outcome=lease.outcome, used=lease.used)

    def renew_manual(self, lease: SyncLease | Lease) -> bool:
        authoritative = self._resolve(lease)
        return self._owner._submit(
            lambda: self._pool.renew_manual(authoritative)
        )

    # -- state / resource management --------------------------------------
    def ban(self, rid: str, *, reason: str, operator: str = "system") -> None:
        self._owner._submit(
            lambda: self._pool.ban(rid, reason=reason, operator=operator)
        )

    def unban(
        self,
        rid: str,
        *,
        operator: str = "system",
        reason: str = "manual unban",
        revalidate: bool = True,
    ) -> None:
        self._owner._submit(
            lambda: self._pool.unban(
                rid, operator=operator, reason=reason, revalidate=revalidate
            )
        )

    def suspend(
        self, rid: str, *, operator: str = "system", reason: str
    ) -> None:
        self._owner._submit(
            lambda: self._pool.suspend(rid, operator=operator, reason=reason)
        )

    def resume(
        self,
        rid: str,
        *,
        operator: str = "system",
        reason: str = "manual resume",
        revalidate: bool = False,
    ) -> None:
        self._owner._submit(
            lambda: self._pool.resume(
                rid, operator=operator, reason=reason, revalidate=revalidate
            )
        )

    def top_up(
        self,
        rid: str,
        *,
        operator: str = "system",
        amount: int | None = None,
        reason: str = "manual top_up",
    ) -> None:
        self._owner._submit(
            lambda: self._pool.top_up(
                rid, operator=operator, amount=amount, reason=reason
            )
        )

    def drain(self, rid: str, *, operator: str = "system", reason: str) -> None:
        self._owner._submit(
            lambda: self._pool.drain(rid, operator=operator, reason=reason)
        )

    def cancel_drain(
        self,
        rid: str,
        *,
        operator: str = "system",
        reason: str = "cancel drain",
    ) -> None:
        self._owner._submit(
            lambda: self._pool.cancel_drain(
                rid, operator=operator, reason=reason
            )
        )

    def restore(
        self, rid: str, *, operator: str = "system", reason: str = "manual restore"
    ) -> None:
        self._owner._submit(
            lambda: self._pool.restore(rid, operator=operator, reason=reason)
        )

    def admin_set_state(
        self,
        rid: str,
        *,
        target: Any,
        operator: str = "system",
        reason: str,
    ) -> None:
        self._owner._submit(
            lambda: self._pool.admin_set_state(
                rid, target=target, operator=operator, reason=reason
            )
        )

    def force_revoke(
        self,
        rid: str,
        *,
        operator: str = "system",
        reason: str,
        token: str | None = None,
    ) -> Lease:
        return self._snapshot_call(
            lambda: self._pool.force_revoke(
                rid, operator=operator, reason=reason, token=token
            )
        )

    def update_resource(
        self,
        rid: str,
        *,
        payload: dict[str, Any] | None = None,
        tags: Any = None,
        max_slots: int | None = None,
        operator: str = "system",
        reason: str,
        **optional: Any,
    ) -> Resource:
        kwargs: dict[str, Any] = {
            "payload": copy.deepcopy(payload),
            "tags": copy.deepcopy(tags),
            "max_slots": max_slots,
            "operator": operator,
            "reason": reason,
        }
        kwargs.update(copy.deepcopy(optional))
        return self._snapshot_call(
            lambda: self._pool.update_resource(rid, **kwargs)
        )

    def move_resource(
        self,
        rid: str,
        target_pool: SyncPool,
        *,
        operator: str = "system",
        reason: str,
    ) -> Resource:
        if not isinstance(target_pool, SyncPool):
            raise TypeError("target_pool must be a SyncPool")
        if target_pool._owner is not self._owner:
            raise ValueError("target_pool belongs to a different SyncPurser")
        return self._snapshot_call(
            lambda: self._pool.move_resource(
                rid, target_pool._pool, operator=operator, reason=reason
            )
        )

    def recall_batch(
        self,
        batch_id: str,
        *,
        action: str = "requalify",
        operator: str = "system",
        reason: str,
    ) -> dict[str, Any]:
        return self._snapshot_call(
            lambda: self._pool.recall_batch(
                batch_id, action=action, operator=operator, reason=reason
            )
        )

    # -- maintenance ------------------------------------------------------
    def run_maintenance(self, name: str) -> dict[str, int]:
        return self._snapshot_call(lambda: self._pool.run_maintenance(name))


class SyncPurser:
    """Owns a dedicated event loop thread and exposes a blocking API."""

    def __init__(
        self,
        *,
        startup_timeout: float | None = 30.0,
        close_timeout: float = 30.0,
        callback_workers: int = 4,
        **kwargs: Any,
    ) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread = threading.Thread(
            target=self._run, name="purser-sync-loop", daemon=True
        )
        self._ready = threading.Event()
        self._startup_error: BaseException | None = None
        self._loop_error: BaseException | None = None
        self._closing = False
        self._closed = False
        self._start_error: BaseException | None = None
        self._lifecycle_lock = threading.RLock()
        self._calls_cond = threading.Condition(threading.Lock())
        self._active_calls = 0
        self._pending: set[concurrent.futures.Future[Any]] = set()
        self._wrappers: dict[str, SyncPool] = {}
        self._wrappers_lock = threading.RLock()
        self._callback_ctx = _CallbackContext()
        self._startup_timeout = startup_timeout
        self._close_timeout = close_timeout
        self._callback_executor = ThreadPoolExecutor(
            max_workers=max(1, callback_workers),
            thread_name_prefix="purser-sync-callback",
        )
        self._owns_redis = bool(kwargs.get("redis_url")) and not kwargs.get("redis_client")

        self._thread.start()
        if not self._ready.wait(startup_timeout):
            self._shutdown_loop(1.0)
            self._callback_executor.shutdown(wait=False)
            raise TimeoutError("purser sync loop did not start")
        if self._startup_error is not None:
            self._callback_executor.shutdown(wait=False)
            raise self._startup_error
        try:
            self._ps = self._call_sync(lambda: Purser(**kwargs))
        except BaseException:
            self._shutdown_loop(1.0)
            self._callback_executor.shutdown(wait=False)
            raise

        self.audit = _LoopProxy(self, self._ps.audit)
        self.metrics = _LoopProxy(self, self._ps.metrics)
        self.idempotency = _LoopProxy(self, self._ps.idempotency)
        self.probe_guard = _LoopProxy(self, self._ps.probe_guard)
        self.usage = _LoopProxy(self, self._ps.usage)
        self.types = _LoopProxy(
            self,
            self._ps.types,
            copy_results=False,
            copy_inputs=False,
        )

    def __getstate__(self) -> Any:
        raise IsolationConflict(PICKLE_MESSAGE)

    # -- thread / bridge --------------------------------------------------
    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._ready.set()
        try:
            loop.run_forever()
        except BaseException as exc:
            self._loop_error = exc
            logger.exception("purser sync loop failed")
        finally:
            try:
                pending = [task for task in asyncio.all_tasks(loop) if not task.done()]
                for task in pending:
                    task.cancel()
                if pending:
                    loop.run_until_complete(
                        asyncio.gather(*pending, return_exceptions=True)
                    )
                loop.run_until_complete(loop.shutdown_asyncgens())
                shutdown = getattr(loop, "shutdown_default_executor", None)
                if shutdown is not None:
                    loop.run_until_complete(
                        asyncio.wait_for(shutdown(), timeout=2.0)
                    )
            except BaseException:
                logger.exception("purser sync loop cleanup failed")
            finally:
                # Wake callers whose callback/task never got a chance to run.
                self._fail_pending(
                    RuntimeError("purser sync loop stopped before completion")
                )
                loop.close()
                asyncio.set_event_loop(None)

    def _fail_pending(self, error: BaseException) -> None:
        with self._calls_cond:
            pending = list(self._pending)
            self._pending.clear()
        for future in pending:
            with contextlib.suppress(BaseException):
                future.set_exception(error)

    def _track(self, future: concurrent.futures.Future[Any]) -> None:
        with self._calls_cond:
            self._pending.add(future)

    def _untrack(self, future: concurrent.futures.Future[Any]) -> None:
        with self._calls_cond:
            self._pending.discard(future)

    @contextmanager
    def _call_scope(
        self,
        *,
        allow_closing: bool = False,
        allow_callback: bool = True,
    ) -> Iterator[None]:
        self._check_callable(
            allow_closing=allow_closing, allow_callback=allow_callback
        )
        with self._calls_cond:
            if self._closed or (self._closing and not allow_closing):
                raise RuntimeError("SyncPurser is closing or closed")
            self._active_calls += 1
        try:
            yield
        finally:
            with self._calls_cond:
                self._active_calls -= 1
                self._calls_cond.notify_all()

    def _shutdown_loop(self, timeout: float | None) -> None:
        loop = self._loop
        if loop is not None and loop.is_running() and not loop.is_closed():
            with contextlib.suppress(RuntimeError):
                loop.call_soon_threadsafe(loop.stop)
        self._thread.join(timeout)
        if self._thread.is_alive():
            raise TimeoutError("purser sync loop did not stop")

    def _check_callable(
        self,
        *,
        allow_closing: bool = False,
        allow_callback: bool = True,
    ) -> None:
        ps = getattr(self, "_ps", None)
        if ps is not None:
            ps._isolation.ensure_process()
        if self._closed or (self._closing and not allow_closing):
            raise RuntimeError("SyncPurser is closing or closed")
        if threading.current_thread() is self._thread:
            raise RuntimeError("SyncPurser cannot be called from its loop thread")
        if not allow_callback and self._callback_ctx.active:
            raise RuntimeError("operation is not allowed from a sync event callback")
        if self._loop_error is not None:
            raise RuntimeError("purser sync loop failed") from self._loop_error
        if self._loop is None or self._loop.is_closed() or not self._thread.is_alive():
            raise RuntimeError("purser sync loop is not running")

    def _call_sync(self, fn: Callable[[], T], timeout: float | None = None) -> T:
        with self._call_scope():
            future: concurrent.futures.Future[T] = concurrent.futures.Future()

            def runner() -> None:
                if not future.set_running_or_notify_cancel():
                    return
                try:
                    future.set_result(fn())
                except BaseException as exc:
                    future.set_exception(exc)

            assert self._loop is not None
            self._track(future)
            try:
                try:
                    self._loop.call_soon_threadsafe(runner)
                except RuntimeError as exc:
                    raise RuntimeError("purser sync loop is not running") from exc
                try:
                    return future.result(timeout)
                except concurrent.futures.TimeoutError:
                    future.cancel()
                    if future.done() and not future.cancelled():
                        return future.result()
                    raise TimeoutError("purser sync call timed out") from None
            finally:
                self._untrack(future)

    def _submit(
        self,
        factory: Callable[[], Awaitable[T]],
        timeout: float | None = None,
        *,
        allow_closing: bool = False,
    ) -> T:
        with self._call_scope(allow_closing=allow_closing):
            raw = factory()
            if not inspect.isawaitable(raw):
                raise TypeError("async facade factory must return an awaitable")
            coro = cast(Coroutine[Any, Any, T], raw)
            assert self._loop is not None
            try:
                future = asyncio.run_coroutine_threadsafe(coro, self._loop)
            except BaseException:
                with contextlib.suppress(Exception):
                    coro.close()
                raise
            self._track(future)
            try:
                try:
                    return future.result(timeout)
                except concurrent.futures.TimeoutError:
                    future.cancel()
                    if future.done() and not future.cancelled():
                        return future.result()
                    raise TimeoutError("purser sync call timed out") from None
                except concurrent.futures.CancelledError as exc:
                    raise RuntimeError("purser sync call cancelled") from exc
            finally:
                self._untrack(future)

    @staticmethod
    def _snapshot(value: T) -> T:
        if isinstance(value, _TERMINAL):
            return cast(T, value)
        return copy.deepcopy(value)

    # -- lifecycle --------------------------------------------------------
    def start(self, timeout: float | None = None) -> None:
        with self._lifecycle_lock:
            self._check_callable()
            if self._start_error is not None:
                raise RuntimeError("SyncPurser failed to start") from self._start_error
            if self._call_sync(lambda: self._ps.started):
                return
            limit = timeout if timeout is not None else self._startup_timeout
            try:
                self._submit(self._ps.start, timeout=limit)
            except BaseException as exc:
                self._start_error = exc
                raise

    def close(self, timeout: float | None = None) -> None:
        with self._lifecycle_lock:
            if self._closed:
                return
            self._check_callable(allow_closing=True, allow_callback=False)
            first_error: BaseException | None = None
            limit = timeout if timeout is not None else self._close_timeout
            with self._calls_cond:
                self._closing = True
                deadline = time.monotonic() + limit
                while self._active_calls:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    self._calls_cond.wait(remaining)
            try:
                try:
                    self._submit(
                        self._close_kernel,
                        timeout=limit,
                        allow_closing=True,
                    )
                except BaseException as exc:
                    first_error = exc
                try:
                    self._shutdown_loop(limit)
                except BaseException as exc:
                    if first_error is None:
                        first_error = exc
            finally:
                self._callback_executor.shutdown(wait=False)
                with self._wrappers_lock:
                    self._wrappers.clear()
                self._closed = True
                self._closing = False
            if first_error is not None:
                raise first_error

    async def _close_kernel(self) -> None:
        ps = self._ps
        ps._running = False
        errors: list[BaseException] = []
        try:
            for pool in list(ps._pools.values()):
                try:
                    await pool.close()
                except BaseException as exc:
                    errors.append(exc)
            try:
                await ps.bus.stop()
            except BaseException as exc:
                errors.append(exc)
            try:
                ps.audit.close()
            except BaseException as exc:
                errors.append(exc)
            if self._owns_redis:
                close = getattr(ps.idempotency, "aclose", None)
                if close is not None:
                    try:
                        await close()
                    except BaseException as exc:
                        errors.append(exc)
        finally:
            # This path bypasses Purser.close(), so release the instance-level
            # isolation claims (redis DB, registered types) here as well.
            ps._isolation.release_all()
        if errors:
            raise errors[0]

    def __enter__(self) -> SyncPurser:
        try:
            self.start()
        except BaseException:
            with contextlib.suppress(Exception):
                self.close()
            raise
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    @property
    def started(self) -> bool:
        if self._closed or self._start_error is not None:
            return False
        return self._call_sync(lambda: self._ps.started)

    @property
    def storage_kind(self) -> str:
        return self._ps.storage_kind

    @property
    def coordination_kind(self) -> str:
        return self._ps.coordination_kind

    @property
    def closed(self) -> bool:
        return self._closed

    # -- types / pools ----------------------------------------------------
    def register_type(self, rtype: ResourceType) -> None:
        self._call_sync(lambda: self._ps.register_type(rtype))

    async def _create_pool_async(
        self,
        name: str,
        kind: str | None,
        config: Any,
        overrides: dict[str, Any],
        *,
        get_only: bool,
    ) -> ResourcePool:
        ps = self._ps
        if get_only:
            existing = ps._pools.get(name)
            if existing is not None:
                return existing
            if kind is None:
                existing = ps.pool(name)
                return existing
        try:
            assert kind is not None
            pool = ps.create_pool(name, kind, config=config, **overrides)
        except BaseException:
            raise
        if ps.started:
            try:
                await pool._ensure_started("create_pool")
                task = pool._start_task
                if task is not None and task.done():
                    if task.cancelled():
                        raise RuntimeError("pool start was cancelled")
                    error = task.exception()
                    if error is not None:
                        raise error
            except BaseException:
                ps._pools.pop(name, None)
                with contextlib.suppress(BaseException):
                    await pool.close()
                raise
        return pool

    def create_pool(
        self,
        name: str,
        kind: str,
        *,
        config: Any = None,
        **overrides: Any,
    ) -> SyncPool:
        copied_config = copy.deepcopy(config)
        copied_overrides = {key: copy.deepcopy(value) for key, value in overrides.items()}
        pool = self._submit(
            lambda: self._create_pool_async(
                name, kind, copied_config, copied_overrides, get_only=False
            )
        )
        return self._remember_pool(pool)

    def pool(
        self,
        name: str,
        kind: str | None = None,
        *,
        config: Any = None,
        **overrides: Any,
    ) -> SyncPool:
        cached = self._cached_pool(name)
        if cached is not None:
            return cached
        copied_config = copy.deepcopy(config)
        copied_overrides = {key: copy.deepcopy(value) for key, value in overrides.items()}
        pool = self._submit(
            lambda: self._create_pool_async(
                name, kind, copied_config, copied_overrides, get_only=True
            )
        )
        return self._remember_pool(pool)

    def get_pool(self, name: str) -> SyncPool | None:
        cached = self._cached_pool(name)
        if cached is not None:
            return cached
        pool = self._call_sync(lambda: self._ps.get_pool(name))
        return None if pool is None else self._remember_pool(pool)

    def require_pool(self, name: str) -> SyncPool:
        cached = self._cached_pool(name)
        if cached is not None:
            return cached
        pool = self._call_sync(lambda: self._ps.require_pool(name))
        return self._remember_pool(pool)

    def pool_names(self) -> list[str]:
        return self._call_sync(lambda: list(self._ps.pool_names()))

    def delete_pool(
        self, name: str, *, force: bool = False, operator: str = "system"
    ) -> dict[str, Any]:
        owner = self

        async def run() -> dict[str, Any]:
            return owner._snapshot(
                await owner._ps.delete_pool(
                    name, force=force, operator=operator
                )
            )

        result = self._submit(run)
        self._drop_pool(name)
        return result

    def _cached_pool(self, name: str) -> SyncPool | None:
        with self._wrappers_lock:
            return self._wrappers.get(name)

    def _remember_pool(self, pool: ResourcePool) -> SyncPool:
        with self._wrappers_lock:
            existing = self._wrappers.get(pool.name)
            if existing is not None and existing._pool is pool:
                return existing
            wrapper = SyncPool(self, pool)
            self._wrappers[pool.name] = wrapper
            return wrapper

    def _drop_pool(self, name: str) -> None:
        with self._wrappers_lock:
            self._wrappers.pop(name, None)

    def _forget_pool(self, pool: SyncPool) -> None:
        with self._wrappers_lock:
            existing = self._wrappers.get(pool.name)
            if existing is pool:
                self._wrappers.pop(pool.name, None)

    # -- reports / backup -------------------------------------------------
    def report_usage(self, **kwargs: Any) -> UsageReport:
        copied = copy.deepcopy(kwargs)
        owner = self

        async def run() -> UsageReport:
            return owner._snapshot(await owner._ps.report_usage(**copied))

        return self._submit(run)

    def backup(self, *, operator: str = "system") -> dict[str, Any]:
        owner = self

        async def run() -> dict[str, Any]:
            return owner._snapshot(await owner._ps.backup(operator=operator))

        return self._submit(run)

    def restore(
        self,
        document: dict[str, Any],
        *,
        conflict: str = "skip",
        operator: str = "system",
    ) -> dict[str, Any]:
        copied = copy.deepcopy(document)
        owner = self

        async def run() -> dict[str, Any]:
            return owner._snapshot(
                await owner._ps.restore(
                    copied, conflict=conflict, operator=operator
                )
            )

        return self._submit(run)

    # -- events -----------------------------------------------------------
    def subscribe(self, callback: Callable[[Any], Any]) -> SyncSubscription:
        if not callable(callback):
            raise TypeError("callback must be callable")
        executor = self._callback_executor
        ctx = self._callback_ctx
        owner = self

        def invoke(event: Any) -> Any:
            with ctx.mark():
                return callback(event)

        async def listener(event: Any) -> None:
            snapshot = owner._snapshot(event)
            loop = owner._loop
            assert loop is not None
            await loop.run_in_executor(executor, invoke, snapshot)

        unsubscribe = self._call_sync(
            lambda: self._ps.bus.subscribe(listener)
        )
        return SyncSubscription(self, unsubscribe)

    def history(self, after_seq: int = 0, limit: int = 500) -> list[Any]:
        return self._call_sync(
            lambda: self._snapshot(self._ps.bus.history(after_seq, limit))
        )


__all__ = [
    "SyncBatchLease",
    "SyncLease",
    "SyncPool",
    "SyncPurser",
    "SyncResourceIterator",
    "SyncSubscription",
]
