"""Synchronous facade for the asyncio-first purser kernel.

A :class:`SyncPurser` owns one dedicated event-loop thread.  The kernel and
every pool are created and used on that loop.  Calls from other threads are
submitted to the loop and wait for their result; they never touch loop-bound
objects directly.

The facade deliberately keeps the same public method names as the async API.
Lease objects are returned as :class:`SyncLease` proxies so heartbeat and
``used`` updates remain visible across the thread boundary.  The proxy types
live in :mod:`purser.sync_primitives` and :mod:`purser.sync_pool` and are
re-exported from here.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import copy
import inspect
import threading
import time
from collections.abc import Awaitable, Callable, Coroutine, Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import Any, cast, overload

from purser.adapters.base import ResourceAdapter
from purser.core.bundle import BundleLease
from purser.core.resource import UsageOutcome
from purser.core.views import UsageReport
from purser.errors import IsolationConflict, PoolExists
from purser.facade import Purser, ResourcePool
from purser.isolation import PICKLE_MESSAGE
from purser.protocols import MISSING
from purser.sync_pool import SyncPool
from purser.sync_primitives import (
    SyncBatchLease,
    SyncLease,
    SyncResourceIterator,
    SyncSubscription,
    T,
    _CallbackContext,
    _LoopProxy,
    logger,
)

_TERMINAL = (str, bytes, int, float, bool, type(None))


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
            self._ps._sync_mode = True
        except BaseException:
            self._shutdown_loop(1.0)
            self._callback_executor.shutdown(wait=False)
            raise

        self.audit = _LoopProxy(self, self._ps.audit)
        self.metrics = _LoopProxy(self, self._ps.metrics)
        self.idempotency = _LoopProxy(self, self._ps.idempotency)
        self.probe_guard = _LoopProxy(self, self._ps.probe_guard)
        self.usage = _LoopProxy(self, self._ps.usage)
        self.adapters = _LoopProxy(
            self,
            self._ps.adapters,
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
            # isolation claims (redis DB, registered adapters) here as well.
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

    # -- adapters / pools ----------------------------------------------------
    def register_adapter(self, adapter: ResourceAdapter) -> None:
        self._call_sync(lambda: self._ps.register_adapter(adapter))

    def acquire_bundle(self, request: Any, **overrides: Any) -> BundleLease:
        """Synchronous cross-pool bundle acquire."""

        return self._submit(
            lambda: self._ps.acquire_bundle(request, **overrides)
        )

    def release_bundle(
        self,
        bundle: BundleLease,
        *,
        outcome_by_pool: dict[str, UsageOutcome] | None = None,
        used_by_pool: dict[str, int] | None = None,
    ) -> None:
        """Synchronous bundle release (reversed acquisition order)."""

        self._submit(
            lambda: self._ps.release_bundle(
                bundle,
                outcome_by_pool=outcome_by_pool,
                used_by_pool=used_by_pool,
            )
        )

    def bundle_gc(self) -> dict[str, int]:
        """Synchronous sweep of expired bundle reservations."""

        return self._submit(lambda: self._ps.bundle_gc())

    async def _create_pool_async(
        self,
        name: str,
        kind: str,
        config: Any,
        overrides: dict[str, Any],
        *,
        profile: Any = None,
    ) -> ResourcePool:
        pool = self._ps.create_pool(
            name, kind, config=config, profile=profile, **overrides
        )
        await self._await_pool_started(pool)
        return pool

    async def _ensure_pool_async(
        self,
        name: str,
        kind: str,
        config: Any,
        overrides: dict[str, Any],
        *,
        profile: Any = None,
    ) -> ResourcePool:
        ps = self._ps
        existing = ps._pools.get(name)
        if existing is not None:
            return existing
        try:
            pool = ps.create_pool(
                name, kind, config=config, profile=profile, **overrides
            )
        except PoolExists:
            return ps.get_pool(name)
        await self._await_pool_started(pool)
        return pool

    async def _get_pool_async(self, name: str, default: Any) -> Any:
        return self._ps.get_pool(name, default=default)

    async def _await_pool_started(self, pool: ResourcePool) -> None:
        if not self._ps.started:
            return
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
            self._ps._pools.pop(pool.name, None)
            with contextlib.suppress(BaseException):
                await pool.close()
            raise

    def create_pool(
        self,
        name: str,
        kind: str,
        *,
        config: Any = None,
        profile: Any = None,
        **overrides: Any,
    ) -> SyncPool:
        copied_config = copy.deepcopy(config)
        copied_profile = copy.deepcopy(profile)
        copied_overrides = {key: copy.deepcopy(value) for key, value in overrides.items()}
        pool = self._submit(
            lambda: self._create_pool_async(
                name,
                kind,
                copied_config,
                copied_overrides,
                profile=copied_profile,
            )
        )
        return self._remember_pool(pool)

    def ensure_pool(
        self,
        name: str,
        kind: str,
        *,
        config: Any = None,
        profile: Any = None,
        **overrides: Any,
    ) -> SyncPool:
        """Return pool ``name``, creating it with ``kind`` when absent."""

        copied_config = copy.deepcopy(config)
        copied_profile = copy.deepcopy(profile)
        copied_overrides = {key: copy.deepcopy(value) for key, value in overrides.items()}
        pool = self._submit(
            lambda: self._ensure_pool_async(
                name,
                kind,
                copied_config,
                copied_overrides,
                profile=copied_profile,
            )
        )
        return self._remember_pool(pool)

    @overload
    def get_pool(self, name: str, *, default: T) -> SyncPool | T: ...

    @overload
    def get_pool(self, name: str, *, default: Any = MISSING) -> SyncPool: ...

    def get_pool(self, name: str, *, default: Any = MISSING) -> Any:
        """Return an existing pool, or ``default`` / raise ``PoolNotFound``."""

        cached = self._cached_pool(name)
        if cached is not None:
            return cached
        result = self._submit(lambda: self._get_pool_async(name, default))
        if not isinstance(result, ResourcePool):
            return result
        return self._remember_pool(result)

    @property
    def pools(self) -> Mapping[str, SyncPool]:
        """Snapshot of locally registered pools; later changes are not reflected."""

        raw = self._call_sync(lambda: dict(self._ps.pools))
        return {name: self._remember_pool(pool) for name, pool in raw.items()}

    def get_pool_names(self) -> list[str]:
        return self._call_sync(lambda: list(self._ps.get_pool_names()))

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

    def restore_backup(
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
                await owner._ps.restore_backup(
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
