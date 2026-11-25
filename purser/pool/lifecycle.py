"""Pool start/stop orchestration.

Kept separate from :class:`~purser.pool.pool.ResourcePool` so the facade class
stays a thin composition root.  ``reconcile`` stays on the pool itself: tests
monkeypatch it at instance level and ``start`` must dispatch through it.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
from typing import TYPE_CHECKING

from purser.errors import NotStarted, PoolExhausted

if TYPE_CHECKING:
    from purser.pool.pool import ResourcePool

logger = logging.getLogger("purser")


class LifecycleService:
    """Starts, waits for and tears down a pool's runtime."""

    def __init__(self, pool: ResourcePool) -> None:
        self.pool = pool

    async def start(self) -> None:
        pool = self.pool
        pool._ensure_process()
        if pool._started:
            return
        try:
            await pool._binding.claim()
        except BaseException:
            await pool._binding.rollback()
            raise
        try:
            await pool._authority.acquire()
            pool._started = True
            if pool.purser.maintenance_enabled:
                await pool._scheduler.start()
            pool._task_registry.start(pool._lease.reap_loop())
            pool._task_registry.start(pool._lease.serve_loop())
            if pool.purser.maintenance_enabled:
                pool._task_registry.start(pool._validation.background_loop())
            await pool._reconcile_leases()
        except BaseException:
            pool._started = False
            for task in pool._task_registry.pending():
                task.cancel()
            pool._task_registry.clear()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await pool._scheduler.stop()
            await pool._authority.release()
            await pool._binding.rollback()
            raise

    async def ensure_started(self, operation: str) -> None:
        pool = self.pool
        pool._ensure_process()
        task = pool._start_task
        if task is not None and not task.done():
            # A pool created after Purser.start() is starting on a pending task;
            # wait for its reconcile to finish instead of racing acquisitions.
            # Shield it: cancelling one waiter (timeout/consumer cancellation)
            # must not cancel the start task shared by every other waiter.
            await asyncio.shield(task)
            return
        if pool._started:
            return
        if pool.purser.started:
            await self.start()
            return
        raise NotStarted(
            pool.name,
            operation=operation,
            sync=bool(getattr(pool.purser, "_sync_mode", False)),
        )

    async def close(self) -> None:
        pool = self.pool
        pool._ensure_process()
        pool._started = False
        start_task, pool._start_task = pool._start_task, None
        if start_task is not None and not start_task.done():
            start_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await start_task
        await pool._scheduler.stop()
        await pool._drain()
        try:
            # Shutdown must never wedge on a stuck heartbeat task.
            await asyncio.wait_for(pool._leases.close(), timeout=10.0)
        except asyncio.TimeoutError:
            logger.warning("lease manager close timed out for pool %s", pool.name)
        pending = pool._task_registry.cancel_pending()
        if pending:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*pending, return_exceptions=True),
                    timeout=10.0,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "pool %s close timed out draining %d tasks", pool.name, len(pending)
                )
                for task in pending:
                    task.cancel()
        pool._task_registry.clear()
        # Fail in-flight waiters promptly so callers do not sleep until their
        # own timeout after the pool is closed.
        pool._wait_queue.fail_all(
            PoolExhausted(f"pool {pool.name} closed while waiting")
        )
        # A waiter may be mid-poll on shared storage. Let that in-flight check
        # land before teardown: closing a sqlite connection under a scan thread
        # crashes the interpreter.
        if pool._wait_queue.active_polls:
            try:
                await asyncio.wait_for(pool._wait_queue.idle.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning(
                    "pool %s close timed out draining waiter polls", pool.name
                )
        try:
            closer = getattr(pool.storage, "close", None)
            if closer is not None:
                result = closer()
                if inspect.isawaitable(result):
                    await result
            await pool._coordination.close()
        finally:
            await pool._authority.release()
            await pool._binding.release()
            pool.purser._release_pool_claims(pool)
