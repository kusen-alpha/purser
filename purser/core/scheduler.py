"""PeriodicScheduler: six idempotent maintenance tasks (5.9 / O19 / X3)."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

logger = logging.getLogger("purser.scheduler")

if TYPE_CHECKING:
    RunFn = Callable[[], Awaitable[None]]


@dataclass
class PeriodicTask:
    name: str
    interval: float
    run: RunFn
    enabled: bool = True
    last_run: float = 0.0


class PeriodicScheduler:
    """Runs registered async tasks on a shared loop.  Tasks are idempotent by
    contract; a distributed lock can be layered on top for multi-instance."""

    def __init__(self) -> None:
        self._tasks: dict[str, PeriodicTask] = {}
        self._runner: asyncio.Task[Any] | None = None

    def add(
        self,
        name: str,
        interval: float,
        run: RunFn,
        *,
        enabled: bool = True,
    ) -> PeriodicTask:
        task = PeriodicTask(name=name, interval=interval, run=run, enabled=enabled)
        self._tasks[name] = task
        return task

    def remove(self, name: str) -> None:
        self._tasks.pop(name, None)

    def get(self, name: str) -> PeriodicTask | None:
        return self._tasks.get(name)

    def names(self) -> list[str]:
        return list(self._tasks)

    async def run_once(self, name: str) -> None:
        task = self._tasks.get(name)
        if task is None or not task.enabled:
            return
        try:
            await task.run()
            task.last_run = asyncio.get_running_loop().time()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("periodic task %s failed", name)

    async def start(self) -> None:
        if self._runner is None or self._runner.done():
            self._runner = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        if self._runner is not None:
            self._runner.cancel()
            try:
                await self._runner
            except asyncio.CancelledError:
                pass
            self._runner = None

    async def _loop(self) -> None:
        while True:
            for task in list(self._tasks.values()):
                if not task.enabled:
                    continue
                await asyncio.sleep(task.interval)
                try:
                    await task.run()
                    task.last_run = asyncio.get_running_loop().time()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("periodic task %s failed", task.name)
