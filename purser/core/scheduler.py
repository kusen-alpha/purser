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
    """Runs registered async tasks on independent runners.  Tasks are
    idempotent by contract; a distributed lock can be layered on top for
    multi-instance.  Each task keeps its own cadence, so a slow task never
    delays the others."""

    def __init__(self) -> None:
        self._tasks: dict[str, PeriodicTask] = {}
        self._runners: dict[str, asyncio.Task[Any]] = {}
        self._running = False

    def add(
        self,
        name: str,
        interval: float,
        run: RunFn,
        *,
        enabled: bool = True,
    ) -> PeriodicTask:
        task = PeriodicTask(name=name, interval=interval, run=run, enabled=enabled)
        old = self._runners.pop(name, None)
        if old is not None:
            old.cancel()
        self._tasks[name] = task
        if self._running and enabled:
            self._start_runner(task)
        return task

    def remove(self, name: str) -> None:
        self._tasks.pop(name, None)
        runner = self._runners.pop(name, None)
        if runner is not None:
            runner.cancel()

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
        self._running = True
        for task in list(self._tasks.values()):
            if task.enabled and task.name not in self._runners:
                self._start_runner(task)

    async def stop(self) -> None:
        self._running = False
        runners = list(self._runners.values())
        self._runners.clear()
        for runner in runners:
            runner.cancel()
        if runners:
            await asyncio.gather(*runners, return_exceptions=True)

    def _start_runner(self, task: PeriodicTask) -> None:
        self._runners[task.name] = asyncio.create_task(self._task_loop(task))

    async def _task_loop(self, task: PeriodicTask) -> None:
        while True:
            await asyncio.sleep(task.interval)
            if not task.enabled:
                continue
            try:
                await task.run()
                task.last_run = asyncio.get_running_loop().time()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("periodic task %s failed", task.name)
