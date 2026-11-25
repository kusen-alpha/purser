"""Tracked asyncio tasks owned by one pool runtime."""

from __future__ import annotations

import asyncio
from typing import Any


class TaskRegistry:
    """Long-lived runtime tasks and fire-and-forget work.

    :meth:`close` drains :meth:`pending`; anything started through
    :meth:`spawn` is discarded from the set once it finishes.
    """

    def __init__(self) -> None:
        self.tasks: list[asyncio.Task[Any]] = []
        self.bg_tasks: set[asyncio.Task[Any]] = set()

    def start(self, coro: Any) -> asyncio.Task[Any]:
        task = asyncio.create_task(coro)
        self.tasks.append(task)
        return task

    def spawn(self, coro: Any) -> asyncio.Task[Any]:
        task = asyncio.create_task(coro)
        self.bg_tasks.add(task)
        task.add_done_callback(self.bg_tasks.discard)
        return task

    def pending(self) -> list[asyncio.Task[Any]]:
        return [*self.tasks, *self.bg_tasks]

    def cancel_pending(self) -> list[asyncio.Task[Any]]:
        tasks = self.pending()
        for task in tasks:
            task.cancel()
        return tasks

    def clear(self) -> None:
        self.tasks.clear()
        self.bg_tasks.clear()
