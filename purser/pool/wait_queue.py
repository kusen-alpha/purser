"""Fair waiter heap plus the in-flight poll counter that guards teardown."""

from __future__ import annotations

import asyncio
import heapq

from purser.facade._helpers import _WaitHandle


class WaitQueue:
    """Priority heap of waiters with a monotonic tie-breaker.

    ``heap`` stays a plain list so callers can keep using :mod:`heapq`
    directly during the incremental facade split.
    """

    def __init__(self) -> None:
        self.heap: list[_WaitHandle] = []
        self.seq = 0
        self.active_polls = 0
        self.idle = asyncio.Event()
        self.idle.set()

    def __len__(self) -> int:
        return len(self.heap)

    def next_seq(self) -> int:
        self.seq += 1
        return self.seq

    def begin_poll(self) -> None:
        if self.active_polls == 0:
            self.idle.clear()
        self.active_polls += 1

    def end_poll(self) -> None:
        self.active_polls -= 1
        if self.active_polls == 0:
            self.idle.set()

    def fail_all(self, exc: BaseException) -> None:
        while self.heap:
            handle = heapq.heappop(self.heap)
            if not handle.future.done():
                handle.future.set_exception(exc)
        self.heap.clear()
