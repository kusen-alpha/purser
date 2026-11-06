"""Event bus with bounded queue and backpressure (E1) plus cursor replay (E3)."""

from __future__ import annotations

import asyncio
import collections
import inspect
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger("purser.events")

Listener = Callable[["Event"], Awaitable[None]]


@dataclass(slots=True)
class Event:
    type: str
    pool: str
    rid: str | None = None
    data: dict[str, Any] = field(default_factory=dict)
    seq: int = 0
    ts: float = field(default_factory=time.time)
    trace_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "type": self.type,
            "pool": self.pool,
            "rid": self.rid,
            "data": self.data,
            "ts": self.ts,
            "trace_id": self.trace_id,
        }


class EventBus:
    """Async-dispatch bus.  Kernel publishes synchronously and never blocks;
    a dispatcher task drains the bounded queue to subscribers.  Slow or broken
    subscribers are dropped after a warning, never fatal to the publisher.
    """

    def __init__(
        self,
        maxsize: int = 10_000,
        *,
        listener_timeout: float = 5.0,
        listener_cooldown: float = 10.0,
    ) -> None:
        self.maxsize = maxsize
        self.listener_timeout = listener_timeout
        self.listener_cooldown = listener_cooldown
        self._queue: collections.deque[Event] = collections.deque(maxlen=maxsize)
        self._history: collections.deque[Event] = collections.deque(maxlen=maxsize)
        self._listeners: list[Listener] = []
        self._listener_fails: dict[int, int] = {}
        self._disabled_until: dict[int, float] = {}
        self._seq = 0
        self._task: asyncio.Task[Any] | None = None
        self.dropped = 0
        self._lock = asyncio.Lock()

    @property
    def last_seq(self) -> int:
        return self._seq

    def subscribe(self, listener: Listener) -> Callable[[], None]:
        """Register an async listener; returns an unsubscribe callable."""

        self._listeners.append(listener)

        def unsubscribe() -> None:
            if listener in self._listeners:
                self._listeners.remove(listener)
                self._listener_fails.pop(id(listener), None)
                self._disabled_until.pop(id(listener), None)

        return unsubscribe

    def publish(
        self,
        type_: str,
        pool: str,
        *,
        rid: str | None = None,
        data: dict[str, Any] | None = None,
        trace_id: str | None = None,
    ) -> Event:
        self._seq += 1
        event = Event(
            type=type_,
            pool=pool,
            rid=rid,
            data=data or {},
            seq=self._seq,
            trace_id=trace_id,
        )
        before = len(self._queue)
        self._queue.append(event)
        self._history.append(event)
        # Bounded queue: when the dispatcher could not keep up, the deque
        # evicts the oldest event; count only real evictions (E1).
        if before == self.maxsize:
            self.dropped += 1
        return event

    def history(self, after_seq: int = 0, limit: int = 500) -> list[Event]:
        """Replay dispatched events with ``seq > after_seq`` (E3).

        The history is a separate bounded ring from the dispatch queue, so
        SSE cursor continuation still sees events the dispatcher consumed.
        """

        return [
            e for e in self._history if e.seq > after_seq
        ][-limit:]

    async def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._dispatch_loop())

    async def stop(self) -> None:
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            try:
                # Never block shutdown on a wedged dispatcher.
                await asyncio.wait_for(asyncio.shield(task), timeout=2.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass

    async def _dispatch_loop(self) -> None:
        while True:
            if not self._queue:
                await asyncio.sleep(0.01)
                continue
            event = self._queue.popleft()
            now = time.time()
            for listener in list(self._listeners):
                key = id(listener)
                if self._disabled_until.get(key, 0.0) > now:
                    continue
                try:
                    result = listener(event)
                    if inspect.iscoroutine(result):
                        task = asyncio.ensure_future(result)
                        done, _ = await asyncio.wait(
                            {task}, timeout=self.listener_timeout
                        )
                        if not done:
                            task.cancel()
                            raise asyncio.TimeoutError
                        if task.cancelled():
                            self.dropped += 1
                            continue
                        task.result()  # propagate listener exceptions
                    self._listener_fails[key] = 0
                except asyncio.TimeoutError:
                    logger.warning(
                        "event listener timed out after %.1fs; pausing it",
                        self.listener_timeout,
                    )
                    self.dropped += 1
                    fails = self._listener_fails.get(key, 0) + 1
                    if fails >= 2:
                        self._disabled_until[key] = time.time() + self.listener_cooldown
                        self._listener_fails[key] = 0
                    else:
                        self._listener_fails[key] = fails
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("event listener failed")
                    self.dropped += 1


class EventSink:
    """Adapter that forwards every event to an async callback."""

    def __init__(self, bus: EventBus, fn: Listener) -> None:
        self._bus = bus
        self._fn = fn
        self._unsub = bus.subscribe(fn)

    def close(self) -> None:
        self._unsub()
