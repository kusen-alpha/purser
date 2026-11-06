"""Lease lifecycle: heartbeat renewal, expiry detection, watchdog reaping."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from purser.core.resource import Lease

logger = logging.getLogger("purser.lease")

if TYPE_CHECKING:
    RenewFn = Callable[[Lease], Awaitable[bool]]
    LostFn = Callable[[Lease], Awaitable[None]]


@dataclass
class LeaseManager:
    """Tracks in-flight leases of one pool.

    ``on_expire`` is invoked by :meth:`reap` for leases past their deadline and
    ``on_lost`` when the heartbeat renewal stops being acknowledged (the
    resource disappeared or was force-revoked by someone else).
    """

    on_expire: LostFn | None = None
    on_lost: LostFn | None = None
    heartbeat_interval: float = 20.0
    max_renew_errors: int = 3
    _leases: dict[str, Lease] = field(default_factory=dict, repr=False)
    _tasks: dict[str, asyncio.Task[Any]] = field(default_factory=dict, repr=False)
    _closed: bool = False

    def register(self, lease: Lease, renew: RenewFn) -> None:
        """Start heartbeat renewal for ``lease``."""

        self._leases[lease.token] = lease
        if self._closed:
            return
        self._tasks[lease.token] = asyncio.create_task(
            self._heartbeat_loop(lease, renew)
        )

    def unregister(self, token: str) -> None:
        self._leases.pop(token, None)
        task = self._tasks.pop(token, None)
        if task is not None:
            task.cancel()

    def get(self, token: str) -> Lease | None:
        return self._leases.get(token)

    def active(self) -> list[Lease]:
        return list(self._leases.values())

    def active_count(self) -> int:
        return len(self._leases)

    async def close(self) -> None:
        self._closed = True
        for task in self._tasks.values():
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        self._tasks.clear()

    def prune(self, now: float | None = None) -> list[Lease]:
        """Remove finished/failed heartbeat tasks; returns live leases."""

        now = time.time() if now is None else now
        for token, task in list(self._tasks.items()):
            if task.done():
                del self._tasks[token]
        return self.active()

    def expired(self, now: float | None = None) -> list[Lease]:
        now = time.time() if now is None else now
        return [lease for lease in self._leases.values() if lease.expired(now)]

    async def reap(self, now: float | None = None) -> list[Lease]:
        """Watchdog: report every exceeded lease to :attr:`on_expire`."""

        expired = self.expired(now)
        if expired and self.on_expire is not None:
            for lease in expired:
                try:
                    await self.on_expire(lease)
                except Exception:
                    logger.exception("on_expire failed for lease %s", lease.token)
        return expired

    async def _heartbeat_loop(self, lease: Lease, renew: RenewFn) -> None:
        consecutive_errors = 0
        while True:
            await asyncio.sleep(self.heartbeat_interval)
            if lease.expired():
                break
            try:
                ok = await renew(lease)
            except asyncio.CancelledError:
                raise
            except Exception:
                # Transient storage/coordination errors should not kill a
                # healthy lease: retry before declaring it lost.
                consecutive_errors += 1
                if consecutive_errors >= self.max_renew_errors:
                    logger.warning(
                        "heartbeat renew failed %d times for %s",
                        consecutive_errors,
                        lease.token,
                    )
                    await self._report_lost(lease)
                    break
                logger.warning("heartbeat renew transient error for %s", lease.token)
                continue
            consecutive_errors = 0
            if not ok:
                await self._report_lost(lease)
                break

    async def _report_lost(self, lease: Lease) -> None:
        if self.on_lost is not None:
            try:
                await self.on_lost(lease)
            except Exception:
                logger.exception("on_lost failed for %s", lease.token)
