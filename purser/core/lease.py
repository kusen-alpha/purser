"""Lease lifecycle: heartbeat renewal, expiry detection, watchdog reaping."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from purser.core.resource import Lease
from purser.errors import PurserError, is_retryable

logger = logging.getLogger("purser.lease")

if TYPE_CHECKING:
    RenewFn = Callable[[Lease], Awaitable[bool]]
    LostFn = Callable[[Lease], Awaitable[None]]


@dataclass(slots=True)
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
    _renew: dict[str, RenewFn] = field(default_factory=dict, repr=False)
    _errors: dict[str, int] = field(default_factory=dict, repr=False)
    _ticker: asyncio.Task[Any] | None = field(default=None, repr=False)
    _consumer_counts: dict[str, int] = field(default_factory=dict, repr=False)
    _closed: bool = False

    # One ticker renews every lease of a pool instead of one task per lease;
    # sleep is capped so short test/deployment intervals still stagger the
    # loop, and each renewal is checked against ``heartbeat_at`` so the
    # cadence stays per-lease.
    _TICKER_MAX_SLEEP = 1.0

    def register(self, lease: Lease, renew: RenewFn) -> None:
        """Start heartbeat renewal for ``lease``."""

        previous = self._leases.get(lease.token)
        if previous is not None and previous.consumer:
            self._drop_consumer(previous.consumer)
        self._leases[lease.token] = lease
        self._renew[lease.token] = renew
        self._errors.pop(lease.token, None)
        if lease.consumer:
            self._consumer_counts[lease.consumer] = (
                self._consumer_counts.get(lease.consumer, 0) + 1
            )
        if self._closed:
            return
        if self._ticker is None or self._ticker.done():
            self._ticker = asyncio.create_task(self._ticker_loop())

    def unregister(self, token: str) -> None:
        lease = self._leases.pop(token, None)
        self._renew.pop(token, None)
        self._errors.pop(token, None)
        if lease is not None and lease.consumer:
            self._drop_consumer(lease.consumer)

    def _drop_consumer(self, consumer: str) -> None:
        remaining = self._consumer_counts.get(consumer, 0) - 1
        if remaining > 0:
            self._consumer_counts[consumer] = remaining
        else:
            self._consumer_counts.pop(consumer, None)

    def consumer_count(self, consumer: str) -> int:
        """Active leases held by ``consumer`` (O(1) for admission checks)."""

        return self._consumer_counts.get(consumer, 0)

    def get(self, token: str) -> Lease | None:
        return self._leases.get(token)

    def active(self) -> list[Lease]:
        return list(self._leases.values())

    def active_count(self) -> int:
        return len(self._leases)

    async def close(self) -> None:
        self._closed = True
        ticker, self._ticker = self._ticker, None
        if ticker is not None:
            ticker.cancel()
            await asyncio.gather(ticker, return_exceptions=True)
        self._renew.clear()
        self._errors.clear()

    def prune(self, now: float | None = None) -> list[Lease]:
        """Returns live leases; heartbeat tasks live on the shared ticker."""

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

    async def _ticker_loop(self) -> None:
        interval = self.heartbeat_interval
        sleep_for = min(interval, self._TICKER_MAX_SLEEP)
        while True:
            await asyncio.sleep(sleep_for)
            now = time.time()
            due = [
                lease
                for lease in list(self._leases.values())
                if lease.token in self._renew
                and not lease.expired(now)
                and now - lease.heartbeat_at >= interval * 0.9
            ]
            if due:
                await asyncio.gather(
                    *(self._renew_lease(lease) for lease in due),
                    return_exceptions=True,
                )

    async def _renew_lease(self, lease: Lease) -> None:
        renew = self._renew.get(lease.token)
        if renew is None:
            return
        try:
            ok = await renew(lease)
        except asyncio.CancelledError:
            raise
        except PurserError as exc:
            if not is_retryable(exc):
                logger.warning(
                    "heartbeat renewal rejected for %s: %s", lease.token, exc
                )
                await self._give_up(lease)
                return
            if self._record_transient(lease, str(exc)):
                await self._give_up(lease)
            return
        except Exception as exc:  # noqa: BLE001 - transient by default
            if self._record_transient(lease, str(exc)):
                await self._give_up(lease)
            return
        self._errors.pop(lease.token, None)
        if not ok:
            await self._give_up(lease)

    def _record_transient(self, lease: Lease, detail: str) -> bool:
        """Count one transient renewal failure; ``True`` when giving up."""

        errors = self._errors.get(lease.token, 0) + 1
        self._errors[lease.token] = errors
        if errors >= self.max_renew_errors:
            logger.warning(
                "heartbeat renew failed %d times for %s", errors, lease.token
            )
            return True
        logger.warning("heartbeat renew transient error for %s: %s", lease.token, detail)
        return False

    async def _give_up(self, lease: Lease) -> None:
        self._renew.pop(lease.token, None)
        self._errors.pop(lease.token, None)
        await self._report_lost(lease)

    async def _report_lost(self, lease: Lease) -> None:
        if self.on_lost is not None:
            try:
                await self.on_lost(lease)
            except Exception:
                logger.exception("on_lost failed for %s", lease.token)
