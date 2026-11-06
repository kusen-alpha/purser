"""Health scoring and the checker protocol (5.3 / X1 / Y6)."""

from __future__ import annotations

import enum
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from purser.core.resource import HealthStats, Resource

if TYPE_CHECKING:
    RefreshCallback = Callable[[Resource], bool]  # True when a refresh is needed

logger = logging.getLogger("purser.health")


def _now() -> float:
    return time.time()


class Action(str, enum.Enum):
    """Suggested action carried by a HealthReport."""

    OK = "ok"
    REVALIDATE = "revalidate"
    BAN = "ban"
    DROP = "drop"


@dataclass
class HealthReport:
    """One validation verdict (5.3)."""

    alive: bool
    latency_ms: float | None = None
    error: str | None = None
    action: Action = Action.OK

    @classmethod
    def ok(cls, latency_ms: float | None = None) -> HealthReport:
        return cls(alive=True, latency_ms=latency_ms, action=Action.OK)

    @classmethod
    def fail(
        cls,
        error: str,
        *,
        action: Action = Action.REVALIDATE,
        latency_ms: float | None = None,
    ) -> HealthReport:
        return cls(alive=False, latency_ms=latency_ms, error=error, action=action)


class HealthChecker(Protocol):
    """Anything that can answer 'is this resource alive?'."""

    async def validate(self, resource: Resource) -> HealthReport: ...


def apply_report(health: HealthStats, report: HealthReport) -> None:
    """Merge a HealthReport into EWMA stats."""

    health.update(
        report.alive,
        latency_ms=report.latency_ms,
        error=report.error,
    )


class ProbeGuard:
    """Probe self-check (X1): auto-remove probes that keep failing, and pause
    mass banning when every probe is down.
    """

    def __init__(
        self,
        *,
        fail_threshold: int = 3,
        cooldown: float = 60.0,
    ) -> None:
        self.fail_threshold = fail_threshold
        self.cooldown = cooldown
        self._fails: dict[str, int] = {}
        self._disabled_until: dict[str, float] = {}
        self._total: dict[str, tuple[int, int]] = {}  # probe -> (ok, fail)

    def record(self, probe: str, ok: bool) -> None:
        oks, fails = self._total.get(probe, (0, 0))
        if ok:
            oks += 1
            self._fails[probe] = 0
        else:
            fails += 1
            self._fails[probe] = self._fails.get(probe, 0) + 1
            if self._fails[probe] >= self.fail_threshold:
                self._disabled_until[probe] = _now() + self.cooldown
        self._total[probe] = (oks, fails)

    def enabled(self, probe: str, now: float | None = None) -> bool:
        until = self._disabled_until.get(probe, 0.0)
        return (now or _now()) >= until

    def all_down(self, probes: list[str], now: float | None = None) -> bool:
        now = now or _now()
        return bool(probes) and all(
            not self.enabled(p, now) for p in probes
        )

    def health(self, probe: str) -> float:
        oks, fails = self._total.get(probe, (0, 0))
        total = oks + fails
        return (oks / total) if total else 1.0


class ValidationGate:
    """Adaptive staleness policy (5.3 / S7): when is on-demand refresh due?"""

    def __init__(
        self,
        *,
        stale_after: float = 30.0,
        background_interval: float = 30.0,
        sample: float = 1.0,
    ) -> None:
        self.stale_after = stale_after
        self.background_interval = background_interval
        self.sample = sample

    def stale(self, resource: Resource, now: float | None = None) -> bool:
        now = now or _now()
        if resource.health.last_checked_at is None:
            return True
        return (now - resource.health.last_checked_at) >= self.stale_after

    def due_background(self, resource: Resource, now: float | None = None) -> bool:
        if not self.stale(resource, now):
            return False
        if self.sample >= 1.0:
            return True
        # Deterministic pseudorandom sampling (Y2): stable per resource.
        h = sum(resource.id.encode("utf-8")) % 10000 / 10000.0
        return h < self.sample
