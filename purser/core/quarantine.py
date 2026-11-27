"""Quarantine / ban escalation policy: isolate first, ban last."""

from __future__ import annotations

import math
import random
import time
from dataclasses import dataclass, field


def _now() -> float:
    return time.time()


@dataclass(slots=True)
class QuarantinePolicy:
    base: float = 30.0
    cap: float = 1800.0
    jitter: float = 10.0
    max_failures: int = 3
    trial_hours: float = 24.0
    rng: random.Random = field(default_factory=random.Random, repr=False)

    def duration(self, fail_count: int) -> float:
        """Exponential backoff with jitter: base*2^(n-1), capped."""

        n = max(1, fail_count)
        raw = min(self.cap, self.base * (2.0 ** (n - 1)))
        return raw + self.rng.uniform(0.0, self.jitter)

    def threshold_for(self, created_at: float, now: float | None = None) -> int:
        """Failures tolerated before BANNED.  Trial protection halves it but
        never bans on the very first validation failure."""

        now = now or _now()
        in_trial = (now - created_at) < self.trial_hours * 3600.0
        if in_trial:
            return max(2, math.ceil(self.max_failures / 2.0))
        return max(1, self.max_failures)
