"""Allocation strategies: score eligible candidates, then pick (5.1)."""

from __future__ import annotations

import math
import random
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from purser.config import Weights
from purser.core.resource import AcquireRequest, Resource


def _now() -> float:
    return time.time()


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, value))


@dataclass
class AllocationContext:
    now: float
    request: AcquireRequest
    weights: Weights
    affinity_scores: dict[str, float] | None = None
    group_priority: dict[str, int] | None = None
    top_k: int = 8
    cursor: int = 0
    rng: random.Random = field(default_factory=random.Random, repr=False)


class Allocator(ABC):
    """Pick one resource from the eligible candidate list."""

    name: str = "base"

    @abstractmethod
    def rank(self, candidates: list[Resource], ctx: AllocationContext) -> list[tuple[Resource, float]]:
        """Return candidates sorted best-first with their raw scores."""

    def pick(self, candidates: list[Resource], ctx: AllocationContext) -> Resource | None:
        ranked = self.rank(candidates, ctx)
        if not ranked:
            return None
        top = ranked[: ctx.top_k]
        return self._weighted_pick(top, ctx)

    @staticmethod
    def _weighted_pick(ranked: list[tuple[Resource, float]], ctx: AllocationContext) -> Resource:
        """Softmax-weighted random over the top-K (anti-thundering-herd)."""

        scores = [max(s, 1e-9) for _, s in ranked]
        mx = max(scores)
        exp = [math.exp((s - mx) * 4.0) for s in scores]
        total = sum(exp)
        r = ctx.rng.random() * total
        acc = 0.0
        # Plain zip: ``strict=`` is Python 3.10+, and the sequences are the
        # same length by construction here.
        for (res, _), e in zip(ranked, exp):  # noqa: B905
            acc += e
            if r <= acc:
                return res
        return ranked[-1][0]


class HealthWeightedAllocator(Allocator):
    """Default: health.score up, longer-idle up (LRU rotation), load down,
    preference and affinity as soft boosts, plus jitter (5.1)."""

    name = "health_weighted"

    def rank(self, candidates: list[Resource], ctx: AllocationContext) -> list[tuple[Resource, float]]:
        w = ctx.weights
        now = ctx.now
        out: list[tuple[Resource, float]] = []
        for res in candidates:
            age = 1.0
            last = res.last_used_at
            if last is not None:
                age = _clamp01((now - last) / 3600.0)
            pref = float(res.meta.get("preference", 0.0))
            aff = 0.0
            if ctx.affinity_scores is not None:
                aff = ctx.affinity_scores.get(res.id, 0.0)
            score = (
                w.health * res.health.score
                + w.age * age
                - w.load * res.concurrency.load_ratio
                + w.preference * pref
                + w.affinity * aff
                + w.jitter * ctx.rng.random()
            )
            out.append((res, score))
        out.sort(key=lambda item: item[1], reverse=True)
        return out


class RoundRobinAllocator(Allocator):
    """Cursor rotation for even, uniform usage."""

    name = "round_robin"

    def __init__(self) -> None:
        self._cursor = 0

    def rank(self, candidates: list[Resource], ctx: AllocationContext) -> list[tuple[Resource, float]]:
        if not candidates:
            return []
        idx = self._cursor % len(candidates)
        ordered = candidates[idx:] + candidates[:idx]
        return [(res, -i) for i, res in enumerate(ordered)]

    def pick(self, candidates: list[Resource], ctx: AllocationContext) -> Resource | None:
        ranked = self.rank(candidates, ctx)
        if not ranked:
            return None
        self._cursor = (self._cursor + 1) % len(candidates)
        return ranked[0][0]


class LeastLoadedAllocator(Allocator):
    """Fewest active leases first; perfect for shared account pools."""

    name = "least_loaded"

    def rank(self, candidates: list[Resource], ctx: AllocationContext) -> list[tuple[Resource, float]]:
        return sorted(
            ((res, -res.concurrency.used_slots + res.health.score * 0.01) for res in candidates),
            key=lambda item: item[1],
            reverse=True,
        )


class StaticPriorityAllocator(Allocator):
    """Tiered resources: higher declared priority always wins."""

    name = "static_priority"

    def rank(self, candidates: list[Resource], ctx: AllocationContext) -> list[tuple[Resource, float]]:
        def priority(res: Resource) -> float:
            if ctx.group_priority and res.meta.get("group"):
                return float(ctx.group_priority.get(res.meta["group"], 0))
            return float(res.meta.get("priority", 0))

        return sorted(
            ((res, priority(res)) for res in candidates),
            key=lambda item: item[1],
            reverse=True,
        )


ALLOCATORS: dict[str, type[Allocator]] = {
    cls.name: cls
    for cls in (
        HealthWeightedAllocator,
        RoundRobinAllocator,
        LeastLoadedAllocator,
        StaticPriorityAllocator,
    )
}


def make_allocator(name: str) -> Allocator:
    try:
        return ALLOCATORS[name]()
    except KeyError:
        from purser.errors import ConfigError

        raise ConfigError(f"unknown allocator {name!r}") from None
