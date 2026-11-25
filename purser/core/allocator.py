"""Allocation strategies: score eligible candidates, then pick."""

from __future__ import annotations

import math
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import cast

from purser.allocator_registry import register_allocator_name
from purser.config import Weights
from purser.core.meta import ResourceMeta
from purser.core.resource import AcquireRequest, Resource
from purser.errors import ConfigError


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, value))


@dataclass(slots=True)
class AllocationContext:
    now: float
    request: AcquireRequest
    weights: Weights
    affinity_scores: dict[str, float] | None = None
    group_priority: dict[str, int] | None = None
    top_k: int = 8
    rng: random.Random = field(default_factory=random.Random, repr=False)


class Allocator(ABC):
    """Pick one resource from the eligible candidate list."""

    name: str = "base"

    @abstractmethod
    def rank(self, candidates: list[Resource], ctx: AllocationContext) -> list[tuple[Resource, float]]:
        """Return candidates sorted best-first with their raw scores."""

    def rank_and_pick(
        self, candidates: list[Resource], ctx: AllocationContext
    ) -> tuple[list[tuple[Resource, float]], Resource | None]:
        """Rank once and pick from that ranking (hot-path API).

        Custom allocators that only override :meth:`pick` keep their policy;
        built-ins override :meth:`pick_ranked` so scoring happens once.
        """

        ranked = self.rank(candidates, ctx)
        if type(self).pick is not Allocator.pick:
            return ranked, self.pick(candidates, ctx)
        return ranked, self.pick_ranked(ranked, ctx)

    def pick_ranked(
        self,
        ranked: list[tuple[Resource, float]],
        ctx: AllocationContext,
    ) -> Resource | None:
        if not ranked:
            return None
        return self._weighted_pick(ranked[: ctx.top_k], ctx)

    def pick(self, candidates: list[Resource], ctx: AllocationContext) -> Resource | None:
        return self.pick_ranked(self.rank(candidates, ctx), ctx)

    @staticmethod
    def _weighted_pick(ranked: list[tuple[Resource, float]], ctx: AllocationContext) -> Resource:
        """Softmax-weighted random over the top-K (anti-thundering-herd)."""

        scores = [max(s, 1e-9) for _, s in ranked]
        mx = max(scores)
        exp = [math.exp((s - mx) * 4.0) for s in scores]
        total = sum(exp)
        r = ctx.rng.random() * total
        acc = 0.0
        for (res, _), e in zip(ranked, exp, strict=True):
            acc += e
            if r <= acc:
                return res
        return ranked[-1][0]


class HealthWeightedAllocator(Allocator):
    """Default: health.score up, longer-idle up (LRU rotation), load down,
    preference and affinity as soft boosts, plus jitter."""

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
            pref = cast(ResourceMeta, res.meta).preference
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

    def pick_ranked(
        self,
        ranked: list[tuple[Resource, float]],
        ctx: AllocationContext,
    ) -> Resource | None:
        if not ranked:
            return None
        self._cursor = (self._cursor + 1) % len(ranked)
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
            meta = cast(ResourceMeta, res.meta)
            if ctx.group_priority and meta.group:
                return float(ctx.group_priority.get(meta.group, 0))
            return float(meta.priority)

        return sorted(
            ((res, priority(res)) for res in candidates),
            key=lambda item: item[1],
            reverse=True,
        )

    def pick_ranked(
        self,
        ranked: list[tuple[Resource, float]],
        ctx: AllocationContext,
    ) -> Resource | None:
        if not ranked:
            return None
        # Never cross a priority tier.  Ties inside the best tier use the same
        # softmax as the other allocators to avoid deterministic starvation.
        best = ranked[0][1]
        tied = [(res, score) for res, score in ranked if score == best]
        return self._weighted_pick(tied[: ctx.top_k], ctx)


class RandomAllocator(Allocator):
    """Uniform pick across every eligible candidate (ignores scores)."""

    name = "random"

    def rank(self, candidates: list[Resource], ctx: AllocationContext) -> list[tuple[Resource, float]]:
        pool = list(candidates)
        ctx.rng.shuffle(pool)
        return [(res, 0.0) for res in pool]

    def pick_ranked(
        self,
        ranked: list[tuple[Resource, float]],
        ctx: AllocationContext,
    ) -> Resource | None:
        # ``rank`` already shuffled; the first entry is a uniform draw.
        return ranked[0][0] if ranked else None


ALLOCATORS: dict[str, type[Allocator]] = {
    cls.name: cls
    for cls in (
        HealthWeightedAllocator,
        RoundRobinAllocator,
        LeastLoadedAllocator,
        StaticPriorityAllocator,
        RandomAllocator,
    )
}


def register_allocator(cls: type[Allocator]) -> type[Allocator]:
    """Register a custom allocator class under its ``name`` attribute.

    Works for the ``@register_allocator`` decorator form or a direct call; the
    registered name is what ``PoolConfig(allocator=...)`` accepts.
    """

    if not isinstance(cls, type) or not issubclass(cls, Allocator):
        raise ConfigError("register_allocator expects an Allocator subclass")
    name = getattr(cls, "name", "")
    if not name or name == Allocator.name:
        raise ConfigError("custom allocator must declare a non-empty class name")
    existing = ALLOCATORS.get(name)
    if existing is not None:
        raise ConfigError(
            f"duplicate allocator name {name!r}: "
            f"{'builtin' if existing.__module__.startswith('purser.') else 'custom'} "
            "allocator already registered"
        )
    ALLOCATORS[name] = cls
    register_allocator_name(name)
    return cls


def allocator_names() -> list[str]:
    return sorted(ALLOCATORS)


def make_allocator(name: str) -> Allocator:
    try:
        return ALLOCATORS[name]()
    except KeyError:
        raise ConfigError(f"unknown allocator {name!r}") from None


__all__ = [
    "ALLOCATORS",
    "AllocationContext",
    "Allocator",
    "HealthWeightedAllocator",
    "LeastLoadedAllocator",
    "RandomAllocator",
    "RoundRobinAllocator",
    "StaticPriorityAllocator",
    "allocator_names",
    "make_allocator",
    "register_allocator",
]
