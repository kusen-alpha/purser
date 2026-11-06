"""Allocator ranking and picking invariants."""

from __future__ import annotations

import random

from hypothesis import given, settings
from hypothesis import strategies as st

from purser.config import Weights
from purser.core.allocator import (
    AllocationContext,
    HealthWeightedAllocator,
    LeastLoadedAllocator,
    RoundRobinAllocator,
)
from purser.core.resource import AcquireRequest, Concurrency, HealthStats, Resource
from purser.core.state import ResourceState


def resource(rid: str, *, health: float = 0.5, used: int = 0, max_slots: int = 1) -> Resource:
    return Resource(
        id=rid,
        kind="fakecheck",
        payload={},
        state=ResourceState.IDLE,
        concurrency=Concurrency(max_slots=max_slots, used_slots=used),
        health=HealthStats(score=health, samples=3),
        meta={"created_at": 0.0, "last_used_at": 0.0},
    )


def ctx(req: AcquireRequest | None = None) -> AllocationContext:
    return AllocationContext(
        now=1000.0,
        request=req or AcquireRequest(),
        weights=Weights(),
        top_k=8,
        rng=random.Random(42),
    )


def test_health_weighted_prefers_healthy():
    candidates = [resource("low", health=0.1), resource("high", health=0.99)]
    ranked = HealthWeightedAllocator().rank(candidates, ctx())
    assert ranked[0][0].id == "high"


def test_health_weighted_pick_returns_candidate():
    candidates = [resource("a", health=0.7), resource("b", health=0.5)]
    picked = HealthWeightedAllocator().pick(candidates, ctx())
    assert picked.id in {"a", "b"}


def test_health_weighted_pick_empty():
    assert HealthWeightedAllocator().pick([], ctx()) is None


def test_round_robin_rotates():
    candidates = [resource("a"), resource("b"), resource("c")]
    allocator = RoundRobinAllocator()
    first = allocator.pick(candidates, ctx())
    second = allocator.pick(candidates, ctx())
    assert first is not None and second is not None
    assert first.id != second.id


def test_least_loaded_prefers_idle():
    candidates = [
        resource("busy", used=3, max_slots=5),
        resource("idle", used=0, max_slots=5),
    ]
    ranked = LeastLoadedAllocator().rank(candidates, ctx())
    assert ranked[0][0].id == "idle"


@given(
    st.lists(
        st.builds(
            resource,
            rid=st.text(min_size=1, max_size=8),
            health=st.floats(min_value=0.0, max_value=1.0),
            used=st.integers(min_value=0, max_value=4),
            max_slots=st.integers(min_value=1, max_value=4),
        ),
        min_size=0,
        max_size=20,
    )
)
@settings(max_examples=50, deadline=None)
def test_rank_preserves_set_and_pick_is_member(candidates):
    allocator = HealthWeightedAllocator()
    ranked = allocator.rank(candidates, ctx())
    assert {r.id for r, _ in ranked} == {r.id for r in candidates}
    picked = allocator.pick(candidates, ctx())
    if candidates:
        assert picked is not None and picked.id in {r.id for r in candidates}
    else:
        assert picked is None
