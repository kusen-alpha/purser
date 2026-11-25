"""Regression tests for the bounded-sampling acquire hot path (P0-1)."""

from __future__ import annotations

import random
from dataclasses import replace

import pytest

from purser.backends.base import ResourcePatch
from purser.backends.memory import MemoryStorage
from purser.config import Weights
from purser.core.allocator import (
    AllocationContext,
    Allocator,
    HealthWeightedAllocator,
    RoundRobinAllocator,
)
from purser.core.lease import LeaseManager
from purser.core.resource import (
    AcquireRequest,
    Concurrency,
    HealthStats,
    Lease,
    Resource,
)
from purser.core.state import ALLOCATABLE_STATES, ResourceState
from purser.pool.leases import LeaseService

POOL = "p"


def resource(
    rid: str,
    *,
    used: int = 0,
    max_slots: int = 1,
    tags: frozenset[str] = frozenset(),
    state: ResourceState = ResourceState.IDLE,
) -> Resource:
    return Resource(
        id=rid,
        kind="fakecheck",
        payload={},
        state=state,
        concurrency=Concurrency(max_slots=max_slots, used_slots=used),
        health=HealthStats(score=0.5, samples=3),
        tags=tags,
        meta={"created_at": 0.0, "last_used_at": 0.0},
    )


async def store_with(*resources: Resource) -> MemoryStorage:
    storage = MemoryStorage()
    for item in resources:
        await storage.put(POOL, item)
    return storage


async def sample_ids(storage: MemoryStorage, **kwargs) -> list[str]:
    states = kwargs.pop("states", set(ALLOCATABLE_STATES))
    items = await storage.sample_allocatable(POOL, states=states, **kwargs)
    return [item.id for item in items]


def alloc_ctx(top_k: int = 8) -> AllocationContext:
    return AllocationContext(
        now=1000.0,
        request=AcquireRequest(),
        weights=Weights(),
        top_k=top_k,
        rng=random.Random(7),
    )


async def test_sample_small_pool_is_exact_and_id_sorted():
    storage = await store_with(resource("r-b"), resource("r-a"), resource("r-c"))
    assert await sample_ids(storage, limit=10) == ["r-a", "r-b", "r-c"]


async def test_sample_large_pool_is_bounded_and_unique():
    storage = await store_with(*(resource(f"r-{i:03d}") for i in range(200)))
    ids = await sample_ids(storage, limit=50, rng=random.Random(3))
    assert len(ids) == 50
    assert len(set(ids)) == 50
    assert set(ids) <= {f"r-{i:03d}" for i in range(200)}


async def test_sample_respects_tag_and_required_tag():
    storage = await store_with(
        resource("eu-fast", tags=frozenset({"EU", "fast"})),
        resource("eu-slow", tags=frozenset({"EU"})),
        resource("us-fast", tags=frozenset({"US", "fast"})),
    )
    assert await sample_ids(storage, tags=frozenset({"EU"}), limit=10) == [
        "eu-fast",
        "eu-slow",
    ]
    assert await sample_ids(storage, required_tag="fast", limit=10) == [
        "eu-fast",
        "us-fast",
    ]
    assert await sample_ids(storage, tags=frozenset({"EU"}), required_tag="fast") == [
        "eu-fast"
    ]
    assert await sample_ids(storage, tags=frozenset({"APAC"})) == []


async def test_sample_respects_slots_and_states():
    storage = await store_with(
        resource("half", used=1, max_slots=2),
        resource("busy", used=1, max_slots=1, state=ResourceState.FULL),
    )
    assert await sample_ids(storage, slots=2) == []
    assert await sample_ids(storage, slots=1) == ["half"]
    assert await sample_ids(storage, states={ResourceState.ACTIVE}) == []


async def test_sample_non_allocatable_state_uses_exact_fallback():
    storage = await store_with(resource("newborn", state=ResourceState.NEW))
    assert await sample_ids(storage, states={ResourceState.NEW}) == ["newborn"]


async def test_alloc_index_tracks_acquire_release():
    storage = await store_with(resource("r-1"))
    assert storage._alloc_ids[POOL] == ["r-1"]

    acquired = await storage.transition_acquire(POOL, "r-1", 1)
    assert acquired.ok and acquired.state is ResourceState.FULL
    assert storage._alloc_ids[POOL] == []

    released = await storage.transition_release(POOL, "r-1", 1)
    assert released.ok and released.state is ResourceState.IDLE
    assert storage._alloc_ids[POOL] == ["r-1"]


async def test_alloc_index_tracks_set_state_and_reset_usage():
    storage = await store_with(resource("r-1"))
    item = await storage.get(POOL, "r-1")
    assert item is not None

    suspended = await storage.set_state(
        POOL,
        "r-1",
        item.version,
        expect_state=ResourceState.IDLE,
        patch=ResourcePatch(state=ResourceState.SUSPENDED),
    )
    assert suspended
    assert storage._alloc_ids[POOL] == []

    item = await storage.get(POOL, "r-1")
    assert item is not None
    revived = await storage.set_state(
        POOL,
        "r-1",
        item.version,
        expect_state=ResourceState.SUSPENDED,
        patch=ResourcePatch(state=ResourceState.IDLE),
    )
    assert revived
    assert storage._alloc_ids[POOL] == ["r-1"]

    assert (await storage.transition_acquire(POOL, "r-1", 1)).ok
    assert storage._alloc_ids[POOL] == []
    assert await storage.reset_usage(POOL, "r-1", 0)
    assert storage._alloc_ids[POOL] == ["r-1"]


async def test_alloc_index_tracks_put_delete_flush():
    storage = await store_with(resource("r-1"))
    await storage.put(POOL, resource("r-1", state=ResourceState.SUSPENDED))
    assert storage._alloc_ids[POOL] == []

    await storage.put(POOL, resource("r-1"))
    assert storage._alloc_ids[POOL] == ["r-1"]

    assert await storage.delete(POOL, "r-1")
    assert storage._alloc_ids[POOL] == []
    assert "r-1" not in storage._alloc_pos[POOL]

    await storage.put(POOL, resource("r-2"))
    await storage.flush(POOL)
    assert POOL not in storage._alloc_ids
    assert POOL not in storage._alloc_pos


async def test_acquire_uses_sampling_not_scan(purser, make_pool, storage_kind, monkeypatch):
    if storage_kind != "memory":
        pytest.skip("memory-only hot path")

    pool = make_pool()
    names = [f"r-{i}" for i in range(5)]
    await pool.import_resources(
        [{"id": rid, "host": "127.0.0.1", "port": 8000} for rid in names],
        import_validate="on_import",
    )
    assert isinstance(pool.storage, MemoryStorage)
    await pool.start()

    limits: list[int] = []
    real_sample = pool.storage.sample_allocatable

    async def recording_sample(*args, **kwargs):
        limits.append(kwargs["limit"])
        return await real_sample(*args, **kwargs)

    def no_scan(*args, **kwargs):
        raise AssertionError("acquire must not full-scan the storage backend")

    monkeypatch.setattr(pool.storage, "sample_allocatable", recording_sample)
    monkeypatch.setattr(pool.storage, "scan", no_scan)

    lease = await pool.acquire(AcquireRequest(consumer="c"))
    assert lease.resource_id in names
    await pool.release(lease)

    expected = max(LeaseService._CANDIDATE_BATCH, pool.cfg.top_k * 8)
    assert limits and all(limit == expected for limit in limits)


async def test_rank_and_pick_ranks_once():
    class CountingAllocator(HealthWeightedAllocator):
        calls = 0

        def rank(self, candidates, ctx):
            self.calls += 1
            return super().rank(candidates, ctx)

    allocator = CountingAllocator()
    candidates = [resource("a", used=0), resource("b", used=0)]
    ctx = alloc_ctx()
    ranked, picked = allocator.rank_and_pick(candidates, ctx)
    assert allocator.calls == 1
    assert picked is not None
    assert picked.id in {item.id for item, _ in ranked[: ctx.top_k]}


async def test_rank_and_pick_honours_custom_pick_override():
    class LastWins(Allocator):
        name = "test_last_wins"

        def rank(self, candidates, ctx):
            return [(item, 0.0) for item in candidates]

        def pick(self, candidates, ctx):
            return candidates[-1]

    candidates = [resource("a"), resource("b"), resource("c")]
    ranked, picked = LastWins().rank_and_pick(candidates, alloc_ctx())
    assert len(ranked) == 3
    assert picked is not None and picked.id == "c"


def test_round_robin_rank_and_pick_rotates():
    allocator = RoundRobinAllocator()
    candidates = [resource("a"), resource("b"), resource("c")]
    ctx = alloc_ctx()
    first = allocator.rank_and_pick(candidates, ctx)[1]
    second = allocator.rank_and_pick(candidates, ctx)[1]
    assert first is not None and second is not None
    assert first.id != second.id


async def test_lease_manager_consumer_count_is_constant_time():
    manager = LeaseManager()

    async def renew(lease: Lease) -> bool:
        return True

    first = Lease.new(POOL, "r-1", consumer="c")
    second = Lease.new(POOL, "r-2", consumer="c")
    manager.register(first, renew)
    manager.register(second, renew)
    assert manager.consumer_count("c") == 2

    manager.unregister(first.token)
    assert manager.consumer_count("c") == 1

    replacement = replace(second, consumer="other")
    manager.register(replacement, renew)
    assert manager.consumer_count("c") == 0
    assert manager.consumer_count("other") == 1
    await manager.close()
