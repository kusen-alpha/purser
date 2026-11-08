"""Contract suites for the two planes: storage (S10) and coordination (F2).

Storage tests run across memory / sqlite; coordination tests across
memory / redis.  The pursuit matrix (storage x coordination) itself is
exercised by the pool-level behavioural suites in test_pool / test_reaper.
"""

from __future__ import annotations

import json
import time

import pytest

from purser.backends.base import ResourcePatch
from purser.backends.coordination import _encode_lease
from purser.config import PoolConfig
from purser.core.resource import Concurrency, HealthStats, Lease, Resource
from purser.core.state import ResourceState

POOL = "p0"


def make_resource(rid: str, *, tags=(), max_slots: int = 1, state: ResourceState = ResourceState.IDLE) -> Resource:
    return Resource(
        id=rid,
        kind="fakecheck",
        payload={"host": "127.0.0.1", "port": 8080},
        state=state,
        tags=frozenset(tags),
        concurrency=Concurrency(max_slots=max_slots),
        health=HealthStats(score=0.9, samples=3),
        meta={"created_at": time.time()},
    )


@pytest.fixture
def storage(purser, storage_kind):
    return purser._make_storage(PoolConfig(kind="fakecheck", storage=storage_kind))


@pytest.fixture
def coordination(purser, coordination_kind):
    return purser._make_coordination(PoolConfig(kind="fakecheck", coordination=coordination_kind))


# -- storage plane ---------------------------------------------------------


@pytest.mark.asyncio
async def test_put_get_roundtrip(storage):
    await storage.put(POOL, make_resource("a", tags={"us"}))
    loaded = await storage.get(POOL, "a")
    assert loaded is not None
    assert loaded.id == "a" and loaded.state is ResourceState.IDLE
    assert loaded.concurrency.max_slots == 1
    assert loaded.health.score == 0.9
    assert loaded.tags == frozenset({"us"})
    assert loaded.version == 1


@pytest.mark.asyncio
async def test_put_bumps_version_and_syncs_tags(storage):
    await storage.put(POOL, make_resource("a", tags={"us"}))
    loaded = await storage.get(POOL, "a")
    assert loaded.version == 1
    await storage.put(POOL, make_resource("a", tags={"eu"}))
    loaded = await storage.get(POOL, "a")
    assert loaded.version == 2 and loaded.tags == frozenset({"eu"})
    assert await storage.query(POOL, tags=frozenset({"us"})) == []
    assert {r.id for r in await storage.query(POOL, tags=frozenset({"eu"}))} == {"a"}


@pytest.mark.asyncio
async def test_query_counts_delete(storage):
    await storage.put(POOL, make_resource("a", tags={"us"}))
    await storage.put(POOL, make_resource("b", tags={"eu"}))
    await storage.put(POOL, make_resource("c", tags={"us"}))
    by_state = await storage.query(POOL, states={ResourceState.IDLE})
    assert {r.id for r in by_state} == {"a", "b", "c"}
    by_tag = await storage.query(POOL, tags=frozenset({"us"}))
    assert {r.id for r in by_tag} == {"a", "c"}
    counts = await storage.counts(POOL)
    assert counts[ResourceState.IDLE] == 3
    assert counts[ResourceState.ARCHIVED] == 0
    assert await storage.delete(POOL, "a") is True
    assert await storage.get(POOL, "a") is None
    assert (await storage.counts(POOL))[ResourceState.IDLE] == 2


@pytest.mark.asyncio
async def test_query_paging_and_and_tags(storage):
    for i in range(6):
        tag = "us" if i % 2 == 0 else "eu"
        await storage.put(POOL, make_resource(f"r{i}", tags={tag}))
    page = await storage.query(
        POOL, states={ResourceState.IDLE}, tags=frozenset({"us"}), limit=2, offset=0
    )
    assert [r.id for r in page] == ["r0", "r2"]
    page2 = await storage.query(
        POOL, states={ResourceState.IDLE}, tags=frozenset({"us"}), limit=2, offset=2
    )
    assert [r.id for r in page2] == ["r4"]
    await storage.put(POOL, make_resource("multi", tags={"us", "eu"}))
    assert {r.id for r in await storage.query(POOL, tags=frozenset({"us", "eu"}))} == {"multi"}


@pytest.mark.asyncio
async def test_transition_acquire_release_semantics(storage):
    await storage.put(POOL, make_resource("a"))
    result = await storage.transition_acquire(POOL, "a", 1)
    assert result.ok and result.state is ResourceState.FULL
    res = await storage.get(POOL, "a")
    assert res is not None and res.state is ResourceState.FULL
    assert res.concurrency.used_slots == 1
    # Over-commit rejected.
    assert not (await storage.transition_acquire(POOL, "a", 1)).ok
    good = await storage.transition_release(POOL, "a", 1)
    assert good.ok and good.state is ResourceState.IDLE
    res = await storage.get(POOL, "a")
    assert res is not None and res.concurrency.used_slots == 0
    # Double release rejected.
    assert not (await storage.transition_release(POOL, "a", 1)).ok


@pytest.mark.asyncio
async def test_transition_shared_never_oversells(storage):
    await storage.put(POOL, make_resource("a", max_slots=2))
    assert (await storage.transition_acquire(POOL, "a", 1)).ok
    assert (await storage.transition_acquire(POOL, "a", 1)).ok
    assert not (await storage.transition_acquire(POOL, "a", 1)).ok
    res = await storage.get(POOL, "a")
    assert res is not None and res.state is ResourceState.FULL
    await storage.transition_release(POOL, "a", 1)
    res = await storage.get(POOL, "a")
    assert res is not None and res.state is ResourceState.ACTIVE


@pytest.mark.asyncio
async def test_transition_draining_archives(storage):
    await storage.put(POOL, make_resource("a"))
    await storage.transition_acquire(POOL, "a", 1)
    res = await storage.get(POOL, "a")
    await storage.set_state(
        POOL, "a", res.version, expect_state=res.state,
        patch=ResourcePatch(state=ResourceState.DRAINING),
    )
    result = await storage.transition_release(POOL, "a", 1)
    assert result.ok and result.state is ResourceState.ARCHIVED


@pytest.mark.asyncio
async def test_reset_usage_reconciliation(storage):
    await storage.put(POOL, make_resource("a", max_slots=2))
    await storage.transition_acquire(POOL, "a", 2)
    res = await storage.get(POOL, "a")
    assert res is not None and res.concurrency.used_slots == 2
    assert await storage.reset_usage(POOL, "a") is True
    res = await storage.get(POOL, "a")
    assert res is not None and res.state is ResourceState.IDLE
    assert res.concurrency.used_slots == 0
    # Nothing to reset the second time.
    assert await storage.reset_usage(POOL, "a") is False


@pytest.mark.asyncio
async def test_set_state_cas_and_meta_merge(storage):
    await storage.put(POOL, make_resource("a"))
    res = await storage.get(POOL, "a")
    ok = await storage.set_state(
        POOL, "a", res.version, patch=ResourcePatch(meta_patch={"hello": "world"})
    )
    assert ok
    res = await storage.get(POOL, "a")
    assert res.meta["hello"] == "world" and "created_at" in res.meta
    # Stale version rejected.
    assert not await storage.set_state(
        POOL, "a", res.version - 1, patch=ResourcePatch(state=ResourceState.BANNED)
    )
    # Wrong expected state rejected.
    assert not await storage.set_state(
        POOL, "a", res.version, expect_state=ResourceState.BANNED,
        patch=ResourcePatch(state=ResourceState.SUSPENDED),
    )


@pytest.mark.asyncio
async def test_set_state_syncs_tags(storage):
    await storage.put(POOL, make_resource("a", tags={"us"}))
    res = await storage.get(POOL, "a")
    assert await storage.set_state(
        POOL, "a", res.version, patch=ResourcePatch(tags=frozenset({"eu"}))
    )
    assert await storage.query(POOL, tags=frozenset({"us"})) == []
    assert {r.id for r in await storage.query(POOL, tags=frozenset({"eu"}))} == {"a"}


@pytest.mark.asyncio
async def test_scan_all_and_filter(storage):
    await storage.put(POOL, make_resource("b"))
    await storage.put(POOL, make_resource("a"))
    await storage.put(POOL, make_resource("c", state=ResourceState.QUARANTINED))
    assert [r.id async for r in storage.scan(POOL)] == ["a", "b", "c"]
    assert [r.id async for r in storage.scan(POOL, states={ResourceState.IDLE})] == ["a", "b"]


@pytest.mark.asyncio
async def test_flush(storage):
    await storage.put(POOL, make_resource("a"))
    await storage.flush(POOL)
    assert await storage.get(POOL, "a") is None
    assert (await storage.counts(POOL))[ResourceState.IDLE] == 0


# -- coordination plane ----------------------------------------------------


@pytest.mark.asyncio
async def test_coordination_lease_lifecycle(coordination):
    lease = Lease.new(POOL, "a", consumer="c", ttl=60.0)
    await coordination.register_lease(POOL, lease)
    assert await coordination.get_lease(POOL, lease.token) is not None
    assert [lease.token for lease in await coordination.leases_for(POOL, "a")] == [lease.token]
    assert {lease.token for lease in await coordination.active(POOL)} == {lease.token}
    later = time.time() + 120
    assert await coordination.renew_lease(POOL, "a", lease.token, later) is True
    assert await coordination.renew_lease(POOL, "a", "unknown", later) is False
    held = await coordination.get_lease(POOL, lease.token)
    assert held is not None and held.expires_at == later
    unregistered = await coordination.unregister_lease(POOL, lease.token)
    assert unregistered is not None and unregistered.consumer == "c"
    assert await coordination.get_lease(POOL, lease.token) is None
    assert await coordination.leases_for(POOL, "a") == []
    assert await coordination.active(POOL) == []


def test_encoded_lease_drops_process_local_snapshot():
    lease = Lease.new(POOL, "a")
    lease.payload = {"host": "127.0.0.1"}
    lease.resource = make_resource("a")
    encoded = json.loads(_encode_lease(lease))
    assert encoded["token"] == lease.token
    assert "payload" not in encoded and "resource" not in encoded


@pytest.mark.asyncio
async def test_coordination_multiple_leases_per_resource(coordination):
    l1 = Lease.new(POOL, "a", consumer="c1", ttl=60.0)
    l2 = Lease.new(POOL, "a", consumer="c2", ttl=60.0)
    l3 = Lease.new(POOL, "b", consumer="c3", ttl=60.0)
    await coordination.register_lease(POOL, l1)
    await coordination.register_lease(POOL, l2)
    await coordination.register_lease(POOL, l3)
    assert {lease.consumer for lease in await coordination.leases_for(POOL, "a")} == {"c1", "c2"}
    assert {lease.token for lease in await coordination.active(POOL)} == {l1.token, l2.token, l3.token}
    await coordination.unregister_lease(POOL, l1.token)
    assert [lease.consumer for lease in await coordination.leases_for(POOL, "a")] == ["c2"]


@pytest.mark.asyncio
async def test_coordination_flush(coordination):
    lease = Lease.new(POOL, "a", ttl=60.0)
    await coordination.register_lease(POOL, lease)
    await coordination.flush(POOL)
    assert await coordination.active(POOL) == []
    assert await coordination.get_lease(POOL, lease.token) is None
