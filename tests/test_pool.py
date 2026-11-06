"""End-to-end pool behaviour; the same suite runs on memory and redis."""

from __future__ import annotations

import asyncio
import time

import pytest
from conftest import FakeChecker, seed

from purser.backends.base import ResourcePatch
from purser.core.resource import (
    AcquireRequest,
    CheckVerdict,
    Lease,
    Resource,
    UsageOutcome,
)
from purser.core.state import ResourceState
from purser.errors import (
    NoSuitableResource,
    PoolEmpty,
    PoolExhausted,
    ResourceBusy,
)


async def test_lifecycle_roundtrip(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    res = await pool.get(rid)
    assert res.state is ResourceState.IDLE

    lease = await pool.acquire(AcquireRequest(consumer="c"))
    res = await pool.get(lease.resource_id)
    assert res.state is ResourceState.FULL and res.concurrency.used_slots == 1

    await pool.release(lease, outcome=UsageOutcome.OK)
    res = await pool.get(rid)
    assert res.state is ResourceState.IDLE and res.concurrency.used_slots == 0


async def test_lease_context_manager_auto_release(make_pool):
    pool = make_pool()
    rid = (await seed(pool, n=1))[0]
    async with pool.lease(AcquireRequest(consumer="ctx")) as lease:
        assert lease.resource_id == rid
        assert lease.payload == {"host": "127.0.0.1", "port": 8000}
    res = await pool.get(rid)
    assert res.state is ResourceState.IDLE


async def test_tags_filter(make_pool):
    pool = make_pool()
    await seed(pool, n=2, tags=["US", "http"])
    await pool.import_resources(
        [{"id": "eu-0", "host": "9.9.9.9", "port": 1, "tags": ["EU", "http"]}],
        import_validate="on_import",
    )
    lease = await pool.acquire(AcquireRequest(tags=frozenset({"EU"})))
    assert lease.resource_id == "eu-0"
    with pytest.raises(PoolExhausted):
        await pool.acquire(AcquireRequest(tags=frozenset({"ASIA"}), timeout=0))


async def test_waiter_is_woken_by_release(make_pool):
    pool = make_pool()
    await seed(pool, n=1)
    first = await pool.acquire(AcquireRequest(consumer="a"))
    acquired: list[Lease] = []

    async def waiter() -> None:
        lease = await pool.acquire(AcquireRequest(consumer="b", timeout=5.0))
        acquired.append(lease)

    task = asyncio.create_task(waiter())
    await asyncio.sleep(0.05)
    assert not acquired
    await pool.release(first)
    await asyncio.wait_for(task, 5.0)
    assert len(acquired) == 1


async def test_wait_timeout_raises_pool_exhausted(make_pool):
    pool = make_pool()
    await seed(pool, n=1)
    await pool.acquire(AcquireRequest(consumer="a"))
    with pytest.raises(PoolExhausted):
        await pool.acquire(AcquireRequest(consumer="b", timeout=0.1))


async def test_empty_pool_raises_immediately(make_pool):
    pool = make_pool()
    with pytest.raises(PoolEmpty):
        await pool.acquire(AcquireRequest(timeout=5.0))


async def test_directed_acquire_and_busy_timeout(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(resource_id=rid, consumer="a"))
    assert lease.resource_id == rid
    with pytest.raises(ResourceBusy):
        await pool.acquire(AcquireRequest(resource_id=rid, consumer="b", timeout=0.1))
    await pool.release(lease)
    again = await pool.acquire(AcquireRequest(resource_id=rid, consumer="b"))
    assert again.resource_id == rid


async def test_duplicate_hold_returns_existing_lease(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    first = await pool.acquire(AcquireRequest(consumer="dup", resource_id=rid))
    second = await pool.acquire(AcquireRequest(consumer="dup", resource_id=rid))
    assert first.token == second.token
    await pool.release(first)
    third = await pool.acquire(AcquireRequest(consumer="dup", resource_id=rid))
    assert third.token != first.token


async def test_shared_concurrency(make_pool):
    pool = make_pool(max_slots=3)
    [rid] = await seed(pool, n=1)
    l1 = await pool.acquire(AcquireRequest())
    l2 = await pool.acquire(AcquireRequest())
    l3 = await pool.acquire(AcquireRequest())
    assert {lease.resource_id for lease in (l1, l2, l3)} == {rid}
    with pytest.raises(PoolExhausted):
        await pool.acquire(AcquireRequest(timeout=0))
    res = await pool.get(rid)
    assert res.state is ResourceState.FULL
    await pool.release(l1)
    res = await pool.get(rid)
    assert res.state is ResourceState.ACTIVE and res.concurrency.used_slots == 2
    await pool.release(l2)
    await pool.release(l3)
    res = await pool.get(rid)
    assert res.state is ResourceState.IDLE


async def test_acquire_many_atomic_and_best_effort(make_pool):
    pool = make_pool()
    await seed(pool, n=2)
    batch = await pool.acquire_many(2, mode="atomic")
    assert len(batch) == 2
    await batch.release_all(pool)
    # With both resources free again, a 3-lease atomic acquire fails and
    # rolls back its partial acquisitions (no leak).
    with pytest.raises(PoolExhausted):
        await pool.acquire_many(3, mode="atomic", timeout=0.1)
    probe = await pool.acquire()
    await pool.release(probe)
    partial = await pool.acquire_many(2, mode="best_effort", n_min=1)
    assert 1 <= len(partial) <= 2
    await partial.release_all(pool)
    await pool.acquire_many(2, mode="best_effort")
    with pytest.raises(PoolExhausted):
        await pool.acquire_many(2, mode="best_effort", n_min=2, timeout=0.1)


async def test_cooldown_after_release(make_pool):
    pool = make_pool(cooldown_after_release=60.0)
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    await pool.release(lease)
    res = await pool.get(rid)
    assert res.next_available_at is not None and res.next_available_at > time.time()
    with pytest.raises(PoolExhausted):
        await pool.acquire(AcquireRequest(consumer="c", timeout=0))
    again = await pool.acquire(AcquireRequest(consumer="c", ignore_cooldown=True))
    assert again.resource_id == rid


async def test_usage_quota_metering(make_pool):
    pool = make_pool(usage_quota=2)
    [rid] = await seed(pool, n=1)
    for _ in range(2):
        lease = await pool.acquire(AcquireRequest(consumer="c"))
        await pool.release(lease, used=1)
    res = await pool.get(rid)
    assert res.state is ResourceState.QUOTA_EXHAUSTED and res.used_count == 2
    with pytest.raises(PoolExhausted):
        await pool.acquire(AcquireRequest(timeout=0))
    await pool.top_up(rid, operator="ops", amount=2)
    res = await pool.get(rid)
    assert res.state is ResourceState.IDLE and res.used_count == 0
    lease = await pool.acquire()
    await pool.release(lease)


async def test_ban_unban_cycle(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    await pool.ban(rid, operator="ops", reason="bad behaviour")
    assert (await pool.get(rid)).state is ResourceState.BANNED
    with pytest.raises(PoolExhausted):
        await pool.acquire(AcquireRequest(timeout=0))
    await pool.unban(rid, operator="ops", reason="reviewed")
    assert (await pool.get(rid)).state is ResourceState.IDLE
    lease = await pool.acquire()
    assert lease.resource_id == rid


async def test_suspend_resume(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    await pool.suspend(rid, operator="ops", reason="maintenance")
    assert (await pool.get(rid)).state is ResourceState.SUSPENDED
    with pytest.raises(PoolExhausted):
        await pool.acquire(AcquireRequest(timeout=0))
    await pool.resume(rid, operator="ops")
    assert (await pool.get(rid)).state is ResourceState.IDLE


async def test_drain_archives_after_release(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire()
    await pool.drain(rid, operator="ops", reason="retiring")
    await pool.release(lease)
    res = await pool.get(rid)
    assert res.state is ResourceState.ARCHIVED
    with pytest.raises(PoolExhausted):
        await pool.acquire(AcquireRequest(timeout=0))
    await pool.restore(rid, operator="ops")
    assert (await pool.get(rid)).state is ResourceState.IDLE


async def test_force_revoke(make_pool):
    pool = make_pool()
    await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="victim"))
    revoked = await pool.force_revoke(
        lease.resource_id, operator="admin", reason="emergency"
    )
    assert revoked.token == lease.token
    res = await pool.get(lease.resource_id)
    assert res.state is ResourceState.IDLE
    assert "cooldown:victim" in res.meta
    with pytest.raises(PoolExhausted):
        await pool.acquire(AcquireRequest(consumer="victim", timeout=0))
    other = await pool.acquire(AcquireRequest(consumer="bystander"))
    assert other.resource_id == lease.resource_id


async def test_import_dedup_report(make_pool):
    pool = make_pool()
    first = await pool.import_resources([{"id": "x", "host": "1.1.1.1", "port": 1}])
    assert first.imported == 1
    second = await pool.import_resources([{"id": "x", "host": "2.2.2.2", "port": 2}])
    assert second.updated == 1 and second.imported == 0
    bad = await pool.import_resources([{"id": "y", "port": "not-a-port"}], fail_fast=False)
    assert len(bad.errors) == 1
    assert bad.skipped == 0


async def test_quarantine_gc_releases_after_cooloff(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    await pool._on_validation_fail(rid, "flaky", operator="system")
    res = await pool.get(rid)
    assert res.state is ResourceState.QUARANTINED
    assert res.meta["quarantine_until"] > time.time()
    res.meta["quarantine_until"] = time.time() - 1
    await pool.storage.set_state(
        pool.name,
        rid,
        res.version,
        expect_state=res.state,
        patch=ResourcePatch(meta_patch={**res.meta, "quarantine_until": time.time() - 1}),
    )
    await pool.run_maintenance("quarantine_gc")
    assert (await pool.get(rid)).state is ResourceState.IDLE


async def test_periodic_unban_and_quota_reset(make_pool):
    pool = make_pool(unban_mode="auto_after_ttl", unban_after=0.0)
    [rid] = await seed(pool, n=1)
    for _ in range(3):
        await pool._on_validation_fail(rid, "bad", operator="system")
    assert (await pool.get(rid)).state is ResourceState.BANNED
    await pool.run_maintenance("periodic_unban")
    assert (await pool.get(rid)).state is ResourceState.IDLE

    qpool = make_pool("q", usage_quota=1)
    rid2 = (await seed(qpool, n=1))[0]
    lease = await qpool.acquire()
    await qpool.release(lease, used=1)
    assert (await qpool.get(rid2)).state is ResourceState.QUOTA_EXHAUSTED
    await qpool.run_maintenance("quota_reset")
    assert (await qpool.get(rid2)).state is ResourceState.IDLE


async def test_validating_gc_times_out(make_pool):
    pool = make_pool(validate_timeout=0.01)
    [rid] = await seed(pool, n=1)
    res = await pool.get(rid)
    await pool.storage.set_state(
        pool.name,
        rid,
        res.version,
        expect_state=res.state,
        patch=ResourcePatch(
            state=ResourceState.VALIDATING,
            meta_patch={**res.meta, "validate_started_at": time.time() - 60},
        ),
    )
    await pool.run_maintenance("validating_gc")
    assert (await pool.get(rid)).state is ResourceState.QUARANTINED


async def test_events_are_delivered(make_pool, purser):
    await purser.start()
    seen: list[str] = []
    purser.bus.subscribe(lambda event: seen.append(event.type))
    pool = make_pool()
    await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="e"))
    await pool.release(lease)
    await asyncio.sleep(0.1)
    assert "lease_acquired" in seen
    assert "lease_released" in seen
    await purser.close()


async def test_audit_hash_chain(make_pool, purser):
    pool = make_pool()
    await pool.ban((await seed(pool, n=1))[0], operator="alice", reason="x")
    entries = purser.audit.entries()
    assert len(entries) >= 2  # create_pool + ban
    assert purser.audit.verify()
    entries[1].reason = "forged"
    assert not purser.audit.verify()


async def test_request_level_check_rejection(make_pool, purser):
    class Guarded(FakeChecker):
        kind = "guarded"

        async def on_request_check(self, resource: Resource, req) -> CheckVerdict:
            if req.target == "blocked-site":
                return CheckVerdict.FAIL
            return CheckVerdict.PASS

    purser.register_type(Guarded())
    pool = purser.pool("guarded", "guarded", max_slots=2)
    records = [{"id": f"g-{i}", "host": "1.1.1.1", "port": i} for i in range(3)]
    await pool.import_resources(records, import_validate="on_import")
    with pytest.raises(NoSuitableResource):
        await pool.acquire(AcquireRequest(target="blocked-site", consumer="c"))
    lease = await pool.acquire(AcquireRequest(target="ok-site", consumer="c"))
    assert lease is not None


async def test_site_failure_index_excludes_candidates(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    res = await pool.get(rid)
    pool._record_site_failure(res.site_failures, "hot-site", time.time())
    await pool.storage.set_state(
        pool.name, rid, res.version, expect_state=res.state,
        patch=ResourcePatch(site_failures=res.site_failures),
    )
    with pytest.raises(PoolExhausted):
        await pool.acquire(AcquireRequest(target="hot-site", timeout=0))
    lease = await pool.acquire(AcquireRequest(target="cold-site"))
    assert lease.resource_id == rid


async def test_mutex_group_blocks_same_group(make_pool):
    pool = make_pool()
    await pool.import_resources(
        [
            {"id": "g-a", "host": "1.1.1.1", "port": 1, "mutex_group": "g"},
            {"id": "g-b", "host": "1.1.1.2", "port": 2, "mutex_group": "g"},
            {"id": "g-c", "host": "1.1.1.3", "port": 3, "mutex_group": "g"},
        ],
        import_validate="on_import",
    )
    first = await pool.acquire(AcquireRequest(consumer="c"))
    # The same consumer cannot hold a second member of the same group.
    with pytest.raises(PoolExhausted):
        await pool.acquire(AcquireRequest(consumer="c", timeout=0.2))
    other = await pool.acquire(AcquireRequest(consumer="other", timeout=0.2))
    assert other.resource_id != first.resource_id
    await pool.release(other)
    await pool.release(first)
    again = await pool.acquire(AcquireRequest(consumer="c", timeout=0.2))
    assert again.resource_id in {"g-a", "g-b", "g-c"}
    await pool.release(again)


async def test_reconfigure_rebuilds_runtime_knobs(make_pool):
    pool = make_pool()
    old_allocator = pool._allocator
    cfg = pool.reconfigure(allocator="round_robin", background_interval=5.0)
    assert pool.cfg.allocator == "round_robin"
    assert pool.cfg.background_interval == 5.0
    assert pool._allocator is not old_allocator
    assert pool._allocator.name == "round_robin"
    assert pool._leases.heartbeat_interval == max(1.0, cfg.lease_ttl / 3.0)


async def test_acquire_rejects_unknown_kwargs(make_pool):
    pool = make_pool()
    await seed(pool, n=1)
    with pytest.raises(TypeError):
        await pool.acquire(AcquireRequest(), bogus_arg=1)
