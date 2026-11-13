"""End-to-end pool behaviour; the same suite runs on memory and redis."""

from __future__ import annotations

import asyncio
import time

import pytest
from conftest import FakeChecker, seed

from purser.adapters.account import AccountResource
from purser.adapters.proxy import ProxyResource
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
    ConfigError,
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
        assert lease.resource is not None
        assert lease.resource.id == rid
        assert lease.resource.payload == lease.payload
    res = await pool.get(rid)
    assert res.state is ResourceState.IDLE


async def test_counts_by_name_matches_stats(make_pool):
    pool = make_pool()
    await seed(pool, n=2)
    enum_counts = await pool.counts()
    named = await pool.counts(by_name=True)
    assert named == {state.value: n for state, n in enum_counts.items()}
    assert named == (await pool.stats())["counts"]


async def test_iter_resources_streams_filters_and_hydrates(make_pool):
    pool = make_pool()
    await seed(pool, n=2, tags=["http"])
    await pool.import_resources(
        [{"id": "eu-1", "host": "9.9.9.9", "port": 1, "tags": ["https"]}],
        import_validate="on_import",
    )

    assert [r.id async for r in pool.iter_resources()] == ["eu-1", "r-0", "r-1"]
    assert [r.id async for r in pool.iter_resources(tags=["http"])] == ["r-0", "r-1"]
    assert [r.id async for r in pool.iter_resources(tags=frozenset())] == [
        "eu-1",
        "r-0",
        "r-1",
    ]
    assert [r.id async for r in pool.iter_resources(search="eu")] == ["eu-1"]

    lease = await pool.acquire(AcquireRequest(consumer="c", resource_id="r-0"))
    idle = [r.id async for r in pool.iter_resources(states={ResourceState.IDLE})]
    assert idle == ["eu-1", "r-1"]
    await pool.release(lease)

    streamed = [r async for r in pool.iter_resources(batch_size=1)]
    assert streamed[0].payload["host"] == "9.9.9.9"
    assert streamed[0].kind == "fakecheck"

    with pytest.raises(ConfigError):
        _ = [r async for r in pool.iter_resources(batch_size=0)]
    with pytest.raises(TypeError):
        _ = [r async for r in pool.iter_resources(tags="http")]


async def test_admin_methods_default_operator_but_require_reason(purser, make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)

    await pool.suspend(rid, reason="maintenance")
    await pool.resume(rid)
    await pool.drain(rid, reason="retired")

    operators = {entry.action: entry.operator for entry in purser.audit.entries()}
    assert operators["suspend"] == "system"
    assert operators["resume"] == "system"
    assert operators["drain"] == "system"

    with pytest.raises(TypeError):
        await pool.ban(rid)


async def test_leases_query_lists_active_leases(make_pool):
    pool = make_pool()
    rids = await seed(pool, n=2)
    assert await pool.leases() == []

    first = await pool.acquire(AcquireRequest(consumer="a", resource_id=rids[0]))
    second = await pool.acquire(AcquireRequest(consumer="b"))
    assert {lease.token for lease in await pool.leases()} == {
        first.token,
        second.token,
    }
    assert [lease.token for lease in await pool.leases(rids[0])] == [first.token]

    res = await pool.get(rids[0])
    assert res is not None
    assert "leases" not in pool.describe(res)

    await pool.release(first)
    assert await pool.leases(rids[0]) == []
    await pool.release(second)


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


async def test_import_on_import_records_validation(make_pool):
    pool = make_pool()
    report = await pool.import_resources(
        [
            {"id": "v-1", "host": "127.0.0.1", "port": 1},
            {"id": "v-2", "host": "127.0.0.1", "port": 2},
        ],
        import_validate="on_import",
    )
    assert {entry["id"] for entry in report.validation} == {"v-1", "v-2"}
    assert all(entry["alive"] is True for entry in report.validation)
    assert all(entry["action"] == "ok" for entry in report.validation)
    assert all(entry["latency_ms"] == 10.0 for entry in report.validation)
    assert report.to_dict()["validation"] == report.validation

    plain = await pool.import_resources([{"id": "v-3", "host": "127.0.0.1", "port": 3}])
    assert plain.validation == []


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

    purser.register_adapter(Guarded())
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


async def test_proxy_location_fields_are_optional_payload(purser):
    pool = purser.create_pool("proxy-locations", "proxy")
    report = await pool.import_resources(
        [
            {
                "id": "p1",
                "host": "1.2.3.4",
                "port": 8080,
                "country": "US",
                "province": "CA",
                "city": "Los Angeles",
                "district": "Hollywood",
            },
            {"id": "p2", "host": "5.6.7.8", "port": 3128},
        ]
    )
    assert report.imported == 2
    p1 = await pool.get("p1")
    assert p1 is not None
    assert p1.payload["country"] == "US"
    assert p1.payload["province"] == "CA"
    assert p1.payload["city"] == "Los Angeles"
    assert p1.payload["district"] == "Hollywood"
    p2 = await pool.get("p2")
    assert p2 is not None
    assert "country" not in p2.payload


async def test_import_accepts_resource_objects(make_pool):
    pool = make_pool()
    built = Resource(
        id="built-1",
        kind="fakecheck",
        payload={"host": "10.0.0.1", "port": 8080},
        tags=frozenset({"US"}),
    )
    report = await pool.import_resources(
        [built, {"id": "dict-1", "host": "10.0.0.2", "port": 8081}]
    )
    assert report.imported == 2
    stored = await pool.get("built-1")
    assert stored is not None
    assert stored.payload == {"host": "10.0.0.1", "port": 8080}
    assert stored.tags == frozenset({"US"})
    assert await pool.get("dict-1") is not None


async def test_import_rejects_resource_kind_mismatch(make_pool):
    pool = make_pool()
    report = await pool.import_resources(
        [Resource(id="wrong", kind="proxy", payload={"host": "1.1.1.1", "port": 1})]
    )
    assert report.imported == 0
    assert len(report.errors) == 1
    assert await pool.get("wrong") is None


async def test_import_proxy_resource_is_adopted_without_mutating_input(purser):
    pool = purser.create_pool("proxy-adopt", "proxy")
    source = Resource(
        id="rp-1",
        kind="proxy",
        payload={
            "host": "1.2.3.4",
            "port": "8080",
            "country": "US",
            "custom": "kept",
        },
    )
    report = await pool.import_resources([source])
    assert report.imported == 1
    stored = await pool.get("rp-1")
    assert stored is not None
    assert stored.payload["port"] == 8080
    assert stored.payload["protocol"] == "http"
    assert stored.payload["country"] == "US"
    assert stored.payload["custom"] == "kept"
    assert source.payload["port"] == "8080"
    assert "protocol" not in source.payload


async def test_import_resource_uses_dedup_key_from_payload(make_pool):
    pool = make_pool()
    report = await pool.import_resources(
        [Resource(id="ignored", kind="fakecheck", payload={"host": "h1", "port": 1})],
        dedup_key="host",
    )
    assert report.imported == 1
    assert await pool.get("h1") is not None
    assert await pool.get("ignored") is None


async def test_generate_accepts_resource_objects(make_pool):
    pool = make_pool()
    report = await pool.generate(
        [Resource(id="gen-1", kind="fakecheck", payload={"host": "1.1.1.1", "port": 1})]
    )
    assert report.imported == 1
    assert await pool.get("gen-1") is not None


async def test_import_accepts_proxy_resource_array(purser):
    pool = purser.create_pool("proxy-typed", "proxy")
    typed = ProxyResource(
        id="typed-1",
        payload={"host": "1.2.3.4", "port": "8080"},
        tags=frozenset({"US"}),
        country="US",
        province="CA",
        city="Los Angeles",
        district="Hollywood",
    )
    inferred = ProxyResource(
        id="typed-2",
        payload={"host": "5.6.7.8", "port": 3128, "city": "Tokyo"},
        country="JP",
    )
    report = await pool.import_resources([typed, inferred])
    assert report.imported == 2

    stored = await pool.get("typed-1")
    assert stored is not None
    assert stored.payload["port"] == 8080
    assert stored.payload["protocol"] == "http"
    assert stored.payload["country"] == "US"
    assert stored.payload["province"] == "CA"
    assert stored.payload["city"] == "Los Angeles"
    assert stored.payload["district"] == "Hollywood"
    assert stored.tags == frozenset({"US"})

    second = await pool.get("typed-2")
    assert second is not None
    assert second.payload["country"] == "JP"
    assert second.payload["city"] == "Tokyo"

    # Construction already canonicalizes typed fields; import deep-copies and
    # adopts without touching the caller's object.
    assert typed.payload["port"] == 8080
    assert typed.port == 8080
    assert inferred.city == "Tokyo"


async def test_proxy_resource_location_syncs_on_adopt(purser):
    pool = purser.create_pool("proxy-typed-mutation", "proxy")
    typed = ProxyResource(id="typed-3", payload={"host": "1.2.3.4", "port": 8080})
    typed.country = "JP"
    typed.city = "Osaka"
    report = await pool.import_resources([typed])
    assert report.imported == 1
    stored = await pool.get("typed-3")
    assert stored is not None
    assert stored.payload["country"] == "JP"
    assert stored.payload["city"] == "Osaka"


async def test_proxy_generate_returns_typed_resource(purser):
    pool = purser.create_pool("proxy-generate-typed", "proxy")
    resource = await pool.adapter.generate(
        {
            "host": "1.2.3.4",
            "port": 8080,
            "country": "US",
            "province": "CA",
            "city": "Los Angeles",
            "district": "Hollywood",
        }
    )
    assert isinstance(resource, ProxyResource)
    assert resource.kind == "proxy"
    assert resource.country == "US"
    assert resource.province == "CA"
    assert resource.payload["country"] == "US"
    assert resource.payload["city"] == "Los Angeles"


async def test_proxy_resource_typed_fields_mirror_payload(purser):
    pool = purser.create_pool("proxy-typed-fields", "proxy")
    report = await pool.import_resources(
        [
            {
                "id": "tf-1",
                "host": "1.2.3.4",
                "port": "8080",
                "protocol": "socks5",
                "username": "user",
                "password": "pass",
                "country": "US",
            }
        ]
    )
    assert report.imported == 1
    stored = await pool.get("tf-1")
    assert isinstance(stored, ProxyResource)
    assert stored.host == "1.2.3.4"
    assert stored.port == 8080
    assert stored.protocol == "socks5"
    assert stored.username == "user"
    assert stored.password == "pass"
    assert stored.country == "US"
    for name in ("host", "port", "protocol", "username", "password", "country"):
        assert stored.payload[name] == getattr(stored, name)


async def test_reads_hydrate_generic_proxy_resource(purser):
    pool = purser.create_pool("proxy-hydrate", "proxy")
    report = await pool.import_resources(
        [
            Resource(
                id="gh-1",
                kind="proxy",
                payload={"host": "9.9.9.9", "port": 1080, "city": "Paris"},
            )
        ]
    )
    assert report.imported == 1
    found = await pool.get("gh-1")
    required = await pool.require("gh-1")
    [queried] = await pool.query()
    for resource in (found, required, queried):
        assert isinstance(resource, ProxyResource)
        assert resource.host == "9.9.9.9"
        assert resource.port == 1080
        assert resource.city == "Paris"
        assert resource.payload["host"] == "9.9.9.9"


async def test_account_resource_typed_fields_and_hydrate(purser):
    pool = purser.create_pool("account-typed-fields", "account")
    report = await pool.import_resources(
        [
            {
                "id": "ac-1",
                "username": "alice",
                "password": "secret",
                "cookie": "session=1",
            },
            Resource(
                id="ac-2",
                kind="account",
                payload={"username": "bob", "access_token": "token"},
            ),
        ]
    )
    assert report.imported == 2

    first = await pool.get("ac-1")
    assert isinstance(first, AccountResource)
    assert first.username == "alice"
    assert first.password == "secret"
    assert first.cookie == "session=1"
    assert first.payload["password"] == "secret"

    second = await pool.get("ac-2")
    assert isinstance(second, AccountResource)
    assert second.username == "bob"
    assert second.access_token == "token"


def test_payload_property_merges_typed_fields():
    resource = ProxyResource(
        id="derive-1",
        payload={"host": "1.2.3.4", "port": 8080, "custom": "kept"},
    )
    assert resource.host == "1.2.3.4"
    assert resource.payload == {
        "host": "1.2.3.4",
        "port": 8080,
        "custom": "kept",
    }

    # Typed attributes are the write API and show up without any resync.
    resource.host = "5.6.7.8"
    resource.city = "Tokyo"
    assert resource.payload["host"] == "5.6.7.8"
    assert resource.payload["city"] == "Tokyo"

    # Whole-payload assignment replaces the typed view as well.
    resource.payload = {"host": "9.9.9.9", "note": "replaced"}
    assert resource.host == "9.9.9.9"
    assert resource.port is None
    assert resource.city is None
    assert resource.payload == {"host": "9.9.9.9", "note": "replaced"}


def test_payload_defaults_to_empty_when_omitted():
    assert Resource(id="r", kind="k").payload == {}
    assert ProxyResource(id="p").payload == {}
    assert AccountResource(id="a").payload == {}


async def test_acquire_does_not_mutate_reused_request(make_pool):
    pool = make_pool()
    await seed(pool, n=2)
    req = AcquireRequest(consumer="c")
    first = await pool.acquire(req)
    second = await pool.acquire(req)
    assert req.trace_id is None and req.timeout is None
    assert first.trace_id != second.trace_id
    await pool.release(first)
    await pool.release(second)


async def test_acquire_kwargs_stay_per_call(make_pool):
    pool = make_pool()
    await seed(pool, n=1)
    req = AcquireRequest(consumer="c")
    lease = await pool.acquire(req, timeout=5.0)
    assert req.timeout is None
    assert lease.trace_id is not None
    await pool.release(lease)


async def test_import_default_leaves_new_and_acquire_explains(make_pool):
    pool = make_pool()
    await pool.import_resources([{"id": "new-1", "host": "127.0.0.1", "port": 1}])
    assert (await pool.counts())[ResourceState.NEW] == 1

    started = time.perf_counter()
    with pytest.raises(PoolExhausted, match="all are in NEW"):
        await pool.acquire(AcquireRequest(consumer="c"))
    assert time.perf_counter() - started < 1.0


async def test_stats_health_ignores_query_pagination(make_pool, monkeypatch):
    pool = make_pool()
    await seed(pool, n=3)

    async def forbidden(*args, **kwargs):
        raise AssertionError("stats must not page through resources")

    monkeypatch.setattr(pool, "query", forbidden)
    stats = await pool.stats()
    assert stats["avg_health"] is not None


async def test_update_resource_payload_updates_typed_fields(purser):
    pool = purser.create_pool("proxy-payload-update", "proxy")
    await pool.import_resources(
        [{"id": "pu-1", "host": "1.1.1.1", "port": 8080, "note": "old"}]
    )
    updated = await pool.update_resource(
        "pu-1",
        payload={"host": "2.2.2.2", "port": 3128, "note": "new"},
        operator="tester",
        reason="payload update",
    )
    assert isinstance(updated, ProxyResource)
    assert updated.host == "2.2.2.2"
    assert updated.port == 3128
    # Reads normalize defaults, so the effective protocol is materialized.
    assert updated.protocol == "http"
    assert updated.payload == {
        "host": "2.2.2.2",
        "port": 3128,
        "protocol": "http",
        "note": "new",
    }
