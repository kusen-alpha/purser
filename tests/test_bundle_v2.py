"""Bundle v2: opt-in two-phase reservations and cross-pool fairness."""

from __future__ import annotations

import asyncio
import time

import fakeredis
import pytest
from test_bundle import SimpleAdapter

from purser import (
    BundleAcquireFailed,
    BundleFairness,
    BundleQuotaExceeded,
    BundleRequest,
    BundleSlot,
    ConfigError,
    MatchRule,
    Purser,
    ResourceState,
    SyncPurser,
)
from purser.core.bundle_store import RedisBundleStore, ReservationMember
from purser.core.resource import AcquireRequest


async def _harness(*, bundle_mode="rollback", bundle_fairness=None, **kwargs):
    ps = Purser(
        maintenance_enabled=False,
        bundle_mode=bundle_mode,
        bundle_fairness=bundle_fairness,
        **kwargs,
    )
    ps.register_adapter(SimpleAdapter())
    accounts = ps.create_pool("accounts", "sim")
    proxies = ps.create_pool("proxies", "sim")
    await accounts.import_resources(
        [
            {"id": "a1", "region": "us"},
            {"id": "a2", "region": "eu"},
        ],
        import_validate="on_import",
    )
    await proxies.import_resources(
        [
            {"id": "p1", "host": "1.1.1.1", "tags": ["region:us"]},
            {"id": "p2", "host": "2.2.2.2", "tags": ["region:eu"]},
        ],
        import_validate="on_import",
    )
    await ps.start()
    return ps, accounts, proxies


def _pair(*, consumer: str = "w1", **changes):
    return BundleRequest(
        slots=(
            BundleSlot(pool="accounts"),
            BundleSlot(pool="proxies"),
        ),
        consumer=consumer,
        **changes,
    )


# -- M1: two-phase ----------------------------------------------------------


async def test_two_phase_acquire_confirm_and_release():
    ps, accounts, proxies = await _harness(
        bundle_mode="two_phase", bundle_gc_interval=3600.0
    )
    try:
        bundle = await ps.acquire_bundle(_pair())
        store = await ps._store()
        res = await store.get(bundle.bundle_id)
        assert res is not None and res.status == "committed"
        assert set(res.members) == {"accounts", "proxies"}
        await ps.release_bundle(bundle)
        assert await store.get(bundle.bundle_id) is None
    finally:
        await ps.close()


async def test_two_phase_rolls_back_and_cleans_reservation():
    ps, accounts, proxies = await _harness(
        bundle_mode="two_phase", bundle_gc_interval=3600.0
    )
    held = [await proxies.acquire(), await proxies.acquire()]
    try:
        with pytest.raises(BundleAcquireFailed):
            await ps.acquire_bundle(_pair(consumer="w1", timeout=0))
        store = await ps._store()
        now = time.time()
        assert await store.active(now) == []
        assert (await accounts.get("a1")).state is ResourceState.IDLE
        assert (await accounts.get("a2")).state is ResourceState.IDLE
    finally:
        for lease in held:
            await proxies.release(lease)
        await ps.close()


async def test_two_phase_gc_revokes_crashed_members():
    ps, accounts, proxies = await _harness(
        bundle_mode="two_phase", bundle_gc_interval=3600.0
    )
    store = await ps._store()
    lease_a = await accounts.acquire()
    lease_p = await proxies.acquire()
    bid = "bundle-crash"
    await store.admit(bid, "w1", ("accounts", "proxies"), 60.0)
    await store.begin_members(bid, 60.0)
    await store.add_member(
        bid, ReservationMember(pool="accounts", rid=lease_a.resource_id, token=lease_a.token)
    )
    await store.add_member(
        bid, ReservationMember(pool="proxies", rid=lease_p.resource_id, token=lease_p.token)
    )
    res = await store.get(bid)
    assert res is not None
    res.expires_at = 0.0  # simulate the assembling process crashing
    try:
        report = await ps.bundle_gc()
        assert report["revoked"] == 2
        assert (await accounts.get(lease_a.resource_id)).state is ResourceState.IDLE
        assert (await proxies.get(lease_p.resource_id)).state is ResourceState.IDLE
        assert await store.get(bid) is None
    finally:
        await ps.close()


async def test_bundle_gc_keeps_live_bundle_marker():
    ps, accounts, proxies = await _harness(
        bundle_mode="two_phase", bundle_gc_interval=3600.0
    )
    try:
        bundle = await ps.acquire_bundle(_pair())
        store = await ps._store()
        res = await store.get(bundle.bundle_id)
        assert res is not None
        res.expires_at = 0.0  # would expire without a refresh
        report = await ps.bundle_gc()
        assert report["revived"] >= 1
        res = await store.get(bundle.bundle_id)
        assert res is not None and res.status == "committed"
        assert res.expires_at > time.time()
        state = (await accounts.get(bundle.members["accounts"].resource_id)).state
        assert state in (ResourceState.ACTIVE, ResourceState.FULL)
        await ps.release_bundle(bundle)
    finally:
        await ps.close()


async def test_bundle_gc_keeps_alive_committed_cross_instance():
    ps, accounts, proxies = await _harness(
        bundle_mode="two_phase", bundle_gc_interval=3600.0
    )
    store = await ps._store()
    lease_a = await accounts.acquire()
    lease_p = await proxies.acquire()
    bid = "bundle-other-instance"
    await store.admit(bid, "w-other", ("accounts", "proxies"), 60.0)
    await store.begin_members(bid, 60.0)
    await store.add_member(
        bid,
        ReservationMember(pool="accounts", rid=lease_a.resource_id, token=lease_a.token),
    )
    await store.add_member(
        bid,
        ReservationMember(pool="proxies", rid=lease_p.resource_id, token=lease_p.token),
    )
    await store.confirm(bid, 60.0)
    res = await store.get(bid)
    assert res is not None
    res.expires_at = 0.0
    try:
        report = await ps.bundle_gc()
        assert report["revived"] >= 1
        assert await store.get(bid) is not None
        state = (await accounts.get(lease_a.resource_id)).state
        assert state in (ResourceState.ACTIVE, ResourceState.FULL)
    finally:
        await accounts.release(lease_a)
        await proxies.release(lease_p)
        await store.release(bid)
        await ps.close()


async def test_bundle_gc_prunes_dead_committed_without_revoking():
    ps, accounts, proxies = await _harness(
        bundle_mode="two_phase", bundle_gc_interval=3600.0
    )
    store = await ps._store()
    lease_a = await accounts.acquire()
    lease_p = await proxies.acquire()
    bid = "bundle-dead"
    await store.admit(bid, "w-dead", ("accounts", "proxies"), 60.0)
    await store.begin_members(bid, 60.0)
    await store.add_member(
        bid,
        ReservationMember(pool="accounts", rid=lease_a.resource_id, token=lease_a.token),
    )
    await store.add_member(
        bid,
        ReservationMember(pool="proxies", rid=lease_p.resource_id, token=lease_p.token),
    )
    await store.confirm(bid, 60.0)
    await accounts.release(lease_a)
    await proxies.release(lease_p)
    res = await store.get(bid)
    assert res is not None
    res.expires_at = 0.0
    try:
        report = await ps.bundle_gc()
        assert report["pruned"] == 1
        assert report["revoked"] == 0
        assert await store.get(bid) is None
    finally:
        await ps.close()


async def test_two_phase_mode_rejects_best_effort():
    ps, accounts, proxies = await _harness(
        bundle_mode="two_phase", bundle_gc_interval=3600.0
    )
    try:
        with pytest.raises(ConfigError, match="strict"):
            await ps.acquire_bundle(_pair(mode="best_effort"))
    finally:
        await ps.close()


def test_two_phase_strict_flag_rejects_best_effort():
    with pytest.raises(ConfigError, match="strict"):
        BundleRequest(
            slots=(BundleSlot(pool="a"), BundleSlot(pool="b")),
            mode="best_effort",
            strict=True,
        )


async def test_two_phase_retries_transient_failure():
    ps, accounts, proxies = await _harness(
        bundle_mode="two_phase", bundle_gc_interval=3600.0
    )
    held = [await proxies.acquire(), await proxies.acquire()]

    async def release_one_later() -> None:
        await asyncio.sleep(0.3)
        await proxies.release(held[0])

    try:
        releaser = asyncio.create_task(release_one_later())
        bundle = await ps.acquire_bundle(
            BundleRequest(
                slots=(
                    BundleSlot(pool="accounts", resource_id="a1"),
                    BundleSlot(pool="proxies"),
                ),
                consumer="w1",
                timeout=0,
                max_retries=4,
            )
        )
        assert "proxies" in bundle.members
        res = await (await ps._store()).get(bundle.bundle_id)
        assert res is not None and res.status == "committed"
        await ps.release_bundle(bundle)
        await releaser
        await proxies.release(held[1])
    finally:
        await ps.close()


async def test_two_phase_match_survives_deterministic_order():
    ps, accounts, proxies = await _harness(
        bundle_mode="two_phase", bundle_gc_interval=3600.0
    )
    try:
        held = await proxies.acquire(AcquireRequest(resource_id="p2", timeout=0))
        bundle = await ps.acquire_bundle(
            BundleRequest(
                slots=(
                    BundleSlot(pool="accounts", resource_id="a1"),
                    BundleSlot(
                        pool="proxies",
                        match=MatchRule(
                            anchor_pool="accounts",
                            from_field="region",
                            to_tag="region",
                        ),
                    ),
                ),
                consumer="w1",
            )
        )
        assert bundle.members["proxies"].resource_id == "p1"
        await ps.release_bundle(bundle)
        await proxies.release(held)
    finally:
        await ps.close()


# -- M2: fairness -----------------------------------------------------------


async def test_fairness_max_per_consumer_and_release():
    fairness = BundleFairness(max_per_consumer=1, wait=False, per_pool_headroom=0)
    ps = Purser(maintenance_enabled=False, bundle_fairness=fairness)
    ps.register_adapter(SimpleAdapter())
    accounts = ps.create_pool("accounts", "sim")
    proxies = ps.create_pool("proxies", "sim")
    await accounts.import_resources(
        [{"id": "a1"}, {"id": "a2"}, {"id": "a3"}],
        import_validate="on_import",
    )
    await proxies.import_resources(
        [{"id": "p1"}, {"id": "p2"}, {"id": "p3"}],
        import_validate="on_import",
    )
    await ps.start()
    try:
        first = await ps.acquire_bundle(_pair(consumer="c1"))
        with pytest.raises(BundleQuotaExceeded, match="max_per_consumer"):
            await ps.acquire_bundle(_pair(consumer="c1"))
        other = await ps.acquire_bundle(_pair(consumer="c2"))
        await ps.release_bundle(first)
        again = await ps.acquire_bundle(_pair(consumer="c1"))
        await ps.release_bundle(again)
        await ps.release_bundle(other)
    finally:
        await ps.close()


async def test_fairness_max_total():
    fairness = BundleFairness(max_total=1, wait=False, per_pool_headroom=0)
    ps, accounts, proxies = await _harness(bundle_fairness=fairness)
    try:
        first = await ps.acquire_bundle(_pair(consumer="c1"))
        with pytest.raises(BundleQuotaExceeded, match="max_total"):
            await ps.acquire_bundle(_pair(consumer="c2"))
        await ps.release_bundle(first)
    finally:
        await ps.close()


async def test_fairness_per_pool_headroom():
    fairness = BundleFairness(per_pool_headroom=1, wait=False)
    ps, accounts, proxies = await _harness(bundle_fairness=fairness)
    try:
        first = await ps.acquire_bundle(_pair(consumer="c1"))
        with pytest.raises(BundleQuotaExceeded, match="headroom"):
            await ps.acquire_bundle(_pair(consumer="c2"))
        await ps.release_bundle(first)
        again = await ps.acquire_bundle(_pair(consumer="c2"))
        await ps.release_bundle(again)
    finally:
        await ps.close()


async def test_fairness_wait_releases_on_release():
    fairness = BundleFairness(
        max_per_consumer=1, wait=True, timeout=3.0, per_pool_headroom=0
    )
    ps, accounts, proxies = await _harness(bundle_fairness=fairness)
    try:
        first = await ps.acquire_bundle(_pair(consumer="c1"))
        waiter = asyncio.create_task(ps.acquire_bundle(_pair(consumer="c1")))
        await asyncio.sleep(0.2)
        assert not waiter.done()
        await ps.release_bundle(first)
        second = await asyncio.wait_for(waiter, 2.0)
        assert "proxies" in second.members
        await ps.release_bundle(second)
    finally:
        await ps.close()


async def test_fairness_marker_cleared_on_rollback():
    fairness = BundleFairness(max_per_consumer=1, wait=False, per_pool_headroom=0)
    ps = Purser(maintenance_enabled=False, bundle_fairness=fairness)
    ps.register_adapter(SimpleAdapter())
    accounts = ps.create_pool("accounts", "sim")
    proxies = ps.create_pool("proxies", "sim")
    await accounts.import_resources(
        [{"id": "a1", "region": "us"}], import_validate="on_import"
    )
    await proxies.import_resources(
        [{"id": "p1", "host": "1.1.1.1", "tags": ["region:eu"]}],
        import_validate="on_import",
    )
    await ps.start()
    try:
        with pytest.raises(BundleAcquireFailed):
            await ps.acquire_bundle(
                BundleRequest(
                    slots=(
                        BundleSlot(pool="accounts", resource_id="a1"),
                        BundleSlot(
                            pool="proxies",
                            match=MatchRule(
                                anchor_pool="accounts",
                                from_field="region",
                                to_tag="region",
                            ),
                        ),
                    ),
                    consumer="c1",
                    timeout=0,
                )
            )
        now = time.time()
        store = await ps._store()
        assert await store.active(now) == []
        assert (await accounts.get("a1")).state is ResourceState.IDLE
    finally:
        await ps.close()


async def test_fairness_wait_timeout_raises():
    fairness = BundleFairness(
        max_per_consumer=1, wait=True, timeout=0.2, per_pool_headroom=0
    )
    ps, accounts, proxies = await _harness(bundle_fairness=fairness)
    try:
        first = await ps.acquire_bundle(_pair(consumer="c1"))
        with pytest.raises(BundleQuotaExceeded, match="timed out"):
            await ps.acquire_bundle(_pair(consumer="c1"))
        await ps.release_bundle(first)
    finally:
        await ps.close()


async def test_fairness_priority_wakes_highest_first():
    fairness = BundleFairness(max_total=1, wait=True, timeout=3.0, per_pool_headroom=0)
    ps, accounts, proxies = await _harness(bundle_fairness=fairness)
    try:
        holder = await ps.acquire_bundle(_pair(consumer="h", priority=0))
        low = asyncio.create_task(
            ps.acquire_bundle(_pair(consumer="lo", priority=0))
        )
        high = asyncio.create_task(
            ps.acquire_bundle(_pair(consumer="hi", priority=10))
        )
        await asyncio.sleep(0.2)
        assert not low.done() and not high.done()
        await ps.release_bundle(holder)
        await asyncio.sleep(0.3)
        assert high.done()
        assert not low.done()
        high_bundle = high.result()
        await ps.release_bundle(high_bundle)
        low_bundle = await asyncio.wait_for(low, 2.0)
        await ps.release_bundle(low_bundle)
    finally:
        await ps.close()


async def test_default_rollback_bundle_uses_no_store():
    ps, accounts, proxies = await _harness()
    try:
        bundle = await ps.acquire_bundle(_pair())
        assert ps._bundle_store is None
        await ps.release_bundle(bundle)
    finally:
        await ps.close()


async def test_fairness_rollback_marker_records_members_and_refreshes():
    fairness = BundleFairness(per_pool_headroom=0, wait=False)
    ps, accounts, proxies = await _harness(
        bundle_fairness=fairness, bundle_gc_interval=3600.0
    )
    try:
        bundle = await ps.acquire_bundle(_pair(consumer="c1"))
        store = await ps._store()
        res = await store.get(bundle.bundle_id)
        assert res is not None and res.status == "committed"
        assert set(res.members) == {"accounts", "proxies"}
        res.expires_at = 0.0
        report = await ps.bundle_gc()
        assert report["revived"] >= 1
        assert await store.get(bundle.bundle_id) is not None
        await ps.release_bundle(bundle)
        assert await store.get(bundle.bundle_id) is None
    finally:
        await ps.close()


async def test_fairness_distributed_poll_wakes_on_foreign_release():
    client = fakeredis.aioredis.FakeRedis()
    fairness = BundleFairness(
        max_total=1, wait=True, timeout=3.0, per_pool_headroom=0
    )
    ps = Purser(
        maintenance_enabled=False,
        coordination="redis",
        redis_client=client,
        bundle_fairness=fairness,
        bundle_gc_interval=3600.0,
    )
    ps.register_adapter(SimpleAdapter())
    accounts = ps.create_pool("accounts", "sim")
    proxies = ps.create_pool("proxies", "sim")
    await accounts.import_resources(
        [{"id": "a1"}, {"id": "a2"}], import_validate="on_import"
    )
    await proxies.import_resources(
        [{"id": "p1"}, {"id": "p2"}], import_validate="on_import"
    )
    await ps.start()
    store = await ps._store()
    try:
        # A marker written by another instance occupies the only quota slot;
        # releasing it emits no local event, so the waiter must wake by
        # polling the shared store.
        await store.admit(
            "bundle-foreign", "foreign", ("accounts", "proxies"), 60.0
        )
        waiter = asyncio.create_task(ps.acquire_bundle(_pair(consumer="me")))
        await asyncio.sleep(0.15)
        assert not waiter.done()
        await store.release("bundle-foreign")
        bundle = await asyncio.wait_for(waiter, 1.5)
        assert "proxies" in bundle.members
        await ps.release_bundle(bundle)
    finally:
        await ps.close()
        await client.aclose()


# -- M3: redis store + sync -------------------------------------------------


async def test_redis_bundle_store_lifecycle():
    client = fakeredis.aioredis.FakeRedis()
    store = RedisBundleStore(client=client)
    seq1 = await store.next_seq()
    seq2 = await store.next_seq()
    assert seq2 == seq1 + 1
    await store.admit("b1", "c1", ("accounts", "proxies"), 60.0)
    await store.begin_members("b1", 60.0)
    await store.add_member(
        "b1", ReservationMember(pool="accounts", rid="a1", token="ta")
    )
    await store.add_member(
        "b1", ReservationMember(pool="proxies", rid="p1", token="tp")
    )
    await store.confirm("b1", 60.0)
    now = time.time()
    assert [r.bundle_id for r in await store.active(now)] == ["b1"]
    res = await store.get("b1")
    assert res.status == "committed"
    assert set(res.members) == {"accounts", "proxies"}
    await store.release("b1")
    assert await store.active(now) == []
    await client.aclose()


async def test_two_phase_redis_coordination_end_to_end():
    client = fakeredis.aioredis.FakeRedis()
    ps = Purser(
        maintenance_enabled=False,
        coordination="redis",
        redis_client=client,
        bundle_mode="two_phase",
        bundle_gc_interval=3600.0,
    )
    ps.register_adapter(SimpleAdapter())
    accounts = ps.create_pool("accounts", "sim")
    proxies = ps.create_pool("proxies", "sim")
    await accounts.import_resources(
        [{"id": "a1"}], import_validate="on_import"
    )
    await proxies.import_resources(
        [{"id": "p1", "host": "1.1.1.1"}], import_validate="on_import"
    )
    await ps.start()
    try:
        bundle = await ps.acquire_bundle(_pair())
        assert set(bundle.members) == {"accounts", "proxies"}
        store = await ps._store()
        res = await store.get(bundle.bundle_id)
        assert res is not None and res.status == "committed"
        await ps.release_bundle(bundle)
        assert await store.get(bundle.bundle_id) is None
    finally:
        await ps.close()
        await client.aclose()


def test_sync_two_phase_bundle():
    with SyncPurser(
        maintenance_enabled=False, bundle_mode="two_phase", bundle_gc_interval=3600.0
    ) as ps:
        ps.register_adapter(SimpleAdapter())
        ps.create_pool("accounts", "sim")
        ps.create_pool("proxies", "sim")
        ps.pool("accounts").import_resources(
            [{"id": "a1"}], import_validate="on_import"
        )
        ps.pool("proxies").import_resources(
            [{"id": "p1", "host": "1.1.1.1"}], import_validate="on_import"
        )
        bundle = ps.acquire_bundle(_pair())
        assert set(bundle.members) == {"accounts", "proxies"}
        ps.release_bundle(bundle)
        report = ps.bundle_gc()
        assert report["pruned"] == 0
