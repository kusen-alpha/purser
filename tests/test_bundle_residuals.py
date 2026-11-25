"""Residual branches of the bundle core value objects and facade orchestration.

Covers ``bundle_request_from_dict`` rejections, ``BundleRequest`` /
``BundleFairness`` validation, override/bundle-id handling, match-rule
resolution, cancellation rollback, fairness admission (timeout, not-wait,
distributed pokes) and the reservation GC sweep.
"""

from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace

import fakeredis
import pytest
from test_bundle import SimpleAdapter

from purser import (
    BundleAcquireFailed,
    BundleFairness,
    BundleLease,
    BundleQuotaExceeded,
    BundleRequest,
    BundleSlot,
    ConfigError,
    MatchRule,
    Purser,
    ResourceState,
)
from purser.core.bundle import bundle_request_from_dict
from purser.core.bundle_store import ReservationMember
from purser.core.resource import AcquireRequest

ACCOUNTS = [{"id": "a1", "region": "us"}, {"id": "a2", "region": "eu"}]
PROXIES = [
    {"id": "p1", "host": "1.1.1.1", "tags": ["region:us"]},
    {"id": "p2", "host": "2.2.2.2", "tags": ["region:eu"]},
]


async def _harness(
    *,
    bundle_mode: str = "rollback",
    bundle_fairness: BundleFairness | None = None,
    accounts_rows: list[dict] | None = None,
    proxies_rows: list[dict] | None = None,
    **kwargs,
):
    kwargs.setdefault("bundle_gc_interval", 3600.0)
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
        accounts_rows or ACCOUNTS, import_validate="on_import"
    )
    await proxies.import_resources(
        proxies_rows or PROXIES, import_validate="on_import"
    )
    await ps.start()
    return ps, accounts, proxies


def _slots():
    return (BundleSlot(pool="accounts"), BundleSlot(pool="proxies"))


def _pair(*, consumer="w1", **changes):
    return BundleRequest(slots=_slots(), consumer=consumer, **changes)


# -- core value objects ------------------------------------------------------


@pytest.mark.parametrize(
    ("slots", "changes", "message"),
    [
        ((), {}, "at least one slot"),
        ((BundleSlot(pool="a"),), {"mode": "greedy"}, "mode must be"),
        ((BundleSlot(pool="a"),), {"order": "random"}, "order must be"),
        ((BundleSlot(pool="a"),), {"max_retries": -1}, "max_retries"),
        ((BundleSlot(pool="a"),), {"priority": True}, "priority must be an integer"),
        (
            (BundleSlot(pool="a"),),
            {"mode": "best_effort", "strict": True},
            "cannot use best_effort",
        ),
        ((BundleSlot(pool="a"), BundleSlot(pool="a")), {}, "distinct pools"),
        (
            (
                BundleSlot(
                    pool="a",
                    match=MatchRule(anchor_pool="ghost", from_field="x", to_tag="t"),
                ),
            ),
            {},
            "must be one of the slots",
        ),
    ],
)
def test_bundle_request_validation(slots, changes, message):
    with pytest.raises(ConfigError, match=message):
        BundleRequest(slots=slots, **changes)


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"per_pool_headroom": -1}, "per_pool_headroom"),
        ({"max_per_consumer": 0}, "max_per_consumer"),
        ({"max_total": -2}, "max_total"),
        ({"timeout": -1.0}, "timeout must be"),
    ],
)
def test_bundle_fairness_validation(kwargs, message):
    with pytest.raises(ConfigError, match=message):
        BundleFairness(**kwargs)


def test_bundle_lease_member_and_ids():
    lease = SimpleNamespace(token="t-1", resource_id="r-1")
    bundle = BundleLease(bundle_id="b-1", consumer=None, members={"pool": lease})
    assert bundle.member("pool") is lease
    assert bundle.ids() == {"pool": "r-1"}


@pytest.mark.parametrize(
    ("data", "message"),
    [
        ({}, "non-empty slots"),
        ({"slots": []}, "non-empty slots"),
        ({"slots": ["p"]}, "slot must be an object"),
        ({"slots": [{"pool": ""}]}, "requires a pool name"),
        ({"slots": [{"pool": "a", "req": "x"}]}, "req must be an object"),
        ({"slots": [{"pool": "a", "match": "x"}]}, "match must be an object"),
        (
            {"slots": [{"pool": "a", "match": {"anchor_pool": "a"}}]},
            "requires from_field",
        ),
        (
            {"slots": [{"pool": "a", "resource_id": 5}]},
            "resource_id must be a string",
        ),
        ({"slots": [{"pool": "a"}], "mode": "greedy"}, "mode must be"),
        ({"slots": [{"pool": "a"}], "order": "random"}, "order must be"),
        ({"slots": [{"pool": "a"}], "priority": "high"}, "priority must be"),
        ({"slots": [{"pool": "a"}], "strict": "yes"}, "strict must be"),
    ],
)
def test_bundle_request_from_dict_rejections(data, message):
    with pytest.raises(ConfigError, match=message):
        bundle_request_from_dict(data)


def test_bundle_request_from_dict_builds_full_request():
    req = bundle_request_from_dict(
        {
            "slots": [
                {
                    "pool": "accounts",
                    "resource_id": "a1",
                    "req": {"tags": ["eu"], "slots": 2},
                },
                {
                    "pool": "proxies",
                    "match": {
                        "anchor_pool": "accounts",
                        "from_field": "region",
                        "to_tag": "region",
                    },
                },
            ],
            "consumer": "w1",
            "mode": "best_effort",
            "order": "declared",
            "timeout": 2.0,
            "bundle_id": "b-1",
            "max_retries": 2,
            "priority": 3,
        }
    )
    assert req.consumer == "w1" and req.bundle_id == "b-1"
    assert (req.mode, req.order, req.max_retries, req.priority) == (
        "best_effort",
        "declared",
        2,
        3,
    )
    assert req.timeout == 2.0 and req.strict is None
    first, second = req.slots
    assert first.resource_id == "a1"
    assert first.req is not None and first.req.slots == 2
    assert second.match == MatchRule(
        anchor_pool="accounts", from_field="region", to_tag="region"
    )


# -- facade: request shaping -------------------------------------------------


async def test_acquire_bundle_rejects_bad_request_and_overrides():
    ps, _accounts, _proxies = await _harness()
    try:
        with pytest.raises(TypeError, match="expects a BundleRequest or dict"):
            await ps.acquire_bundle("nope")
        with pytest.raises(TypeError, match="unexpected keyword argument"):
            await ps.acquire_bundle(_pair(), bogus=1)

        first = await ps.acquire_bundle(_pair(consumer="w1", bundle_id="dup-1"))
        try:
            with pytest.raises(ConfigError, match="already in use"):
                await ps.acquire_bundle(_pair(consumer="w2", bundle_id="dup-1"))
        finally:
            await ps.release_bundle(first)
    finally:
        await ps.close()


async def test_acquire_bundle_applies_overrides_and_reuses_ids():
    ps, _accounts, _proxies = await _harness()
    try:
        bundle = await ps.acquire_bundle(
            _pair(), consumer="override", order="declared", priority=1
        )
        assert bundle.consumer == "override"
        assert set(bundle.members) == {"accounts", "proxies"}

        again = await ps.acquire_bundle(
            _pair(consumer="override", bundle_id=bundle.bundle_id)
        )
        assert again is bundle
        await ps.release_bundle(bundle)
    finally:
        await ps.close()


async def test_each_slot_honours_its_own_request():
    ps, accounts, proxies = await _harness()
    try:
        bundle = await ps.acquire_bundle(
            BundleRequest(
                slots=(
                    BundleSlot(pool="accounts", req=AcquireRequest()),
                    BundleSlot(pool="proxies", resource_id="p2"),
                ),
                consumer="w1",
                order="declared",
            )
        )
        assert bundle.members["proxies"].resource_id == "p2"
        await ps.release_bundle(bundle)
        assert (await proxies.get("p2")).state is ResourceState.IDLE
    finally:
        await ps.close()


# -- facade: two-phase store -------------------------------------------------


async def test_two_phase_explicit_bundle_id_keeps_marker():
    ps, _accounts, _proxies = await _harness(bundle_mode="two_phase")
    try:
        bundle = await ps.acquire_bundle(_pair(bundle_id="two-phase-1"))
        assert bundle.bundle_id == "two-phase-1"
        store = await ps._store()
        marker = await store.get("two-phase-1")
        assert marker is not None and marker.status == "committed"
        await ps.release_bundle(bundle)
        assert await store.get("two-phase-1") is None
    finally:
        await ps.close()


async def test_release_skips_members_of_unknown_pools():
    ps, accounts, proxies = await _harness(bundle_mode="two_phase")
    try:
        bundle = await ps.acquire_bundle(_pair())
        ghost = BundleLease(
            bundle_id="ghost-bundle",
            consumer=None,
            members={"ghost": bundle.members["accounts"]},
        )
        await ps.release_bundle(ghost)  # unknown pool is skipped, no crash
        assert "ghost-bundle" not in ps._bundles
        await ps.release_bundle(bundle)
        assert "ghost" not in ps._bundles and bundle.bundle_id not in ps._bundles
    finally:
        await ps.close()


async def test_rollback_logs_member_release_failures(caplog):
    ps, accounts, proxies = await _harness()
    held = [await proxies.acquire(), await proxies.acquire()]
    original = accounts.release

    async def refuse(*args, **kwargs):
        raise RuntimeError("release backend down")

    accounts.release = refuse
    try:
        with caplog.at_level(logging.WARNING, logger="purser"):
            with pytest.raises(BundleAcquireFailed):
                await ps.acquire_bundle(_pair(timeout=0, order="declared"))
        assert "bundle member release failed" in caplog.text
        assert "release backend down" in caplog.text
    finally:
        accounts.release = original
        for lease in held:
            await proxies.release(lease)
        await ps.close()


# -- facade: match rules -----------------------------------------------------


async def test_scarce_first_order_emits_match_anchor_first():
    ps, accounts, proxies = await _harness()
    try:
        bundle = await ps.acquire_bundle(
            BundleRequest(
                slots=(
                    BundleSlot(
                        pool="proxies",
                        match=MatchRule(
                            anchor_pool="accounts", from_field="region", to_tag="region"
                        ),
                    ),
                    BundleSlot(pool="accounts"),
                ),
                consumer="w1",
                order="scarce_first",
            )
        )
        assert list(bundle.members) == ["accounts", "proxies"]
        anchor = bundle.members["accounts"]
        proxy = await proxies.get(bundle.members["proxies"].resource_id)
        region = (await accounts.get(anchor.resource_id)).payload["region"]
        assert proxy is not None and f"region:{region}" in proxy.tags
        await ps.release_bundle(bundle)
    finally:
        await ps.close()


async def test_match_slot_before_its_anchor_is_rejected():
    ps, _accounts, _proxies = await _harness()
    try:
        with pytest.raises(BundleAcquireFailed, match="must be acquired before"):
            await ps.acquire_bundle(
                BundleRequest(
                    slots=(
                        BundleSlot(
                            pool="proxies",
                            match=MatchRule(
                                anchor_pool="accounts",
                                from_field="region",
                                to_tag="region",
                            ),
                        ),
                        BundleSlot(pool="accounts"),
                    ),
                    consumer="w1",
                    order="declared",
                )
            )
    finally:
        await ps.close()


async def test_match_missing_field_fails_and_rolls_back():
    ps, accounts, _proxies = await _harness()
    try:
        with pytest.raises(BundleAcquireFailed, match="missing on resource"):
            await ps.acquire_bundle(
                BundleRequest(
                    slots=(
                        BundleSlot(pool="accounts"),
                        BundleSlot(
                            pool="proxies",
                            match=MatchRule(
                                anchor_pool="accounts",
                                from_field="no_such_field",
                                to_tag="region",
                            ),
                        ),
                    ),
                    consumer="w1",
                    order="declared",
                )
            )
        await asyncio.sleep(0)
        for rid in ("a1", "a2"):
            assert (await accounts.get(rid)).concurrency.used_slots == 0
    finally:
        await ps.close()


async def test_match_reads_payload_fields():
    ps, _accounts, proxies = await _harness()
    try:
        # ``region`` lives in the payload, not on the Resource dataclass.
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
                order="declared",
            )
        )
        assert bundle.members["proxies"].resource_id == "p1"
        await ps.release_bundle(bundle)
    finally:
        await ps.close()


async def test_match_reads_typed_attributes():
    ps, accounts, _proxies = await _harness(
        proxies_rows=[
            {"id": "a1", "host": "1.1.1.1", "tags": ["id:a1"]},
            {"id": "p2", "host": "2.2.2.2"},
        ],
        accounts_rows=[{"id": "a1", "region": "us"}],
    )
    try:
        bundle = await ps.acquire_bundle(
            BundleRequest(
                slots=(
                    BundleSlot(pool="accounts", resource_id="a1"),
                    BundleSlot(
                        pool="proxies",
                        match=MatchRule(
                            anchor_pool="accounts", from_field="id", to_tag="id"
                        ),
                    ),
                ),
                consumer="w1",
                order="declared",
            )
        )
        assert bundle.members["proxies"].resource_id == "a1"
        await ps.release_bundle(bundle)
    finally:
        await ps.close()


# -- facade: cancellation ----------------------------------------------------


async def test_cancel_during_acquire_releases_earlier_members():
    ps, accounts, proxies = await _harness()
    held = [await proxies.acquire(), await proxies.acquire()]
    try:
        task = asyncio.create_task(
            ps.acquire_bundle(_pair(timeout=30.0, order="declared"))
        )

        async def started() -> bool:
            while (await accounts.stats())["active_leases"] == 0:
                await asyncio.sleep(0.01)
            return True

        await asyncio.wait_for(started(), timeout=5.0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert ps._bundles == {}
        for rid in ("a1", "a2"):
            assert (await accounts.get(rid)).concurrency.used_slots == 0
    finally:
        for lease in held:
            await proxies.release(lease)
        await ps.close()


# -- facade: fairness --------------------------------------------------------


async def _wait_for(predicate, timeout: float = 5.0) -> None:
    async def poll() -> None:
        while not predicate():
            await asyncio.sleep(0)

    await asyncio.wait_for(poll(), timeout)


def _single(name="accounts"):
    return BundleRequest(slots=(BundleSlot(pool=name),), consumer="c1")


async def test_fairness_rejects_without_waiting():
    ps, _accounts, _proxies = await _harness(
        bundle_fairness=BundleFairness(wait=False),
        accounts_rows=[{"id": "a1", "region": "us"}],
    )
    try:
        with pytest.raises(BundleQuotaExceeded, match="per_pool_headroom"):
            await ps.acquire_bundle(_single())
    finally:
        await ps.close()


async def test_fairness_zero_timeout_rejects_immediately():
    ps, _accounts, _proxies = await _harness(
        bundle_fairness=BundleFairness(timeout=0.0),
        accounts_rows=[{"id": "a1", "region": "us"}],
    )
    try:
        with pytest.raises(BundleQuotaExceeded, match="wait timed out"):
            await ps.acquire_bundle(_single())
    finally:
        await ps.close()


async def test_fairness_waiter_times_out():
    ps, _accounts, _proxies = await _harness(
        bundle_fairness=BundleFairness(timeout=0.05),
        accounts_rows=[{"id": "a1", "region": "us"}],
    )
    try:
        with pytest.raises(BundleQuotaExceeded, match="wait timed out"):
            await ps.acquire_bundle(_single())
    finally:
        await ps.close()


async def test_cancelled_waiter_is_skipped_when_capacity_frees():
    ps, _accounts, _proxies = await _harness(
        bundle_fairness=BundleFairness(max_total=1),
    )
    try:
        held = await ps.acquire_bundle(_single())
        blocked = asyncio.create_task(
            ps.acquire_bundle(
                BundleRequest(
                    slots=(BundleSlot(pool="accounts"),),
                    consumer="c2",
                    timeout=5.0,
                )
            )
        )
        await _wait_for(lambda: bool(ps._fair_heap))
        blocked.cancel()
        with pytest.raises(asyncio.CancelledError):
            await blocked

        await ps.release_bundle(held)  # pops the cancelled waiter, no crash
        third = await ps.acquire_bundle(
            BundleRequest(
                slots=(BundleSlot(pool="accounts"),), consumer="c3", timeout=5.0
            )
        )
        assert third.consumer == "c3"
        await ps.release_bundle(third)
    finally:
        await ps.close()


async def test_distributed_fairness_polls_and_wakes():
    ps, _accounts, _proxies = await _harness(
        bundle_fairness=BundleFairness(max_total=1),
        coordination="redis",
        redis_client=fakeredis.aioredis.FakeRedis(),
    )
    try:
        held = await ps.acquire_bundle(
            BundleRequest(slots=(BundleSlot(pool="accounts"),), consumer=None)
        )
        blocked = asyncio.create_task(
            ps.acquire_bundle(
                BundleRequest(
                    slots=(BundleSlot(pool="accounts"),),
                    consumer="c2",
                    timeout=5.0,
                )
            )
        )
        await _wait_for(lambda: bool(ps._fair_heap))
        await ps.release_bundle(held)
        bundle = await asyncio.wait_for(blocked, timeout=5.0)
        assert bundle.consumer == "c2"
        await ps.release_bundle(bundle)
    finally:
        await ps.close()


# -- facade: reservation GC --------------------------------------------------


async def test_bundle_gc_without_store_is_a_noop():
    ps, _accounts, _proxies = await _harness()
    try:
        assert await ps.bundle_gc() == {"revoked": 0, "pruned": 0, "revived": 0}
    finally:
        await ps.close()


async def test_bundle_gc_skips_unknown_pools_and_logs_revoke_failures(caplog):
    ps, _accounts, _proxies = await _harness(bundle_mode="two_phase")
    store = await ps._store()
    bid = "bundle-broken"
    await store.admit(bid, "w1", ("accounts",), 60.0)
    await store.begin_members(bid, 60.0)
    await store.add_member(
        bid, ReservationMember(pool="ghost", rid="gone", token="t-ghost")
    )
    await store.add_member(
        bid, ReservationMember(pool="accounts", rid="a1", token="not-a-lease")
    )
    marker = await store.get(bid)
    assert marker is not None
    marker.expires_at = 0.0
    try:
        with caplog.at_level(logging.WARNING, logger="purser"):
            report = await ps.bundle_gc()
        assert report == {"revoked": 0, "pruned": 1, "revived": 0}
        assert "bundle lease force-revoke failed" in caplog.text
        assert await store.get(bid) is None
    finally:
        await ps.close()


async def test_bundle_gc_prunes_committed_marker_with_dead_member():
    ps, accounts, _proxies = await _harness(bundle_mode="two_phase")
    store = await ps._store()
    bid = "bundle-orphan"
    await store.admit(bid, "w1", ("ghost",), 60.0)
    await store.begin_members(bid, 60.0)
    await store.add_member(
        bid, ReservationMember(pool="ghost", rid="a1", token="t-1")
    )
    await store.confirm(bid, 60.0)
    marker = await store.get(bid)
    assert marker is not None and marker.status == "committed"
    marker.expires_at = 0.0
    try:
        report = await ps.bundle_gc()
        assert report["pruned"] == 1 and report["revived"] == 0
        assert await store.get(bid) is None
        # The live pool is untouched.
        assert (await accounts.get("a1")).concurrency.used_slots == 0
    finally:
        await ps.close()


async def test_gc_loop_logs_failures_and_propagates_cancellation(
    monkeypatch, caplog
):
    ps, _accounts, _proxies = await _harness(bundle_gc_interval=0.01)
    service = ps._bundle_service
    calls = {"n": 0}

    async def boom() -> dict[str, int]:
        calls["n"] += 1
        raise RuntimeError("sweep exploded")

    monkeypatch.setattr(service, "gc", boom)
    task = asyncio.create_task(service.gc_loop())
    try:
        with caplog.at_level(logging.ERROR, logger="purser"):

            async def failed() -> bool:
                while "bundle_gc sweep failed" not in caplog.text:
                    await asyncio.sleep(0.01)
                return True

            await asyncio.wait_for(failed(), timeout=10.0)
        assert not task.done()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        if not task.done():
            task.cancel()
        await ps.close()
