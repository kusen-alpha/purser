"""Cross-pool bundle leases (O8): atomic acquire, rollback, match rules."""

from __future__ import annotations

import asyncio

import pytest
from fastapi.testclient import TestClient

from purser import (
    BundleAcquireFailed,
    BundleLease,
    BundleRequest,
    BundleSlot,
    ConfigError,
    HealthReport,
    MatchRule,
    Purser,
    Resource,
    ResourceState,
    SyncPurser,
)
from purser.adapters.base import ResourceAdapter
from purser.core.resource import AcquireRequest
from purser.ui.server import create_app


class SimpleAdapter(ResourceAdapter):
    """Tiny adapter that copies the spec into the payload (tag-aware)."""

    kind = "sim"
    default_concurrency = 1

    async def generate(self, spec: dict):
        payload = {
            key: value
            for key, value in spec.items()
            if key not in ("id", "tags", "mutex_group") and value is not None
        }
        return Resource(
            id=spec["id"],
            kind=self.kind,
            payload=payload,
            tags=frozenset(spec.get("tags") or ()),
            meta={},
        )

    async def validate(self, resource: Resource) -> HealthReport:
        return HealthReport.ok()


async def _harness():
    ps = Purser(maintenance_enabled=False)
    ps.register_adapter(SimpleAdapter())
    accounts = ps.create_pool("accounts", "sim")
    proxies = ps.create_pool("proxies", "sim")
    await accounts.import_resources(
        [{"id": "a1", "region": "us"}, {"id": "a2", "region": "eu"}],
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


async def test_bundle_atomic_acquire_and_release():
    ps, accounts, proxies = await _harness()
    try:
        bundle = await ps.acquire_bundle(
            BundleRequest(
                slots=(
                    BundleSlot(pool="accounts"),
                    BundleSlot(pool="proxies"),
                ),
                consumer="worker-1",
                order="declared",
            )
        )
        assert isinstance(bundle, BundleLease)
        assert set(bundle.members) == {"accounts", "proxies"}
        account_rid = bundle.members["accounts"].resource_id
        assert (await accounts.get(account_rid)).state in (
            ResourceState.ACTIVE,
            ResourceState.FULL,
        )
        await ps.release_bundle(bundle)
        assert (await accounts.get(account_rid)).state is ResourceState.IDLE
        lease = await accounts.acquire(
            AcquireRequest(resource_id=account_rid, timeout=0)
        )
        assert lease.resource_id == account_rid
        await accounts.release(lease)
    finally:
        await ps.close()


async def test_bundle_rolls_back_all_members_on_failure():
    ps, accounts, proxies = await _harness()
    try:
        held = [await proxies.acquire(), await proxies.acquire()]
        with pytest.raises(BundleAcquireFailed):
            await ps.acquire_bundle(
                BundleRequest(
                    slots=(
                        BundleSlot(pool="accounts"),
                        BundleSlot(pool="proxies"),
                    ),
                    consumer="worker-1",
                    order="declared",
                    timeout=0,
                )
            )
        assert (await accounts.get("a1")).state is ResourceState.IDLE
        assert (await accounts.get("a2")).state is ResourceState.IDLE
        for lease in held:
            await proxies.release(lease)
    finally:
        await ps.close()


async def test_bundle_best_effort_returns_partial():
    ps, accounts, proxies = await _harness()
    try:
        held = [await proxies.acquire(), await proxies.acquire()]
        bundle = await ps.acquire_bundle(
            BundleRequest(
                slots=(
                    BundleSlot(pool="accounts"),
                    BundleSlot(pool="proxies"),
                ),
                consumer="worker-1",
                order="declared",
                mode="best_effort",
                timeout=0,
            )
        )
        assert set(bundle.members) == {"accounts"}
        await ps.release_bundle(bundle)
        for lease in held:
            await proxies.release(lease)
    finally:
        await ps.close()


async def test_bundle_match_rule_derives_region_tag():
    ps, accounts, proxies = await _harness()
    try:
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
                consumer="worker-1",
                order="declared",
            )
        )
        assert bundle.members["proxies"].resource_id == "p1"
        await ps.release_bundle(bundle)
    finally:
        await ps.close()


async def test_bundle_directed_binding_pins_both_ids():
    ps, accounts, proxies = await _harness()
    try:
        bundle = await ps.acquire_bundle(
            BundleRequest(
                slots=(
                    BundleSlot(pool="accounts", resource_id="a1"),
                    BundleSlot(pool="proxies", resource_id="p2"),
                ),
                consumer="worker-1",
                order="declared",
            )
        )
        assert bundle.ids() == {"accounts": "a1", "proxies": "p2"}
        await ps.release_bundle(bundle)
    finally:
        await ps.close()


async def test_bundle_match_survives_scarce_first_ordering():
    ps, accounts, proxies = await _harness()
    try:
        # Occupy the region:eu proxy so the proxies pool is scarcer than
        # accounts; scarce_first would pick proxies first without the anchor
        # ordering constraint.
        held = await proxies.acquire(
            AcquireRequest(resource_id="p2", timeout=0)
        )
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
                consumer="worker-1",
            )
        )
        assert bundle.members["proxies"].resource_id == "p1"
        await ps.release_bundle(bundle)
        await proxies.release(held)
    finally:
        await ps.close()


async def test_bundle_retries_transient_failure_with_max_retries():
    ps, accounts, proxies = await _harness()
    held = [await proxies.acquire(), await proxies.acquire()]
    held_leases = list(held)

    async def release_one_later() -> None:
        await asyncio.sleep(0.3)
        await proxies.release(held_leases[0])

    try:
        releaser = asyncio.create_task(release_one_later())
        bundle = await ps.acquire_bundle(
            BundleRequest(
                slots=(
                    BundleSlot(pool="accounts", resource_id="a1"),
                    BundleSlot(pool="proxies"),
                ),
                consumer="worker-1",
                order="declared",
                timeout=0,
                max_retries=4,
            )
        )
        assert "proxies" in bundle.members
        await ps.release_bundle(bundle)
        await releaser
        await proxies.release(held_leases[1])
    finally:
        await ps.close()


async def test_bundle_max_retries_exhausted_raises_and_rolls_back():
    ps, accounts, proxies = await _harness()
    held = [await proxies.acquire(), await proxies.acquire()]
    try:
        with pytest.raises(BundleAcquireFailed):
            await ps.acquire_bundle(
                BundleRequest(
                    slots=(
                        BundleSlot(pool="accounts", resource_id="a1"),
                        BundleSlot(pool="proxies"),
                    ),
                    consumer="worker-1",
                    order="declared",
                    timeout=0,
                    max_retries=1,
                )
            )
        assert (await accounts.get("a1")).state is ResourceState.IDLE
        for lease in held:
            await proxies.release(lease)
    finally:
        await ps.close()


def test_bundle_request_validates_slots():
    with pytest.raises(ConfigError, match="distinct pools"):
        BundleRequest(slots=(BundleSlot(pool="a"), BundleSlot(pool="a")))
    with pytest.raises(ConfigError, match="anchor pool"):
        BundleRequest(
            slots=(
                BundleSlot(pool="a"),
                BundleSlot(
                    pool="b",
                    match=MatchRule(
                        anchor_pool="missing", from_field="x", to_tag="y"
                    ),
                ),
            )
        )
    with pytest.raises(ConfigError, match="max_retries"):
        BundleRequest(slots=(BundleSlot(pool="a"),), max_retries=-1)


class SessionAdapter(ResourceAdapter):
    """Composite resource: one row models an account + proxy pair (scenario C)."""

    kind = "session"
    default_concurrency = 1

    async def generate(self, spec: dict):
        payload = {
            key: value
            for key, value in spec.items()
            if key not in ("id", "tags") and value is not None
        }
        return Resource(
            id=spec["id"],
            kind=self.kind,
            payload=payload,
            tags=frozenset(spec.get("tags") or ()),
            meta={},
        )

    async def validate(self, resource: Resource) -> HealthReport:
        return HealthReport.ok()


async def test_composite_resource_covers_fixed_binding():
    ps = Purser(maintenance_enabled=False)
    ps.register_adapter(SessionAdapter())
    pool = ps.create_pool("sessions", "session")
    await pool.import_resources(
        [
            {
                "id": "s1",
                "account": "u1",
                "password": "secret",
                "proxy": "https://1.1.1.1:8080",
            }
        ],
        import_validate="on_import",
    )
    try:
        await ps.start()
        async with pool.lease(consumer="worker-1") as lease:
            assert lease.payload["account"] == "u1"
            assert lease.payload["proxy"] == "https://1.1.1.1:8080"
            assert (await pool.get("s1")).state in (
                ResourceState.ACTIVE,
                ResourceState.FULL,
            )
        assert (await pool.get("s1")).state is ResourceState.IDLE
    finally:
        await ps.close()


def test_sync_bundle_acquire_release():
    with SyncPurser(maintenance_enabled=False) as ps:
        ps.register_adapter(SimpleAdapter())
        ps.create_pool("accounts", "sim")
        ps.create_pool("proxies", "sim")
        ps.pool("accounts").import_resources(
            [{"id": "a1", "region": "us"}], import_validate="on_import"
        )
        ps.pool("proxies").import_resources(
            [{"id": "p1", "host": "1.1.1.1"}], import_validate="on_import"
        )
        bundle = ps.acquire_bundle(
            BundleRequest(
                slots=(
                    BundleSlot(pool="accounts"),
                    BundleSlot(pool="proxies"),
                ),
                consumer="worker-1",
                order="declared",
            )
        )
        assert set(bundle.members) == {"accounts", "proxies"}
        ps.release_bundle(bundle)


async def test_rest_bundle_field():
    ps = Purser(maintenance_enabled=False)
    ps.register_adapter(SimpleAdapter())
    accounts = ps.create_pool("accounts", "sim")
    proxies = ps.create_pool("proxies", "sim")
    await accounts.import_resources(
        [{"id": "a1", "region": "us"}], import_validate="on_import"
    )
    await proxies.import_resources(
        [{"id": "p1", "host": "1.1.1.1"}], import_validate="on_import"
    )
    headers = {"X-Operator": "tester"}
    with TestClient(create_app(ps)) as client:
        resp = client.post(
            "/pools/accounts/leases",
            json={
                "consumer": "worker-1",
                "bundle": {
                    "slots": [
                        {"pool": "accounts"},
                        {"pool": "proxies"},
                    ],
                    "order": "declared",
                },
            },
            headers=headers,
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert set(data["members"]) == {"accounts", "proxies"}
        bad = client.post(
            "/pools/accounts/leases",
            json={"bundle": {"slots": [{"pool": "proxies"}]}},
            headers=headers,
        )
        assert bad.status_code == 422
