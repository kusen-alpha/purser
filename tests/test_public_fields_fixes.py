"""Fixes for the public field audit: meta isolation, quota naming, editable
period quota / hold-until, default backfill and expired-resource GC."""

from __future__ import annotations

import json
import time

import pytest
from conftest import seed

from purser.backends.base import ResourcePatch
from purser.config import PoolConfig
from purser.core.meta import ResourceMeta
from purser.core.resource import AcquireRequest, Resource
from purser.core.state import ResourceState
from purser.errors import ConfigError, PoolExhausted

# -- meta isolation ---------------------------------------------------------


def test_resource_system_and_user_meta_split():
    resource = Resource(
        id="r1",
        kind="fake",
        meta={
            "created_at": 111.0,
            "last_used_at": 222.0,
            "cooldown:alice": 333.0,
            "quarantine_until": 444.0,
            "owner": "team-a",
            "note": "hello",
        },
    )
    assert resource.user_meta == {"owner": "team-a", "note": "hello"}
    assert resource.system_meta == {
        "created_at": 111.0,
        "last_used_at": 222.0,
        "cooldown:alice": 333.0,
        "quarantine_until": 444.0,
    }
    # The full bag is still available for backwards compatibility.
    assert resource.meta["owner"] == "team-a"
    assert resource.meta["created_at"] == 111.0


def test_framework_written_meta_keys_are_all_system_owned():
    from purser.core.resource import is_system_meta_key

    # Keys the framework itself writes via admin/import/switch paths.
    for key in ("priority", "preference", "group", "switched_from"):
        assert is_system_meta_key(key), key

    resource = Resource(
        id="r1",
        kind="fake",
        meta={
            "priority": 5,
            "preference": 0.5,
            "group": "g1",
            "switched_from": "old",
            "owner": "team-a",
        },
    )
    assert resource.user_meta == {"owner": "team-a"}
    assert set(resource.system_meta) == {"priority", "preference", "group", "switched_from"}
    assert resource.system_meta | resource.user_meta == resource.meta


# -- PoolConfig first-class knobs and legacy meta migration -----------------


def test_pool_config_expire_policy_and_scheduler_intervals_fields():
    cfg = PoolConfig(
        kind="k",
        expire_policy="quota",
        scheduler_intervals={"expire_gc": 60.0},
        expired_retention=3600.0,
        requalify_on_release=True,
    )
    assert cfg.expire_policy == "quota"
    assert cfg.scheduler_intervals == {"expire_gc": 60.0}
    assert cfg.expired_retention == 3600.0
    assert cfg.requalify_on_release is True


def test_pool_config_ignores_legacy_meta_knob_keys():
    # The lifted knobs are first-class fields now; ``meta`` entries with the
    # same names are inert and must not leak into the fields.
    cfg = PoolConfig(
        kind="k",
        meta={
            "expire_policy": "quota",
            "scheduler_intervals": {"quota_reset": 42.0},
            "requalify_on_release": True,
        },
    )
    assert cfg.expire_policy == "suspended"
    assert cfg.scheduler_intervals is None
    assert cfg.requalify_on_release is False
    assert cfg.meta["expire_policy"] == "quota"
    assert cfg.to_dict()["quota_limit"] is None
    assert cfg.to_dict()["expire_policy"] == "suspended"


@pytest.mark.parametrize(
    "overrides",
    [
        {"expire_policy": "ban"},
        {"scheduler_intervals": {"not_a_task": 5.0}},
        {"scheduler_intervals": {"quota_reset": float("nan")}},
        {"scheduler_intervals": "fast"},
    ],
)
def test_pool_config_new_knobs_reject_invalid_values(overrides):
    with pytest.raises(ConfigError):
        PoolConfig(kind="k", **overrides)


# -- editable period quota / hold-until -------------------------------------


async def test_import_accepts_quota_limit_and_next_available_at(make_pool):
    pool = make_pool()
    hold = time.time() + 5000
    report = await pool.import_resources(
        [
            {
                "id": "q1",
                "host": "127.0.0.1",
                "port": 1,
                "quota_limit": 3,
                "next_available_at": hold,
            }
        ]
    )
    assert report.imported == 1
    res = await pool.get("q1")
    assert res is not None
    assert res.quota is not None and res.quota.limit == 3
    assert res.next_available_at == hold


def test_resource_meta_typed_properties():
    meta = ResourceMeta({"priority": "2", "preference": 0.5})
    assert meta.priority == 2
    assert meta.preference == 0.5
    assert meta.group is None
    assert meta.quarantine_until is None

    meta.priority = 9
    meta.group = "g1"
    meta.quarantine_until = 12.5
    assert meta == {
        "priority": 9,
        "preference": 0.5,
        "group": "g1",
        "quarantine_until": 12.5,
    }
    assert json.loads(json.dumps(meta)) == dict(meta)

    meta.priority = None
    assert "priority" not in meta

    # Construction keeps the instance as-is, so identity is preserved.
    resource = Resource(id="r1", kind="fake", meta=meta)
    assert resource.meta is meta
    assert resource.system_meta["group"] == "g1"


async def test_meta_survives_backends_and_admin_updates(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    res = await pool.get(rid)
    assert res is not None and isinstance(res.meta, ResourceMeta)

    res = await pool.update_resource(rid, priority=3, preference=0.75, reason="rank")
    assert isinstance(res.meta, ResourceMeta)
    assert res.meta.priority == 3 and res.meta.preference == 0.75

    res = await pool.update_resource(rid, priority=None, preference=None, reason="clear")
    assert isinstance(res.meta, ResourceMeta)
    assert res.meta.priority == 0 and res.meta.preference == 0.0


async def test_update_resource_scheduling_keys_stay_out_of_user_meta(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    await pool.update_resource(
        rid,
        priority=7,
        preference=0.25,
        operator="ops",
        reason="rank",
    )
    res = await pool.get(rid)
    assert res is not None
    assert res.system_meta["priority"] == 7
    assert res.system_meta["preference"] == 0.25
    assert res.user_meta == {}
    assert res.system_meta | res.user_meta == res.meta


async def test_switch_resource_marks_switched_from_as_system_meta():
    from conftest import FakeChecker

    from purser import Purser

    class SwitchingChecker(FakeChecker):
        kind = "switcher"

        async def switch(self, resource):
            return Resource(
                id="r2",
                kind=self.kind,
                payload=dict(resource.payload),
                tags=resource.tags,
            )

    ps = Purser(maintenance_enabled=False)
    ps.register_adapter(SwitchingChecker())
    await ps.start()
    try:
        pool = ps.ensure_pool("sw", "switcher")
        await pool.import_resources(
            [{"id": "r1", "host": "127.0.0.1", "port": 1}],
            import_validate="on_import",
        )
        switched = await pool.switch_resource("r1", operator="ops", reason="rotate")
        assert switched is not None and switched.id == "r2"
        res = await pool.get("r2")
        assert res is not None
        assert res.system_meta["switched_from"] == "r1"
        assert "switched_from" not in res.user_meta
    finally:
        await ps.close()


async def test_update_resource_sets_and_clears_quota_limit_and_hold(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    await pool.update_resource(
        rid,
        quota_limit=4,
        next_available_at=time.time() + 100,
        operator="ops",
        reason="edit",
    )
    res = await pool.get(rid)
    assert res is not None
    assert res.quota is not None and res.quota.limit == 4
    assert res.next_available_at is not None

    await pool.update_resource(
        rid,
        quota_limit=None,
        next_available_at=None,
        operator="ops",
        reason="clear",
    )
    res = await pool.get(rid)
    assert res is not None
    assert res.quota is None
    assert res.next_available_at is None


async def test_update_resource_rejects_invalid_quota_limit(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    with pytest.raises(ConfigError):
        await pool.update_resource(rid, quota_limit=0, reason="bad")


# -- pool default backfill ---------------------------------------------------


async def test_apply_defaults_fills_only_missing_values(make_pool):
    pool = make_pool()
    report = await pool.import_resources(
        [
            {"id": "a", "host": "127.0.0.1", "port": 1, "usage_quota": 10},
            {"id": "b", "host": "127.0.0.1", "port": 2},
        ]
    )
    assert report.imported == 2
    pool.reconfigure(expires_in=60, usage_quota=5, quota_limit=2)

    result = await pool.apply_defaults()
    assert result == {"updated": 2}

    a = await pool.get("a")
    b = await pool.get("b")
    assert a is not None and b is not None
    # Never overwrite an explicit resource-level value.
    assert a.usage_quota == 10
    assert b.usage_quota == 5
    assert a.expires_at is not None and b.expires_at is not None
    assert a.quota is not None and a.quota.limit == 2
    assert b.quota is not None and b.quota.limit == 2

    second = await pool.apply_defaults()
    assert second == {"updated": 0}


# -- expired-resource GC -----------------------------------------------------


async def test_expire_gc_is_disabled_by_default(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    res = await pool.get(rid)
    await pool.storage.set_state(
        pool.name,
        rid,
        res.version,
        expect_state=res.state,
        patch=ResourcePatch(expires_at=time.time() - 1),
    )
    with pytest.raises(PoolExhausted):
        await pool.acquire(AcquireRequest(timeout=0.05))
    result = await pool.run_maintenance("expire_gc")
    assert result == {"expire_gc": 0}
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.SUSPENDED


async def test_expire_gc_archives_expired_resources_past_retention(make_pool):
    pool = make_pool(expired_retention=0.0)
    [rid] = await seed(pool, n=1)
    res = await pool.get(rid)
    await pool.storage.set_state(
        pool.name,
        rid,
        res.version,
        expect_state=res.state,
        patch=ResourcePatch(expires_at=time.time() - 1),
    )
    with pytest.raises(PoolExhausted):
        await pool.acquire(AcquireRequest(timeout=0.05))
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.SUSPENDED
    assert res.meta.get("expired_at") is not None

    result = await pool.run_maintenance("expire_gc")
    assert result == {"expire_gc": 1}
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.ARCHIVED


async def test_expire_gc_leaves_manually_suspended_resources_alone(make_pool):
    pool = make_pool(expired_retention=0.0)
    [rid] = await seed(pool, n=1)
    await pool.suspend(rid, operator="ops", reason="manual hold")
    result = await pool.run_maintenance("expire_gc")
    assert result == {"expire_gc": 0}
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.SUSPENDED
    assert "expired_at" not in res.meta


async def test_expire_gc_respects_retention_within_window(make_pool):
    pool = make_pool(expired_retention=3600.0)
    [rid] = await seed(pool, n=1)
    res = await pool.get(rid)
    await pool.storage.set_state(
        pool.name,
        rid,
        res.version,
        expect_state=res.state,
        patch=ResourcePatch(expires_at=time.time() - 1),
    )
    with pytest.raises(PoolExhausted):
        await pool.acquire(AcquireRequest(timeout=0.05))
    result = await pool.run_maintenance("expire_gc")
    assert result == {"expire_gc": 0}
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.SUSPENDED


# -- REST surface ------------------------------------------------------------


def test_ui_update_resource_exposes_quota_limit_and_hold(make_client):
    client, _ = make_client()
    head = {"Authorization": "Bearer admin-key"}
    created = client.post(
        "/pools", json={"name": "uiq", "kind": "fakecheck"}, headers=head
    )
    assert created.status_code == 200, created.text
    imported = client.post(
        "/pools/uiq/resources",
        json={"records": [{"id": "u1", "host": "127.0.0.1", "port": 1}]},
        headers=head,
    )
    assert imported.status_code == 200, imported.text
    hold = time.time() + 100
    updated = client.put(
        "/pools/uiq/resources/u1",
        json={"quota_limit": 3, "next_available_at": hold, "reason": "ui"},
        headers=head,
    )
    assert updated.status_code == 200, updated.text
    resource = updated.json()["resource"]
    assert resource["quota"]["limit"] == 3
    assert resource["next_available_at"] == hold
    cleared = client.put(
        "/pools/uiq/resources/u1",
        json={"quota_limit": None, "next_available_at": None, "reason": "ui"},
        headers=head,
    )
    assert cleared.status_code == 200, cleared.text
    resource = cleared.json()["resource"]
    assert resource["quota"] is None
    assert resource["next_available_at"] is None
