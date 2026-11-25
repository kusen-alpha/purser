"""One configuration contract across Python, REST and the local scheduler."""

from __future__ import annotations

import json
from dataclasses import fields

import pytest

from purser import (
    AcquireMode,
    BundleMode,
    ConfigError,
    CoordinationKind,
    ExpirePolicy,
    ImportValidate,
    PoolConfig,
    Purser,
    SchedulerKind,
    StorageKind,
    UnbanMode,
)
from purser.config import (
    IMMUTABLE_POOL_FIELDS,
    POOL_CONFIG_SCHEMA,
    POOL_PROFILES,
    RESOURCE_SCHEDULING_SCHEMA,
    config_schema_payload,
    parse_pool_config,
    resolve_pool_config,
)


def auth(key: str = "admin-key") -> dict[str, str]:
    return {"Authorization": f"Bearer {key}"}


# -- schema ---------------------------------------------------------------


def test_schema_is_the_single_field_inventory():
    declared = {field.name for field in fields(PoolConfig)} - {"kind"}
    assert set(POOL_CONFIG_SCHEMA) == declared
    assert set(config_schema_payload()["fields"][0]) >= {
        "name",
        "type",
        "group",
        "label",
        "mutable",
        "nullable",
        "hidden",
    }
    assert set(POOL_CONFIG_SCHEMA["weights"].fields) == {
        "health",
        "age",
        "load",
        "preference",
        "affinity",
        "jitter",
    }
    assert set(RESOURCE_SCHEDULING_SCHEMA) == {
        "max_slots",
        "usage_quota",
        "quota_limit",
        "expires_at",
        "next_available_at",
        "mutex_group",
        "schedule_window",
        "priority",
        "preference",
    }
    assert set(IMMUTABLE_POOL_FIELDS) <= declared


def test_schema_payload_exposes_profiles_and_marks_immutable_fields():
    payload = config_schema_payload()
    assert {item["name"] for item in payload["profiles"]} == set(POOL_PROFILES)
    assert tuple(payload["immutable"]) == tuple(sorted(IMMUTABLE_POOL_FIELDS))
    by_name = {item["name"]: item for item in payload["fields"]}
    assert by_name["storage"]["mutable"] is False
    assert by_name["scheduler"]["hidden"] is False
    assert by_name["acquire_mode"]["choices"] == ["lease", "stateless"]
    assert by_name["quota_cooldown"]["nullable"] is False
    assert by_name["max_lease_duration"]["nullable"] is True


# -- parsing --------------------------------------------------------------


def test_from_dict_applies_named_profile_then_explicit_values():
    cfg = PoolConfig.from_dict(
        {
            "kind": "proxy",
            "profile": "basic",
            "lease_ttl": 12,
            "quarantine": {"base": 5},
            "secret_fields": ["password"],
        }
    )
    assert cfg.lease_ttl == 12
    assert cfg.import_validate == "on_import"
    assert cfg.requalify_on_release is True
    assert cfg.quarantine.base == 5
    assert cfg.secret_fields == ("password",)


def test_parse_rejects_unknown_fields_and_inline_profile():
    with pytest.raises(ConfigError, match="unknown pool config field"):
        parse_pool_config({"lease_tll": 10})
    with pytest.raises(ConfigError, match="profile is a create-time preset"):
        parse_pool_config({"profile": "basic"})


def test_patch_drops_unchanged_immutables_and_rejects_real_changes():
    base = resolve_pool_config(
        "proxy", config={"storage": "memory", "event_queue_size": 5000}
    )
    unchanged = parse_pool_config(
        {"storage": "memory", "event_queue_size": 5000, "lease_ttl": 30},
        mode="patch",
        base=base,
    )
    assert unchanged == {"lease_ttl": 30}
    with pytest.raises(ConfigError, match="cannot change storage"):
        parse_pool_config({"storage": "sqlite"}, mode="patch", base=base)


def test_null_clear_is_only_allowed_for_nullable_fields():
    assert parse_pool_config({"max_lease_duration": None}) == {
        "max_lease_duration": None
    }
    with pytest.raises(ConfigError, match="lease_ttl cannot be null"):
        parse_pool_config({"lease_ttl": None})


def test_unknown_profile_lists_available_names():
    with pytest.raises(ConfigError, match="available: .*basic"):
        resolve_pool_config("proxy", profile="missing")


# -- typed kebab values ---------------------------------------------------


def test_kind_enums_coerce_and_stay_plain_strings_outside_python():
    cfg = PoolConfig(
        kind="proxy",
        storage="sqlite",
        coordination="redis",
        scheduler="fifo",
        acquire_mode="stateless",
        unban_mode="manual_only",
        import_validate="none",
        expire_policy="quota",
    )
    assert cfg.storage is StorageKind.SQLITE
    assert cfg.coordination is CoordinationKind.REDIS
    assert cfg.scheduler is SchedulerKind.FIFO
    assert cfg.acquire_mode is AcquireMode.STATELESS
    assert cfg.unban_mode is UnbanMode.MANUAL_ONLY
    assert cfg.import_validate is ImportValidate.NONE
    assert cfg.expire_policy is ExpirePolicy.QUOTA

    assert f"{cfg.storage}" == "sqlite"
    assert f"{cfg.acquire_mode}" == "stateless"
    dumped = cfg.to_dict()
    assert dumped["storage"] == "sqlite"
    assert type(dumped["storage"]) is str
    json.dumps(dumped)


def test_schema_choices_come_from_the_enums():
    by_name = {item["name"]: item for item in config_schema_payload()["fields"]}
    assert by_name["storage"]["choices"] == ["memory", "sqlite", "mysql"]
    assert by_name["coordination"]["choices"] == ["memory", "redis"]
    assert by_name["scheduler"]["choices"] == [
        "priority_quota",
        "fifo",
        "token_bucket",
    ]
    assert by_name["storage"]["default"] == "memory"


def test_unknown_kind_reports_the_expected_choices():
    with pytest.raises(ConfigError, match="unknown storage 'bogus'.*memory"):
        PoolConfig(kind="proxy", storage="bogus")
    with pytest.raises(ConfigError, match="unknown acquire_mode 'bogus'"):
        PoolConfig(kind="proxy", acquire_mode="bogus")


def test_probe_targets_is_not_a_pool_field():
    assert "probe_targets" not in POOL_CONFIG_SCHEMA
    with pytest.raises(ConfigError, match="unknown pool config field"):
        resolve_pool_config("proxy", config={"probe_targets": ["http://x"]})


def test_bundle_mode_is_typed():
    ps = Purser(bundle_mode="two_phase")
    assert ps.bundle_mode is BundleMode.TWO_PHASE
    assert f"{ps.bundle_mode}" == "two_phase"
    with pytest.raises(ConfigError, match="unknown bundle_mode"):
        Purser(bundle_mode="bogus")


# -- Python facade --------------------------------------------------------


async def test_create_pool_profile_and_overrides_precedence(purser: Purser):
    pool = purser.create_pool(
        "contract-py",
        "fakecheck",
        profile="basic",
        config={"lease_ttl": 15, "requalify_on_release": False},
        requalify_on_release=True,
    )
    assert pool.profile == "basic"
    assert pool.cfg.lease_ttl == 15
    assert pool.cfg.import_validate == "on_import"
    assert pool.cfg.requalify_on_release is True

    inline = purser.create_pool(
        "contract-inline",
        "fakecheck",
        profile={"lease_ttl": 9, "import_validate": "batch"},
    )
    assert inline.profile is None
    assert inline.cfg.lease_ttl == 9
    assert inline.cfg.import_validate == "batch"


# -- REST -----------------------------------------------------------------


def test_rest_schema_and_profile_create(make_client):
    client, _ = make_client()
    schema = client.get("/config/schema", headers=auth())
    assert schema.status_code == 200
    body = schema.json()
    assert body["profiles"]
    assert {item["name"] for item in body["resource_fields"]} >= {
        "priority",
        "preference",
    }

    created = client.post(
        "/pools",
        json={
            "name": "contract-rest",
            "kind": "fakecheck",
            "profile": "basic",
            "config": {
                "max_slots": None,
                "requalify_on_release": False,
                "lease_ttl": 11,
            },
        },
        headers=auth(),
    )
    assert created.status_code == 200, created.text
    assert created.json()["profile"] == "basic"
    stats = client.get("/pools/contract-rest", headers=auth()).json()
    assert stats["profile"] == "basic"


def test_rest_patch_clears_nullable_but_refuses_immutable(make_client):
    client, _ = make_client()
    assert (
        client.post(
            "/pools",
            json={
                "name": "contract-patch",
                "kind": "fakecheck",
                "config": {"max_slots": 2, "lease_ttl": 30},
            },
            headers=auth(),
        ).status_code
        == 200
    )
    patched = client.patch(
        "/pools/contract-patch",
        json={"config": {"max_slots": None, "lease_ttl": 45}},
        headers=auth(),
    )
    assert patched.status_code == 200, patched.text
    assert patched.json()["config"]["max_slots"] is None
    assert patched.json()["config"]["lease_ttl"] == 45

    refused = client.patch(
        "/pools/contract-patch",
        json={"config": {"storage": "sqlite"}},
        headers=auth(),
    )
    assert refused.status_code == 422


def test_rest_resource_scheduling_fields_roundtrip(make_client):
    client, _ = make_client()
    assert (
        client.post(
            "/pools",
            json={"name": "contract-res", "kind": "fakecheck"},
            headers=auth(),
        ).status_code
        == 200
    )
    added = client.post(
        "/pools/contract-res/resources",
        json={
            "records": [
                {
                    "id": "r1",
                    "host": "10.0.0.1",
                    "port": 80,
                    "priority": 3,
                    "preference": 0.5,
                    "quota_limit": 10,
                }
            ]
        },
        headers=auth(),
    )
    assert added.status_code == 200, added.text
    listed = client.get(
        "/pools/contract-res/resources?limit=10", headers=auth()
    ).json()
    item = next(row for row in listed["resources"] if row["id"] == "r1")
    assert item["meta"]["priority"] == 3
    assert item["meta"]["preference"] == 0.5
    assert item["quota"]["limit"] == 10

    updated = client.put(
        "/pools/contract-res/resources/r1",
        json={"priority": None, "preference": 0.25},
        headers=auth(),
    )
    assert updated.status_code == 200, updated.text
    listed = client.get(
        "/pools/contract-res/resources?limit=10", headers=auth()
    ).json()
    item = next(row for row in listed["resources"] if row["id"] == "r1")
    assert "priority" not in item["meta"]
    assert item["meta"]["preference"] == 0.25
