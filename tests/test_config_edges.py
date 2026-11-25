"""Branch edges of the configuration contract (``purser.config``).

The happy path is covered by ``test_config_contract.py``; this module walks the
rejection surface: every validation guard, the nested normalisers, profile
merging and the schema read model.
"""

from __future__ import annotations

import json
import math

import pytest
from conftest import FakeChecker

from purser.config import (
    IMMUTABLE_POOL_FIELDS,
    POOL_CONFIG_SCHEMA,
    POOL_PROFILES,
    Weights,
    _json_value,
    _kind_for,
    build_pool_config,
    config_schema_payload,
    parse_pool_config,
    profile_defaults,
    resolve_pool_config,
)
from purser.config_kinds import (
    AcquireMode,
    CoordinationKind,
    ExpirePolicy,
    ImportValidate,
    SchedulerKind,
    StorageKind,
    UnbanMode,
)
from purser.core.quarantine import QuarantinePolicy
from purser.errors import ConfigError

# -- PoolConfig.__post_init__ guards --------------------------------------


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"max_slots": 0}, "max_slots must be > 0"),
        ({"allocator": "nope"}, "unknown allocator"),
        ({"max_leases_per_consumer": 0}, "max_leases_per_consumer"),
        ({"max_leases_per_consumer": True}, "max_leases_per_consumer"),
        ({"max_leases_per_consumer": "2"}, "max_leases_per_consumer"),
        ({"rate_limit": 0}, "rate_limit must be a finite number > 0"),
        ({"rate_limit": True}, "rate_limit must be a finite number > 0"),
        ({"rate_limit": math.inf}, "rate_limit must be a finite number > 0"),
        ({"rate_burst": 0}, "rate_burst must be an integer >= 1"),
        ({"rate_burst": 1.5}, "rate_burst must be an integer >= 1"),
        (
            {"scheduler": "token_bucket"},
            "scheduler='token_bucket' requires rate_limit",
        ),
        ({"background_validate_sample": 0}, "background_validate_sample"),
        ({"background_validate_sample": 1.5}, "background_validate_sample"),
    ],
)
def test_post_init_rejects_scheduling_knobs(kwargs, match):
    from purser.config import PoolConfig

    with pytest.raises(ConfigError, match=match):
        PoolConfig(kind="k", **kwargs)


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"lease_ttl": 0}, "lease_ttl must be > 0"),
        ({"wait_timeout": -1.0}, "wait_timeout must be a finite"),
        ({"background_interval": math.inf}, "background_interval must be a finite"),
        ({"stale_after": -1.0}, "stale_after must be a finite"),
        ({"validate_timeout": -1.0}, "validate_timeout must be a finite"),
        ({"check_cache_ttl": -1.0}, "check_cache_ttl must be a finite"),
        ({"site_failure_ttl": -1.0}, "site_failure_ttl must be a finite"),
        ({"heartbeat_interval": 0}, "heartbeat_interval must be finite and > 0"),
        ({"top_k": 0}, "top_k must be > 0"),
        ({"usage_quota": -1}, "usage_quota must be a non-negative integer"),
        ({"usage_quota": "3"}, "usage_quota must be a non-negative integer"),
        ({"quota_limit": 0}, "quota_limit must be a positive integer"),
        ({"quota_limit": True}, "quota_limit must be a positive integer"),
        ({"quota_limit": "3"}, "quota_limit must be a positive integer"),
        ({"requalify_on_release": "yes"}, "requalify_on_release"),
        ({"expires_in": -1.0}, "expires_in must be finite and >= 0"),
        ({"expires_in": math.nan}, "expires_in must be finite and >= 0"),
        ({"max_lease_duration": 0}, "max_lease_duration must be finite and > 0"),
        ({"quota_cooldown": -1.0}, "quota_cooldown"),
        ({"unban_after": -1.0}, "unban_after"),
        ({"validate_concurrency": 0}, "validate_concurrency"),
        ({"site_failure_max": 0}, "site_failure_max"),
        ({"event_queue_size": 0}, "event_queue_size"),
        ({"drain_timeout": -1.0}, "drain_timeout must be a finite"),
        ({"archive_retention": -1.0}, "archive_retention must be a finite"),
        ({"lost_retention": -1.0}, "lost_retention must be a finite"),
        ({"expired_retention": -1.0}, "expired_retention"),
        ({"schema_version": 2}, "unsupported schema_version"),
        ({"preempt_threshold": "x"}, "preempt_threshold must be an integer"),
        ({"preempt_threshold": -1}, "preempt_threshold must be >= 0"),
        ({"fail_open": "yes"}, "fail_open must be a boolean"),
        ({"preemptible": 0}, "preemptible must be a boolean"),
    ],
)
def test_post_init_rejects_lifecycle_knobs(kwargs, match):
    from purser.config import PoolConfig

    with pytest.raises(ConfigError, match=match):
        PoolConfig(kind="k", **kwargs)


@pytest.mark.parametrize(
    ("intervals", "match"),
    [
        ([], "scheduler_intervals must be an object"),
        ({"nope": 1.0}, "unknown maintenance task"),
        ({"quota_reset": True}, "must be a positive number"),
        ({"quota_reset": "1"}, "must be a positive number"),
        ({"quota_reset": 0}, "must be a positive number"),
        ({"quota_reset": math.inf}, "must be a positive number"),
    ],
)
def test_post_init_rejects_scheduler_interval_overrides(intervals, match):
    from purser.config import PoolConfig

    with pytest.raises(ConfigError, match=match):
        PoolConfig(kind="k", scheduler_intervals=intervals)


@pytest.mark.parametrize(
    ("quarantine", "match"),
    [
        ({"base": -1.0}, r"quarantine\.base"),
        ({"cap": math.inf}, r"quarantine\.cap"),
        ({"max_failures": 0}, r"quarantine\.max_failures"),
    ],
)
def test_post_init_rejects_quarantine_values(quarantine, match):
    from purser.config import PoolConfig

    with pytest.raises(ConfigError, match=match):
        PoolConfig(kind="k", quarantine=quarantine)


def test_quarantine_mapping_drops_rng_and_accepts_partial_values():
    from purser.config import PoolConfig

    cfg = PoolConfig(kind="k", quarantine={"base": 7.0, "rng": object()})
    assert isinstance(cfg.quarantine, QuarantinePolicy)
    assert cfg.quarantine.base == 7.0


def test_weights_dict_is_merged_and_empty_dict_keeps_defaults():
    from purser.config import PoolConfig

    cfg = PoolConfig(kind="k", weights={"health": 2.0}, affinity_weight=0.5)
    assert isinstance(cfg.weights, Weights)
    assert cfg.weights.health == 2.0
    assert cfg.weights.affinity == 0.5  # derived from affinity_weight
    assert Weights.from_dict(None) == Weights()
    assert Weights.from_dict({"load": 3}) == Weights(load=3.0)


def test_empty_kind_and_heartbeat_default():
    from purser.config import PoolConfig

    with pytest.raises(ConfigError, match="kind must not be empty"):
        PoolConfig(kind="")
    cfg = PoolConfig(kind="k", lease_ttl=3.0)
    assert cfg.heartbeat_interval == pytest.approx(1.0)


def test_with_overrides_revalidates_and_recomputes_derived_values():
    from purser.config import PoolConfig

    cfg = PoolConfig(kind="k")
    updated = cfg.with_overrides(lease_ttl=9.0, heartbeat_interval=None)
    assert updated.lease_ttl == 9.0
    assert updated.heartbeat_interval == pytest.approx(3.0)
    assert cfg.lease_ttl == 60.0  # original untouched
    with pytest.raises(ConfigError, match="lease_ttl must be > 0"):
        cfg.with_overrides(lease_ttl=0)
    with pytest.raises(TypeError):
        cfg.with_overrides(not_a_field=1)


def test_default_for_applies_adapter_defaults_then_call_overrides():
    from purser.config import PoolConfig

    cfg = PoolConfig.default_for("fakecheck", adapter=FakeChecker())
    assert cfg.max_slots == FakeChecker.default_concurrency
    overridden = PoolConfig.default_for(
        "fakecheck", adapter=FakeChecker(), max_slots=5, lease_ttl=11
    )
    assert overridden.max_slots == 5
    assert overridden.lease_ttl == 11
    # Without an adapter the config is built from the kind alone.
    assert PoolConfig.default_for("fakecheck").max_slots is None


def test_adapter_defaults_fill_secret_fields_only_when_missing():
    from purser.adapters.base import ResourceAdapter
    from purser.config import PoolConfig

    class SecretAdapter(ResourceAdapter):
        kind = "secretcheck"
        default_concurrency = 2
        secret_fields = ("password",)

    cfg = PoolConfig.default_for("secretcheck", adapter=SecretAdapter())
    assert cfg.max_slots == 2
    assert cfg.secret_fields == ("password",)
    explicit = PoolConfig.default_for(
        "secretcheck", adapter=SecretAdapter(), secret_fields=("token",)
    )
    assert explicit.secret_fields == ("token",)


def test_scheduler_intervals_accept_multiple_valid_entries():
    from purser.config import PoolConfig

    cfg = PoolConfig(
        kind="k",
        scheduler_intervals={"quota_reset": 10, "periodic_unban": 20.5},
    )
    assert cfg.scheduler_intervals == {"quota_reset": 10, "periodic_unban": 20.5}


# -- enum coercion ---------------------------------------------------------


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("storage", "bogus"),
        ("coordination", "bogus"),
        ("scheduler", "bogus"),
        ("acquire_mode", "bogus"),
        ("unban_mode", "bogus"),
        ("import_validate", "bogus"),
        ("expire_policy", "bogus"),
    ],
)
def test_unknown_enum_values_name_the_field_and_choices(field, bad):
    from purser.config import PoolConfig

    with pytest.raises(ConfigError) as err:
        PoolConfig(kind="k", **{field: bad})
    message = str(err.value)
    assert f"unknown {field}" in message
    assert "expected one of" in message


def test_enum_instances_are_accepted_as_is():
    from purser.config import PoolConfig

    cfg = PoolConfig(
        kind="k",
        storage=StorageKind.SQLITE,
        coordination=CoordinationKind.MEMORY,
        scheduler=SchedulerKind.FIFO,
        acquire_mode=AcquireMode.LEASE,
        unban_mode=UnbanMode.MIXED,
        import_validate=ImportValidate.NONE,
        expire_policy=ExpirePolicy.QUOTA,
    )
    assert cfg.storage is StorageKind.SQLITE


# -- parse / normalise -----------------------------------------------------


def test_parse_accepts_none_and_poolconfig_instances():
    from purser.config import PoolConfig

    assert parse_pool_config(None) == {}
    cfg = PoolConfig(kind="k", lease_ttl=7.0)
    parsed = parse_pool_config(cfg)
    assert "kind" not in parsed
    assert parsed["lease_ttl"] == 7.0


@pytest.mark.parametrize("raw", [5, "cfg", [("lease_ttl", 1)]])
def test_parse_rejects_non_mapping_payloads(raw):
    with pytest.raises(ConfigError, match="config must be an object"):
        parse_pool_config(raw)


def test_parse_mode_and_patch_base_are_validated():
    with pytest.raises(ConfigError, match="mode must be 'create' or 'patch'"):
        parse_pool_config({}, mode="update")
    with pytest.raises(ConfigError, match="patch mode requires"):
        parse_pool_config({"lease_ttl": 1}, mode="patch")


def test_parse_rejects_inline_profile_and_null_non_nullable_fields():
    with pytest.raises(ConfigError, match="profile is a create-time preset"):
        parse_pool_config({"profile": "basic"})
    with pytest.raises(ConfigError, match="lease_ttl cannot be null"):
        parse_pool_config({"lease_ttl": None})


def test_parse_patch_rejects_an_immutable_change():
    from purser.config import PoolConfig

    with pytest.raises(ConfigError, match="cannot change storage"):
        parse_pool_config(
            {"storage": "sqlite"}, mode="patch", base=PoolConfig(kind="k")
        )


@pytest.mark.parametrize(
    ("payload", "match"),
    [
        ({"secret_fields": "password"}, "must be a list of non-empty strings"),
        ({"secret_fields": 5}, "must be a list of non-empty strings"),
        ({"secret_fields": [" "]}, "must not contain empty names"),
        ({"weights": "heavy"}, "weights must be an object"),
        ({"quarantine": ["base"]}, "quarantine must be an object"),
        ({"scheduler_intervals": 5}, "scheduler_intervals must be an object"),
        ({"meta": 5}, "meta must be an object"),
        ({"weights": {"nope": 1}}, "unknown weights field"),
        ({"weights": {"health": True}}, r"weights\.health must be a number"),
        ({"weights": {"health": "x"}}, r"weights\.health must be a number"),
        ({"weights": {"health": math.inf}}, r"weights\.health must be finite"),
        ({"quarantine": {"nope": 1}}, "unknown quarantine field"),
        ({"quarantine": {"base": "x"}}, r"quarantine\.base must be a number"),
        ({"quarantine": {"base": -1}}, r"quarantine\.base must be a finite"),
        (
            {"quarantine": {"max_failures": 0}},
            r"quarantine\.max_failures must be an integer >= 1",
        ),
    ],
)
def test_nested_normalisers_reject_bad_shapes(payload, match):
    with pytest.raises(ConfigError, match=match):
        parse_pool_config(payload)


def test_nested_normalisers_convert_valid_values():
    parsed = parse_pool_config(
        {
            "secret_fields": [" password ", "token"],
            "weights": {"health": 2},
            "quarantine": {"base": 3},
            "scheduler_intervals": {"quota_reset": 10},
            "meta": None,
        }
    )
    assert parsed["secret_fields"] == ("password", "token")
    assert parsed["weights"] == {"health": 2}
    assert parsed["quarantine"] == {"base": 3}
    assert parsed["scheduler_intervals"] == {"quota_reset": 10}
    assert parsed["meta"] == {}


def test_patch_mode_pops_repeated_immutable_values():
    from purser.config import PoolConfig

    base = PoolConfig(kind="k")
    parsed = parse_pool_config(
        {
            "storage": str(base.storage),
            "coordination": str(base.coordination),
            "secret_fields": list(base.secret_fields),
            "event_queue_size": base.event_queue_size,
            "schema_version": base.schema_version,
            "lease_ttl": 5,
        },
        mode="patch",
        base=base,
    )
    assert parsed == {"lease_ttl": 5}
    assert IMMUTABLE_POOL_FIELDS  # documented set is non-empty


# -- profiles --------------------------------------------------------------


def test_profile_defaults_accepts_none_name_and_inline_mapping():
    assert profile_defaults(None) == {}
    assert profile_defaults("basic")["import_validate"] == "on_import"
    assert profile_defaults({"lease_ttl": 4}) == {"lease_ttl": 4}
    with pytest.raises(ConfigError, match="profile must be a name or an object"):
        profile_defaults(5)
    with pytest.raises(ConfigError, match="unknown profile"):
        profile_defaults("nope")
    assert "basic" in POOL_PROFILES


def test_resolve_rejects_profile_and_poolconfig_combination():
    from purser.config import PoolConfig

    cfg = PoolConfig(kind="k")
    with pytest.raises(ConfigError, match="cannot be combined with a PoolConfig"):
        resolve_pool_config("k", profile="basic", config=cfg)
    assert resolve_pool_config("k", config=cfg) is cfg


def test_resolve_reads_embedded_profile_and_validates_config_type():
    cfg = resolve_pool_config("k", config={"profile": "basic", "lease_ttl": 8})
    assert cfg.import_validate is ImportValidate.ON_IMPORT
    assert cfg.lease_ttl == 8
    with pytest.raises(ConfigError, match="config must be an object or PoolConfig"):
        resolve_pool_config("k", config=5)
    with pytest.raises(ConfigError, match="config must be an object"):
        resolve_pool_config("k", overrides=5)
    with pytest.raises(ConfigError, match="unknown pool config field"):
        resolve_pool_config("k", overrides={"nope": 1})


def test_resolve_applies_defaults_profile_config_and_overrides_in_order():
    cfg = resolve_pool_config(
        "k",
        defaults={"lease_ttl": 1, "top_k": 2},
        profile={"lease_ttl": 2},
        config={"lease_ttl": 3},
        overrides={"lease_ttl": 4},
    )
    assert cfg.lease_ttl == 4
    assert cfg.top_k == 2


# -- build / schema --------------------------------------------------------


def test_build_pool_config_keeps_the_error_surface_stable():
    with pytest.raises(ConfigError, match="config must be an object"):
        build_pool_config("k", None)
    with pytest.raises(ConfigError, match="invalid pool config"):
        build_pool_config("k", {"lease_ttl": "soon"})
    with pytest.raises(ConfigError, match="invalid pool config"):
        build_pool_config("k", {"not_a_field": 1})
    with pytest.raises(ConfigError, match="max_slots must be > 0"):
        build_pool_config("k", {"max_slots": 0})
    with pytest.raises(ConfigError, match="kind must not be empty"):
        build_pool_config("", {})


def test_kind_for_classifies_every_default_type():
    assert _kind_for(True) == "bool"
    assert _kind_for(1) == "int"
    assert _kind_for(1.5) == "float"
    assert _kind_for("x") == "string"
    assert _kind_for(["x"]) == "array"
    assert _kind_for({"x": 1}) == "object"
    assert _kind_for(object()) == "string"


def test_json_value_converts_enums_collections_and_dataclasses():
    assert _json_value(StorageKind.SQLITE) == "sqlite"
    assert _json_value(None) is None
    assert _json_value("x") == "x"
    assert _json_value([StorageKind.MEMORY]) == ["memory"]
    assert _json_value({"k": StorageKind.SQLITE}) == {"k": "sqlite"}
    payload = _json_value(Weights())
    assert payload["health"] == 1.0
    assert _json_value(object()).startswith("<object object")


def test_schema_payload_is_json_safe_and_lists_nested_fields():
    payload = config_schema_payload()
    json.dumps(payload)
    fields = {item["name"]: item for item in payload["fields"]}
    assert [item["name"] for item in fields["weights"]["fields"]] == [
        "health",
        "age",
        "load",
        "preference",
        "affinity",
        "jitter",
    ]
    assert {item["name"] for item in fields["quarantine"]["fields"]} == {
        "base",
        "cap",
        "jitter",
        "max_failures",
        "trial_hours",
    }
    assert fields["allocator"]["choices"] == sorted(
        POOL_CONFIG_SCHEMA["allocator"].choices
    )
    assert POOL_CONFIG_SCHEMA["max_lease_duration"].nullable is True
    assert POOL_CONFIG_SCHEMA["lease_ttl"].nullable is False
    assert fields["storage"]["default"] == "memory"


def test_to_dict_round_trips_through_from_dict():
    from purser.config import PoolConfig

    cfg = PoolConfig(kind="k", lease_ttl=12, quota_limit=3)
    again = PoolConfig.from_dict(cfg.to_dict(), kind="k")
    assert again.lease_ttl == 12
    assert again.quota_limit == 3
    with pytest.raises(ConfigError, match="kind must not be empty"):
        PoolConfig.from_dict({"lease_ttl": 1})
    with pytest.raises(ConfigError, match="config must be an object"):
        PoolConfig.from_dict(5, kind="k")


def test_quarantine_policy_default_is_shared_but_not_aliased():
    from purser.config import PoolConfig

    first = PoolConfig(kind="k")
    second = PoolConfig(kind="k")
    assert first.quarantine is not second.quarantine
    assert first.quarantine.base == second.quarantine.base
    assert first.quarantine.max_failures == second.quarantine.max_failures


def test_weights_affinity_is_derived_from_affinity_weight():
    from purser.config import PoolConfig

    cfg = PoolConfig(kind="k", affinity_weight=0.75)
    assert cfg.to_dict()["weights"]["affinity"] == 0.75
