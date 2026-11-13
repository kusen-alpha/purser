"""Regression suite for the hardening pass (P0/P1 defects).

Every test here pins a defect that was reproduced during the backtest audit:
quota reset consistency, storage CAS, audit-chain forking, key rotation,
duplicate/lost lease slot accounting, state-machine bypass, API input
validation, SSE hygiene, scheduler independence, config validation and
cross-instance idempotency.
"""

from __future__ import annotations

import asyncio
import copy
import json
import sqlite3
import sys
import time

import fakeredis
import pytest
from conftest import FakeChecker, seed

from purser import Purser
from purser.backends.base import TransitionResult
from purser.backends.memory import MemoryStorage
from purser.backends.sqlite import SqliteStorage
from purser.config import PoolConfig
from purser.core.audit import AuditLog
from purser.core.events import EventBus
from purser.core.resource import AcquireRequest, Resource
from purser.core.scheduler import PeriodicScheduler
from purser.core.state import ResourceState
from purser.crypto import SecretCodec
from purser.errors import (
    BadPayload,
    BadState,
    ConfigError,
    LeaseConflict,
    VersionConflict,
)
from purser.idempotency import RedisIdempotencyStore

HEAD = {"Authorization": "Bearer admin-key"}


def make_resource(rid: str, **kwargs: object) -> Resource:
    payload = {"host": "127.0.0.1", "port": 8080}
    payload.update(kwargs.pop("payload", {}))  # type: ignore[arg-type]
    return Resource(
        id=rid,
        kind="fakecheck",
        payload=payload,
        state=ResourceState.NEW,
        **kwargs,  # type: ignore[arg-type]
    )


# -- quota reset -----------------------------------------------------------


async def test_quota_reset_is_not_reapplied_within_same_day(make_pool):
    pool = make_pool(quota_limit=1)
    [rid] = await seed(pool, n=1)

    lease = await pool.acquire(
        AcquireRequest(consumer="c", target="site1", timeout=2.0)
    )
    await pool.release(lease, used=1)
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.QUOTA_EXHAUSTED

    assert (await pool.run_maintenance("quota_reset"))["quota_reset"] == 1
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.IDLE
    assert res.quota is not None and res.quota.used == 0
    assert res.quota.reset_at is not None and res.quota.reset_at > time.time()

    lease = await pool.acquire(
        AcquireRequest(consumer="c", target="site1", timeout=2.0)
    )
    await pool.release(lease, used=1)
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.QUOTA_EXHAUSTED

    # Same day: reset_daily() refuses, so the resource must stay exhausted
    # instead of being flipped to IDLE with spent quota (which used to make
    # the next acquire fail with PoolExhausted).
    assert (await pool.run_maintenance("quota_reset"))["quota_reset"] == 0
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.QUOTA_EXHAUSTED
    assert res.quota is not None and res.quota.used == 1


# -- storage CAS / import --------------------------------------------------


async def _backend(storage_kind: str, tmp_path) -> object:
    if storage_kind == "memory":
        return MemoryStorage()
    return SqliteStorage(path=str(tmp_path / "cas.db"))


async def test_put_expected_version_guards_stale_writes(storage_kind, tmp_path):
    backend = await _backend(storage_kind, tmp_path)
    stored = await backend.put("p", make_resource("r1"))  # type: ignore[attr-defined]
    assert stored.version == 1

    with pytest.raises(VersionConflict):
        await backend.put("p", make_resource("r1"), expected_version=0)  # type: ignore[attr-defined]

    updated = await backend.put(  # type: ignore[attr-defined]
        "p", make_resource("r1", payload={"host": "10.0.0.2"}), expected_version=1
    )
    assert updated.version == 2 and updated.payload["host"] == "10.0.0.2"

    with pytest.raises(VersionConflict):
        await backend.put(  # type: ignore[attr-defined]
            "p", make_resource("r1"), expected_version=1
        )


async def test_import_retries_after_stale_snapshot(make_pool, monkeypatch):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    snapshot = await pool.storage.get(pool.name, rid)
    assert snapshot is not None

    # Another writer bumps the stored version after we took our snapshot.
    await pool.storage.put(pool.name, copy.deepcopy(snapshot))
    bumped = await pool.storage.get(pool.name, rid)
    assert bumped is not None
    baseline = bumped.version
    real_get = pool.storage.get
    injected = {"done": False}

    async def stale_get(name: str, resource_id: str):
        if name == pool.name and resource_id == rid and not injected["done"]:
            injected["done"] = True
            return snapshot
        return await real_get(name, resource_id)

    monkeypatch.setattr(pool.storage, "get", stale_get)
    report = await pool.import_resources(
        [{"id": rid, "host": "10.9.9.9", "port": 1}]
    )
    assert injected["done"] is True
    assert report.updated == 1 and report.imported == 0
    res = await pool.get(rid)
    assert res is not None and res.version == baseline + 1
    assert res.payload["host"] == "10.9.9.9"


# -- audit chain -----------------------------------------------------------


def test_audit_chain_survives_concurrent_instances(tmp_path):
    db = str(tmp_path / "audit.db")
    a = AuditLog(sqlite_path=db)
    b = AuditLog(sqlite_path=db)
    try:
        a.record(operator="a", action="one")
        b.record(operator="b", action="two")
        a.record(operator="a", action="three")
    finally:
        a.close()
        b.close()

    reader = AuditLog(sqlite_path=db)
    try:
        entries = reader.entries()
        assert [(e.seq, e.action) for e in entries] == [
            (1, "one"),
            (2, "two"),
            (3, "three"),
        ]
        assert [e.prev_hash for e in entries] == [
            "",
            entries[0].digest,
            entries[1].digest,
        ]
        assert reader.verify()
    finally:
        reader.close()


def test_audit_detects_on_disk_tampering(tmp_path):
    db = str(tmp_path / "audit.db")
    log = AuditLog(sqlite_path=db)
    log.record(operator="a", action="one")
    log.close()

    conn = sqlite3.connect(db)
    try:
        conn.execute("UPDATE audit_log SET reason = 'forged' WHERE seq = 1")
        conn.commit()
    finally:
        conn.close()

    reader = AuditLog(sqlite_path=db)
    try:
        assert not reader.verify()
    finally:
        reader.close()


# -- crypto ----------------------------------------------------------------


def test_secret_codec_rejects_short_keys():
    with pytest.raises(ConfigError):
        SecretCodec(key=b"too-short")


def test_secret_codec_honours_key_id_and_rotation():
    old = SecretCodec(key=b"a" * 32, key_id="1")
    stored = old.encrypt_payload({"password": "s3cret"}, ("password",))
    assert stored["_enc"]["password"]["k"] == "1"

    rotated = SecretCodec(
        key=b"b" * 32, key_id="2", previous_keys={"1": b"a" * 32}
    )
    assert rotated.decrypt_payload(stored, ("password",)) == {"password": "s3cret"}

    unknown = SecretCodec(key=b"b" * 32, key_id="2")
    with pytest.raises(BadPayload):
        unknown.decrypt_payload(stored, ("password",))


def test_mysql_secret_fields_require_explicit_key():
    ps = Purser(storage="mysql", mysql_url="mysql://user:pass@localhost/db")
    ps.register_adapter(FakeChecker())
    try:
        with pytest.raises(ConfigError):
            ps.create_pool(
                "p", "fakecheck", storage="mysql", secret_fields=("password",)
            )
    finally:
        ps.audit.close()


def test_sqlite_storage_requires_explicit_path():
    ps = Purser(storage="sqlite")
    ps.register_adapter(FakeChecker())
    try:
        with pytest.raises(ConfigError):
            ps.create_pool("p", "fakecheck")
    finally:
        ps.audit.close()


# -- lease accounting ------------------------------------------------------


async def test_concurrent_release_returns_slots_once(make_pool):
    pool = make_pool(max_slots=1)
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    results = await asyncio.gather(
        pool.release(lease), pool.release(lease), return_exceptions=True
    )
    assert results == [True, True]
    res = await pool.get(rid)
    assert res is not None and res.concurrency.used_slots == 0

    again = await pool.acquire(AcquireRequest(consumer="c2", timeout=0.2))
    await pool.release(again)


async def test_lost_lease_returns_storage_slots(make_pool):
    pool = make_pool(max_slots=1)
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c", use_limit=1))
    lease.used = 1  # the heartbeat now refuses to renew

    await pool._on_lease_lost(lease)

    res = await pool.get(rid)
    assert res is not None and res.concurrency.used_slots == 0
    assert await pool._coordination.get_lease(pool.name, lease.token) is None
    again = await pool.acquire(AcquireRequest(consumer="c2", timeout=0.2))
    assert again.resource_id == rid
    await pool.release(again)


async def test_release_restores_token_when_storage_rejects(make_pool, monkeypatch):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    real = pool.storage.transition_release
    calls = {"n": 0}

    async def failing(name: str, resource_id: str, slots: int):
        calls["n"] += 1
        if calls["n"] == 1:
            return TransitionResult(ok=False)
        return await real(name, resource_id, slots)

    monkeypatch.setattr(pool.storage, "transition_release", failing)
    with pytest.raises(LeaseConflict):
        await pool.release(lease)
    held = await pool._coordination.get_lease(pool.name, lease.token)
    assert held is not None  # token restored, no orphaned slot

    assert await pool.release(lease) is True
    res = await pool.get(rid)
    assert res is not None and res.concurrency.used_slots == 0
    again = await pool.acquire(AcquireRequest(consumer="c2", timeout=0.2))
    await pool.release(again)


# -- state machine ---------------------------------------------------------


async def test_admin_set_state_rejects_unlisted_source(make_pool):
    pool = make_pool()
    await pool.import_resources([{"id": "new-1", "host": "10.0.0.1", "port": 1}])
    res = await pool.get("new-1")
    assert res is not None and res.state is ResourceState.NEW
    with pytest.raises(BadState):
        await pool.admin_set_state(
            "new-1",
            target=ResourceState.SUSPENDED,
            operator="ops",
            reason="not allowed from NEW",
        )


async def test_admin_archive_from_idle(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    await pool.admin_set_state(
        rid, target=ResourceState.ARCHIVED, operator="ops", reason="delete"
    )
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.ARCHIVED


# -- API input validation --------------------------------------------------


def test_api_rejects_unhashable_tags_and_bad_resource_id(make_client):
    client, _ = make_client()
    client.post("/pools", json={"name": "px", "kind": "fakecheck"}, headers=HEAD)
    client.post(
        "/pools/px/resources",
        json={"records": [{"id": "r1"}], "import_validate": "on_import"},
        headers=HEAD,
    )
    bad_tags = client.post(
        "/pools/px/leases", json={"tags": [{}, {}]}, headers=HEAD
    )
    assert bad_tags.status_code == 422
    bad_rid = client.post(
        "/pools/px/leases", json={"resource_id": {}}, headers=HEAD
    )
    assert bad_rid.status_code == 422


def test_api_release_validates_used(make_client):
    client, _ = make_client()
    client.post("/pools", json={"name": "px", "kind": "fakecheck"}, headers=HEAD)
    client.post(
        "/pools/px/resources",
        json={"records": [{"id": "r1"}], "import_validate": "on_import"},
        headers=HEAD,
    )
    lease = client.post(
        "/pools/px/leases", json={"consumer": "c"}, headers=HEAD
    ).json()["leases"][0]
    bad = client.request(
        "DELETE",
        f"/pools/px/leases/{lease['token']}",
        content=json.dumps({"used": "abc"}),
        headers={**HEAD, "Content-Type": "application/json"},
    )
    assert bad.status_code == 422


def test_api_recall_validates_batch_and_action(make_client):
    client, _ = make_client()
    client.post("/pools", json={"name": "px", "kind": "fakecheck"}, headers=HEAD)
    missing = client.post("/pools/px/recall", json={"action": "requalify"}, headers=HEAD)
    assert missing.status_code == 422
    unknown = client.post(
        "/pools/px/recall", json={"batch_id": "b1", "action": "explode"}, headers=HEAD
    )
    assert unknown.status_code == 422


def test_api_delete_archives_idle_resource(make_client):
    client, _ = make_client()
    client.post("/pools", json={"name": "px", "kind": "fakecheck"}, headers=HEAD)
    client.post(
        "/pools/px/resources",
        json={"records": [{"id": "r1"}], "import_validate": "on_import"},
        headers=HEAD,
    )
    resp = client.request("DELETE", "/pools/px/resources/r1", headers=HEAD)
    assert resp.status_code == 200
    assert resp.json()["state"] == "archived"
    detail = client.get("/pools/px/resources/r1", headers=HEAD).json()
    assert detail["state"] == "archived"


# -- events / SSE ----------------------------------------------------------


async def test_event_history_limit_advances_the_cursor():
    bus = EventBus()
    for i in range(300):
        bus.publish(f"e{i}", "p")

    page = bus.history(0, limit=100)
    assert [e.seq for e in page[:2]] == [1, 2]
    assert page[-1].seq == 100
    page2 = bus.history(page[-1].seq, limit=100)
    assert page2[0].seq == 101 and page2[-1].seq == 200


async def test_lease_events_do_not_leak_tokens(make_pool, purser):
    await purser.start()
    seen = []
    purser.bus.subscribe(lambda event: seen.append(event))
    pool = make_pool()
    await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    await pool.release(lease)
    await asyncio.sleep(0.1)
    assert any(event.type == "lease_acquired" for event in seen)
    assert all("token" not in event.data for event in seen)


def test_sse_filters_by_pool(make_client):
    client, ps = make_client()
    client.post("/pools", json={"name": "pa", "kind": "fakecheck"}, headers=HEAD)
    client.post("/pools", json={"name": "pb", "kind": "fakecheck"}, headers=HEAD)
    ps.bus.publish("noise", "pb", rid="x")
    ps.bus.publish("signal", "pa", rid="r1")

    payloads: list[dict] = []
    with client.stream("GET", "/pools/pa/events", headers=HEAD) as resp:
        assert resp.headers["content-type"].startswith("text/event-stream")
        for line in resp.iter_lines():
            if not line.startswith("data:"):
                continue
            payloads.append(json.loads(line[5:].strip()))
            if any(item["type"] == "signal" for item in payloads):
                break
    assert payloads
    assert all(item["pool"] == "pa" for item in payloads)
    assert all("token" not in item["data"] for item in payloads)


# -- scheduler / config ----------------------------------------------------


async def test_scheduler_tasks_run_independently():
    scheduler = PeriodicScheduler()
    counts = {"slow": 0, "fast": 0}

    async def slow() -> None:
        counts["slow"] += 1
        await asyncio.sleep(1.0)

    async def fast() -> None:
        counts["fast"] += 1

    scheduler.add("slow", 0.01, slow)
    scheduler.add("fast", 0.05, fast)
    await scheduler.start()
    try:
        await asyncio.sleep(0.35)
    finally:
        await scheduler.stop()
    assert counts["slow"] >= 1
    assert counts["fast"] >= 3


@pytest.mark.parametrize(
    "overrides",
    [
        {"heartbeat_interval": 0},
        {"validate_concurrency": 0},
        {"usage_quota": -1},
        {"max_lease_duration": float("nan")},
        {"expires_in": -5},
        {"unban_after": -1},
        {"quota_limit": 0},
        {"expire_policy": "bogus"},
        {"expired_retention": -1},
        {"expired_retention": float("nan")},
        {"requalify_on_release": "yes"},
        {"scheduler_intervals": {"quota_reset": 0}},
        {"schema_version": 2},
        {"scheduler": "token_bucket"},
        {"scheduler": "fifo"},
        {"drain_timeout": 5},
        {"preempt_threshold": 5},
        {"fail_open": True},
        {"preemptible": True},
        {"probe_targets": ["http://example.com"]},
    ],
)
def test_invalid_pool_config_is_rejected(overrides):
    with pytest.raises(ConfigError):
        PoolConfig(kind="k", **overrides)


def test_quota_limit_is_the_period_quota_knob():
    cfg = PoolConfig(kind="k", quota_limit=2)
    assert cfg.quota_limit == 2
    dumped = cfg.to_dict()
    assert dumped["quota_limit"] == 2
    assert "total_quota" not in dumped
    assert "target_quota" not in dumped
    replaced = cfg.with_overrides(quota_limit=3)
    assert replaced.quota_limit == 3


def test_acquire_request_rejects_unimplemented_hints():
    # probe is now a supported per-target live check; only affinity is unused.
    assert AcquireRequest(probe="site").probe == "site"
    with pytest.raises(ConfigError, match="affinity"):
        AcquireRequest(affinity="c")


def test_acquire_request_normalises_tags():
    assert AcquireRequest(tags=["http", "us"]).tags == frozenset({"http", "us"})
    assert AcquireRequest(tags={"http"}).tags == frozenset({"http"})
    with pytest.raises(TypeError, match="iterable"):
        AcquireRequest(tags="http")
    with pytest.raises(TypeError, match="iterable"):
        AcquireRequest(tags=5)
    with pytest.raises(ValueError, match="non-empty"):
        AcquireRequest(tags=["http", ""])


# -- idempotency -----------------------------------------------------------


async def test_redis_idempotency_store_roundtrip():
    client = fakeredis.aioredis.FakeRedis()
    store = RedisIdempotencyStore(client=client, ttl=60)
    try:
        assert await store.aget("missing") is None
        await store.aput("k", {"ok": True})
        assert await store.aget("k") == {"ok": True}
    finally:
        await client.aclose()


async def test_purser_uses_redis_idempotency_when_configured():
    client = fakeredis.aioredis.FakeRedis()
    ps = Purser(maintenance_enabled=False, redis_client=client)
    try:
        assert isinstance(ps.idempotency, RedisIdempotencyStore)
        await ps.idempotency.aput("k", {"n": 1})
        assert await ps.idempotency.aget("k") == {"n": 1}
    finally:
        await ps.close()
        await client.aclose()


def test_all_exceptions_are_exported_from_top_level():
    import purser
    import purser.errors as errors

    for name, value in vars(errors).items():
        if isinstance(value, type) and issubclass(value, errors.PurserError):
            assert getattr(purser, name) is value, name
            assert name in purser.__all__, name


async def test_build_purser_reads_service_env(monkeypatch, tmp_path):
    from purser.ui.server import build_purser

    monkeypatch.setenv("PURSER_STORAGE", "sqlite")
    monkeypatch.setenv("PURSER_SQLITE_PATH", str(tmp_path / "svc.db"))
    monkeypatch.setenv("PURSER_COORDINATION", "memory")
    monkeypatch.setenv("PURSER_AUDIT_PATH", str(tmp_path / "audit.db"))
    monkeypatch.delenv("PURSER_REDIS_URL", raising=False)
    monkeypatch.delenv("PURSER_MYSQL_URL", raising=False)

    ps = build_purser()
    try:
        assert ps.storage_kind == "sqlite"
        assert ps.coordination_kind == "memory"
        assert ps.sqlite_path == str(tmp_path / "svc.db")
        assert ps.audit_path == str(tmp_path / "audit.db")
    finally:
        await ps.close()

    override = build_purser(storage="memory")
    try:
        assert override.storage_kind == "memory"
    finally:
        await override.close()


def test_report_views_are_exported_from_top_level():
    import purser
    from purser.core import views

    for name in (
        "PoolStats",
        "UsageReport",
        "UsageWindow",
        "UsageTotals",
        "UsageGroup",
        "ActiveUsage",
        "ActiveUsageGroup",
        "PoolUsage",
        "ValidateJobView",
        "ValidateJobCancelView",
        "ValidateJobError",
    ):
        assert getattr(purser, name) is getattr(views, name)
        assert name in purser.__all__

    # TypedDict.__required_keys__ only exists on Python 3.9+.
    if sys.version_info >= (3, 9):
        assert views.PoolStats.__required_keys__ >= {"name", "counts", "config"}
        assert views.UsageReport.__required_keys__ >= {
            "window",
            "totals",
            "groups",
            "active",
            "pools",
        }
        assert views.ValidateJobView.__required_keys__ >= {
            "id",
            "status",
            "pending",
            "errors",
        }
        assert "cancelled" in views.ValidateJobCancelView.__required_keys__
