"""Last pour-over: tiny real paths still untouched by any suite."""

from __future__ import annotations

import time

import pytest
from conftest import seed
from test_api import auth

from purser.core.health import HealthStats
from purser.core.resource import AcquireRequest, Quota
from purser.core.state import ResourceState
from purser.errors import PoolExhausted
from purser.types.proxy import parse_endpoint


def test_parse_endpoint_variants():
    assert parse_endpoint("http://1.2.3.4:8080") == ("1.2.3.4", 8080)
    assert parse_endpoint("1.2.3.4:3128") == ("1.2.3.4", 3128)
    assert parse_endpoint("1.2.3.4") == ("1.2.3.4", 80)
    assert parse_endpoint("https://x.com:443") == ("x.com", 443)


async def test_archived_import_is_skipped(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    await pool.drain(rid, operator="ops", reason="retire")
    await pool.release(lease)
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.ARCHIVED
    report = await pool.import_resources([{"id": rid, "host": "9.9.9.9", "port": 9}])
    assert report.skipped == 1 and report.imported == 0
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.ARCHIVED
    assert res.payload["host"] != "9.9.9.9"  # archived row untouched


async def test_ignore_cooldown_bypasses_consumer_only(make_pool):
    pool = make_pool(cooldown_after_release=60, cooldown_consumer_only=True)
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    await pool.release(lease)
    with pytest.raises(PoolExhausted):
        await pool.acquire(AcquireRequest(consumer="c", timeout=0.05))
    bypassed = await pool.acquire(
        AcquireRequest(consumer="c", ignore_cooldown=True, timeout=2.0)
    )
    assert bypassed.resource_id == rid
    await pool.release(bypassed)


def test_health_quota_boundaries():
    degraded = HealthStats()
    for _ in range(3):
        degraded.update(alive=False)
    assert degraded.degraded is True
    assert HealthStats(score=0.9).degraded is False

    quota = Quota(limit=2)
    assert quota.consume(1) is True and quota.used == 1
    assert quota.consume(2) is False and quota.used == 1  # over-limit rejected
    quota.top_up(1)
    assert quota.used == 0
    quota.top_up(5)
    assert quota.used == 0  # floored at zero
    future = Quota(limit=1, used=1, reset_at=time.time() + 1000)
    assert future.reset_daily() is False
    past = Quota(limit=1, used=1, reset_at=time.time() - 1000)
    assert past.reset_daily() is True and past.used == 0


def test_metrics_endpoint_renders_text(make_client):
    client, _ = make_client()
    client.post("/pools", json={"name": "px", "kind": "fakecheck"}, headers=auth("admin-key"))
    resp = client.get("/metrics")
    assert resp.status_code == 200
    assert "text/plain" in resp.headers["content-type"]
    assert "purser_" in resp.text


def test_open_mode_include_secrets(make_client):
    client, _ = make_client(api_key=None, readonly_key=None)
    client.post("/pools", json={"name": "acc", "kind": "account"})
    client.post(
        "/pools/acc/resources",
        json={"records": [{"id": "a1", "username": "u", "password": "topsecret"}]},
    )
    resp = client.get("/pools/acc/resources/a1", params={"include_secrets": "true"})
    assert resp.status_code == 200
    assert resp.json()["payload"]["password"] == "topsecret"  # documented open mode
