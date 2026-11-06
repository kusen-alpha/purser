"""Remaining real paths: required_tag, affinity, windows, generate, revoke."""

from __future__ import annotations

import base64

import pytest
from conftest import seed

from purser.core.resource import AcquireRequest, Resource
from purser.core.state import ResourceState
from purser.crypto import SecretCodec
from purser.errors import PoolExhausted
from purser.facade import ResourcePool


async def test_required_tag_hard_constraint(make_pool):
    pool = make_pool()
    await pool.import_resources(
        [
            {"id": "us", "host": "1.1.1.1", "port": 1, "tags": ["US"]},
            {"id": "eu", "host": "2.2.2.2", "port": 2, "tags": ["EU"]},
        ],
        import_validate="on_import",
    )
    with pytest.raises(PoolExhausted):
        await pool.acquire(AcquireRequest(required_tag="ASIA", timeout=0.05))
    lease = await pool.acquire(AcquireRequest(required_tag="US", timeout=2.0))
    assert lease.resource_id == "us"
    await pool.release(lease)


async def test_affinity_records_and_normalises(make_pool):
    pool = make_pool(affinity_weight=1.0)
    ids = await seed(pool, n=2)
    lease = await pool.acquire(AcquireRequest(consumer="c", timeout=2.0))
    assert lease.resource_id in ids
    assert pool._affinity["c"].get(lease.resource_id, 0) >= 1
    candidates = [await pool.get(rid) for rid in ids]
    scores = await pool._affinity_scores("c", candidates)
    assert scores[lease.resource_id] == 1.0
    await pool.release(lease)


def test_schedule_open_windows(monkeypatch):
    def with_time(hhmm: str):
        monkeypatch.setattr(
            "purser.facade.time.strftime", lambda fmt, t=None: hhmm
        )

    with_time("03:00")
    overnight = Resource(
        id="r", kind="k", payload={}, schedule_window=("22:00", "06:00")
    )
    assert ResourcePool._schedule_open(overnight, 0.0) is True  # night gap open
    with_time("12:00")
    assert ResourcePool._schedule_open(overnight, 0.0) is False
    overnight.schedule_window = ("08:00", "18:00")
    assert ResourcePool._schedule_open(overnight, 0.0) is True
    overnight.schedule_window = None
    assert ResourcePool._schedule_open(overnight, 0.0) is True


async def test_pool_generate_builtin_path(make_pool):
    pool = make_pool()
    report = await pool.generate([{"id": "g1", "host": "1.1.1.1", "port": 1}])
    assert report.imported == 1
    res = await pool.get("g1")
    assert res is not None and res.state is ResourceState.IDLE  # on_import


async def test_force_revoke_specific_token(make_pool):
    pool = make_pool(max_slots=2)
    [rid] = await seed(pool, n=1)
    first = await pool.acquire(AcquireRequest(consumer="a", timeout=2.0))
    second = await pool.acquire(AcquireRequest(consumer="b", timeout=2.0))
    assert first.token != second.token
    revoked = await pool.force_revoke(
        rid, operator="ops", reason="kick", token=first.token
    )
    assert revoked.token == first.token
    res = await pool.get(rid)
    assert res is not None and res.concurrency.used_slots == 1
    await pool.release(second)
    res = await pool.get(rid)
    assert res is not None and res.concurrency.used_slots == 0


def test_secret_codec_base64_env(monkeypatch):
    key = b"k" * 32
    monkeypatch.setenv("PURSER_SECRET_KEY", base64.b64encode(key).decode("ascii"))
    codec = SecretCodec()
    assert codec.key == key
