"""Final edge batch: lost-lease tracking, draining revoke, quorum, bounds."""

from __future__ import annotations

import asyncio

import pytest
from conftest import seed

from purser.adapters.proxy import ProxyAdapter
from purser.core.events import EventBus
from purser.core.resource import AcquireRequest, Resource
from purser.core.state import ResourceState
from purser.crypto import SecretCodec
from purser.metrics import Metrics


async def test_lease_lost_unregisters_from_manager(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    assert pool._leases.active_count() == 1
    await pool._on_lease_lost(lease)
    assert pool._leases.active_count() == 0  # no lingering tracked lease
    await pool.release(lease)
    again = await pool.acquire(AcquireRequest(consumer="c"))
    assert again.resource_id == rid
    await pool.release(again)


async def test_force_revoke_draining_archives(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    await pool.acquire(AcquireRequest(consumer="victim"))
    await pool.drain(rid, operator="ops", reason="retiring")
    await pool.force_revoke(rid, operator="ops", reason="kick-drain")
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.ARCHIVED
    assert res.concurrency.used_slots == 0


async def test_query_empty_tags_equals_all(make_pool):
    pool = make_pool()
    await seed(pool, n=2)
    assert [r.id for r in await pool.query(tags=frozenset())] == [
        r.id for r in await pool.query()
    ]


async def test_bus_restart_drains_pending():
    bus = EventBus(listener_timeout=1.0)
    seen: list[str] = []

    async def listening(event) -> None:
        seen.append(event.type)

    bus.subscribe(listening)
    await bus.start()
    bus.publish("a", "p")
    await bus.stop()
    bus.publish("b", "p")  # queued while the dispatcher is down
    await bus.start()
    try:
        for _ in range(100):
            if "a" in seen and "b" in seen:
                break
            await asyncio.sleep(0.01)
        assert "a" in seen and "b" in seen
    finally:
        await bus.stop()


def test_metrics_histogram_bounded():
    m = Metrics()
    for _ in range(10_001):
        m.observe("lat", 1.0)
    snap = m.snapshot()
    assert snap["histograms"]["lat"]["count"] == 10_000


def test_secret_codec_short_keys():
    for size in (16, 24, 32):
        codec = SecretCodec(key=b"\x01" * size)
        stored = codec.encrypt_payload({"password": "s3cret", "n": 7}, ("password",))
        out = codec.decrypt_payload(stored, ("password",))
        assert out == {"password": "s3cret", "n": 7}


@pytest.mark.parametrize(
    "probe_results,expected_alive,expected_latency",
    [
        ([(True, 2.0, None), (True, 1.0, None), (False, None, "down")], True, 1.0),
        ([(True, 5.0, None), (False, None, "a"), (False, None, "b")], False, None),
    ],
)
async def test_proxy_validate_quorum_and_latency(
    probe_results, expected_alive, expected_latency
):
    adapter = ProxyAdapter(probe_targets=["t1", "t2", "t3"])
    calls = iter(probe_results)

    async def fake_probe(protocol, host, port, target, auth, timeout):
        return next(calls)

    adapter._probe = fake_probe
    resource = Resource(
        id="p", kind="proxy", payload={"host": "1.2.3.4", "port": 8080}
    )
    report = await adapter.validate(resource)
    assert report.alive is expected_alive
    assert report.latency_ms == expected_latency


async def test_same_purser_restart_cycle():
    from conftest import FakeChecker

    from purser import Purser

    ps = Purser(maintenance_enabled=True)
    ps.register_adapter(FakeChecker())
    pool = ps.create_pool("px", "fakecheck")
    await ps.start()
    [rid] = await seed(pool, n=1)
    await pool.acquire(AcquireRequest(consumer="c"))
    lease = next(iter(pool._leases.active()))
    await pool.release(lease)
    await ps.close()
    await ps.start()
    again = await pool.acquire(AcquireRequest(consumer="c"))
    assert again.resource_id == rid
    await pool.release(again)
    await ps.close()
