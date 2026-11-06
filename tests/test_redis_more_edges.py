"""More real-Redis boundary edges (skip unless ``PURSER_TEST_REDIS_URL``).

Continues ``test_redis_real.py`` with: ids/token index self-healing after TTL
expiry, ``use_limit`` lease shutdown and restart reclaim, coordination failure
rolling the storage slot back (fail-closed), and a partial-orphan reconcile on
a real shared Redis.
"""

from __future__ import annotations

import asyncio
import os
import time

import pytest
from conftest import FakeChecker
from redis import asyncio as aioredis

from purser import Purser
from purser.backends.coordination import RedisCoordination
from purser.core.resource import AcquireRequest, Lease
from purser.errors import PurserError

URL = os.environ.get("PURSER_TEST_REDIS_URL")
pytestmark = pytest.mark.skipif(not URL, reason="PURSER_TEST_REDIS_URL not set")


@pytest.fixture(autouse=True)
async def _flush_real_redis():
    client = _redis()
    try:
        await client.flushdb()
    finally:
        await client.aclose()
    yield
    client = _redis()
    try:
        await client.flushdb()
    finally:
        await client.aclose()


def _redis() -> aioredis.Redis:
    return aioredis.Redis.from_url(URL, protocol=2, socket_connect_timeout=5)


def _purser(sqlite_path: str, redis_url: str | None = None) -> Purser:
    ps = Purser(
        maintenance_enabled=False,
        storage="sqlite",
        coordination="redis",
        sqlite_path=sqlite_path,
        redis_url=redis_url or URL,
    )
    ps.register_type(FakeChecker())
    return ps


async def test_redis_ids_and_token_index_self_heal_after_expiry():
    client = _redis()
    co = RedisCoordination(client=client, key_prefix="purser-real")
    lease = Lease.new("p", "r0", consumer="c0", slots=1, ttl=0.6)
    try:
        await co.register_lease("p", lease)
        assert await client.scard(co._ids_key("p")) == 1
        assert await client.hlen(co._token_key("p")) == 1
        deadline = time.monotonic() + 5
        while await co.leases_for("p", "r0") and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        assert await co.leases_for("p", "r0") == []
        # The key is gone, so renewal must fail instead of resurrecting it.
        assert await co.renew_lease("p", "r0", lease.token, time.time() + 30) is False
        # active() prunes the stale indexes left behind by TTL expiry.
        assert await co.active("p") == []
        assert await client.scard(co._ids_key("p")) == 0
        assert await client.hlen(co._token_key("p")) == 0
    finally:
        await client.aclose()


async def test_redis_use_limit_expiry_reclaim_on_restart(tmp_path):
    path = str(tmp_path / "ul.db")
    ps = _purser(path)
    pool = ps.create_pool("ul", "fakecheck", max_slots=1, lease_ttl=1.0)
    await pool.import_resources(
        [{"id": "r0", "host": "10.1.1.1", "port": 7000}],
        import_validate="on_import",
    )
    lease = await pool.acquire(AcquireRequest(consumer="c", slots=1, use_limit=1))
    lease.used = 1  # quota reached: the heartbeat would stop renewing
    pool._leases.unregister(lease.token)  # stop local heartbeats now
    try:
        client = _redis()
        co = RedisCoordination(client=client, key_prefix="purser")
        try:
            deadline = time.monotonic() + 5
            while await co.leases_for("ul", "r0") and time.monotonic() < deadline:
                await asyncio.sleep(0.05)
            assert await co.leases_for("ul", "r0") == []
        finally:
            await client.aclose()
        res = await pool.get("r0")
        assert res is not None and res.concurrency.used_slots == 1  # not stolen
        await ps.close()

        ps2 = _purser(path)
        pool2 = ps2.create_pool("ul", "fakecheck", max_slots=1, lease_ttl=1.0)
        try:
            await pool2.start()  # restart reconcile reclaims the orphaned slot
            res = await pool2.get("r0")
            assert res is not None and res.concurrency.used_slots == 0
            assert res.payload == {"host": "10.1.1.1", "port": 7000}
            lease2 = await pool2.acquire(
                AcquireRequest(resource_id="r0", consumer="c2", slots=1, timeout=1.0)
            )
            assert lease2.slots == 1
            await pool2.release(lease2)
            res = await pool2.get("r0")
            assert res is not None and res.concurrency.used_slots == 0
        finally:
            await ps2.close()
    finally:
        await ps.close()


async def test_redis_register_failure_rolls_back_slot(tmp_path):
    path = str(tmp_path / "rb.db")
    dead = "redis://127.0.0.1:1/15"  # refused: coordination is unavailable
    ps_bad = _purser(path, redis_url=dead)
    pool_bad = ps_bad.create_pool("rb", "fakecheck", max_slots=1)
    await pool_bad.import_resources(
        [{"id": "r0", "host": "2.2.2.2", "port": 80}],
        import_validate="on_import",
    )
    try:
        with pytest.raises(PurserError):
            await pool_bad.acquire(
                AcquireRequest(consumer="c", slots=1, timeout=0.6)
            )
        res = await pool_bad.get("r0")
        assert res is not None and res.concurrency.used_slots == 0, "rollback leaked a slot"
        assert res.state.value in ("idle", "active")
    finally:
        await ps_bad.close()

    # The same storage recovers cleanly once coordination is reachable again.
    ps_good = _purser(path)
    pool_good = ps_good.create_pool("rb", "fakecheck", max_slots=1)
    try:
        lease = await pool_good.acquire(
            AcquireRequest(resource_id="r0", consumer="c2", slots=1, timeout=1.0)
        )
        assert lease.slots == 1
        res = await pool_good.get("r0")
        assert res is not None and res.concurrency.used_slots == 1
        await pool_good.release(lease)
        res = await pool_good.get("r0")
        assert res is not None and res.concurrency.used_slots == 0
    finally:
        await ps_good.close()


async def test_redis_partial_orphan_reconcile_real(tmp_path):
    path = str(tmp_path / "po.db")
    ps_a = _purser(path)
    pool_a = ps_a.create_pool("shared", "fakecheck", max_slots=2, lease_ttl=30.0)
    await pool_a.import_resources(
        [{"id": "r0", "host": "3.3.3.3", "port": 443}],
        import_validate="on_import",
    )
    lease_a = await pool_a.acquire(AcquireRequest(consumer="a", slots=1, timeout=1.0))

    ps_b = _purser(path)
    pool_b = ps_b.create_pool("shared", "fakecheck", max_slots=2, lease_ttl=30.0)
    lease_b = await pool_b.acquire(AcquireRequest(consumer="b", slots=1, timeout=1.0))
    try:
        res = await pool_a.get("r0")
        assert res is not None and res.concurrency.used_slots == 2
        # A's lease vanishes (revoked/expired elsewhere) but its slot remains.
        await pool_b._coordination.unregister_lease("shared", lease_a.token)
        assert [lease.token for lease in await pool_b._coordination.active("shared")] == [
            lease_b.token
        ]

        ps_c = _purser(path)
        pool_c = ps_c.create_pool("shared", "fakecheck", max_slots=2, lease_ttl=30.0)
        try:
            await pool_c.start()  # restart reconcile aligns to the live lease
            res = await pool_c.get("r0")
            assert res is not None and res.concurrency.used_slots == 1
            assert res.state is not None
            assert res.payload == {"host": "3.3.3.3", "port": 443}
        finally:
            await ps_c.close()

        assert await pool_b.release(lease_b) is True
        res = await pool_a.get("r0")
        assert res is not None and res.concurrency.used_slots == 0
        assert await pool_b._coordination.active("shared") == []
    finally:
        await ps_a.close()
        await ps_b.close()
