"""Real-Redis integration tests (skip unless ``PURSER_TEST_REDIS_URL`` is set).

These tests talk to an actual Redis server, never ``fakeredis``: lease
hash/token-index round trips, ``PEXPIRE``-driven TTL expiry, renewal extending
the key TTL, a hard subprocess crash followed by expiry-based slot reclaim,
cross-instance visibility/force-revoke, a stale release that must not steal a
currently held slot, and multi-process no-oversell contention.

Point ``PURSER_TEST_REDIS_URL`` at a dedicated DB, for example
``redis://127.0.0.1:7001/15``; the DB is flushed before and after every test.
"""

from __future__ import annotations

import asyncio
import faulthandler
import json
import multiprocessing as mp
import os
import sqlite3
import threading
import time

import pytest
from conftest import FakeChecker
from redis import asyncio as aioredis

from purser import Purser
from purser.backends.coordination import RedisCoordination
from purser.core.resource import AcquireRequest, Lease
from purser.errors import PoolExhausted, PurserError, ResourceBusy

URL = os.environ.get("PURSER_TEST_REDIS_URL")
pytestmark = pytest.mark.skipif(not URL, reason="PURSER_TEST_REDIS_URL not set")


@pytest.fixture(autouse=True)
async def _flush_real_redis():
    if not URL:
        return
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


def _tail_logs(sqlite_path: str, n: int = 8) -> str:
    blocks: list[str] = []
    for i in range(3):
        path = f"{sqlite_path}.w{i}.log"
        if os.path.exists(path):
            with open(path, encoding="utf-8") as fh:
                tail = fh.read().splitlines()[-n:]
            blocks.append(f"--- w{i} ---\n" + "\n".join(tail))
    return "\n".join(blocks)


async def _make_purser(*, storage: str, coordination: str, sqlite_path: str | None = None):
    return Purser(
        maintenance_enabled=False,
        storage=storage,
        coordination=coordination,
        sqlite_path=sqlite_path,
        redis_url=URL if coordination == "redis" else None,
    )


async def test_redis_lease_roundtrip_and_ttl():
    client = _redis()
    co = RedisCoordination(client=client, key_prefix="purser-real")
    lease = Lease.new("p", "r0", consumer="c0", slots=1, ttl=30.0)
    try:
        await co.register_lease("p", lease)
        got = await co.get_lease("p", lease.token)
        assert got is not None and got.token == lease.token
        assert [lease.token for lease in await co.leases_for("p", "r0")] == [lease.token]
        assert [lease.token for lease in await co.active("p")] == [lease.token]

        pttl = await client.pttl(co._lease_key("p", "r0"))
        assert 0 < pttl <= 35_500, pttl  # ttl + 5s heartbeat grace

        assert (
            await co.renew_lease("p", "r0", lease.token, time.time() + 60.0)
            is True
        )
        pttl2 = await client.pttl(co._lease_key("p", "r0"))
        assert pttl2 > 55_000, pttl2

        assert await co.unregister_lease("p", lease.token) is not None
        assert await co.get_lease("p", lease.token) is None
        assert await co.active("p") == []
    finally:
        await client.aclose()


async def test_redis_lease_survives_client_reconnect():
    """A lease registered by one connection is visible/renewable from another."""

    lease = Lease.new("p", "r0", consumer="c0", slots=1, ttl=30.0)
    c1 = _redis()
    co1 = RedisCoordination(client=c1, key_prefix="purser-real")
    await co1.register_lease("p", lease)
    await c1.aclose()  # simulate the registering instance going away

    c2 = _redis()
    co2 = RedisCoordination(client=c2, key_prefix="purser-real")
    try:
        assert [lease.token for lease in await co2.active("p")] == [lease.token]
        assert (
            await co2.renew_lease("p", "r0", lease.token, time.time() + 30.0)
            is True
        )
        assert await co2.unregister_lease("p", lease.token) is not None
        assert await co2.active("p") == []
    finally:
        await c2.aclose()


def _crash_worker(sqlite_path: str, marker_path: str, ttl: float) -> None:
    """Acquire a lease, write a marker, then die without any cleanup."""

    async def run() -> None:
        ps = Purser(
            maintenance_enabled=False,
            storage="sqlite",
            coordination="redis",
            sqlite_path=sqlite_path,
            redis_url=URL,
        )
        ps.register_type(FakeChecker())
        pool = ps.create_pool("crash", "fakecheck", max_slots=1, lease_ttl=ttl)
        await ps.start()
        await pool.import_resources(
            [{"id": "r0", "host": "10.0.0.9", "port": 9001}],
            import_validate="on_import",
        )
        lease = await pool.acquire(
            AcquireRequest(consumer="dead", slots=1, timeout=1.0)
        )
        with open(marker_path, "w", encoding="utf-8") as fh:
            fh.write(lease.token)
        os._exit(0)  # hard crash: no release, no close, no flush

    asyncio.run(run())


async def test_hard_crash_reclaims_slot_after_ttl(tmp_path):
    """A hard-crashed holder's slot stays busy, then PEXPIRE + reconcile frees it."""

    ttl = 1.5
    sqlite_path = str(tmp_path / "crash.db")
    marker = str(tmp_path / "crash.marker")

    seed = await _make_purser(storage="sqlite", coordination="memory", sqlite_path=sqlite_path)
    seed.register_type(FakeChecker())
    pool_seed = seed.create_pool("crash", "fakecheck", max_slots=1, lease_ttl=ttl)
    await pool_seed.import_resources(
        [{"id": "r0", "host": "10.0.0.9", "port": 9001}],
        import_validate="on_import",
    )
    await seed.close()

    ctx = mp.get_context("spawn")
    proc = ctx.Process(target=_crash_worker, args=(sqlite_path, marker, ttl))
    proc.start()
    try:
        deadline = time.monotonic() + 15
        while not os.path.exists(marker) and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        assert os.path.exists(marker), "crash worker never acquired"
        proc.join(timeout=10)
    finally:
        if proc.is_alive():
            proc.terminate()

    # While the dead lease is still live, the slot must stay busy (no oversell).
    ps = await _make_purser(storage="sqlite", coordination="redis", sqlite_path=sqlite_path)
    ps.register_type(FakeChecker())
    pool = ps.create_pool("crash", "fakecheck", max_slots=1, lease_ttl=ttl)
    try:
        with pytest.raises((ResourceBusy, PoolExhausted, PurserError)):
            await pool.acquire(
                AcquireRequest(resource_id="r0", consumer="c2", slots=1, timeout=0.5)
            )
        res = await pool.get("r0")
        assert res is not None and res.concurrency.used_slots == 1

        # The Redis TTL must actually fire and drop the lease.
        client = _redis()
        co = RedisCoordination(client=client, key_prefix="purser")
        try:
            deadline = time.monotonic() + ttl + 5
            while await co.leases_for("crash", "r0") and time.monotonic() < deadline:
                await asyncio.sleep(0.1)
            assert await co.leases_for("crash", "r0") == [], "redis lease never expired"
        finally:
            await client.aclose()

        # Restart path reconciles the orphaned slot, master data stays intact.
        await pool.start()
        res = await pool.get("r0")
        assert res is not None and res.concurrency.used_slots == 0
        assert res.payload == {"host": "10.0.0.9", "port": 9001}
        assert res.state.value in ("idle", "active")

        lease = await pool.acquire(
            AcquireRequest(resource_id="r0", consumer="c2", slots=1, timeout=1.0)
        )
        assert lease.slots == 1
        res = await pool.get("r0")
        assert res is not None and res.concurrency.used_slots == 1
        assert len(await pool._coordination.active("crash")) == 1
        await pool.release(lease)
        res = await pool.get("r0")
        assert res is not None and res.concurrency.used_slots == 0
        assert await pool._coordination.active("crash") == []
    finally:
        await ps.close()


async def test_cross_instance_visibility_revoke_and_stale_release(
    tmp_path, shared_backend_harness
):
    """B sees A's lease, revokes it, then a stale A release must not steal B's slot."""

    sqlite_path = str(tmp_path / "x.db")
    ps_a = await _make_purser(storage="sqlite", coordination="redis", sqlite_path=sqlite_path)
    ps_a.register_type(FakeChecker())
    pool_a = ps_a.create_pool("x", "fakecheck", max_slots=1, lease_ttl=30.0)
    await pool_a.import_resources(
        [{"id": "r0", "host": "5.6.7.8", "port": 3306}],
        import_validate="on_import",
    )
    await ps_a.start()
    lease_a = await pool_a.acquire(AcquireRequest(consumer="a", slots=1, timeout=1.0))

    ps_b = await _make_purser(storage="sqlite", coordination="redis", sqlite_path=sqlite_path)
    ps_b.register_type(FakeChecker())
    pool_b = ps_b.create_pool("x", "fakecheck", max_slots=1, lease_ttl=30.0)
    await ps_b.start()
    try:
        # B sees A's lease through real Redis and cannot double-assign.
        assert [lease.token for lease in await pool_b._coordination.active("x")] == [
            lease_a.token
        ]
        with pytest.raises((ResourceBusy, PoolExhausted, PurserError)):
            await pool_b.acquire(
                AcquireRequest(resource_id="r0", consumer="b", slots=1, timeout=0.5)
            )

        revoked = await pool_b.force_revoke("r0", operator="test", reason="real redis")
        assert revoked.token == lease_a.token
        res = await pool_b.get("r0")
        assert res is not None and res.concurrency.used_slots == 0

        # B re-acquires before A ever calls release (the realistic late-release).
        lease_b = await pool_b.acquire(
            AcquireRequest(resource_id="r0", consumer="b", slots=1, timeout=1.0)
        )
        res = await pool_b.get("r0")
        assert res is not None and res.concurrency.used_slots == 1

        # A's stale release arrived after the revoke: it must be a no-op on the
        # storage plane, not a decrement of B's freshly acquired slot.
        assert await pool_a.release(lease_a) is True
        res = await pool_b.get("r0")
        assert res is not None and res.concurrency.used_slots == 1, "stale release stole a slot"
        assert [lease.token for lease in await pool_b._coordination.active("x")] == [
            lease_b.token
        ]

        # B can still release cleanly and leave the pool empty.
        assert await pool_b.release(lease_b) is True
        res = await pool_b.get("r0")
        assert res is not None and res.concurrency.used_slots == 0
        assert await pool_b._coordination.active("x") == []
    finally:
        await ps_a.close()
        await ps_b.close()


def _mp_worker(
    outcome_path: str,
    log_path: str,
    sqlite_path: str,
    worker_id: int,
    attempts: int,
) -> None:
    """Runs in a separate OS process: own Purser + real Redis coordination."""

    def log(msg: str) -> None:
        with open(log_path, "a", encoding="utf-8") as fh:
            fh.write(f"{time.time():.3f} pid={os.getpid()} {msg}\n")

    # Temporary diagnostic: dump every thread stack if the worker wedges so we
    # can pinpoint which await inside close() blocks forever.
    faulthandler.dump_traceback_later(25, exit=True)

    async def run() -> dict:
        ok = 0
        surprises: list[str] = []
        log("purser-begin")
        ps = Purser(
            maintenance_enabled=False,
            storage="sqlite",
            coordination="redis",
            sqlite_path=sqlite_path,
            redis_url=URL,
        )
        ps.register_type(FakeChecker())
        pool = ps.create_pool("shared", "fakecheck", max_slots=2, lease_ttl=5.0)
        log("pool-created")
        held: list = []
        try:
            await ps.start()
            log("started")
            for _ in range(attempts):
                try:
                    lease = await pool.acquire(
                        AcquireRequest(consumer=f"w{worker_id}", slots=1, timeout=0.1)
                    )
                    ok += 1
                    held.append(lease)
                    if len(held) >= 2 or len(held) % 3 == 0:
                        await pool.release(held.pop(0))
                except (PurserError, asyncio.TimeoutError):
                    pass  # contention timeouts are expected
                except Exception as exc:
                    surprises.append(f"{type(exc).__name__}: {exc}")
            log("loop-done")
            for lease in list(held):
                try:
                    await pool.release(lease)
                except Exception as exc:
                    surprises.append(f"release: {type(exc).__name__}: {exc}")
            log("releases-done")
        finally:
            log("close-begin")
            await ps.close()
            log("close-done")
        return {"pid": os.getpid(), "ok": ok, "surprises": surprises}

    outcome = {"pid": os.getpid(), "ok": 0, "surprises": ["worker crashed"]}
    log("run-begin")
    try:
        outcome = asyncio.run(run())
    except BaseException as exc:  # noqa: BLE001 - always report worker state
        outcome["surprises"].append(f"fatal: {type(exc).__name__}: {exc}")
    finally:
        log("outcome-writing")
        with open(outcome_path, "w", encoding="utf-8") as fh:
            json.dump(outcome, fh)
            fh.flush()
            os.fsync(fh.fileno())
        log("outcome-written")


async def test_multiprocess_real_redis_no_oversell(tmp_path):
    """3 OS processes share one SQLite file + one real Redis; CAS must not oversell."""

    sqlite_path = str(tmp_path / "shared.db")
    seed = await _make_purser(storage="sqlite", coordination="memory", sqlite_path=sqlite_path)
    seed.register_type(FakeChecker())
    pool_seed = seed.create_pool("shared", "fakecheck", max_slots=2)
    await pool_seed.import_resources(
        [{"id": "r0", "host": "1.1.1.1", "port": 1}],
        import_validate="on_import",
    )
    await seed.close()

    ctx = mp.get_context("spawn")
    procs = [
        ctx.Process(
            target=_mp_worker,
            args=(
                f"{sqlite_path}.w{i}.json",
                f"{sqlite_path}.w{i}.log",
                sqlite_path,
                i,
                100,
            ),
        )
        for i in range(3)
    ]
    max_seen = [0]
    stop = [False]
    conn = sqlite3.connect(sqlite_path)

    def sample() -> None:
        while not stop[0]:
            try:
                row = conn.execute(
                    "SELECT used_slots FROM resources WHERE pool = 'shared' AND id = 'r0'"
                ).fetchone()
                if row:
                    max_seen[0] = max(max_seen[0], int(row[0]))
            except Exception:
                pass
            time.sleep(0.003)

    sampler = threading.Thread(target=sample)
    sampler.start()
    try:
        for proc in procs:
            proc.start()
        for proc in procs:
            proc.join(timeout=90)
    finally:
        stop[0] = True
        sampler.join(timeout=2)
        conn.close()
        for proc in procs:
            if proc.is_alive():
                proc.terminate()

    outcome_paths = [f"{sqlite_path}.w{i}.json" for i in range(3)]
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and not all(
        os.path.exists(p) for p in outcome_paths
    ):
        await asyncio.sleep(0.05)
    outcomes = []
    for path in outcome_paths:
        assert os.path.exists(path), (
            f"worker outcome missing: {path}; tail of log:\n"
            + _tail_logs(sqlite_path)
        )
        with open(path, encoding="utf-8") as fh:
            outcomes.append(json.load(fh))
    for outcome in outcomes:
        assert outcome["surprises"] == [], outcome
    assert sum(outcome["ok"] for outcome in outcomes) > 0, outcomes
    assert max_seen[0] <= 2, f"oversell observed: {max_seen[0]}"

    # Coordination settles (TTL sweeps any straggler keys), then the restart
    # path reconciles and must find a clean pool: no leases, no leaked slots.
    client = _redis()
    co = RedisCoordination(client=client, key_prefix="purser")
    try:
        deadline = time.monotonic() + 12
        while await co.active("shared") and time.monotonic() < deadline:
            await asyncio.sleep(0.1)
        assert await co.active("shared") == [], "redis leases did not settle"
    finally:
        await client.aclose()

    check = await _make_purser(storage="sqlite", coordination="redis", sqlite_path=sqlite_path)
    check.register_type(FakeChecker())
    pool_check = check.create_pool("shared", "fakecheck", max_slots=2)
    await pool_check.start()
    try:
        res = await pool_check.get("r0")
        assert res is not None and res.concurrency.used_slots == 0, (
            f"slots leaked across processes: {res.concurrency.used_slots}"
        )
    finally:
        await check.close()
