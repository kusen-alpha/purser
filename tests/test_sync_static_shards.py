"""Static-sharding acceptance for the synchronous facade.

Each OS process owns one independent shard: its own storage (a private SQLite
file, a distinct MySQL pool namespace, or an in-memory kernel) and, when Redis
is used, its own pool-name namespace.  There is no cross-shard scheduling,
quota or mutex by design; the tests verify that each shard still enforces its
own capacity and that shards do not see or disturb each other.

Covered shapes (``storage-coordination``):

* memory-memory      -- fully private kernels, no external service;
* sqlite-memory      -- private SQLite files, no shared coordination;
* sqlite-redis       -- private SQLite files + real Redis, distinct pool names;
* mysql-memory       -- one MySQL schema, distinct pool names;
* mysql-redis        -- one MySQL schema + real Redis, distinct pool names.

Two extra tests document the two configuration conditions that make a shard
safe: overlapping instances on one memory-coordinated shard double-book
(known gap, xfail), and sharing a Redis pool name across private storage
shards pollutes startup reconcile.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import multiprocessing as mp
import os
import time
import traceback
import uuid

import pytest
import redis as redis_sync
from conftest import FakeChecker
from redis import asyncio as aioredis

from purser import AcquireRequest, SyncPurser
from purser.errors import PoolExhausted, PurserError, ResourceBusy

REAL_REDIS_URL = os.environ.get("PURSER_TEST_REDIS_URL")
MYSQL_URL = os.environ.get("PURSER_TEST_MYSQL_URL")

BUSY = (ResourceBusy, PoolExhausted, PurserError)


@pytest.fixture(autouse=True)
def _simulate_separate_hosts(shared_backend_harness):
    """Every shard models a separate OS process in the same test process."""

    yield


SHAPES = [
    pytest.param("memory-memory", id="memory-memory"),
    pytest.param("sqlite-memory", id="sqlite-memory"),
    pytest.param(
        "sqlite-redis",
        id="sqlite-redis",
        marks=pytest.mark.skipif(
            not REAL_REDIS_URL, reason="PURSER_TEST_REDIS_URL not set"
        ),
    ),
    pytest.param(
        "mysql-memory",
        id="mysql-memory",
        marks=pytest.mark.skipif(
            not MYSQL_URL, reason="PURSER_TEST_MYSQL_URL not set (not tested)"
        ),
    ),
    pytest.param(
        "mysql-redis",
        id="mysql-redis",
        marks=pytest.mark.skipif(
            not (MYSQL_URL and REAL_REDIS_URL),
            reason="PURSER_TEST_MYSQL_URL/PURSER_TEST_REDIS_URL not set",
        ),
    ),
]


def _flush_redis() -> None:
    async def run() -> None:
        client = aioredis.Redis.from_url(
            REAL_REDIS_URL, protocol=2, socket_connect_timeout=5
        )
        try:
            await client.flushdb()
        finally:
            await client.aclose()

    asyncio.run(run())


@pytest.fixture(autouse=True)
def _clean_redis():
    if REAL_REDIS_URL:
        _flush_redis()
    yield
    if REAL_REDIS_URL:
        _flush_redis()


@pytest.fixture
def mysql_cleanup():
    items: list[tuple[dict, str]] = []
    yield items
    for cfg, name in items:
        _delete_pool(cfg, name)


def _split(shape: str) -> tuple[str, str]:
    storage, coordination = shape.split("-", 1)
    return storage, coordination


def _shard(shape: str, tmp_path, label: str) -> tuple[dict, str]:
    storage, coordination = _split(shape)
    cfg: dict = {"storage": storage, "coordination": coordination}
    if storage == "sqlite":
        cfg["sqlite_path"] = str(tmp_path / f"{label}.sqlite3")
    elif storage == "mysql":
        cfg["mysql_url"] = MYSQL_URL
    if coordination == "redis":
        cfg["redis_url"] = REAL_REDIS_URL
    return cfg, f"static-{label}-{uuid.uuid4().hex[:8]}"


def _new_instance(cfg: dict) -> SyncPurser:
    ps = SyncPurser(maintenance_enabled=False, **cfg)
    ps.register_type(FakeChecker())
    return ps


def _open_pool(
    ps: SyncPurser, name: str, *, max_slots: int = 1, lease_ttl: float = 30.0
):
    # Create before start so start() awaits the pool's reconcile: assertions
    # right after it are deterministic.
    pool = ps.create_pool(
        name, "fakecheck", max_slots=max_slots, lease_ttl=lease_ttl
    )
    ps.start()
    return pool


def _seed(pool, host: str) -> None:
    specs = [
        {"id": "r0", "host": host, "port": 9300},
        {"id": "r1", "host": host, "port": 9301},
    ]
    report = pool.generate(specs)
    assert report.imported == 2 and not report.errors, report.errors


def _write_json(path: str, payload: dict) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh)
    os.replace(tmp, path)


def _read_json(path: str) -> dict:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def _delete_pool(cfg: dict, name: str) -> None:
    ps: SyncPurser | None = None
    try:
        ps = _new_instance(cfg)
        ps.start()
        pool = ps.pool(name, "fakecheck", max_slots=1, lease_ttl=30.0)
        for lease in list(pool.leases()):
            with contextlib.suppress(PurserError):
                pool.force_revoke(
                    lease.resource_id,
                    operator="cleanup",
                    reason="static-shard cleanup",
                    token=lease.token,
                )
        ps.delete_pool(name, force=True)
    except BaseException:
        pass
    finally:
        if ps is not None:
            with contextlib.suppress(BaseException):
                ps.close()


def _shard_worker(
    cfg: dict,
    name: str,
    host: str,
    marker: str,
    release_path: str,
    result_path: str,
) -> None:
    """Hold one slot in this process's shard until the parent releases it."""

    result: dict = {
        "ok": False,
        "pid": os.getpid(),
        "pool": name,
        "token": None,
        "error": None,
    }
    ps: SyncPurser | None = None
    lease = None
    try:
        ps = _new_instance(cfg)
        pool = _open_pool(ps, name)
        _seed(pool, host)
        lease = pool.acquire(
            AcquireRequest(
                resource_id="r0",
                consumer=f"shard-{os.getpid()}",
                slots=1,
                timeout=10.0,
            )
        )
        resource = pool.require("r0")
        result.update(
            {
                "ok": True,
                "token": lease.token,
                "used": resource.concurrency.used_slots,
                "leases": [item.token for item in pool.leases("r0")],
                "host": resource.payload.get("host"),
                "resource_ids": sorted(item.id for item in pool.query()),
                "stats_active": pool.stats()["active_leases"],
            }
        )
        _write_json(marker, result)

        deadline = time.monotonic() + 60.0
        while not os.path.exists(release_path):
            if time.monotonic() > deadline:
                raise TimeoutError("parent never signaled release")
            time.sleep(0.02)

        result["released"] = pool.release(lease)
        result["used_after"] = pool.require("r0").concurrency.used_slots
        result["leases_after"] = [item.token for item in pool.leases("r0")]
    except BaseException:
        result["error"] = traceback.format_exc()
    finally:
        _write_json(result_path, result)
        if ps is not None:
            with contextlib.suppress(BaseException):
                ps.close()


def _redis_token_index(name: str) -> set[str]:
    client = redis_sync.Redis.from_url(
        REAL_REDIS_URL, protocol=2, decode_responses=True
    )
    try:
        return set(client.hkeys(f"purser:{{{name}}}:leasetoken"))
    finally:
        client.close()


def _inspect(cfg: dict, name: str) -> tuple[list[str], list[str], dict]:
    """Open the shard afresh and return (resource ids, hosts, used slots)."""

    ps = _new_instance(cfg)
    try:
        pool = _open_pool(ps, name)
        resources = pool.query()
        return (
            sorted(resource.id for resource in resources),
            sorted(str(resource.payload.get("host")) for resource in resources),
            {resource.id: resource.concurrency.used_slots for resource in resources},
        )
    finally:
        with contextlib.suppress(BaseException):
            ps.close()


@pytest.mark.parametrize("shape", SHAPES)
def test_static_shards_run_independently(shape, tmp_path, mysql_cleanup):
    cfg_a, name_a = _shard(shape, tmp_path, "a")
    cfg_b, name_b = _shard(shape, tmp_path, "b")
    host_a, host_b = "shard-a.internal", "shard-b.internal"
    for cfg, name in ((cfg_a, name_a), (cfg_b, name_b)):
        if cfg["storage"] == "mysql":
            mysql_cleanup.append((cfg, name))

    marker_a = str(tmp_path / "a.acquired.json")
    marker_b = str(tmp_path / "b.acquired.json")
    result_a = str(tmp_path / "a.result.json")
    result_b = str(tmp_path / "b.result.json")
    release_path = str(tmp_path / "release.flag")

    ctx = mp.get_context("spawn")
    procs = [
        ctx.Process(
            target=_shard_worker,
            args=(cfg, name, host, marker, release_path, result),
        )
        for cfg, name, host, marker, result in (
            (cfg_a, name_a, host_a, marker_a, result_a),
            (cfg_b, name_b, host_b, marker_b, result_b),
        )
    ]
    for proc in procs:
        proc.start()

    both_ready = False
    try:
        deadline = time.monotonic() + 90.0
        while time.monotonic() < deadline:
            if os.path.exists(marker_a) and os.path.exists(marker_b):
                both_ready = True
                break
            if any(not proc.is_alive() for proc in procs):
                break
            time.sleep(0.05)
        if both_ready and _split(shape)[1] == "redis":
            # Still holding: lease namespaces are per pool name, so neither
            # shard can see the other's token.
            held = [_read_json(marker_a), _read_json(marker_b)]
            assert _redis_token_index(name_a) == {held[0]["token"]}
            assert _redis_token_index(name_b) == {held[1]["token"]}
        with open(release_path, "w", encoding="utf-8") as fh:
            fh.write("release")
        for proc in procs:
            proc.join(timeout=120)
    finally:
        for proc in procs:
            if proc.is_alive():
                proc.terminate()
                proc.join(timeout=10)

    assert all(proc.exitcode == 0 for proc in procs), [p.exitcode for p in procs]
    results = [_read_json(result_a), _read_json(result_b)]
    assert all(item["error"] is None for item in results), results
    assert both_ready, "both shards never held a slot at the same time"
    assert all(item["ok"] for item in results), results

    first, second = results
    assert first["token"] != second["token"]
    assert first["host"] == host_a and second["host"] == host_b
    for item in results:
        # Same resource ids in every shard, and each shard independently
        # admits one holder even though both have max_slots == 1.
        assert item["resource_ids"] == ["r0", "r1"]
        assert item["used"] == 1, item
        assert item["leases"] == [item["token"]], item
        assert item["stats_active"] == 1, item
        assert item["released"] is True
        assert item["used_after"] == 0, item
        assert item["leases_after"] == [], item

    if _split(shape)[1] == "redis":
        # Both namespaces are empty after release; the held-time check above
        # proved each shard saw only its own token.
        assert _redis_token_index(name_a) == set()
        assert _redis_token_index(name_b) == set()

    # A fresh instance re-opens each shard: data must be isolated per shard.
    ids_a, hosts_a, used_a = _inspect(cfg_a, name_a)
    ids_b, hosts_b, used_b = _inspect(cfg_b, name_b)
    if _split(shape)[0] == "memory":
        # In-memory storage is per process; a restart starts empty.
        assert ids_a == [] and ids_b == [], (ids_a, ids_b)
        assert hosts_a == [] and hosts_b == []
    else:
        assert ids_a == ["r0", "r1"] and ids_b == ["r0", "r1"]
        assert hosts_a == [host_a, host_a], hosts_a
        assert hosts_b == [host_b, host_b], hosts_b
        assert used_a == {"r0": 0, "r1": 0} and used_b == {"r0": 0, "r1": 0}


@pytest.mark.xfail(
    strict=False,
    reason="V02/P0-2: memory coordination is instance-private; a second "
    "instance on the same SQLite shard reconciles away the first instance's "
    "live slot, so overlapping instances can double-book one shard",
)
def test_one_memory_coordinated_shard_rejects_overlapping_instances(tmp_path):
    """Static-sharding condition: old and new instances must not overlap."""

    name = f"static-overlap-{uuid.uuid4().hex[:8]}"
    cfg = {
        "storage": "sqlite",
        "coordination": "memory",
        "sqlite_path": str(tmp_path / "overlap.sqlite3"),
    }
    ps_a = _new_instance(cfg)
    ps_b = _new_instance(cfg)
    lease_a = None
    lease_b = None
    try:
        pool_a = _open_pool(ps_a, name)
        _seed(pool_a, "overlap.internal")
        lease_a = pool_a.acquire(
            AcquireRequest(resource_id="r0", consumer="old", slots=1, timeout=2.0)
        )
        assert pool_a.require("r0").concurrency.used_slots == 1

        # A new process opens the same shard while the old holder is alive.
        pool_b = _open_pool(ps_b, name)
        assert pool_b.require("r0").concurrency.used_slots == 0, (
            "startup reconcile unexpectedly preserved the live slot"
        )
        with pytest.raises(BUSY):
            lease_b = pool_b.acquire(
                AcquireRequest(
                    resource_id="r0", consumer="new", slots=1, timeout=0.6
                )
            )
    finally:
        if lease_b is not None:
            with contextlib.suppress(PurserError):
                pool_b.release(lease_b)
        if lease_a is not None:
            with contextlib.suppress(PurserError):
                pool_a.release(lease_a)
        with contextlib.suppress(BaseException):
            ps_b.close()
        with contextlib.suppress(BaseException):
            ps_a.close()


@pytest.mark.skipif(not REAL_REDIS_URL, reason="PURSER_TEST_REDIS_URL not set")
def test_shared_redis_requires_distinct_shard_names(tmp_path):
    """A shared coordination plane keys leases by pool name only.

    Two SQLite files are genuinely independent storage shards, but with the
    same pool name and one Redis DB the second shard's startup reconcile
    adopts the first shard's lease as its own usage.  Distinct pool names (or
    separate Redis DBs) are therefore mandatory; this test pins the hazard.
    """

    name = f"static-collision-{uuid.uuid4().hex[:8]}"
    cfg_a = {
        "storage": "sqlite",
        "coordination": "redis",
        "sqlite_path": str(tmp_path / "collision-a.sqlite3"),
        "redis_url": REAL_REDIS_URL,
    }
    cfg_b = {
        "storage": "sqlite",
        "coordination": "redis",
        "sqlite_path": str(tmp_path / "collision-b.sqlite3"),
        "redis_url": REAL_REDIS_URL,
    }

    # Seed shard B first so its rows exist when it reconciles later.
    preseed = _new_instance(cfg_b)
    try:
        _seed(_open_pool(preseed, name), "collision-b.internal")
    finally:
        preseed.close()

    ps_a = _new_instance(cfg_a)
    ps_b = _new_instance(cfg_b)
    lease_a = None
    try:
        pool_a = _open_pool(ps_a, name)
        _seed(pool_a, "collision-a.internal")
        lease_a = pool_a.acquire(
            AcquireRequest(resource_id="r0", consumer="a", slots=1, timeout=2.0)
        )

        pool_b = _open_pool(ps_b, name)
        assert pool_b.require("r0").concurrency.used_slots == 1, (
            "private storage shard did not adopt the shared lease"
        )
        assert [item.token for item in pool_b.leases("r0")] == [lease_a.token]
        with pytest.raises(BUSY):
            pool_b.acquire(
                AcquireRequest(resource_id="r0", consumer="b", slots=1, timeout=0.6)
            )
    finally:
        if lease_a is not None:
            with contextlib.suppress(PurserError):
                pool_a.release(lease_a)
        with contextlib.suppress(BaseException):
            ps_b.close()
        with contextlib.suppress(BaseException):
            ps_a.close()

    # After the lease is gone, reopening the shard reconciles it back to zero.
    ids, hosts, used = _inspect(cfg_b, name)
    assert ids == ["r0", "r1"]
    assert hosts == ["collision-b.internal", "collision-b.internal"]
    assert used == {"r0": 0, "r1": 0}
