"""Acceptance matrix: synchronous facade, single process, single instance.

Each test runs against every supported storage x coordination pair.  MySQL
pairs are reported as not-tested unless ``PURSER_TEST_MYSQL_URL`` is set.
Redis pairs use a real server when ``PURSER_TEST_REDIS_URL`` is set and
fakeredis otherwise; ``sync_backend.backend`` plus ``sync_backend.simulated``
records which one was used.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import threading
import time
import uuid

import fakeredis
import pytest
from conftest import FakeChecker

from purser import (
    AcquireRequest,
    PoolExhausted,
    ResourceState,
    SyncPool,
    SyncPurser,
    UsageOutcome,
)

REAL_REDIS_URL = os.environ.get("PURSER_TEST_REDIS_URL")
MYSQL_URL = os.environ.get("PURSER_TEST_MYSQL_URL")

BACKENDS = [
    pytest.param(("memory", "memory"), id="memory-memory"),
    pytest.param(("memory", "redis"), id="memory-redis"),
    pytest.param(("sqlite", "memory"), id="sqlite-memory"),
    pytest.param(("sqlite", "redis"), id="sqlite-redis"),
    pytest.param(
        ("mysql", "memory"),
        id="mysql-memory",
        marks=pytest.mark.skipif(
            not MYSQL_URL, reason="PURSER_TEST_MYSQL_URL not set (not tested)"
        ),
    ),
    pytest.param(
        ("mysql", "redis"),
        id="mysql-redis",
        marks=pytest.mark.skipif(
            not MYSQL_URL, reason="PURSER_TEST_MYSQL_URL not set (not tested)"
        ),
    ),
]

POOL_METHODS = {
    "start",
    "close",
    "reconfigure",
    "validate_jobs",
    "validate_job",
    "cancel_validate_job",
    "get",
    "require",
    "query",
    "iter_resources",
    "count_matching",
    "batches",
    "counts",
    "stats",
    "describe",
    "leases",
    "import_resources",
    "generate",
    "acquire",
    "lease",
    "release",
    "release_consumer",
    "validate_now",
    "ban",
    "unban",
    "suspend",
    "resume",
    "top_up",
    "drain",
    "cancel_drain",
    "restore",
    "admin_set_state",
    "force_revoke",
    "update_resource",
    "move_resource",
    "recall_batch",
    "run_maintenance",
    "acquire_many",
    "renew_manual",
}

PURSER_METHODS = {
    "start",
    "close",
    "create_pool",
    "pool",
    "get_pool",
    "require_pool",
    "pool_names",
    "delete_pool",
    "register_adapter",
    "report_usage",
    "backup",
    "restore",
}


@pytest.fixture(params=BACKENDS)
def sync_backend(request, tmp_path):
    storage, coordination = request.param
    created: list[SyncPurser] = []
    clients: list[object] = []

    def factory(**overrides):
        kwargs: dict = {
            "maintenance_enabled": False,
            "storage": storage,
            "coordination": coordination,
        }
        if storage == "sqlite":
            kwargs["sqlite_path"] = str(tmp_path / "purser.sqlite3")
        elif storage == "mysql":
            kwargs["mysql_url"] = MYSQL_URL
        if coordination == "redis":
            if REAL_REDIS_URL:
                kwargs["redis_url"] = REAL_REDIS_URL
            else:
                client = fakeredis.aioredis.FakeRedis()
                clients.append(client)
                kwargs["redis_client"] = client
        kwargs.update(overrides)
        ps = SyncPurser(**kwargs)
        ps.register_adapter(FakeChecker())
        ps.start()
        created.append(ps)
        return ps

    factory.backend = (storage, coordination)  # type: ignore[attr-defined]
    factory.simulated = (  # type: ignore[attr-defined]
        coordination == "redis" and not REAL_REDIS_URL
    )
    yield factory
    for ps in reversed(created):
        with contextlib.suppress(BaseException):
            ps.close()
    for client in clients:
        with contextlib.suppress(BaseException):
            asyncio.run(client.aclose())


@pytest.fixture
def pool_name():
    suffix = uuid.uuid4().hex[:10]
    return lambda base="p": f"{base}-{suffix}"


def seed(pool: SyncPool, n: int = 2, *, prefix: str = "r") -> list[str]:
    specs = [
        {"id": f"{prefix}{i}", "host": "127.0.0.1", "port": 9000 + i}
        for i in range(n)
    ]
    report = pool.generate(specs)
    assert report.imported == n and not report.errors, report.errors
    return [spec["id"] for spec in specs]


def test_public_api_surface():
    assert POOL_METHODS <= {name for name in dir(SyncPool) if not name.startswith("_")}
    assert PURSER_METHODS <= {
        name for name in dir(SyncPurser) if not name.startswith("_")
    }


def test_lifecycle_crud_and_events(sync_backend, pool_name):
    ps = sync_backend()
    assert ps.started and not ps.closed
    ps.start()  # idempotent
    name = pool_name("p")
    pool = ps.create_pool(name, "fakecheck")
    ids = seed(pool, 3)

    assert pool.counts(by_name=True).get("idle") == 3
    assert [resource.id for resource in pool.query()] == ids
    with pool.iter_resources(batch_size=1) as iterator:
        assert [resource.id for resource in iterator] == ids

    lease = pool.acquire(consumer="c")
    assert lease.resource_id in ids
    assert len(pool.leases()) == 1
    assert pool.renew_manual(lease) is True
    lease.used = 2
    lease.outcome = UsageOutcome.OK
    assert pool.release(lease) is True
    assert pool.leases() == []
    assert pool.counts(by_name=True).get("idle") == 3

    seen: list[str] = []
    done = threading.Event()

    def callback(event) -> None:
        seen.append(event.type)
        if "lease_released" in seen:
            done.set()

    subscription = ps.subscribe(callback)
    extra = pool.acquire(consumer="event-c")
    pool.release(extra)
    assert done.wait(10), seen
    subscription.close()
    assert {"lease_acquired", "lease_released"} <= set(seen)
    assert any(event.type == "pool_created" for event in ps.history())

    report = ps.report_usage()
    assert report["window"]["events"] >= 1
    assert report["totals"]["releases"] >= 1


def test_backup_restore_roundtrip(sync_backend, pool_name):
    ps = sync_backend()
    name = pool_name("p")
    pool = ps.create_pool(name, "fakecheck")
    seed(pool, 2)

    backup = ps.backup()
    assert backup["format"] == "purser-backup"
    assert [entry["name"] for entry in backup["pools"]] == [name]

    ps.delete_pool(name, force=True)
    assert ps.pool_names() == []
    report = ps.restore(backup)
    assert report["ok"] is True and report["created_pools"] == [name]
    assert ps.require_pool(name).counts(by_name=True).get("idle") == 2


def test_multithread_no_oversell(sync_backend, pool_name):
    ps = sync_backend()
    pool = ps.create_pool(pool_name("p"), "fakecheck")
    seed(pool, 1)

    workers, iterations = 6, 10
    errors: list[BaseException] = []
    successes = [0] * workers
    max_live = [0]
    state_lock = threading.Lock()
    barrier = threading.Barrier(workers)

    def worker(index: int) -> None:
        try:
            barrier.wait(timeout=20)
            for _ in range(iterations):
                lease = pool.acquire(
                    AcquireRequest(consumer=f"t{index}", timeout=5.0)
                )
                with state_lock:
                    live = len(pool.leases())
                    max_live[0] = max(max_live[0], live)
                pool.release(lease)
                successes[index] += 1
        except BaseException as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)

    assert not errors, errors
    assert sum(successes) == workers * iterations
    assert max_live[0] <= 1
    assert pool.leases() == []
    assert pool.counts(by_name=True).get("idle") == 1


def test_wait_timeout_and_outer_timeout(sync_backend, pool_name):
    ps = sync_backend()
    name = pool_name("p")
    pool = ps.create_pool(name, "fakecheck")
    seed(pool, 1)

    held = pool.acquire(consumer="holder")
    started = time.monotonic()
    with pytest.raises(PoolExhausted):
        pool.acquire(AcquireRequest(consumer="waiter", timeout=0.05))
    assert time.monotonic() - started < 10
    pool.release(held)

    with pytest.raises(TimeoutError):
        ps._submit(lambda: asyncio.sleep(0.5), timeout=0.02)
    assert ps.pool_names() == [name]
    assert pool.counts(by_name=True).get("idle") == 1


def test_close_is_idempotent_and_rejects_calls(sync_backend, pool_name):
    ps = sync_backend()
    pool = ps.create_pool(pool_name("p"), "fakecheck")
    seed(pool, 1)
    ps.close()
    assert ps.closed
    ps.close()
    with pytest.raises(RuntimeError):
        pool.counts()
    with pytest.raises(RuntimeError):
        ps.create_pool(pool_name("q"), "fakecheck")


def test_close_under_load_does_not_hang(sync_backend, pool_name):
    ps = sync_backend(close_timeout=10)
    pool = ps.create_pool(pool_name("p"), "fakecheck")
    seed(pool, 2)

    stop = threading.Event()
    unexpected: list[BaseException] = []
    barrier = threading.Barrier(3)

    def worker() -> None:
        barrier.wait(timeout=10)
        while not stop.is_set():
            try:
                lease = pool.acquire(consumer="w")
                pool.release(lease)
            except (RuntimeError, PoolExhausted):
                # Pool.close() fails queued waiters with PoolExhausted.
                return
            except BaseException as exc:  # pragma: no cover - surfaced below
                unexpected.append(exc)
                return

    threads = [threading.Thread(target=worker) for _ in range(3)]
    for thread in threads:
        thread.start()
    time.sleep(0.05)
    ps.close()
    stop.set()
    for thread in threads:
        thread.join(timeout=10)

    assert not any(thread.is_alive() for thread in threads)
    assert not unexpected


def test_restart_persistence(sync_backend, pool_name):
    storage, _ = sync_backend.backend
    name = pool_name("p")
    ps = sync_backend()
    pool = ps.create_pool(name, "fakecheck")
    seed(pool, 2)
    ps.close()

    restarted = sync_backend()
    reused = restarted.create_pool(name, "fakecheck")
    counts = reused.counts(by_name=True)
    if storage == "memory":
        assert counts.get("idle", 0) == 0
    else:
        assert counts.get("idle") == 2


def test_admin_surface(sync_backend, pool_name):
    ps = sync_backend()
    pool = ps.create_pool(pool_name("p"), "fakecheck")
    seed(pool, 3)

    pool.ban("r0", reason="risk")
    assert pool.require("r0").state is ResourceState.BANNED
    pool.unban("r0", reason="safe", revalidate=False)
    assert pool.validate_now("r0").alive is True
    assert pool.require("r0").state is ResourceState.IDLE

    pool.suspend("r1", reason="hold")
    assert pool.require("r1").state is ResourceState.SUSPENDED
    pool.resume("r1", reason="back")
    assert pool.require("r1").state is ResourceState.IDLE

    updated = pool.update_resource("r1", tags={"eu"}, reason="retag")
    assert updated.tags == frozenset({"eu"})

    other = ps.create_pool(pool_name("q"), "fakecheck")
    pool.move_resource("r1", other, reason="rebalance")
    assert other.require("r1").id == "r1"

    lease = other.acquire(consumer="c")
    revoked = other.force_revoke(lease.resource_id, reason="kick")
    assert revoked.token == lease.token
    assert other.leases() == []

    assert isinstance(other.run_maintenance("quota_reset"), dict)
    assert isinstance(pool.validate_jobs(), list)
