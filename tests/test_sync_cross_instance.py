"""Cross-instance acceptance for the synchronous facade.

Two independent :class:`SyncPurser` instances (each owning its own event-loop
thread) share a storage plane (one SQLite file or one MySQL schema) and a real
Redis coordination plane.  Covered scenarios:

* B sees A's lease, startup reconcile keeps A's live slot, capacity is global;
* B can force-revoke A's lease and A's late release must not steal B's slot;
* a directed acquire in B is woken by polling when A releases;
* an undirected waiter in B is woken by the same polling loop;
* concurrent threads on both instances never oversell globally;
* a hard-crashed process's slot is reclaimed after the lease TTL + a restart;
* two OS processes contend without oversell.

Real Redis is mandatory: a fakeredis server is bound to one event loop and
cannot back two independent sync instances.  The dedicated DB is flushed
before and after every test.  MySQL pools are removed with
``delete_pool(force=True)`` so the dedicated schema does not accumulate rows.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import multiprocessing as mp
import os
import threading
import time
import uuid

import pytest
from conftest import FakeChecker
from redis import asyncio as aioredis

from purser import AcquireRequest, SyncPurser
from purser.errors import PoolExhausted, PurserError, ResourceBusy

REAL_REDIS_URL = os.environ.get("PURSER_TEST_REDIS_URL")
MYSQL_URL = os.environ.get("PURSER_TEST_MYSQL_URL")

pytestmark = pytest.mark.skipif(
    not REAL_REDIS_URL,
    reason="cross-instance needs a shared real Redis (PURSER_TEST_REDIS_URL)",
)

BACKENDS = [
    pytest.param("sqlite", id="sqlite-redis"),
    pytest.param(
        "mysql",
        id="mysql-redis",
        marks=pytest.mark.skipif(
            not MYSQL_URL, reason="PURSER_TEST_MYSQL_URL not set (not tested)"
        ),
    ),
]

BUSY = (ResourceBusy, PoolExhausted, PurserError)


@pytest.fixture(autouse=True)
def _simulate_separate_hosts(shared_backend_harness):
    """Each of the two instances models a separate host process."""

    yield


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


class Backend:
    """Creates sync instances over one shared storage + Redis coordination."""

    def __init__(self, kind: str, tmp_path) -> None:
        self.kind = kind
        self.sqlite_path = str(tmp_path / "shared.sqlite3")
        self.instances: list[SyncPurser] = []
        self.pools: dict[str, tuple[int, float]] = {}

    def kwargs(self) -> dict:
        kwargs: dict = {
            "maintenance_enabled": False,
            "storage": self.kind,
            "coordination": "redis",
            "redis_url": REAL_REDIS_URL,
        }
        if self.kind == "sqlite":
            kwargs["sqlite_path"] = self.sqlite_path
        else:
            kwargs["mysql_url"] = MYSQL_URL
        return kwargs

    def name(self) -> str:
        return f"xinst-{uuid.uuid4().hex[:10]}"

    def make(
        self, name: str, *, max_slots: int = 1, lease_ttl: float = 30.0
    ) -> tuple[SyncPurser, object]:
        ps = SyncPurser(**self.kwargs())
        ps.register_adapter(FakeChecker())
        # Create before starting: ``ps.start()`` then awaits the pool's
        # startup reconcile, so assertions after it are deterministic.
        pool = ps.create_pool(
            name, "fakecheck", max_slots=max_slots, lease_ttl=lease_ttl
        )
        ps.start()
        self.instances.append(ps)
        self.pools[name] = (max_slots, lease_ttl)
        return ps, pool

    def teardown(self) -> None:
        for ps in reversed(self.instances):
            with contextlib.suppress(BaseException):
                ps.close()
        for name, (max_slots, lease_ttl) in self.pools.items():
            self._destroy(name, max_slots, lease_ttl)

    def _destroy(self, name: str, max_slots: int, lease_ttl: float) -> None:
        ps: SyncPurser | None = None
        try:
            ps = SyncPurser(**self.kwargs())
            ps.register_adapter(FakeChecker())
            ps.start()
            pool = ps.pool(
                name, "fakecheck", max_slots=max_slots, lease_ttl=lease_ttl
            )
            for lease in list(pool.leases()):
                with contextlib.suppress(PurserError):
                    pool.force_revoke(
                        lease.resource_id,
                        operator="cleanup",
                        reason="cross-instance cleanup",
                        token=lease.token,
                    )
            ps.delete_pool(name, force=True)
        except BaseException:
            pass
        finally:
            if ps is not None:
                with contextlib.suppress(BaseException):
                    ps.close()


@pytest.fixture(params=BACKENDS)
def backend(request, tmp_path):
    instance = Backend(request.param, tmp_path)
    yield instance
    instance.teardown()


def seed(pool, n: int = 1) -> list[str]:
    specs = [
        {"id": f"r{i}", "host": "127.0.0.1", "port": 9100 + i} for i in range(n)
    ]
    report = pool.generate(specs)
    assert report.imported == n and not report.errors, report.errors
    return [spec["id"] for spec in specs]


def wait_until(predicate, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return predicate()


def test_cross_instance_visibility_capacity_and_startup_reconcile(backend):
    name = backend.name()
    _, pool_a = backend.make(name, max_slots=1)
    seed(pool_a)
    lease_a = pool_a.acquire(
        AcquireRequest(resource_id="r0", consumer="a", slots=1, timeout=2.0)
    )

    # B's start() reconciles against shared storage while A holds the slot.
    ps_b, pool_b = backend.make(name, max_slots=1)
    assert ps_b.started

    resource = pool_b.get("r0")
    assert resource is not None and resource.concurrency.used_slots == 1, (
        "startup reconcile wiped another instance's live slot"
    )
    assert [lease.token for lease in pool_b.leases("r0")] == [lease_a.token]
    assert pool_b.stats()["active_leases"] == 1

    with pytest.raises(BUSY):
        pool_b.acquire(
            AcquireRequest(resource_id="r0", consumer="b", slots=1, timeout=0.6)
        )
    # The failed attempt must not corrupt the authoritative lease.
    assert [lease.token for lease in pool_b.leases("r0")] == [lease_a.token]
    assert pool_b.get("r0").concurrency.used_slots == 1

    assert pool_a.release(lease_a) is True
    assert pool_b.leases("r0") == []
    lease_b = pool_b.acquire(
        AcquireRequest(resource_id="r0", consumer="b", slots=1, timeout=2.0)
    )
    assert pool_b.release(lease_b) is True
    assert pool_b.get("r0").concurrency.used_slots == 0


def test_cross_instance_force_revoke_and_stale_release(backend):
    name = backend.name()
    _, pool_a = backend.make(name, max_slots=1)
    _, pool_b = backend.make(name, max_slots=1)
    seed(pool_a)

    lease_a = pool_a.acquire(
        AcquireRequest(resource_id="r0", consumer="a", slots=1, timeout=2.0)
    )
    revoked = pool_b.force_revoke(
        "r0", operator="test", reason="cross-instance", token=lease_a.token
    )
    assert revoked.token == lease_a.token
    assert pool_b.leases("r0") == []
    assert pool_b.get("r0").concurrency.used_slots == 0

    lease_b = pool_b.acquire(
        AcquireRequest(resource_id="r0", consumer="b", slots=1, timeout=2.0)
    )
    # A never saw the revoke; its late release must be a storage-plane no-op.
    assert pool_a.release(lease_a) is True
    assert pool_b.get("r0").concurrency.used_slots == 1, "stale release stole a slot"
    assert [lease.token for lease in pool_b.leases("r0")] == [lease_b.token]

    assert pool_b.release(lease_b) is True
    assert pool_b.get("r0").concurrency.used_slots == 0
    assert pool_b.leases("r0") == []


def test_cross_instance_directed_acquire_wakes_by_polling(backend):
    name = backend.name()
    _, pool_a = backend.make(name, max_slots=1)
    _, pool_b = backend.make(name, max_slots=1)
    seed(pool_a)

    lease_a = pool_a.acquire(
        AcquireRequest(resource_id="r0", consumer="a", slots=1, timeout=2.0)
    )
    result: dict = {}

    def waiter() -> None:
        started = time.monotonic()
        try:
            result["lease"] = pool_b.acquire(
                AcquireRequest(resource_id="r0", consumer="b", slots=1, timeout=3.0)
            )
        except BaseException as exc:  # pragma: no cover - diagnostic path
            result["error"] = exc
        result["elapsed"] = time.monotonic() - started

    thread = threading.Thread(target=waiter)
    thread.start()
    time.sleep(0.35)
    assert pool_a.release(lease_a) is True
    thread.join(timeout=10)
    assert not thread.is_alive()

    assert "lease" in result, result.get("error")
    # Directed acquisition polls shared storage every 100 ms.
    assert result["elapsed"] < 2.5, result["elapsed"]
    assert pool_b.release(result["lease"]) is True


def test_cross_instance_undirected_waiter_wakes_by_polling(backend):
    name = backend.name()
    _, pool_a = backend.make(name, max_slots=1)
    _, pool_b = backend.make(name, max_slots=1)
    seed(pool_a)

    lease_a = pool_a.acquire(
        AcquireRequest(resource_id="r0", consumer="a", slots=1, timeout=2.0)
    )
    result: dict = {}

    def waiter() -> None:
        started = time.monotonic()
        try:
            result["lease"] = pool_b.acquire(
                AcquireRequest(consumer="b", slots=1, timeout=1.5)
            )
        except BaseException as exc:
            result["error"] = exc
        result["elapsed"] = time.monotonic() - started

    thread = threading.Thread(target=waiter)
    thread.start()
    time.sleep(0.35)
    assert pool_a.release(lease_a) is True
    thread.join(timeout=10)
    assert not thread.is_alive()

    if "lease" in result:
        pool_b.release(result["lease"])
    assert "lease" in result, result.get("error")
    # The waiter poller runs every 100 ms; it must beat the 1.5 s timeout.
    assert result["elapsed"] < 1.2, result["elapsed"]


def test_cross_instance_threads_never_oversell(backend):
    name = backend.name()
    _, pool_a = backend.make(name, max_slots=2)
    _, pool_b = backend.make(name, max_slots=2)
    seed(pool_a)

    max_slots = 2
    observed = {"used": 0}
    errors: list[BaseException] = []
    lock = threading.Lock()
    successes = {"n": 0}

    def run(pool, tag: str) -> None:
        for i in range(15):
            try:
                lease = pool.acquire(
                    AcquireRequest(
                        resource_id="r0",
                        consumer=f"{tag}-{i}",
                        slots=1,
                        timeout=0.5,
                    )
                )
            except PurserError:
                continue
            try:
                resource = pool.get("r0")
                if resource is not None:
                    with lock:
                        observed["used"] = max(
                            observed["used"], resource.concurrency.used_slots
                        )
                with lock:
                    successes["n"] += 1
            except BaseException as exc:  # pragma: no cover - diagnostic path
                errors.append(exc)
            finally:
                pool.release(lease)

    threads = [
        threading.Thread(target=run, args=(pool, f"{label}-{i}"))
        for label, pool in (("a", pool_a), ("b", pool_b))
        for i in range(3)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)
    assert not any(thread.is_alive() for thread in threads)
    assert not errors, errors

    assert observed["used"] <= max_slots, f"global oversell: {observed['used']}"
    assert successes["n"] > 0, "no acquisition succeeded"

    assert wait_until(
        lambda: pool_a.get("r0").concurrency.used_slots == 0
        and pool_a.leases("r0") == [],
        10.0,
    ), "slots leaked after concurrent load"


def _crash_worker(
    kind: str,
    sqlite_path: str | None,
    mysql_url: str | None,
    redis_url: str,
    name: str,
    lease_ttl: float,
    marker: str,
) -> None:
    """Acquire through the sync facade, then die without release or close."""

    ps = SyncPurser(
        maintenance_enabled=False,
        storage=kind,
        coordination="redis",
        sqlite_path=sqlite_path,
        mysql_url=mysql_url,
        redis_url=redis_url,
    )
    ps.register_adapter(FakeChecker())
    pool = ps.create_pool(name, "fakecheck", max_slots=1, lease_ttl=lease_ttl)
    ps.start()
    lease = pool.acquire(
        AcquireRequest(resource_id="r0", consumer="dead", slots=1, timeout=5.0)
    )
    with open(marker, "w", encoding="utf-8") as fh:
        fh.write(lease.token)
    os._exit(0)


def test_cross_instance_hard_crash_reclaims_after_ttl(backend, tmp_path):
    name = backend.name()
    lease_ttl = 1.0
    seed_ps, pool_seed = backend.make(name, max_slots=1, lease_ttl=lease_ttl)
    seed(pool_seed)
    seed_ps.close()

    marker = str(tmp_path / "crashed.marker")
    ctx = mp.get_context("spawn")
    proc = ctx.Process(
        target=_crash_worker,
        args=(
            backend.kind,
            backend.sqlite_path if backend.kind == "sqlite" else None,
            MYSQL_URL,
            REAL_REDIS_URL,
            name,
            lease_ttl,
            marker,
        ),
    )
    proc.start()
    try:
        deadline = time.monotonic() + 30
        while not os.path.exists(marker) and time.monotonic() < deadline:
            time.sleep(0.05)
        assert os.path.exists(marker), "crash worker never acquired"
        proc.join(timeout=30)
    finally:
        if proc.is_alive():
            proc.terminate()
            proc.join(timeout=10)
    assert proc.exitcode == 0

    ps_b, pool_b = backend.make(name, max_slots=1, lease_ttl=lease_ttl)
    resource = pool_b.get("r0")
    assert resource is not None and resource.concurrency.used_slots == 1
    assert len(pool_b.leases("r0")) == 1
    with pytest.raises(BUSY):
        pool_b.acquire(
            AcquireRequest(resource_id="r0", consumer="b", slots=1, timeout=0.6)
        )

    # Wait for the coordination key to expire (ttl + 5 s heartbeat grace).
    assert wait_until(lambda: pool_b.leases("r0") == [], lease_ttl + 12), (
        "dead lease never expired from Redis"
    )

    # A restart reconciles the orphaned slot back to zero.
    _, pool_b2 = backend.make(name, max_slots=1, lease_ttl=lease_ttl)
    resource = pool_b2.get("r0")
    assert resource is not None and resource.concurrency.used_slots == 0
    lease = pool_b2.acquire(
        AcquireRequest(resource_id="r0", consumer="b2", slots=1, timeout=2.0)
    )
    assert pool_b2.release(lease) is True
    assert pool_b2.get("r0").concurrency.used_slots == 0
    assert pool_b2.leases("r0") == []


def _contend_worker(
    kind: str,
    sqlite_path: str | None,
    mysql_url: str | None,
    redis_url: str,
    name: str,
    max_slots: int,
    rounds: int,
    result_path: str,
) -> None:
    """One OS process hammering the shared pool; records oversell evidence."""

    result: dict = {"ok": 0, "busy": 0, "oversell": 0, "max_used": 0, "error": None}
    ps = SyncPurser(
        maintenance_enabled=False,
        storage=kind,
        coordination="redis",
        sqlite_path=sqlite_path,
        mysql_url=mysql_url,
        redis_url=redis_url,
    )
    ps.register_adapter(FakeChecker())
    pool = ps.create_pool(name, "fakecheck", max_slots=max_slots, lease_ttl=30.0)
    try:
        ps.start()
        for i in range(rounds):
            try:
                lease = pool.acquire(
                    AcquireRequest(
                        resource_id="r0",
                        consumer=f"p{os.getpid()}-{i}",
                        slots=1,
                        timeout=0.5,
                    )
                )
            except PurserError:
                result["busy"] += 1
                continue
            try:
                resource = pool.get("r0")
                if resource is not None:
                    result["max_used"] = max(
                        result["max_used"], resource.concurrency.used_slots
                    )
                    if resource.concurrency.used_slots > max_slots:
                        result["oversell"] += 1
                time.sleep(0.005)
                result["ok"] += 1
            finally:
                pool.release(lease)
    except BaseException as exc:  # pragma: no cover - diagnostic path
        result["error"] = repr(exc)
    finally:
        with open(result_path, "w", encoding="utf-8") as fh:
            json.dump(result, fh)
        with contextlib.suppress(BaseException):
            ps.close()


def test_cross_process_contention_never_oversells(backend, tmp_path):
    name = backend.name()
    max_slots = 2
    setup_ps, setup_pool = backend.make(name, max_slots=max_slots)
    seed(setup_pool)
    setup_ps.close()

    ctx = mp.get_context("spawn")
    paths = [str(tmp_path / f"worker{i}.json") for i in range(2)]
    procs = [
        ctx.Process(
            target=_contend_worker,
            args=(
                backend.kind,
                backend.sqlite_path if backend.kind == "sqlite" else None,
                MYSQL_URL,
                REAL_REDIS_URL,
                name,
                max_slots,
                12,
                path,
            ),
        )
        for path in paths
    ]
    for proc in procs:
        proc.start()
    try:
        for proc in procs:
            proc.join(timeout=120)
    finally:
        for proc in procs:
            if proc.is_alive():
                proc.terminate()
                proc.join(timeout=10)

    assert all(proc.exitcode == 0 for proc in procs), [p.exitcode for p in procs]
    results = []
    for path in paths:
        with open(path, encoding="utf-8") as fh:
            results.append(json.load(fh))
    assert all(item["error"] is None for item in results), results
    assert all(item["oversell"] == 0 for item in results), results
    assert all(item["max_used"] <= max_slots for item in results), results
    assert sum(item["ok"] for item in results) > 0, results

    _, pool = backend.make(name, max_slots=max_slots)
    assert wait_until(
        lambda: pool.get("r0").concurrency.used_slots == 0
        and pool.leases("r0") == [],
        10.0,
    ), f"shared state not quiesced: {pool.stats()}"
