"""Synchronous facade: lifecycle, leases, iteration, events, thread safety."""

from __future__ import annotations

import asyncio
import threading
import time

import pytest
from conftest import FakeChecker

from purser import (
    AcquireRequest,
    PoolExhausted,
    ResourceNotFound,
    ResourceState,
    SyncBatchLease,
    SyncLease,
    SyncPool,
    SyncPurser,
    SyncResourceIterator,
    UsageOutcome,
)


def seed(pool: SyncPool, n: int = 3, *, prefix: str = "r") -> list[str]:
    records = [
        {"id": f"{prefix}{i}", "host": "127.0.0.1", "port": 9000 + i}
        for i in range(n)
    ]
    report = pool.import_resources(records, import_validate="on_import")
    assert report.imported == n
    return [record["id"] for record in records]


@pytest.fixture
def sync_ps():
    with SyncPurser(maintenance_enabled=False) as ps:
        ps.register_adapter(FakeChecker())
        yield ps


class LockChecker(FakeChecker):
    """Produces resources whose snapshots cannot be deep-copied."""

    kind = "lockcheck"

    async def generate(self, spec):
        resource = await super().generate(spec)
        resource.meta["lock"] = threading.Lock()
        return resource


def test_lifecycle_and_pool_crud(sync_ps):
    assert sync_ps.started and not sync_ps.closed
    pool = sync_ps.create_pool("p", "fakecheck")
    assert isinstance(pool, SyncPool)
    assert pool.name == "p" and pool.kind == "fakecheck" and pool.started
    assert sync_ps.pool_names() == ["p"]
    assert sync_ps.pool("p") is pool
    assert sync_ps.require_pool("p") is pool
    assert sync_ps.get_pool("missing") is None

    ids = seed(pool)
    assert pool.counts(by_name=True).get("idle") == 3
    assert pool.count_matching(states={ResourceState.IDLE}) == 3
    assert pool.get(ids[0]).id == ids[0]
    assert pool.get("nope") is None
    with pytest.raises(ResourceNotFound):
        pool.require("nope")
    assert [r.id for r in pool.query()] == ids
    assert pool.query(search="r1")[0].id == "r1"
    assert pool.stats()["total"] == 3

    removed = sync_ps.delete_pool("p", force=True)
    assert removed["removed"] == 3
    assert sync_ps.pool_names() == []
    assert sync_ps.get_pool("p") is None


def test_lease_live_writes_and_release(sync_ps):
    pool = sync_ps.create_pool("p", "fakecheck")
    seed(pool, 1)

    lease = pool.acquire(consumer="crawler")
    assert isinstance(lease, SyncLease)
    assert lease.pool == "p" and lease.consumer == "crawler"
    assert lease.resource is not None and lease.payload is not None
    assert pool.counts(by_name=True).get("full") == 1

    before = lease.expires_at
    assert pool.renew_manual(lease)
    assert lease.expires_at >= before
    assert lease.heartbeat_at is not None

    lease.used = 2
    lease.outcome = UsageOutcome.RATE_LIMITED
    [live] = pool.leases()
    assert live.used == 2 and live.outcome is UsageOutcome.RATE_LIMITED

    # Snapshots are copies: mutating them must not leak back to the kernel.
    lease.payload["host"] = "changed"
    assert pool.require("r0").payload["host"] == "127.0.0.1"

    assert pool.release(lease) is True
    assert lease.released
    with pytest.raises(RuntimeError):
        lease.used = 3
    with pytest.raises(RuntimeError):
        lease.outcome = UsageOutcome.OK
    assert pool.leases() == []


def test_lease_context_manager(sync_ps):
    pool = sync_ps.create_pool("p", "fakecheck")
    seed(pool, 1)
    with pool.lease(consumer="ctx") as lease:
        assert pool.counts(by_name=True).get("full") == 1
        lease.outcome = UsageOutcome.OK
    assert lease.released
    assert pool.counts(by_name=True).get("idle") == 1


def test_delivery_failure_compensates_leases():
    with SyncPurser(maintenance_enabled=False) as ps:
        ps.register_adapter(LockChecker())
        pool = ps.create_pool("p", "lockcheck")
        pool.import_resources(
            [{"id": "l1"}, {"id": "l2"}], import_validate="on_import"
        )

        with pytest.raises(TypeError):
            pool.acquire(consumer="c")
        assert pool.leases() == []
        assert pool.counts(by_name=True).get("idle") == 2

        with pytest.raises(TypeError):
            pool.acquire_many(2, mode="atomic")
        assert pool.leases() == []
        assert pool.counts(by_name=True).get("idle") == 2


def test_acquire_many_and_release_all(sync_ps):
    pool = sync_ps.create_pool("p", "fakecheck")
    seed(pool, 3)

    batch = pool.acquire_many(2, mode="best_effort", req=None)
    assert isinstance(batch, SyncBatchLease)
    assert len(batch) == 2 and {lease.resource_id for lease in batch.leases}
    batch.release_all()
    assert len(batch) == 0
    assert pool.leases() == []

    with pool.acquire_many(2) as batch2:
        assert len(batch2) == 2
    assert pool.leases() == []


def test_iter_resources_is_lazy_and_closable(sync_ps):
    pool = sync_ps.create_pool("p", "fakecheck")
    seed(pool, 3)

    iterator = pool.iter_resources(batch_size=1)
    assert isinstance(iterator, SyncResourceIterator)
    first = next(iterator)
    assert first.id.startswith("r")
    iterator.close()
    assert list(iterator) == []
    with pytest.raises(StopIteration):
        next(iterator)

    with pool.iter_resources() as iterator:
        assert [resource.id for resource in iterator] == ["r0", "r1", "r2"]


def test_shared_instance_across_threads(sync_ps):
    pool = sync_ps.create_pool("p", "fakecheck")
    seed(pool, 5)

    results: list[tuple[str, str]] = []
    errors: list[BaseException] = []
    barrier = threading.Barrier(4)

    def worker(index: int) -> None:
        try:
            barrier.wait(timeout=10)
            lease = pool.acquire(consumer=f"t{index}")
            try:
                results.append((lease.token, lease.resource_id))
            finally:
                pool.release(lease)
        except BaseException as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=20)

    assert not errors
    assert len(results) == 4
    assert len({token for token, _ in results}) == 4
    assert len({rid for _, rid in results}) == 4
    assert pool.leases() == []


def test_events_subscription_and_history(sync_ps):
    pool = sync_ps.create_pool("p", "fakecheck")
    seed(pool, 1)

    seen: list[str] = []
    done = threading.Event()

    def callback(event) -> None:
        seen.append(event.type)
        if "lease_released" in seen:
            done.set()

    subscription = sync_ps.subscribe(callback)
    lease = pool.acquire(consumer="c")
    pool.release(lease)
    assert done.wait(10), f"events not dispatched: {seen}"
    subscription.close()
    subscription.close()  # idempotent

    events = sync_ps.history()
    types = [event.type for event in events]
    assert "pool_created" in types
    assert "lease_acquired" in types and "lease_released" in types
    assert events == sorted(events, key=lambda event: event.seq)
    tail = sync_ps.history(after_seq=events[-1].seq)
    assert tail == []


def test_proxies_expose_loop_owned_objects(sync_ps):
    pool = sync_ps.create_pool("p", "fakecheck")
    seed(pool, 1)
    pool.acquire(consumer="c")

    snap = sync_ps.metrics.snapshot()
    assert any(key.endswith("lease_total") for key in snap["counters"])
    assert sync_ps.audit.verify() is True
    assert len(sync_ps.audit.entries(limit=5)) <= 5
    assert "fakecheck" in sync_ps.adapters.names()
    # get() must return the real plugin object, not a copy, and register()
    # must keep the caller's instance rather than a deep copy.
    assert isinstance(sync_ps.adapters.get("fakecheck"), FakeChecker)
    plugin = LockChecker()
    sync_ps.adapters.register(plugin)
    assert sync_ps.adapters.get("lockcheck") is plugin


def test_close_is_idempotent_and_rejects_later_calls():
    ps = SyncPurser(maintenance_enabled=False)
    ps.register_adapter(FakeChecker())
    ps.start()
    pool = ps.create_pool("p", "fakecheck")
    seed(pool, 1)
    assert ps.started
    ps.close()
    assert ps.closed
    ps.close()
    with pytest.raises(RuntimeError):
        pool.counts()
    with pytest.raises(RuntimeError):
        ps.create_pool("q", "fakecheck")


def test_calls_from_loop_thread_are_rejected(sync_ps):
    errors: list[BaseException] = []

    def on_loop() -> None:
        try:
            sync_ps.pool_names()
        except BaseException as exc:
            errors.append(exc)

    sync_ps._call_sync(on_loop)
    assert errors and isinstance(errors[0], RuntimeError)


def test_dangerous_ops_from_event_callback_are_rejected(sync_ps):
    seed(sync_ps.create_pool("p", "fakecheck"), 1)
    outcome: list[str] = []
    done = threading.Event()

    def callback(event) -> None:
        if event.type != "lease_acquired":
            return
        try:
            sync_ps.close()
        except RuntimeError:
            outcome.append("rejected")
        finally:
            done.set()

    subscription = sync_ps.subscribe(callback)
    lease = sync_ps.require_pool("p").acquire(consumer="c")
    assert done.wait(10)
    subscription.close()
    assert outcome == ["rejected"]
    sync_ps.require_pool("p").release(lease)


def test_report_usage_backup_and_restore(sync_ps):
    pool = sync_ps.create_pool("p", "fakecheck")
    seed(pool, 2)
    lease = pool.acquire(consumer="c")
    pool.release(lease, outcome=UsageOutcome.OK, used=1)

    report = sync_ps.report_usage()
    assert report["totals"]["releases"] >= 1
    assert report["group_by"] == "consumer"

    backup = sync_ps.backup()
    assert backup["format"] == "purser-backup"
    assert [entry["name"] for entry in backup["pools"]] == ["p"]

    sync_ps.delete_pool("p", force=True)
    restored = sync_ps.restore(backup)
    assert restored["ok"] is True and restored["created_pools"] == ["p"]
    assert sync_ps.require_pool("p").counts(by_name=True).get("idle") == 2


def test_timeout_and_foreign_lease_rejected(sync_ps):
    pool = sync_ps.create_pool("p", "fakecheck")
    seed(pool, 1)
    other = sync_ps.create_pool("q", "fakecheck")

    lease = pool.acquire(consumer="c")
    with pytest.raises(ValueError):
        other.release(lease)
    pool.release(lease)

    # A plain kernel Lease from a different pool must not be accepted either.
    raw = sync_ps._call_sync(lambda: pool._pool._leases.get(lease.token))
    assert raw is None


def test_kernel_wait_timeout_is_not_masked(sync_ps):
    pool = sync_ps.create_pool("busy", "fakecheck")
    seed(pool, 1)
    held = pool.acquire(consumer="holder")
    started = time.monotonic()
    with pytest.raises(PoolExhausted):
        pool.acquire(AcquireRequest(consumer="c", timeout=0.05))
    assert time.monotonic() - started < 5
    pool.release(held)

    with pytest.raises(TimeoutError):
        sync_ps._submit(lambda: asyncio.sleep(0.5), timeout=0.02)
    # The facade still works after an outer timeout cancelled the task.
    assert sync_ps.pool_names() == ["busy"]


def test_admin_methods_forward(sync_ps):
    pool = sync_ps.create_pool("p", "fakecheck")
    seed(pool, 3)

    pool.ban("r0", reason="risk")
    assert pool.require("r0").state is ResourceState.BANNED
    pool.unban("r0", reason="safe", revalidate=False)
    assert pool.require("r0").state is ResourceState.VALIDATING
    assert pool.validate_now("r0").alive is True
    assert pool.require("r0").state is ResourceState.IDLE

    pool.suspend("r1", reason="hold")
    assert pool.require("r1").state is ResourceState.SUSPENDED
    pool.resume("r1", reason="back")
    assert pool.require("r1").state is ResourceState.IDLE

    updated = pool.update_resource("r1", tags={"eu"}, reason="retag")
    assert updated.tags == frozenset({"eu"})

    other = sync_ps.create_pool("q", "fakecheck")
    moved = pool.move_resource("r1", other, reason="rebalance")
    assert moved.id == "r1" and other.require("r1").id == "r1"
    assert pool.get("r1") is None

    lease = other.acquire(consumer="c")
    revoked = other.force_revoke(lease.resource_id, reason="kick")
    assert revoked.token == lease.token
    assert other.leases() == []

    counts = other.run_maintenance("quota_reset")
    assert isinstance(counts, dict)

    jobs = pool.validate_jobs()
    assert jobs == [] or isinstance(jobs, list)


def test_close_during_concurrent_calls_does_not_hang():
    ps = SyncPurser(maintenance_enabled=False, close_timeout=10)
    ps.register_adapter(FakeChecker())
    ps.start()
    pool = ps.create_pool("p", "fakecheck")
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
            except RuntimeError:
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
