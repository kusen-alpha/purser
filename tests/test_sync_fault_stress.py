"""Sync facade: injected backend faults, long-run contention and heartbeat.

These tests exercise the thread bridge rather than the async kernel alone:
every fault is raised on the dedicated loop and observed from caller threads.
They use fakeredis unless ``PURSER_TEST_REDIS_URL`` points at a dedicated DB.
"""

from __future__ import annotations

import asyncio
import os
import threading
import time

import fakeredis
import pytest
from conftest import FakeChecker
from redis.exceptions import RedisError

from purser import (
    AcquireRequest,
    BackendUnavailable,
    BadState,
    LeaseConflict,
    PoolExhausted,
    SyncLease,
    SyncPool,
    SyncPurser,
)

REAL_REDIS_URL = os.environ.get("PURSER_TEST_REDIS_URL")


class FaultyRedis(fakeredis.aioredis.FakeRedis):
    """FakeRedis with switchable write/removal failures."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.fail_writes = False
        self.fail_eval = False

    async def hset(self, name, key=None, value=None, mapping=None, items=None):
        if self.fail_writes:
            raise RedisError("injected coordination write failure")
        return await super().hset(
            name, key=key, value=value, mapping=mapping, items=items
        )

    async def eval(self, *args, **kwargs):
        if self.fail_eval:
            raise RedisError("injected coordination removal failure")
        return await super().eval(*args, **kwargs)


class FaultyStorage:
    """Delegating storage proxy with an injectable release failure."""

    def __init__(self, inner):
        self._inner = inner
        self.fail_release = False

    def __getattr__(self, name):
        return getattr(self._inner, name)

    async def transition_release(self, *args, **kwargs):
        if self.fail_release:
            raise BackendUnavailable("injected storage release failure")
        return await self._inner.transition_release(*args, **kwargs)


def _sync_ps(tmp_path, *, redis_client=None):
    kwargs = {
        "maintenance_enabled": False,
        "storage": "sqlite",
        "coordination": "redis",
        "sqlite_path": str(tmp_path / "faults.db"),
    }
    if redis_client is None:
        if REAL_REDIS_URL:
            kwargs["redis_url"] = REAL_REDIS_URL
        else:
            redis_client = fakeredis.aioredis.FakeRedis()
            kwargs["redis_client"] = redis_client
    else:
        kwargs["redis_client"] = redis_client
    ps = SyncPurser(**kwargs)
    ps.register_type(FakeChecker())
    ps.start()
    ps._test_client = redis_client  # type: ignore[attr-defined]
    return ps


def _close(ps: SyncPurser) -> None:
    client = getattr(ps, "_test_client", None)
    ps.close()
    if client is not None:
        asyncio.run(client.aclose())


def _seed(pool: SyncPool, n: int = 1) -> list[str]:
    records = [
        {"id": f"r{i}", "host": "127.0.0.1", "port": 9100 + i}
        for i in range(n)
    ]
    report = pool.import_resources(records, import_validate="on_import")
    assert report.imported == n
    return [record["id"] for record in records]


def test_coordination_write_failure_rolls_back_and_recovers(tmp_path):
    client = FaultyRedis()
    ps = _sync_ps(tmp_path, redis_client=client)
    try:
        pool = ps.create_pool("fault-write", "fakecheck")
        [rid] = _seed(pool)

        client.fail_writes = True
        started = time.monotonic()
        with pytest.raises((PoolExhausted, BackendUnavailable)):
            pool.acquire(AcquireRequest(consumer="c", timeout=0.2))
        assert time.monotonic() - started < 5

        # The storage slot acquired before register_lease failed is returned.
        res = pool.get(rid)
        assert res is not None and res.concurrency.used_slots == 0
        assert pool.leases() == []

        client.fail_writes = False
        lease = pool.acquire(AcquireRequest(consumer="c", timeout=1.0))
        assert pool.get(rid).concurrency.used_slots == 1
        assert pool.release(lease) is True
        assert pool.get(rid).concurrency.used_slots == 0
    finally:
        _close(ps)


def test_coordination_removal_failure_keeps_lease_releasable(tmp_path):
    client = FaultyRedis()
    ps = _sync_ps(tmp_path, redis_client=client)
    try:
        pool = ps.create_pool("fault-remove", "fakecheck")
        [rid] = _seed(pool)
        lease = pool.acquire(AcquireRequest(consumer="c"))

        client.fail_eval = True
        with pytest.raises(BackendUnavailable):
            pool.release(lease)
        # Nothing was removed: the lease is still live and the slot still used.
        res = pool.get(rid)
        assert res is not None and res.concurrency.used_slots == 1
        assert [live.token for live in pool.leases()] == [lease.token]
        assert not lease.released

        client.fail_eval = False
        assert pool.release(lease) is True
        assert lease.released
        assert pool.get(rid).concurrency.used_slots == 0
        assert pool.leases() == []
    finally:
        _close(ps)


def test_storage_release_failure_keeps_token_for_retry(tmp_path):
    ps = _sync_ps(tmp_path)
    try:
        pool = ps.create_pool("fault-storage", "fakecheck")
        [rid] = _seed(pool)
        lease = pool.acquire(AcquireRequest(consumer="c"))

        def install() -> FaultyStorage:
            proxy = FaultyStorage(pool._pool.storage)
            proxy.fail_release = True
            pool._pool.storage = proxy
            return proxy

        proxy = ps._call_sync(install)
        with pytest.raises(BackendUnavailable):
            pool.release(lease)
        # The compensation path returns the token because the slot was not
        # actually freed; a retry must be able to release it cleanly.
        assert pool.get(rid).concurrency.used_slots == 1
        assert [live.token for live in pool.leases()] == [lease.token]
        assert not lease.released

        proxy.fail_release = False
        assert pool.release(lease) is True
        assert pool.get(rid).concurrency.used_slots == 0
        assert pool.leases() == []
    finally:
        _close(ps)


def test_force_revoke_release_races_leave_no_ghost(tmp_path):
    ps = _sync_ps(tmp_path)
    try:
        pool = ps.create_pool("revoke-race", "fakecheck", max_slots=1)
        [rid] = _seed(pool)

        for i in range(60):
            lease = pool.acquire(AcquireRequest(consumer=f"c{i}", timeout=2.0))
            barrier = threading.Barrier(2)
            outcomes: dict[str, str] = {}

            def do_release(
                lease=lease,
                barrier=barrier,
                outcomes=outcomes,
            ) -> None:
                barrier.wait(timeout=5)
                try:
                    pool.release(lease)
                    outcomes["release"] = "ok"
                except LeaseConflict:
                    outcomes["release"] = "conflict"

            def do_revoke(
                barrier=barrier,
                outcomes=outcomes,
            ) -> None:
                barrier.wait(timeout=5)
                try:
                    pool.force_revoke(rid, operator="ops", reason="race")
                    outcomes["revoke"] = "ok"
                except (LeaseConflict, BadState):
                    outcomes["revoke"] = "lost"

            threads = [
                threading.Thread(target=do_release),
                threading.Thread(target=do_revoke),
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=10)
                assert not thread.is_alive()

            assert "ok" in outcomes.values(), outcomes
            res = pool.get(rid)
            assert res is not None and res.concurrency.used_slots == 0, outcomes
            assert pool.leases() == [], outcomes
    finally:
        _close(ps)


def test_multithread_stress_no_oversell_and_no_leak(tmp_path):
    ps = _sync_ps(tmp_path)
    try:
        pool = ps.create_pool("stress", "fakecheck", max_slots=2)
        _seed(pool, 3)
        workers, iterations = 8, 40
        errors: list[BaseException] = []
        successes = [0] * workers
        max_live = [0]
        lock = threading.Lock()
        barrier = threading.Barrier(workers)

        def worker(index: int) -> None:
            try:
                barrier.wait(timeout=20)
                for _ in range(iterations):
                    lease = pool.acquire(
                        AcquireRequest(consumer=f"w{index}", timeout=10.0)
                    )
                    assert isinstance(lease, SyncLease)
                    lease.used = 1
                    with lock:
                        live = len(pool.leases())
                        max_live[0] = max(max_live[0], live)
                    assert pool.release(lease) is True
                    successes[index] += 1
            except BaseException as exc:  # surfaced below
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(workers)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=120)

        assert all(not thread.is_alive() for thread in threads)
        assert not errors, errors
        assert sum(successes) == workers * iterations
        assert max_live[0] <= 6
        assert pool.leases() == []
        assert pool.counts(by_name=True).get("idle") == 3
        for resource in pool.query():
            assert resource.concurrency.used_slots == 0
    finally:
        _close(ps)


def test_heartbeat_keeps_sync_lease_alive_across_threads(tmp_path):
    ps = _sync_ps(tmp_path)
    try:
        pool = ps.create_pool(
            "heartbeat",
            "fakecheck",
            lease_ttl=0.6,
            heartbeat_interval=0.2,
        )
        [rid] = _seed(pool)
        lease = pool.acquire(AcquireRequest(consumer="long"))
        first_expiry = lease.expires_at
        time.sleep(1.5)

        assert not lease.expired(), "heartbeat did not renew the lease"
        assert lease.expires_at >= first_expiry
        assert lease.heartbeat_at is not None
        res = pool.get(rid)
        assert res is not None and res.concurrency.used_slots == 1
        assert [live.token for live in pool.leases()] == [lease.token]
        assert pool.release(lease) is True
        assert pool.get(rid).concurrency.used_slots == 0
    finally:
        _close(ps)
