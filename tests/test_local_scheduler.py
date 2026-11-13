"""Local IPC scheduler acceptance for same-host multiprocessing.

The scheduler process owns the only ``SyncPurser``.  Workers connect through
``multiprocessing.connection`` (named pipe on Windows, UDS on POSIX); no worker
creates a second memory authority.  Real Redis/MySQL cases only run when the
usual acceptance env vars are set and are strictly sequential.
"""

from __future__ import annotations

import contextlib
import multiprocessing as mp
import os
import threading
import time
import uuid
from concurrent.futures import ProcessPoolExecutor

import pytest
from conftest import FakeChecker

from purser import PoolExhausted, PoolNotFound
from purser.local import (
    ENV_VAR,
    LocalScheduler,
    SchedulerAddress,
    SchedulerUnavailable,
    close_client,
    get_client,
    init_worker,
)
from purser.local.protocol import decode_message, encode_message

REAL_REDIS_URL = os.environ.get("PURSER_TEST_REDIS_URL")
MYSQL_URL = os.environ.get("PURSER_TEST_MYSQL_URL")


def _scheduler(**kwargs) -> LocalScheduler:
    return LocalScheduler(
        adapters=[FakeChecker()], maintenance_enabled=False, **kwargs
    )


def _seed(sched: LocalScheduler, name: str = "p", count: int = 1) -> list[str]:
    prefix = uuid.uuid4().hex[:6]
    sched.create_pool(name, "fakecheck", config={"max_slots": 1})
    records = [
        {"id": f"{prefix}-{index}", "host": "127.0.0.1", "port": 9000 + index}
        for index in range(count)
    ]
    report = sched.import_resources(name, records, import_validate="on_import")
    assert report["imported"] == count
    return [record["id"] for record in records]


# -- worker entry points (module level: spawn-safe) -----------------------


def _worker_with_address(address: SchedulerAddress, pool: str, consumer: str) -> dict:
    client = init_worker(address)
    try:
        with client.lease(pool, consumer=consumer, timeout=10.0) as lease:
            return {
                "pid": os.getpid(),
                "token": lease.token,
                "resource_id": lease.resource_id,
                "expired": lease.expired(),
            }
    finally:
        close_client()


def _worker_from_env(pool: str, consumer: str, timeout: float) -> dict:
    client = get_client()
    with client.lease(pool, consumer=consumer, timeout=timeout) as lease:
        return {"pid": os.getpid(), "token": lease.token}


def _crash_worker(address: SchedulerAddress, pool: str, marker: str) -> None:
    client = init_worker(address)
    lease = client.acquire(pool, consumer="crash", timeout=10.0)
    with open(marker, "w", encoding="utf-8") as fh:
        fh.write(lease.token)
    os._exit(0)


def test_protocol_bytes_roundtrip():
    message = {"blob": b"\x00\xff", "nested": [{"value": b"x"}]}
    decoded = decode_message(encode_message(message))
    assert decoded == message
    assert isinstance(decoded["blob"], bytes)
    assert decode_message(encode_message({"__bytes__": "literal"})) == {
        "__bytes__": "literal"
    }


# -- in-process data plane ------------------------------------------------


def test_in_process_data_plane_and_idempotency():
    with _scheduler() as sched:
        health = sched.health()
        assert health["protocol"] == 1
        assert health["storage"] == "memory"
        assert health["coordination"] == "memory"
        assert health["pools"] == []

        [rid] = _seed(sched)
        assert sched.pool_names() == ["p"]

        lease = sched.acquire("p", consumer="c1", idempotency_key="k1")
        assert lease.resource_id == rid and not lease.replayed
        replay = sched.acquire("p", consumer="c1", idempotency_key="k1")
        assert replay.token == lease.token and replay.replayed

        with pytest.raises(PoolExhausted):
            sched.acquire("p", consumer="c2", timeout=0.15)

        started = time.monotonic()
        assert lease.release() is True
        assert lease.release() is True
        assert time.monotonic() - started < 5

        # Server-side idempotent release: a token that is already gone is a
        # success, matching the in-process facade contract.
        assert sched.client.release("p", lease.token) is True
        assert sched.client.release("p", lease.token) is True

        with sched.lease("p", consumer="c3") as second:
            assert second.resource_id == rid
            assert second.renew() is True
            assert second.expires_at >= lease.started_at
        assert sched.stats("p")["active_leases"] == 0

        with pytest.raises(PoolNotFound):
            sched.acquire("missing", consumer="c4")

        with pytest.raises(PoolNotFound):
            sched.stats("missing")

        assert sched.delete_pool("p", force=True)["removed"] == 1
        assert sched.pool_names() == []


def test_release_wakes_blocked_acquire():
    with _scheduler() as sched:
        _seed(sched)
        holder = sched.acquire("p", consumer="holder")
        result: dict = {}

        def waiter() -> None:
            started = time.monotonic()
            with sched.client.lease("p", consumer="waiter", timeout=10.0) as lease:
                result["resource_id"] = lease.resource_id
                result["waited"] = time.monotonic() - started

        thread = threading.Thread(target=waiter)
        thread.start()
        time.sleep(0.3)
        holder.release()
        thread.join(10)
        assert not thread.is_alive()
        assert result["resource_id"] == holder.resource_id
        assert result["waited"] >= 0.2


# -- same-host processes --------------------------------------------------


def test_multiprocessing_process_worker():
    with _scheduler() as sched:
        [rid] = _seed(sched)
        ctx = mp.get_context("spawn")
        proc = ctx.Process(
            target=_worker_with_address,
            args=(sched.address, "p", "proc"),
        )
        proc.start()
        proc.join(60)
        assert proc.exitcode == 0
        assert sched.stats("p")["active_leases"] == 0
        assert sched.stats("p")["counts"]["idle"] == 1
        # The worker's acquire/release must be visible to the scheduler.
        lease = sched.acquire("p", consumer="main")
        assert lease.resource_id == rid
        lease.release()


def test_pool_initializer_shares_capacity():
    with _scheduler() as sched:
        _seed(sched)
        holder = sched.acquire("p", consumer="main")
        ctx = mp.get_context("spawn")
        with ctx.Pool(1, initializer=init_worker, initargs=(sched.address,)) as pool:
            with pytest.raises(PoolExhausted):
                pool.apply(_worker_from_env, ("p", "pool", 0.3))
            holder.release()
            outcome = pool.apply(_worker_from_env, ("p", "pool", 10.0))
        assert outcome["pid"] != os.getpid()
        assert sched.stats("p")["active_leases"] == 0


def test_process_pool_executor_initializer():
    with _scheduler() as sched:
        _seed(sched)
        ctx = mp.get_context("spawn")
        with ProcessPoolExecutor(
            max_workers=2,
            mp_context=ctx,
            initializer=init_worker,
            initargs=(sched.address,),
        ) as executor:
            futures = [
                executor.submit(_worker_from_env, "p", f"exec-{index}", 10.0)
                for index in range(2)
            ]
            outcomes = [future.result(30) for future in futures]
        assert outcomes[0]["pid"] != outcomes[1]["pid"]
        assert sched.stats("p")["active_leases"] == 0


def test_env_discovery_for_spawned_children():
    previous = os.environ.get(ENV_VAR)
    with _scheduler() as sched:
        _seed(sched)
        ctx = mp.get_context("spawn")
        with sched.env():
            queue = ctx.Queue()
            proc = ctx.Process(target=_env_worker, args=(queue, "p", "env"))
            proc.start()
            proc.join(60)
            assert proc.exitcode == 0
            outcome = queue.get(timeout=5)
            queue.close()
        assert outcome["token"]
        assert os.environ.get(ENV_VAR) == previous


def _env_worker(queue, pool: str, consumer: str) -> None:
    client = get_client()
    try:
        with client.lease(pool, consumer=consumer, timeout=10.0) as lease:
            queue.put({"pid": os.getpid(), "token": lease.token})
    finally:
        close_client()


def test_worker_crash_releases_lease(tmp_path):
    with _scheduler() as sched:
        _seed(sched)
        marker = tmp_path / "crash.marker"
        ctx = mp.get_context("spawn")
        proc = ctx.Process(
            target=_crash_worker,
            args=(sched.address, "p", str(marker)),
        )
        proc.start()
        proc.join(60)
        assert proc.exitcode == 0
        assert marker.exists()
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if sched.stats("p")["active_leases"] == 0:
                break
            time.sleep(0.05)
        assert sched.stats("p")["active_leases"] == 0


def test_scheduler_death_surfaces_unavailable():
    sched = _scheduler().start()
    try:
        _seed(sched)
        assert sched.alive
        sched._process.terminate()
        sched._process.join(10)
        with pytest.raises(SchedulerUnavailable):
            sched.client.health()
    finally:
        sched.close()
    with pytest.raises(SchedulerUnavailable):
        sched.client.health()


# -- shared backends (real services, still one scheduler authority) --------


def _real_kwargs(tmp_path, backend: str) -> dict:
    if backend == "sqlite":
        return {
            "storage": "sqlite",
            "coordination": "redis",
            "sqlite_path": str(tmp_path / f"ipc-{uuid.uuid4().hex}.db"),
            "redis_url": REAL_REDIS_URL,
        }
    return {
        "storage": "mysql",
        "coordination": "redis",
        "mysql_url": MYSQL_URL,
        "redis_url": REAL_REDIS_URL,
    }


@pytest.mark.skipif(not REAL_REDIS_URL, reason="needs real Redis")
@pytest.mark.parametrize(
    "backend",
    [
        "sqlite",
        pytest.param(
            "mysql",
            marks=pytest.mark.skipif(not MYSQL_URL, reason="needs real MySQL"),
        ),
    ],
)
def test_shared_backend_scheduler_with_process_worker(backend, tmp_path):
    pool_name = f"ipc-{uuid.uuid4().hex[:8]}"
    sched = _scheduler(**_real_kwargs(tmp_path, backend)).start()
    try:
        _seed(sched, name=pool_name)
        ctx = mp.get_context("spawn")
        proc = ctx.Process(
            target=_worker_with_address,
            args=(sched.address, pool_name, "proc"),
        )
        proc.start()
        proc.join(120)
        assert proc.exitcode == 0
        assert sched.stats(pool_name)["active_leases"] == 0
        assert sched.acquire(pool_name, consumer="main").release() is True
    finally:
        with contextlib.suppress(Exception):
            sched.delete_pool(pool_name, force=True)
        sched.close()
