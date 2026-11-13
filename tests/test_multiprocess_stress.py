"""Real multi-process contention on one SQLite file (CAS must not oversell).

Every worker process opens its own connection to the same shared file and uses
its own in-memory coordination.  The storage-plane version CAS is what must
keep ``used_slots <= max_slots``; any oversell or unexpected error is a bug.
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
import threading
import time

from purser import Purser
from purser.adapters.base import ResourceAdapter
from purser.core.health import HealthReport
from purser.core.resource import AcquireRequest, Resource
from purser.errors import PurserError


class _WorkerAdapter(ResourceAdapter):
    kind = "w"

    async def generate(self, spec: dict) -> Resource:
        return Resource(id=spec["id"], kind=self.kind, payload={"host": "x", "port": 1})

    async def validate(self, resource: Resource) -> HealthReport:
        return HealthReport.ok()


def _worker(sqlite_path: str, worker_id: int, attempts: int) -> dict:
    """Runs in a separate OS process; returns a picklable outcome dict."""

    async def run() -> dict:
        ok = 0
        surprises: list[str] = []
        ps = Purser(
            maintenance_enabled=False,
            storage="sqlite",
            coordination="memory",
            sqlite_path=sqlite_path,
        )
        ps.register_adapter(_WorkerAdapter())
        pool = ps.create_pool("shared", "w", max_slots=2)
        held: list = []
        try:
            for _ in range(attempts):
                try:
                    lease = await pool.acquire(
                        AcquireRequest(consumer=f"w{worker_id}", slots=1, timeout=0.05)
                    )
                    ok += 1
                    held.append(lease)
                    if len(held) >= 2 or len(held) % 3 == 0:  # occasionally hold two
                        await pool.release(held.pop(0))
                except (PurserError, asyncio.TimeoutError):
                    pass  # contention timeouts are expected
                except Exception as exc:  # pragma: no cover - bug if it happens
                    surprises.append(f"{type(exc).__name__}: {exc}")
            for lease in list(held):
                try:
                    await pool.release(lease)
                except Exception as exc:  # pragma: no cover
                    surprises.append(f"release: {type(exc).__name__}: {exc}")
        finally:
            await ps.close()
        return {"pid": os.getpid(), "ok": ok, "surprises": surprises}

    return asyncio.run(run())


def test_multiprocess_shared_sqlite_no_oversell(tmp_path):
    import multiprocessing as mp

    path = str(tmp_path / "shared.db")

    async def seed_once() -> None:
        ps = Purser(maintenance_enabled=False, storage="sqlite", coordination="memory", sqlite_path=path)
        ps.register_adapter(_WorkerAdapter())
        pool = ps.create_pool("shared", "w", max_slots=2)
        await pool.import_resources(
            [{"id": "r0", "host": "1.1.1.1", "port": 1}], import_validate="on_import"
        )
        await ps.close()

    asyncio.run(seed_once())

    ctx = mp.get_context("spawn")
    queue = ctx.Queue()
    processes = [
        ctx.Process(target=_worker_runner, args=(queue, path, i, 40)) for i in range(3)
    ]

    max_seen = [0]
    stop = [False]
    conn = sqlite3.connect(path)

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
        for proc in processes:
            proc.start()
        for proc in processes:
            proc.join(timeout=120)
    finally:
        stop[0] = True
        sampler.join(timeout=2)
        conn.close()

    outcomes = [queue.get(timeout=10) for _ in processes]
    for outcome in outcomes:
        assert outcome["surprises"] == [], outcome
    assert max_seen[0] <= 2, f"oversell observed: {max_seen[0]}"
    conn = sqlite3.connect(path)
    try:
        final = conn.execute(
            "SELECT used_slots FROM resources WHERE pool = 'shared' AND id = 'r0'"
        ).fetchone()[0]
    finally:
        conn.close()
    assert final == 0, f"slots leaked across processes: {final}"


def _worker_runner(queue, sqlite_path: str, worker_id: int, attempts: int) -> None:
    queue.put(_worker(sqlite_path, worker_id, attempts))
