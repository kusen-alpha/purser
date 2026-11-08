"""Sync facade single-instance benchmark.

Runs the same acquire/release workload through ``SyncPurser`` for every
available storage x coordination pair and prints throughput plus per-call
latency percentiles.  External services are opt-in via the same environment
variables used by the test suite:

    PURSER_TEST_REDIS_URL=redis://host:6379/15
    PURSER_TEST_MYSQL_URL=mysql://root:@host:3306/purser_bench

Usage:
    python benchmarks/sync_facade_bench.py --threads 8 --ops 200
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import tempfile
import threading
import time
import uuid
from pathlib import Path

from purser import AcquireRequest, SyncPurser
from purser.core.health import HealthReport
from purser.core.resource import Resource
from purser.types.base import ResourceType


class BenchResource(ResourceType):
    kind = "bench"
    default_concurrency = 4

    async def generate(self, spec):
        return Resource(
            id=spec["id"],
            kind=self.kind,
            payload={"host": spec["host"], "port": spec["port"]},
        )

    async def validate(self, resource):
        return HealthReport.ok(latency_ms=0.0)


def _percentile(samples: list[float], q: float) -> float:
    ordered = sorted(samples)
    if not ordered:
        return 0.0
    index = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * q)))
    return ordered[index]


def run_case(
    storage: str,
    coordination: str,
    *,
    threads: int,
    ops: int,
    resources: int,
    max_slots: int,
    warmup: int,
    redis_url: str | None,
    mysql_url: str | None,
) -> dict:
    name = f"bench-{uuid.uuid4().hex[:10]}"
    tmp = tempfile.TemporaryDirectory(prefix="purser-bench-")
    kwargs: dict = {
        "maintenance_enabled": False,
        "storage": storage,
        "coordination": coordination,
        "startup_timeout": 60.0,
        "close_timeout": 60.0,
    }
    if storage == "sqlite":
        kwargs["sqlite_path"] = str(Path(tmp.name) / "bench.db")
    elif storage == "mysql":
        kwargs["mysql_url"] = mysql_url
    if coordination == "redis":
        kwargs["redis_url"] = redis_url

    ps = SyncPurser(**kwargs)
    ps.register_type(BenchResource())
    ps.start()
    try:
        pool = ps.create_pool(name, "bench", max_slots=max_slots)
        pool.import_resources(
            [
                {"id": f"r{i}", "host": "127.0.0.1", "port": 9200 + i}
                for i in range(resources)
            ],
            import_validate="on_import",
        )
        for _ in range(warmup):
            lease = pool.acquire(AcquireRequest(consumer="warm", timeout=30.0))
            pool.release(lease)

        samples: list[float] = []
        samples_lock = threading.Lock()
        errors: list[BaseException] = []
        barrier = threading.Barrier(threads)
        started = time.perf_counter()

        def worker(index: int) -> None:
            local: list[float] = []
            try:
                barrier.wait(timeout=60)
                for _ in range(ops):
                    begin = time.perf_counter()
                    lease = pool.acquire(
                        AcquireRequest(consumer=f"t{index}", timeout=60.0)
                    )
                    pool.release(lease)
                    local.append(time.perf_counter() - begin)
            except BaseException as exc:  # surfaced below
                errors.append(exc)
            finally:
                with samples_lock:
                    samples.extend(local)

        workers = [
            threading.Thread(target=worker, args=(i,), name=f"bench-{i}")
            for i in range(threads)
        ]
        for thread in workers:
            thread.start()
        for thread in workers:
            thread.join(timeout=600)
        elapsed = time.perf_counter() - started
        alive = [thread.name for thread in workers if thread.is_alive()]
        if errors or alive:
            raise RuntimeError(f"benchmark failed: errors={errors} alive={alive}")

        total = threads * ops
        return {
            "storage": storage,
            "coordination": coordination,
            "threads": threads,
            "ops_per_thread": ops,
            "resources": resources,
            "max_slots": max_slots,
            "total_ops": total,
            "elapsed_s": round(elapsed, 3),
            "throughput_ops_s": round(total / elapsed, 1),
            "latency_ms": {
                "p50": round(_percentile(samples, 0.50) * 1000, 3),
                "p95": round(_percentile(samples, 0.95) * 1000, 3),
                "p99": round(_percentile(samples, 0.99) * 1000, 3),
                "max": round(max(samples) * 1000, 3),
                "mean": round(statistics.fmean(samples) * 1000, 3),
            },
        }
    finally:
        try:
            ps.delete_pool(name, force=True)
        finally:
            ps.close()
            tmp.cleanup()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--ops", type=int, default=200)
    parser.add_argument("--resources", type=int, default=4)
    parser.add_argument("--max-slots", type=int, default=4)
    parser.add_argument("--warmup", type=int, default=10)
    parser.add_argument("--json", nargs="?", const="-", default=None)
    args = parser.parse_args()

    redis_url = os.environ.get("PURSER_TEST_REDIS_URL")
    mysql_url = os.environ.get("PURSER_TEST_MYSQL_URL")
    cases = [("memory", "memory"), ("sqlite", "memory")]
    if redis_url:
        cases += [("memory", "redis"), ("sqlite", "redis")]
    if mysql_url:
        cases += [("mysql", "memory"), ("mysql", "redis")]

    rows = []
    for storage, coordination in cases:
        row = run_case(
            storage,
            coordination,
            threads=args.threads,
            ops=args.ops,
            resources=args.resources,
            max_slots=args.max_slots,
            warmup=args.warmup,
            redis_url=redis_url,
            mysql_url=mysql_url,
        )
        rows.append(row)
        latency = row["latency_ms"]
        print(
            f"{storage:7} {coordination:7} "
            f"{row['throughput_ops_s']:9.1f} ops/s  "
            f"p50={latency['p50']:7.2f}ms p95={latency['p95']:7.2f}ms "
            f"p99={latency['p99']:7.2f}ms max={latency['max']:8.2f}ms"
        )

    if args.json:
        payload = json.dumps(rows, indent=2)
        if args.json == "-":
            print(payload)
        else:
            Path(args.json).write_text(payload, encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
