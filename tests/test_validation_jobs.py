"""Background validation loop and batch validate jobs.

Jobs are started by ``import_resources(import_validate="batch")``; the public
surface is ``pool.validate_jobs()`` / ``pool.validate_job()`` /
``pool.cancel_validate_job()``.
"""

from __future__ import annotations

import asyncio
import logging

from conftest import FakeChecker

from purser import HealthReport, Purser, Resource, ResourceAdapter

LOGGER = "purser"


class SlowChecker(FakeChecker):
    """Fake checker that yields to the loop so jobs stay observably running."""

    kind = "slowcheck"

    def __init__(self, *, delay: float = 0.1) -> None:
        super().__init__(delay=delay)


class DeadChecker(FakeChecker):
    kind = "deadcheck"

    def __init__(self) -> None:
        super().__init__(alive=False)


class CountingChecker(ResourceAdapter):
    kind = "counting"
    default_concurrency = 1

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def generate(self, spec: dict) -> Resource:
        return Resource(id=spec["id"], kind=self.kind, payload={}, meta={})

    async def validate(self, resource: Resource) -> HealthReport:
        self.calls.append(resource.id)
        return HealthReport.ok(latency_ms=1.0)


async def _wait_job(pool, job_id: str, *, timeout: float = 10.0):
    """Poll ``validate_job`` until the job reaches a terminal status."""

    async def poll():
        while True:
            view = pool.validate_job(job_id)
            if view is not None and view["status"] in ("completed", "cancelled"):
                return view
            await asyncio.sleep(0)

    return await asyncio.wait_for(poll(), timeout)


def _records(prefix: str, n: int) -> list[dict]:
    return [
        {"id": f"{prefix}-{i}", "host": "127.0.0.1", "port": 8000 + i}
        for i in range(n)
    ]


async def test_batch_import_job_completes(purser, make_pool):
    pool = make_pool("jobs", validate_concurrency=2)
    await pool.import_resources(
        _records("r", 3), import_validate="batch", batch_id="job-1"
    )

    view = await _wait_job(pool, "job-1")
    assert view["id"] == "job-1"
    assert view["status"] == "completed"
    assert (view["total"], view["done"], view["alive"], view["failed"]) == (3, 3, 3, 0)
    assert view["pending"] == 0 and view["errors"] == []
    assert view["operator"] == "system"
    assert [item["id"] for item in pool.validate_jobs()] == ["job-1"]
    assert pool.validate_job("unknown") is None


async def test_batch_job_counts_dead_resources(purser):
    purser.register_adapter(DeadChecker())
    pool = purser.ensure_pool("jobs-dead", "deadcheck")
    await pool.import_resources(
        _records("d", 2), import_validate="batch", batch_id="job-dead"
    )

    view = await _wait_job(pool, "job-dead")
    assert view["status"] == "completed"
    assert (view["alive"], view["failed"]) == (0, 2)


async def test_batch_job_records_error_for_banned_resources(purser, make_pool):
    pool = make_pool("jobs-err")
    await pool.import_resources(_records("b", 1), import_validate="on_import")
    await pool.ban("b-0", reason="risk")

    await pool.import_resources(
        _records("b", 1), import_validate="batch", batch_id="job-err"
    )
    view = await _wait_job(pool, "job-err")
    assert view["failed"] == 1
    assert view["errors"][0]["rid"] == "b-0"
    assert "banned" in view["errors"][0]["error"]
    # A raising probe is a failure, never an "alive" count.
    assert view["alive"] == 0


async def test_batch_job_truncates_error_log_to_100():
    ps = Purser(maintenance_enabled=False)
    ps.register_adapter(FakeChecker())
    await ps.start()
    pool = ps.create_pool("jobs-many", "fakecheck", validate_concurrency=16)
    try:
        records = _records("m", 101)
        await pool.import_resources(records, import_validate="on_import")
        for record in records:
            await pool.ban(record["id"], reason="risk")

        await pool.import_resources(
            records, import_validate="batch", batch_id="job-many"
        )
        view = await _wait_job(pool, "job-many", timeout=30.0)
        assert view["failed"] == 101
        assert len(view["errors"]) == 100  # oldest entries are dropped
    finally:
        await ps.close()


async def test_cancel_job_skips_pending_items(purser):
    purser.register_adapter(SlowChecker(delay=0.2))
    pool = purser.ensure_pool("jobs-cancel", "slowcheck", validate_concurrency=1)
    await pool.import_resources(
        _records("c", 4), import_validate="batch", batch_id="job-cancel"
    )

    first = pool.cancel_validate_job("job-cancel")
    assert first is not None and first["cancelled"] is True
    assert first["status"] in ("cancelling", "cancelled")
    assert first["skipped"] >= 1

    view = await _wait_job(pool, "job-cancel")
    assert view["status"] == "cancelled"
    assert view["done"] == view["total"] == 4
    assert view["pending"] == 0

    again = pool.cancel_validate_job("job-cancel")
    assert again is not None and again["cancelled"] is False
    assert pool.cancel_validate_job("no-such-job") is None


async def test_second_import_extends_a_running_job(purser):
    purser.register_adapter(SlowChecker(delay=0.2))
    pool = purser.ensure_pool("jobs-extend", "slowcheck", validate_concurrency=1)
    await pool.import_resources(
        _records("e", 2), import_validate="batch", batch_id="job-extend"
    )
    await pool.import_resources(
        [{"id": "e-2", "host": "127.0.0.1", "port": 9000}],
        import_validate="batch",
        batch_id="job-extend",
    )

    view = pool.validate_job("job-extend")
    assert view is not None
    assert view["total"] == 3  # the running job absorbed the new ids

    pool.cancel_validate_job("job-extend")
    done = await _wait_job(pool, "job-extend")
    assert done["status"] == "cancelled" and done["done"] == 3


async def test_worker_skips_items_that_arrive_after_cancellation(purser, make_pool):
    pool = make_pool("jobs-skip")
    service = pool._validation
    service.start_job("job-skip", [], "tester")
    service.cancel_job("job-skip")

    job = service._jobs.get("job-skip")
    queue = service._queues["job-skip"]
    assert job is not None and queue is not None
    queue.put_nowait("r-late")
    await service._worker("job-skip")

    # Whether the spawned worker or this direct call consumes the item, a
    # cancelled job only counts it as skipped.
    assert job["skipped"] == 1
    assert job["done"] == 1
    assert job["status"] == "cancelled"


async def test_background_stale_refreshes_due_resources(purser):
    checker = CountingChecker()
    purser.register_adapter(checker)
    pool = purser.ensure_pool("bg", "counting", stale_after=0.0)
    await pool.import_resources(_records("g", 3), import_validate="on_import")
    checker.calls.clear()

    checked = await pool._validation.background_stale()
    assert checked == 3
    assert sorted(checker.calls) == ["g-0", "g-1", "g-2"]


async def test_background_loop_runs_and_stops_on_cancel(purser):
    checker = CountingChecker()
    purser.register_adapter(checker)
    pool = purser.ensure_pool(
        "bg-loop", "counting", stale_after=0.0, background_interval=0.5
    )
    await pool.import_resources(_records("l", 2), import_validate="on_import")
    checker.calls.clear()

    task = asyncio.create_task(pool._validation.background_loop())
    try:

        async def poll():
            while len(checker.calls) < 2:
                await asyncio.sleep(0)
            return True

        await asyncio.wait_for(poll(), 10.0)
    finally:
        task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    else:  # pragma: no cover - cancellation must propagate
        raise AssertionError("background loop ignored cancellation")


async def test_background_validation_swallows_per_resource_errors(
    purser, monkeypatch, caplog
):
    pool = purser.ensure_pool("bg-err", "fakecheck", stale_after=0.0)
    await pool.import_resources(_records("x", 2), import_validate="on_import")
    service = pool._validation

    async def boom(rid: str) -> None:
        raise RuntimeError(f"cannot probe {rid}")

    monkeypatch.setattr(service, "validate_now", boom)
    with caplog.at_level(logging.ERROR, logger=LOGGER):
        checked = await service.background_stale()

    assert checked == 2  # the sweep continues past a failing resource
    assert "background validate" in caplog.text


async def test_background_loop_logs_and_continues_after_sweep_error(
    purser, monkeypatch, caplog
):
    pool = purser.ensure_pool(
        "bg-boom", "fakecheck", stale_after=0.0, background_interval=0.5
    )
    service = pool._validation

    async def boom() -> int:
        raise RuntimeError("sweep exploded")

    monkeypatch.setattr(service, "background_stale", boom)
    task = asyncio.create_task(service.background_loop())
    try:
        with caplog.at_level(logging.ERROR, logger=LOGGER):

            async def poll():
                while "background validation failed" not in caplog.text:
                    await asyncio.sleep(0.01)
                return True

            await asyncio.wait_for(poll(), 10.0)
        assert not task.done()  # the loop survives the failure
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
