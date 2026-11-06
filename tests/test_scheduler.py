"""PeriodicScheduler: execution, failure containment and lifecycle."""

from __future__ import annotations

import asyncio

from purser.core.scheduler import PeriodicScheduler


async def test_run_once_updates_last_run_and_ignores_missing():
    scheduler = PeriodicScheduler()
    calls: list[int] = []

    async def task() -> None:
        calls.append(1)

    scheduler.add("x", 0.01, task)
    await scheduler.run_once("x")
    assert calls == [1]
    assert scheduler.get("x").last_run > 0
    await scheduler.run_once("missing")  # no-op
    assert scheduler.names() == ["x"]


async def test_disabled_task_is_skipped():
    scheduler = PeriodicScheduler()
    calls: list[int] = []

    async def task() -> None:
        calls.append(1)

    scheduler.add("x", 0.01, task, enabled=False)
    await scheduler.run_once("x")
    assert calls == []


async def test_failing_task_does_not_kill_scheduler():
    scheduler = PeriodicScheduler()

    async def bad() -> None:
        raise RuntimeError("boom")

    scheduler.add("bad", 0.01, bad)
    await scheduler.run_once("bad")  # must not raise


async def test_start_runs_loop_and_stop_is_idempotent():
    scheduler = PeriodicScheduler()
    calls: list[int] = []

    async def task() -> None:
        calls.append(1)

    scheduler.add("x", 0.01, task)
    await scheduler.start()
    for _ in range(100):
        if calls:
            break
        await asyncio.sleep(0.02)
    assert calls
    await scheduler.stop()
    await scheduler.stop()  # idempotent
    await scheduler.start()  # restartable
    await scheduler.stop()
