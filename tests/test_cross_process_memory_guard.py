"""Cross-process authority guard for the unsupported memory-based mixes.

Only ``sqlite``/``mysql`` storage + Redis coordination can be shared by
independent ``Purser`` instances in different processes.  This suite proves
that the other two memory-based mixes are rejected across a real process
boundary and become usable again after the owner closes (or crashes).
"""

from __future__ import annotations

import asyncio
import json
import multiprocessing as mp
import os
import uuid

import pytest
from conftest import FakeChecker

from purser import Purser

REDIS_URL = os.environ.get("PURSER_TEST_REDIS_URL")
MYSQL_URL = os.environ.get("PURSER_TEST_MYSQL_URL")

CTX = mp.get_context("spawn")


def _report(path: str, outcome: dict) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(outcome, fh)
        fh.flush()
        os.fsync(fh.fileno())


def _start_child(
    outcome_path: str,
    *,
    storage: str,
    coordination: str,
    pool_name: str,
    sqlite_path: str | None = None,
    redis_url: str | None = None,
    mysql_url: str | None = None,
    crash: bool = False,
) -> None:
    """Runs in a spawned OS process with its own ``Purser`` instance."""

    outcome: dict = {"ok": False, "pid": os.getpid(), "error": "worker crashed"}

    async def run() -> dict:
        ps = Purser(
            maintenance_enabled=False,
            storage=storage,
            coordination=coordination,
            sqlite_path=sqlite_path,
            redis_url=redis_url,
            mysql_url=mysql_url,
        )
        ps.register_type(FakeChecker())
        ps.create_pool(pool_name, "fakecheck")
        try:
            await ps.start()
        except BaseException as exc:  # noqa: BLE001 - report the typed error
            error = f"{type(exc).__name__}: {exc}"
            await ps.close()
            return {"ok": False, "pid": os.getpid(), "error": error}
        if crash:
            _report(
                outcome_path,
                {"ok": True, "pid": os.getpid(), "error": "", "crashed": True},
            )
            os._exit(17)  # no close, no lock release in Python
        await ps.close()
        return {"ok": True, "pid": os.getpid(), "error": ""}

    try:
        outcome = asyncio.run(run())
    except BaseException as exc:  # noqa: BLE001 - always report worker state
        outcome = {
            "ok": False,
            "pid": os.getpid(),
            "error": f"fatal {type(exc).__name__}: {exc}",
        }
    _report(outcome_path, outcome)


def _run(target, *args: object, timeout: float = 60.0, **kwargs: object) -> int:
    proc = CTX.Process(target=target, args=args, kwargs=kwargs)
    proc.start()
    proc.join(timeout)
    if proc.is_alive():
        proc.terminate()
        proc.join(5)
        raise AssertionError("child process did not finish")
    return proc.exitcode


def _child(
    tmp_path,
    name: str,
    *,
    storage: str,
    coordination: str,
    pool_name: str,
    sqlite_path: str | None = None,
    redis_url: str | None = None,
    mysql_url: str | None = None,
    crash: bool = False,
) -> tuple[int, dict]:
    outcome_path = str(tmp_path / f"{name}.json")
    code = _run(
        _start_child,
        outcome_path,
        storage=storage,
        coordination=coordination,
        pool_name=pool_name,
        sqlite_path=sqlite_path,
        redis_url=redis_url,
        mysql_url=mysql_url,
        crash=crash,
    )
    with open(outcome_path, encoding="utf-8") as fh:
        return code, json.load(fh)


def _build(storage: str, coordination: str, *, sqlite_path=None, redis_url=None, mysql_url=None):
    ps = Purser(
        maintenance_enabled=False,
        storage=storage,
        coordination=coordination,
        sqlite_path=sqlite_path,
        redis_url=redis_url,
        mysql_url=mysql_url,
    )
    ps.register_type(FakeChecker())
    return ps


# -- memory + Redis --------------------------------------------------------


@pytest.mark.skipif(not REDIS_URL, reason="PURSER_TEST_REDIS_URL not set")
async def test_memory_redis_second_process_rejected(tmp_path):
    pool_name = f"auth-redis-{uuid.uuid4().hex[:8]}"
    ps = _build("memory", "redis", redis_url=REDIS_URL)
    ps.create_pool(pool_name, "fakecheck")
    await ps.start()
    try:
        code, out = _child(
            tmp_path,
            "conflict",
            storage="memory",
            coordination="redis",
            pool_name=pool_name,
            redis_url=REDIS_URL,
        )
        assert code == 0
        assert out["ok"] is False
        assert "IsolationConflict" in out["error"]
        assert "in-memory storage" in out["error"]
    finally:
        await ps.close()

    code, out = _child(
        tmp_path,
        "after-close",
        storage="memory",
        coordination="redis",
        pool_name=pool_name,
        redis_url=REDIS_URL,
    )
    assert code == 0 and out["ok"] is True, out


@pytest.mark.skipif(not REDIS_URL, reason="PURSER_TEST_REDIS_URL not set")
async def test_memory_redis_different_pools_coexist(tmp_path):
    pool_name = f"auth-redis-a-{uuid.uuid4().hex[:8]}"
    ps = _build("memory", "redis", redis_url=REDIS_URL)
    ps.create_pool(pool_name, "fakecheck")
    await ps.start()
    try:
        code, out = _child(
            tmp_path,
            "other-pool",
            storage="memory",
            coordination="redis",
            pool_name=f"{pool_name}-b",
            redis_url=REDIS_URL,
        )
        assert code == 0 and out["ok"] is True, out
    finally:
        await ps.close()


# -- SQLite + memory -------------------------------------------------------


async def test_sqlite_memory_second_process_rejected(tmp_path):
    db = str(tmp_path / "authority.db")
    ps = _build("sqlite", "memory", sqlite_path=db)
    ps.create_pool("p", "fakecheck")
    await ps.start()
    try:
        code, out = _child(
            tmp_path,
            "conflict",
            storage="sqlite",
            coordination="memory",
            pool_name="p",
            sqlite_path=db,
        )
        assert code == 0
        assert out["ok"] is False
        assert "IsolationConflict" in out["error"]
        assert "SQLite file" in out["error"]
    finally:
        await ps.close()

    code, out = _child(
        tmp_path,
        "after-close",
        storage="sqlite",
        coordination="memory",
        pool_name="p",
        sqlite_path=db,
    )
    assert code == 0 and out["ok"] is True, out


async def test_sqlite_memory_hardlink_alias_rejected(tmp_path):
    db = str(tmp_path / "authority.db")
    alias = str(tmp_path / "alias.db")
    ps = _build("sqlite", "memory", sqlite_path=db)
    ps.create_pool("p", "fakecheck")
    await ps.start()
    try:
        try:
            os.link(db, alias)
        except OSError:
            pytest.skip("hardlinks are not supported on this filesystem")
        code, out = _child(
            tmp_path,
            "alias",
            storage="sqlite",
            coordination="memory",
            pool_name="p",
            sqlite_path=alias,
        )
        assert code == 0
        assert out["ok"] is False
        assert "IsolationConflict" in out["error"]
        assert "SQLite file" in out["error"]
    finally:
        await ps.close()

    code, out = _child(
        tmp_path,
        "after-close",
        storage="sqlite",
        coordination="memory",
        pool_name="p",
        sqlite_path=alias,
    )
    assert code == 0 and out["ok"] is True, out


async def test_sqlite_memory_lock_released_on_crash(tmp_path):
    db = str(tmp_path / "crash.db")
    code, out = _child(
        tmp_path,
        "crashed",
        storage="sqlite",
        coordination="memory",
        pool_name="p",
        sqlite_path=db,
        crash=True,
    )
    assert code == 17, out
    assert out["ok"] is True

    ps = _build("sqlite", "memory", sqlite_path=db)
    ps.create_pool("p", "fakecheck")
    try:
        await ps.start()
    finally:
        await ps.close()


async def test_sqlite_memory_different_pools_coexist(tmp_path):
    db = str(tmp_path / "shared-file.db")
    ps = _build("sqlite", "memory", sqlite_path=db)
    ps.create_pool("a", "fakecheck")
    await ps.start()
    try:
        code, out = _child(
            tmp_path,
            "other-pool",
            storage="sqlite",
            coordination="memory",
            pool_name="b",
            sqlite_path=db,
        )
        assert code == 0 and out["ok"] is True, out
    finally:
        await ps.close()


async def test_escape_hatch_allows_simulated_hosts(tmp_path, monkeypatch):
    monkeypatch.setenv("PURSER_ALLOW_SHARED_BACKENDS", "1")
    db = str(tmp_path / "harness.db")
    ps = _build("sqlite", "memory", sqlite_path=db)
    ps.create_pool("p", "fakecheck")
    await ps.start()
    try:
        code, out = _child(
            tmp_path,
            "harness",
            storage="sqlite",
            coordination="memory",
            pool_name="p",
            sqlite_path=db,
        )
        assert code == 0 and out["ok"] is True, out
    finally:
        await ps.close()


# -- memory + memory -------------------------------------------------------


async def test_memory_memory_is_independent_not_shared(tmp_path):
    ps = _build("memory", "memory")
    ps.create_pool("p", "fakecheck")
    await ps.start()
    try:
        code, out = _child(
            tmp_path,
            "independent",
            storage="memory",
            coordination="memory",
            pool_name="p",
        )
        assert code == 0 and out["ok"] is True, out
    finally:
        await ps.close()


# -- MySQL + memory --------------------------------------------------------


@pytest.mark.skipif(not MYSQL_URL, reason="PURSER_TEST_MYSQL_URL not set")
async def test_mysql_memory_second_process_rejected(tmp_path):
    pool_name = f"auth-mysql-{uuid.uuid4().hex[:8]}"
    ps = _build("mysql", "memory", mysql_url=MYSQL_URL)
    ps.create_pool(pool_name, "fakecheck")
    await ps.start()
    try:
        code, out = _child(
            tmp_path,
            "conflict",
            storage="mysql",
            coordination="memory",
            pool_name=pool_name,
            mysql_url=MYSQL_URL,
        )
        assert code == 0
        assert out["ok"] is False
        assert "IsolationConflict" in out["error"]
        assert "MySQL database" in out["error"]
    finally:
        await ps.close()

    code, out = _child(
        tmp_path,
        "after-close",
        storage="mysql",
        coordination="memory",
        pool_name=pool_name,
        mysql_url=MYSQL_URL,
    )
    assert code == 0 and out["ok"] is True, out
