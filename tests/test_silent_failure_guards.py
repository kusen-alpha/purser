"""Failure paths must leave a trace instead of silently swallowing errors."""

from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace

import pytest
from conftest import seed

from purser.errors import PoolEmpty
from purser.facade.bundle import BundleService


async def test_acquire_many_rollback_failure_is_logged(purser, make_pool, monkeypatch, caplog):
    pool = make_pool("p")
    await seed(pool, n=2)

    real_acquire = pool.acquire
    calls = {"n": 0}

    async def flaky(basis):
        calls["n"] += 1
        if calls["n"] == 2:
            raise PoolEmpty("second slot unavailable")
        return await real_acquire(basis)

    async def boom(lease):
        raise RuntimeError("release down")

    monkeypatch.setattr(pool, "acquire", flaky)
    monkeypatch.setattr(pool, "release", boom)
    with caplog.at_level(logging.WARNING, logger="purser"):
        with pytest.raises(PoolEmpty):
            await pool.acquire_many(2, n_min=2)
    assert "acquire_many rollback failed" in caplog.text
    assert "release down" in caplog.text


async def test_bundle_member_release_failure_is_logged(caplog):
    class _Pool:
        async def release(self, lease):
            raise RuntimeError("release down")

    service = BundleService(SimpleNamespace(_pools={"a": _Pool()}))
    with caplog.at_level(logging.WARNING, logger="purser"):
        await service._release_bundle_members([("a", object())])
    assert "bundle member release failed" in caplog.text


async def test_guard_error_scheduling_failure_is_logged(purser, make_pool, monkeypatch, caplog):
    pool = make_pool("p")

    def no_loop():
        raise RuntimeError("no running loop")

    monkeypatch.setattr(asyncio, "get_running_loop", no_loop)
    with caplog.at_level(logging.DEBUG, logger="purser"):
        pool._guard_error("validate", RuntimeError("boom"))
    assert "on_error hook not scheduled" in caplog.text
