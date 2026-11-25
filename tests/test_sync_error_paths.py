"""Error and misuse paths of :mod:`purser.sync`.

Happy-path behaviour lives in ``test_sync.py`` / ``test_sync_acceptance.py``;
these tests pin the lifecycle failures (bad kernel kwargs, slow startup,
stopped loop) and the bridge's timeout/cancellation translations.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import pickle
import time

import pytest
from conftest import FakeChecker

from purser import ConfigError, IsolationConflict, SyncPurser


@pytest.fixture
def sync_ps():
    ps = SyncPurser(maintenance_enabled=False)
    ps.register_adapter(FakeChecker())
    try:
        yield ps
    finally:
        if not ps.closed:
            ps.close()


def test_bad_kernel_kwargs_shut_the_loop_down():
    with pytest.raises(ConfigError):
        SyncPurser(maintenance_enabled=False, storage="unknown-storage")


def test_startup_timeout_is_reported(monkeypatch):
    monkeypatch.setattr(SyncPurser, "_run", lambda self: None)
    with pytest.raises(TimeoutError):
        SyncPurser(maintenance_enabled=False, startup_timeout=0.05)


def test_startup_error_is_reraised(monkeypatch):
    def broken(self) -> None:
        self._startup_error = RuntimeError("kernel exploded")
        self._ready.set()

    monkeypatch.setattr(SyncPurser, "_run", broken)
    with pytest.raises(RuntimeError, match="kernel exploded"):
        SyncPurser(maintenance_enabled=False)


def test_submit_rejects_non_awaitable_factories_and_cancellation(sync_ps):
    with pytest.raises(TypeError):
        sync_ps._submit(lambda: 42)

    async def cancelled():
        raise asyncio.CancelledError

    with pytest.raises(RuntimeError, match="cancelled"):
        sync_ps._submit(cancelled)


def test_call_sync_timeout_does_not_poison_the_loop(sync_ps):
    with pytest.raises(TimeoutError):
        sync_ps._call_sync(lambda: time.sleep(0.3), timeout=0.01)
    # The bridge still accepts calls after an outer timeout.
    assert sync_ps.get_pool_names() == []


def test_start_error_is_sticky_and_enter_closes(sync_ps):
    sync_ps._start_error = RuntimeError("start failed")
    with pytest.raises(RuntimeError, match="failed to start"):
        sync_ps.start()
    assert sync_ps.started is False
    with pytest.raises(RuntimeError, match="failed to start"):
        with sync_ps:
            pass
    assert sync_ps.closed


def test_stopped_loop_and_loop_failure_are_reported(sync_ps):
    sync_ps._loop_error = RuntimeError("loop died")
    with pytest.raises(RuntimeError, match="loop failed"):
        _ = sync_ps.started
    sync_ps._loop_error = None

    loop = sync_ps._loop
    sync_ps._loop = None
    try:
        with pytest.raises(RuntimeError, match="not running"):
            _ = sync_ps.started
        with pytest.raises(TimeoutError):
            sync_ps._shutdown_loop(0)
    finally:
        sync_ps._loop = loop


def test_fail_pending_wakes_tracked_futures(sync_ps):
    future: concurrent.futures.Future = concurrent.futures.Future()
    sync_ps._track(future)
    sync_ps._fail_pending(RuntimeError("loop stopped"))
    with pytest.raises(RuntimeError, match="loop stopped"):
        future.result()


def test_pickle_and_subscribe_reject_misuse(sync_ps):
    with pytest.raises(IsolationConflict):
        pickle.dumps(sync_ps)
    with pytest.raises(TypeError):
        sync_ps.subscribe(42)


def test_pool_registry_handles_unknown_kind_and_close(sync_ps):
    with pytest.raises(ConfigError):
        sync_ps.create_pool("bad", "no-such-kind")
    assert sync_ps.get_pool_names() == []

    pool = sync_ps.create_pool("p", "fakecheck")
    assert sync_ps.ensure_pool("p", "fakecheck") is pool
    pool.close()
    assert sync_ps._cached_pool("p") is None
    sync_ps._forget_pool(pool)  # already forgotten: no-op
