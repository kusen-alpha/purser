"""One Purser instance must not cross a process boundary.

Pickling is refused with a typed error; a fork-inherited instance is stamped
with its owner PID and rejects the first call in the child.  A child that
creates its own instance is unaffected because the backend registry is reset
after fork.
"""

from __future__ import annotations

import asyncio
import multiprocessing as mp
import os
import pickle
import sys

import pytest

import purser.isolation as isolation
from purser import IsolationConflict, Purser, SyncPurser


class _OtherPid:
    """Delegates to ``os`` but reports a different process (simulates fork)."""

    def __init__(self, pid: int) -> None:
        self._pid = pid

    def __getattr__(self, name: str):
        return getattr(os, name)

    def getpid(self) -> int:
        return self._pid


def test_pid_mismatch_rejected(monkeypatch):
    ps = Purser(maintenance_enabled=False)
    monkeypatch.setattr(isolation, "os", _OtherPid(ps._isolation._pid + 1))
    with pytest.raises(IsolationConflict, match="created in process"):
        ps.create_pool("p", "proxy", max_slots=1)
    with pytest.raises(IsolationConflict, match="created in process"):
        asyncio.run(ps.start())


def test_purser_not_picklable():
    ps = Purser(maintenance_enabled=False)
    with pytest.raises(IsolationConflict):
        pickle.dumps(ps)


def test_pool_not_picklable():
    ps = Purser(maintenance_enabled=False)
    pool = ps.create_pool("p", "proxy", max_slots=1)
    with pytest.raises(IsolationConflict):
        pickle.dumps(pool)


def test_sync_facade_not_picklable():
    with SyncPurser(maintenance_enabled=False) as ps:
        pool = ps.create_pool("p", "proxy", max_slots=1)
        with pytest.raises(IsolationConflict):
            pickle.dumps(ps)
        with pytest.raises(IsolationConflict):
            pickle.dumps(pool)


def _inherit_create_pool(conn, ps):
    try:
        ps.create_pool("child", "proxy", max_slots=1)
    except BaseException as exc:
        conn.send((type(exc).__name__, str(exc)))
    else:
        conn.send(("ok", ""))
    finally:
        conn.close()


def _inherit_start_pool(conn, pool):
    try:
        asyncio.run(pool.start())
    except BaseException as exc:
        conn.send((type(exc).__name__, str(exc)))
    else:
        conn.send(("ok", ""))
    finally:
        conn.close()


def _own_instance(conn):
    try:
        ps = Purser(maintenance_enabled=False)
        pool = ps.create_pool("child", "proxy", max_slots=1)
        conn.send(("ok", pool.name))
    except BaseException as exc:
        conn.send((type(exc).__name__, str(exc)))
    finally:
        conn.close()


@pytest.mark.skipif(sys.platform == "win32", reason="fork is POSIX-only")
def test_fork_inherited_purser_rejected():
    ps = Purser(maintenance_enabled=False)
    ctx = mp.get_context("fork")
    receiver, sender = ctx.Pipe(duplex=False)
    proc = ctx.Process(target=_inherit_create_pool, args=(sender, ps))
    proc.start()
    sender.close()
    try:
        name, message = receiver.recv(timeout=10)
        proc.join(timeout=10)
    finally:
        receiver.close()
        if proc.is_alive():
            proc.terminate()
            proc.join(timeout=5)
    assert name == "IsolationConflict", message
    assert "created in process" in message


@pytest.mark.skipif(sys.platform == "win32", reason="fork is POSIX-only")
def test_fork_inherited_pool_rejected():
    ps = Purser(maintenance_enabled=False)
    pool = ps.create_pool("shared", "proxy", max_slots=1)
    ctx = mp.get_context("fork")
    receiver, sender = ctx.Pipe(duplex=False)
    proc = ctx.Process(target=_inherit_start_pool, args=(sender, pool))
    proc.start()
    sender.close()
    try:
        name, message = receiver.recv(timeout=10)
        proc.join(timeout=10)
    finally:
        receiver.close()
        if proc.is_alive():
            proc.terminate()
            proc.join(timeout=5)
    assert name == "IsolationConflict", message


@pytest.mark.skipif(sys.platform == "win32", reason="fork is POSIX-only")
def test_fork_child_can_create_own_purser():
    ps = Purser(maintenance_enabled=False)
    ps.create_pool("parent", "proxy", max_slots=1)
    ctx = mp.get_context("fork")
    receiver, sender = ctx.Pipe(duplex=False)
    proc = ctx.Process(target=_own_instance, args=(sender,))
    proc.start()
    sender.close()
    try:
        outcome = receiver.recv(timeout=10)
        proc.join(timeout=10)
    finally:
        receiver.close()
        if proc.is_alive():
            proc.terminate()
            proc.join(timeout=5)
    assert outcome == ("ok", "child"), outcome
