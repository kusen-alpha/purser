"""Every facade must expose the same declared surface.

The protocols in ``purser.protocols`` are the source of truth; a facade that
drifts fails here before the difference reaches users.
"""

from __future__ import annotations

import inspect

import pytest

from purser import (
    PoolProtocol,
    PoolStats,
    ProcessPurser,
    Purser,
    PurserProtocol,
    RemotePool,
    ResourcePool,
    SyncPool,
    SyncPurser,
    ThreadPool,
    ThreadPurser,
)
from purser.protocols import MISSING

_ASYNC_POOLS = (ResourcePool, ThreadPool, RemotePool)
_MANAGEMENT = (Purser, ThreadPurser, SyncPurser, ProcessPurser)
_CORE_METHODS = (
    "acquire",
    "lease",
    "release",
    "renew",
    "stats",
    "import_resources",
    "generate",
)
_ADMIN_METHODS = (
    "apply_defaults",
    "ban",
    "unban",
    "suspend",
    "resume",
    "drain",
    "cancel_drain",
    "top_up",
    "force_revoke",
    "recall_batch",
    "run_maintenance",
    "reconfigure",
    "query",
    "count_matching",
    "counts",
    "describe",
    "get",
    "require",
    "leases",
    "iter_resources",
    "update_resource",
    "switch_resource",
    "move_resource",
    "restore_resource",
    "validate_now",
    "validate_check",
    "validate_job",
    "validate_jobs",
    "cancel_validate_job",
)
_MANAGEMENT_METHODS = (
    "register_adapter",
    "create_pool",
    "ensure_pool",
    "get_pool",
    "get_pool_names",
    "delete_pool",
    "start",
    "close",
)


def _public_names(cls: type) -> set[str]:
    return {name for name in dir(cls) if not name.startswith("_")}


@pytest.mark.parametrize("cls", _ASYNC_POOLS)
def test_async_pools_share_the_core_data_plane(cls: type) -> None:
    missing = set(_CORE_METHODS) - _public_names(cls)
    assert not missing, missing


def test_sync_pool_matches_the_sync_protocol() -> None:
    missing = set(_CORE_METHODS) - _public_names(SyncPool)
    assert not missing, missing


@pytest.mark.parametrize("cls", _ASYNC_POOLS + (SyncPool,))
def test_every_pool_exposes_ban_and_unban(cls: type) -> None:
    missing = {"ban", "unban"} - _public_names(cls)
    assert not missing, missing


@pytest.mark.parametrize("cls", [ResourcePool, SyncPool])
def test_core_pools_expose_the_admin_plane(cls: type) -> None:
    missing = set(_ADMIN_METHODS) - _public_names(cls)
    assert not missing, missing


@pytest.mark.parametrize("cls", _MANAGEMENT)
def test_management_facades_share_the_same_calls(cls: type) -> None:
    missing = set(_MANAGEMENT_METHODS) - _public_names(cls)
    assert not missing, missing
    assert isinstance(inspect.getattr_static(cls, "pools"), property)


def test_management_calls_are_synchronous_and_lifecycle_is_not() -> None:
    for cls in (Purser, ThreadPurser, ProcessPurser):
        for name in ("register_adapter", "create_pool", "ensure_pool", "get_pool"):
            assert not inspect.iscoroutinefunction(getattr(cls, name)), name
        for name in ("delete_pool", "start", "close"):
            assert inspect.iscoroutinefunction(getattr(cls, name)), name

    # SyncPurser blocks on the loop thread: nothing in its surface is awaitable.
    for name in ("create_pool", "delete_pool", "start", "close"):
        assert not inspect.iscoroutinefunction(getattr(SyncPurser, name)), name


def test_renew_signature_is_uniform() -> None:
    assert inspect.iscoroutinefunction(ResourcePool.renew)
    assert not inspect.iscoroutinefunction(SyncPool.renew)
    for cls in _ASYNC_POOLS + (SyncPool,):
        params = inspect.signature(cls.renew).parameters
        assert list(params)[1:3] == ["lease", "used"], (cls, params)


def test_missing_sentinel_is_stable() -> None:
    assert MISSING is MISSING
    assert not isinstance(MISSING, (str, bytes, int))


async def test_stats_covers_every_pool_stats_key(purser: Purser) -> None:
    pool = purser.ensure_pool("contract-stats", "fakecheck")
    stats = await pool.stats()
    assert set(PoolStats.__annotations__) <= set(stats)


async def test_builtin_pools_satisfy_the_protocol(purser: Purser) -> None:
    pool = purser.ensure_pool("contract-protocol", "fakecheck")
    assert isinstance(pool, PoolProtocol)
    assert isinstance(purser, PurserProtocol)
