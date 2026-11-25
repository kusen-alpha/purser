"""ThreadPurser / ThreadPool facade plus AdapterSpec validation."""

from __future__ import annotations

import pickle

import pytest
from facade_adapters import FacadeAdapter

from purser import (
    AdapterSpec,
    EmbeddedPurser,
    Purser,
    SyncPurser,
    ThreadPool,
    ThreadPurser,
)
from purser.errors import ConfigError, IsolationConflict, PoolExists, PoolNotFound


def test_embedded_alias_is_purser():
    assert EmbeddedPurser is Purser


def test_adapter_spec_of_importable_class():
    spec = AdapterSpec.of(FacadeAdapter)
    assert spec.path.endswith(":FacadeAdapter")
    assert spec.to_dict() == {"path": spec.path, "kwargs": {}}


def test_adapter_spec_rejects_locals_and_non_json():
    class LocalAdapter(FacadeAdapter):
        pass

    with pytest.raises(ConfigError):
        AdapterSpec.of(LocalAdapter)
    with pytest.raises(ConfigError):
        AdapterSpec.of(FacadeAdapter, marker=object())


async def test_thread_facade_data_plane():
    async with ThreadPurser() as ps:
        ps.register_adapter(FacadeAdapter())
        pool = ps.create_pool("devices", "facade")
        assert isinstance(pool, ThreadPool)
        assert ps.get_pool_names() == ["devices"]
        assert list(ps.pools) == ["devices"]
        assert ps.pools["devices"].name == "devices"
        assert ps.get_pool("devices").name == "devices"
        assert ps.get_pool("devices").kind == "facade"
        assert ps.get_pool("missing", default=None) is None

        report = await pool.import_resources([{"id": "a", "host": "10.0.0.1"}])
        assert report.imported == 1, report

        async with pool.lease(consumer="tests") as lease:
            assert lease.payload["host"] == "10.0.0.1"
            lease.outcome = "ok"

        stats = await pool.stats()
        assert stats["total"] == 1, stats

        removed = await ps.delete_pool("devices", force=True)
        assert removed["removed"] == 1, removed
        assert ps.get_pool_names() == []
        assert ps.pools == {}
        assert ps.get_pool("devices", default=None) is None

    assert ps.closed


async def test_thread_facade_pool_is_read_only():
    async with ThreadPurser() as ps:
        ps.register_adapter(FacadeAdapter())
        with pytest.raises(PoolNotFound):
            ps.get_pool("missing")
        assert ps.get_pool("missing", default=None) is None
        with pytest.raises(TypeError):
            ps.get_pool("missing", "facade")


async def test_thread_facade_ensure_pool_is_idempotent():
    async with ThreadPurser() as ps:
        ps.register_adapter(FacadeAdapter())
        created = ps.ensure_pool("devices", "facade")
        again = ps.ensure_pool("devices", "other-kind")
        assert again.name == created.name and again.kind == created.kind
        assert ps.get_pool("devices").name == "devices"
        with pytest.raises(PoolExists):
            ps.create_pool("devices", "facade")


async def test_thread_facade_pickle_is_rejected():
    ps = ThreadPurser()
    try:
        await ps.start()
        ps.register_adapter(FacadeAdapter())
        pool = ps.create_pool("devices", "facade")
        with pytest.raises(IsolationConflict):
            pickle.dumps(ps)
        with pytest.raises(IsolationConflict):
            pickle.dumps(pool)
    finally:
        await ps.close()


async def test_thread_facade_attach_leaves_kernel_running():
    sync = SyncPurser()
    sync.start()
    try:
        sync.register_adapter(FacadeAdapter())
        ps = ThreadPurser(sync=sync)
        assert not ps.owned
        await ps.start()
        pool = ps.create_pool("devices", "facade")
        await pool.import_resources([{"id": "b"}])
        await ps.close()
        assert not sync.closed
        assert sync.get_pool_names() == ["devices"]
    finally:
        sync.close()
