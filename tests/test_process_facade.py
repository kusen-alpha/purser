"""ProcessPurser: owned/attach lifecycles, pickling, remote adapter specs."""

from __future__ import annotations

import pickle

import pytest
from facade_adapters import FacadeAdapter, NeedsArgsAdapter

from purser import AdapterSpec, ProcessPurser
from purser.errors import ConfigError, IsolationConflict, PoolNotFound
from purser.local import SchedulerUnavailable


async def test_owned_process_facade_round_trip():
    async with ProcessPurser() as ps:
        assert ps.owned
        ps.register_adapter(AdapterSpec.of(FacadeAdapter))
        pool = ps.create_pool("devices", "facade")
        assert ps.get_pool_names() == ["devices"]
        assert ps.pools == {"devices": pool}
        assert ps.get_pool("devices") is pool
        assert ps.get_pool("missing", default=None) is None

        report = await pool.import_resources([{"id": "a", "host": "10.1.1.1"}])
        assert report["imported"] == 1, report
        async with pool.lease(consumer="tests") as lease:
            assert lease.payload["host"] == "10.1.1.1"
        stats = await pool.stats()
        assert stats["total"] == 1, stats

        removed = await ps.delete_pool("devices", force=True)
        assert removed["removed"] == 1, removed
        assert ps.get_pool_names() == []
        assert ps.pools == {}
        assert ps.get_pool("devices", default=None) is None

    assert ps.closed
    assert ps.pools == {}
    with pytest.raises(SchedulerUnavailable):
        ps.get_pool_names()


async def test_process_facade_pool_is_read_only():
    async with ProcessPurser() as ps:
        with pytest.raises(PoolNotFound):
            ps.get_pool("missing")
        assert ps.get_pool("missing", default=None) is None
        with pytest.raises(TypeError):
            ps.get_pool("missing", "facade")


async def test_owned_process_facade_refuses_pickle():
    ps = ProcessPurser()
    try:
        await ps.start()
        with pytest.raises(IsolationConflict):
            pickle.dumps(ps)
    finally:
        await ps.close()


async def test_attach_mode_pickles_and_does_not_own():
    async with ProcessPurser() as ps:
        ps.register_adapter(AdapterSpec.of(FacadeAdapter))
        ps.create_pool("devices", "facade")

        attached = ProcessPurser(address=ps.address)
        assert not attached.owned
        restored = pickle.loads(pickle.dumps(attached))
        assert restored.address.address == ps.address.address

        await restored.start()
        assert restored.get_pool_names() == ["devices"]
        reused = restored.ensure_pool("devices", "facade")
        assert reused.name == "devices"
        assert restored.get_pool("devices") is reused
        second = restored.create_pool("more", "facade")
        await second.import_resources([{"id": "c"}])
        await restored.close()

        # Closing an attached handle leaves the owner's scheduler alive.
        assert ps.get_pool_names() == ["devices", "more"]


async def test_register_adapter_requires_importable_spec():
    async with ProcessPurser() as ps:
        with pytest.raises(ConfigError):
            ps.register_adapter(NeedsArgsAdapter("x"))
        ps.register_adapter(AdapterSpec.of(NeedsArgsAdapter, prefix="y"))
        pool = ps.create_pool("needs", "needsargs")
        report = await pool.import_resources([{"id": "n1"}])
        assert report["imported"] == 1, report
