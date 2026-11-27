"""Edges of the process facade (:mod:`purser.process`) and the async remote
pool (:mod:`purser.remote`): adapter-spec coercion, attach-mode validation,
lifecycle idempotency and the RemotePool release/renew/stats surface.
"""

from __future__ import annotations

import pytest
from conftest import FakeChecker
from facade_adapters import FacadeAdapter, NeedsArgsAdapter

from purser import AdapterSpec, ConfigError, ProcessPurser, ResourceState
from purser.errors import PoolNotFound, ResourceNotFound
from purser.local import LocalScheduler, SchedulerProtocolError
from purser.local.client import resolve_address
from purser.local.protocol import default_address
from purser.process import _as_spec, _require_zero_arg
from purser.remote import RemotePool, connect

# -- adapter-spec coercion -------------------------------------------------


def test_as_spec_accepts_specs_and_mappings():
    spec = AdapterSpec.of(FacadeAdapter)
    assert _as_spec(spec) is spec
    assert _as_spec({"path": "facade_adapters:FacadeAdapter"}) == spec
    assert _as_spec(
        {"path": "facade_adapters:FacadeAdapter", "kwargs": {}}
    ) == spec


def test_as_spec_rejects_bad_mappings():
    with pytest.raises(ConfigError, match="'path'"):
        _as_spec({})
    with pytest.raises(ConfigError, match="'path'"):
        _as_spec({"path": 42})
    with pytest.raises(ConfigError, match="kwargs"):
        _as_spec({"path": "facade_adapters:FacadeAdapter", "kwargs": ["x"]})


@pytest.mark.parametrize("kwargs", [[], "", 0, False])
def test_as_spec_rejects_falsy_non_mapping_kwargs(kwargs):
    with pytest.raises(ConfigError, match="kwargs"):
        _as_spec({"path": "facade_adapters:FacadeAdapter", "kwargs": kwargs})


def test_as_spec_treats_null_kwargs_as_empty():
    expected = AdapterSpec.of(FacadeAdapter)
    assert _as_spec({"path": "facade_adapters:FacadeAdapter", "kwargs": None}) == expected


def test_as_spec_uses_factory_spec_attribute():
    class WithFactory:
        def factory_spec(self):
            return AdapterSpec.of(FacadeAdapter)

    class BadFactory:
        def factory_spec(self):
            return "facade_adapters:FacadeAdapter"

    assert _as_spec(WithFactory()) == AdapterSpec.of(FacadeAdapter)
    with pytest.raises(ConfigError, match="must return an AdapterSpec"):
        _as_spec(BadFactory())


def test_as_spec_materialises_classes_and_zero_arg_instances():
    assert _as_spec(FacadeAdapter) == AdapterSpec.of(FacadeAdapter)
    assert _as_spec(FacadeAdapter()) == AdapterSpec.of(FacadeAdapter)
    with pytest.raises(ConfigError, match="prefix"):
        _as_spec(NeedsArgsAdapter("x"))


def test_require_zero_arg_checks_keyword_only_parameters():
    class KeywordOnly:
        def __init__(self, *, token):
            self.token = token

    with pytest.raises(ConfigError, match="token"):
        _require_zero_arg(KeywordOnly.__new__(KeywordOnly))
    assert _require_zero_arg(FacadeAdapter()) is FacadeAdapter


def test_shared_resolve_address_forms_and_misuse(monkeypatch):
    from purser.local.protocol import ENV_VAR

    address = default_address()
    assert resolve_address(address) is address
    assert resolve_address(address.to_dict()) == address
    assert resolve_address(address.to_env()) == address

    monkeypatch.setenv(ENV_VAR, address.to_env())
    assert resolve_address(None) == address

    monkeypatch.delenv(ENV_VAR)
    with pytest.raises(ConfigError, match="no scheduler address"):
        resolve_address(None)
    with pytest.raises(TypeError):
        resolve_address(42)


# -- ProcessPurser attach validation --------------------------------------


async def test_attach_mode_rejects_owned_scheduler_options():
    address = default_address()
    attached = ProcessPurser(address=address)
    assert not attached.owned and attached.address == address
    await attached.close()

    with pytest.raises(ConfigError, match="storage"):
        ProcessPurser(address=address, storage="sqlite")
    with pytest.raises(ConfigError, match="sqlite_path"):
        ProcessPurser(address=address, sqlite_path="state.db")
    with pytest.raises(ConfigError, match="adapters"):
        ProcessPurser(address=address, adapters=[FacadeAdapter()])


def test_attach_mode_rejects_unparseable_address_string():
    with pytest.raises(SchedulerProtocolError):
        ProcessPurser(address="not-json")


# -- owned lifecycle -------------------------------------------------------


async def test_owned_lifecycle_is_idempotent_and_closed_after_close():
    ps = ProcessPurser()
    assert "owned" in repr(ps) and ps.owned
    await ps.start()
    await ps.start()  # second start is a no-op
    assert ps.started

    ps.register_adapter({"path": "facade_adapters:FacadeAdapter"})
    pool = ps.ensure_pool("devices", "facade")
    assert ps.ensure_pool("devices", "facade") is pool
    assert ps.get_pool("devices") is pool

    await ps.close()
    assert ps.closed and not ps.started
    await ps.close()
    with pytest.raises(ConfigError, match="closed"):
        await ps.start()


async def test_delete_pool_of_a_missing_name_raises_pool_not_found():
    async with ProcessPurser() as ps:
        with pytest.raises(PoolNotFound):
            await ps.delete_pool("missing")


# -- RemotePool / connect --------------------------------------------------


def _scheduler() -> LocalScheduler:
    return LocalScheduler(adapters=[FakeChecker()], maintenance_enabled=False)


def _seed_scheduler(sched: LocalScheduler) -> None:
    sched.create_pool("p", "fakecheck", config={"max_slots": 1})
    sched.import_resources(
        "p",
        [
            {"id": "r1", "host": "1.2.3.4", "port": 8080},
            {"id": "r2", "host": "5.6.7.8", "port": 8081},
        ],
        import_validate="on_import",
    )


def test_connect_requires_a_usable_address(monkeypatch):
    from purser.local.protocol import ENV_VAR

    monkeypatch.delenv(ENV_VAR, raising=False)
    with pytest.raises(ConfigError, match="no scheduler address"):
        connect("p")
    with pytest.raises(TypeError):
        connect("p", 42)
    with pytest.raises(ConfigError, match="cannot be empty"):
        connect("", default_address())


async def test_remote_pool_lease_renew_release_and_repr():
    with _scheduler() as sched:
        _seed_scheduler(sched)
        pool = connect("p", sched.address)
        assert isinstance(pool, RemotePool)
        assert "name='p'" in repr(pool) and not pool.closed
        try:
            async with pool.lease(consumer="c", timeout=5) as lease:
                assert await pool.renew(lease) > 0
                assert await pool.release(lease) is True
            assert lease.released
            stats = await pool.stats()
            assert stats["total"] == 2
            assert (await pool.acquire(consumer="d", timeout=5)) is not None
        finally:
            await pool.aclose()
            await pool.aclose()  # idempotent
        assert pool.closed
        assert pool._client._closed


async def test_remote_pool_admin_and_bulk_operations():
    with _scheduler() as sched:
        _seed_scheduler(sched)
        pool = connect("p", sched.address)
        try:
            await pool.admin.ban("r1", reason="risk")
            assert (await pool.stats())["counts"]["banned"] == 1
            await pool.admin.unban("r1", reason="cleared", revalidate=False)
            assert (await pool.stats())["counts"].get("banned", 0) == 0

            imported = await pool.import_resources([{"id": "r3", "host": "h"}])
            assert imported["imported"] == 1
            generated = await pool.generate([{"id": "g1", "host": "h"}])
            assert generated["imported"] == 1
        finally:
            await pool.aclose()


async def test_remote_pool_exposes_the_read_only_plane():
    with _scheduler() as sched:
        _seed_scheduler(sched)
        pool = connect("p", sched.address)
        try:
            assert pool.kind == "fakecheck"
            assert pool.profile is None
            assert set(await pool.refresh_info()) >= {"kind", "profile", "counts"}

            resource = await pool.get("r1")
            assert resource is not None and resource.id == "r1"
            assert await pool.get("missing") is None
            with pytest.raises(ResourceNotFound):
                await pool.require("missing")

            found = {item.id for item in await pool.query(limit=10)}
            assert {"r1", "r2"} <= found
            assert await pool.count_matching() == 2
            counts = await pool.counts()
            assert counts[ResourceState.IDLE] == 2
            by_name = await pool.counts(by_name=True)
            assert by_name["idle"] == 2
            assert isinstance(await pool.batches(), list)
            assert await pool.leases() == []

            described = await pool.describe(resource)
            assert described["id"] == "r1" and described["kind"] == "fakecheck"
        finally:
            await pool.aclose()


async def test_connect_with_a_shared_client_keeps_it_open():
    with _scheduler() as sched:
        _seed_scheduler(sched)
        client = sched.client
        pool = connect("p", client=client)
        assert pool.address == sched.address
        assert await pool.stats() is not None
        await pool.aclose()
        assert client.health()["pools"] == ["p"]

        # Address mappings and env strings build their own client.
        async with connect("p", sched.address.to_dict()) as from_dict:
            assert (await from_dict.stats())["total"] == 2
        assert from_dict.closed
        from_env = connect("p", sched.address.to_env())
        try:
            assert (await from_env.stats())["total"] == 2
        finally:
            await from_env.aclose()
