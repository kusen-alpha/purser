"""Plugin hooks, infrastructure units and crypto/registry edges."""

from __future__ import annotations

import asyncio
import types as _types

import pytest

from purser import Purser
from purser.adapters.base import (
    ResourceAdapter,
    ResourceAdapterRegistry,
    clear_default_adapters,
    register,
)
from purser.core.health import HealthReport
from purser.core.resource import AcquireRequest, CheckVerdict, Resource, UsageOutcome
from purser.core.state import ResourceState
from purser.crypto import SecretCodec, load_or_create_key
from purser.errors import BadState, ConfigError
from purser.idempotency import IdempotencyStore
from purser.metrics import Metrics


@pytest.fixture(autouse=True)
def _clear_default_adapters():
    clear_default_adapters()
    yield
    clear_default_adapters()


class HookAdapter(ResourceAdapter):
    kind = "hooks"

    def __init__(self) -> None:
        self.released: list[tuple[str, UsageOutcome]] = []
        self.changes: list[tuple[ResourceState, ResourceState, str]] = []
        self.errors: list[str] = []
        self.acquired: list[str] = []
        self.renewed: list[str] = []
        self.before_released: list[str] = []
        self.revoked: list[tuple[str, str]] = []
        self.lost: list[str] = []

    async def generate(self, spec: dict) -> Resource:
        return Resource(id=spec["id"], kind=self.kind, payload={"host": "x", "port": 1})

    async def validate(self, resource: Resource) -> HealthReport:
        return HealthReport.ok()

    async def on_request_check(self, resource: Resource, req) -> CheckVerdict:
        return CheckVerdict.PASS

    async def on_release(self, resource: Resource, outcome: UsageOutcome) -> None:
        self.released.append((resource.id, outcome))

    async def on_acquire(self, resource: Resource, lease) -> None:
        self.acquired.append(resource.id)

    async def on_renew(self, resource, lease) -> None:
        self.renewed.append(lease.resource_id)

    async def before_release(self, resource: Resource, lease) -> None:
        self.before_released.append(resource.id)

    async def on_revoke(self, resource: Resource, lease, reason: str) -> None:
        self.revoked.append((resource.id, reason))

    async def on_lease_lost(self, resource: Resource, lease) -> None:
        self.lost.append(resource.id)

    async def on_state_changed(
        self, resource: Resource, old: ResourceState, new: ResourceState, reason: str
    ) -> None:
        self.changes.append((old, new, reason))

    async def on_error(self, context: str, error: Exception) -> None:
        self.errors.append(context)


class BrokenHookAdapter(HookAdapter):
    kind = "broken"

    async def on_release(self, resource: Resource, outcome: UsageOutcome) -> None:
        raise RuntimeError("hook exploded")


class SwitchAdapter(ResourceAdapter):
    kind = "switchkind"

    def __init__(self) -> None:
        self.mode = "decline"

    async def generate(self, spec: dict) -> Resource:
        return Resource(
            id=spec["id"], kind=self.kind, payload={"host": "old", "port": 80}
        )

    async def validate(self, resource: Resource) -> HealthReport:
        return HealthReport.ok()

    async def switch(self, resource: Resource) -> Resource | None:
        if self.mode == "decline":
            return None
        if self.mode == "inplace":
            resource.payload = {**resource.payload, "host": "new"}
            return resource
        return Resource(
            id=f"{resource.id}-v2", kind=self.kind, payload={"host": "v2", "port": 81}
        )


async def test_plugin_hooks_fire(make_pool, purser):
    checker = HookAdapter()
    purser.register_adapter(checker)
    pool = purser.pool("hookpool", "hooks")
    await pool.import_resources([{"id": "r1"}], import_validate="on_import")
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    await pool.release(lease, outcome=UsageOutcome.RATE_LIMITED)
    assert ("r1", UsageOutcome.RATE_LIMITED) in checker.released
    assert any(
        old is ResourceState.IDLE and new is ResourceState.FULL
        for old, new, _ in checker.changes
    )
    assert any(
        old is ResourceState.NEW and new is ResourceState.VALIDATING
        for old, new, _ in checker.changes
    )


async def test_plugin_hook_exception_is_contained_and_reported(make_pool, purser):
    checker = BrokenHookAdapter()
    purser.register_adapter(checker)
    pool = purser.pool("brokenpool", "broken")
    await pool.import_resources([{"id": "r1"}], import_validate="on_import")
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    assert await pool.release(lease) is True  # hook failure must not break release
    for _ in range(50):
        if checker.errors:
            break
        await asyncio.sleep(0.01)
    assert "on_release" in checker.errors


async def test_lease_lifecycle_hooks_fire(make_pool, purser):
    checker = HookAdapter()
    purser.register_adapter(checker)
    pool = purser.pool("lifecycle", "hooks")
    await pool.import_resources([{"id": "r1"}], import_validate="on_import")

    lease = await pool.acquire(AcquireRequest(consumer="c"))
    assert checker.acquired == ["r1"]

    assert await pool.renew_manual(lease) is True
    assert checker.renewed == ["r1"]

    assert await pool.release(lease) is True
    assert checker.before_released == ["r1"]
    assert ("r1", UsageOutcome.OK) in checker.released

    lease = await pool.acquire(AcquireRequest(consumer="c2"))
    await pool.force_revoke("r1", reason="admin kick")
    assert ("r1", "admin kick") in checker.revoked

    lease = await pool.acquire(AcquireRequest(consumer="c3"))
    await pool._on_lease_lost(lease)
    assert checker.lost == ["r1"]


async def test_switch_declines_and_leaves_resource_untouched(purser):
    checker = SwitchAdapter()
    purser.register_adapter(checker)
    pool = purser.pool("switch-decline", "switchkind")
    await pool.import_resources([{"id": "r1"}], import_validate="on_import")
    assert await pool.switch_resource("r1") is None
    assert (await pool.get("r1")).payload["host"] == "old"


async def test_switch_mutates_old_resource_in_place(purser):
    checker = SwitchAdapter()
    checker.mode = "inplace"
    purser.register_adapter(checker)
    pool = purser.pool("switch-inplace", "switchkind")
    await pool.import_resources([{"id": "r1"}], import_validate="on_import")

    switched = await pool.switch_resource("r1")
    assert switched is not None and switched.id == "r1"
    assert switched.payload["host"] == "new"
    assert (await pool.get("r1")).payload["host"] == "new"

    lease = await pool.acquire(AcquireRequest(consumer="c"))
    assert lease.resource_id == "r1"
    await pool.release(lease)


async def test_switch_replaces_with_new_resource(purser):
    checker = SwitchAdapter()
    checker.mode = "replace"
    purser.register_adapter(checker)
    pool = purser.pool("switch-replace", "switchkind")
    await pool.import_resources([{"id": "r1"}], import_validate="on_import")

    switched = await pool.switch_resource("r1")
    assert switched is not None and switched.id == "r1-v2"
    assert (await pool.get("r1")).state is ResourceState.ARCHIVED
    assert (await pool.get("r1-v2")).payload["host"] == "v2"


async def test_switch_rejects_busy_resource(purser):
    checker = SwitchAdapter()
    purser.register_adapter(checker)
    pool = purser.pool("switch-busy", "switchkind")
    await pool.import_resources([{"id": "r1"}], import_validate="on_import")
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    try:
        with pytest.raises(BadState):
            await pool.switch_resource("r1")
    finally:
        await pool.release(lease)


class DefaultAdapter(ResourceAdapter):
    kind = "default-kind"

    def __init__(self, region: str = "us") -> None:
        self.region = region
        self.seen: list[str] = []

    async def generate(self, spec: dict) -> Resource:
        return Resource(id=spec["id"], kind=self.kind, payload={})

    async def validate(self, resource: Resource) -> HealthReport:
        return HealthReport.ok()


async def test_register_decorator_bare_registers_default():
    @register
    class BareAdapter(DefaultAdapter):
        kind = "bare-default"

    ps = Purser(maintenance_enabled=False)
    try:
        loaded = ps.adapters.get("bare-default")
        assert isinstance(loaded, BareAdapter)
        assert loaded.region == "us"
    finally:
        await ps.close()


async def test_register_decorator_factory_config():
    @register(region="eu")
    class ConfType(DefaultAdapter):
        kind = "conf-default"

    ps = Purser(maintenance_enabled=False)
    try:
        assert ps.adapters.get("conf-default").region == "eu"
    finally:
        await ps.close()


async def test_register_defaults_are_per_purser_instances():
    @register
    class StatefulType(DefaultAdapter):
        kind = "stateful-default"

    first = Purser(maintenance_enabled=False)
    second = Purser(maintenance_enabled=False)
    try:
        t1 = first.adapters.get("stateful-default")
        t2 = second.adapters.get("stateful-default")
        assert t1 is not t2
        t1.seen.append("x")
        assert t2.seen == []
    finally:
        await first.close()
        await second.close()


def test_register_duplicate_kind_raises():
    @register
    class DupA(DefaultAdapter):
        kind = "dup-default"

    with pytest.raises(ConfigError, match="duplicate"):
        @register
        class DupB(DefaultAdapter):
            kind = "dup-default"


def test_register_builtin_kind_conflict_raises():
    @register
    class ProxyReboot(DefaultAdapter):
        kind = "proxy"

    with pytest.raises(ConfigError, match="builtin"):
        Purser(maintenance_enabled=False)


async def test_register_use_globals_false_skips_defaults():
    @register
    class SkippedType(DefaultAdapter):
        kind = "skipped-default"

    ps = Purser(maintenance_enabled=False, use_globals=False)
    try:
        with pytest.raises(ConfigError):
            ps.adapters.get("skipped-default")
    finally:
        await ps.close()


def test_registry_lifecycle():
    class T(ResourceAdapter):
        kind = "t"

        async def generate(self, spec: dict) -> Resource:
            return Resource(id="x", kind=self.kind, payload={})

        async def validate(self, resource: Resource) -> HealthReport:
            return HealthReport.ok()

    reg = ResourceAdapterRegistry()
    t = T()
    reg.register(t)
    assert t.kind in reg and reg.get(t.kind) is t and reg.names() == ["t"]
    reg.unregister(t.kind)
    assert t.kind not in reg
    with pytest.raises(ConfigError):
        reg.get(t.kind)


def test_metrics_snapshot_and_render():
    m = Metrics()
    m.inc("a")
    m.inc("a")
    m.inc("b", 3)
    m.observe("lat", 1.0)
    m.observe("lat", 3.0)
    snap = m.snapshot()
    assert snap["counters"]["a"] == 2 and snap["counters"]["b"] == 3
    assert snap["histograms"]["lat"]["count"] == 2
    assert snap["histograms"]["lat"]["avg"] == 2.0
    text = m.render_prometheus()
    assert "purser_a 2" in text and "purser_lat_count 2" in text


def test_idempotency_ttl(monkeypatch):
    now = [1000.0]
    monkeypatch.setattr(
        "purser.idempotency.time", _types.SimpleNamespace(time=lambda: now[0])
    )
    store = IdempotencyStore(ttl=100)
    store.put("k", {"ok": True})
    assert store.get("k") == {"ok": True}
    now[0] = 1000 + 101
    assert store.get("k") is None
    store.put("k2", {})
    now[0] = 1000 + 202
    assert store.sweep() >= 1


def test_secret_codec_env_and_keyfile(monkeypatch, tmp_path):
    monkeypatch.setenv("PURSER_SECRET_KEY", "a" * 32)
    codec = SecretCodec()
    assert len(codec.key) == 32
    path = str(tmp_path / "s.db")
    first = load_or_create_key(path)
    second = load_or_create_key(path)
    assert first == second and len(first) == 32
