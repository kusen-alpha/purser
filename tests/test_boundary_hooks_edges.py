"""Plugin hooks, infrastructure units and crypto/registry edges."""

from __future__ import annotations

import asyncio
import types as _types

import pytest

from purser.core.health import HealthReport
from purser.core.resource import AcquireRequest, CheckVerdict, Resource, UsageOutcome
from purser.core.state import ResourceState
from purser.crypto import SecretCodec, load_or_create_key
from purser.errors import ConfigError
from purser.idempotency import IdempotencyStore
from purser.metrics import Metrics
from purser.types.base import ResourceType, ResourceTypeRegistry


class HookType(ResourceType):
    kind = "hooks"

    def __init__(self) -> None:
        self.released: list[tuple[str, UsageOutcome]] = []
        self.changes: list[tuple[ResourceState, ResourceState, str]] = []
        self.errors: list[str] = []

    async def generate(self, spec: dict) -> Resource:
        return Resource(id=spec["id"], kind=self.kind, payload={"host": "x", "port": 1})

    async def validate(self, resource: Resource) -> HealthReport:
        return HealthReport.ok()

    async def on_request_check(self, resource: Resource, req) -> CheckVerdict:
        return CheckVerdict.PASS

    async def on_release(self, resource: Resource, outcome: UsageOutcome) -> None:
        self.released.append((resource.id, outcome))

    async def on_state_changed(
        self, resource: Resource, old: ResourceState, new: ResourceState, reason: str
    ) -> None:
        self.changes.append((old, new, reason))

    async def on_error(self, context: str, error: Exception) -> None:
        self.errors.append(context)


class BrokenHookType(HookType):
    kind = "broken"

    async def on_release(self, resource: Resource, outcome: UsageOutcome) -> None:
        raise RuntimeError("hook exploded")


async def test_plugin_hooks_fire(make_pool, purser):
    checker = HookType()
    purser.register_type(checker)
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
    checker = BrokenHookType()
    purser.register_type(checker)
    pool = purser.pool("brokenpool", "broken")
    await pool.import_resources([{"id": "r1"}], import_validate="on_import")
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    assert await pool.release(lease) is True  # hook failure must not break release
    for _ in range(50):
        if checker.errors:
            break
        await asyncio.sleep(0.01)
    assert "on_release" in checker.errors


def test_registry_lifecycle():
    class T(ResourceType):
        kind = "t"

        async def generate(self, spec: dict) -> Resource:
            return Resource(id="x", kind=self.kind, payload={})

        async def validate(self, resource: Resource) -> HealthReport:
            return HealthReport.ok()

    reg = ResourceTypeRegistry()
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
