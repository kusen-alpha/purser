"""Named validator checks: collection, lifecycle composition, UI surfacing."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from purser import (
    ConfigError,
    HealthReport,
    Purser,
    Resource,
    ResourceAdapter,
    ResourceState,
    validator,
)
from purser.ui.server import create_app


class CheckedAdapter(ResourceAdapter):
    """Auto and manual validators; ``calls`` records execution order."""

    kind = "checked"

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def generate(self, spec: dict) -> Resource:
        return Resource(id=spec["id"], kind=self.kind, payload=dict(spec))

    @validator(name="creds", both=True)
    async def check_creds(self, resource: Resource) -> HealthReport:
        self.calls.append("creds")
        if resource.payload.get("token"):
            return HealthReport.ok(latency_ms=2.0)
        return HealthReport.fail("missing token")

    @validator(both=False)
    async def check_expiry(self, resource: Resource) -> HealthReport:
        self.calls.append("expiry")
        return HealthReport.ok(latency_ms=3.0)

    @validator(name="extra", both=True)
    async def check_extra(self, resource: Resource) -> HealthReport:
        self.calls.append("extra")
        return HealthReport.ok(latency_ms=4.0)


def test_validator_registry_collected():
    specs = {name: spec.both for name, spec in CheckedAdapter()._validators.items()}
    assert specs == {"creds": True, "check_expiry": False, "extra": True}


def test_adapter_validators_public_contract():
    specs = {spec.name: spec.both for spec in CheckedAdapter().validators()}
    assert specs == {"creds": True, "check_expiry": False, "extra": True}


def test_validators_merge_across_mro_and_nearer_overrides():
    class Child(CheckedAdapter):
        @validator(name="extra", both=False)
        async def other(self, resource: Resource) -> HealthReport:
            return HealthReport.ok()

    specs = {name: spec.both for name, spec in Child()._validators.items()}
    assert specs == {"creds": True, "check_expiry": False, "extra": False}
    assert Child()._validators["extra"].fn.__name__ == "other"

    class GrandChild(Child):
        pass

    assert set(GrandChild()._validators) == {"creds", "check_expiry", "extra"}


def test_duplicate_validator_name_raises():
    with pytest.raises(ConfigError):

        class Dup(ResourceAdapter):
            kind = "dup"

            @validator(name="x")
            async def a(self, resource: Resource) -> HealthReport:
                return HealthReport.ok()

            @validator(name="x")
            async def b(self, resource: Resource) -> HealthReport:
                return HealthReport.ok()


def test_sync_validator_rejected():
    with pytest.raises(TypeError):

        class Sync(ResourceAdapter):
            kind = "sync"

            @validator(name="s")
            def check(self, resource: Resource) -> HealthReport:
                return HealthReport.ok()


async def test_both_true_failure_quarantines_and_short_circuits(purser):
    checker = CheckedAdapter()
    purser.register_adapter(checker)
    pool = purser.create_pool("checked-pool", "checked")
    await pool.import_resources([{"id": "r1"}], import_validate="on_import")

    res = await pool.get("r1")
    assert res.state is ResourceState.QUARANTINED
    assert checker.calls == ["creds"]


async def test_both_true_passes_and_runs_in_order(purser):
    checker = CheckedAdapter()
    purser.register_adapter(checker)
    pool = purser.create_pool("checked-pool", "checked")
    await pool.import_resources(
        [{"id": "r1", "token": "abc"}], import_validate="on_import"
    )

    res = await pool.get("r1")
    assert res.state is ResourceState.IDLE
    assert checker.calls == ["creds", "extra"]


async def test_manual_check_runs_only_that_check_and_skips_state(purser):
    checker = CheckedAdapter()
    purser.register_adapter(checker)
    pool = purser.create_pool("checked-pool", "checked")
    await pool.import_resources(
        [{"id": "r1", "token": "abc"}], import_validate="on_import"
    )
    checker.calls.clear()

    report = await pool.validate_check("r1", "check_expiry")
    assert report.alive and report.latency_ms == 3.0
    assert checker.calls == ["expiry"]

    before = (await pool.get("r1")).health
    after = (await pool.get("r1")).health
    assert (await pool.get("r1")).state is ResourceState.IDLE
    assert before == after


async def test_unknown_validator_raises(purser):
    purser.register_adapter(CheckedAdapter())
    pool = purser.create_pool("checked-pool", "checked")
    await pool.import_resources(
        [{"id": "r1", "token": "abc"}], import_validate="on_import"
    )

    with pytest.raises(ConfigError):
        await pool.validate_check("r1", "nope")


def test_ui_lists_and_runs_validators():
    checker = CheckedAdapter()
    ps = Purser(maintenance_enabled=False)
    ps.register_adapter(checker)
    with TestClient(create_app(ps)) as client:
        adapters = {item["kind"]: item for item in client.get("/adapters").json()["adapters"]}
        checks = {v["name"]: v["both"] for v in adapters["checked"]["validators"]}
        assert checks == {"creds": True, "check_expiry": False, "extra": True}

        created = client.post("/pools", json={"name": "ui-checked", "kind": "checked"})
        assert created.status_code == 200
        imported = client.post(
            "/pools/ui-checked/resources",
            json={
                "records": [{"id": "r1", "token": "abc"}],
                "import_validate": "on_import",
            },
        )
        assert imported.status_code == 200

        checker.calls.clear()
        manual = client.post(
            "/pools/ui-checked/resources/r1/validate",
            json={"check": "check_expiry"},
        )
        assert manual.status_code == 200
        body = manual.json()
        assert body["validation"]["alive"] is True
        assert body["validation"]["check"] == "check_expiry"
        assert checker.calls == ["expiry"]

        checker.calls.clear()
        full = client.post("/pools/ui-checked/resources/r1/validate")
        assert full.status_code == 200
        assert checker.calls == ["creds", "extra"]

        missing = client.post(
            "/pools/ui-checked/resources/r1/validate",
            json={"check": "nope"},
        )
        assert missing.status_code == 422
