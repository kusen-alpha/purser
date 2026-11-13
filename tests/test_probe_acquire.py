"""Acquire-time per-target live check (AcquireRequest.probe, opt-in)."""

from __future__ import annotations

import asyncio

import pytest
from fastapi.testclient import TestClient

from purser import (
    AcquireRequest,
    CheckVerdict,
    HealthReport,
    NoSuitableResource,
    PoolExhausted,
    Purser,
    Resource,
    ResourceState,
)
from purser.adapters.base import ResourceAdapter
from purser.adapters.proxy import ProxyAdapter
from purser.ui.server import create_app


class ProbeAdapter(ResourceAdapter):
    """Records on_request_check calls; verdict programmable."""

    kind = "prob"
    default_concurrency = 1

    def __init__(self, verdict: CheckVerdict = CheckVerdict.PASS) -> None:
        self.verdict = verdict
        self.calls: list[tuple[str, str | None]] = []

    async def generate(self, spec: dict):
        return Resource(
            id=spec["id"],
            kind=self.kind,
            payload={"host": spec.get("host", "1.2.3.4")},
            meta={},
        )

    async def validate(self, resource: Resource) -> HealthReport:
        return HealthReport.ok()

    async def on_request_check(
        self, resource: Resource, req: AcquireRequest
    ) -> CheckVerdict:
        self.calls.append((resource.id, req.target))
        return self.verdict


async def _pool(adapter: ResourceAdapter):
    ps = Purser(maintenance_enabled=False)
    ps.register_adapter(adapter)
    pool = ps.create_pool("px", adapter.kind)
    await pool.import_resources(
        [{"id": "r1"}], import_validate="on_import"
    )
    await ps.start()
    return ps, pool


async def test_probe_not_set_keeps_default_path():
    adapter = ProbeAdapter()
    ps, pool = await _pool(adapter)
    try:
        lease = await pool.acquire()
        assert adapter.calls == []
        await pool.release(lease)
    finally:
        await ps.close()


async def test_probe_pass_acquires_and_records_target():
    adapter = ProbeAdapter()
    ps, pool = await _pool(adapter)
    try:
        lease = await pool.acquire(
            AcquireRequest(probe="http://target.example:80")
        )
        assert lease.resource_id == "r1"
        assert adapter.calls == [("r1", "http://target.example:80")]
        await pool.release(lease)
    finally:
        await ps.close()


async def test_probe_check_cached_until_ttl():
    adapter = ProbeAdapter()
    ps, pool = await _pool(adapter)
    try:
        lease = await pool.acquire(
            AcquireRequest(probe="http://target.example:80")
        )
        await pool.release(lease)
        lease = await pool.acquire(
            AcquireRequest(probe="http://target.example:80")
        )
        await pool.release(lease)
        assert len(adapter.calls) == 1  # same (consumer, target, rid) cached
        await asyncio.sleep(2.1)  # check_cache_ttl default is 2.0s
        lease = await pool.acquire(
            AcquireRequest(probe="http://target.example:80")
        )
        await pool.release(lease)
        assert len(adapter.calls) == 2
    finally:
        await ps.close()


async def test_probe_fail_skips_candidate():
    adapter = ProbeAdapter(verdict=CheckVerdict.FAIL)
    ps, pool = await _pool(adapter)
    try:
        with pytest.raises(NoSuitableResource):
            await pool.acquire(AcquireRequest(probe="http://target.example:80"))
        assert adapter.calls == [("r1", "http://target.example:80")]
        assert (await pool.get("r1")).state is ResourceState.IDLE
    finally:
        await ps.close()


async def test_probe_ban_bans_resource():
    adapter = ProbeAdapter(verdict=CheckVerdict.BAN)
    ps, pool = await _pool(adapter)
    try:
        with pytest.raises((NoSuitableResource, PoolExhausted)):
            await pool.acquire(AcquireRequest(probe="http://target.example:80"))
        # A single request-level BAN quarantines first; repeated failures
        # escalate to BANNED through the normal health pipeline.
        assert (await pool.get("r1")).state in (
            ResourceState.QUARANTINED,
            ResourceState.BANNED,
        )
    finally:
        await ps.close()


async def test_proxy_probe_on_acquire_wiring(monkeypatch):
    class StubProxy(ProxyAdapter):
        kind = "sproxy"

        async def validate(self, resource: Resource) -> HealthReport:
            return HealthReport.ok()

    adapter = StubProxy(probe_targets=[], timeout=1.0)
    ps = Purser(maintenance_enabled=False)
    ps.register_adapter(adapter)
    pool = ps.create_pool("px", "sproxy")
    await pool.import_resources(
        [
            {
                "id": "p1",
                "host": "1.2.3.4",
                "port": 8080,
                "protocol": "http",
                "username": "u",
                "password": "pw",
            }
        ],
        import_validate="on_import",
    )
    await ps.start()
    calls: list[tuple] = []

    async def fake_probe(protocol, host, port, target, auth, timeout):
        calls.append((host, port, target, auth))
        return (True, 1.0, None)

    monkeypatch.setattr(ps.adapters.get("sproxy"), "_probe", fake_probe)
    try:
        lease = await pool.acquire(
            AcquireRequest(probe="http://target.example:80")
        )
        assert calls[0] == ("1.2.3.4", 8080, "http://target.example:80", ("u", "pw"))
        await pool.release(lease)

        async def fake_fail(*_args):
            return (False, None, "no route")

        monkeypatch.setattr(ps.adapters.get("sproxy"), "_probe", fake_fail)
        with pytest.raises(NoSuitableResource):
            # Different target -> separate cache key, so the FAIL path runs.
            await pool.acquire(AcquireRequest(probe="http://other.example:80"))
    finally:
        await ps.close()


async def test_rest_acquire_probe_passthrough():
    adapter = ProbeAdapter()
    ps = Purser(maintenance_enabled=False)
    ps.register_adapter(adapter)
    pool = ps.create_pool("px", "prob")
    await pool.import_resources(
        [{"id": "r1"}], import_validate="on_import"
    )
    with TestClient(create_app(ps)) as client:
        resp = client.post(
            "/pools/px/leases",
            json={"probe": "http://target.example:80"},
            headers={"X-Operator": "ops"},
        )
        assert resp.status_code == 200, resp.text
        assert adapter.calls == [("r1", "http://target.example:80")]
