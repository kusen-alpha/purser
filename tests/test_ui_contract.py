"""Console UI contract: the served shell, every fetch/EventSource target the
JS relies on, the response shapes it consumes, and the SSE event stream.

Kept as pure Python on top of FastAPI's TestClient so CI can run it on any
supported Python without a browser or a Node toolchain.
"""

from __future__ import annotations

import threading
import time
from typing import Any

import httpx
import uvicorn
from fastapi.testclient import TestClient

from purser import Purser
from purser.core.health import HealthReport
from purser.core.resource import Resource
from purser.types.base import ResourceType
from purser.ui.server import create_app


class _UiChecker(ResourceType):
    """Deterministic plugin for the console flow tests."""

    kind = "uifake"
    default_concurrency = 1

    async def generate(self, spec: dict[str, Any]) -> Resource:
        return Resource(
            id=str(spec["id"]),
            kind=self.kind,
            payload=dict(spec),
            tags=frozenset(spec.get("tags") or ()),
        )

    async def validate(self, resource: Resource) -> HealthReport:
        return HealthReport.ok(latency_ms=1.0)


def _client() -> TestClient:
    ps = Purser(maintenance_enabled=False)
    ps.register_type(_UiChecker())
    return TestClient(create_app(ps))


def test_console_served_and_endpoints_exist():
    client = _client()
    resp = client.get("/")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]
    assert "purSer" in resp.text
    # The DOM ids the JS drives and the round-trip markers it depends on.
    for marker in (
        'id="pools"',
        'id="kpis"',
        'id="resRows"',
        'id="evlist"',
        "EventSource",
        'localStorage.getItem("purser_key")',
        "fetch(path",
    ):
        assert marker in resp.text, marker


def test_console_fetch_targets_registered():
    client = _client()
    methods_by_path: dict[str, set[str]] = {}
    for route in client.app.routes:
        methods_by_path.setdefault(route.path, set()).update(
            set(getattr(route, "methods", set()) or set())
        )
    expected = [
        ("GET", "/pools"),
        ("POST", "/pools"),
        ("GET", "/pools/{pool_name}"),
        ("GET", "/pools/{pool_name}/resources"),
        ("POST", "/pools/{pool_name}/resources"),
        ("POST", "/pools/{pool_name}/resources/{rid}/revoke"),
        ("POST", "/pools/{pool_name}/resources/{rid}/{action}"),
        ("POST", "/pools/{pool_name}/leases"),
        ("GET", "/pools/{pool_name}/leases"),
        ("DELETE", "/pools/{pool_name}/leases/{token}"),
        ("GET", "/pools/{pool_name}/events"),
    ]
    for method, path in expected:
        assert path in methods_by_path, f"missing route {path}"
        assert method in methods_by_path[path], (method, path)


def test_console_flow_response_shapes():
    """Play the console's own call sequence and pin every JSON field the JS
    reads: pool stats, resource rows, lease rows, actions and errors."""

    client = _client()

    created = client.post("/pools", json={"name": "ui-demo", "kind": "uifake"})
    assert created.status_code == 200
    assert created.json() == {"pool": "ui-demo", "kind": "uifake"}

    imported = client.post(
        "/pools/ui-demo/resources",
        json={
            "records": [
                {"id": "r1", "tags": ["us"]},
                {"id": "r2", "tags": ["eu"]},
            ],
            "import_validate": "on_import",
        },
    )
    report = imported.json()
    assert report["imported"] == 2 and report["updated"] == 0 and report["skipped"] == 0
    assert report["errors"] == []  # JS reads rep.errors.length

    pools = client.get("/pools").json()["pools"]
    assert pools[0]["name"] == "ui-demo"
    assert pools[0]["counts"]["idle"] == 2  # JS reads p.counts.idle
    assert pools[0]["total"] == 2  # JS reads p.total

    stat = client.get("/pools/ui-demo").json()
    for key in ("kind", "total", "allocatable", "active_leases", "waiting", "avg_health", "counts"):
        assert key in stat, key
    assert stat["total"] == 2 and stat["allocatable"] == 2 and stat["active_leases"] == 0
    assert stat["counts"]["idle"] == 2

    data = client.get("/pools/ui-demo/resources").json()
    resources = data["resources"]
    assert len(resources) == 2
    by_id = {res["id"]: res for res in resources}
    for res in resources:
        for key in ("id", "state", "health", "used_slots", "max_slots", "tags"):
            assert key in res, res
        assert isinstance(res["health"]["score"], (int, float))
        assert res["state"] == "idle"
        assert res["used_slots"] == 0 and res["max_slots"] == 1
    assert by_id["r1"]["tags"] == ["us"]
    assert data["counts"]["idle"] == 2

    # The filter controls the console wires to query params.
    one = client.get("/pools/ui-demo/resources", params={"q": "r1"}).json()
    assert [r["id"] for r in one["resources"]] == ["r1"]
    tagged = client.get("/pools/ui-demo/resources", params={"tag": "eu"}).json()
    assert [r["id"] for r in tagged["resources"]] == ["r2"]

    lease = client.post(
        "/pools/ui-demo/leases",
        json={"consumer": "ui", "timeout": 1.0},
    ).json()["leases"][0]
    for key in ("token", "resource_id", "consumer", "slots", "payload", "expires_at"):
        assert key in lease, lease
    assert lease["consumer"] == "ui"

    assert client.get("/pools/ui-demo").json()["active_leases"] == 1

    active = client.get("/pools/ui-demo/leases").json()["leases"]
    assert [item["resource_id"] for item in active] == [lease["resource_id"]]
    for field in ("token", "resource_id", "consumer", "slots", "expires_at", "outcome"):
        assert field in active[0], active[0]

    released = client.request(
        "DELETE",
        f"/pools/ui-demo/leases/{lease['token']}",
        json={"outcome": "ok"},
    )
    assert released.status_code == 200 and released.json()["ok"] is True

    acted = client.post("/pools/ui-demo/resources/r1/validate", json={"reason": "ui"})
    assert acted.status_code == 200 and acted.json()["state"] == "idle"

    banned = client.post("/pools/ui-demo/resources/r1/ban", json={"reason": "ui"})
    assert banned.status_code == 200
    states = {
        r["id"]: r["state"]
        for r in client.get("/pools/ui-demo/resources").json()["resources"]
    }
    assert states["r1"] == "banned"

    # The console revoke button posts to the static revoke route; it must not
    # be swallowed by the {action} catch-all (route order regression guard).
    lease2 = client.post(
        "/pools/ui-demo/leases", json={"consumer": "ui2", "timeout": 1.0}
    ).json()["leases"][0]
    revoked = client.post(
        f"/pools/ui-demo/resources/{lease2['resource_id']}/revoke",
        json={"reason": "ui"},
    )
    assert revoked.status_code == 200
    assert revoked.json()["revoked_token"] == lease2["token"]
    assert client.get("/pools/ui-demo/leases").json()["leases"] == []

    # The JS api() helper surfaces `detail` for non-2xx responses.
    missing = client.get("/pools/ui-demo/resources/nope")
    assert missing.status_code == 404 and "detail" in missing.json()
    bad = client.post("/pools/ui-demo/resources", json={"records": "not-a-list"})
    assert bad.status_code == 422 and "detail" in bad.json()


def test_sse_stream_emits_published_events():
    ps = Purser(maintenance_enabled=False)
    ps.register_type(_UiChecker())
    config = uvicorn.Config(
        create_app(ps), host="127.0.0.1", port=0, log_level="error"
    )
    server = uvicorn.Server(config)
    runner = threading.Thread(target=server.run, daemon=True)
    runner.start()
    deadline = time.monotonic() + 10.0
    while not server.started and time.monotonic() < deadline:
        time.sleep(0.02)
    assert server.started
    port = server.servers[0].sockets[0].getsockname()[1]
    base = f"http://127.0.0.1:{port}"

    seen: list[str] = []
    errors: list[Exception] = []
    stop = threading.Event()

    def consume() -> None:
        try:
            with httpx.Client(base_url=base, timeout=10.0) as client:
                with client.stream("GET", "/pools/ui-sse/events") as resp:
                    assert resp.headers["content-type"].startswith("text/event-stream")
                    for line in resp.iter_lines():
                        if line.startswith("data:"):
                            seen.append(line[5:].strip())
                        if stop.is_set() or any('"pool_created"' in e for e in seen):
                            break
        except Exception as exc:  # keep thread failures visible to the caller
            errors.append(exc)
        finally:
            stop.set()

    try:
        with httpx.Client(base_url=base) as client:
            client.post("/pools", json={"name": "ui-sse", "kind": "uifake"})
            thread = threading.Thread(target=consume, daemon=True)
            thread.start()
            deadline = time.monotonic() + 8.0
            while time.monotonic() < deadline and not any(
                '"pool_created"' in e for e in seen
            ):
                time.sleep(0.1)
            stop.set()
            thread.join(timeout=5.0)
        assert not errors, errors
        assert any('"pool_created"' in e for e in seen), seen
    finally:
        server.should_exit = True
        runner.join(timeout=10.0)
