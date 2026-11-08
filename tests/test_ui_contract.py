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
import pytest
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


@pytest.fixture
def client() -> TestClient:
    ps = Purser(maintenance_enabled=False)
    ps.register_type(_UiChecker())
    # Enter the context so the app lifespan runs (ps.start()).
    with TestClient(create_app(ps)) as client:
        yield client


def test_console_served_and_endpoints_exist(client: TestClient):
    resp = client.get("/")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]
    assert "Purser" in resp.text
    # The DOM ids the JS drives and the round-trip markers it depends on.
    for marker in (
        'id="pools"',
        'id="kpis"',
        'id="resRows"',
        'id="evlist"',
        'id="addResourceBtn"',
        'id="addModal"',
        'id="addAdvanced"',
        'id="auditModal"',
        'id="auditOlderBtn"',
        'id="backupBtn"',
        'id="backupModal"',
        'id="restoreFile"',
        'id="restoreBtn"',
        'id="reportBtn"',
        'id="reportModal"',
        'id="jobModal"',
        'id="jobCancelBtn"',
        "data-job",
        'id="batchList"',
        'id="resPager"',
        'id="fSort"',
        'qs.set("sort"',
        'id="leaseOutcome"',
        'id="leaseIdem"',
        'id="leaseFilterConsumer"',
        'id="leaseFilterRid"',
        'id="leaseModal"',
        'id="leaseModalBody"',
        'id="metricBody"',
        'id="metricMeta"',
        'id="poolAdvanced"',
        'id="poolDeploy"',
        'id="cfgStorage"',
        'id="cfgCoordination"',
        'id="cfgSecrets"',
        'id="cfgWHealth"',
        'id="cfgQBase"',
        'id="cfgTotalQuota"',
        'id="maintTask"',
        'qs.set("tail"',
        "EventSource",
        'localStorage.getItem("purser_key")',
        "fetch(path",
        "loadOlderAudit",
        "releaseConsumerLeases",
        "clearLeaseFilters",
        "openLeaseDetail",
        "data-leaseview",
        "loadMetrics",
        '"/metrics/snapshot"',
        "downloadBackup",
        "restoreBackup",
        "purser-backup",
        "openReport",
        "openValidateJob",
        "/reports/usage",
        "/validate-jobs",
        "cancel-drain",
        "/service",
        "/audit/verify",
        "events/history",
        "/recall",
        "/batches",
    ):
        assert marker in resp.text, marker
    # Bulk import is intentionally out of the console for now; resources are
    # added through the schema-driven modal instead.
    assert 'id="importText"' not in resp.text
    assert "importResources" not in resp.text


def test_console_fetch_targets_registered(client: TestClient):
    methods_by_path: dict[str, set[str]] = {}
    for route in client.app.routes:
        methods_by_path.setdefault(route.path, set()).update(
            set(getattr(route, "methods", set()) or set())
        )
    expected = [
        ("GET", "/service"),
        ("GET", "/audit"),
        ("GET", "/audit/verify"),
        ("GET", "/metrics/snapshot"),
        ("GET", "/backup"),
        ("POST", "/restore"),
        ("GET", "/reports/usage"),
        ("GET", "/types"),
        ("GET", "/pools"),
        ("POST", "/pools"),
        ("PATCH", "/pools/{pool_name}"),
        ("DELETE", "/pools/{pool_name}"),
        ("GET", "/pools/{pool_name}"),
        ("GET", "/pools/{pool_name}/resources"),
        ("POST", "/pools/{pool_name}/resources"),
        ("GET", "/pools/{pool_name}/resources/{rid}"),
        ("PUT", "/pools/{pool_name}/resources/{rid}"),
        ("DELETE", "/pools/{pool_name}/resources/{rid}"),
        ("POST", "/pools/{pool_name}/resources/{rid}/revoke"),
        ("POST", "/pools/{pool_name}/resources/{rid}/move"),
        ("POST", "/pools/{pool_name}/resources/{rid}/{action}"),
        ("POST", "/pools/{pool_name}/leases"),
        ("GET", "/pools/{pool_name}/leases"),
        ("DELETE", "/pools/{pool_name}/leases"),
        ("GET", "/pools/{pool_name}/leases/{token}"),
        ("POST", "/pools/{pool_name}/leases/{token}/renew"),
        ("DELETE", "/pools/{pool_name}/leases/{token}"),
        ("GET", "/pools/{pool_name}/events"),
        ("GET", "/pools/{pool_name}/events/history"),
        ("GET", "/pools/{pool_name}/batches"),
        ("GET", "/pools/{pool_name}/validate-jobs"),
        ("GET", "/pools/{pool_name}/validate-jobs/{job_id}"),
        ("POST", "/pools/{pool_name}/validate-jobs/{job_id}/cancel"),
        ("POST", "/pools/{pool_name}/drain"),
        ("POST", "/pools/{pool_name}/maintenance/{task}"),
        ("POST", "/pools/{pool_name}/recall"),
    ]
    for method, path in expected:
        assert path in methods_by_path, f"missing route {path}"
        assert method in methods_by_path[path], (method, path)


def test_types_schema_drives_resource_forms(client: TestClient):
    """The add-resource modal is generated from this payload, so every field
    marker (type/required/secret/default) has to be present and JSON-safe."""

    types = {item["kind"]: item for item in client.get("/types").json()["types"]}
    assert {"proxy", "account", "uifake"} <= set(types)

    proxy = types["proxy"]
    assert proxy["resource_class"] == "ProxyResource"
    assert proxy["default_concurrency"] == 1
    fields = {f["name"]: f for f in proxy["fields"]}
    assert list(fields) == [
        "host",
        "port",
        "protocol",
        "username",
        "password",
        "country",
        "province",
        "city",
        "district",
    ]
    assert fields["host"]["required"] is True
    assert fields["port"]["required"] is True
    assert fields["port"]["type"] == "int"
    assert fields["protocol"]["default"] == "http"
    assert fields["password"]["secret"] is True
    assert fields["host"]["secret"] is False

    account = {f["name"]: f for f in types["account"]["fields"]}
    for secret in ("password", "cookie", "access_token", "refresh_token"):
        assert account[secret]["secret"] is True
    assert all(f["required"] is False for f in types["account"]["fields"])

    # A plugin whose resource class declares no typed fields exposes none.
    assert types["uifake"]["fields"] == []


def test_console_flow_response_shapes(client: TestClient):
    """Play the console's own call sequence and pin every JSON field the JS
    reads: pool stats, resource rows, lease rows, actions and errors."""


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
    idem = {"Idempotency-Key": "ui-lease-1"}
    replay_a = client.post(
        "/pools/ui-demo/leases", json={"consumer": "ui-idem", "timeout": 1.0}, headers=idem
    ).json()
    replay_b = client.post(
        "/pools/ui-demo/leases", json={"consumer": "ui-idem", "timeout": 1.0}, headers=idem
    ).json()
    assert replay_b["replayed"] is True
    assert replay_b["leases"][0]["token"] == replay_a["leases"][0]["token"]
    held = client.get(f"/pools/ui-demo/leases/{replay_a['leases'][0]['token']}").json()[
        "lease"
    ]
    assert held["consumer"] == "ui-idem"
    for key in (
        "token",
        "resource_id",
        "consumer",
        "target",
        "slots",
        "priority",
        "use_limit",
        "used",
        "started_at",
        "expires_at",
        "outcome",
        "trace_id",
        "payload",
    ):
        assert key in held, key
    assert client.get("/pools/ui-demo/leases/not-a-token").status_code == 404

    # The metrics card reads counters/histograms filtered by the pool prefix.
    snapshot = client.get("/metrics/snapshot").json()
    assert isinstance(snapshot["counters"], dict)
    assert isinstance(snapshot["histograms"], dict)
    assert snapshot["counters"].get("pool.ui-demo.acquire_total", 0) >= 2
    filtered = client.get(
        "/pools/ui-demo/leases", params={"consumer": "ui-idem"}
    ).json()
    assert filtered["count"] == 1
    assert filtered["leases"][0]["token"] == replay_a["leases"][0]["token"]
    assert (
        client.get(
            "/pools/ui-demo/leases", params={"consumer": "nobody"}
        ).json()["count"]
        == 0
    )
    by_rid = client.get(
        "/pools/ui-demo/leases",
        params={"resource_id": replay_a["leases"][0]["resource_id"]},
    ).json()
    assert by_rid["count"] == 1
    bulk = client.request(
        "DELETE", "/pools/ui-demo/leases", params={"consumer": "ui-idem"}
    ).json()
    assert bulk["count"] == 1 and bulk["outcome"] == "ok"
    assert client.get("/pools/ui-demo/leases").json()["leases"] == []

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


def test_console_extended_console_flow(client: TestClient):
    """Pin the response shapes behind the console's newer controls: pagination,
    scheduling edits, batches/recall, lease renewal, drain, maintenance, audit
    and event replay."""

    service = client.get("/service").json()
    for key in ("version", "uptime_s", "storage", "coordination", "pools", "types", "maintenance"):
        assert key in service, key

    for name in ("ui-ext", "ui-ext2"):
        body: dict[str, Any] = {"name": name, "kind": "uifake"}
        if name == "ui-ext":
            body["config"] = {
                "storage": "memory",
                "coordination": "memory",
                "secret_fields": ["token"],
                "weights": {"health": 1.5, "age": 0.25},
                "quarantine": {"base": 10.0, "max_failures": 2},
                "total_quota": 4,
            }
        created = client.post("/pools", json=body)
        assert created.status_code == 200, created.text

    cfg = client.get("/pools/ui-ext").json()["config"]
    assert cfg["secret_fields"] == ["token"]
    assert cfg["weights"]["health"] == 1.5
    assert cfg["quarantine"]["max_failures"] == 2
    assert cfg["total_quota"] == 4

    patched = client.patch(
        "/pools/ui-ext",
        json={
            "config": {
                "weights": {"health": 0.7},
                "quarantine": {"cap": 90.0},
                "unban_after": 600.0,
                "cooldown_consumer_only": True,
                "total_quota": 2,
            }
        },
    )
    assert patched.status_code == 200, patched.text
    patched_cfg = patched.json()["config"]
    assert patched_cfg["unban_after"] == 600.0
    assert patched_cfg["cooldown_consumer_only"] is True
    assert patched_cfg["quarantine"]["cap"] == 90.0
    assert patched_cfg["total_quota"] == 2

    report = client.post(
        "/pools/ui-ext/resources",
        json={"records": [{"id": "x1"}, {"id": "x2"}], "import_validate": "on_import"},
    ).json()
    assert report["imported"] == 2
    batch_id = report["batch_id"]

    page = client.get("/pools/ui-ext/resources", params={"limit": 1, "offset": 1}).json()
    assert page["total"] == 2 and page["limit"] == 1 and page["offset"] == 1
    assert len(page["resources"]) == 1

    batches = client.get("/pools/ui-ext/batches").json()["batches"]
    assert len(batches) == 1
    batch = batches[0]
    for key in ("batch_id", "total", "states", "source", "created_at"):
        assert key in batch, key
    assert batch["batch_id"] == batch_id and batch["total"] == 2

    detail = client.get("/pools/ui-ext/resources/x1").json()
    for key in (
        "expires_at",
        "next_available_at",
        "mutex_group",
        "schedule_window",
        "usage_quota",
        "quota",
        "used_count",
        "health",
    ):
        assert key in detail, key

    updated = client.put(
        "/pools/ui-ext/resources/x1",
        json={
            "usage_quota": 5,
            "expires_at": time.time() + 3600,
            "mutex_group": "grp",
            "schedule_window": ["09:00", "17:00"],
        },
    )
    assert updated.status_code == 200
    payload = updated.json()["resource"]
    assert payload["usage_quota"] == 5
    assert payload["mutex_group"] == "grp"
    assert payload["schedule_window"] == ["09:00", "17:00"]

    cleared = client.put(
        "/pools/ui-ext/resources/x1",
        json={"usage_quota": None, "expires_at": None, "mutex_group": None, "schedule_window": None},
    ).json()["resource"]
    assert cleared["usage_quota"] is None
    assert cleared["expires_at"] is None
    assert cleared["mutex_group"] is None
    assert cleared["schedule_window"] is None

    recalled = client.post(
        "/pools/ui-ext/recall", json={"batch_id": batch_id, "action": "requalify"}
    ).json()
    assert recalled["batch_id"] == batch_id and recalled["processed"] == 2
    assert recalled["errors"] == []

    moved = client.post(
        "/pools/ui-ext/resources/x1/move", json={"target_pool": "ui-ext2"}
    ).json()
    assert moved["target_pool"] == "ui-ext2"
    assert client.get("/pools/ui-ext2/resources/x1").status_code == 200
    assert client.get("/pools/ui-ext/resources/x1").status_code == 404

    lease = client.post(
        "/pools/ui-ext/leases", json={"consumer": "ui-ext", "timeout": 1.0}
    ).json()["leases"][0]
    renewed = client.post(f"/pools/ui-ext/leases/{lease['token']}/renew")
    assert renewed.status_code == 200 and renewed.json()["ok"] is True
    assert renewed.json()["expires_at"] >= lease["expires_at"]
    released = client.request(
        "DELETE", f"/pools/ui-ext/leases/{lease['token']}", json={"outcome": "ok", "used": 1}
    )
    assert released.status_code == 200

    drained = client.post("/pools/ui-ext/drain", json={}).json()
    assert drained["ok"] is True and drained["drained"] == 1
    assert drained["states"] == ["active", "full", "idle"]
    assert drained["errors"] == []

    resumed = client.post("/pools/ui-ext/resources/x2/cancel-drain", json={})
    assert resumed.status_code == 200 and resumed.json()["state"] == "idle"

    maint = client.post("/pools/ui-ext/maintenance/quota_reset")
    assert maint.status_code == 200
    assert "quota_reset" in maint.json()["result"]

    audit = client.get("/audit", params={"pool": "ui-ext", "limit": 50}).json()
    assert audit["entries"] and audit["cursor"] == audit["entries"][-1]["seq"]
    assert isinstance(audit["has_older"], bool)
    actions = {entry["action"] for entry in audit["entries"]}
    assert {"import_resources", "update_resource", "move_resource"} <= actions
    latest = client.get(
        "/audit", params={"pool": "ui-ext", "limit": 5, "tail": "true"}
    ).json()
    full = client.get("/audit", params={"pool": "ui-ext", "limit": 1000}).json()
    assert len(latest["entries"]) == 5
    assert latest["entries"][-1]["seq"] == full["entries"][-1]["seq"]
    older = client.get(
        "/audit",
        params={
            "pool": "ui-ext",
            "limit": 5,
            "before_seq": latest["entries"][0]["seq"],
        },
    ).json()
    assert older["entries"][-1]["seq"] < latest["entries"][0]["seq"]
    assert older["has_older"] is (len(full["entries"]) > 10)
    verified = client.get("/audit/verify").json()
    assert verified["ok"] is True and verified["entries"] >= len(audit["entries"])

    history = client.get("/pools/ui-ext/events/history", params={"limit": 10}).json()
    assert isinstance(history["events"], list) and history["events"]
    first = history["events"][0]
    for key in ("seq", "ts", "type", "pool", "rid", "data"):
        assert key in first, key
    assert all(event["pool"] == "ui-ext" for event in history["events"])

    metrics = client.get("/metrics/snapshot").json()
    assert isinstance(metrics, dict) and metrics

    # Usage report shapes consumed by the console 报表 modal.
    usage = client.get(
        "/reports/usage", params={"pool": "ui-ext", "group_by": "consumer"}
    ).json()
    for key in (
        "window",
        "group_by",
        "totals",
        "groups",
        "groups_total",
        "truncated",
        "active",
        "pools",
    ):
        assert key in usage, key
    assert usage["group_by"] == "consumer"
    assert usage["totals"]["releases"] >= 1
    assert usage["groups"] and usage["groups"][0]["key"] == "ui-ext"
    for key in ("key", "releases", "used", "slots", "outcomes"):
        assert key in usage["groups"][0], key
    assert usage["pools"][0]["name"] == "ui-ext"

    # Batch validation jobs served to the 验活进度 modal.
    batch_report = client.post(
        "/pools/ui-ext/resources",
        json={"records": [{"id": "x3"}], "import_validate": "batch"},
    ).json()
    job_id = batch_report["batch_id"]
    deadline = time.monotonic() + 5.0
    while True:
        job = client.get(f"/pools/ui-ext/validate-jobs/{job_id}").json()
        if job["status"] in ("completed", "cancelled"):
            break
        if time.monotonic() > deadline:
            raise AssertionError(job)
        time.sleep(0.01)
    for key in (
        "id",
        "pool",
        "status",
        "total",
        "done",
        "alive",
        "failed",
        "skipped",
        "pending",
        "errors",
        "created_at",
        "updated_at",
        "operator",
    ):
        assert key in job, key
    assert job["id"] == job_id and job["status"] == "completed"
    assert job["alive"] == 1 and job["pending"] == 0
    listed = client.get("/pools/ui-ext/validate-jobs").json()["jobs"]
    assert listed[0]["id"] == job_id

    deleted = client.delete("/pools/ui-ext", params={"force": "true"})
    assert deleted.status_code == 200 and deleted.json()["ok"] is True


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
