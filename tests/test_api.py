"""REST API surface: pools, resources, leases, auth, idempotency."""

from __future__ import annotations

import json

from conftest import FakeChecker
from fastapi.testclient import TestClient

from purser import Purser
from purser.ui.server import create_app


def make_client(api_key: str | None = "admin-key", readonly_key: str | None = "ro-key"):
    ps = Purser(maintenance_enabled=False)
    ps.register_type(FakeChecker())
    app = create_app(ps, api_key=api_key, readonly_key=readonly_key)
    client = TestClient(app)
    return client, ps


def auth(key: str | None) -> dict[str, str]:
    return {"Authorization": f"Bearer {key}"} if key else {}


def test_health_and_ready():
    client, _ = make_client()
    assert client.get("/healthz").json() == {"status": "ok"}
    assert client.get("/readyz").json()["status"] == "ready"


def test_pool_and_resource_flow():
    client, ps = make_client()
    head = auth("admin-key")
    assert client.post("/pools", json={"name": "px", "kind": "fakecheck"}, headers=head).json()["pool"] == "px"
    records = [
        {"id": "p1", "host": "10.0.0.1", "port": 8080, "tags": ["US"]},
        {"id": "p2", "host": "10.0.0.2", "port": 8080, "tags": ["EU"]},
    ]
    rep = client.post(
        "/pools/px/resources",
        json={"records": records, "import_validate": "on_import"},
        headers={**head, "Idempotency-Key": "imp-1"},
    ).json()
    assert rep["imported"] == 2 and rep["errors"] == []

    # Same idempotency key replays the stored result instead of re-importing.
    rep2 = client.post(
        "/pools/px/resources",
        json={"records": records, "import_validate": "on_import"},
        headers={**head, "Idempotency-Key": "imp-1"},
    ).json()
    assert rep2 == rep
    assert client.get("/pools/px/resources", headers=auth("ro-key")).json()["counts"]["idle"] == 2

    lease = client.post(
        "/pools/px/leases",
        json={"consumer": "c", "target": "target.com", "timeout": 5},
        headers=head,
    ).json()["leases"][0]
    assert lease["resource_id"] in {"p1", "p2"}
    assert "host" in lease["payload"] and lease["payload"]["port"] == 8080
    token = lease["token"]
    assert client.get("/pools/px/leases", headers=auth("ro-key")).json()["leases"]

    released = client.request(
        "DELETE",
        f"/pools/px/leases/{token}",
        content=json.dumps({"outcome": "ok"}),
        headers={**head, "Content-Type": "application/json"},
    ).json()
    assert released["ok"] is True


def test_resource_actions_and_secrets():
    client, _ = make_client()
    head = auth("admin-key")
    client.post("/pools", json={"name": "acc", "kind": "account"}, headers=head)
    client.post(
        "/pools/acc/resources",
        json={"records": [{"id": "a1", "username": "u", "password": "topsecret"}]},
        headers=head,
    )
    masked = client.get("/pools/acc/resources/a1", headers=auth("ro-key")).json()
    assert set(masked["payload"].values()) == {"***"}
    secret = client.get(
        "/pools/acc/resources/a1", params={"include_secrets": "true"}, headers=head
    ).json()
    assert secret["payload"]["password"] == "topsecret"
    # Readonly key cannot fetch secrets when an admin key exists.
    assert (
        client.get(
            "/pools/acc/resources/a1", params={"include_secrets": "true"}, headers=auth("ro-key")
        ).status_code
        == 403
    )
    # Write action lifecycle.
    client.post("/pools/acc/resources/a1/validate", json={"reason": "check"}, headers=head)
    banned = client.post("/pools/acc/resources/a1/ban", json={"reason": "risk"}, headers=head).json()
    assert banned["state"] == "banned"
    unbanned = client.post("/pools/acc/resources/a1/unban", json={"reason": "ok"}, headers=head).json()
    assert unbanned["state"] == "validating" or unbanned["state"] == "idle"


def test_auth_enforced():
    client, _ = make_client()
    assert client.post("/pools", json={"name": "x", "kind": "proxy"}).status_code == 401
    assert (
        client.post("/pools", json={"name": "x", "kind": "proxy"}, headers=auth("ro-key")).status_code
        == 403
    )
    assert client.post("/pools", json={"name": "x", "kind": "proxy"}, headers=auth("admin-key")).status_code == 200
    assert client.get("/pools", headers=auth("ro-key")).status_code == 200
    assert client.get("/pools").status_code == 401  # keys configured => auth required


def test_auth_optional_without_keys():
    client, _ = make_client(api_key=None, readonly_key=None)
    assert client.post("/pools", json={"name": "x", "kind": "proxy"}).status_code == 200


def test_admin_state_rejects_bad_target():
    client, _ = make_client()
    head = auth("admin-key")
    client.post("/pools", json={"name": "px", "kind": "fakecheck"}, headers=head)
    client.post(
        "/pools/px/resources",
        json={
            "records": [{"id": "p1", "host": "10.0.0.1", "port": 8080}],
            "import_validate": "on_import",
        },
        headers=head,
    )
    resp = client.post(
        "/pools/px/resources/p1/admin-state",
        json={"target": "not-a-state", "reason": "x"},
        headers=head,
    )
    assert resp.status_code == 422


def test_patch_pool_config_hot_applies():
    client, ps = make_client()
    head = auth("admin-key")
    client.post("/pools", json={"name": "px", "kind": "fakecheck"}, headers=head)
    resp = client.patch(
        "/pools/px",
        json={"config": {"allocator": "round_robin", "background_interval": 5.0}},
        headers=head,
    )
    assert resp.status_code == 200
    pool = ps.get_pool("px")
    assert pool is not None
    assert pool.cfg.allocator == "round_robin"
    assert pool._allocator.name == "round_robin"
    # Invalid knob values are rejected with 422, not 500.
    bad = client.patch("/pools/px", json={"config": {"lease_ttl": -1}}, headers=head)
    assert bad.status_code == 422


def test_bad_lease_inputs_return_422():
    client, _ = make_client()
    head = auth("admin-key")
    client.post("/pools", json={"name": "px", "kind": "fakecheck"}, headers=head)
    client.post(
        "/pools/px/resources",
        json={
            "records": [{"id": "p1", "host": "10.0.0.1", "port": 8080}],
            "import_validate": "on_import",
        },
        headers=head,
    )
    assert client.post("/pools/px/leases", json={"count": "abc"}, headers=head).status_code == 422
    assert client.post("/pools/px/leases", json={"slots": 0}, headers=head).status_code == 422
    lease = client.post("/pools/px/leases", json={"consumer": "c"}, headers=head).json()["leases"][0]
    bad_outcome = client.request(
        "DELETE",
        f"/pools/px/leases/{lease['token']}",
        content=json.dumps({"outcome": "nope"}),
        headers={**head, "Content-Type": "application/json"},
    )
    assert bad_outcome.status_code == 422
    client.request(
        "DELETE",
        f"/pools/px/leases/{lease['token']}",
        content=json.dumps({"outcome": "ok"}),
        headers={**head, "Content-Type": "application/json"},
    )
