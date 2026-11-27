"""UI route table regressions after the router split."""

from __future__ import annotations

EXPECTED_PATHS = frozenset(
    {
        "/healthz",
        "/readyz",
        "/metrics",
        "/metrics/snapshot",
        "/reports/usage",
        "/backup",
        "/restore",
        "/",
        "/service",
        "/audit",
        "/audit/verify",
        "/config/schema",
        "/adapters",
        "/pools",
        "/pools/{pool_name}",
        "/pools/{pool_name}/resources",
        "/pools/{pool_name}/resources/{rid}",
        "/pools/{pool_name}/resources/{rid}/revoke",
        "/pools/{pool_name}/resources/{rid}/move",
        "/pools/{pool_name}/resources/{rid}/{action}",
        "/pools/{pool_name}/leases",
        "/pools/{pool_name}/leases/{token}",
        "/pools/{pool_name}/leases/{token}/renew",
        "/pools/{pool_name}/events",
        "/pools/{pool_name}/events/history",
        "/pools/{pool_name}/batches",
        "/pools/{pool_name}/validate-jobs",
        "/pools/{pool_name}/validate-jobs/{job_id}",
        "/pools/{pool_name}/validate-jobs/{job_id}/cancel",
        "/pools/{pool_name}/drain",
        "/pools/{pool_name}/maintenance/{task}",
        "/pools/{pool_name}/recall",
    }
)

ADMIN = {"Authorization": "Bearer admin-key"}
READONLY = {"Authorization": "Bearer ro-key"}


def test_openapi_route_table_matches_split(make_client) -> None:
    client, _ = make_client()
    spec = client.get("/openapi.json").json()
    assert set(spec["paths"]) == set(EXPECTED_PATHS)
    operations = sum(
        1
        for entry in spec["paths"].values()
        for method in entry
        if method in ("get", "post", "put", "patch", "delete")
    )
    assert operations == 41


def test_revoke_route_wins_over_action_catch_all(make_client) -> None:
    client, ps = make_client()
    created = client.post(
        "/pools", json={"name": "p", "kind": "fakecheck"}, headers=ADMIN
    )
    assert created.status_code == 200, created.text
    imported = client.post(
        "/pools/p/resources",
        json={"records": [{"id": "r-0", "host": "127.0.0.1", "port": 80}]},
        headers=ADMIN,
    )
    assert imported.status_code == 200, imported.text
    acquired = client.post(
        "/pools/p/leases", json={"rid": "r-0"}, headers=ADMIN
    )
    assert acquired.status_code == 200, acquired.text
    token = acquired.json()["leases"][0]["token"]

    revoked = client.post(
        "/pools/p/resources/r-0/revoke",
        json={"reason": "route check"},
        headers=ADMIN,
    )
    assert revoked.status_code == 200, revoked.text
    assert revoked.json() == {"ok": True, "revoked_token": token}
    assert client.get(f"/pools/p/leases/{token}", headers=ADMIN).status_code == 404

    unknown = client.post("/pools/p/resources/r-0/nope", headers=ADMIN)
    assert unknown.status_code == 404
    assert unknown.json()["detail"] == "unknown action 'nope'"

    assert any(
        entry.action == "force_revoke" for entry in ps.audit.entries(after_seq=0)
    )


def test_unauthorized_and_readonly(make_client) -> None:
    client, _ = make_client()
    assert client.get("/pools").status_code == 401
    assert client.get("/pools", headers=READONLY).status_code == 200
    denied = client.post(
        "/pools", json={"name": "p", "kind": "fakecheck"}, headers=READONLY
    )
    assert denied.status_code == 403
    assert denied.json()["detail"] == "readonly role cannot write"
