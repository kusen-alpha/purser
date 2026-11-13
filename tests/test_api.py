"""REST API surface: pools, resources, leases, auth, idempotency."""

from __future__ import annotations

import json
import time

from conftest import FakeChecker


def auth(key: str | None) -> dict[str, str]:
    return {"Authorization": f"Bearer {key}"} if key else {}


def test_backup_restore_roundtrip(make_client):
    src, _ = make_client()
    head = auth("admin-key")
    assert src.get("/backup", headers=auth("ro-key")).status_code == 403
    assert src.get("/backup").status_code == 401

    src.post(
        "/pools",
        json={
            "name": "p1",
            "kind": "fakecheck",
            "config": {"weights": {"health": 1.5}, "quarantine": {"base": 7.0}},
        },
        headers=head,
    )
    src.post(
        "/pools/p1/resources",
        json={
            "records": [
                {
                    "id": "r1",
                    "host": "10.0.0.1",
                    "port": 80,
                    "tags": ["us"],
                    "usage_quota": 5,
                    "mutex_group": "g",
                },
                {"id": "r2", "host": "10.0.0.2", "port": 81},
            ],
            "import_validate": "on_import",
        },
        headers=head,
    )
    src.post(
        "/pools/p1/leases",
        json={"consumer": "c", "resource_id": "r2", "timeout": 5},
        headers=head,
    )
    src.post("/pools/p1/resources/r1/ban", json={"reason": "risk"}, headers=head)

    backup = src.get("/backup", headers=head).json()
    assert backup["format"] == "purser-backup" and backup["version"] == 1
    assert [pool["name"] for pool in backup["pools"]] == ["p1"]
    entry = backup["pools"][0]
    exported = {item["id"]: item for item in entry["resources"]}
    assert exported["r1"]["state"] == "banned"
    assert exported["r2"]["state"] in {"active", "full"}
    assert exported["r2"]["payload"]["host"] == "10.0.0.2"
    assert entry["leases"]  # exported for reference, ignored on restore
    assert entry["leases"][0]["resource_id"] == "r2"
    assert entry["config"]["weights"]["health"] == 1.5

    dst, dst_ps = make_client()
    assert dst.post("/restore", json=backup, headers=auth("ro-key")).status_code == 403
    report = dst.post("/restore?conflict=skip", json=backup, headers=head).json()
    assert report["ok"] is True and report["created_pools"] == ["p1"]
    assert report["imported"] == 2 and report["normalized"] == 1
    assert report["leases_ignored"] == 1

    resources = dst.get("/pools/p1/resources?limit=100", headers=head).json()[
        "resources"
    ]
    restored = {item["id"]: item for item in resources}
    assert restored["r1"]["state"] == "banned"
    assert restored["r2"]["state"] == "idle" and restored["r2"]["used_slots"] == 0
    detail = dst.get(
        "/pools/p1/resources/r1",
        params={"include_secrets": "true"},
        headers=head,
    ).json()
    assert detail["payload"]["host"] == "10.0.0.1"
    assert detail["tags"] == ["us"]
    assert detail["usage_quota"] == 5 and detail["mutex_group"] == "g"
    assert dst.get("/pools/p1", headers=head).json()["config"]["weights"]["health"] == 1.5
    assert dst.get("/pools/p1/leases", headers=head).json()["leases"] == []

    again = dst.post("/restore?conflict=skip", json=backup, headers=head).json()
    assert again["skipped"] == 2 and again["updated"] == 0
    overwritten = dst.post(
        "/restore",
        params={"conflict": "overwrite"},
        json=backup,
        headers={**head, "Idempotency-Key": "rst-1"},
    ).json()
    assert overwritten["updated"] == 2
    replay = dst.post(
        "/restore",
        params={"conflict": "overwrite"},
        json=backup,
        headers={**head, "Idempotency-Key": "rst-1"},
    ).json()
    assert replay["replayed"] is True

    assert (
        dst.post("/restore", params={"conflict": "merge"}, json=backup, headers=head).status_code
        == 422
    )
    assert (
        dst.post("/restore", json={"format": "nope", "pools": []}, headers=head).status_code
        == 422
    )
    audit = dst.get("/audit", params={"pool": "p1", "limit": 100}, headers=head).json()
    assert "restore" in {item["action"] for item in audit["entries"]}
    backup_audit = src.get("/audit", params={"action": "backup"}, headers=head).json()
    assert backup_audit["entries"][0]["detail"]["resources"] == 2
    assert dst_ps.get_pool("p1") is not None


def test_health_and_ready(make_client):
    client, _ = make_client()
    assert client.get("/healthz").json() == {"status": "ok"}
    assert client.get("/readyz").json()["status"] == "ready"


def test_pool_and_resource_flow(make_client):
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
    assert {entry["id"] for entry in rep["validation"]} == {"p1", "p2"}
    assert all(entry["alive"] is True for entry in rep["validation"])

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


def test_resource_actions_and_secrets(make_client):
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


def test_auth_enforced(make_client):
    client, _ = make_client()
    assert client.post("/pools", json={"name": "x", "kind": "proxy"}).status_code == 401
    assert (
        client.post("/pools", json={"name": "x", "kind": "proxy"}, headers=auth("ro-key")).status_code
        == 403
    )
    assert client.post("/pools", json={"name": "x", "kind": "proxy"}, headers=auth("admin-key")).status_code == 200
    assert client.get("/pools", headers=auth("ro-key")).status_code == 200
    assert client.get("/pools").status_code == 401  # keys configured => auth required


def test_auth_optional_without_keys(make_client):
    client, _ = make_client(api_key=None, readonly_key=None)
    assert client.post("/pools", json={"name": "x", "kind": "proxy"}).status_code == 200


def test_admin_state_rejects_bad_target(make_client):
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


def test_patch_pool_config_hot_applies(make_client):
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


def test_bad_lease_inputs_return_422(make_client):
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


def test_resource_query_pagination_search_and_batches(make_client):
    client, _ = make_client()
    head = auth("admin-key")
    client.post("/pools", json={"name": "px", "kind": "fakecheck"}, headers=head)
    records = [
        {"id": f"p{i:03d}", "tags": ["us"] if i % 2 else ["eu"]}
        for i in range(12)
    ]
    rep = client.post(
        "/pools/px/resources",
        json={
            "records": records,
            "batch_id": "batch-a",
            "source": "ui",
            "import_validate": "on_import",
        },
        headers=head,
    ).json()
    assert rep["batch_id"] == "batch-a" and rep["total"] == 12

    page = client.get(
        "/pools/px/resources", params={"limit": 5, "offset": 5}, headers=head
    ).json()
    assert [r["id"] for r in page["resources"]] == [
        "p005",
        "p006",
        "p007",
        "p008",
        "p009",
    ]
    assert page["total"] == 12 and page["limit"] == 5 and page["offset"] == 5

    # Search applies before pagination and reports the filtered total.
    found = client.get(
        "/pools/px/resources", params={"q": "p011", "limit": 5}, headers=head
    ).json()
    assert [r["id"] for r in found["resources"]] == ["p011"]
    assert found["total"] == 1

    by_source = client.get(
        "/pools/px/resources", params={"q": "ui"}, headers=head
    ).json()
    assert by_source["total"] == 12

    tags = client.get(
        "/pools/px/resources", params={"tag": "us"}, headers=head
    ).json()
    assert tags["total"] == 6
    both = client.get(
        "/pools/px/resources", params={"tag": "us,eu"}, headers=head
    ).json()
    assert both["total"] == 0  # tags combine as AND

    batches = client.get("/pools/px/batches", headers=head).json()["batches"]
    assert len(batches) == 1
    assert batches[0]["batch_id"] == "batch-a"
    assert batches[0]["total"] == 12
    assert batches[0]["states"] == {"idle": 12}
    assert batches[0]["source"] == "ui"
    assert isinstance(batches[0]["created_at"], float)


def test_resource_query_sort_health_recent_and_validation(make_client):
    client, _ = make_client(checker=FakeChecker(fail_until=1))
    head = auth("admin-key")
    client.post("/pools", json={"name": "ps", "kind": "fakecheck"}, headers=head)
    client.post(
        "/pools/ps/resources",
        json={
            "records": [{"id": "a"}, {"id": "b"}, {"id": "c"}],
            "import_validate": "none",
        },
        headers=head,
    )
    # One programmed failure, then two passes: a drops to 0.2, b/c rise to 0.8.
    for rid in ("a", "b", "c"):
        assert client.post(f"/pools/ps/resources/{rid}/validate", headers=head).status_code == 200

    desc = client.get("/pools/ps/resources", params={"sort": "health"}, headers=head).json()
    assert [r["id"] for r in desc["resources"]] == ["b", "c", "a"]
    assert desc["sort"] == "health" and desc["order"] == "desc"
    asc = client.get(
        "/pools/ps/resources", params={"sort": "health", "order": "asc"}, headers=head
    ).json()
    assert [r["id"] for r in asc["resources"]] == ["a", "b", "c"]
    # Sorting happens before pagination.
    page = client.get(
        "/pools/ps/resources", params={"sort": "health", "limit": 2}, headers=head
    ).json()
    assert [r["id"] for r in page["resources"]] == ["b", "c"]

    # last_used_at is stamped on acquire; b was used before c, a never.
    for rid in ("b", "c"):
        leased = client.post(
            "/pools/ps/leases", json={"resource_id": rid}, headers=head
        ).json()
        assert leased["count"] == 1
        client.delete(f"/pools/ps/leases/{leased['leases'][0]['token']}", headers=head)
        time.sleep(0.01)

    recent = client.get(
        "/pools/ps/resources", params={"sort": "recent"}, headers=head
    ).json()
    assert [r["id"] for r in recent["resources"]] == ["c", "b", "a"]
    assert recent["sort"] == "recent" and recent["order"] == "desc"
    stale = client.get(
        "/pools/ps/resources", params={"sort": "recent", "order": "asc"}, headers=head
    ).json()
    assert [r["id"] for r in stale["resources"]] == ["a", "b", "c"]

    # Sort composes with filters: the default echo is id/asc.
    tagged = client.get(
        "/pools/ps/resources", params={"sort": "id", "order": "desc", "limit": 2}, headers=head
    ).json()
    assert [r["id"] for r in tagged["resources"]] == ["c", "b"]
    assert tagged["sort"] == "id" and tagged["order"] == "desc"
    default = client.get("/pools/ps/resources", headers=head).json()
    assert default["sort"] == "id" and default["order"] == "asc"
    assert [r["id"] for r in default["resources"]] == ["a", "b", "c"]

    for params in ({"sort": "score"}, {"sort": "health", "order": "sideways"}):
        assert client.get("/pools/ps/resources", params=params, headers=head).status_code == 422


def test_import_scheduling_fields_and_update_clears(make_client):
    client, _ = make_client()
    head = auth("admin-key")
    client.post("/pools", json={"name": "px", "kind": "fakecheck"}, headers=head)
    client.post(
        "/pools/px/resources",
        json={
            "records": [
                {
                    "id": "r1",
                    "host": "10.0.0.1",
                    "port": 8080,
                    "max_slots": 3,
                    "usage_quota": 5,
                    "expires_at": 4102444800.0,
                    "mutex_group": "m1",
                    "schedule_window": ["00:00", "23:59"],
                }
            ],
            "source": "ui",
        },
        headers=head,
    )
    res = client.get("/pools/px/resources/r1", headers=head).json()
    assert res["max_slots"] == 3
    assert res["usage_quota"] == 5
    assert res["expires_at"] == 4102444800.0
    assert res["mutex_group"] == "m1"
    assert res["schedule_window"] == ["00:00", "23:59"]
    assert res["meta"]["source"] == "ui"

    updated = client.put(
        "/pools/px/resources/r1",
        json={
            "max_slots": 2,
            "tags": ["a", "b"],
            "usage_quota": None,
            "expires_at": None,
            "mutex_group": None,
            "schedule_window": None,
            "reason": "reset",
        },
        headers=head,
    )
    assert updated.status_code == 200
    res = updated.json()["resource"]
    assert res["max_slots"] == 2 and res["tags"] == ["a", "b"]
    assert res["usage_quota"] is None
    assert res["expires_at"] is None
    assert res["mutex_group"] is None
    assert res["schedule_window"] is None

    bad_window = client.put(
        "/pools/px/resources/r1",
        json={"schedule_window": ["25:00", "26:00"]},
        headers=head,
    )
    assert bad_window.status_code == 422


def test_move_resource_between_same_kind_pools(make_client):
    client, _ = make_client()
    head = auth("admin-key")
    for name in ("src", "dst"):
        client.post("/pools", json={"name": name, "kind": "fakecheck"}, headers=head)
    client.post("/pools", json={"name": "acc", "kind": "account"}, headers=head)
    client.post(
        "/pools/src/resources",
        json={"records": [{"id": "r1", "host": "10.0.0.1", "port": 8080}]},
        headers=head,
    )

    moved = client.post(
        "/pools/src/resources/r1/move",
        json={"target_pool": "dst", "reason": "rebalance"},
        headers=head,
    )
    assert moved.status_code == 200
    assert moved.json()["target_pool"] == "dst"
    assert client.get("/pools/src/resources/r1", headers=head).status_code == 404
    assert client.get("/pools/dst/resources/r1", headers=head).status_code == 200

    cross_kind = client.post(
        "/pools/dst/resources/r1/move",
        json={"target_pool": "acc"},
        headers=head,
    )
    assert cross_kind.status_code == 409
    same = client.post(
        "/pools/dst/resources/r1/move",
        json={"target_pool": "dst"},
        headers=head,
    )
    assert same.status_code == 422


def test_pool_delete_requires_force_and_no_active_leases(make_client):
    client, ps = make_client()
    head = auth("admin-key")
    client.post("/pools", json={"name": "px", "kind": "fakecheck"}, headers=head)
    client.post(
        "/pools/px/resources",
        json={
            "records": [{"id": "r1", "host": "10.0.0.1", "port": 8080}],
            "import_validate": "on_import",
        },
        headers=head,
    )
    assert client.delete("/pools/px", headers=head).status_code == 409

    lease = client.post(
        "/pools/px/leases", json={"consumer": "c"}, headers=head
    ).json()["leases"][0]
    assert (
        client.delete(
            "/pools/px", params={"force": "true"}, headers=head
        ).status_code
        == 409
    )
    client.request(
        "DELETE",
        f"/pools/px/leases/{lease['token']}",
        content=json.dumps({"outcome": "ok"}),
        headers={**head, "Content-Type": "application/json"},
    )

    removed = client.delete("/pools/px", params={"force": "true"}, headers=head)
    assert removed.status_code == 200
    assert removed.json()["removed"] == 1
    assert ps.get_pool("px") is None
    assert client.get("/pools/px", headers=head).status_code == 404


def test_audit_endpoints_and_event_history(make_client):
    client, _ = make_client()
    head = auth("admin-key")
    client.post("/pools", json={"name": "px", "kind": "fakecheck"}, headers=head)
    client.post(
        "/pools/px/resources",
        json={"records": [{"id": "r1", "host": "10.0.0.1", "port": 8080}]},
        headers=head,
    )
    client.post("/pools/px/resources/r1/validate", json={"reason": "ui"}, headers=head)

    audit = client.get("/audit", params={"limit": 50}, headers=head).json()
    actions = [entry["action"] for entry in audit["entries"]]
    assert "create_pool" in actions and "validate" in actions
    assert audit["cursor"] == audit["entries"][-1]["seq"]

    filtered = client.get(
        "/audit", params={"pool": "px", "action": "create_pool"}, headers=head
    ).json()["entries"]
    assert len(filtered) == 1 and filtered[0]["pool"] == "px"

    verify = client.get("/audit/verify", headers=head).json()
    assert verify["ok"] is True and verify["entries"] >= 2

    history = client.get(
        "/pools/px/events/history", params={"limit": 50}, headers=head
    ).json()
    kinds = {event["type"] for event in history["events"]}
    assert "pool_created" in kinds and "validate_ok" in kinds
    assert history["cursor"] == history["events"][-1]["seq"]

    imports = client.get(
        "/pools/px/events/history",
        params={"type": "import_finished", "limit": 5},
        headers=head,
    ).json()["events"]
    assert imports and all(event["type"] == "import_finished" for event in imports)


def test_pool_config_advanced_knobs_and_immutables(make_client):
    client, _ = make_client()
    head = auth("admin-key")
    created = client.post(
        "/pools",
        json={
            "name": "pc",
            "kind": "fakecheck",
            "config": {
                "storage": "memory",
                "coordination": "memory",
                "secret_fields": ["token"],
                "weights": {"health": 2.0, "age": 0.2},
                "quarantine": {"base": 5.0, "max_failures": 2},
                "quota_limit": 3,
            },
        },
        headers=head,
    )
    assert created.status_code == 200, created.text
    cfg = client.get("/pools/pc", headers=head).json()["config"]
    assert cfg["secret_fields"] == ["token"]
    assert cfg["weights"]["health"] == 2.0
    assert cfg["quarantine"]["base"] == 5.0
    assert cfg["quota_limit"] == 3

    patched = client.patch(
        "/pools/pc",
        json={
            "config": {
                "weights": {"health": 0.5},
                "quarantine": {"cap": 60.0},
                "unban_after": 120.0,
                "quota_limit": 3,
            }
        },
        headers=head,
    )
    assert patched.status_code == 200, patched.text
    assert patched.json()["config"]["unban_after"] == 120.0

    # A config round-tripped from stats() may repeat immutable knobs verbatim.
    roundtrip = client.patch("/pools/pc", json={"config": cfg}, headers=head)
    assert roundtrip.status_code == 200, roundtrip.text

    for bad in (
        {"secret_fields": ["other"]},
        {"event_queue_size": 7},
        {"storage": "sqlite"},
    ):
        resp = client.patch("/pools/pc", json={"config": bad}, headers=head)
        assert resp.status_code == 422, (bad, resp.text)
    unknown = client.patch(
        "/pools/pc", json={"config": {"weights": {"bogus": 1}}}, headers=head
    )
    assert unknown.status_code == 422


def test_audit_and_event_cursor_pagination_keeps_backlog(make_client):
    """Regression: a forward page used to return the *newest* ``limit`` items,
    silently dropping the backlog behind the cursor."""

    client, ps = make_client()
    head = auth("admin-key")
    client.post("/pools", json={"name": "px", "kind": "fakecheck"}, headers=head)
    for i in range(300):
        ps.bus.publish("probe_event", "px", rid=f"r{i}", data={"i": i})
        ps.audit.record(operator="t", action="probe", pool="px", detail={"i": i})

    seen: list[int] = []
    cursor = 0
    while True:
        page = client.get(
            "/pools/px/events/history",
            params={"after_seq": cursor, "limit": 200},
            headers=head,
        ).json()
        if not page["events"]:
            break
        seqs = [event["seq"] for event in page["events"]]
        assert seqs[0] == cursor + 1
        assert seqs == sorted(seqs)
        seen.extend(seqs)
        cursor = page["cursor"]
    assert seen == list(range(1, len(seen) + 1))
    assert len(seen) >= 301

    first = client.get(
        "/audit", params={"after_seq": 0, "limit": 200, "pool": "px"}, headers=head
    ).json()
    assert first["entries"][0]["seq"] == 1
    second = client.get(
        "/audit",
        params={"after_seq": first["cursor"], "limit": 200, "pool": "px"},
        headers=head,
    ).json()
    assert len(first["entries"]) == 200
    assert len(second["entries"]) == 101
    assert second["entries"][0]["seq"] == 201

    latest = client.get(
        "/audit", params={"after_seq": 0, "limit": 200, "tail": "true"}, headers=head
    ).json()
    assert latest["entries"][-1]["seq"] == second["cursor"]
    assert len(latest["entries"]) == 200

    older = client.get(
        "/audit",
        params={"before_seq": latest["entries"][0]["seq"], "limit": 50, "pool": "px"},
        headers=head,
    ).json()
    assert len(older["entries"]) == 50
    assert older["entries"][-1]["seq"] == latest["entries"][0]["seq"] - 1
    assert older["has_older"] is True

    rest = client.get(
        "/audit",
        params={"before_seq": older["entries"][0]["seq"], "limit": 200, "pool": "px"},
        headers=head,
    ).json()
    assert rest["entries"][0]["seq"] == 1
    assert rest["entries"][-1]["seq"] == older["entries"][0]["seq"] - 1
    assert rest["has_older"] is False
    assert (
        client.get("/audit", params={"before_seq": -1}, headers=head).status_code
        == 422
    )


def test_lease_single_get_idempotency_and_bulk_release(make_client):
    client, _ = make_client()
    head = auth("admin-key")
    client.post("/pools", json={"name": "px", "kind": "fakecheck"}, headers=head)
    client.post(
        "/pools/px/resources",
        json={
            "records": [
                {"id": "r1", "host": "h1", "max_slots": 2},
                {"id": "r2", "host": "h2", "max_slots": 2},
            ],
            "import_validate": "on_import",
        },
        headers=head,
    )

    first = client.post(
        "/pools/px/leases",
        json={"consumer": "bulk"},
        headers={**head, "Idempotency-Key": "lease-1"},
    ).json()
    replay = client.post(
        "/pools/px/leases",
        json={"consumer": "bulk"},
        headers={**head, "Idempotency-Key": "lease-1"},
    ).json()
    assert replay["replayed"] is True
    assert replay["leases"][0]["token"] == first["leases"][0]["token"]

    token = first["leases"][0]["token"]
    one = client.get(f"/pools/px/leases/{token}", headers=head)
    assert one.status_code == 200
    assert one.json()["lease"]["resource_id"] == first["leases"][0]["resource_id"]
    assert client.get("/pools/px/leases/missing", headers=head).status_code == 404

    second = client.post(
        "/pools/px/leases", json={"consumer": "bulk"}, headers=head
    ).json()
    assert second["count"] == 1
    assert second["leases"][0]["resource_id"] != first["leases"][0]["resource_id"]

    assert (
        client.get(
            "/pools/px/leases", params={"consumer": "bulk"}, headers=head
        ).json()["count"]
        == 2
    )
    assert (
        client.get(
            "/pools/px/leases", params={"consumer": "other"}, headers=head
        ).json()["count"]
        == 0
    )
    only = client.get(
        "/pools/px/leases",
        params={"resource_id": first["leases"][0]["resource_id"]},
        headers=head,
    ).json()
    assert only["count"] == 1
    assert only["leases"][0]["token"] == first["leases"][0]["token"]

    drained = client.request(
        "DELETE",
        "/pools/px/leases",
        params={"consumer": "bulk", "outcome": "ok"},
        headers=head,
    ).json()
    assert drained["count"] == 2 and drained["outcome"] == "ok"
    assert client.get("/pools/px/leases", headers=head).json()["leases"] == []
    assert (
        client.request("DELETE", "/pools/px/leases", headers=head).status_code
        == 422
    )
    # The bulk release is an auditable admin operation.
    audit = client.get(
        "/audit", params={"action": "release_consumer", "pool": "px"}, headers=head
    ).json()
    assert audit["entries"][-1]["detail"]["consumer"] == "bulk"


def test_cancel_drain_returns_resource_to_service(make_client):
    client, _ = make_client()
    head = auth("admin-key")
    client.post("/pools", json={"name": "px", "kind": "fakecheck"}, headers=head)
    client.post(
        "/pools/px/resources",
        json={
            "records": [
                {"id": "r1", "host": "h1", "max_slots": 2},
                {"id": "r2", "host": "h2"},
            ],
            "import_validate": "on_import",
        },
        headers=head,
    )

    lease = client.post(
        "/pools/px/leases",
        json={"consumer": "c", "resource_id": "r1", "timeout": 5},
        headers=head,
    ).json()["leases"][0]
    assert (
        client.post(
            "/pools/px/resources/r1/drain", json={"reason": "rotate"}, headers=head
        ).json()["state"]
        == "draining"
    )
    # With one slot still busy the resource comes back as ACTIVE, not IDLE.
    resumed = client.post(
        "/pools/px/resources/r1/cancel-drain", json={"reason": "undo"}, headers=head
    ).json()
    assert resumed["state"] == "active"
    client.request(
        "DELETE",
        f"/pools/px/leases/{lease['token']}",
        content=json.dumps({"outcome": "ok"}),
        headers={**head, "Content-Type": "application/json"},
    )
    assert (
        client.get("/pools/px/resources/r1", headers=head).json()["state"] == "idle"
    )

    client.post(
        "/pools/px/resources/r2/drain", json={"reason": "rotate"}, headers=head
    )
    idle = client.post(
        "/pools/px/resources/r2/cancel-drain", json={"reason": "undo"}, headers=head
    )
    assert idle.json()["state"] == "idle"
    # Cancelling twice is a state conflict, not a silent no-op.
    assert (
        client.post(
            "/pools/px/resources/r2/cancel-drain", json={"reason": "undo"}, headers=head
        ).status_code
        == 409
    )


def test_acquire_extras_batch_mode_and_lease_renew(make_client):
    client, _ = make_client()
    head = auth("admin-key")
    client.post("/pools", json={"name": "px", "kind": "fakecheck"}, headers=head)
    client.post(
        "/pools/px/resources",
        json={
            "records": [
                {"id": f"r{i}", "host": f"10.0.0.{i}", "port": 8080, "tags": ["gold"]}
                for i in range(1, 3)
            ],
            "import_validate": "on_import",
        },
        headers=head,
    )

    bad_mode = client.post(
        "/pools/px/leases", json={"count": 2, "mode": "all"}, headers=head
    )
    assert bad_mode.status_code == 422

    single = client.post(
        "/pools/px/leases",
        json={
            "consumer": "c",
            "tags": ["gold"],
            "required_tag": "gold",
            "priority": 2,
            "use_limit": 3,
            "ignore_cooldown": True,
            "allow_duplicate": True,
        },
        headers=head,
    )
    assert single.status_code == 200
    lease = single.json()["leases"][0]
    assert single.json()["count"] == 1
    for field in ("token", "priority", "use_limit", "used", "started_at", "outcome"):
        assert field in lease, field
    assert lease["priority"] == 2 and lease["use_limit"] == 3

    # probe is now a supported per-target live check; affinity is still unused.
    ok_probe = client.post(
        "/pools/px/leases",
        json={"consumer": "c", "probe": "http://example.com:80"},
        headers=head,
    )
    assert ok_probe.status_code == 200, ok_probe.text
    probe_lease = ok_probe.json()["leases"][0]
    assert (
        client.delete(
            f"/pools/px/leases/{probe_lease['token']}", headers=head
        ).status_code
        == 200
    )
    rejected = client.post(
        "/pools/px/leases", json={"consumer": "c", "affinity": "x"}, headers=head
    )
    assert rejected.status_code == 422, rejected.text
    assert "affinity" in rejected.json()["detail"]

    renewed = client.post(
        f"/pools/px/leases/{lease['token']}/renew", headers=head
    )
    assert renewed.status_code == 200
    assert renewed.json()["expires_at"] >= lease["expires_at"]
    assert client.post("/pools/px/leases/nope/renew", headers=head).status_code == 404

    # best_effort returns what it could lease instead of waiting for the rest.
    batch = client.post(
        "/pools/px/leases",
        json={"count": 3, "mode": "best_effort", "n_min": 1, "timeout": 0},
        headers=head,
    )
    assert batch.status_code == 200
    payload = batch.json()
    assert payload["requested"] == 3 and payload["mode"] == "best_effort"
    assert payload["n_min"] == 1
    assert payload["count"] == len(payload["leases"]) == 1

    # Without n_min the batch must reach the full count.
    strict = client.post(
        "/pools/px/leases",
        json={"count": 2, "mode": "best_effort", "timeout": 0},
        headers=head,
    )
    assert strict.status_code == 409


def test_drain_pool_and_validation_report(make_client):
    client, _ = make_client()
    head = auth("admin-key")
    client.post("/pools", json={"name": "px", "kind": "fakecheck"}, headers=head)
    client.post(
        "/pools/px/resources",
        json={
            "records": [
                {"id": "r1", "host": "10.0.0.1", "port": 8080},
                {"id": "r2", "host": "10.0.0.2", "port": 8080},
            ],
            "import_validate": "on_import",
        },
        headers=head,
    )
    client.post("/pools/px/leases", json={"consumer": "c"}, headers=head)

    report = client.post(
        "/pools/px/resources/r2/validate", json={"reason": "ui"}, headers=head
    ).json()
    assert report["validation"]["alive"] is True
    assert report["validation"]["action"] == "ok"

    drained = client.post(
        "/pools/px/drain", json={"reason": "rotate"}, headers=head
    ).json()
    assert drained["drained"] == 2
    assert drained["states"] == ["active", "full", "idle"]
    states = {
        res["id"]: res["state"]
        for res in client.get("/pools/px/resources", headers=head).json()["resources"]
    }
    assert set(states.values()) == {"draining"}


def test_service_metrics_and_persistence(make_client, monkeypatch, tmp_path):
    monkeypatch.setenv("PURSER_POOLS_FILE", str(tmp_path / "pools.json"))
    client, ps = make_client()
    head = auth("admin-key")
    service = client.get("/service", headers=head).json()
    assert service["version"] and service["storage"] == "memory"
    assert service["coordination"] == "memory" and service["maintenance"] is False

    client.post(
        "/pools",
        json={
            "name": "px",
            "kind": "fakecheck",
            "config": {
                "secret_fields": ["token"],
                "weights": {"health": 1.3},
                "quota_limit": 2,
            },
        },
        headers=head,
    )
    assert (tmp_path / "pools.json").exists()
    assert client.get("/metrics").headers["content-type"].startswith("text/plain")
    snapshot = client.get("/metrics/snapshot", headers=head).json()
    assert "counters" in snapshot and "histograms" in snapshot

    # A fresh app on the same registry file restores the pool.
    client2, ps2 = make_client()
    assert client2.get("/pools", headers=head).json()["pools"][0]["name"] == "px"
    restored = ps2.get_pool("px")
    assert restored is not None
    assert restored.cfg.secret_fields == ("token",)
    assert restored.cfg.weights.health == 1.3
    assert restored.cfg.quota_limit == 2
    client2.delete("/pools/px", params={"force": "true"}, headers=head)


def test_usage_report_totals_groups_and_live_snapshot(make_client):
    client, _ = make_client()
    head = auth("admin-key")
    ro = auth("ro-key")
    client.post("/pools", json={"name": "p1", "kind": "fakecheck"}, headers=head)
    client.post("/pools", json={"name": "p2", "kind": "fakecheck"}, headers=head)
    client.post(
        "/pools/p1/resources",
        json={
            "records": [
                {"id": f"r{i}", "host": f"10.0.0.{i}", "port": 80} for i in range(1, 4)
            ],
            "import_validate": "on_import",
        },
        headers=head,
    )

    first = client.post(
        "/pools/p1/leases", json={"consumer": "c1", "resource_id": "r1"}, headers=head
    ).json()["leases"][0]
    assert (
        client.request(
            "DELETE",
            f"/pools/p1/leases/{first['token']}",
            json={"outcome": "ok", "used": 2},
            headers=head,
        ).status_code
        == 200
    )
    second = client.post(
        "/pools/p1/leases", json={"consumer": "c2", "resource_id": "r2"}, headers=head
    ).json()["leases"][0]
    client.request(
        "DELETE",
        f"/pools/p1/leases/{second['token']}",
        json={"outcome": "captcha"},
        headers=head,
    )
    held = client.post(
        "/pools/p1/leases", json={"consumer": "c3", "resource_id": "r3"}, headers=head
    ).json()["leases"][0]

    # Read-only keys can read reports.
    report = client.get("/reports/usage", headers=ro).json()
    assert report["group_by"] == "consumer"
    assert report["totals"]["releases"] == 2
    # A release without an explicit ``used`` counts the implicit single use.
    assert report["totals"]["used"] == 3
    assert report["totals"]["outcomes"] == {"ok": 1, "captcha": 1}
    assert report["window"]["events"] == 2 and report["window"]["retained"] == 2
    groups = {row["key"]: row for row in report["groups"]}
    assert groups["c1"]["releases"] == 1 and groups["c1"]["used"] == 2
    assert groups["c2"]["outcomes"] == {"captcha": 1} and groups["c2"]["used"] == 1
    assert "c3" not in groups  # active only, never returned

    active = report["active"]
    assert active["group_by"] == "consumer"
    assert [(row["key"], row["leases"]) for row in active["groups"]] == [("c3", 1)]
    pools = {row["name"]: row for row in report["pools"]}
    assert set(pools) == {"p1", "p2"}
    assert pools["p1"]["total"] == 3 and pools["p1"]["active_leases"] == 1
    assert pools["p1"]["kind"] == "fakecheck"

    by_target = client.get(
        "/reports/usage", params={"group_by": "target", "pool": "p1"}, headers=head
    ).json()
    assert [row["key"] for row in by_target["groups"]] == ["(no target)"]
    assert by_target["totals"]["releases"] == 2

    future = client.get(
        "/reports/usage", params={"since": 10**12}, headers=head
    ).json()
    assert future["totals"]["releases"] == 0 and future["groups"] == []

    bad_group = client.get(
        "/reports/usage", params={"group_by": "nope"}, headers=head
    )
    assert bad_group.status_code == 422
    assert (
        client.get("/reports/usage", params={"limit": 0}, headers=head).status_code
        == 422
    )
    assert (
        client.get(
            "/reports/usage", params={"since": 10, "until": 5}, headers=head
        ).status_code
        == 422
    )
    assert (
        client.get(
            "/reports/usage", params={"pool": "missing"}, headers=head
        ).status_code
        == 404
    )
    assert client.get("/reports/usage").status_code == 401
    assert held["consumer"] == "c3"


def _wait_validate_job(client, head, pool: str, job_id: str, timeout: float = 10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        job = client.get(
            f"/pools/{pool}/validate-jobs/{job_id}", headers=head
        ).json()
        if job["status"] in ("completed", "cancelled"):
            return job
        time.sleep(0.02)
    raise AssertionError(f"validate job {job_id} did not finish: {job}")


def test_validate_jobs_progress_listing_and_cancel(make_client):
    client, _ = make_client(checker=FakeChecker(delay=0.05))
    head = auth("admin-key")
    # One worker keeps the queue non-empty, so cancellation has pending work.
    client.post(
        "/pools",
        json={
            "name": "pv",
            "kind": "fakecheck",
            "config": {"validate_concurrency": 1},
        },
        headers=head,
    )

    report = client.post(
        "/pools/pv/resources",
        json={
            "records": [
                {"id": f"r{i}", "host": f"10.0.0.{i}", "port": 80}
                for i in range(1, 6)
            ],
            "import_validate": "batch",
        },
        headers=head,
    ).json()
    job_id = report["batch_id"]
    assert report["imported"] == 5

    listed = client.get("/pools/pv/validate-jobs", headers=head).json()["jobs"]
    assert [job["id"] for job in listed] == [job_id]
    assert listed[0]["status"] in ("running", "cancelling")
    assert listed[0]["total"] == 5
    assert listed[0]["done"] + listed[0]["pending"] == 5
    assert listed[0]["pending"] >= 1

    cancelled = client.post(
        f"/pools/pv/validate-jobs/{job_id}/cancel", headers=head
    )
    assert cancelled.status_code == 200
    payload = cancelled.json()
    assert payload["ok"] is True and payload["cancelled"] is True
    assert payload["status"] in ("cancelling", "cancelled")

    final = _wait_validate_job(client, head, "pv", job_id)
    assert final["status"] == "cancelled"
    assert final["done"] == final["total"] == 5
    assert final["skipped"] >= 1
    assert final["alive"] + final["failed"] + final["skipped"] == 5
    assert final["pending"] == 0

    # Cancelling a terminal job is a no-op and leaves the counters alone.
    again = client.post(f"/pools/pv/validate-jobs/{job_id}/cancel", headers=head)
    assert again.status_code == 200 and again.json()["cancelled"] is False
    assert again.json()["skipped"] == final["skipped"]

    audit = client.get("/audit", params={"pool": "pv"}, headers=head).json()
    assert "cancel_validate_job" in {entry["action"] for entry in audit["entries"]}

    assert (
        client.get("/pools/pv/validate-jobs/nope", headers=head).status_code == 404
    )
    assert (
        client.post(
            "/pools/pv/validate-jobs/nope/cancel", headers=head
        ).status_code
        == 404
    )
    assert client.get("/pools/pv/validate-jobs").status_code == 401
    assert (
        client.post(
            f"/pools/pv/validate-jobs/{job_id}/cancel", headers=auth("ro-key")
        ).status_code
        == 403
    )


def test_validate_job_completes_and_counts_alive(make_client):
    client, _ = make_client(checker=FakeChecker(fail_until=1))
    head = auth("admin-key")
    client.post("/pools", json={"name": "pv", "kind": "fakecheck"}, headers=head)

    report = client.post(
        "/pools/pv/resources",
        json={
            "records": [
                {"id": f"r{i}", "host": f"10.0.0.{i}", "port": 80}
                for i in range(1, 4)
            ],
            "import_validate": "batch",
        },
        headers=head,
    ).json()
    job_id = report["batch_id"]

    job = _wait_validate_job(client, head, "pv", job_id)
    assert job["status"] == "completed" and job["pending"] == 0
    assert job["total"] == 3 and job["done"] == 3
    assert job["failed"] == 1 and job["alive"] == 2
    assert job["skipped"] == 0 and job["errors"] == []
    assert job["operator"] == "api" and job["pool"] == "pv"

    assert (
        client.get("/pools/nope/validate-jobs", headers=head).status_code == 404
    )


def test_api_missing_resource_uses_404(make_client):
    client, _ = make_client()
    head = auth("admin-key")
    client.post("/pools", json={"name": "px", "kind": "fakecheck"}, headers=head)

    assert (
        client.post(
            "/pools/px/resources/missing/ban", json={"reason": "x"}, headers=head
        ).status_code
        == 404
    )
    assert (
        client.put(
            "/pools/px/resources/missing", json={"tags": ["a"]}, headers=head
        ).status_code
        == 404
    )
    assert (
        client.post(
            "/pools/px/resources/missing/validate", json={}, headers=head
        ).status_code
        == 404
    )
    assert client.delete("/pools/px/resources/missing", headers=head).status_code == 404


def test_public_plugin_api_exports():
    import purser
    from purser.core.health import HealthReport
    from purser.core.resource import PayloadField

    assert purser.HealthReport is HealthReport
    assert purser.PayloadField is PayloadField
