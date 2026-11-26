"""Edge branches of the UI routers: validation, conflicts and SSE control flow.

Happy paths live in ``test_api.py``/``test_ui_contract.py``; these tests pin the
422/404/409 mappings, the extended ``update_resource`` fields, the drain/delete
state machine and the streaming generator's disconnect handling.
"""

from __future__ import annotations

import asyncio
import json

from purser.errors import BadState

ADMIN = {"Authorization": "Bearer admin-key"}
READONLY = {"Authorization": "Bearer ro-key"}


def _setup(make_client, pools=("p",), *, records=None, **cfg):
    client, ps = make_client()
    for name in pools:
        created = client.post(
            "/pools", json={"name": name, "kind": "fakecheck", **cfg}, headers=ADMIN
        )
        assert created.status_code == 200, created.text
    rows = records if records is not None else [
        {"id": "r-0", "host": "127.0.0.1", "port": 8000},
        {"id": "r-1", "host": "127.0.0.1", "port": 8001, "tags": ["eu"]},
    ]
    for name in pools:
        imported = client.post(
            f"/pools/{name}/resources", json={"records": rows}, headers=ADMIN
        )
        assert imported.status_code == 200, imported.text
    return client, ps


def _acquire(client, rid, *, pool="p", **body):
    resp = client.post(
        f"/pools/{pool}/leases", json={"resource_id": rid, **body}, headers=ADMIN
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["leases"][0]


# -- resources: listing ------------------------------------------------------


def test_list_resources_rejects_bad_filters(make_client):
    client, _ = _setup(make_client)
    url = "/pools/p/resources"

    assert client.get(url, params={"limit": 0}, headers=ADMIN).status_code == 422
    assert client.get(url, params={"limit": 1001}, headers=ADMIN).status_code == 422
    assert client.get(url, params={"offset": -1}, headers=ADMIN).status_code == 422

    bad_sort = client.get(url, params={"sort": "bogus"}, headers=ADMIN)
    assert bad_sort.status_code == 422
    assert "sort must be one of" in bad_sort.json()["detail"]

    bad_order = client.get(url, params={"sort": "id", "order": "up"}, headers=ADMIN)
    assert bad_order.status_code == 422
    assert "order must be asc or desc" in bad_order.json()["detail"]

    bad_state = client.get(url, params={"state": "idle,bogus"}, headers=ADMIN)
    assert bad_state.status_code == 422
    assert "bad state value(s)" in bad_state.json()["detail"]


def test_list_resources_forwards_filters(make_client):
    client, _ = _setup(make_client)
    resp = client.get(
        "/pools/p/resources",
        params={
            "state": "idle,active",
            "tag": "eu",
            "q": "r-1",
            "sort": "health",
            "order": "asc",
            "limit": 5,
            "offset": 0,
        },
        headers=ADMIN,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert [row["id"] for row in body["resources"]] == ["r-1"]
    assert body["total"] == 1 and body["limit"] == 5 and body["offset"] == 0
    assert body["sort"] == "health" and body["order"] == "asc"


def test_get_resource_secrets_require_admin(make_client):
    client, _ = _setup(make_client)
    missing = client.get("/pools/p/resources/ghost", headers=ADMIN)
    assert missing.status_code == 404
    assert missing.json()["detail"] == "resource 'ghost' not found"

    denied = client.get(
        "/pools/p/resources/r-0", params={"include_secrets": True}, headers=READONLY
    )
    assert denied.status_code == 403
    assert denied.json()["detail"] == "admin role required for secrets"

    masked = client.get("/pools/p/resources/r-0", headers=READONLY).json()
    assert masked["payload"]["host"] == "***"
    clear = client.get(
        "/pools/p/resources/r-0", params={"include_secrets": True}, headers=ADMIN
    ).json()
    assert clear["payload"]["host"] == "127.0.0.1"


def test_import_is_idempotent_and_rejects_bad_records(make_client):
    client, _ = _setup(make_client)
    head = {**ADMIN, "Idempotency-Key": "import-1"}
    body = {"records": [{"id": "x-1", "host": "10.0.0.1", "port": 80}]}

    first = client.post("/pools/p/resources", json=body, headers=head)
    assert first.status_code == 200, first.text
    replayed = client.post("/pools/p/resources", json=body, headers=head)
    assert replayed.json() == first.json()

    bad = client.post("/pools/p/resources", json={"records": "nope"}, headers=ADMIN)
    assert bad.status_code == 422
    assert bad.json()["detail"] == "records must be a list"


# -- resources: update / delete ---------------------------------------------


def test_update_resource_applies_extended_fields(make_client):
    client, _ = _setup(make_client)
    url = "/pools/p/resources/r-0"

    resp = client.put(
        url,
        json={
            "payload": {"host": "10.0.0.9", "port": 9000},
            "tags": ["a", "b"],
            "usage_quota": 5,
            "quota_limit": 3,
            "expires_at": 4102444800,
            "next_available_at": 0,
            "mutex_group": "g1",
            "schedule_window": ["00:00", "23:59"],
            "priority": 2,
            "preference": 0.5,
            "max_slots": 2,
            "reason": "tune",
        },
        headers=ADMIN,
    )
    assert resp.status_code == 200, resp.text
    res = resp.json()["resource"]
    assert res["tags"] == ["a", "b"] and res["max_slots"] == 2
    assert res["usage_quota"] == 5 and res["quota"]["limit"] == 3
    assert res["expires_at"] == 4102444800.0
    assert res["next_available_at"] == 0.0
    assert res["mutex_group"] == "g1"
    assert res["schedule_window"] == ["00:00", "23:59"]

    cleared = client.put(
        url,
        json={
            "usage_quota": None,
            "quota_limit": None,
            "expires_at": None,
            "next_available_at": None,
            "mutex_group": None,
            "schedule_window": None,
            "priority": None,
            "preference": None,
        },
        headers=ADMIN,
    )
    assert cleared.status_code == 200, cleared.text
    res = cleared.json()["resource"]
    assert res["usage_quota"] is None and res["quota"] is None
    assert res["expires_at"] is None and res["next_available_at"] is None
    assert res["mutex_group"] is None and res["schedule_window"] is None


def test_update_resource_rejects_malformed_fields(make_client):
    client, _ = _setup(make_client)
    url = "/pools/p/resources/r-0"
    cases = [
        {"payload": "not-an-object"},
        {"tags": "not-a-list"},
        {"mutex_group": 5},
        {"schedule_window": "00:00-23:59"},
        {"priority": "high"},
        {"preference": "fast"},
        {"max_slots": 0},
        {"usage_quota": -1},
        {"quota_limit": 0},
        {"expires_at": -5},
        {"next_available_at": "soon"},
    ]
    for body in cases:
        resp = client.put(url, json=body, headers=ADMIN)
        assert resp.status_code == 422, (body, resp.text)


def test_update_unknown_resource_is_not_found(make_client):
    client, _ = _setup(make_client)
    resp = client.put("/pools/p/resources/ghost", json={"tags": []}, headers=ADMIN)
    assert resp.status_code == 404
    assert "ghost" in resp.json()["detail"]


def test_delete_busy_resource_drains_then_archives(make_client):
    client, _ = _setup(make_client)
    lease = _acquire(client, "r-0")

    first = client.delete("/pools/p/resources/r-0", headers=ADMIN)
    assert first.status_code == 200, first.text
    assert first.json() == {"ok": True, "rid": "r-0", "state": "draining"}

    released = client.delete(f"/pools/p/leases/{lease['token']}", headers=ADMIN)
    assert released.status_code == 200, released.text

    second = client.delete("/pools/p/resources/r-0", headers=ADMIN)
    assert second.status_code == 200, second.text
    assert second.json()["state"] == "archived"


def test_delete_new_resource_conflicts_and_missing_is_404(make_client):
    client, _ = _setup(make_client, records=[])
    imported = client.post(
        "/pools/p/resources",
        json={
            "records": [{"id": "n-0", "host": "127.0.0.1", "port": 9100}],
            "import_validate": "none",
        },
        headers=ADMIN,
    )
    assert imported.status_code == 200, imported.text
    assert client.get("/pools/p/resources/n-0", headers=ADMIN).json()["state"] == "new"

    conflict = client.delete("/pools/p/resources/n-0", headers=ADMIN)
    assert conflict.status_code == 409
    assert "illegal transition" in conflict.json()["detail"]

    missing = client.delete("/pools/p/resources/ghost", headers=ADMIN)
    assert missing.status_code == 404


def test_delete_drain_failure_maps_to_conflict(make_client):
    client, ps = _setup(make_client)
    _acquire(client, "r-1")

    async def refuse(*args, **kwargs):
        raise BadState("cannot drain right now")

    ps.get_pool("p").admin.drain = refuse
    resp = client.delete("/pools/p/resources/r-1", headers=ADMIN)
    assert resp.status_code == 409
    assert resp.json()["detail"] == "cannot drain right now"


# -- resource actions --------------------------------------------------------


def test_revoke_conflicts(make_client):
    client, _ = _setup(make_client)
    idle = client.post("/pools/p/resources/r-0/revoke", headers=ADMIN)
    assert idle.status_code == 409
    assert "no active lease" in idle.json()["detail"]

    lease = _acquire(client, "r-0")
    wrong = client.post(
        "/pools/p/resources/r-0/revoke", json={"token": "nope"}, headers=ADMIN
    )
    assert wrong.status_code == 409
    assert "not found on r-0" in wrong.json()["detail"]

    ok = client.post(
        "/pools/p/resources/r-0/revoke", json={"reason": "ops"}, headers=ADMIN
    )
    assert ok.status_code == 200, ok.text
    assert ok.json() == {"ok": True, "revoked_token": lease["token"]}


def test_move_resource_validation_and_success(make_client):
    client, _ = _setup(make_client)
    created = client.post(
        "/pools", json={"name": "q", "kind": "fakecheck"}, headers=ADMIN
    )
    assert created.status_code == 200, created.text

    missing = client.post("/pools/p/resources/r-0/move", json={}, headers=ADMIN)
    assert missing.status_code == 422
    same = client.post(
        "/pools/p/resources/r-0/move", json={"target_pool": "p"}, headers=ADMIN
    )
    assert same.status_code == 422
    assert same.json()["detail"] == "target_pool must differ"
    unknown = client.post(
        "/pools/p/resources/r-0/move", json={"target_pool": "ghost"}, headers=ADMIN
    )
    assert unknown.status_code == 404

    moved = client.post(
        "/pools/p/resources/r-0/move",
        json={"target_pool": "q", "reason": "rebalance"},
        headers=ADMIN,
    )
    assert moved.status_code == 200, moved.text
    assert moved.json()["resource"]["id"] == "r-0"
    assert moved.json()["target_pool"] == "q"
    assert client.get("/pools/q/resources/r-0", headers=ADMIN).status_code == 200

    _acquire(client, "r-1")
    busy = client.post(
        "/pools/p/resources/r-1/move", json={"target_pool": "q"}, headers=ADMIN
    )
    assert busy.status_code == 409


def test_admin_state_action_branches(make_client):
    client, _ = _setup(make_client)
    url = "/pools/p/resources/r-0/admin-state"

    assert client.post(url, json={}, headers=ADMIN).status_code == 422
    assert client.post(url, json={"target": "bogus"}, headers=ADMIN).status_code == 422

    ok = client.post(
        url, json={"target": "suspended", "reason": "pause"}, headers=ADMIN
    )
    assert ok.status_code == 200, ok.text
    assert ok.json() == {"ok": True, "rid": "r-0", "state": "suspended"}

    _acquire(client, "r-1")
    busy = client.post(
        "/pools/p/resources/r-1/admin-state", json={"target": "idle"}, headers=ADMIN
    )
    assert busy.status_code == 409
    assert "is busy" in busy.json()["detail"]


def test_top_up_and_drain_actions(make_client):
    client, _ = _setup(make_client)
    bad_amount = client.post(
        "/pools/p/resources/r-0/top-up", json={"amount": 0}, headers=ADMIN
    )
    assert bad_amount.status_code == 422

    drained = client.post(
        "/pools/p/resources/r-0/drain", json={"reason": "rotation"}, headers=ADMIN
    )
    assert drained.status_code == 200, drained.text
    assert drained.json()["state"] == "draining"

    restored = client.post("/pools/p/resources/r-0/cancel-drain", headers=ADMIN)
    assert restored.status_code == 200, restored.text
    assert restored.json()["state"] == "idle"


# -- leases ------------------------------------------------------------------


def test_lease_request_validation(make_client):
    client, _ = _setup(make_client)
    url = "/pools/p/leases"
    cases = [
        {"tags": "eu"},
        {"tags": [1]},
        {"consumer": 5},
        {"affinity": True},
        {"mode": "greedy"},
        {"priority": -1},
        {"slots": 0},
        {"timeout": -1},
        {"use_limit": 0},
        {"count": -1},
        {"count": 2, "n_min": 0},
    ]
    for body in cases:
        resp = client.post(url, json=body, headers=ADMIN)
        assert resp.status_code == 422, (body, resp.text)


def test_bundle_lease_validation_and_replay(make_client):
    client, _ = _setup(make_client)
    url = "/pools/p/leases"

    not_object = client.post(url, json={"bundle": "x"}, headers=ADMIN)
    assert not_object.status_code == 422
    assert not_object.json()["detail"] == "bundle must be an object"

    no_anchor = client.post(
        url, json={"bundle": {"slots": [{"pool": "other"}]}}, headers=ADMIN
    )
    assert no_anchor.status_code == 422
    assert "anchor pool" in no_anchor.json()["detail"]

    unsatisfiable = client.post(
        url,
        json={"bundle": {"slots": [{"pool": "p", "req": {"tags": ["missing"]}}]}},
        headers=ADMIN,
    )
    assert unsatisfiable.status_code == 409

    head = {**ADMIN, "Idempotency-Key": "bundle-1"}
    body = {"bundle": {"slots": [{"pool": "p", "resource_id": "r-0"}], "consumer": "w1"}}
    ok = client.post(url, json=body, headers=head)
    assert ok.status_code == 200, ok.text
    bundle = ok.json()
    assert bundle["consumer"] == "w1"
    assert bundle["members"]["p"]["resource_id"] == "r-0"

    replayed = client.post(url, json=body, headers=head)
    assert replayed.json()["replayed"] is True
    assert replayed.json()["bundle_id"] == bundle["bundle_id"]


def test_lease_idempotency_and_conflict(make_client):
    client, _ = _setup(make_client)
    url = "/pools/p/leases"
    head = {**ADMIN, "Idempotency-Key": "lease-1"}

    first = client.post(url, json={"resource_id": "r-0"}, headers=head)
    assert first.status_code == 200, first.text
    replayed = client.post(url, json={"resource_id": "r-0"}, headers=head)
    assert replayed.json()["replayed"] is True

    occupied = client.post(url, json={"resource_id": "r-0"}, headers=ADMIN)
    assert occupied.status_code == 409


def test_lease_listing_filters_and_missing_token(make_client):
    client, _ = _setup(make_client)
    url = "/pools/p/leases"
    _acquire(client, "r-0", consumer="c-1")

    listed = client.get(url, headers=ADMIN).json()
    assert listed["count"] == 1
    assert listed["leases"][0]["consumer"] == "c-1"

    filtered = client.get(url, params={"consumer": "nobody"}, headers=ADMIN).json()
    assert filtered == {"leases": [], "count": 0}

    missing = client.get(f"{url}/nope", headers=ADMIN)
    assert missing.status_code == 404
    assert missing.json()["detail"] == "lease not found on this pool"


def test_release_consumer_validation_and_mapping(make_client):
    client, ps = _setup(make_client)
    url = "/pools/p/leases"

    blank = client.delete(url, headers=ADMIN)
    assert blank.status_code == 422
    assert blank.json()["detail"] == "consumer is required"

    bad_outcome = client.delete(
        url, params={"consumer": "c-1", "outcome": "bogus"}, headers=ADMIN
    )
    assert bad_outcome.status_code == 422

    _acquire(client, "r-0", consumer="c-1")
    done = client.delete(url, params={"consumer": "c-1", "outcome": "ok"}, headers=ADMIN)
    assert done.status_code == 200, done.text
    assert done.json()["count"] == 1 and done.json()["outcome"] == "ok"

    async def refuse(*args, **kwargs):
        raise BadState("coordination down")

    ps.get_pool("p").release_consumer = refuse
    failed = client.delete(url, params={"consumer": "c-1"}, headers=ADMIN)
    assert failed.status_code == 409
    assert failed.json()["detail"] == "coordination down"


def test_renew_lease_errors(make_client):
    client, _ = _setup(make_client)
    url = "/pools/p/leases"

    unknown = client.post(f"{url}/nope/renew", headers=ADMIN)
    assert unknown.status_code == 404

    lease = _acquire(client, "r-0", use_limit=1)
    limited = client.post(
        f"{url}/{lease['token']}/renew", json={"used": 1}, headers=ADMIN
    )
    assert limited.status_code == 409
    assert "use_limit" in limited.json()["detail"]

    negative = client.post(
        f"{url}/{lease['token']}/renew", json={"used": -1}, headers=ADMIN
    )
    assert negative.status_code == 422


def test_release_lease_errors(make_client):
    client, ps = _setup(make_client)
    url = "/pools/p/leases"

    unknown = client.delete(f"{url}/nope", headers=ADMIN)
    assert unknown.status_code == 404

    lease = _acquire(client, "r-0")
    done = client.request(
        "DELETE",
        f"{url}/{lease['token']}",
        content=json.dumps({"outcome": "ok", "used": 0}),
        headers={**ADMIN, "Content-Type": "application/json"},
    )
    assert done.status_code == 200, done.text
    assert done.json() == {"ok": True, "token": lease["token"], "outcome": "ok"}

    async def refuse(*args, **kwargs):
        raise BadState("release refused")

    held = _acquire(client, "r-1")
    ps.get_pool("p").release = refuse
    failed = client.request(
        "DELETE", f"{url}/{held['token']}", headers=ADMIN
    )
    assert failed.status_code == 409
    assert failed.json()["detail"] == "release refused"


# -- maintenance: events, drain, sweeps, recall -----------------------------


def test_events_stream_stops_when_client_disconnects(make_client):
    client, ps = _setup(make_client, pools=("p", "q"))
    ps.bus.publish("pool_note", "q", data={"note": "other"})
    ps.bus.publish("pool_note", "p", data={"note": "mine"})

    endpoint = next(
        route.endpoint
        for route in client.app.routes
        if getattr(route, "path", "") == "/pools/{pool_name}/events"
    )

    class _Request:
        def __init__(self) -> None:
            self.headers = {"authorization": "Bearer admin-key"}
            self.calls = 0

        async def is_disconnected(self) -> bool:
            self.calls += 1
            return self.calls > 1

    request = _Request()

    async def consume() -> list[str]:
        response = await endpoint(request, pool_name="p")
        assert response.media_type == "text/event-stream"
        chunks: list[str] = []
        async for chunk in response.body_iterator:
            chunks.append(chunk)
        return chunks

    chunks = asyncio.run(consume())
    assert chunks
    assert all('"pool": "p"' in chunk for chunk in chunks)
    assert all('"pool": "q"' not in chunk for chunk in chunks)
    assert request.calls == 2  # second poll saw the disconnect and stopped


def test_event_history_validation_and_filters(make_client):
    client, _ = _setup(make_client)
    url = "/pools/p/events/history"

    assert client.get(url, params={"after_seq": -1}, headers=ADMIN).status_code == 422
    assert client.get(url, params={"limit": 0}, headers=ADMIN).status_code == 422
    assert client.get(url, params={"limit": 1001}, headers=ADMIN).status_code == 422

    recent = client.get(url, params={"limit": 3}, headers=ADMIN).json()
    assert recent["events"]
    assert recent["cursor"] == recent["events"][-1]["seq"]

    typed = client.get(url, params={"type": "import_finished"}, headers=ADMIN).json()
    assert typed["events"]
    assert {event["type"] for event in typed["events"]} == {"import_finished"}

    empty = client.get(url, params={"type": "no_such_event"}, headers=ADMIN).json()
    assert empty == {"events": [], "cursor": 0}


def test_drain_pool_defaults_and_state_validation(make_client):
    client, _ = _setup(make_client)
    url = "/pools/p/drain"

    bad = client.post(url, json={"states": "idle"}, headers=ADMIN)
    assert bad.status_code == 422
    assert bad.json()["detail"] == "states must be a list"

    unknown = client.post(url, json={"states": ["bogus"]}, headers=ADMIN)
    assert unknown.status_code == 422

    drained = client.post(url, headers=ADMIN)
    assert drained.status_code == 200, drained.text
    body = drained.json()
    assert body["drained"] == 2
    assert sorted(body["resources"]) == ["r-0", "r-1"]
    assert body["states"] == ["active", "full", "idle"]
    assert body["errors"] == []


def test_drain_pool_collects_per_resource_errors(make_client):
    client, ps = _setup(make_client)
    pool = ps.get_pool("p")
    original = pool.admin.drain

    async def flaky(rid, *, operator="system", reason=""):
        if rid == "r-1":
            raise BadState("refusing drain")
        return await original(rid, operator=operator, reason=reason)

    pool.admin.drain = flaky
    resp = client.post("/pools/p/drain", headers=ADMIN)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["drained"] == 1
    assert body["resources"] == ["r-0"]
    assert body["errors"] == [{"rid": "r-1", "error": "refusing drain"}]


def test_maintenance_and_recall_error_paths(make_client):
    client, _ = _setup(make_client)

    unknown = client.post("/pools/p/maintenance/nope", headers=ADMIN)
    assert unknown.status_code == 422
    assert "unknown maintenance task" in unknown.json()["detail"]

    ok = client.post("/pools/p/maintenance/quota_reset", headers=ADMIN)
    assert ok.status_code == 200, ok.text
    assert ok.json()["task"] == "quota_reset"

    no_batch = client.post("/pools/p/recall", json={}, headers=ADMIN)
    assert no_batch.status_code == 422
    assert no_batch.json()["detail"] == "batch_id must be a non-empty string"

    bad_action = client.post(
        "/pools/p/recall", json={"batch_id": "b-1", "action": "explode"}, headers=ADMIN
    )
    assert bad_action.status_code == 422
    assert "unknown recall action" in bad_action.json()["detail"]

    empty = client.post(
        "/pools/p/recall", json={"batch_id": "b-1", "action": "requalify"}, headers=ADMIN
    )
    assert empty.status_code == 200, empty.text
    assert empty.json()["processed"] == 0
