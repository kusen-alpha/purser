"""Console pool-registry persistence: save/load round-trip and bad files.

``PURSER_POOLS_FILE`` is set through ``monkeypatch`` so every test writes to
``tmp_path``; ``create_app``'s lifespan calls ``load_pools`` before the app
starts, which is exactly the restart path these tests pin down.
"""

from __future__ import annotations

import json
import logging

import pytest

from purser.ui.persistence import load_pools, save_pools

ADMIN = {"Authorization": "Bearer admin-key"}


@pytest.fixture
def registry(tmp_path, monkeypatch):
    path = tmp_path / "state" / "pools.json"
    monkeypatch.setenv("PURSER_POOLS_FILE", str(path))
    return path


def _write(path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = payload if isinstance(payload, str) else json.dumps(payload)
    path.write_text(text, encoding="utf-8")


def _create(client, name="p", **config):
    body = {"name": name, "kind": "fakecheck"}
    if config:
        body["config"] = config
    created = client.post("/pools", json=body, headers=ADMIN)
    assert created.status_code == 200, created.text
    return created


def test_registry_round_trips_through_the_api(registry, make_client):
    client, _ = make_client()
    _create(client, max_slots=2, lease_ttl=90)

    payload = json.loads(registry.read_text(encoding="utf-8"))
    [entry] = payload["pools"]
    assert entry["name"] == "p" and entry["kind"] == "fakecheck"
    assert "kind" not in entry["config"]
    assert "profile" not in entry

    client2, ps2 = make_client()
    cfg = ps2.get_pool("p").cfg
    assert cfg.max_slots == 2 and cfg.lease_ttl == 90
    assert client2.get("/pools", headers=ADMIN).json()["pools"][0]["name"] == "p"


def test_registry_round_trip_preserves_profile(registry, make_client):
    client, _ = make_client()
    created = client.post(
        "/pools",
        json={"name": "p", "kind": "fakecheck", "profile": "long_running"},
        headers=ADMIN,
    )
    assert created.status_code == 200, created.text
    [entry] = json.loads(registry.read_text(encoding="utf-8"))["pools"]
    assert entry["profile"] == "long_running"

    _, ps2 = make_client()
    assert ps2.get_pool("p").profile == "long_running"


def test_patch_and_delete_rewrite_the_registry(registry, make_client):
    client, _ = make_client()
    _create(client, max_slots=2)

    patched = client.patch(
        "/pools/p", json={"config": {"max_slots": 3}}, headers=ADMIN
    )
    assert patched.status_code == 200, patched.text
    [entry] = json.loads(registry.read_text(encoding="utf-8"))["pools"]
    assert entry["config"]["max_slots"] == 3

    removed = client.delete("/pools/p?force=true", headers=ADMIN)
    assert removed.status_code == 200, removed.text
    assert json.loads(registry.read_text(encoding="utf-8"))["pools"] == []


def test_save_pools_writes_atomically(registry, make_client):
    client, ps = make_client()
    _create(client)
    save_pools(ps)
    assert registry.exists()
    assert not registry.with_name(registry.name + ".tmp").exists()
    assert json.loads(registry.read_text(encoding="utf-8"))["pools"]


def test_missing_or_shapeless_registry_is_ignored(registry, make_client, caplog):
    assert not registry.exists()
    _, ps = make_client()
    assert dict(ps.pools) == {}

    caplog.set_level(logging.WARNING, logger="purser.ui")
    _write(registry, [])
    load_pools(ps)
    assert dict(ps.pools) == {}
    assert any("has no pools list" in r.message for r in caplog.records)

    _write(registry, {"pools": "nope"})
    load_pools(ps)
    assert dict(ps.pools) == {}

    _write(registry, "{not json")
    load_pools(ps)
    assert dict(ps.pools) == {}
    assert any("failed to read pool registry" in r.message for r in caplog.records)


def test_malformed_and_duplicate_specs_are_skipped(registry, make_client):
    _write(
        registry,
        {
            "pools": [
                "not-a-spec",
                {"name": "no-kind"},
                {"kind": "fakecheck"},
                {"name": "p", "kind": "fakecheck", "config": {"max_slots": 4}},
                {"name": "p", "kind": "fakecheck", "config": {"max_slots": 9}},
            ]
        },
    )
    _, ps = make_client()
    assert list(ps.pools) == ["p"]
    assert ps.get_pool("p").cfg.max_slots == 4


def test_unknown_kind_and_invalid_config_are_logged(
    registry, make_client, caplog
):
    _write(
        registry,
        {
            "pools": [
                {"name": "bad-kind", "kind": "no-such-kind"},
                {"name": "bad-config", "kind": "fakecheck", "config": {"max_slots": 0}},
                {"name": "ok", "kind": "fakecheck"},
            ]
        },
    )
    caplog.set_level(logging.ERROR, logger="purser.ui")
    _, ps = make_client()
    assert list(ps.pools) == ["ok"]
    messages = [r.message for r in caplog.records]
    assert any("failed to restore pool 'bad-kind'" in m for m in messages)
    assert any("failed to restore pool 'bad-config'" in m for m in messages)


def test_save_pools_is_a_noop_without_the_env(monkeypatch, make_client):
    client, ps = make_client()
    _create(client)
    monkeypatch.delenv("PURSER_POOLS_FILE", raising=False)
    save_pools(ps)
    assert "p" in ps.pools


def test_save_pools_swallows_unwritable_path(
    tmp_path, monkeypatch, caplog, make_client
):
    blocker = tmp_path / "blocker"
    blocker.write_text("file, not a directory", encoding="utf-8")
    monkeypatch.setenv("PURSER_POOLS_FILE", str(blocker / "pools.json"))
    caplog.set_level(logging.ERROR, logger="purser.ui")

    client, ps = make_client()
    _create(client)  # already logs the failed write
    save_pools(ps)
    assert any("failed to persist pool registry" in r.message for r in caplog.records)
    assert blocker.is_file()
