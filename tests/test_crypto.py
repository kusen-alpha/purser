"""Payload encryption (C1) and masked views."""

from __future__ import annotations

import json
import logging
import sqlite3

import pytest

from purser.core.resource import Resource, describe_resource
from purser.core.state import ResourceState
from purser.crypto import SecretCodec, load_or_create_key, mask_payload
from purser.errors import ConfigError

SECRETS = ("password", "cookie")


def test_secret_codec_roundtrip():
    codec = SecretCodec()
    payload = {"username": "alice", "password": "s3cret", "note": "plain"}
    stored = codec.encrypt_payload(payload, SECRETS)
    assert stored["username"] == "alice"
    assert stored["note"] == "plain"
    assert "_enc" in stored and "password" in stored["_enc"]
    assert "s3cret" not in json.dumps(stored)
    assert codec.decrypt_payload(stored, SECRETS) == payload


def test_secret_codec_preserves_types():
    codec = SecretCodec()
    payload = {"password": "x", "age": 30, "active": True, "price": 1.5, "extra": None}
    fields = ("password", "age", "active", "price", "extra")
    stored = codec.encrypt_payload(payload, fields)
    out = codec.decrypt_payload(stored, fields)
    assert out == payload
    assert out["age"] == 30 and isinstance(out["age"], int)
    assert out["active"] is True
    assert out["price"] == 1.5
    assert out["extra"] is None


def test_no_secret_fields_is_plain():
    codec = SecretCodec()
    assert codec.encrypt_payload({"a": 1}, ()) == {"a": 1}


async def test_sqlite_stores_encrypted_payload(tmp_path):
    db = str(tmp_path / "secrets.db")
    from purser.backends.sqlite import SqliteStorage

    backend = SqliteStorage(path=db, secret_fields=("password",))
    resource = Resource(
        id="acc-1",
        kind="account",
        payload={"username": "u", "password": "super-secret-value"},
        state=ResourceState.NEW,
    )
    await backend.put("spool", resource)
    conn = sqlite3.connect(db)
    try:
        blob = conn.execute(
            "SELECT payload FROM resources WHERE pool = 'spool' AND id = 'acc-1'"
        ).fetchone()[0]
    finally:
        conn.close()
    stored_payload = json.loads(blob)
    assert "password" in stored_payload["_enc"]
    assert "super-secret-value" not in blob
    loaded = await backend.get("spool", "acc-1")
    assert loaded.payload == {"username": "u", "password": "super-secret-value"}
    await backend.close()


def test_describe_masks_payload():
    resource = Resource(
        id="a",
        kind="account",
        payload={"username": "alice", "password": "hunter2"},
        state=ResourceState.IDLE,
    )
    view = describe_resource(resource)
    assert view["payload"] == {"username": "***", "password": "***"}
    assert "hunter2" not in json.dumps(view)
    secret_view = describe_resource(resource, secret=True)
    assert secret_view["payload"]["password"] == "hunter2"


def test_mask_payload():
    assert mask_payload({"a": 1}) == {"a": "***"}


def test_load_or_create_key_creates_then_reuses(tmp_path):
    path = str(tmp_path / "store.db")
    first = load_or_create_key(path)
    assert first is not None and len(first) == 32
    assert load_or_create_key(path) == first


def test_load_or_create_key_skips_memory_stores():
    assert load_or_create_key(":memory:") is None
    assert load_or_create_key("") is None


def test_corrupt_keyfile_raises(tmp_path):
    path = str(tmp_path / "store.db")
    (tmp_path / "store.db.key").write_bytes(b"short")
    with pytest.raises(ConfigError, match="corrupt key file"):
        load_or_create_key(path)


def test_unreadable_keyfile_raises(tmp_path, monkeypatch):
    path = str(tmp_path / "store.db")
    (tmp_path / "store.db.key").write_bytes(b"x" * 32)
    real_open = open

    def fake_open(file, mode="r", *args, **kwargs):
        if str(file).endswith(".key") and "r" in mode:
            raise PermissionError("denied")
        return real_open(file, mode, *args, **kwargs)

    monkeypatch.setattr("builtins.open", fake_open)
    with pytest.raises(ConfigError, match="cannot read key file"):
        load_or_create_key(path)


def test_unwritable_keyfile_raises(tmp_path, monkeypatch):
    path = str(tmp_path / "store.db")
    real_open = open

    def fake_open(file, mode="r", *args, **kwargs):
        if str(file).endswith(".key") and "w" in mode:
            raise OSError("read-only filesystem")
        return real_open(file, mode, *args, **kwargs)

    monkeypatch.setattr("builtins.open", fake_open)
    with pytest.raises(ConfigError, match="cannot persist key file"):
        load_or_create_key(path)


def test_ephemeral_key_warns(monkeypatch, caplog):
    monkeypatch.delenv("PURSER_SECRET_KEY", raising=False)
    with caplog.at_level(logging.WARNING, logger="purser"):
        codec = SecretCodec()
    assert codec.ephemeral is True
    assert "ephemeral key" in caplog.text
