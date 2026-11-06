"""Payload encryption (C1) and masked views."""

from __future__ import annotations

import json
import sqlite3

from purser.core.resource import Resource, describe_resource
from purser.core.state import ResourceState
from purser.crypto import SecretCodec, mask_payload

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
    backend.close()


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
