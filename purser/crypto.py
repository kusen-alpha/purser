"""AES-GCM payload encryption for sensitive fields (C1 / K1)."""

from __future__ import annotations

import base64
import json
import os
from dataclasses import dataclass
from typing import Any

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from purser.errors import BadPayload, ConfigError


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _unb64(text: str) -> bytes:
    return base64.b64decode(text.encode("ascii"))


@dataclass(slots=True)
class SecretCodec:
    """Encrypts a configured subset of payload fields before persistence.

    The key is injected externally (env ``PURSER_SECRET_KEY``, base64 or raw
    32 bytes).  ``key_id`` is stored alongside ciphertext so keys can be
    rotated while old records remain readable (K1-lite).
    """

    key: bytes | None = None
    key_id: str = "1"

    def __post_init__(self) -> None:
        if self.key is None:
            raw = os.environ.get("PURSER_SECRET_KEY")
            if raw:
                self.key = self._parse_key(raw)
            else:
                # Test/dev convenience only: an ephemeral key is generated.
                self.key = os.urandom(32)
        if len(self.key) not in (16, 24, 32):
            raise ConfigError("PURSER_SECRET_KEY must be 16/24/32 bytes (or base64)")

    @staticmethod
    def _parse_key(raw: str) -> bytes:
        try:
            if len(raw) >= 44:
                return _unb64(raw)
        except Exception:
            pass
        key = raw.encode("utf-8")
        if len(key) not in (16, 24, 32):
            key = key.ljust(32, b"\0")[:32]
        return key

    def encrypt_payload(
        self, payload: dict[str, Any], secret_fields: tuple[str, ...]
    ) -> dict[str, Any]:
        if not secret_fields:
            return dict(payload)
        stored: dict[str, Any] = {}
        enc: dict[str, Any] = {}
        for key, value in payload.items():
            if key in secret_fields:
                enc[key] = self._seal(value)
            else:
                stored[key] = value
        if enc:
            stored["_enc"] = enc
        return stored

    def decrypt_payload(
        self, stored: dict[str, Any], secret_fields: tuple[str, ...]
    ) -> dict[str, Any]:
        if not secret_fields:
            return dict(stored)
        payload = {k: v for k, v in stored.items() if k != "_enc"}
        enc = stored.get("_enc") or {}
        for key, sealed in enc.items():
            try:
                payload[key] = self._open(sealed)
            except Exception as exc:
                raise BadPayload(
                    f"cannot decrypt secret field {key!r} (key mismatch?)"
                ) from exc
        return payload

    def _seal(self, value: Any) -> dict[str, str]:
        aes = AESGCM(self.key)
        nonce = os.urandom(12)
        # JSON keeps the field's type (int/float/bool/None/list/dict) intact
        # through the encrypt/decrypt round trip.
        plaintext = json.dumps(value, ensure_ascii=False).encode("utf-8")
        ct = aes.encrypt(nonce, plaintext, None)
        return {"k": self.key_id, "v": _b64(nonce + ct)}

    def _open(self, sealed: dict[str, str]) -> Any:
        blob = _unb64(sealed["v"])
        nonce, ct = blob[:12], blob[12:]
        aes = AESGCM(self.key)
        return json.loads(aes.decrypt(nonce, ct, None).decode("utf-8"))


def mask_payload(payload: dict[str, Any]) -> dict[str, str]:
    return {k: "***" for k in payload} or {}


def load_or_create_key(store_path: str) -> bytes | None:
    """Return the 32-byte key persisted next to ``store_path``.

    Created on first use so payloads encrypted by a dev/test instance stay
    readable after a restart even without ``PURSER_SECRET_KEY``.  Returns
    ``None`` for in-memory stores or when the key file cannot be written.
    """

    if not store_path or store_path == ":memory:":
        return None
    keyfile = f"{store_path}.key"
    try:
        with open(keyfile, "rb") as fh:
            raw = fh.read()
        if len(raw) != 32:
            raise ConfigError(f"corrupt key file {keyfile!r}: expected 32 bytes")
        return raw
    except OSError:
        pass
    key = os.urandom(32)
    try:
        with open(keyfile, "wb") as fh:
            fh.write(key)
    except OSError:
        return None
    return key
