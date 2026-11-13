"""Wire format for the local scheduler IPC.

The transport is :mod:`multiprocessing.connection` (named pipes on Windows,
Unix domain sockets on POSIX).  Messages are length-prefixed UTF-8 JSON rather
than pickles: workers and the scheduler share the same trust boundary, but a
JSON schema keeps the protocol explicit and avoids arbitrary object loading.
"""

from __future__ import annotations

import base64
import json
import os
import sys
import tempfile
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Any, Dict, cast

from purser.local.errors import (
    SchedulerProtocolError,
    SchedulerTimeout,
    SchedulerUnavailable,
)

ENV_VAR = "PURSER_LOCAL_SCHEDULER"
PROTOCOL_VERSION = 1
MAX_MESSAGE_BYTES = 32 * 1024 * 1024


@dataclass(frozen=True)
class SchedulerAddress:
    """Serializable address of a local scheduler process."""

    family: str
    address: str
    authkey: bytes
    protocol: int = PROTOCOL_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "family": self.family,
            "address": self.address,
            "authkey": base64.b64encode(self.authkey).decode("ascii"),
            "protocol": self.protocol,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SchedulerAddress:
        try:
            return cls(
                family=str(data["family"]),
                address=str(data["address"]),
                authkey=base64.b64decode(str(data["authkey"]), validate=True),
                protocol=int(data.get("protocol", PROTOCOL_VERSION)),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise SchedulerProtocolError(
                "invalid scheduler address; pass SchedulerAddress or set "
                f"{ENV_VAR} to a value produced by address.to_env()"
            ) from exc

    def to_env(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=True, separators=(",", ":"))

    @classmethod
    def from_env(cls, value: str | None = None) -> SchedulerAddress | None:
        raw = value if value is not None else os.environ.get(ENV_VAR)
        if not raw:
            return None
        try:
            data = json.loads(raw)
        except ValueError as exc:
            raise SchedulerProtocolError(f"{ENV_VAR} is not valid JSON") from exc
        if not isinstance(data, dict):
            raise SchedulerProtocolError(f"{ENV_VAR} must contain a JSON object")
        address = cls.from_dict(data)
        if address.protocol != PROTOCOL_VERSION:
            raise SchedulerProtocolError(
                f"unsupported scheduler protocol {address.protocol}; "
                f"this client speaks {PROTOCOL_VERSION}"
            )
        return address


def default_address(authkey: bytes | None = None) -> SchedulerAddress:
    """Create a unique local address without binding it."""

    key = authkey if authkey is not None else os.urandom(32)
    if sys.platform == "win32":
        family = "AF_PIPE"
        address = rf"\\.\pipe\purser-{uuid.uuid4().hex}"
    else:
        family = "AF_UNIX"
        address = os.path.join(tempfile.gettempdir(), f"purser-{uuid.uuid4().hex}.sock")
    return SchedulerAddress(family=family, address=address, authkey=key)


def cleanup_address(address: SchedulerAddress) -> None:
    if address.family != "AF_UNIX":
        return
    try:
        os.unlink(address.address)
    except FileNotFoundError:
        pass
    except OSError:
        pass


def _json_default(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (frozenset, set, tuple)):
        return list(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, bytes):
        return {"__bytes__": base64.b64encode(value).decode("ascii")}
    raise TypeError(f"value of type {type(value).__name__} is not JSON serializable")


def encode_message(message: dict[str, Any]) -> bytes:
    try:
        raw = json.dumps(
            message, ensure_ascii=False, default=_json_default, separators=(",", ":")
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise SchedulerProtocolError(f"cannot encode scheduler message: {exc}") from exc
    if len(raw) > MAX_MESSAGE_BYTES:
        raise SchedulerProtocolError(
            f"scheduler message is too large ({len(raw)} bytes > {MAX_MESSAGE_BYTES})"
        )
    return raw


def send_message(conn: Any, message: dict[str, Any]) -> None:
    try:
        conn.send_bytes(encode_message(message))
    except (EOFError, OSError) as exc:
        raise SchedulerUnavailable("scheduler connection closed") from exc


def _revive(value: Any) -> Any:
    if isinstance(value, dict):
        if set(value) == {"__bytes__"} and isinstance(value["__bytes__"], str):
            try:
                return base64.b64decode(value["__bytes__"], validate=True)
            except ValueError:
                return value
        return {key: _revive(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_revive(item) for item in value]
    return value


def decode_message(raw: bytes) -> dict[str, Any]:
    try:
        message = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise SchedulerProtocolError("scheduler sent invalid JSON") from exc
    if not isinstance(message, dict):
        raise SchedulerProtocolError("scheduler message must be a JSON object")
    return cast(Dict[str, Any], _revive(message))


def recv_message(conn: Any, timeout: float | None = None) -> dict[str, Any]:
    try:
        if timeout is not None and not conn.poll(timeout):
            raise SchedulerTimeout("scheduler did not respond before the deadline")
        raw = conn.recv_bytes(maxlength=MAX_MESSAGE_BYTES)
    except SchedulerTimeout:
        raise
    except (EOFError, OSError) as exc:
        raise SchedulerUnavailable("scheduler connection closed") from exc
    return decode_message(raw)
