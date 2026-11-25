"""Pure helpers shared by the UI routers (no app or service state)."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from fastapi import HTTPException

from purser import ResourcePool
from purser.config import parse_pool_config
from purser.core.resource import Resource, UsageOutcome
from purser.core.state import ResourceState
from purser.crypto import mask_payload
from purser.errors import (
    BadPayload,
    ConfigError,
    PoolNotFound,
    PurserError,
    ResourceNotFound,
)


def http_purser_error(exc: PurserError) -> HTTPException:
    """Map a library error to a client error instead of a 500.

    Not-found errors keep REST semantics (404); malformed input is 422;
    state/lease conflicts stay 409 so callers can retry after reconciling.
    """

    if isinstance(exc, (ResourceNotFound, PoolNotFound)):
        status = 404
    elif isinstance(exc, (ConfigError, BadPayload)):
        status = 422
    else:
        status = 409
    return HTTPException(status_code=status, detail=str(exc))


def field_type(coerce: Callable[[Any], Any] | None) -> str:
    if coerce is int:
        return "int"
    if coerce is float:
        return "float"
    if coerce is bool:
        return "bool"
    return "string"


def json_scalar(value: Any) -> Any:
    # Defaults are hints for form prefilling; never leak arbitrary objects.
    return value if isinstance(value, (str, int, float, bool)) else None


def resource_view(
    pool: ResourcePool,
    resource: Resource,
    *,
    include_secrets: bool,
    admin: bool,
) -> dict[str, Any]:
    secret = bool(admin and include_secrets)
    return pool.describe(resource, secret=secret)


def state_value(value: Any) -> ResourceState:
    if not isinstance(value, str) or not value:
        raise HTTPException(
            status_code=422, detail=f"state must be a non-empty string, got {value!r}"
        )
    try:
        return ResourceState(value)
    except ValueError:
        raise HTTPException(status_code=422, detail=f"unknown state {value!r}") from None


def outcome_value(value: Any) -> UsageOutcome:
    if value is None:
        return UsageOutcome.OK
    if isinstance(value, UsageOutcome):
        return value
    try:
        return UsageOutcome(str(value))
    except ValueError:
        raise HTTPException(status_code=422, detail=f"unknown outcome {value!r}") from None


def coerce(
    value: Any,
    *,
    name: str,
    cast: Callable[[Any], Any],
    minimum: int | float | None = None,
) -> Any:
    if value is None:
        return None
    try:
        out = cast(value)
    except (TypeError, ValueError):
        raise HTTPException(
            status_code=422, detail=f"{name} must be {cast.__name__}"
        ) from None
    if minimum is not None and out < minimum:
        raise HTTPException(status_code=422, detail=f"{name} must be >= {minimum}")
    return out


def clean_pool_config(raw: Any, *, mode: str = "create", base: Any = None) -> dict[str, Any]:
    """Single pool-config parser shared with Python and the local scheduler."""

    try:
        return parse_pool_config(raw, mode=mode, base=base)
    except ConfigError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def optional_epoch(value: Any, name: str) -> float | None:
    if value is None:
        return None
    parsed = coerce(value, name=name, cast=float, minimum=0)
    return None if parsed is None else float(parsed)


def lease_view(lease: Any, *, admin: bool) -> dict[str, Any]:
    return {
        "token": lease.token,
        "resource_id": lease.resource_id,
        "consumer": lease.consumer,
        "slots": lease.slots,
        "started_at": lease.started_at,
        "expires_at": lease.expires_at,
        "priority": lease.priority,
        "use_limit": lease.use_limit,
        "used": lease.used,
        "target": lease.target,
        "trace_id": lease.trace_id,
        "outcome": lease.outcome.value,
        "payload": lease.payload if admin else mask_payload(lease.payload or {}),
    }


def bundle_view(bundle: Any, *, admin: bool) -> dict[str, Any]:
    return {
        "bundle_id": bundle.bundle_id,
        "consumer": bundle.consumer,
        "members": {
            pool_name: {
                **{"pool": pool_name},
                **lease_view(lease, admin=admin),
            }
            for pool_name, lease in bundle.members.items()
        },
    }


def audit_view(entry: Any) -> dict[str, Any]:
    return {
        "seq": entry.seq,
        "ts": entry.ts,
        "operator": entry.operator,
        "action": entry.action,
        "pool": entry.pool,
        "rid": entry.rid,
        "reason": entry.reason,
        "trace_id": entry.trace_id,
        "detail": entry.detail,
    }
