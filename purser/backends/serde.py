"""Shared row (de)serialization for durable backends (S10).

The memory storage keeps live objects; sqlite and mysql persist rows, so
health/quota/site-failures/meta are stored as JSON.  Keeping the mapping here
guarantees identical round trips and means a new ``Resource`` field is added
in one place, not once per backend.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict
from typing import Any, Protocol

from purser.core.resource import Concurrency, HealthStats, Quota, Resource, SiteFailure
from purser.core.state import ResourceState

# Column order shared by the INSERT/UPDATE statements of every SQL backend.
STORAGE_COLUMNS: tuple[str, ...] = (
    "kind",
    "state",
    "max_slots",
    "used_slots",
    "payload",
    "tags",
    "health",
    "quota",
    "site_failures",
    "usage_quota",
    "used_count",
    "expires_at",
    "next_available_at",
    "mutex_group",
    "schedule_window",
    "meta",
    "updated_at",
)


class PayloadCodec(Protocol):
    """The subset of ``SecretCodec`` the row mapping depends on."""

    def encrypt_payload(
        self, payload: dict[str, Any], secret_fields: tuple[str, ...]
    ) -> dict[str, Any]: ...

    def decrypt_payload(
        self, stored: dict[str, Any], secret_fields: tuple[str, ...]
    ) -> dict[str, Any]: ...


def json_str(value: Any) -> str:
    return json.dumps(value)


def encode_health(h: HealthStats) -> str:
    return json.dumps(asdict(h))


def decode_health(raw: str) -> HealthStats:
    data = json.loads(raw) if raw else {}
    return HealthStats(**{k: data[k] for k in HealthStats.__dataclass_fields__ if k in data})


def encode_quota(q: Quota | None) -> str:
    return json.dumps(asdict(q)) if q else ""


def decode_quota(raw: str) -> Quota | None:
    data = json.loads(raw) if raw else {}
    return Quota(**data) if data else None


def encode_site_failures(f: dict[str, SiteFailure]) -> str:
    return json.dumps({site: asdict(fail) for site, fail in f.items()})


def decode_site_failures(raw: str) -> dict[str, SiteFailure]:
    data = json.loads(raw) if raw else {}
    return {site: SiteFailure(**item) for site, item in data.items()}


def encode_meta(meta: dict[str, Any]) -> str:
    return json.dumps(meta)


def serialize_row(
    resource: Resource,
    *,
    codec: PayloadCodec,
    secret_fields: tuple[str, ...],
    updated_at: float,
) -> dict[str, Any]:
    """Map a resource onto the shared SQL column set."""

    return {
        "kind": resource.kind,
        "state": resource.state.value,
        "max_slots": resource.concurrency.max_slots,
        "used_slots": resource.concurrency.used_slots,
        "payload": json_str(
            codec.encrypt_payload(resource.payload, secret_fields)
        ),
        "tags": json_str(sorted(resource.tags)),
        "health": encode_health(resource.health),
        "quota": encode_quota(resource.quota),
        "site_failures": encode_site_failures(resource.site_failures),
        "usage_quota": resource.usage_quota,
        "used_count": resource.used_count,
        "expires_at": resource.expires_at,
        "next_available_at": resource.next_available_at,
        "mutex_group": resource.mutex_group,
        "schedule_window": (
            json_str(list(resource.schedule_window))
            if resource.schedule_window
            else ""
        ),
        "meta": encode_meta(resource.meta),
        "updated_at": updated_at,
    }


def deserialize_row(
    row: Mapping[str, Any],
    *,
    codec: PayloadCodec,
    secret_fields: tuple[str, ...],
) -> Resource:
    """Rebuild a generic ``Resource`` from a stored row."""

    def parse(raw: str, default: Any) -> Any:
        return json.loads(raw) if raw else default

    payload = codec.decrypt_payload(parse(row["payload"], {}), secret_fields)
    return Resource(
        id=row["id"],
        kind=row["kind"],
        payload=payload,
        state=ResourceState(row["state"]),
        version=int(row["version"]),
        tags=frozenset(parse(row["tags"], [])),
        concurrency=Concurrency(
            max_slots=int(row["max_slots"]),
            used_slots=int(row["used_slots"]),
        ),
        health=decode_health(row["health"] or ""),
        site_failures=decode_site_failures(row["site_failures"] or "{}"),
        quota=decode_quota(row["quota"] or ""),
        usage_quota=row["usage_quota"],
        used_count=int(row["used_count"]),
        expires_at=row["expires_at"],
        next_available_at=row["next_available_at"],
        mutex_group=row["mutex_group"],
        schedule_window=(
            tuple(parse(row["schedule_window"], []))
            if row["schedule_window"]
            else None
        ),
        meta=parse(row["meta"], {}),
    )
