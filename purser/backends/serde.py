"""Shared JSON (de)serialization helpers for durable backends (S10).

The memory storage keeps live objects; the sqlite storage persists rows, so
health/quota/site-failures/meta are stored as JSON.  Keeping the codecs here
guarantees identical round trips across storage implementations.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from typing import Any

from purser.core.resource import HealthStats, Quota, SiteFailure


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
