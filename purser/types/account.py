"""Account resource type (S3): manual validation by default, hooks exposed."""

from __future__ import annotations

from typing import Any

from purser.core.health import HealthReport
from purser.core.resource import Resource
from purser.types.base import ResourceType


class AccountType(ResourceType):
    kind = "account"
    default_concurrency = 4
    secret_fields = ("password", "cookie", "access_token", "refresh_token")

    async def generate(self, spec: dict[str, Any]) -> Resource:
        username = spec.get("username") or spec.get("email") or spec.get("id")
        if not username:
            raise ValueError("account spec requires username/email/id")
        payload = {k: v for k, v in spec.items() if v is not None}
        rid = spec.get("id") or f"account://{username}"
        return Resource(
            id=rid,
            kind=self.kind,
            payload=payload,
            tags=frozenset(spec.get("tags") or ()),
            meta={"source": spec.get("source", "manual")},
        )

    async def validate(self, resource: Resource) -> HealthReport:
        # Design decision S3: accounts are manual-marked by default and never
        # hit the network unless a subclass overrides validate/on_request_check.
        return HealthReport.ok()
