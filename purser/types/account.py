"""Account resource type (S3): manual validation by default, hooks exposed."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from purser.core.health import HealthReport
from purser.core.resource import PayloadField, Resource
from purser.types.base import ResourceType

ACCOUNT_FIELDS = (
    PayloadField("username"),
    PayloadField("email"),
    PayloadField("password", secret=True),
    PayloadField("cookie", secret=True),
    PayloadField("access_token", secret=True),
    PayloadField("refresh_token", secret=True),
)


@dataclass
class AccountResource(Resource):
    """Typed account resource; ``payload`` merges these fields with custom keys."""

    kind: str = "account"
    payload_fields = ACCOUNT_FIELDS
    username: str | None = None
    email: str | None = None
    password: str | None = None
    cookie: str | None = None
    access_token: str | None = None
    refresh_token: str | None = None


class AccountType(ResourceType):
    kind = "account"
    default_concurrency = 4
    resource_class = AccountResource

    async def generate(self, spec: dict[str, Any]) -> Resource:
        username = spec.get("username") or spec.get("email") or spec.get("id")
        if not username:
            raise ValueError("account spec requires username/email/id")
        payload = {k: v for k, v in spec.items() if v is not None}
        rid = spec.get("id") or f"account://{username}"
        return self.adopt(
            AccountResource(
                id=rid,
                kind=self.kind,
                payload=payload,
                tags=frozenset(spec.get("tags") or ()),
                meta={"source": spec["source"]} if spec.get("source") else {},
                username=spec.get("username"),
                email=spec.get("email"),
                password=spec.get("password"),
                cookie=spec.get("cookie"),
                access_token=spec.get("access_token"),
                refresh_token=spec.get("refresh_token"),
            )
        )

    async def validate(self, resource: Resource) -> HealthReport:
        # Design decision S3: accounts are manual-marked by default and never
        # hit the network unless a subclass overrides validate/on_request_check.
        return HealthReport.ok()
