"""Per-app service context: authentication, pool lookup and idempotency."""

from __future__ import annotations

import hmac
from collections.abc import Callable
from typing import Any

from fastapi import HTTPException, Request

from purser import Purser, ResourcePool


def _bearer(request: Request) -> str | None:
    header = request.headers.get("authorization")
    if header and header.lower().startswith("bearer "):
        return str(header[7:]).strip()
    return None


class UiContext:
    """Closures over one ``Purser`` instance and its API keys."""

    def __init__(
        self,
        ps: Purser,
        *,
        api_key: str | None = None,
        readonly_key: str | None = None,
    ) -> None:
        self.ps = ps
        self.api_key = api_key
        self.readonly_key = readonly_key

    def role(self, request: Request) -> str | None:
        """Return ``admin`` / ``readonly`` or None when unauthorized."""

        token = _bearer(request)
        if not token:
            # Without any configured key the service is fully open: everyone is
            # treated as admin, so include_secrets actually works.
            return (
                "admin"
                if self.api_key is None and self.readonly_key is None
                else None
            )
        if self.api_key and hmac.compare_digest(token, self.api_key):
            return "admin"
        if self.readonly_key and hmac.compare_digest(token, self.readonly_key):
            return "readonly"
        return None

    def require(self, request: Request, *, write: bool) -> tuple[str, str | None]:
        role = self.role(request)
        if role is None:
            raise HTTPException(status_code=401, detail="invalid or missing API key")
        if write and role == "readonly":
            raise HTTPException(status_code=403, detail="readonly role cannot write")
        operator = request.headers.get("x-operator") or "api"
        trace_id = request.headers.get("x-trace-id")
        return operator, trace_id

    def pool_of(self, name: str) -> ResourcePool:
        pool = self.ps.get_pool(name, default=None)
        if pool is None:
            raise HTTPException(status_code=404, detail=f"pool {name!r} not found")
        return pool

    async def idempotent(
        self, key: str | None, fn: Callable[[], dict[str, Any]]
    ) -> dict[str, Any]:
        if not key:
            return fn()
        cached = await self.ps.idempotency.aget(key)
        if cached is not None:
            return cached
        result = fn()
        await self.ps.idempotency.aput(key, result)
        return result
