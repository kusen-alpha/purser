"""Service metadata: usage reports, backup/restore, audit, schema, adapters."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Body, Header, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from starlette.responses import Response

from purser import __version__
from purser.config import config_schema_payload
from purser.core.views import UsageReport
from purser.errors import PurserError
from purser.ui._helpers import audit_view, field_type, http_purser_error, json_scalar
from purser.ui.context import UiContext

STATIC_DIR = Path(__file__).parent.parent / "static"
_STARTED = time.time()


def build(ctx: UiContext) -> APIRouter:
    ps = ctx.ps
    router = APIRouter()

    @router.get("/reports/usage", response_model=None)
    async def usage_report(
        request: Request,
        since: float | None = None,
        until: float | None = None,
        group_by: str = "consumer",
        pool: str | None = None,
        limit: int = 20,
    ) -> UsageReport:
        ctx.require(request, write=False)
        if pool is not None and ps.get_pool(pool, default=None) is None:
            raise HTTPException(status_code=404, detail=f"pool {pool!r} not found")
        if group_by not in ("consumer", "target", "pool", "kind"):
            raise HTTPException(
                status_code=422,
                detail="group_by must be one of consumer/target/pool/kind",
            )
        if limit < 1 or limit > 500:
            raise HTTPException(status_code=422, detail="limit must be in [1, 500]")
        try:
            return await ps.report_usage(
                since=since,
                until=until,
                group_by=group_by,
                pool=pool,
                limit=limit,
            )
        except PurserError as exc:
            raise http_purser_error(exc) from exc

    @router.get("/backup", response_model=None)
    async def backup(request: Request) -> dict[str, Any]:
        role = ctx.role(request)
        if role is None:
            raise HTTPException(status_code=401, detail="invalid or missing API key")
        if role != "admin":
            raise HTTPException(status_code=403, detail="admin role required for backup")
        operator = request.headers.get("x-operator") or "api"
        return await ps.backup(operator=operator)

    @router.post("/restore", response_model=None)
    async def restore(
        request: Request,
        body: dict[str, Any] = Body(...),  # noqa: B008
        conflict: str = "skip",
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    ) -> dict[str, Any]:
        operator, _ = ctx.require(request, write=True)
        if conflict not in ("skip", "overwrite"):
            raise HTTPException(
                status_code=422, detail="conflict must be 'skip' or 'overwrite'"
            )
        role = ctx.role(request) or "public"
        key = f"restore:{role}:{idempotency_key}" if idempotency_key else None
        if key:
            cached = await ps.idempotency.aget(key)
            if cached is not None:
                return {**cached, "replayed": True}
        try:
            report = await ps.restore_backup(body, conflict=conflict, operator=operator)
        except PurserError as exc:
            raise http_purser_error(exc) from exc
        if key:
            await ps.idempotency.aput(key, report)
        return report

    @router.get("/")
    async def index() -> Response:
        file = STATIC_DIR / "index.html"
        if file.exists():
            return FileResponse(file)
        return JSONResponse({"service": "purser"})

    @router.get("/service", response_model=None)
    async def service_status(request: Request) -> dict[str, Any]:
        ctx.require(request, write=False)
        return {
            "version": __version__,
            "uptime_s": round(time.time() - _STARTED, 1),
            "storage": ps.storage_kind,
            "coordination": ps.coordination_kind,
            "pools": ps.get_pool_names(),
            "adapters": sorted(ps.adapters.names()),
            "maintenance": ps.maintenance_enabled,
        }

    @router.get("/audit", response_model=None)
    async def audit_log(
        request: Request,
        after_seq: int = 0,
        before_seq: int | None = None,
        limit: int = 200,
        tail: bool = False,
        pool: str | None = None,
        action: str | None = None,
        rid: str | None = None,
        operator: str | None = None,
    ) -> dict[str, Any]:
        """Cursor replay by default; ``tail=true`` returns the newest page.

        ``after_seq`` always means "strictly newer than this seq", so a forward
        page must be the *first* ``limit`` matches.  Taking the last matches
        instead would silently skip the backlog.  ``before_seq`` pages backwards
        (strictly older) and reports ``has_older`` so the console can offer a
        "load earlier" control.
        """

        ctx.require(request, write=False)
        if after_seq < 0:
            raise HTTPException(status_code=422, detail="after_seq must be >= 0")
        if before_seq is not None and before_seq < 0:
            raise HTTPException(status_code=422, detail="before_seq must be >= 0")
        if limit < 1 or limit > 1000:
            raise HTTPException(status_code=422, detail="limit must be in 1..1000")
        matched = [
            audit_view(entry)
            for entry in ps.audit.entries(after_seq=after_seq)
            if (pool is None or entry.pool == pool)
            and (action is None or entry.action == action)
            and (rid is None or entry.rid == rid)
            and (operator is None or entry.operator == operator)
        ]
        if tail:
            entries = matched[-limit:]
            has_older = len(matched) > len(entries)
        elif before_seq is not None:
            older = [e for e in matched if e["seq"] < before_seq]
            entries = older[-limit:]
            has_older = len(older) > len(entries)
        else:
            entries = matched[:limit]
            has_older = False
        return {
            "entries": entries,
            "cursor": entries[-1]["seq"] if entries else after_seq,
            "has_older": has_older,
        }

    @router.get("/audit/verify", response_model=None)
    async def audit_verify(request: Request) -> dict[str, Any]:
        ctx.require(request, write=False)
        return {"ok": ps.audit.verify(), "entries": len(ps.audit)}

    @router.get("/config/schema", response_model=None)
    async def config_schema(request: Request) -> dict[str, Any]:
        """Pool/resource scheduling schema; the console renders from this."""

        ctx.require(request, write=False)
        return config_schema_payload()

    @router.get("/adapters", response_model=None)
    async def list_adapters(request: Request) -> dict[str, Any]:
        """Describe registered resource adapters so the console can render forms."""

        ctx.require(request, write=False)
        out = []
        for name in sorted(ps.adapters.names()):
            adapter = ps.adapters.get(name)
            out.append(
                {
                    "kind": adapter.kind,
                    "resource_class": adapter.resource_class.__name__,
                    "default_concurrency": adapter.default_concurrency,
                    "validators": [
                        {"name": spec.name, "both": spec.both}
                        for spec in adapter.validators()
                    ],
                    "fields": [
                        {
                            "name": spec.name,
                            "type": field_type(spec.coerce),
                            "required": spec.required,
                            "secret": spec.secret,
                            "default": json_scalar(spec.default),
                            "doc": spec.doc,
                        }
                        for spec in adapter.fields()
                    ],
                }
            )
        return {"adapters": out}

    return router
