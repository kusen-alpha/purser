"""Resource listing, import, update and deletion."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, Header, HTTPException, Request

from purser.core.state import ResourceState
from purser.errors import PurserError
from purser.ui._helpers import (
    coerce,
    http_purser_error,
    optional_epoch,
    resource_view,
)
from purser.ui.context import UiContext


def _state(s: str | None) -> set[ResourceState] | None:
    if not s:
        return None
    names = [part.strip() for part in s.split(",") if part.strip()]
    try:
        return {ResourceState(n) for n in names}
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=f"bad state value(s): {s}") from exc


def build(ctx: UiContext) -> APIRouter:
    ps = ctx.ps
    router = APIRouter()

    @router.get("/pools/{pool_name}/resources", response_model=None)
    async def list_resources(
        request: Request,
        pool_name: str,
        state: str | None = None,
        tag: str | None = None,
        q: str | None = None,
        sort: str | None = None,
        order: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, Any]:
        ctx.require(request, write=False)
        pool = ctx.pool_of(pool_name)
        if limit < 1 or limit > 1000:
            raise HTTPException(status_code=422, detail="limit must be in 1..1000")
        if offset < 0:
            raise HTTPException(status_code=422, detail="offset must be >= 0")
        sort_key = sort or "id"
        if sort_key not in ("id", "health", "recent"):
            raise HTTPException(
                status_code=422, detail=f"sort must be one of id, health, recent (got {sort!r})"
            )
        direction = (order or ("asc" if sort_key == "id" else "desc")).lower()
        if direction not in ("asc", "desc"):
            raise HTTPException(
                status_code=422, detail=f"order must be asc or desc (got {order!r})"
            )
        states = _state(state)
        tags = (
            frozenset(part.strip() for part in tag.split(",") if part.strip())
            if tag
            else None
        ) or None
        search = (q or "").strip() or None
        resources = await pool.query(
            states=states,
            tags=tags,
            limit=limit,
            offset=offset,
            search=search,
            sort=sort,
            order=direction if sort else None,
        )
        total = await pool.count_matching(states=states, tags=tags, search=search)
        return {
            "resources": [
                resource_view(pool, r, include_secrets=False, admin=False)
                for r in resources
            ],
            "counts": await pool.counts(by_name=True),
            "total": total,
            "limit": limit,
            "offset": offset,
            "sort": sort_key,
            "order": direction,
        }

    @router.get("/pools/{pool_name}/resources/{rid}", response_model=None)
    async def get_resource(
        request: Request,
        pool_name: str,
        rid: str,
        include_secrets: bool = False,
    ) -> dict[str, Any]:
        ctx.require(request, write=False)
        role = ctx.role(request) or "public"
        pool = ctx.pool_of(pool_name)
        resource = await pool.get(rid)
        if resource is None:
            raise HTTPException(status_code=404, detail=f"resource {rid!r} not found")
        if include_secrets and role != "admin" and ctx.api_key is not None:
            raise HTTPException(status_code=403, detail="admin role required for secrets")
        return resource_view(pool, resource, include_secrets=include_secrets, admin=role == "admin")

    @router.post("/pools/{pool_name}/resources", response_model=None)
    async def import_resources(
        request: Request,
        pool_name: str,
        body: dict[str, Any] = Body(...),  # noqa: B008
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    ) -> dict[str, Any]:
        operator, _ = ctx.require(request, write=True)
        pool = ctx.pool_of(pool_name)
        key = idempotency_key
        if key:
            cached = await ps.idempotency.aget(key)
            if cached is not None:
                return cached
        records = body.get("records") or []
        if not isinstance(records, list):
            raise HTTPException(status_code=422, detail="records must be a list")
        try:
            report = await pool.import_resources(
                records,
                dedup_key=body.get("dedup_key") or "id",
                source=body.get("source") or "manual",
                operator=operator,
                fail_fast=bool(body.get("fail_fast")),
                import_validate=body.get("import_validate"),
                batch_id=body.get("batch_id"),
            )
        except PurserError as exc:
            raise http_purser_error(exc) from exc
        result = report.to_dict()
        if key:
            await ps.idempotency.aput(key, result)
        return result

    @router.put("/pools/{pool_name}/resources/{rid}", response_model=None)
    async def update_resource(
        request: Request,
        pool_name: str,
        rid: str,
        body: dict[str, Any] = Body(...),  # noqa: B008
    ) -> dict[str, Any]:
        operator, _ = ctx.require(request, write=True)
        pool = ctx.pool_of(pool_name)
        payload = body.get("payload")
        if payload is not None and not isinstance(payload, dict):
            raise HTTPException(status_code=422, detail="payload must be an object")
        tags_raw = body.get("tags")
        if tags_raw is not None and not isinstance(tags_raw, (list, set, frozenset)):
            raise HTTPException(status_code=422, detail="tags must be a list")
        tags = frozenset(tags_raw) if tags_raw is not None else None
        extra: dict[str, Any] = {}
        if "usage_quota" in body:
            extra["usage_quota"] = (
                None
                if body["usage_quota"] is None
                else coerce(
                    body["usage_quota"], name="usage_quota", cast=int, minimum=0
                )
            )
        if "quota_limit" in body:
            extra["quota_limit"] = (
                None
                if body["quota_limit"] is None
                else coerce(
                    body["quota_limit"], name="quota_limit", cast=int, minimum=1
                )
            )
        if "expires_at" in body:
            extra["expires_at"] = optional_epoch(body["expires_at"], "expires_at")
        if "next_available_at" in body:
            extra["next_available_at"] = optional_epoch(
                body["next_available_at"], "next_available_at"
            )
        if "mutex_group" in body:
            mutex = body["mutex_group"]
            if mutex is not None and not isinstance(mutex, str):
                raise HTTPException(status_code=422, detail="mutex_group must be a string")
            extra["mutex_group"] = mutex
        if "schedule_window" in body:
            window = body["schedule_window"]
            if window is not None and not isinstance(window, (list, tuple)):
                raise HTTPException(
                    status_code=422, detail="schedule_window must be [start, end]"
                )
            extra["schedule_window"] = window
        if "priority" in body:
            value = body["priority"]
            extra["priority"] = (
                None
                if value is None
                else coerce(value, name="priority", cast=int)
            )
        if "preference" in body:
            value = body["preference"]
            extra["preference"] = (
                None
                if value is None
                else coerce(value, name="preference", cast=float)
            )
        try:
            updated = await pool.admin.update_resource(
                rid,
                payload=payload,
                tags=tags,
                max_slots=coerce(body.get("max_slots"), name="max_slots", cast=int, minimum=1),
                operator=operator,
                reason=body.get("reason") or "api update",
                **extra,
            )
        except PurserError as exc:
            raise http_purser_error(exc) from exc
        return {"ok": True, "resource": pool.describe(updated)}

    @router.delete("/pools/{pool_name}/resources/{rid}", response_model=None)
    async def delete_resource(
        request: Request,
        pool_name: str,
        rid: str,
        body: dict[str, Any] | None = Body(None),  # noqa: B008
    ) -> dict[str, Any]:
        operator, _ = ctx.require(request, write=True)
        pool = ctx.pool_of(pool_name)
        resource = await pool.get(rid)
        if resource is None:
            raise HTTPException(status_code=404, detail=f"resource {rid!r} not found")
        if resource.concurrency.used_slots > 0:
            try:
                await pool.admin.drain(rid, operator=operator, reason="api delete")
            except PurserError as exc:
                raise http_purser_error(exc) from exc
            return {"ok": True, "rid": rid, "state": "draining"}
        try:
            await pool.admin.admin_set_state(
                rid,
                target=ResourceState.ARCHIVED,
                operator=operator,
                reason="api delete",
            )
        except PurserError as exc:
            raise http_purser_error(exc) from exc
        return {"ok": True, "rid": rid, "state": "archived"}

    return router
