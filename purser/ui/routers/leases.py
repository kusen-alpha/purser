"""Lease acquisition, lookup, renewal and release."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, Header, HTTPException, Request

from purser.core.bundle import bundle_request_from_dict
from purser.core.resource import AcquireRequest
from purser.errors import PurserError
from purser.ui._helpers import (
    bundle_view,
    coerce,
    http_purser_error,
    lease_view,
    outcome_value,
)
from purser.ui.context import UiContext


def build(ctx: UiContext) -> APIRouter:
    ps = ctx.ps
    router = APIRouter()

    @router.post("/pools/{pool_name}/leases", response_model=None)
    async def acquire_lease(
        request: Request,
        pool_name: str,
        body: dict[str, Any] = Body(...),  # noqa: B008
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    ) -> dict[str, Any]:
        _, trace_id = ctx.require(request, write=True)
        pool = ctx.pool_of(pool_name)
        role = ctx.role(request) or "public"
        admin = role == "admin"
        key = f"lease:{pool.name}:{role}:{idempotency_key}" if idempotency_key else None
        if key:
            cached = await ps.idempotency.aget(key)
            if cached is not None:
                return {**cached, "replayed": True}
        bundle_raw = body.get("bundle")
        if bundle_raw is not None:
            if not isinstance(bundle_raw, dict):
                raise HTTPException(status_code=422, detail="bundle must be an object")
            slots_raw = bundle_raw.get("slots")
            if not isinstance(slots_raw, list) or not any(
                isinstance(slot, dict) and slot.get("pool") == pool_name
                for slot in slots_raw
            ):
                raise HTTPException(
                    status_code=422,
                    detail="bundle must include the anchor pool in slots",
                )
            bundle_raw.setdefault("consumer", body.get("consumer"))
            try:
                bundle_req = bundle_request_from_dict(bundle_raw)
                bundle = await ps.acquire_bundle(bundle_req)
            except PurserError as exc:
                raise http_purser_error(exc) from exc
            bundle_out = bundle_view(bundle, admin=admin)
            if key:
                await ps.idempotency.aput(key, bundle_out)
            return bundle_out
        tags_raw = body.get("tags") or ()
        if not isinstance(tags_raw, (list, tuple, set, frozenset)):
            raise HTTPException(status_code=422, detail="tags must be a list")
        if any(not isinstance(tag, str) or not tag for tag in tags_raw):
            raise HTTPException(
                status_code=422, detail="tags must contain non-empty strings"
            )
        optional_strings = {
            "rid": body.get("rid"),
            "required_tag": body.get("required_tag"),
            "consumer": body.get("consumer"),
            "target": body.get("target"),
            "probe": body.get("probe"),
        }
        for name, value in optional_strings.items():
            if value is not None and not isinstance(value, str):
                raise HTTPException(status_code=422, detail=f"{name} must be a string")
        for name in ("affinity",):
            if body.get(name) is not None:
                raise HTTPException(
                    status_code=422,
                    detail=(
                        f"{name} is not implemented; "
                        + (
                            "enable PoolConfig(affinity_weight=...) for affinity"
                        )
                    ),
                )
        mode = body.get("mode", "atomic")
        if mode not in ("atomic", "best_effort"):
            raise HTTPException(
                status_code=422, detail="mode must be 'atomic' or 'best_effort'"
            )
        req = AcquireRequest(
            consumer=body.get("consumer"),
            target=body.get("target"),
            priority=coerce(body.get("priority", 0), name="priority", cast=int, minimum=0),
            slots=coerce(body.get("slots", 1), name="slots", cast=int, minimum=1),
            tags=frozenset(tags_raw),
            timeout=coerce(body.get("timeout"), name="timeout", cast=float, minimum=0),
            trace_id=trace_id or body.get("trace_id"),
            use_limit=coerce(body.get("use_limit"), name="use_limit", cast=int, minimum=1),
            allow_duplicate=bool(body.get("allow_duplicate")),
            ignore_cooldown=bool(body.get("ignore_cooldown")),
            rid=body.get("rid"),
            required_tag=body.get("required_tag"),
            probe=body.get("probe"),
        )
        count = 0
        n_min: int | None = None
        try:
            if body.get("count"):
                count = coerce(body.get("count"), name="count", cast=int, minimum=1)
                n_min = coerce(body.get("n_min"), name="n_min", cast=int, minimum=1)
                batch = await pool.acquire_many(
                    count, req=req, mode=mode, n_min=n_min
                )
                leases = batch.leases
            else:
                leases = [await pool.acquire(req)]
        except PurserError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        out: dict[str, Any] = {
            "leases": [lease_view(lease, admin=admin) for lease in leases],
            "count": len(leases),
        }
        if count:
            out.update({"requested": count, "mode": mode, "n_min": n_min})
        if key:
            await ps.idempotency.aput(key, out)
        return out

    @router.get("/pools/{pool_name}/leases", response_model=None)
    async def list_leases(
        request: Request,
        pool_name: str,
        consumer: str | None = None,
        rid: str | None = None,
    ) -> dict[str, Any]:
        ctx.require(request, write=False)
        pool = ctx.pool_of(pool_name)
        role = ctx.role(request) or "public"
        admin = role == "admin"
        # Read through the coordination plane so leases issued by other
        # instances sharing it are visible here too.
        leases = await pool.leases(rid)
        if consumer is not None:
            leases = [lease for lease in leases if lease.consumer == consumer]
        return {
            "leases": [lease_view(lease, admin=admin) for lease in leases],
            "count": len(leases),
        }

    @router.get("/pools/{pool_name}/leases/{token}", response_model=None)
    async def get_lease(
        request: Request, pool_name: str, token: str
    ) -> dict[str, Any]:
        ctx.require(request, write=False)
        pool = ctx.pool_of(pool_name)
        lease = await pool.lease_by_token(token)
        if lease is None:
            raise HTTPException(status_code=404, detail="lease not found on this pool")
        role = ctx.role(request) or "public"
        return {"lease": lease_view(lease, admin=role == "admin")}

    @router.delete("/pools/{pool_name}/leases", response_model=None)
    async def release_consumer_leases(
        request: Request,
        pool_name: str,
        consumer: str = "",
        outcome: str | None = None,
    ) -> dict[str, Any]:
        operator, _ = ctx.require(request, write=True)
        pool = ctx.pool_of(pool_name)
        name = consumer.strip()
        if not name:
            raise HTTPException(status_code=422, detail="consumer is required")
        parsed = outcome_value(outcome)
        try:
            leases = await pool.release_consumer(
                name, outcome=parsed, operator=operator
            )
        except PurserError as exc:
            raise http_purser_error(exc) from exc
        return {
            "ok": True,
            "consumer": name,
            "outcome": parsed.value,
            "count": len(leases),
            "tokens": [lease.token for lease in leases],
        }

    @router.post("/pools/{pool_name}/leases/{token}/renew", response_model=None)
    async def renew_lease(
        request: Request,
        pool_name: str,
        token: str,
        body: dict[str, Any] | None = Body(None),  # noqa: B008
    ) -> dict[str, Any]:
        ctx.require(request, write=True)
        pool = ctx.pool_of(pool_name)
        body = body or {}
        lease = await pool.lease_by_token(token)
        if lease is None:
            raise HTTPException(status_code=404, detail="lease not found on this pool")
        used = coerce(body.get("used"), name="used", cast=int, minimum=0)
        try:
            await pool.renew(lease, used=used)
        except PurserError as exc:
            raise http_purser_error(exc) from exc
        return {
            "ok": True,
            "token": token,
            "expires_at": lease.expires_at,
            "used": lease.used,
        }

    @router.delete("/pools/{pool_name}/leases/{token}", response_model=None)
    async def release_lease(
        request: Request,
        pool_name: str,
        token: str,
        body: dict[str, Any] | None = Body(None),  # noqa: B008
    ) -> dict[str, Any]:
        ctx.require(request, write=True)
        pool = ctx.pool_of(pool_name)
        body = body or {}
        lease = await pool.lease_by_token(token)
        if lease is None:
            raise HTTPException(status_code=404, detail="lease not found on this pool")
        outcome = outcome_value(body.get("outcome"))
        used = coerce(body.get("used"), name="used", cast=int, minimum=0)
        try:
            await pool.release(lease, outcome=outcome, used=used)
        except PurserError as exc:
            raise http_purser_error(exc) from exc
        return {"ok": True, "token": token, "outcome": outcome.value}

    return router
