"""Pool CRUD and stats."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, Header, HTTPException, Request

from purser.core.views import PoolStats
from purser.errors import PoolExists, PurserError
from purser.ui._helpers import clean_pool_config, http_purser_error
from purser.ui.context import UiContext
from purser.ui.persistence import save_pools


def build(ctx: UiContext) -> APIRouter:
    ps = ctx.ps
    router = APIRouter()

    @router.get("/pools", response_model=None)
    async def list_pools(request: Request) -> dict[str, Any]:
        ctx.require(request, write=False)
        out = []
        for pool in ps.pools.values():
            out.append(await pool.stats())
        return {"pools": out}

    @router.post("/pools", response_model=None)
    async def create_pool(
        request: Request,
        body: dict[str, Any] = Body(...),  # noqa: B008
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    ) -> dict[str, Any]:
        ctx.require(request, write=True)

        def do() -> dict[str, Any]:
            name = body.get("name")
            kind = body.get("kind")
            if not name or not kind:
                raise HTTPException(status_code=422, detail="name and kind required")
            raw_config = body.get("config") or {}
            profile = body.get("profile")
            if isinstance(raw_config, dict):
                raw_config = dict(raw_config)
                embedded = raw_config.pop("profile", None)
                if profile is None:
                    profile = embedded
            config = clean_pool_config(raw_config)
            try:
                pool = ps.create_pool(
                    name, kind, config=config, profile=profile
                )
            except PoolExists:
                raise HTTPException(
                    status_code=409, detail=f"pool {name!r} already exists"
                ) from None
            except (PurserError, TypeError) as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc
            save_pools(ps)
            payload = {"pool": name, "kind": pool.cfg.kind}
            if pool.profile:
                payload["profile"] = pool.profile
            return payload

        return await ctx.idempotent(idempotency_key, do)

    @router.get("/pools/{pool_name}", response_model=None)
    async def pool_stats(request: Request, pool_name: str) -> PoolStats:
        ctx.require(request, write=False)
        return await ctx.pool_of(pool_name).stats()

    @router.patch("/pools/{pool_name}", response_model=None)
    async def update_pool(
        request: Request,
        pool_name: str,
        body: dict[str, Any] = Body(...),  # noqa: B008
    ) -> dict[str, Any]:
        operator, _ = ctx.require(request, write=True)
        pool = ctx.pool_of(pool_name)
        raw = body["config"] if "config" in body else body
        # Unchanged immutable knobs from a stats() round-trip are dropped by
        # the parser; a real change raises ConfigError -> 422.
        overrides = clean_pool_config(raw, mode="patch", base=pool.cfg)
        try:
            cfg = pool.reconfigure(**overrides)
        except (PurserError, TypeError, ValueError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        ps.audit.record(
            operator=operator,
            action="update_pool_config",
            pool=pool_name,
            detail={"applied": list(overrides)},
        )
        save_pools(ps)
        return {"pool": pool_name, "config": cfg.to_dict()}

    @router.delete("/pools/{pool_name}", response_model=None)
    async def delete_pool(
        request: Request,
        pool_name: str,
        force: bool = False,
    ) -> dict[str, Any]:
        operator, _ = ctx.require(request, write=True)
        ctx.pool_of(pool_name)
        try:
            result = await ps.delete_pool(pool_name, force=force, operator=operator)
        except PurserError as exc:
            raise http_purser_error(exc) from exc
        save_pools(ps)
        return {"ok": True, **result}

    return router
