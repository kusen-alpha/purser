"""Per-resource write actions and lease revocation."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from fastapi import APIRouter, Body, HTTPException, Request

from purser.errors import PurserError
from purser.ui._helpers import coerce, http_purser_error, state_value
from purser.ui.context import UiContext

WriteAction = Callable[[Any, str, dict[str, Any] | None, str], Any]


def _write_actions() -> dict[str, WriteAction]:
    return {
        "ban": lambda pool, rid, b, op: pool.ban(
            rid, reason=(b or {}).get("reason") or "api ban", operator=op
        ),
        "unban": lambda pool, rid, b, op: pool.unban(rid, operator=op, reason=(b or {}).get("reason") or "api unban"),
        "suspend": lambda pool, rid, b, op: pool.suspend(
            rid, reason=(b or {}).get("reason") or "api suspend", operator=op
        ),
        "resume": lambda pool, rid, b, op: pool.resume(rid, operator=op, reason=(b or {}).get("reason") or "api resume"),
        "top-up": lambda pool, rid, b, op: pool.top_up(
            rid, operator=op,
            amount=coerce((b or {}).get("amount"), name="amount", cast=int, minimum=1),
            reason=(b or {}).get("reason") or "api top-up",
        ),
        "drain": lambda pool, rid, b, op: pool.drain(
            rid, reason=(b or {}).get("reason") or "api drain", operator=op
        ),
        "cancel-drain": lambda pool, rid, b, op: pool.cancel_drain(
            rid, reason=(b or {}).get("reason") or "api cancel-drain", operator=op
        ),
        "restore": lambda pool, rid, b, op: pool.restore_resource(rid, operator=op, reason=(b or {}).get("reason") or "api restore"),
    }


def build(ctx: UiContext) -> APIRouter:
    ps = ctx.ps
    router = APIRouter()
    write_actions = _write_actions()

    # Registered before the `{action}` route so the static `revoke` segment
    # wins over the dynamic catch-all (Starlette matches routes in order).
    @router.post("/pools/{pool_name}/resources/{rid}/revoke", response_model=None)
    async def revoke_resource(
        request: Request,
        pool_name: str,
        rid: str,
        body: dict[str, Any] | None = Body(None),  # noqa: B008
    ) -> dict[str, Any]:
        operator, _ = ctx.require(request, write=True)
        pool = ctx.pool_of(pool_name)
        try:
            lease = await pool.force_revoke(
                rid,
                operator=operator,
                reason=(body or {}).get("reason") or "api revoke",
                token=(body or {}).get("token"),
            )
        except PurserError as exc:
            raise http_purser_error(exc) from exc
        return {"ok": True, "revoked_token": lease.token}

    @router.post(
        "/pools/{pool_name}/resources/{rid}/move", response_model=None
    )
    async def move_resource(
        request: Request,
        pool_name: str,
        rid: str,
        body: dict[str, Any] = Body(...),  # noqa: B008
    ) -> dict[str, Any]:
        operator, _ = ctx.require(request, write=True)
        pool = ctx.pool_of(pool_name)
        target_name = body.get("target_pool")
        if not isinstance(target_name, str) or not target_name:
            raise HTTPException(
                status_code=422, detail="target_pool must be a non-empty string"
            )
        if target_name == pool_name:
            raise HTTPException(status_code=422, detail="target_pool must differ")
        try:
            target = ps.get_pool(target_name)
        except PurserError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        try:
            moved = await pool.move_resource(
                rid,
                target,
                operator=operator,
                reason=body.get("reason") or "api move",
            )
        except PurserError as exc:
            raise http_purser_error(exc) from exc
        return {
            "ok": True,
            "rid": rid,
            "target_pool": target_name,
            "resource": target.describe(moved),
        }

    @router.post("/pools/{pool_name}/resources/{rid}/{action}", response_model=None)
    async def resource_action(
        request: Request,
        pool_name: str,
        rid: str,
        action: str,
        body: dict[str, Any] | None = Body(None),  # noqa: B008
    ) -> dict[str, Any]:
        operator, _ = ctx.require(request, write=True)
        pool = ctx.pool_of(pool_name)
        fn = write_actions.get(action)
        if action == "validate":
            check = (body or {}).get("check")
            try:
                if check:
                    report = await pool.validate_check(rid, check)
                    audit_action = "validate_check"
                else:
                    report = await pool.validate_now(rid, operator=operator)
                    audit_action = "validate"
            except PurserError as exc:
                raise http_purser_error(exc) from exc
            ps.audit.record(
                operator=operator,
                action=audit_action,
                pool=pool_name,
                rid=rid,
                reason=(body or {}).get("reason"),
                detail={
                    "alive": report.alive,
                    "latency_ms": report.latency_ms,
                    "error": report.error,
                    **({"check": check} if check else {}),
                },
            )
            res = await pool.get(rid)
            return {
                "ok": True,
                "rid": rid,
                "state": res.state.value if res else None,
                "validation": {
                    "alive": report.alive,
                    "latency_ms": report.latency_ms,
                    "error": report.error,
                    "action": report.action.value,
                    **({"check": check} if check else {}),
                },
            }
        if action == "admin-state":
            target = state_value((body or {}).get("target"))
            try:
                await pool.admin_set_state(
                    rid,
                    target=target,
                    operator=operator,
                    reason=(body or {}).get("reason") or "api admin",
                )
            except PurserError as exc:
                raise http_purser_error(exc) from exc
            return {"ok": True, "rid": rid, "state": target.value}
        if fn is None:
            raise HTTPException(status_code=404, detail=f"unknown action {action!r}")
        try:
            await fn(pool, rid, body, operator)
        except PurserError as exc:
            raise http_purser_error(exc) from exc
        res = await pool.get(rid)
        return {"ok": True, "rid": rid, "state": res.state.value if res else None}

    return router
