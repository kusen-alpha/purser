"""FastAPI service: REST API, SSE events, health endpoints (8.2 / C2 / D1)."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import Body, FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from starlette.responses import Response

from purser import Purser, ResourcePool
from purser.core.resource import AcquireRequest, Resource, UsageOutcome
from purser.core.state import ResourceState
from purser.crypto import mask_payload
from purser.errors import ConfigError, PoolExists, PurserError


def _http_purser_error(exc: PurserError) -> HTTPException:
    """Map a library error to a client error instead of a 500."""

    status = 422 if isinstance(exc, ConfigError) else 409
    return HTTPException(status_code=status, detail=str(exc))

STATIC_DIR = Path(__file__).parent / "static"


def _bearer(request: Request) -> str | None:
    header = request.headers.get("authorization")
    if header and header.lower().startswith("bearer "):
        return str(header[7:]).strip()
    return None


def _role(request: Request, api_key: str | None, readonly_key: str | None) -> str | None:
    """Return ``admin`` / ``readonly`` or None when unauthorized."""

    token = _bearer(request)
    if not token:
        # Without any configured key the service is fully open: everyone is
        # treated as admin, so include_secrets actually works.
        return "admin" if api_key is None and readonly_key is None else None
    if api_key and token == api_key:
        return "admin"
    if readonly_key and token == readonly_key:
        return "readonly"
    return None


def _resource_view(
    pool: ResourcePool,
    resource: Resource,
    *,
    include_secrets: bool,
    admin: bool,
) -> dict[str, Any]:
    secret = bool(admin and include_secrets)
    return pool.describe(resource, secret=secret)


def _state_value(value: Any) -> ResourceState:
    if not isinstance(value, str) or not value:
        raise HTTPException(status_code=422, detail=f"state must be a non-empty string, got {value!r}")
    try:
        return ResourceState(value)
    except ValueError:
        raise HTTPException(status_code=422, detail=f"unknown state {value!r}") from None


def _outcome_value(value: Any) -> UsageOutcome:
    if value is None:
        return UsageOutcome.OK
    if isinstance(value, UsageOutcome):
        return value
    try:
        return UsageOutcome(str(value))
    except ValueError:
        raise HTTPException(status_code=422, detail=f"unknown outcome {value!r}") from None


def _coerce(
    value: Any,
    *,
    name: str,
    cast: Callable[[Any], Any],
    minimum: int | float | None = None,
) -> Any:
    if value is None:
        return None
    try:
        out = cast(value)
    except (TypeError, ValueError):
        raise HTTPException(status_code=422, detail=f"{name} must be {cast.__name__}") from None
    if minimum is not None and out < minimum:
        raise HTTPException(status_code=422, detail=f"{name} must be >= {minimum}")
    return out


def build_purser() -> Purser:
    ps = Purser()
    if os.environ.get("PURSER_REDIS_URL"):
        ps.redis_url = os.environ["PURSER_REDIS_URL"]
    return ps


def create_app(
    purser: Purser | None = None,
    *,
    api_key: str | None = None,
    readonly_key: str | None = None,
) -> FastAPI:
    ps = purser or build_purser()
    api_key = api_key if api_key is not None else os.environ.get("PURSER_API_KEY")
    readonly_key = (
        readonly_key
        if readonly_key is not None
        else os.environ.get("PURSER_READONLY_KEY")
    )

    @contextlib.asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        await ps.start()
        yield
        await ps.close()

    app = FastAPI(title="purSer", version="0.2.0", lifespan=lifespan)
    app.state.purser = ps

    def require(request: Request, *, write: bool) -> tuple[str, str | None]:
        role = _role(request, api_key, readonly_key)
        if role is None:
            raise HTTPException(status_code=401, detail="invalid or missing API key")
        if write and role == "readonly":
            raise HTTPException(status_code=403, detail="readonly role cannot write")
        operator = request.headers.get("x-operator") or "api"
        trace_id = request.headers.get("x-trace-id")
        return operator, trace_id

    def pool_of(name: str) -> ResourcePool:
        pool = ps.get_pool(name)
        if pool is None:
            raise HTTPException(status_code=404, detail=f"pool {name!r} not found")
        return pool

    def idempotent(
        key: str | None, fn: Callable[[], dict[str, Any]]
    ) -> dict[str, Any]:
        if not key:
            return fn()
        cached = ps.idempotency.get(key)
        if cached is not None:
            return cached
        result = fn()
        ps.idempotency.put(key, result)
        return result

    # -- health ------------------------------------------------------------

    @app.get("/healthz", response_model=None)
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz", response_model=None)
    async def readyz() -> dict[str, Any]:
        return {"status": "ready", "pools": ps.pool_names()}

    @app.get("/metrics")
    async def metrics() -> JSONResponse:
        return JSONResponse(
            content=ps.metrics.render_prometheus(),
            media_type="text/plain",
        )

    @app.get("/")
    async def index() -> Response:
        file = STATIC_DIR / "index.html"
        if file.exists():
            return FileResponse(file)
        return JSONResponse({"service": "purser"})

    # -- pools -------------------------------------------------------------

    @app.get("/pools", response_model=None)
    async def list_pools(request: Request) -> dict[str, Any]:
        require(request, write=False)
        out = []
        for name in ps.pool_names():
            pool = ps.get_pool(name)
            if pool is not None:
                out.append(await pool.stats())
        return {"pools": out}

    @app.post("/pools", response_model=None)
    async def create_pool(
        request: Request,
        body: Dict[str, Any] = Body(...),  # noqa: B008
        idempotency_key: Optional[str] = Header(default=None, alias="Idempotency-Key"),
    ) -> dict[str, Any]:
        operator, _ = require(request, write=True)

        def do() -> dict[str, Any]:
            name = body.get("name")
            kind = body.get("kind")
            if not name or not kind:
                raise HTTPException(status_code=422, detail="name and kind required")
            try:
                pool = ps.create_pool(name, kind, config=body.get("config") or {})
            except PoolExists:
                raise HTTPException(
                    status_code=409, detail=f"pool {name!r} already exists"
                ) from None
            except (PurserError, TypeError) as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc
            return {"pool": name, "kind": pool.cfg.kind}

        return idempotent(idempotency_key, do)

    @app.get("/pools/{pool_name}", response_model=None)
    async def pool_stats(request: Request, pool_name: str) -> dict[str, Any]:
        require(request, write=False)
        return await pool_of(pool_name).stats()

    @app.patch("/pools/{pool_name}", response_model=None)
    async def update_pool(
        request: Request,
        pool_name: str,
        body: Dict[str, Any] = Body(...),  # noqa: B008
    ) -> dict[str, Any]:
        operator, _ = require(request, write=True)
        pool = pool_of(pool_name)
        overrides = body.get("config") or body
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
        return {"pool": pool_name, "config": cfg.to_dict()}

    # -- resources ---------------------------------------------------------

    def _state(s: str | None) -> set[ResourceState] | None:
        if not s:
            return None
        names = [part.strip() for part in s.split(",") if part.strip()]
        try:
            return {ResourceState(n) for n in names}
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=f"bad state value(s): {s}") from exc

    @app.get("/pools/{pool_name}/resources", response_model=None)
    async def list_resources(
        request: Request,
        pool_name: str,
        state: Optional[str] = None,
        tag: Optional[str] = None,
        q: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, Any]:
        operator, _ = require(request, write=False)
        pool = pool_of(pool_name)
        resources = await pool.query(
            states=_state(state),
            tags=frozenset({tag}) if tag else None,
            limit=limit,
            offset=offset,
        )
        if q:
            resources = [r for r in resources if q in r.id or q in json.dumps(r.tags)]
        return {
            "resources": [
                _resource_view(pool, r, include_secrets=False, admin=False)
                for r in resources
            ],
            "counts": {s.value: n for s, n in (await pool.counts()).items()},
        }

    @app.get("/pools/{pool_name}/resources/{rid}", response_model=None)
    async def get_resource(
        request: Request,
        pool_name: str,
        rid: str,
        include_secrets: bool = False,
    ) -> dict[str, Any]:
        operator, _ = require(request, write=False)
        role = _role(request, api_key, readonly_key) or "public"
        pool = pool_of(pool_name)
        resource = await pool.get(rid)
        if resource is None:
            raise HTTPException(status_code=404, detail=f"resource {rid!r} not found")
        if include_secrets and role != "admin" and api_key is not None:
            raise HTTPException(status_code=403, detail="admin role required for secrets")
        return _resource_view(pool, resource, include_secrets=include_secrets, admin=role == "admin")

    @app.post("/pools/{pool_name}/resources", response_model=None)
    async def import_resources(
        request: Request,
        pool_name: str,
        body: Dict[str, Any] = Body(...),  # noqa: B008
        idempotency_key: Optional[str] = Header(default=None, alias="Idempotency-Key"),
    ) -> dict[str, Any]:
        operator, _ = require(request, write=True)
        pool = pool_of(pool_name)
        key = idempotency_key
        if key:
            cached = ps.idempotency.get(key)
            if cached is not None:
                return cached
        records = body.get("records") or []
        if not isinstance(records, list):
            raise HTTPException(status_code=422, detail="records must be a list")
        report = await pool.import_resources(
            records,
            dedup_key=body.get("dedup_key") or "id",
            source=body.get("source") or "manual",
            operator=operator,
            fail_fast=bool(body.get("fail_fast")),
            import_validate=body.get("import_validate"),
            batch_id=body.get("batch_id"),
        )
        result = report.to_dict()
        if key:
            ps.idempotency.put(key, result)
        return result

    write_actions: dict[
        str, Callable[[Any, str, dict[str, Any] | None, str], Any]
    ] = {
        "validate": lambda pool, rid, b, op: pool.validate_now(rid, operator=op),
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
            amount=_coerce((b or {}).get("amount"), name="amount", cast=int, minimum=1),
            reason=(b or {}).get("reason") or "api top-up",
        ),
        "drain": lambda pool, rid, b, op: pool.drain(
            rid, reason=(b or {}).get("reason") or "api drain", operator=op
        ),
        "restore": lambda pool, rid, b, op: pool.restore(rid, operator=op, reason=(b or {}).get("reason") or "api restore"),
    }

    @app.post("/pools/{pool_name}/resources/{rid}/{action}", response_model=None)
    async def resource_action(
        request: Request,
        pool_name: str,
        rid: str,
        action: str,
        body: Optional[Dict[str, Any]] = Body(None),  # noqa: B008
    ) -> dict[str, Any]:
        operator, _ = require(request, write=True)
        pool = pool_of(pool_name)
        fn = write_actions.get(action)
        if action == "admin-state":
            target = _state_value((body or {}).get("target"))
            try:
                await pool.admin_set_state(
                    rid,
                    target=target,
                    operator=operator,
                    reason=(body or {}).get("reason") or "api admin",
                )
            except PurserError as exc:
                raise _http_purser_error(exc) from exc
            return {"ok": True, "rid": rid, "state": target.value}
        if fn is None:
            raise HTTPException(status_code=404, detail=f"unknown action {action!r}")
        try:
            await fn(pool, rid, body, operator)
        except PurserError as exc:
            raise _http_purser_error(exc) from exc
        res = await pool.get(rid)
        return {"ok": True, "rid": rid, "state": res.state.value if res else None}

    @app.put("/pools/{pool_name}/resources/{rid}", response_model=None)
    async def update_resource(
        request: Request,
        pool_name: str,
        rid: str,
        body: Dict[str, Any] = Body(...),  # noqa: B008
    ) -> dict[str, Any]:
        operator, _ = require(request, write=True)
        pool = pool_of(pool_name)
        payload = body.get("payload")
        if payload is not None and not isinstance(payload, dict):
            raise HTTPException(status_code=422, detail="payload must be an object")
        tags_raw = body.get("tags")
        if tags_raw is not None and not isinstance(tags_raw, (list, set, frozenset)):
            raise HTTPException(status_code=422, detail="tags must be a list")
        tags = frozenset(tags_raw) if tags_raw is not None else None
        try:
            updated = await pool.update_resource(
                rid,
                payload=payload,
                tags=tags,
                max_slots=_coerce(body.get("max_slots"), name="max_slots", cast=int, minimum=1),
                operator=operator,
                reason=body.get("reason") or "api update",
            )
        except PurserError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return {"ok": True, "resource": pool.describe(updated)}

    @app.post("/pools/{pool_name}/resources/{rid}/revoke", response_model=None)
    async def revoke_resource(
        request: Request,
        pool_name: str,
        rid: str,
        body: Optional[Dict[str, Any]] = Body(None),  # noqa: B008
    ) -> dict[str, Any]:
        operator, _ = require(request, write=True)
        pool = pool_of(pool_name)
        try:
            lease = await pool.force_revoke(
                rid,
                operator=operator,
                reason=(body or {}).get("reason") or "api revoke",
                token=(body or {}).get("token"),
            )
        except PurserError as exc:
            raise _http_purser_error(exc) from exc
        return {"ok": True, "revoked_token": lease.token}

    @app.delete("/pools/{pool_name}/resources/{rid}", response_model=None)
    async def delete_resource(
        request: Request,
        pool_name: str,
        rid: str,
        body: Optional[Dict[str, Any]] = Body(None),  # noqa: B008
    ) -> dict[str, Any]:
        operator, _ = require(request, write=True)
        pool = pool_of(pool_name)
        resource = await pool.get(rid)
        if resource is None:
            raise HTTPException(status_code=404, detail=f"resource {rid!r} not found")
        if resource.concurrency.used_slots > 0:
            try:
                await pool.drain(rid, operator=operator, reason="api delete")
            except PurserError as exc:
                raise _http_purser_error(exc) from exc
            return {"ok": True, "rid": rid, "state": "draining"}
        try:
            await pool.admin_set_state(
                rid,
                target=ResourceState.ARCHIVED,
                operator=operator,
                reason="api delete",
            )
        except PurserError as exc:
            raise _http_purser_error(exc) from exc
        return {"ok": True, "rid": rid, "state": "archived"}

    # -- leases ------------------------------------------------------------

    @app.post("/pools/{pool_name}/leases", response_model=None)
    async def acquire_lease(
        request: Request,
        pool_name: str,
        body: Dict[str, Any] = Body(...),  # noqa: B008
    ) -> dict[str, Any]:
        operator, trace_id = require(request, write=True)
        pool = pool_of(pool_name)
        tags_raw = body.get("tags") or ()
        if not isinstance(tags_raw, (list, tuple, set, frozenset)):
            raise HTTPException(status_code=422, detail="tags must be a list")
        req = AcquireRequest(
            consumer=body.get("consumer"),
            target=body.get("target"),
            priority=_coerce(body.get("priority", 0), name="priority", cast=int, minimum=0),
            slots=_coerce(body.get("slots", 1), name="slots", cast=int, minimum=1),
            tags=frozenset(tags_raw),
            timeout=_coerce(body.get("timeout"), name="timeout", cast=float, minimum=0),
            probe=body.get("probe"),
            trace_id=trace_id or body.get("trace_id"),
            use_limit=_coerce(body.get("use_limit"), name="use_limit", cast=int, minimum=1),
            allow_duplicate=bool(body.get("allow_duplicate")),
            resource_id=body.get("resource_id"),
        )
        try:
            if body.get("count"):
                count = _coerce(body.get("count"), name="count", cast=int, minimum=1)
                batch = await pool.acquire_many(
                    count,
                    req=req,
                    mode=body.get("mode", "atomic"),
                    n_min=_coerce(body.get("n_min"), name="n_min", cast=int, minimum=1),
                )
                leases = batch.leases
            else:
                leases = [await pool.acquire(req)]
        except PurserError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        role = _role(request, api_key, readonly_key) or "public"
        admin = role == "admin"
        return {
            "leases": [
                {
                    "token": lease.token,
                    "resource_id": lease.resource_id,
                    "slots": lease.slots,
                    "expires_at": lease.expires_at,
                    "consumer": lease.consumer,
                    "target": lease.target,
                    "trace_id": lease.trace_id,
                    "payload": lease.payload if admin else mask_payload(lease.payload or {}),
                }
                for lease in leases
            ]
        }

    @app.get("/pools/{pool_name}/leases", response_model=None)
    async def list_leases(request: Request, pool_name: str) -> dict[str, Any]:
        require(request, write=False)
        pool = pool_of(pool_name)
        return {
            "leases": [
                {
                    "token": lease.token,
                    "resource_id": lease.resource_id,
                    "consumer": lease.consumer,
                    "slots": lease.slots,
                    "expires_at": lease.expires_at,
                    "outcome": lease.outcome.value,
                }
                for lease in pool._leases.active()
            ]
        }

    @app.delete("/pools/{pool_name}/leases/{token}", response_model=None)
    async def release_lease(
        request: Request,
        pool_name: str,
        token: str,
        body: Optional[Dict[str, Any]] = Body(None),  # noqa: B008
    ) -> dict[str, Any]:
        operator, _ = require(request, write=True)
        pool = pool_of(pool_name)
        body = body or {}
        lease = pool._leases.get(token)
        if lease is None:
            raise HTTPException(status_code=404, detail="lease not found on this pool")
        outcome = _outcome_value(body.get("outcome"))
        try:
            await pool.release(lease, outcome=outcome, used=body.get("used"))
        except PurserError as exc:
            raise _http_purser_error(exc) from exc
        return {"ok": True, "token": token, "outcome": outcome.value}

    # -- events / drain / recall ------------------------------------------

    @app.get("/pools/{pool_name}/events")
    async def stream_events(
        request: Request, pool_name: str, after_seq: int = 0
    ) -> StreamingResponse:
        require(request, write=False)
        pool_of(pool_name)

        async def gen() -> AsyncIterator[str]:
            last = after_seq
            while True:
                if await request.is_disconnected():
                    break
                for event in ps.bus.history(last, limit=100):
                    yield f"data: {json.dumps(event.to_dict(), ensure_ascii=False)}\n\n"
                    last = event.seq
                await asyncio.sleep(0.5)

        return StreamingResponse(gen(), media_type="text/event-stream")

    @app.post("/pools/{pool_name}/drain", response_model=None)
    async def drain_pool(
        request: Request,
        pool_name: str,
        body: Optional[Dict[str, Any]] = Body(None),  # noqa: B008
    ) -> dict[str, Any]:
        operator, _ = require(request, write=True)
        pool = pool_of(pool_name)
        body = body or {}
        count = 0
        try:
            async for resource in pool.storage.scan(pool.name, states={ResourceState.IDLE}):
                await pool.drain(
                    resource.id,
                    operator=operator,
                    reason=body.get("reason") or "api drain pool",
                )
                count += 1
        except PurserError as exc:
            raise _http_purser_error(exc) from exc
        return {"ok": True, "drained": count}

    @app.post("/pools/{pool_name}/recall", response_model=None)
    async def recall(
        request: Request,
        pool_name: str,
        body: Dict[str, Any] = Body(...),  # noqa: B008
    ) -> dict[str, Any]:
        operator, _ = require(request, write=True)
        pool = pool_of(pool_name)
        report = await pool.recall_batch(
            body["batch_id"],
            action=body.get("action", "requalify"),
            operator=operator,
            reason=body.get("reason") or "api recall",
        )
        return report

    return app
def serve(host: str = "127.0.0.1", port: int = 8000) -> None:
    import uvicorn

    uvicorn.run(create_app(), host=host, port=port)
