"""FastAPI service: REST API, SSE events, health endpoints (8.2 / C2 / D1)."""

from __future__ import annotations

import asyncio
import contextlib
import hmac
import json
import logging
import os
import time
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import Body, FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from starlette.responses import Response

from purser import Purser, ResourcePool, __version__
from purser.config import Weights
from purser.core.quarantine import QuarantinePolicy
from purser.core.resource import AcquireRequest, Resource, UsageOutcome
from purser.core.state import ResourceState
from purser.core.views import PoolStats, UsageReport, ValidateJobView
from purser.crypto import mask_payload
from purser.errors import (
    BadPayload,
    ConfigError,
    PoolExists,
    PoolNotFound,
    PurserError,
    ResourceNotFound,
)

_STARTED = time.time()
logger = logging.getLogger("purser.ui")

POOLS_FILE_ENV = "PURSER_POOLS_FILE"


def _http_purser_error(exc: PurserError) -> HTTPException:
    """Map a library error to a client error instead of a 500.

    Not-found errors keep REST semantics (404); malformed input is 422;
    state/lease conflicts stay 409 so callers can retry after reconciling.
    """

    if isinstance(exc, (ResourceNotFound, PoolNotFound)):
        status = 404
    elif isinstance(exc, (ConfigError, BadPayload)):
        status = 422
    else:
        status = 409
    return HTTPException(status_code=status, detail=str(exc))


def _field_type(coerce: Callable[[Any], Any] | None) -> str:
    if coerce is int:
        return "int"
    if coerce is float:
        return "float"
    if coerce is bool:
        return "bool"
    return "string"


def _json_scalar(value: Any) -> Any:
    # Defaults are hints for form prefilling; never leak arbitrary objects.
    return value if isinstance(value, (str, int, float, bool)) else None

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
    if api_key and hmac.compare_digest(token, api_key):
        return "admin"
    if readonly_key and hmac.compare_digest(token, readonly_key):
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


def _clean_pool_config(raw: Any) -> dict[str, Any]:
    """Validate and normalize a PoolConfig patch before it hits the dataclass.

    Immutable knobs are filtered by the caller so a round-tripped config (the
    exact dict returned by ``stats()``) is still accepted when unchanged.
    """

    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise HTTPException(status_code=422, detail="config must be an object")
    data = dict(raw)
    data.pop("kind", None)  # supplied separately at create time
    if "secret_fields" in data:
        secret_fields = data["secret_fields"]
        if not isinstance(secret_fields, (list, tuple)) or any(
            not isinstance(name, str) or not name for name in secret_fields
        ):
            raise HTTPException(
                status_code=422,
                detail="secret_fields must be a list of non-empty strings",
            )
        data["secret_fields"] = tuple(secret_fields)
    if isinstance(data.get("weights"), dict):
        unknown = set(data["weights"]) - set(Weights.__dataclass_fields__)
        if unknown:
            raise HTTPException(
                status_code=422,
                detail=f"unknown weights field(s): {', '.join(sorted(unknown))}",
            )
        try:
            data["weights"] = Weights.from_dict(data["weights"])
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=422, detail=f"bad weights: {exc}") from exc
    quarantine = data.get("quarantine")
    if isinstance(quarantine, dict):
        allowed = {
            name
            for name in QuarantinePolicy.__dataclass_fields__
            if name != "rng"
        }
        unknown = set(quarantine) - allowed
        if unknown:
            raise HTTPException(
                status_code=422,
                detail=f"unknown quarantine field(s): {', '.join(sorted(unknown))}",
            )
        data["quarantine"] = QuarantinePolicy(**quarantine)
    for key in ("target_quota", "meta"):
        if key in data and data[key] is not None and not isinstance(data[key], dict):
            raise HTTPException(status_code=422, detail=f"{key} must be an object")
    total_quota = data.get("total_quota")
    if total_quota is not None and (
        isinstance(total_quota, bool)
        or not isinstance(total_quota, int)
        or total_quota <= 0
    ):
        raise HTTPException(
            status_code=422, detail="total_quota must be a positive integer"
        )
    return data


def _optional_epoch(value: Any, name: str) -> float | None:
    if value is None:
        return None
    parsed = _coerce(value, name=name, cast=float, minimum=0)
    return None if parsed is None else float(parsed)


def _lease_view(lease: Any, *, admin: bool) -> dict[str, Any]:
    return {
        "token": lease.token,
        "resource_id": lease.resource_id,
        "consumer": lease.consumer,
        "slots": lease.slots,
        "started_at": lease.started_at,
        "expires_at": lease.expires_at,
        "priority": lease.priority,
        "use_limit": lease.use_limit,
        "used": lease.used,
        "target": lease.target,
        "trace_id": lease.trace_id,
        "outcome": lease.outcome.value,
        "payload": lease.payload
        if admin
        else mask_payload(lease.payload or {}),
    }


def _audit_view(entry: Any) -> dict[str, Any]:
    return {
        "seq": entry.seq,
        "ts": entry.ts,
        "operator": entry.operator,
        "action": entry.action,
        "pool": entry.pool,
        "rid": entry.rid,
        "reason": entry.reason,
        "trace_id": entry.trace_id,
        "detail": entry.detail,
    }


def _pools_file() -> Path | None:
    raw = os.environ.get(POOLS_FILE_ENV)
    return Path(raw) if raw else None


def _pool_specs(ps: Purser) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for name in ps.pool_names():
        pool = ps.get_pool(name)
        if pool is None:
            continue
        config = pool.cfg.to_dict()
        config.pop("kind", None)
        out.append({"name": name, "kind": pool.cfg.kind, "config": config})
    return out


def save_pools(ps: Purser) -> None:
    """Write-through pool registry so console-created pools survive restarts."""

    path = _pools_file()
    if path is None:
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        payload = json.dumps({"pools": _pool_specs(ps)}, ensure_ascii=False, indent=2)
        tmp.write_text(payload, encoding="utf-8")
        tmp.replace(path)
    except OSError:
        logger.exception("failed to persist pool registry to %s", path)


def load_pools(ps: Purser) -> None:
    path = _pools_file()
    if path is None or not path.exists():
        return
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        logger.exception("failed to read pool registry %s", path)
        return
    specs = data.get("pools") if isinstance(data, dict) else None
    if not isinstance(specs, list):
        logger.warning("pool registry %s has no pools list", path)
        return
    for spec in specs:
        if not isinstance(spec, dict):
            continue
        name = spec.get("name")
        kind = spec.get("kind")
        if not name or not kind or ps.get_pool(name) is not None:
            continue
        try:
            ps.create_pool(
                name,
                kind,
                config=_clean_pool_config(spec.get("config") or {}),
            )
        except Exception:
            logger.exception("failed to restore pool %r from %s", name, path)


def build_purser(
    *,
    storage: str | None = None,
    sqlite_path: str | None = None,
    coordination: str | None = None,
    redis_url: str | None = None,
    mysql_url: str | None = None,
    audit_path: str | None = None,
) -> Purser:
    """Build the service instance from env vars, with explicit overrides.

    Recognised env vars: ``PURSER_STORAGE``, ``PURSER_SQLITE_PATH``,
    ``PURSER_COORDINATION``, ``PURSER_REDIS_URL``, ``PURSER_MYSQL_URL`` and
    ``PURSER_AUDIT_PATH``.  ``PURSER_REDIS_URL`` alone does not switch the
    coordination plane; set ``PURSER_COORDINATION=redis`` as well.
    """

    options: dict[str, Any] = {}
    for key, value, env in (
        ("storage", storage, "PURSER_STORAGE"),
        ("sqlite_path", sqlite_path, "PURSER_SQLITE_PATH"),
        ("coordination", coordination, "PURSER_COORDINATION"),
        ("redis_url", redis_url, "PURSER_REDIS_URL"),
        ("mysql_url", mysql_url, "PURSER_MYSQL_URL"),
        ("audit_path", audit_path, "PURSER_AUDIT_PATH"),
    ):
        resolved = value if value is not None else os.environ.get(env)
        if resolved:
            options[key] = resolved
    return Purser(**options)


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
        load_pools(ps)
        await ps.start()
        yield
        await ps.close()

    app = FastAPI(title="Purser", version="0.2.0", lifespan=lifespan)
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

    async def idempotent(
        key: str | None, fn: Callable[[], dict[str, Any]]
    ) -> dict[str, Any]:
        if not key:
            return fn()
        cached = await ps.idempotency.aget(key)
        if cached is not None:
            return cached
        result = fn()
        await ps.idempotency.aput(key, result)
        return result

    # -- health ------------------------------------------------------------

    @app.get("/healthz", response_model=None)
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz", response_model=None)
    async def readyz() -> dict[str, Any]:
        return {"status": "ready", "pools": ps.pool_names()}

    @app.get("/metrics")
    async def metrics() -> Response:
        return Response(
            content=ps.metrics.render_prometheus(),
            media_type="text/plain; version=0.0.4",
        )

    @app.get("/metrics/snapshot", response_model=None)
    async def metrics_snapshot(request: Request) -> dict[str, Any]:
        require(request, write=False)
        return ps.metrics.snapshot()

    @app.get("/reports/usage", response_model=None)
    async def usage_report(
        request: Request,
        since: Optional[float] = None,
        until: Optional[float] = None,
        group_by: str = "consumer",
        pool: Optional[str] = None,
        limit: int = 20,
    ) -> UsageReport:
        require(request, write=False)
        if pool is not None and ps.get_pool(pool) is None:
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
            raise _http_purser_error(exc) from exc

    @app.get("/backup", response_model=None)
    async def backup(request: Request) -> dict[str, Any]:
        role = _role(request, api_key, readonly_key)
        if role is None:
            raise HTTPException(status_code=401, detail="invalid or missing API key")
        if role != "admin":
            raise HTTPException(status_code=403, detail="admin role required for backup")
        operator = request.headers.get("x-operator") or "api"
        return await ps.backup(operator=operator)

    @app.post("/restore", response_model=None)
    async def restore(
        request: Request,
        body: Dict[str, Any] = Body(...),  # noqa: B008
        conflict: str = "skip",
        idempotency_key: Optional[str] = Header(default=None, alias="Idempotency-Key"),
    ) -> dict[str, Any]:
        operator, _ = require(request, write=True)
        if conflict not in ("skip", "overwrite"):
            raise HTTPException(
                status_code=422, detail="conflict must be 'skip' or 'overwrite'"
            )
        role = _role(request, api_key, readonly_key) or "public"
        key = f"restore:{role}:{idempotency_key}" if idempotency_key else None
        if key:
            cached = await ps.idempotency.aget(key)
            if cached is not None:
                return {**cached, "replayed": True}
        try:
            report = await ps.restore(body, conflict=conflict, operator=operator)
        except PurserError as exc:
            raise _http_purser_error(exc) from exc
        if key:
            await ps.idempotency.aput(key, report)
        return report

    @app.get("/")
    async def index() -> Response:
        file = STATIC_DIR / "index.html"
        if file.exists():
            return FileResponse(file)
        return JSONResponse({"service": "purser"})

    @app.get("/service", response_model=None)
    async def service_status(request: Request) -> dict[str, Any]:
        require(request, write=False)
        return {
            "version": __version__,
            "uptime_s": round(time.time() - _STARTED, 1),
            "storage": ps.storage_kind,
            "coordination": ps.coordination_kind,
            "pools": ps.pool_names(),
            "types": sorted(ps.types.names()),
            "maintenance": ps.maintenance_enabled,
        }

    @app.get("/audit", response_model=None)
    async def audit_log(
        request: Request,
        after_seq: int = 0,
        before_seq: Optional[int] = None,
        limit: int = 200,
        tail: bool = False,
        pool: Optional[str] = None,
        action: Optional[str] = None,
        rid: Optional[str] = None,
        operator: Optional[str] = None,
    ) -> dict[str, Any]:
        """Cursor replay by default; ``tail=true`` returns the newest page.

        ``after_seq`` always means "strictly newer than this seq", so a forward
        page must be the *first* ``limit`` matches.  Taking the last matches
        instead would silently skip the backlog.  ``before_seq`` pages backwards
        (strictly older) and reports ``has_older`` so the console can offer a
        "load earlier" control.
        """

        require(request, write=False)
        if after_seq < 0:
            raise HTTPException(status_code=422, detail="after_seq must be >= 0")
        if before_seq is not None and before_seq < 0:
            raise HTTPException(status_code=422, detail="before_seq must be >= 0")
        if limit < 1 or limit > 1000:
            raise HTTPException(status_code=422, detail="limit must be in 1..1000")
        matched = [
            _audit_view(entry)
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

    @app.get("/audit/verify", response_model=None)
    async def audit_verify(request: Request) -> dict[str, Any]:
        require(request, write=False)
        return {"ok": ps.audit.verify(), "entries": len(ps.audit)}

    @app.get("/types", response_model=None)
    async def list_types(request: Request) -> dict[str, Any]:
        """Describe registered resource types so the console can render forms."""

        require(request, write=False)
        out = []
        for name in sorted(ps.types.names()):
            rtype = ps.types.get(name)
            out.append(
                {
                    "kind": rtype.kind,
                    "resource_class": rtype.resource_class.__name__,
                    "default_concurrency": rtype.default_concurrency,
                    "fields": [
                        {
                            "name": spec.name,
                            "type": _field_type(spec.coerce),
                            "required": spec.required,
                            "secret": spec.secret,
                            "default": _json_scalar(spec.default),
                            "doc": spec.doc,
                        }
                        for spec in getattr(rtype.resource_class, "payload_fields", ())
                    ],
                }
            )
        return {"types": out}

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
            config = _clean_pool_config(body.get("config") or {})
            try:
                pool = ps.create_pool(name, kind, config=config)
            except PoolExists:
                raise HTTPException(
                    status_code=409, detail=f"pool {name!r} already exists"
                ) from None
            except (PurserError, TypeError) as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc
            save_pools(ps)
            return {"pool": name, "kind": pool.cfg.kind}

        return await idempotent(idempotency_key, do)

    @app.get("/pools/{pool_name}", response_model=None)
    async def pool_stats(request: Request, pool_name: str) -> PoolStats:
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
        overrides = _clean_pool_config(body.get("config") or body)
        # Accept a round-tripped config (unchanged immutable knobs) but make
        # real attempts to change them explicit instead of silently dropping.
        immutable_keys = (
            "storage",
            "coordination",
            "secret_fields",
            "event_queue_size",
            "schema_version",
        )
        for key in immutable_keys:
            if key in overrides and overrides[key] == getattr(pool.cfg, key):
                overrides.pop(key)
        immutable = sorted(
            set(overrides)
            & {"secret_fields", "event_queue_size", "schema_version"}
        )
        if immutable:
            raise HTTPException(
                status_code=422,
                detail=f"cannot change {', '.join(immutable)} at runtime",
            )
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

    @app.delete("/pools/{pool_name}", response_model=None)
    async def delete_pool(
        request: Request,
        pool_name: str,
        force: bool = False,
    ) -> dict[str, Any]:
        operator, _ = require(request, write=True)
        pool_of(pool_name)
        try:
            result = await ps.delete_pool(pool_name, force=force, operator=operator)
        except PurserError as exc:
            raise _http_purser_error(exc) from exc
        save_pools(ps)
        return {"ok": True, **result}

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
        sort: Optional[str] = None,
        order: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, Any]:
        operator, _ = require(request, write=False)
        pool = pool_of(pool_name)
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
                _resource_view(pool, r, include_secrets=False, admin=False)
                for r in resources
            ],
            "counts": await pool.counts(by_name=True),
            "total": total,
            "limit": limit,
            "offset": offset,
            "sort": sort_key,
            "order": direction,
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
            cached = await ps.idempotency.aget(key)
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
            await ps.idempotency.aput(key, result)
        return result

    write_actions: dict[
        str, Callable[[Any, str, dict[str, Any] | None, str], Any]
    ] = {
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
        "cancel-drain": lambda pool, rid, b, op: pool.cancel_drain(
            rid, reason=(b or {}).get("reason") or "api cancel-drain", operator=op
        ),
        "restore": lambda pool, rid, b, op: pool.restore(rid, operator=op, reason=(b or {}).get("reason") or "api restore"),
    }

    # Registered before the `{action}` route so the static `revoke` segment
    # wins over the dynamic catch-all (Starlette matches routes in order).
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

    @app.post(
        "/pools/{pool_name}/resources/{rid}/move", response_model=None
    )
    async def move_resource(
        request: Request,
        pool_name: str,
        rid: str,
        body: Dict[str, Any] = Body(...),  # noqa: B008
    ) -> dict[str, Any]:
        operator, _ = require(request, write=True)
        pool = pool_of(pool_name)
        target_name = body.get("target_pool")
        if not isinstance(target_name, str) or not target_name:
            raise HTTPException(
                status_code=422, detail="target_pool must be a non-empty string"
            )
        if target_name == pool_name:
            raise HTTPException(status_code=422, detail="target_pool must differ")
        try:
            target = ps.require_pool(target_name)
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
            raise _http_purser_error(exc) from exc
        return {
            "ok": True,
            "rid": rid,
            "target_pool": target_name,
            "resource": target.describe(moved),
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
        if action == "validate":
            try:
                report = await pool.validate_now(rid, operator=operator)
            except PurserError as exc:
                raise _http_purser_error(exc) from exc
            ps.audit.record(
                operator=operator,
                action="validate",
                pool=pool_name,
                rid=rid,
                reason=(body or {}).get("reason"),
                detail={
                    "alive": report.alive,
                    "latency_ms": report.latency_ms,
                    "error": report.error,
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
                },
            }
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
        extra: dict[str, Any] = {}
        if "usage_quota" in body:
            extra["usage_quota"] = (
                None
                if body["usage_quota"] is None
                else _coerce(
                    body["usage_quota"], name="usage_quota", cast=int, minimum=0
                )
            )
        if "expires_at" in body:
            extra["expires_at"] = _optional_epoch(body["expires_at"], "expires_at")
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
        try:
            updated = await pool.update_resource(
                rid,
                payload=payload,
                tags=tags,
                max_slots=_coerce(body.get("max_slots"), name="max_slots", cast=int, minimum=1),
                operator=operator,
                reason=body.get("reason") or "api update",
                **extra,
            )
        except PurserError as exc:
            raise _http_purser_error(exc) from exc
        return {"ok": True, "resource": pool.describe(updated)}

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
        idempotency_key: Optional[str] = Header(default=None, alias="Idempotency-Key"),
    ) -> dict[str, Any]:
        operator, trace_id = require(request, write=True)
        pool = pool_of(pool_name)
        role = _role(request, api_key, readonly_key) or "public"
        admin = role == "admin"
        key = f"lease:{pool.name}:{role}:{idempotency_key}" if idempotency_key else None
        if key:
            cached = await ps.idempotency.aget(key)
            if cached is not None:
                return {**cached, "replayed": True}
        tags_raw = body.get("tags") or ()
        if not isinstance(tags_raw, (list, tuple, set, frozenset)):
            raise HTTPException(status_code=422, detail="tags must be a list")
        if any(not isinstance(tag, str) or not tag for tag in tags_raw):
            raise HTTPException(
                status_code=422, detail="tags must contain non-empty strings"
            )
        optional_strings = {
            "resource_id": body.get("resource_id"),
            "required_tag": body.get("required_tag"),
            "consumer": body.get("consumer"),
            "target": body.get("target"),
        }
        for name, value in optional_strings.items():
            if value is not None and not isinstance(value, str):
                raise HTTPException(status_code=422, detail=f"{name} must be a string")
        for name in ("probe", "affinity"):
            if body.get(name) is not None:
                raise HTTPException(
                    status_code=422,
                    detail=(
                        f"{name} is not implemented; "
                        + (
                            "configure ProxyType(probe_targets=[...]) for probes"
                            if name == "probe"
                            else "enable PoolConfig(affinity_weight=...) for affinity"
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
            priority=_coerce(body.get("priority", 0), name="priority", cast=int, minimum=0),
            slots=_coerce(body.get("slots", 1), name="slots", cast=int, minimum=1),
            tags=frozenset(tags_raw),
            timeout=_coerce(body.get("timeout"), name="timeout", cast=float, minimum=0),
            trace_id=trace_id or body.get("trace_id"),
            use_limit=_coerce(body.get("use_limit"), name="use_limit", cast=int, minimum=1),
            allow_duplicate=bool(body.get("allow_duplicate")),
            ignore_cooldown=bool(body.get("ignore_cooldown")),
            resource_id=body.get("resource_id"),
            required_tag=body.get("required_tag"),
        )
        count = 0
        n_min: int | None = None
        try:
            if body.get("count"):
                count = _coerce(body.get("count"), name="count", cast=int, minimum=1)
                n_min = _coerce(body.get("n_min"), name="n_min", cast=int, minimum=1)
                batch = await pool.acquire_many(
                    count, req=req, mode=mode, n_min=n_min
                )
                leases = batch.leases
            else:
                leases = [await pool.acquire(req)]
        except PurserError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        out: dict[str, Any] = {
            "leases": [_lease_view(lease, admin=admin) for lease in leases],
            "count": len(leases),
        }
        if count:
            out.update({"requested": count, "mode": mode, "n_min": n_min})
        if key:
            await ps.idempotency.aput(key, out)
        return out

    @app.get("/pools/{pool_name}/leases", response_model=None)
    async def list_leases(
        request: Request,
        pool_name: str,
        consumer: Optional[str] = None,
        resource_id: Optional[str] = None,
    ) -> dict[str, Any]:
        require(request, write=False)
        pool = pool_of(pool_name)
        role = _role(request, api_key, readonly_key) or "public"
        admin = role == "admin"
        # Read through the coordination plane so leases issued by other
        # instances sharing it are visible here too.
        leases = await pool.leases(resource_id)
        if consumer is not None:
            leases = [lease for lease in leases if lease.consumer == consumer]
        return {
            "leases": [_lease_view(lease, admin=admin) for lease in leases],
            "count": len(leases),
        }

    @app.get("/pools/{pool_name}/leases/{token}", response_model=None)
    async def get_lease(
        request: Request, pool_name: str, token: str
    ) -> dict[str, Any]:
        require(request, write=False)
        pool = pool_of(pool_name)
        lease = await pool._coordination.get_lease(pool.name, token)
        if lease is None:
            raise HTTPException(status_code=404, detail="lease not found on this pool")
        role = _role(request, api_key, readonly_key) or "public"
        return {"lease": _lease_view(lease, admin=role == "admin")}

    @app.delete("/pools/{pool_name}/leases", response_model=None)
    async def release_consumer_leases(
        request: Request,
        pool_name: str,
        consumer: str = "",
        outcome: Optional[str] = None,
    ) -> dict[str, Any]:
        operator, _ = require(request, write=True)
        pool = pool_of(pool_name)
        name = consumer.strip()
        if not name:
            raise HTTPException(status_code=422, detail="consumer is required")
        parsed = _outcome_value(outcome)
        try:
            leases = await pool.release_consumer(
                name, outcome=parsed, operator=operator
            )
        except PurserError as exc:
            raise _http_purser_error(exc) from exc
        return {
            "ok": True,
            "consumer": name,
            "outcome": parsed.value,
            "count": len(leases),
            "tokens": [lease.token for lease in leases],
        }

    @app.post("/pools/{pool_name}/leases/{token}/renew", response_model=None)
    async def renew_lease(
        request: Request,
        pool_name: str,
        token: str,
    ) -> dict[str, Any]:
        require(request, write=True)
        pool = pool_of(pool_name)
        lease = pool._leases.get(token) or await pool._coordination.get_lease(
            pool.name, token
        )
        if lease is None:
            raise HTTPException(status_code=404, detail="lease not found on this pool")
        if await pool.renew_manual(lease):
            return {
                "ok": True,
                "token": token,
                "expires_at": lease.expires_at,
            }
        raise HTTPException(status_code=409, detail="lease renewal was rejected")

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
            # The lease may have been issued by another instance sharing the
            # same coordination plane; releasing it here is still valid.
            lease = await pool._coordination.get_lease(pool.name, token)
        if lease is None:
            raise HTTPException(status_code=404, detail="lease not found on this pool")
        outcome = _outcome_value(body.get("outcome"))
        used = _coerce(body.get("used"), name="used", cast=int, minimum=0)
        try:
            await pool.release(lease, outcome=outcome, used=used)
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
                for event in ps.bus.history(last, limit=1000):
                    # Advance over other pools' events too, otherwise a busy
                    # neighbour keeps this stream stuck on the same window.
                    last = event.seq
                    if event.pool != pool_name:
                        continue
                    yield f"data: {json.dumps(event.to_dict(), ensure_ascii=False)}\n\n"
                await asyncio.sleep(0.5)

        return StreamingResponse(gen(), media_type="text/event-stream")

    @app.get("/pools/{pool_name}/events/history", response_model=None)
    async def event_history(
        request: Request,
        pool_name: str,
        after_seq: int = 0,
        type: Optional[str] = None,
        rid: Optional[str] = None,
        limit: int = 200,
    ) -> dict[str, Any]:
        """Replayable event log for clients that cannot hold an SSE stream
        (``EventSource`` cannot attach an Authorization header)."""

        require(request, write=False)
        pool_of(pool_name)
        if after_seq < 0:
            raise HTTPException(status_code=422, detail="after_seq must be >= 0")
        if limit < 1 or limit > 1000:
            raise HTTPException(status_code=422, detail="limit must be in 1..1000")
        kinds = (
            {part.strip() for part in type.split(",") if part.strip()} if type else None
        )
        # Scan the whole retained ring before slicing: other pools must not be
        # able to starve this pool's replay window.
        events = [
            event.to_dict()
            for event in ps.bus.history(after_seq, limit=ps.bus.maxsize)
            if event.pool == pool_name
            and (kinds is None or event.type in kinds)
            and (rid is None or event.rid == rid)
        ][:limit]
        return {
            "events": events,
            "cursor": events[-1]["seq"] if events else after_seq,
        }

    @app.get("/pools/{pool_name}/batches", response_model=None)
    async def list_batches(request: Request, pool_name: str) -> dict[str, Any]:
        require(request, write=False)
        return {"batches": await pool_of(pool_name).batches()}

    @app.get("/pools/{pool_name}/validate-jobs", response_model=None)
    async def list_validate_jobs(
        request: Request, pool_name: str
    ) -> dict[str, Any]:
        require(request, write=False)
        return {"jobs": pool_of(pool_name).validate_jobs()}

    @app.get("/pools/{pool_name}/validate-jobs/{job_id}", response_model=None)
    async def get_validate_job(
        request: Request, pool_name: str, job_id: str
    ) -> ValidateJobView:
        require(request, write=False)
        job = pool_of(pool_name).validate_job(job_id)
        if job is None:
            raise HTTPException(
                status_code=404, detail=f"validate job {job_id!r} not found"
            )
        return job

    @app.post(
        "/pools/{pool_name}/validate-jobs/{job_id}/cancel", response_model=None
    )
    async def cancel_validate_job(
        request: Request, pool_name: str, job_id: str
    ) -> dict[str, Any]:
        operator, _ = require(request, write=True)
        pool = pool_of(pool_name)
        job = pool.cancel_validate_job(job_id)
        if job is None:
            raise HTTPException(
                status_code=404, detail=f"validate job {job_id!r} not found"
            )
        ps.audit.record(
            operator=operator,
            action="cancel_validate_job",
            pool=pool_name,
            detail={"job_id": job_id, "cancelled": job["cancelled"]},
        )
        return {"ok": True, **job}

    @app.post("/pools/{pool_name}/drain", response_model=None)
    async def drain_pool(
        request: Request,
        pool_name: str,
        body: Optional[Dict[str, Any]] = Body(None),  # noqa: B008
    ) -> dict[str, Any]:
        operator, _ = require(request, write=True)
        pool = pool_of(pool_name)
        body = body or {}
        reason = body.get("reason") or "api drain pool"
        raw_states = body.get("states")
        if raw_states is None:
            states = {
                ResourceState.IDLE,
                ResourceState.ACTIVE,
                ResourceState.FULL,
            }
        else:
            if not isinstance(raw_states, (list, tuple, set)):
                raise HTTPException(status_code=422, detail="states must be a list")
            states = {_state_value(value) for value in raw_states}
        drained: list[str] = []
        errors: list[dict[str, Any]] = []
        async for resource in pool.storage.scan(pool.name, states=states):
            try:
                await pool.drain(resource.id, operator=operator, reason=reason)
                drained.append(resource.id)
            except PurserError as exc:
                errors.append({"rid": resource.id, "error": str(exc)})
        return {
            "ok": True,
            "drained": len(drained),
            "resources": drained,
            "states": sorted(state.value for state in states),
            "errors": errors,
        }

    @app.post("/pools/{pool_name}/maintenance/{task}", response_model=None)
    async def run_maintenance(
        request: Request, pool_name: str, task: str
    ) -> dict[str, Any]:
        operator, _ = require(request, write=True)
        pool = pool_of(pool_name)
        try:
            result = await pool.run_maintenance(task)
        except PurserError as exc:
            raise _http_purser_error(exc) from exc
        ps.audit.record(
            operator=operator,
            action="maintenance",
            pool=pool_name,
            detail={"task": task, "result": result},
        )
        return {"ok": True, "task": task, "result": result}

    @app.post("/pools/{pool_name}/recall", response_model=None)
    async def recall(
        request: Request,
        pool_name: str,
        body: Dict[str, Any] = Body(...),  # noqa: B008
    ) -> dict[str, Any]:
        operator, _ = require(request, write=True)
        pool = pool_of(pool_name)
        batch_id = body.get("batch_id")
        if not isinstance(batch_id, str) or not batch_id:
            raise HTTPException(
                status_code=422, detail="batch_id must be a non-empty string"
            )
        try:
            return await pool.recall_batch(
                batch_id,
                action=body.get("action", "requalify"),
                operator=operator,
                reason=body.get("reason") or "api recall",
            )
        except PurserError as exc:
            raise _http_purser_error(exc) from exc

    return app


def serve(
    host: str = "127.0.0.1",
    port: int = 8000,
    *,
    storage: str | None = None,
    sqlite_path: str | None = None,
    coordination: str | None = None,
    redis_url: str | None = None,
    mysql_url: str | None = None,
    audit_path: str | None = None,
) -> None:
    import uvicorn

    ps = build_purser(
        storage=storage,
        sqlite_path=sqlite_path,
        coordination=coordination,
        redis_url=redis_url,
        mysql_url=mysql_url,
        audit_path=audit_path,
    )
    uvicorn.run(create_app(ps), host=host, port=port)
