"""FastAPI service: REST API, SSE events, health endpoints."""

from __future__ import annotations

import contextlib
import os
from collections.abc import AsyncIterator
from typing import Any

from fastapi import FastAPI

from purser import Purser, __version__
from purser.ui import routers
from purser.ui.context import UiContext
from purser.ui.persistence import POOLS_FILE_ENV, load_pools, save_pools

__all__ = ["POOLS_FILE_ENV", "build_purser", "create_app", "load_pools", "save_pools", "serve"]

_ROUTER_BUILDERS = (
    routers.health.build,
    routers.meta.build,
    routers.pools.build,
    routers.resources.build,
    routers.resource_actions.build,
    routers.leases.build,
    routers.maintenance.build,
)


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

    app = FastAPI(title="Purser", version=__version__, lifespan=lifespan)
    app.state.purser = ps

    ctx = UiContext(ps, api_key=api_key, readonly_key=readonly_key)
    # include_router() keeps a lazy ``_IncludedRouter`` entry in ``app.routes``
    # (FastAPI 0.162+), so the flat route table disappears from introspection;
    # extend the list directly to keep routes visible in registration order.
    for build in _ROUTER_BUILDERS:
        app.router.routes.extend(build(ctx).routes)
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
