"""Health, readiness and metrics endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from starlette.responses import Response

from purser.ui.context import UiContext


def build(ctx: UiContext) -> APIRouter:
    ps = ctx.ps
    router = APIRouter()

    @router.get("/healthz", response_model=None)
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @router.get("/readyz", response_model=None)
    async def readyz() -> dict[str, Any]:
        return {"status": "ready", "pools": ps.get_pool_names()}

    @router.get("/metrics")
    async def metrics() -> Response:
        return Response(
            content=ps.metrics.render_prometheus(),
            media_type="text/plain; version=0.0.4",
        )

    @router.get("/metrics/snapshot", response_model=None)
    async def metrics_snapshot(request: Request) -> dict[str, Any]:
        ctx.require(request, write=False)
        return ps.metrics.snapshot()

    return router
