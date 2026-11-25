"""Events, batches, validation jobs, drain, maintenance and recall."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any

from fastapi import APIRouter, Body, HTTPException, Request
from fastapi.responses import StreamingResponse

from purser.core.state import ResourceState
from purser.core.views import ValidateJobView
from purser.errors import PurserError
from purser.ui._helpers import http_purser_error, state_value
from purser.ui.context import UiContext


def build(ctx: UiContext) -> APIRouter:
    ps = ctx.ps
    router = APIRouter()

    @router.get("/pools/{pool_name}/events")
    async def stream_events(
        request: Request, pool_name: str, after_seq: int = 0
    ) -> StreamingResponse:
        ctx.require(request, write=False)
        ctx.pool_of(pool_name)

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

    @router.get("/pools/{pool_name}/events/history", response_model=None)
    async def event_history(
        request: Request,
        pool_name: str,
        after_seq: int = 0,
        type: str | None = None,
        rid: str | None = None,
        limit: int = 200,
    ) -> dict[str, Any]:
        """Replayable event log for clients that cannot hold an SSE stream
        (``EventSource`` cannot attach an Authorization header)."""

        ctx.require(request, write=False)
        ctx.pool_of(pool_name)
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

    @router.get("/pools/{pool_name}/batches", response_model=None)
    async def list_batches(request: Request, pool_name: str) -> dict[str, Any]:
        ctx.require(request, write=False)
        return {"batches": await ctx.pool_of(pool_name).batches()}

    @router.get("/pools/{pool_name}/validate-jobs", response_model=None)
    async def list_validate_jobs(
        request: Request, pool_name: str
    ) -> dict[str, Any]:
        ctx.require(request, write=False)
        return {"jobs": ctx.pool_of(pool_name).validate_jobs()}

    @router.get("/pools/{pool_name}/validate-jobs/{job_id}", response_model=None)
    async def get_validate_job(
        request: Request, pool_name: str, job_id: str
    ) -> ValidateJobView:
        ctx.require(request, write=False)
        job = ctx.pool_of(pool_name).validate_job(job_id)
        if job is None:
            raise HTTPException(
                status_code=404, detail=f"validate job {job_id!r} not found"
            )
        return job

    @router.post(
        "/pools/{pool_name}/validate-jobs/{job_id}/cancel", response_model=None
    )
    async def cancel_validate_job(
        request: Request, pool_name: str, job_id: str
    ) -> dict[str, Any]:
        operator, _ = ctx.require(request, write=True)
        pool = ctx.pool_of(pool_name)
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

    @router.post("/pools/{pool_name}/drain", response_model=None)
    async def drain_pool(
        request: Request,
        pool_name: str,
        body: dict[str, Any] | None = Body(None),  # noqa: B008
    ) -> dict[str, Any]:
        operator, _ = ctx.require(request, write=True)
        pool = ctx.pool_of(pool_name)
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
            states = {state_value(value) for value in raw_states}
        drained: list[str] = []
        errors: list[dict[str, Any]] = []
        async for resource in pool.iter_resources(states=states):
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

    @router.post("/pools/{pool_name}/maintenance/{task}", response_model=None)
    async def run_maintenance(
        request: Request, pool_name: str, task: str
    ) -> dict[str, Any]:
        operator, _ = ctx.require(request, write=True)
        pool = ctx.pool_of(pool_name)
        try:
            result = await pool.run_maintenance(task)
        except PurserError as exc:
            raise http_purser_error(exc) from exc
        ps.audit.record(
            operator=operator,
            action="maintenance",
            pool=pool_name,
            detail={"task": task, "result": result},
        )
        return {"ok": True, "task": task, "result": result}

    @router.post("/pools/{pool_name}/recall", response_model=None)
    async def recall(
        request: Request,
        pool_name: str,
        body: dict[str, Any] = Body(...),  # noqa: B008
    ) -> dict[str, Any]:
        operator, _ = ctx.require(request, write=True)
        pool = ctx.pool_of(pool_name)
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
            raise http_purser_error(exc) from exc

    return router
