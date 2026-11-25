"""Dependencies shared by the pool services.

Mutable holders (``cfg`` and ``allocator``) are replaced in place by
:meth:`ResourcePool.reconfigure`; everything else is stable for the lifetime
of the pool.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, cast

from purser.core.resource import Resource
from purser.errors import ResourceNotFound

logger = logging.getLogger("purser")


def coordination_degraded(ctx: PoolContext, action: str, error: Exception) -> bool:
    """F4: decide whether a coordination outage may be absorbed locally.

    Fail-closed (the default) re-raises; ``fail_open`` keeps the pool usable
    from local state and records the degradation for operators.
    """

    if not ctx.cfg.fail_open:
        return False
    logger.warning(
        "coordination unavailable during %s on pool %s (%s); "
        "continuing in fail-open local mode",
        action,
        ctx.name,
        error,
    )
    ctx.purser.metrics.inc(f"pool.{ctx.name}.fail_open_total")
    ctx.purser.bus.publish(
        "coordination_degraded", ctx.name, data={"action": action}
    )
    return True


async def require_resource(ctx: PoolContext, rid: str) -> Resource:
    resource = await ctx.storage.get(ctx.name, rid)
    if resource is None:
        raise ResourceNotFound(f"{ctx.name}:{rid}")
    return cast(Resource, ctx.adapter.hydrate(resource))


@dataclass(slots=True)
class PoolContext:
    """Stable handles a pool service may use."""

    name: str
    purser: Any
    adapter: Any
    cfg: Any
    storage: Any
    coordination: Any
    authority: Any
    binding: Any
    leases: Any
    scheduler: Any
    allocator: Any
    profile: str | None
    guard_error: Callable[[str, Exception], None]

    @property
    def bus(self) -> Any:
        return self.purser.bus

    @property
    def audit(self) -> Any:
        return self.purser.audit
