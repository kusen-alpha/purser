"""HTTP routers: one module per resource family."""

from purser.ui.routers import (
    health,
    leases,
    maintenance,
    meta,
    pools,
    resource_actions,
    resources,
)

__all__ = [
    "health",
    "leases",
    "maintenance",
    "meta",
    "pools",
    "resource_actions",
    "resources",
]
