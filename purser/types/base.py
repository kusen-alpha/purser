"""ResourceType base class and registry (4.4 / 4.5 / S13)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from purser.core.health import HealthReport
from purser.core.resource import (
    AcquireRequest,
    CheckVerdict,
    Resource,
    UsageOutcome,
)
from purser.core.state import ResourceState
from purser.errors import ConfigError


class ResourceType(ABC):
    """One plugin per resource family.  The kernel has zero knowledge of
    proxies, accounts etc.; everything type-specific flows through here.
    """

    kind: str = ""
    default_concurrency: int = 1
    secret_fields: tuple[str, ...] = ()

    @abstractmethod
    async def generate(self, spec: dict[str, Any]) -> Resource:
        """Build a NEW resource from an import/generation spec (S11)."""

    @abstractmethod
    async def validate(self, resource: Resource) -> HealthReport:
        """Lifecycle validation: is the resource alive right now (5.3)?"""

    async def on_request_check(
        self, resource: Resource, req: AcquireRequest
    ) -> CheckVerdict:
        """Request-level check: can this resource serve *this* request (5.8)?
        Default is a pass-through; subclasses return FAIL/BAN as needed."""

        return CheckVerdict.PASS

    async def on_release(self, resource: Resource, outcome: UsageOutcome) -> None:  # noqa: B027
        """Called after a lease returns with the reported usage outcome."""

    async def on_state_changed(  # noqa: B027
        self,
        resource: Resource,
        old: ResourceState,
        new: ResourceState,
        reason: str,
    ) -> None:
        """Read-only observer hook; never alters the flow."""

    async def on_error(self, context: str, error: Exception) -> None:  # noqa: B027
        """Fires when a hook raises; never propagates into the kernel."""

    def describe(self, resource: Resource) -> str:
        """Masked human-readable summary for UI/logs (A1)."""

        return f"{self.kind}:{resource.id} [{resource.state.value}]"


class ResourceTypeRegistry:
    """Runtime-registrable plugin registry (X7)."""

    def __init__(self) -> None:
        self._types: dict[str, ResourceType] = {}

    def register(self, rtype: ResourceType) -> None:
        if not rtype.kind:
            raise ConfigError("resource type must declare a non-empty kind")
        self._types[rtype.kind] = rtype

    def unregister(self, kind: str) -> None:
        self._types.pop(kind, None)

    def get(self, kind: str) -> ResourceType:
        try:
            return self._types[kind]
        except KeyError:
            raise ConfigError(f"unknown resource type {kind!r}") from None

    def __contains__(self, kind: str) -> bool:
        return kind in self._types

    def names(self) -> list[str]:
        return list(self._types)
