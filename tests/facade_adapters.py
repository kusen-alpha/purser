"""Module-level adapters used by the facade tests.

They must stay importable by name: ``ProcessPurser.register_adapter`` sends an
``AdapterSpec`` path over IPC and the scheduler process imports the class.
"""

from __future__ import annotations

from purser.adapters.base import ResourceAdapter
from purser.core.health import HealthReport
from purser.core.resource import Resource


class FacadeAdapter(ResourceAdapter):
    kind = "facade"
    default_slots = 1

    async def generate(self, spec: dict) -> Resource:
        return Resource(
            id=spec.get("id") or "facade-1",
            kind=self.kind,
            payload={"host": spec.get("host", "127.0.0.1")},
            tags=frozenset(),
        )

    async def validate(self, resource: Resource) -> HealthReport:
        return HealthReport.ok(latency_ms=1.0)


class NeedsArgsAdapter(ResourceAdapter):
    kind = "needsargs"
    default_slots = 1

    def __init__(self, prefix: str) -> None:
        self.prefix = prefix

    async def generate(self, spec: dict) -> Resource:
        return Resource(
            id=spec.get("id") or f"{self.prefix}-1",
            kind=self.kind,
            payload={"host": spec.get("host", "127.0.0.1")},
            tags=frozenset(),
        )

    async def validate(self, resource: Resource) -> HealthReport:
        return HealthReport.ok(latency_ms=1.0)
