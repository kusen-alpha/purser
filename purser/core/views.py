"""Typed return shapes for reporting APIs.

Runtime values stay plain ``dict`` objects; these definitions only give
type checkers (and IDE completion) an exact field contract.
"""

from __future__ import annotations

from typing import Any, Literal, TypedDict


class PoolStats(TypedDict):
    name: str
    kind: str
    total: int
    counts: dict[str, int]
    allocatable: int
    active_leases: int
    waiting: int
    avg_health: float | None
    config: dict[str, Any]


class UsageWindow(TypedDict):
    since: float | None
    until: float | None
    oldest: float | None
    newest: float | None
    events: int
    retained: int
    capacity: int


class UsageTotals(TypedDict):
    releases: int
    used: int
    slots: int
    outcomes: dict[str, int]


class UsageGroup(TypedDict):
    key: str
    releases: int
    used: int
    slots: int
    outcomes: dict[str, int]


class ActiveUsageGroup(TypedDict):
    key: str
    leases: int
    slots: int


class ActiveUsage(TypedDict):
    group_by: str
    groups: list[ActiveUsageGroup]


class PoolUsage(TypedDict):
    name: str
    kind: str
    total: int
    counts: dict[str, int]
    allocatable: int
    active_leases: int
    waiting: int
    avg_health: float | None


class UsageReport(TypedDict):
    window: UsageWindow
    group_by: str
    totals: UsageTotals
    groups: list[UsageGroup]
    groups_total: int
    truncated: bool
    active: ActiveUsage
    pools: list[PoolUsage]


class ValidateJobError(TypedDict):
    rid: str
    error: str


class ValidateJobView(TypedDict):
    id: str
    pool: str
    status: Literal["running", "cancelling", "completed", "cancelled"]
    total: int
    done: int
    alive: int
    failed: int
    skipped: int
    errors: list[ValidateJobError]
    created_at: float
    updated_at: float
    operator: str
    pending: int


class ValidateJobCancelView(ValidateJobView):
    cancelled: bool
