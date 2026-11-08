"""Usage ledger (R1): bounded release history for consumer/target reports."""

from __future__ import annotations

from collections import Counter, deque
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class UsageRecord:
    ts: float
    pool: str
    kind: str
    rid: str
    consumer: str | None
    target: str | None
    outcome: str
    used: int
    slots: int


_GROUP_FIELDS = {
    "consumer": ("consumer", "(anonymous)"),
    "target": ("target", "(no target)"),
    "pool": ("pool", "(unknown)"),
    "kind": ("kind", "(unknown)"),
}


class UsageLedger:
    """In-memory, bounded log of completed leases.

    The hot path records one row per returned lease.  Capacity is fixed at
    construction; the oldest rows are evicted first, so reports cover the
    retained window instead of unbounded history.
    """

    def __init__(self, capacity: int = 50_000) -> None:
        if not isinstance(capacity, int) or capacity < 1:
            raise ValueError("usage ledger capacity must be an integer >= 1")
        self.capacity = capacity
        self._records: deque[UsageRecord] = deque(maxlen=capacity)

    def __len__(self) -> int:
        return len(self._records)

    def record(
        self,
        *,
        ts: float,
        pool: str,
        kind: str,
        rid: str,
        consumer: str | None,
        target: str | None,
        outcome: str,
        used: int,
        slots: int,
    ) -> None:
        self._records.append(
            UsageRecord(
                ts=ts,
                pool=pool,
                kind=kind,
                rid=rid,
                consumer=consumer,
                target=target,
                outcome=outcome,
                used=max(0, int(used)),
                slots=max(0, int(slots)),
            )
        )

    def _select(
        self,
        *,
        since: float | None,
        until: float | None,
        pool: str | None,
    ) -> Iterable[UsageRecord]:
        for item in self._records:
            if pool is not None and item.pool != pool:
                continue
            if since is not None and item.ts < since:
                continue
            if until is not None and item.ts > until:
                continue
            yield item

    def snapshot(
        self,
        *,
        since: float | None = None,
        until: float | None = None,
        group_by: str = "consumer",
        pool: str | None = None,
        limit: int = 20,
    ) -> dict[str, Any]:
        if group_by not in _GROUP_FIELDS:
            raise ValueError(f"unknown group_by {group_by!r}")
        field, fallback = _GROUP_FIELDS[group_by]
        grouped: dict[str, dict[str, Any]] = {}
        totals: dict[str, Any] = {
            "releases": 0,
            "used": 0,
            "slots": 0,
            "outcomes": Counter(),
        }
        newest: float | None = None
        oldest: float | None = None
        for item in self._select(since=since, until=until, pool=pool):
            totals["releases"] += 1
            totals["used"] += item.used
            totals["slots"] += item.slots
            totals["outcomes"][item.outcome] += 1
            newest = item.ts if newest is None else max(newest, item.ts)
            oldest = item.ts if oldest is None else min(oldest, item.ts)
            key = getattr(item, field) or fallback
            group = grouped.setdefault(
                key, {"key": key, "releases": 0, "used": 0, "slots": 0, "outcomes": Counter()}
            )
            group["releases"] += 1
            group["used"] += item.used
            group["slots"] += item.slots
            group["outcomes"][item.outcome] += 1
        rows = sorted(
            grouped.values(),
            key=lambda row: (-row["used"], -row["releases"], str(row["key"])),
        )
        trimmed = rows[: max(0, limit)]
        for row in trimmed:
            row["outcomes"] = dict(row["outcomes"])
        return {
            "window": {
                "since": since,
                "until": until,
                "oldest": oldest,
                "newest": newest,
                "events": totals["releases"],
                "retained": len(self._records),
                "capacity": self.capacity,
            },
            "group_by": group_by,
            "totals": {
                "releases": totals["releases"],
                "used": totals["used"],
                "slots": totals["slots"],
                "outcomes": dict(totals["outcomes"]),
            },
            "groups": trimmed,
            "groups_total": len(rows),
            "truncated": len(rows) > len(trimmed),
        }

    def export(self) -> list[dict[str, Any]]:
        """Raw retained rows (diagnostics / manual export)."""

        return [asdict(item) for item in self._records]
