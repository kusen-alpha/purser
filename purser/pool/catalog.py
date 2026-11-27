"""Catalog reads, bulk import and backup restore."""

from __future__ import annotations

import copy
import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable, Iterable, Sequence
from typing import TYPE_CHECKING, Any, Literal, overload

from purser.backends.base import ResourcePatch
from purser.config import IMPORT_VALIDATE_AUTO_MAX, ImportValidate, _coerce_kind
from purser.core.health import Action
from purser.core.meta import ResourceMeta
from purser.core.resource import (
    HealthStats,
    Lease,
    Quota,
    Resource,
    SiteFailure,
    Slots,
    describe_resource,
)
from purser.core.state import ResourceState
from purser.core.views import PoolStats
from purser.errors import ConfigError, VersionConflict
from purser.facade._helpers import (
    _IMPORT_RESERVED,
    _MISSING,
    _RESTORE_IDLE_STATES,
    _RESTORE_TRANSIENT_META,
    _apply_import_fields,
    _now,
    _optional_float,
    _optional_int,
    _record_value,
    _schedule_window,
)
from purser.results import ImportReport

if TYPE_CHECKING:
    from purser.pool.context import PoolContext
    from purser.pool.state_ops import StateOps


class CatalogService:
    """Resource reads, imports and restore, independent of acquisition."""

    def __init__(
        self,
        ctx: PoolContext,
        state: StateOps,
        *,
        waiters: list[Any],
        validate_now: Callable[..., Awaitable[Any]],
        start_validate_job: Callable[..., Any],
    ) -> None:
        self.ctx = ctx
        self.state = state
        self.waiters = waiters
        self.validate_now = validate_now
        self.start_validate_job = start_validate_job

    async def query(
        self,
        *,
        states: set[ResourceState] | None = None,
        tags: frozenset[str] | None = None,
        limit: int = 100,
        offset: int = 0,
        search: str | None = None,
        sort: str | None = None,
        order: str | None = None,
    ) -> list[Resource]:
        if limit < 0 or offset < 0:
            raise ConfigError("limit and offset must be >= 0")
        sort_key = sort or "id"
        if sort_key not in ("id", "health", "recent"):
            raise ConfigError(f"unsupported sort {sort_key!r}")
        direction = (order or ("asc" if sort_key == "id" else "desc")).lower()
        if direction not in ("asc", "desc"):
            raise ConfigError(f"unsupported order {order!r}")
        if search or sort:
            matched = await self._search_resources(states=states, tags=tags, search=search)
            if sort:
                # _search_resources already ordered by id, and Python sorts are
                # stable, so equal scores keep an id tiebreak in either direction.
                if sort_key == "health":
                    matched.sort(
                        key=lambda r: r.health.score, reverse=direction == "desc"
                    )
                elif sort_key == "recent":
                    matched.sort(
                        key=lambda r: r.last_used_at or 0.0, reverse=direction == "desc"
                    )
                elif direction == "desc":
                    matched.reverse()
            if limit == 0:
                return []
            return [self.ctx.adapter.hydrate(r) for r in matched[offset : offset + limit]]
        resources = await self.ctx.storage.query(
            self.ctx.name, states=states, tags=tags, limit=limit, offset=offset
        )
        return [self.ctx.adapter.hydrate(resource) for resource in resources]

    async def iter_resources(
        self,
        *,
        states: set[ResourceState] | None = None,
        tags: Iterable[str] | None = None,
        search: str | None = None,
        batch_size: int = 100,
    ) -> AsyncGenerator[Resource, None]:
        """Stream matching resources without building a full result list.

        Ordering and filters match ``query()``; there is no offset window, so
        writes during iteration cannot shift the caller's position.
        """

        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size < 1:
            raise ConfigError("batch_size must be an integer >= 1")
        tag_filter: frozenset[str] | None = None
        if tags is not None:
            if isinstance(tags, str):
                raise TypeError("tags must be an iterable of strings, not a str")
            tag_filter = frozenset(tags)
            if any(not isinstance(tag, str) or not tag for tag in tag_filter):
                raise ValueError("tags must contain non-empty strings")
        needle = (search or "").casefold()
        async for resource in self.ctx.storage.scan(
            self.ctx.name, states=states, batch=batch_size
        ):
            if tag_filter and not tag_filter.issubset(resource.tags):
                continue
            if needle and not self._matches(resource, needle):
                continue
            yield self.ctx.adapter.hydrate(resource)

    async def count_matching(
        self,
        *,
        states: set[ResourceState] | None = None,
        tags: frozenset[str] | None = None,
        search: str | None = None,
    ) -> int:
        """Number of resources matching the filters, ignoring pagination."""

        if not search and not tags and not states:
            return sum((await self.counts()).values())
        return len(await self._search_resources(states=states, tags=tags, search=search))

    async def _search_resources(
        self,
        *,
        states: set[ResourceState] | None = None,
        tags: frozenset[str] | None = None,
        search: str | None = None,
    ) -> list[Resource]:
        needle = (search or "").casefold()
        matched: list[Resource] = []
        async for resource in self.ctx.storage.scan(self.ctx.name, states=states):
            if tags and not tags.issubset(resource.tags):
                continue
            if needle and not self._matches(resource, needle):
                continue
            matched.append(resource)
        matched.sort(key=lambda r: r.id)
        return matched

    @staticmethod
    def _matches(resource: Resource, needle: str) -> bool:
        fields = [resource.id, resource.kind, *resource.tags]
        for key in ("batch_id", "source"):
            value = resource.meta.get(key)
            if value is not None:
                fields.append(str(value))
        return needle in " ".join(fields).casefold()

    async def batches(self) -> list[dict[str, Any]]:
        """Aggregate resources by their import/generation batch."""

        grouped: dict[str, dict[str, Any]] = {}
        async for resource in self.ctx.storage.scan(self.ctx.name):
            batch_id = str(resource.meta.get("batch_id") or "").strip()
            if not batch_id:
                continue
            item = grouped.setdefault(
                batch_id,
                {
                    "batch_id": batch_id,
                    "total": 0,
                    "states": {},
                    "source": resource.meta.get("source"),
                    "created_at": None,
                },
            )
            item["total"] += 1
            states = item["states"]
            states[resource.state.value] = states.get(resource.state.value, 0) + 1
            created = resource.meta.get("created_at")
            if isinstance(created, (int, float)) and (
                item["created_at"] is None or created < item["created_at"]
            ):
                item["created_at"] = created
        return sorted(grouped.values(), key=lambda item: item["batch_id"])

    @overload
    async def counts(
        self, *, by_name: Literal[False] = False
    ) -> dict[ResourceState, int]: ...

    @overload
    async def counts(self, *, by_name: Literal[True]) -> dict[str, int]: ...

    async def counts(self, *, by_name: bool = False) -> dict[Any, int]:
        """State distribution.

        The default returns enum keys; ``by_name=True`` returns the same data
        with ``ResourceState.value`` string keys, matching ``stats()``.
        """

        counts = await self.ctx.storage.counts(self.ctx.name)
        if by_name:
            return {state.value: n for state, n in counts.items()}
        return dict(counts)

    async def stats(self) -> PoolStats:
        counts = await self.counts(by_name=True)
        health_scores = await self.ctx.storage.health_scores(self.ctx.name)
        return {
            "name": self.ctx.name,
            "kind": self.ctx.cfg.kind,
            "profile": self.ctx.profile,
            "total": sum(counts.values()),
            "counts": counts,
            "allocatable": counts[ResourceState.IDLE.value]
            + counts[ResourceState.ACTIVE.value],
            "active_leases": len(await self.ctx.coordination.active(self.ctx.name)),
            "waiting": len(self.waiters),
            "avg_health": round(sum(health_scores) / len(health_scores), 3) if health_scores else None,
            "config": self.ctx.cfg.to_dict(),
        }

    def describe(self, resource: Resource, *, secret: bool = False) -> dict[str, Any]:
        return describe_resource(resource, secret=secret)

    async def leases(self, rid: str | None = None) -> list[Lease]:
        """Active leases from the coordination plane.

        ``rid=None`` returns every active lease in the pool.  Leases
        issued by other instances sharing the same coordination backend are
        included; their ``resource`` snapshot is ``None``.
        """

        if rid is None:
            return list(await self.ctx.coordination.active(self.ctx.name))
        return list(
            await self.ctx.coordination.leases_for(self.ctx.name, rid)
        )

    # -- import / generate ------------------------------------------------

    async def import_resources(
        self,
        records: Sequence[dict[str, Any] | Resource],
        *,
        dedup_key: str = "id",
        source: str = "manual",
        operator: str = "system",
        fail_fast: bool = False,
        import_validate: ImportValidate | str | None = None,
        batch_id: str | None = None,
    ) -> ImportReport:
        """Bulk import dict specs or pre-built Resources with dedup."""

        report = ImportReport(batch_id=batch_id or uuid.uuid4().hex)
        mode = _coerce_kind(
            ImportValidate,
            import_validate
            if import_validate is not None
            else self.ctx.cfg.import_validate,
            "import_validate",
        )
        if mode is ImportValidate.AUTO:
            mode = (
                ImportValidate.ON_IMPORT
                if len(records) <= IMPORT_VALIDATE_AUTO_MAX
                else ImportValidate.BATCH
            )
        pending_validate: list[str] = []
        for index, record in enumerate(records):
            try:
                explicit_slots = False
                if isinstance(record, Resource):
                    resource = self.ctx.adapter.adopt(copy.deepcopy(record))
                elif isinstance(record, dict):
                    spec = dict(record)
                    reserved = {
                        key: spec.pop(key) for key in _IMPORT_RESERVED if key in spec
                    }
                    resource = await self.ctx.adapter.generate(spec)
                    _apply_import_fields(resource, reserved)
                    explicit_slots = "max_slots" in reserved
                else:
                    raise TypeError("import record must be a dict or Resource")
            except Exception as exc:
                self.ctx.guard_error("import.generate", exc)
                report.errors.append({"line": index, "error": str(exc)})
                if fail_fast:
                    break
                continue
            value = _record_value(record, dedup_key) if dedup_key else _MISSING
            if value is not _MISSING:
                resource.id = str(value)
            resource.meta.setdefault("source", source)
            resource.meta["batch_id"] = report.batch_id
            if not explicit_slots and self.ctx.cfg.max_slots:
                resource.slots.max_slots = self.ctx.cfg.max_slots
            if resource.expires_at is None and self.ctx.cfg.expires_in:
                resource.expires_at = _now() + self.ctx.cfg.expires_in
            if self.ctx.cfg.usage_quota is not None and resource.usage_quota is None:
                resource.usage_quota = self.ctx.cfg.usage_quota
            if self.ctx.cfg.quota_limit is not None and resource.quota is None:
                resource.quota = Quota(limit=int(self.ctx.cfg.quota_limit))
            stored = False
            for attempt in range(3):
                existing = await self.ctx.storage.get(self.ctx.name, resource.id)
                if existing is not None and existing.state is ResourceState.ARCHIVED:
                    report.skipped += 1
                    break
                if existing is not None:
                    resource.state = existing.state
                    resource.slots = existing.slots
                    resource.health = existing.health
                    resource.quota = existing.quota
                    resource.site_failures = existing.site_failures
                    resource.used_count = existing.used_count
                    resource.next_available_at = existing.next_available_at
                    resource.meta = ResourceMeta({**existing.meta, **resource.meta})
                    resource.meta["updated_at"] = _now()
                    expected = existing.version
                else:
                    resource.meta["created_at"] = _now()
                    expected = 0
                try:
                    await self.ctx.storage.put(
                        self.ctx.name, resource, expected_version=expected
                    )
                except VersionConflict:
                    if attempt == 2:
                        raise
                    continue
                if existing is not None:
                    report.updated += 1
                else:
                    report.imported += 1
                stored = True
                break
            if not stored:
                continue
            if mode is ImportValidate.ON_IMPORT:
                # Batch jobs swallow per-record validate errors; the inline
                # path must too, or re-importing a QUOTA_EXHAUSTED/SUSPENDED
                # resource aborts the whole import.
                try:
                    verdict = await self.validate_now(resource.id, operator=operator)
                except Exception as exc:
                    report.validation.append(
                        {
                            "id": resource.id,
                            "alive": False,
                            "latency_ms": None,
                            "error": str(exc),
                            "action": Action.REVALIDATE.value,
                        }
                    )
                    if fail_fast:
                        break
                    continue
                report.validation.append(
                    {
                        "id": resource.id,
                        "alive": verdict.alive,
                        "latency_ms": verdict.latency_ms,
                        "error": verdict.error,
                        "action": verdict.action.value,
                    }
                )
            elif mode is ImportValidate.BATCH:
                pending_validate.append(resource.id)
        if mode is ImportValidate.BATCH and pending_validate:
            self.start_validate_job(
                report.batch_id or uuid.uuid4().hex, pending_validate, operator
            )
        self.ctx.purser.bus.publish(
            "import_finished",
            self.ctx.name,
            data={"report": report.to_dict()},
        )
        self.ctx.purser.audit.record(
            operator=operator,
            action="import_resources",
            pool=self.ctx.name,
            detail={"report": report.to_dict(), "batch_id": report.batch_id},
        )
        return report

    async def generate(self, specs: Sequence[dict[str, Any] | Resource], *, source: str = "builtin", operator: str = "system") -> ImportReport:
        return await self.import_resources(
            specs, source=source, operator=operator, import_validate="on_import"
        )

    async def apply_defaults(
        self,
        *,
        operator: str = "system",
        reason: str = "apply pool defaults",
    ) -> dict[str, int]:
        """Backfill pool-level defaults onto resources that lack them.

        Mirrors the import-time defaults (``expires_in``, ``usage_quota``,
        ``quota_limit``) but only ever fills a missing value; existing
        resource-level settings are never overwritten, so this is safe to run
        on a live pool.  ``max_slots`` cannot be detected as "unset"
        retroactively and is left alone.
        """

        updated = 0
        now = _now()
        expires_in = self.ctx.cfg.expires_in
        usage_quota = self.ctx.cfg.usage_quota
        quota_limit = self.ctx.cfg.quota_limit
        if expires_in is None and usage_quota is None and quota_limit is None:
            return {"updated": 0}
        async for resource in self.ctx.storage.scan(self.ctx.name):
            extra: dict[str, Any] = {}
            clear: set[str] = set()
            if (
                expires_in is not None
                and resource.expires_at is None
            ):
                extra["expires_at"] = now + expires_in
            if usage_quota is not None and resource.usage_quota is None:
                extra["usage_quota"] = int(usage_quota)
            if quota_limit is not None and resource.quota is None:
                extra["quota"] = Quota(limit=int(quota_limit))
            if not extra:
                continue
            if await self.state.cas(
                resource.id,
                ResourcePatch(
                    meta_patch={**resource.meta, "updated_by": operator, "update_reason": reason},
                    clear_fields=frozenset(clear),
                    **extra,
                ),
            ):
                updated += 1
        if updated:
            self.ctx.purser.audit.record(
                operator=operator,
                action="apply_defaults",
                pool=self.ctx.name,
                reason=reason,
                detail={"updated": updated},
            )
        return {"updated": updated}

    async def _restore_resources(
        self, records: Sequence[Any], *, conflict: str, operator: str
    ) -> dict[str, Any]:
        stats: dict[str, Any] = {
            "imported": 0,
            "updated": 0,
            "skipped": 0,
            "normalized": 0,
            "errors": [],
        }
        for index, entry in enumerate(records):
            try:
                resource, normalized = self._restore_resource(entry)
            except Exception as exc:
                stats["errors"].append({"line": index, "error": str(exc)})
                continue
            existing = await self.ctx.storage.get(self.ctx.name, resource.id)
            if existing is not None and conflict == "skip":
                stats["skipped"] += 1
                continue
            if existing is not None:
                resource.version = existing.version
                resource.meta.setdefault("created_at", existing.meta.get("created_at", _now()))
                expected = existing.version
            else:
                resource.meta["created_at"] = resource.meta.get("created_at") or _now()
                expected = 0
            resource.meta["updated_at"] = _now()
            try:
                for attempt in range(3):
                    try:
                        await self.ctx.storage.put(
                            self.ctx.name, resource, expected_version=expected
                        )
                        break
                    except VersionConflict:
                        if attempt == 2:
                            raise
                        current = await self.ctx.storage.get(self.ctx.name, resource.id)
                        expected = current.version if current is not None else 0
            except Exception as exc:
                stats["errors"].append(
                    {"line": index, "id": resource.id, "error": str(exc)}
                )
                continue
            if existing is not None:
                stats["updated"] += 1
            else:
                stats["imported"] += 1
            if normalized:
                stats["normalized"] += 1
            self.ctx.purser.bus.publish(
                "resource_restored",
                self.ctx.name,
                rid=resource.id,
                data={"state": resource.state.value, "conflict": conflict},
            )
        return stats

    def _restore_resource(self, entry: Any) -> tuple[Resource, bool]:
        if not isinstance(entry, dict):
            raise ValueError("resource entry must be an object")
        rid = str(entry.get("id") or "").strip()
        if not rid:
            raise ValueError("resource id must be non-empty")
        payload = entry.get("payload")
        if payload is None:
            payload = {}
        if not isinstance(payload, dict):
            raise ValueError("payload must be an object")
        tags_raw = entry.get("tags") or ()
        if not isinstance(tags_raw, (list, tuple, set, frozenset)):
            raise ValueError("tags must be a list")
        raw_state = str(entry.get("state") or "idle")
        try:
            state = ResourceState(raw_state)
        except ValueError:
            state = ResourceState.IDLE
            normalized = True
        else:
            normalized = state in _RESTORE_IDLE_STATES
            if normalized:
                state = ResourceState.IDLE
        meta_raw = entry.get("meta")
        meta = dict(meta_raw) if isinstance(meta_raw, dict) else {}
        if normalized:
            for key in _RESTORE_TRANSIENT_META:
                meta.pop(key, None)
        slots_raw = entry.get("slots")
        slots_raw = slots_raw if isinstance(slots_raw, dict) else {}
        max_slots_raw = slots_raw.get("max_slots", entry.get("max_slots"))
        max_slots = (
            int(max_slots_raw)
            if max_slots_raw is not None
            else (self.ctx.cfg.max_slots or self.ctx.adapter.default_slots)
        )
        if max_slots <= 0:
            raise ValueError("max_slots must be positive")
        health_raw = entry.get("health")
        health_raw = health_raw if isinstance(health_raw, dict) else {}
        health = HealthStats(
            score=float(health_raw.get("score", 0.5)),
            samples=int(health_raw.get("samples", 0)),
            last_latency_ms=_optional_float(health_raw.get("last_latency_ms")),
            last_checked_at=_optional_float(health_raw.get("last_checked_at")),
            last_error=health_raw.get("last_error"),
            consecutive_failures=int(health_raw.get("consecutive_failures", 0)),
        )
        quota_raw = entry.get("quota")
        quota = None
        if isinstance(quota_raw, dict):
            quota = Quota(
                limit=int(quota_raw.get("limit", 0)),
                used=int(quota_raw.get("used", 0)),
                reset_at=_optional_float(quota_raw.get("reset_at")),
            )
        site_failures: dict[str, SiteFailure] = {}
        failures_raw = entry.get("site_failures")
        if isinstance(failures_raw, dict):
            now = _now()
            for site, data in failures_raw.items():
                data = data if isinstance(data, dict) else {}
                site_failures[str(site)] = SiteFailure(
                    site=str(site),
                    count=int(data.get("count", 1)),
                    since=_optional_float(data.get("since")) or now,
                    until=_optional_float(data.get("until")) or now,
                )
        resource = Resource(
            id=rid,
            kind=self.ctx.adapter.kind,
            payload=payload,
            state=state,
            tags=frozenset(str(tag) for tag in tags_raw),
            slots=Slots(max_slots=max_slots),
            health=health,
            site_failures=site_failures,
            quota=quota,
            usage_quota=_optional_int(entry.get("usage_quota"), "usage_quota"),
            used_count=max(0, int(entry.get("used_count") or 0)),
            expires_at=_optional_float(entry.get("expires_at")),
            next_available_at=_optional_float(entry.get("next_available_at")),
            mutex_group=str(entry["mutex_group"]) if entry.get("mutex_group") else None,
            schedule_window=_schedule_window(entry.get("schedule_window")),
            meta=meta,
        )
        typed = self.ctx.adapter.adopt(resource)
        return typed, normalized

    # -- acquisition -------------------------------------------------------
