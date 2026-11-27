"""Two-plane backend contracts.

* :class:`StorageBackend` is the durable system of record: resource master
  data, important state, slot accounting and the version CAS.  A loss here is
  a business loss, so sqlite / mysql live on this plane.
* :class:`Coordination` holds transient scheduling state: in-flight leases,
  heartbeats, wake-ups and caches.  Losing it on an abnormal restart only
  costs scheduling accuracy; the pool reconciles slot usage on start.
"""

from __future__ import annotations

import random
import time
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

from purser.core.resource import (
    HealthStats,
    Lease,
    Quota,
    Resource,
    SiteFailure,
    Slots,
)
from purser.core.state import ResourceState


def _now() -> float:
    return time.time()


@dataclass(slots=True)
class ResourcePatch:
    """Structured updates committed atomically with a state change."""

    state: ResourceState | None = None
    health: HealthStats | None = None
    site_failures: dict[str, SiteFailure] | None = None
    quota: Quota | None = None
    used_count: int | None = None
    usage_quota: int | None = None
    expires_at: float | None = None
    next_available_at: float | None = None
    mutex_group: str | None = None
    schedule_window: tuple[str, str] | None = None
    tags: frozenset[str] | None = None
    payload: dict[str, Any] | None = None
    slots: Slots | None = None
    meta_patch: dict[str, Any] | None = None
    meta_clear: frozenset[str] = frozenset()
    # Fields explicitly reset to ``None``; ``None`` alone means "unchanged"
    # everywhere else on this patch, so clearing needs its own marker.
    clear_fields: frozenset[str] = frozenset()

    _CLEARABLE = frozenset(
        {
            "usage_quota",
            "expires_at",
            "next_available_at",
            "mutex_group",
            "schedule_window",
            "quota",
        }
    )

    def apply(self, resource: Resource) -> Resource:
        unknown = self.clear_fields - self._CLEARABLE
        if unknown:
            raise ValueError(f"cannot clear field(s): {', '.join(sorted(unknown))}")
        for name in self.clear_fields:
            setattr(resource, name, None)
        if self.state is not None:
            resource.state = self.state
        if self.health is not None:
            resource.health = self.health
        if self.site_failures is not None:
            resource.site_failures = self.site_failures
        if self.quota is not None:
            resource.quota = self.quota
        if self.used_count is not None:
            resource.used_count = self.used_count
        if self.usage_quota is not None:
            resource.usage_quota = self.usage_quota
        if self.expires_at is not None:
            resource.expires_at = self.expires_at
        if self.next_available_at is not None:
            resource.next_available_at = self.next_available_at
        if self.mutex_group is not None:
            resource.mutex_group = self.mutex_group
        if self.schedule_window is not None:
            resource.schedule_window = self.schedule_window
        if self.tags is not None:
            resource.tags = self.tags
        if self.payload is not None:
            resource.payload = self.payload
        if self.slots is not None:
            resource.slots = self.slots
        if self.meta_clear:
            for name in self.meta_clear:
                resource.meta.pop(name, None)
        if self.meta_patch:
            resource.meta.update(self.meta_patch)
        resource.mark_updated()
        return resource


@dataclass(slots=True)
class TransitionResult:
    ok: bool
    state: ResourceState | None = None
    resource: Resource | None = None


class StorageBackend(ABC):
    """Authoritative, durable resource store.  No leases live here."""

    @abstractmethod
    async def get(self, pool: str, rid: str) -> Resource | None: ...

    @abstractmethod
    async def put(
        self,
        pool: str,
        resource: Resource,
        *,
        expected_version: int | None = None,
    ) -> Resource:
        """Insert or update a resource.

        ``expected_version=None`` keeps the legacy upsert semantics.  ``0``
        means create-only and ``N`` means update-only when the stored version
        is exactly ``N``; a mismatch raises :class:`VersionConflict`.
        """

    @abstractmethod
    async def delete(self, pool: str, rid: str) -> bool: ...

    @abstractmethod
    async def query(
        self,
        pool: str,
        *,
        states: set[ResourceState] | None = None,
        tags: frozenset[str] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Resource]: ...

    @abstractmethod
    async def counts(self, pool: str) -> dict[ResourceState, int]: ...

    async def health_scores(self, pool: str) -> list[float]:
        """Scores of resources that have at least one health sample.

        The default walks :meth:`scan`; SQL backends override it to read only
        the ``health`` column so payloads are never decrypted for statistics.
        """

        scores: list[float] = []
        async for resource in self.scan(pool):
            if resource.health.samples:
                scores.append(resource.health.score)
        return scores

    @abstractmethod
    async def transition_acquire(
        self, pool: str, rid: str, slots: int
    ) -> TransitionResult:
        """Slot-aware CAS: bump ``used_slots`` and flip to ACTIVE/FULL."""

    @abstractmethod
    async def transition_release(
        self, pool: str, rid: str, slots: int
    ) -> TransitionResult:
        """Return ``slots`` to the pool; DRAINING resources archive."""

    @abstractmethod
    async def reset_usage(self, pool: str, rid: str, slots: int = 0) -> bool:
        """Reconciliation: align ``used_slots`` with ``slots``.

        ``slots == 0`` drops orphaned usage back to IDLE (or archives a
        DRAINING resource whose in-flight leases were lost); ``slots > 0``
        re-aligns the count to the surviving leases' total without touching
        master data."""

    @abstractmethod
    async def set_state(
        self,
        pool: str,
        rid: str,
        expected_version: int,
        *,
        expect_state: ResourceState | None = None,
        patch: ResourcePatch,
    ) -> bool: ...

    @abstractmethod
    def scan(
        self, pool: str, *, states: set[ResourceState] | None = None, batch: int = 50
    ) -> AsyncIterator[Resource]: ...

    async def sample_allocatable(
        self,
        pool: str,
        *,
        states: set[ResourceState],
        slots: int = 1,
        tags: frozenset[str] | None = None,
        required_tag: str | None = None,
        limit: int = 256,
        rng: random.Random | None = None,
    ) -> list[Resource]:
        """Return up to ``limit`` candidates for the acquisition hot path.

        Backends may override this to avoid materializing the whole pool; the
        default walks :meth:`scan` and stops after ``limit`` matches.  Callers
        must treat a full ``limit`` result as a *sample*: more candidates may
        exist, so they retry or fall back rather than concluding exhaustion.
        Filters applied here (state, free slots, tags) are hard constraints;
        scheduling-only filters stay in the lease service.
        """

        out: list[Resource] = []
        async for resource in self.scan(pool, states=states):
            if resource.state not in states:
                continue
            if resource.slots.available < slots:
                continue
            if tags and not tags <= resource.tags:
                continue
            if required_tag is not None and required_tag not in resource.tags:
                continue
            out.append(resource)
            if len(out) >= limit:
                break
        return out

    @abstractmethod
    async def flush(self, pool: str) -> None: ...


class Coordination(ABC):
    """Transient lease / wake-up store.  Contents are rebuildable.

    Lease liveness is judged from ``expires_at`` against wall clocks, so
    instances sharing a coordination plane must run NTP-synchronised clocks;
    the Redis key TTL is only a cleanup grace (``_lease_ttl``), not the
    authority on whether a lease is still valid.
    """

    @abstractmethod
    async def register_lease(self, pool: str, lease: Lease) -> None: ...

    @abstractmethod
    async def unregister_lease(self, pool: str, token: str) -> Lease | None: ...

    @abstractmethod
    async def get_lease(self, pool: str, token: str) -> Lease | None: ...

    @abstractmethod
    async def leases_for(self, pool: str, rid: str) -> list[Lease]: ...

    @abstractmethod
    async def renew_lease(
        self, pool: str, rid: str, token: str, expires_at: float
    ) -> bool: ...

    @abstractmethod
    async def active(self, pool: str) -> list[Lease]: ...

    @abstractmethod
    async def flush(self, pool: str) -> None: ...

    @abstractmethod
    async def close(self) -> None: ...

    async def try_leader(self, pool: str, name: str, ttl: float) -> bool:
        """Whether this instance may run the shared periodic task ``name``.

        Default ``True``: an in-process coordination plane has no peer to
        exclude.  Shared planes override this with a lease-based election so
        one instance at a time performs cross-resource sweeps.
        """

        return True

    def _lease_ttl(self, lease: Lease) -> int:
        return max(1, int(lease.expires_at - _now()) + 5)
