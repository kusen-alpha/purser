"""In-memory storage plane: dict store plus state/tag inverted indexes."""

from __future__ import annotations

import copy
import random
import time
from collections.abc import AsyncIterator

from purser.backends.base import (
    ResourcePatch,
    StorageBackend,
    TransitionResult,
)
from purser.core.resource import Resource
from purser.core.state import (
    ALL_STATES,
    ALLOCATABLE_STATES,
    Event,
    ResourceState,
    StateContext,
    StateMachine,
)
from purser.errors import VersionConflict


def _now() -> float:
    return time.time()


class MemoryStorage(StorageBackend):
    """Single-process storage.  All mutations happen synchronously inside the
    event loop, so read-check-write sequences are naturally atomic."""

    def __init__(self) -> None:
        self._pools: dict[str, dict[str, Resource]] = {}
        self._by_state: dict[str, dict[ResourceState, set[str]]] = {}
        self._by_tag: dict[str, dict[str, set[str]]] = {}
        # Dense allocatable index: swap-removal list + position map so the
        # acquisition hot path can draw a bounded random sample in O(k)
        # instead of copying and filtering every stored resource.
        self._alloc_ids: dict[str, list[str]] = {}
        self._alloc_pos: dict[str, dict[str, int]] = {}

    # -- helpers -----------------------------------------------------------

    def _store(self, pool: str) -> dict[str, Resource]:
        return self._pools.setdefault(pool, {})

    def _states(self, pool: str) -> dict[ResourceState, set[str]]:
        return self._by_state.setdefault(pool, {s: set() for s in ALL_STATES})

    def _tags(self, pool: str) -> dict[str, set[str]]:
        return self._by_tag.setdefault(pool, {})

    def _alloc_index(self, pool: str) -> list[str]:
        return self._alloc_ids.setdefault(pool, [])

    def _alloc_positions(self, pool: str) -> dict[str, int]:
        return self._alloc_pos.setdefault(pool, {})

    def _alloc_add(self, pool: str, rid: str) -> None:
        pos = self._alloc_positions(pool)
        if rid in pos:
            return
        ids = self._alloc_index(pool)
        pos[rid] = len(ids)
        ids.append(rid)

    def _alloc_discard(self, pool: str, rid: str) -> None:
        pos = self._alloc_positions(pool)
        index = pos.pop(rid, None)
        if index is None:
            return
        ids = self._alloc_index(pool)
        last = ids.pop()
        if index < len(ids):
            ids[index] = last
            pos[last] = index

    def _sync_allocatable(self, pool: str, resource: Resource, was_open: bool) -> None:
        is_open = resource.allocatable
        if was_open and not is_open:
            self._alloc_discard(pool, resource.id)
        elif is_open and not was_open:
            self._alloc_add(pool, resource.id)

    def _reindex(self, pool: str, rid: str, old_state: ResourceState, new_state: ResourceState) -> None:
        states = self._states(pool)
        states[old_state].discard(rid)
        states[new_state].add(rid)

    def _unindex_tags(self, pool: str, resource: Resource) -> None:
        tags = self._tags(pool)
        for tag in resource.tags:
            bucket = tags.get(tag)
            if bucket is not None:
                bucket.discard(resource.id)

    def _index_tags(self, pool: str, resource: Resource) -> None:
        tags = self._tags(pool)
        for tag in resource.tags:
            tags.setdefault(tag, set()).add(resource.id)

    def _list_by_state(self, pool: str, states: set[ResourceState] | None) -> list[Resource]:
        store = self._store(pool)
        if not states:
            return list(store.values())
        index = self._states(pool)
        ids: set[str] = set()
        for state in states:
            ids |= index.get(state, set())
        return [store[rid] for rid in ids if rid in store]

    # -- contract ----------------------------------------------------------

    async def get(self, pool: str, rid: str) -> Resource | None:
        return self._store(pool).get(rid)

    async def put(
        self,
        pool: str,
        resource: Resource,
        *,
        expected_version: int | None = None,
    ) -> Resource:
        store = self._store(pool)
        old = store.get(resource.id)
        was_open = old.allocatable if old is not None else False
        if expected_version is not None:
            current = old.version if old is not None else 0
            if current != expected_version:
                raise VersionConflict(
                    f"resource {resource.id!r} version {current} != expected {expected_version}"
                )
        if old is not None:
            self._reindex(pool, resource.id, old.state, resource.state)
            self._unindex_tags(pool, old)
            resource.version = old.version + 1
        else:
            self._states(pool)[resource.state].add(resource.id)
            resource.version = 1
        resource.mark_updated()
        self._index_tags(pool, resource)
        store[resource.id] = resource
        self._sync_allocatable(pool, resource, was_open)
        return resource

    async def delete(self, pool: str, rid: str) -> bool:
        store = self._store(pool)
        resource = store.pop(rid, None)
        if resource is None:
            return False
        self._states(pool)[resource.state].discard(rid)
        self._unindex_tags(pool, resource)
        self._alloc_discard(pool, rid)
        return True

    async def query(
        self,
        pool: str,
        *,
        states: set[ResourceState] | None = None,
        tags: frozenset[str] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Resource]:
        resources = self._list_by_state(pool, states)
        if tags:
            tag_index = self._tags(pool)
            for tag in tags:
                bucket = tag_index.get(tag, set())
                resources = [r for r in resources if r.id in bucket]
        resources.sort(key=lambda r: r.id)
        return resources[offset : offset + limit]

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
        if limit <= 0:
            return []
        if not states or not set(states) <= set(ALLOCATABLE_STATES):
            return await super().sample_allocatable(
                pool,
                states=states,
                slots=slots,
                tags=tags,
                required_tag=required_tag,
                limit=limit,
                rng=rng,
            )
        store = self._store(pool)
        index = self._alloc_index(pool)
        if not store or not index:
            return []
        picker = rng or random
        ids: list[str]
        if tags or required_tag:
            tag_index = self._tags(pool)
            eligible: set[str] | None = None
            for tag in sorted(tags or ()):
                bucket = tag_index.get(tag)
                if not bucket:
                    return []
                eligible = set(bucket) if eligible is None else eligible & bucket
                if not eligible:
                    return []
            if required_tag is not None:
                bucket = tag_index.get(required_tag, set())
                eligible = set(bucket) if eligible is None else eligible & bucket
                if eligible is None or not eligible:
                    return []
            pool_size = len(eligible)
            if pool_size <= limit:
                ids = sorted(eligible)
            else:
                sample_size = min(pool_size, limit if slots <= 1 else limit * 4)
                ids = picker.sample(tuple(eligible), sample_size)
        else:
            pool_size = len(index)
            if pool_size <= limit:
                # Preserve the historical id-sorted order for small pools.
                ids = sorted(index)
            else:
                sample_size = min(pool_size, limit if slots <= 1 else limit * 4)
                ids = [index[i] for i in picker.sample(range(pool_size), sample_size)]
        out: list[Resource] = []
        for rid in ids:
            resource = store.get(rid)
            if resource is None or resource.state not in states:
                continue
            if resource.slots.available < slots:
                continue
            out.append(resource)
            if len(out) >= limit:
                break
        return out

    async def counts(self, pool: str) -> dict[ResourceState, int]:
        states = self._states(pool)
        return {s: len(v) for s, v in states.items()}

    async def health_scores(self, pool: str) -> list[float]:
        return [
            resource.health.score
            for resource in self._list_by_state(pool, None)
            if resource.health.samples
        ]

    async def transition_acquire(
        self, pool: str, rid: str, slots: int
    ) -> TransitionResult:
        resource = self._store(pool).get(rid)
        if resource is None or resource.state not in ALLOCATABLE_STATES:
            return TransitionResult(ok=False)
        if not resource.slots.can_acquire(slots):
            return TransitionResult(ok=False)
        was_open = resource.allocatable
        old_state = resource.state
        used_before = resource.slots.used_slots
        target = StateMachine.trigger(
            resource.state,
            Event.ACQUIRE,
            ctx=StateContext(
                used_slots=used_before,
                slots=slots,
                max_slots=resource.slots.max_slots,
            ),
        )
        resource.slots.acquire(slots)
        now = _now()
        resource.meta["last_used_at"] = now
        resource.meta["updated_at"] = now
        resource.state = target
        resource.version += 1
        self._reindex(pool, rid, old_state, target)
        self._sync_allocatable(pool, resource, was_open)
        return TransitionResult(ok=True, state=target, resource=resource)

    async def transition_release(
        self, pool: str, rid: str, slots: int
    ) -> TransitionResult:
        resource = self._store(pool).get(rid)
        if resource is None:
            return TransitionResult(ok=False)
        if resource.slots.used_slots <= 0:
            return TransitionResult(ok=False)
        was_open = resource.allocatable
        return_slots = min(slots, resource.slots.used_slots)
        resource.slots.release(return_slots)
        resource.version += 1
        resource.meta["updated_at"] = _now()
        if resource.state is ResourceState.DRAINING:
            new_state = (
                ResourceState.ARCHIVED
                if resource.slots.used_slots <= 0
                else ResourceState.DRAINING
            )
        else:
            new_state = StateMachine.trigger(
                resource.state,
                Event.RELEASE,
                ctx=StateContext(
                    used_slots=resource.slots.used_slots + return_slots,
                    slots=return_slots,
                    max_slots=resource.slots.max_slots,
                ),
            )
        old_state = resource.state
        resource.state = new_state
        self._reindex(pool, rid, old_state, new_state)
        self._sync_allocatable(pool, resource, was_open)
        return TransitionResult(ok=True, state=new_state, resource=resource)

    async def reset_usage(self, pool: str, rid: str, slots: int = 0) -> bool:
        resource = self._store(pool).get(rid)
        if resource is None:
            return False
        if slots < 0:
            return False
        slots = min(slots, resource.slots.max_slots)
        was_open = resource.allocatable
        old_state = resource.state
        if resource.state is ResourceState.DRAINING:
            if slots <= 0:
                new_state = ResourceState.ARCHIVED
                resource.meta.setdefault("archived_at", _now())
                resource.meta.setdefault("archived_reason", "reconcile: lost leases")
            else:
                new_state = ResourceState.DRAINING
        elif (
            resource.state in set(ALLOCATABLE_STATES) | {ResourceState.FULL}
        ):
            if slots == 0:
                if resource.slots.used_slots == 0:
                    return False
                new_state = ResourceState.IDLE
            elif resource.slots.used_slots == slots:
                return False
            else:
                new_state = (
                    ResourceState.FULL
                    if slots >= resource.slots.max_slots
                    else ResourceState.ACTIVE
                )
        else:
            return False
        resource.slots.used_slots = slots
        resource.state = new_state
        resource.version += 1
        resource.meta["updated_at"] = _now()
        self._reindex(pool, rid, old_state, new_state)
        self._sync_allocatable(pool, resource, was_open)
        return True

    async def set_state(
        self,
        pool: str,
        rid: str,
        expected_version: int,
        *,
        expect_state: ResourceState | None = None,
        patch: ResourcePatch,
    ) -> bool:
        resource = self._store(pool).get(rid)
        if resource is None:
            return False
        if resource.version != expected_version:
            return False
        if expect_state is not None and resource.state != expect_state:
            return False
        was_open = resource.allocatable
        old_state = resource.state
        tags_changed = patch.tags is not None and patch.tags != resource.tags
        if tags_changed:
            self._unindex_tags(pool, resource)
        patch.apply(resource)
        new_state = patch.state if patch.state is not None else old_state
        if tags_changed:
            self._index_tags(pool, resource)
        resource.version += 1
        self._reindex(pool, rid, old_state, new_state)
        self._sync_allocatable(pool, resource, was_open)
        return True

    async def scan(
        self, pool: str, *, states: set[ResourceState] | None = None, batch: int = 50
    ) -> AsyncIterator[Resource]:
        resources = self._list_by_state(pool, states)
        resources.sort(key=lambda r: r.id)
        for i in range(0, len(resources), batch):
            for resource in resources[i : i + batch]:
                yield copy.copy(resource)

    async def flush(self, pool: str) -> None:
        self._pools.pop(pool, None)
        self._by_state.pop(pool, None)
        self._by_tag.pop(pool, None)
        self._alloc_ids.pop(pool, None)
        self._alloc_pos.pop(pool, None)
