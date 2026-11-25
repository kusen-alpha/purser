"""SQL storage bounded candidate sampling (P0-2)."""

from __future__ import annotations

import random

import pytest

from purser.backends.sqlite import SqliteStorage
from purser.core.resource import AcquireRequest, Concurrency, HealthStats, Resource
from purser.core.state import ALLOCATABLE_STATES, ResourceState

POOL = "p"


def make(
    rid: str,
    *,
    tags: tuple[str, ...] = (),
    used: int = 0,
    max_slots: int = 1,
    state: ResourceState = ResourceState.IDLE,
    password: str | None = None,
) -> Resource:
    payload = {"host": "127.0.0.1", "port": 8080}
    if password is not None:
        payload["password"] = password
    return Resource(
        id=rid,
        kind="fakecheck",
        payload=payload,
        state=state,
        tags=frozenset(tags),
        concurrency=Concurrency(max_slots=max_slots, used_slots=used),
        health=HealthStats(score=0.75, samples=4),
        used_count=3,
        meta={"created_at": 10.0, "last_used_at": 20.0, "preference": 0.4},
    )


async def store_with(
    resources: list[Resource], *, secret_fields: tuple[str, ...] = ()
) -> SqliteStorage:
    storage = SqliteStorage(path=":memory:", secret_fields=secret_fields)
    for item in resources:
        await storage.put(POOL, item)
    return storage


async def sample_ids(storage: SqliteStorage, rng: random.Random, **kwargs) -> list[str]:
    states = kwargs.pop("states", set(ALLOCATABLE_STATES))
    items = await storage.sample_allocatable(
        POOL, states=states, rng=rng, **kwargs
    )
    return [item.id for item in items]


async def test_sqlite_sample_small_pool_is_exact_and_sorted():
    storage = await store_with([make("r-b"), make("r-a"), make("r-c")])
    try:
        assert await sample_ids(storage, random.Random(1), limit=10) == [
            "r-a",
            "r-b",
            "r-c",
        ]
    finally:
        await storage.close()


async def test_sqlite_sample_large_pool_is_bounded_unique_and_allocatable():
    resources = [make(f"r-{i:04d}") for i in range(300)]
    resources += [
        make(f"full-{i}", used=1, state=ResourceState.FULL) for i in range(20)
    ]
    storage = await store_with(resources)
    try:
        seen: set[str] = set()
        for seed in range(5):
            ids = await sample_ids(storage, random.Random(seed), limit=40)
            assert len(ids) == 40
            assert len(set(ids)) == 40
            assert not any(rid.startswith("full-") for rid in ids)
            seen.update(ids)
        assert len(seen) > 40
    finally:
        await storage.close()


async def test_sqlite_sample_respects_tags_required_tag_and_slots():
    storage = await store_with(
        [
            make("eu-fast", tags=("EU", "fast")),
            make("eu-slow", tags=("EU",)),
            make("us-fast", tags=("US", "fast")),
            make("half", tags=("EU",), used=1, max_slots=2),
        ]
    )
    try:
        rng = random.Random(2)
        assert sorted(await sample_ids(storage, rng, tags=frozenset({"EU"}), limit=10)) == [
            "eu-fast",
            "eu-slow",
            "half",
        ]
        assert sorted(
            await sample_ids(storage, rng, required_tag="fast", limit=10)
        ) == ["eu-fast", "us-fast"]
        assert await sample_ids(
            storage, rng, tags=frozenset({"EU"}), required_tag="fast", limit=10
        ) == ["eu-fast"]
        assert await sample_ids(storage, rng, tags=frozenset({"APAC"})) == []
        assert await sample_ids(storage, rng, slots=2) == []
        assert sorted(await sample_ids(storage, rng, slots=1)) == [
            "eu-fast",
            "eu-slow",
            "half",
            "us-fast",
        ]
    finally:
        await storage.close()


async def test_sqlite_sample_skips_payload_decryption():
    storage = await store_with(
        [make(f"r-{i}", password="hunter2") for i in range(10)],
        secret_fields=("password",),
    )
    try:
        calls: list[dict] = []
        inner = storage._codec

        class SpyCodec:
            def encrypt_payload(self, payload, fields):
                return inner.encrypt_payload(payload, fields)

            def decrypt_payload(self, stored, fields):
                calls.append(stored)
                return inner.decrypt_payload(stored, fields)

        storage._codec = SpyCodec()  # type: ignore[assignment]

        sampled = await storage.sample_allocatable(
            POOL, states=set(ALLOCATABLE_STATES), limit=5, rng=random.Random(3)
        )
        assert len(sampled) == 5
        assert calls == []
        assert all(item.payload == {} for item in sampled)

        loaded = await storage.get(POOL, sampled[0].id)
        assert loaded is not None
        assert calls and loaded.payload["password"] == "hunter2"
    finally:
        await storage.close()


async def test_sqlite_sample_keeps_ranking_fields():
    storage = await store_with([make("a")])
    try:
        sampled = await storage.sample_allocatable(
            POOL, states=set(ALLOCATABLE_STATES), limit=1
        )
        assert len(sampled) == 1
        item = sampled[0]
        assert item.health.score == pytest.approx(0.75)
        assert item.health.samples == 4
        assert item.used_count == 3
        assert item.meta["last_used_at"] == 20.0
        assert item.meta["preference"] == 0.4
        assert item.version == 1
    finally:
        await storage.close()


async def test_sqlite_sample_non_allocatable_states_use_exact_scan():
    storage = await store_with([make("newborn", state=ResourceState.NEW)])
    try:
        items = await storage.sample_allocatable(
            POOL, states={ResourceState.NEW}, limit=5
        )
        assert [item.id for item in items] == ["newborn"]
    finally:
        await storage.close()


async def test_sqlite_acquire_does_not_scan(purser, make_pool, storage_kind, monkeypatch):
    if storage_kind != "sqlite":
        pytest.skip("sqlite-only hot path")

    pool = make_pool()
    names = [f"r-{i}" for i in range(5)]
    await pool.import_resources(
        [{"id": rid, "host": "127.0.0.1", "port": 8000} for rid in names],
        import_validate="on_import",
    )
    assert isinstance(pool.storage, SqliteStorage)
    await pool.start()

    def no_scan(*args, **kwargs):
        raise AssertionError("acquire must not full-scan the storage backend")

    monkeypatch.setattr(pool.storage, "scan", no_scan)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    assert lease.resource_id in names
    await pool.release(lease)
