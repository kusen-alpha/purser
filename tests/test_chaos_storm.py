"""Deterministic chaos: fault rain, interleaved churn and restart invariants.

Uses a seeded RNG so sequences are reproducible.  Invariants after every step:
* slot accounting never exceeds ``max_slots`` or goes negative;
* states stay inside the 12-state enum;
* after quiescing, no lease is left in coordination and no slot is stuck.
"""

from __future__ import annotations

import asyncio
import random

import fakeredis
import pytest
from conftest import REAL_REDIS_URL, FakeChecker

from purser import Purser
from purser.core.resource import AcquireRequest, UsageOutcome
from purser.core.state import ResourceState
from purser.errors import PurserError

ALLOWED_ERRORS = (
    PurserError,
    asyncio.TimeoutError,
)


def _check_invariants(pool, resources, max_slots: int) -> None:
    assert 0 <= len(resources) <= 5
    for resource in resources:
        assert 0 <= resource.concurrency.used_slots <= min(
            resource.concurrency.max_slots, max_slots
        ), resource.id
        assert resource.state in ResourceState


async def test_chaos_storm_invariants(storage_kind, coordination_kind):
    rng = random.Random(20261106)
    ps = Purser(
        maintenance_enabled=False,
        storage=storage_kind,
        coordination=coordination_kind,
        sqlite_path=":memory:" if storage_kind == "sqlite" else None,
        redis_client=fakeredis.aioredis.FakeRedis() if coordination_kind == "redis" else None,
    )
    ps.register_adapter(FakeChecker())
    pool = ps.create_pool("chaos", "fakecheck", max_slots=3)
    try:
        records = [{"id": f"r{i}", "host": "127.0.0.1", "port": i} for i in range(5)]
        await pool.import_resources(records, import_validate="on_import")

        # Fault rain: register/renew may fail ~5% of the time.
        orig_register = pool._coordination.register_lease
        orig_renew = pool._coordination.renew_lease

        async def chaos_register(pname, lease):
            if rng.random() < 0.05:
                raise OSError("chaos: register")
            return await orig_register(pname, lease)

        async def chaos_renew(pname, rid, token, expires_at):
            if rng.random() < 0.05:
                raise OSError("chaos: renew")
            return await orig_renew(pname, rid, token, expires_at)

        pool._coordination.register_lease = chaos_register
        pool._coordination.renew_lease = chaos_renew

        held: list = []
        for _ in range(150):
            op = rng.choice(
                [
                    "acquire",
                    "release",
                    "revoke",
                    "ban",
                    "unban",
                    "validate",
                    "drain",
                    "top-up",
                    "gc",
                ]
            )
            rid = f"r{rng.randrange(5)}"
            try:
                if op == "acquire":
                    lease = await pool.acquire(
                        AcquireRequest(
                            consumer=rng.choice(["c0", "c1", "c2"]),
                            slots=rng.choice([1, 1, 2]),
                            timeout=0.05,
                        )
                    )
                    held.append(lease)
                elif op == "release" and held:
                    lease = held.pop(rng.randrange(len(held)))
                    await pool.release(
                        lease,
                        outcome=rng.choice(list(UsageOutcome)),
                        used=rng.choice([0, 1, 2]),
                    )
                elif op == "revoke":
                    await pool.force_revoke(rid, operator="chaos", reason="rain")
                elif op == "ban":
                    await pool.ban(rid, operator="chaos", reason="rain")
                elif op == "unban":
                    await pool.unban(rid, operator="chaos", reason="rain")
                elif op == "validate":
                    await pool.validate_now(rid)
                elif op == "drain":
                    await pool.drain(rid, operator="chaos", reason="rain")
                elif op == "top-up":
                    await pool.top_up(rid, operator="chaos", amount=1)
                elif op == "gc":
                    await pool.run_maintenance("quarantine_gc")
            except ALLOWED_ERRORS:
                pass
            resources = await pool.query(limit=10_000)
            _check_invariants(pool, resources, max_slots=3)

        # Quiesce: return every lease this process still holds.
        for lease in list(held):
            try:
                await pool.release(lease)
            except ALLOWED_ERRORS:
                pass
        held.clear()

        resources = await pool.query(limit=10_000)
        _check_invariants(pool, resources, max_slots=3)
        assert await pool._coordination.active(pool.name) == []
    finally:
        await ps.close()


async def test_chaos_restart_across_churn(tmp_path, storage_kind, coordination_kind):
    if storage_kind != "sqlite":
        pytest.skip("persistent storage required")
    path = str(tmp_path / "chaos.db")
    rng = random.Random(7)

    def build() -> Purser:
        ps = Purser(
            maintenance_enabled=False,
            storage="sqlite",
            coordination=coordination_kind,
            sqlite_path=path,
            redis_client=fakeredis.aioredis.FakeRedis() if coordination_kind == "redis" else None,
        )
        ps.register_adapter(FakeChecker())
        return ps

    ps1 = build()
    pool = ps1.create_pool("px", "fakecheck", max_slots=2)
    records = [{"id": f"r{i}", "host": f"1.1.1.{i}", "port": i} for i in range(4)]
    await pool.import_resources(records, import_validate="on_import")
    held: list = []
    for _ in range(60):
        rid = f"r{rng.randrange(4)}"
        try:
            if rng.random() < 0.5:
                held.append(
                    await pool.acquire(
                        AcquireRequest(consumer="c", slots=1, timeout=0.05)
                    )
                )
            elif held:
                await pool.release(held.pop())
            elif rng.random() < 0.5:
                await pool.force_revoke(rid, operator="chaos", reason="restart")
        except ALLOWED_ERRORS:
            pass
    # Hard stop with some leases possibly still held.
    await ps1.close()

    ps2 = build()
    try:
        pool2 = ps2.create_pool("px", "fakecheck", max_slots=2)
        await pool2.start()  # reconcile orphaned slot usage
        resources = await pool2.query(limit=10_000)
        assert len(resources) == 4  # master data intact
        hosts = {r.id: r.payload["host"] for r in resources}
        assert hosts == {f"r{i}": f"1.1.1.{i}" for i in range(4)}
        for resource in resources:
            assert 0 <= resource.concurrency.used_slots <= 2
            assert resource.state in ResourceState
            if resource.state in (ResourceState.IDLE, ResourceState.ACTIVE, ResourceState.FULL):
                assert resource.concurrency.used_slots == 0  # reconciled
    finally:
        await ps2.close()


async def test_multi_instance_chaos_no_oversell(
    tmp_path, storage_kind, coordination_kind, shared_backend_harness
):
    if storage_kind != "sqlite" or coordination_kind != "redis":
        pytest.skip("sqlite + redis combination only")
    if not REAL_REDIS_URL:
        pytest.skip("real Redis required for the shared-backend chaos test")
    path = str(tmp_path / "chaos-shared.db")
    rng = random.Random(99)

    def build() -> Purser:
        ps = Purser(
            maintenance_enabled=False,
            storage="sqlite",
            coordination="redis",
            sqlite_path=path,
            redis_url=REAL_REDIS_URL,
        )
        ps.register_adapter(FakeChecker())
        return ps

    instances = [build() for _ in range(3)]
    pools = [ps.create_pool("shared", "fakecheck", max_slots=2) for ps in instances]
    held: list[list] = [[] for _ in range(3)]
    try:
        await pools[0].import_resources(
            [{"id": "r0", "host": "1.1.1.1", "port": 1}], import_validate="on_import"
        )
        for _ in range(90):
            idx = rng.randrange(3)
            pool = pools[idx]
            if rng.random() < 0.6:
                try:
                    held[idx].append(
                        await pool.acquire(
                            AcquireRequest(consumer=f"m{idx}", slots=1, timeout=0.05)
                        )
                    )
                except ALLOWED_ERRORS:
                    pass
            elif held[idx]:
                await pool.release(held[idx].pop())
            # Invariant: never over-commit beyond max_slots on the shared store.
            res = await pools[0].get("r0")
            assert res is not None and 0 <= res.concurrency.used_slots <= 2
        for i in range(3):
            for lease in list(held[i]):
                try:
                    await pools[i].release(lease)
                except ALLOWED_ERRORS:
                    pass
            held[i].clear()
        res = await pools[0].get("r0")
        assert res is not None and res.concurrency.used_slots == 0
        assert await pools[0]._coordination.active("shared") == []
    finally:
        for ps in instances:
            await ps.close()
