"""Admin/ops edges: move collisions, forced states, ban-with-leases, plane pinning."""

from __future__ import annotations

import fakeredis
import pytest
from conftest import FakeChecker, seed

from purser import Purser
from purser.backends.base import ResourcePatch
from purser.core.resource import AcquireRequest, CheckVerdict, Resource, UsageOutcome
from purser.core.state import ResourceState
from purser.errors import BadState, ConfigError, NoSuitableResource, PoolExhausted


class _BanOnTargetChecker(FakeChecker):
    """Request-level checker that BANs any request carrying a target (5.8)."""

    kind = "bantarget"

    async def on_request_check(
        self, resource: Resource, req: AcquireRequest
    ) -> CheckVerdict:
        return CheckVerdict.BAN if req.target else CheckVerdict.PASS


async def test_move_collision_rejected(make_pool, purser):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    target = purser.pool("target", "fakecheck")
    await target.import_resources([{"id": rid, "host": "9.9.9.9", "port": 9}])
    with pytest.raises(BadState):
        await pool.move_resource(rid, target, operator="ops", reason="x")
    assert await pool.get(rid) is not None
    res = await target.get(rid)
    assert res is not None and res.payload["host"] == "9.9.9.9"  # target untouched


async def test_admin_set_state_busy_rejected(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    with pytest.raises(BadState):
        await pool.admin_set_state(rid, target=ResourceState.IDLE, operator="ops", reason="x")
    with pytest.raises(BadState):
        await pool.admin_set_state(
            rid, target=ResourceState.SUSPENDED, operator="ops", reason="x"
        )
    await pool.release(lease)
    await pool.admin_set_state(
        rid, target=ResourceState.SUSPENDED, operator="ops", reason="ok"
    )
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.SUSPENDED


async def test_ban_revokes_active_leases(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    await pool.ban(rid, operator="ops", reason="abuse")
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.BANNED
    assert res.concurrency.used_slots == 0
    assert await pool._coordination.active(pool.name) == []
    assert await pool.release(lease) is True  # stale release is harmless


async def test_request_level_ban_records_site_failure(purser):
    purser.register_type(_BanOnTargetChecker())
    pool = purser.pool("bantargetpool", "bantarget")
    await pool.import_resources(
        [{"id": "r1", "host": "10.0.0.1", "port": 8081}], import_validate="on_import"
    )
    with pytest.raises(NoSuitableResource):
        await pool.acquire(AcquireRequest(consumer="c", target="site.com", timeout=0))
    res = await pool.get("r1")
    assert res is not None
    assert res.site_failures["site.com"].count == 1
    assert res.state in (ResourceState.QUARANTINED, ResourceState.BANNED)


async def test_reconfigure_immutable_planes_rejected(make_pool):
    pool = make_pool()
    await seed(pool, n=1)
    with pytest.raises(ConfigError):
        pool.reconfigure(storage="sqlite")
    with pytest.raises(ConfigError):
        pool.reconfigure(coordination="redis")
    pool.reconfigure(allocator="round_robin")  # runtime knobs still fine
    assert pool._allocator.name == "round_robin"


async def test_query_empty_states_equals_all(make_pool):
    pool = make_pool()
    await seed(pool, n=2)
    all_res = await pool.query()
    empty_res = await pool.query(states=set())
    assert [r.id for r in all_res] == [r.id for r in empty_res]
    assert len(all_res) == 2


async def test_outcome_captcha_and_timeout(make_pool):
    pool = make_pool()
    ids = await seed(pool, n=2)
    for rid, outcome in zip(
        ids, [UsageOutcome.CAPTCHA, UsageOutcome.TIMEOUT], strict=True
    ):
        lease = await pool.acquire(
            AcquireRequest(resource_id=rid, consumer="c", target="site.com")
        )
        await pool.release(lease, outcome=outcome)
        res = await pool.get(rid)
        assert res is not None and res.state is ResourceState.IDLE
        if outcome is UsageOutcome.CAPTCHA:
            assert res.site_failures.get("site.com") is not None
            assert res.health.last_error is None
        else:
            assert res.health.last_error == "timeout"
            assert res.meta.get("needs_validate") is not None


async def test_restart_new_state_preserved(tmp_path, storage_kind, coordination_kind):
    if storage_kind != "sqlite":
        pytest.skip("persistent storage required")
    path = str(tmp_path / "new.db")

    def build() -> Purser:
        ps = Purser(
            maintenance_enabled=False,
            storage="sqlite",
            coordination=coordination_kind,
            sqlite_path=path,
            redis_client=fakeredis.aioredis.FakeRedis()
            if coordination_kind == "redis"
            else None,
        )
        ps.register_type(FakeChecker())
        return ps

    ps1 = build()
    pool = ps1.create_pool("px", "fakecheck")
    await pool.import_resources([{"id": "r1", "host": "1.1.1.1", "port": 1}])
    rid = "r1"
    res = await pool.get(rid)
    assert res is not None and res.state is ResourceState.NEW
    await ps1.close()

    ps2 = build()
    try:
        pool2 = ps2.create_pool("px", "fakecheck")
        res = await pool2.get(rid)
        assert res is not None and res.state is ResourceState.NEW
        await pool2.validate_now(rid)
        res = await pool2.get(rid)
        assert res is not None and res.state is ResourceState.IDLE
    finally:
        await ps2.close()


async def test_restart_suspended_and_resume(tmp_path, storage_kind, coordination_kind):
    if storage_kind != "sqlite":
        pytest.skip("persistent storage required")
    path = str(tmp_path / "suspend.db")

    def build() -> Purser:
        ps = Purser(
            maintenance_enabled=False,
            storage="sqlite",
            coordination=coordination_kind,
            sqlite_path=path,
            redis_client=fakeredis.aioredis.FakeRedis()
            if coordination_kind == "redis"
            else None,
        )
        ps.register_type(FakeChecker())
        return ps

    ps1 = build()
    pool = ps1.create_pool("px", "fakecheck")
    [rid] = await seed(pool, n=1)
    await pool.suspend(rid, operator="ops", reason="maintenance")
    await ps1.close()

    ps2 = build()
    try:
        pool2 = ps2.create_pool("px", "fakecheck")
        res = await pool2.get(rid)
        assert res is not None and res.state is ResourceState.SUSPENDED
        await pool2.resume(rid, operator="ops", reason="back")
        res = await pool2.get(rid)
        assert res is not None and res.state is ResourceState.IDLE
    finally:
        await ps2.close()


async def test_restart_cooldown_preserved(tmp_path, storage_kind, coordination_kind):
    if storage_kind != "sqlite":
        pytest.skip("persistent storage required")
    path = str(tmp_path / "cooldown.db")

    def build() -> Purser:
        ps = Purser(
            maintenance_enabled=False,
            storage="sqlite",
            coordination=coordination_kind,
            sqlite_path=path,
            redis_client=fakeredis.aioredis.FakeRedis()
            if coordination_kind == "redis"
            else None,
        )
        ps.register_type(FakeChecker())
        return ps

    ps1 = build()
    pool = ps1.create_pool("px", "fakecheck", cooldown_after_release=60)
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    await pool.release(lease)
    await ps1.close()

    ps2 = build()
    try:
        pool2 = ps2.create_pool("px", "fakecheck", cooldown_after_release=60)
        await pool2.start()
        with pytest.raises(PoolExhausted):
            await pool2.acquire(AcquireRequest(timeout=0))
        res = await pool2.get(rid)
        assert res is not None and res.next_available_at is not None
        await pool2.storage.set_state(
            pool2.name, rid, res.version, expect_state=res.state,
            patch=ResourcePatch(next_available_at=0.0),
        )
        ok = await pool2.acquire(AcquireRequest(consumer="c"))
        assert ok.resource_id == rid
        await pool2.release(ok, used=0)
    finally:
        await ps2.close()


async def test_import_dedup_key_non_id(make_pool):
    pool = make_pool()
    report = await pool.import_resources(
        [
            {"id": "a", "host": "h1", "port": 1},
            {"id": "b", "host": "h1", "port": 2},
        ],
        dedup_key="host",
    )
    assert report.imported == 1 and report.updated == 1
    assert await pool.get("a") is None
    assert await pool.get("h1") is not None
