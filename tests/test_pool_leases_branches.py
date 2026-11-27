"""Branch coverage for lease renewal, release, reaping and acquisition paths.

These tests pin documented behaviour (return values, error classes and the
state left behind in the storage plane) rather than internal bookkeeping, and
drive the rarer branches — idempotent replay, degraded coordination, the wait
queue, preemption, request-level checks and failure rollbacks — with the
``purser``/``make_pool``/``seed`` fixtures.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
import time

import pytest
from conftest import seed

import purser.pool.leases as leases_module
from purser.backends.base import ResourcePatch
from purser.core.resource import (
    AcquireRequest,
    CheckVerdict,
    Lease,
    Quota,
    Resource,
    SiteFailure,
    Slots,
)
from purser.core.state import ResourceState
from purser.errors import (
    BackendUnavailable,
    BadState,
    ConfigError,
    LeaseConflict,
    LeaseLimitReached,
    LeaseLost,
    NoSuitableResource,
    PoolExhausted,
    ResourceBusy,
    ResourceNotFound,
)


def _events(pool, seq: int) -> list[str]:
    return [event.type for event in pool.purser.bus.history(after_seq=seq)]


def _seq(pool) -> int:
    return pool.purser.bus.last_seq


async def _slots(pool, rid: str) -> int:
    resource = await pool.storage.get(pool.name, rid)
    assert resource is not None
    return resource.slots.used_slots


def _resource(**kwargs) -> Resource:
    kwargs.setdefault("id", "synthetic")
    kwargs.setdefault("kind", "fakecheck")
    kwargs.setdefault("state", ResourceState.IDLE)
    return Resource(**kwargs)


# -- renew -----------------------------------------------------------------


async def test_renew_keeps_peak_usage_and_extends_deadline(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    before = lease.expires_at

    assert await pool.renew(lease, used=7) >= before
    assert lease.used == 7
    # A stale retry must never roll the usage counter back.
    assert await pool._lease.renew(lease, used=3) is True
    assert lease.used == 7
    assert lease.heartbeat_at > 0
    await pool.release(lease)


async def test_renew_and_release_reject_negative_used(make_pool):
    pool = make_pool()
    await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))

    with pytest.raises(ConfigError):
        await pool.renew(lease, used=-1)
    with pytest.raises(ConfigError):
        await pool.release(lease, used=-1)
    await pool.release(lease)


async def test_renew_after_release_reports_lease_lost(make_pool):
    pool = make_pool()
    await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    await pool.release(lease)

    with pytest.raises(LeaseLost):
        await pool.renew(lease)


async def test_renew_after_force_revoke_reports_lease_lost(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    await pool.admin.force_revoke(rid, reason="ops")

    with pytest.raises(LeaseLost):
        await pool.renew(lease)


async def test_renew_fail_closed_propagates_backend_error(make_pool, monkeypatch):
    pool = make_pool()
    await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))

    async def down(*args, **kwargs):
        raise BackendUnavailable("redis down")

    monkeypatch.setattr(pool._coordination, "renew_lease", down)
    with pytest.raises(BackendUnavailable):
        await pool.renew(lease)
    monkeypatch.undo()
    await pool.release(lease)


async def test_renew_fail_open_absorbs_backend_error(make_pool, monkeypatch):
    pool = make_pool(fail_open=True)
    await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    seq = _seq(pool)

    async def down(*args, **kwargs):
        raise BackendUnavailable("redis down")

    monkeypatch.setattr(pool._coordination, "renew_lease", down)
    assert await pool.renew(lease) > 0
    assert "coordination_degraded" in _events(pool, seq)
    monkeypatch.undo()
    await pool.release(lease)


async def test_renew_enforces_use_limit(make_pool):
    pool = make_pool()
    await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c", use_limit=2))

    with pytest.raises(LeaseLimitReached):
        await pool.renew(lease, used=2)
    await pool.release(lease, used=2)


async def test_renew_enforces_max_lease_duration(make_pool):
    pool = make_pool(max_lease_duration=120.0)
    await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))

    lease.started_at = time.time() - 3600
    with pytest.raises(LeaseLimitReached):
        await pool.renew(lease)
    lease.started_at = time.time()
    assert await pool.renew(lease) > 0
    assert lease.expires_at <= lease.started_at + 120.0
    await pool.release(lease)


async def test_renew_hook_errors_are_guarded(make_pool, monkeypatch):
    pool = make_pool()
    await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))

    async def boom(resource, target):
        raise RuntimeError("hook exploded")

    monkeypatch.setattr(pool.adapter, "on_renew", boom)
    assert await pool.renew(lease) > 0
    monkeypatch.undo()
    await pool.release(lease)


# -- release ---------------------------------------------------------------


async def test_release_twice_is_idempotent(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    seq = _seq(pool)

    assert await pool.release(lease) is True
    assert await pool.release(lease) is True
    assert await _slots(pool, rid) == 0
    assert _events(pool, seq).count("lease_released") == 1


async def test_release_with_stale_token_does_not_return_slots(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    stale = Lease.new(pool.name, rid, consumer="c")

    assert await pool.release(stale) is True
    assert await _slots(pool, rid) == 1
    await pool.release(lease)
    assert await _slots(pool, rid) == 0


async def test_release_after_force_revoke_is_accepted(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    await pool.admin.force_revoke(rid, reason="ops")

    assert await pool.release(lease) is True
    assert await _slots(pool, rid) == 0


async def test_release_with_missing_slots_raises_conflict(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    assert await pool.storage.reset_usage(pool.name, rid, 0)

    with pytest.raises(LeaseConflict):
        await pool.release(lease)


async def test_release_storage_error_restores_claim(make_pool, monkeypatch):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    real = pool.storage.transition_release
    calls = {"n": 0}

    async def flaky(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("storage gone")
        return await real(*args, **kwargs)

    monkeypatch.setattr(pool.storage, "transition_release", flaky)
    with pytest.raises(OSError):
        await pool.release(lease)
    # The token was put back so the retry can settle the slot.
    assert await pool._coordination.get_lease(pool.name, lease.token) is not None
    assert await pool.release(lease) is True
    assert await _slots(pool, rid) == 0


async def test_release_restore_failure_is_logged(make_pool, monkeypatch, caplog):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))

    async def broken(*args, **kwargs):
        raise OSError("storage gone")

    async def cannot_register(*args, **kwargs):
        raise RuntimeError("coordination gone")

    monkeypatch.setattr(pool.storage, "transition_release", broken)
    monkeypatch.setattr(pool._coordination, "register_lease", cannot_register)
    with caplog.at_level(logging.ERROR, logger="purser"):
        with pytest.raises(OSError):
            await pool.release(lease)
    assert "failed to restore lease" in caplog.text
    monkeypatch.undo()
    await pool.storage.reset_usage(pool.name, rid, 0)


async def test_restore_claim_keeps_token_when_storage_unknown(make_pool, monkeypatch):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    await pool._coordination.unregister_lease(pool.name, lease.token)

    async def gone(*args, **kwargs):
        raise OSError("storage gone")

    monkeypatch.setattr(pool.storage, "get", gone)
    await pool._lease.releases._restore_release_claim(lease, lease)
    monkeypatch.undo()
    assert await pool._coordination.get_lease(pool.name, lease.token) is not None
    assert await pool.release(lease) is True
    assert await _slots(pool, rid) == 0


@pytest.mark.parametrize(
    "outcome", ["banned", "timeout", "rate_limited", "preempted"]
)
async def test_release_outcomes_feed_back_into_storage(make_pool, outcome):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c", target="site"))
    seq = _seq(pool)

    assert await pool.release(lease, outcome=outcome) is True
    resource = await pool.storage.get(pool.name, rid)
    assert resource is not None
    released = [
        event for event in pool.purser.bus.history(after_seq=seq)
        if event.type == "lease_released"
    ]
    assert released[-1].data["outcome"] == outcome
    if outcome == "rate_limited":
        assert resource.next_available_at >= time.time() + 4.0
    elif outcome == "preempted":
        assert resource.meta["cooldown:c"] > time.time()
    else:
        assert resource.meta["needs_validate"] > 0
    if outcome == "timeout":
        assert resource.health.last_error == "timeout"
    elif outcome == "banned":
        assert resource.state is not ResourceState.IDLE


async def test_release_consuming_usage_quota_applies_cooldown(make_pool):
    pool = make_pool(usage_quota=1, quota_cooldown=30.0)
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    seq = _seq(pool)

    assert await pool.release(lease, used=1) is True
    resource = await pool.storage.get(pool.name, rid)
    assert resource is not None
    assert resource.state is ResourceState.QUOTA_EXHAUSTED
    assert resource.next_available_at >= time.time() + 25.0
    assert "quota_exhausted" in _events(pool, seq)


async def test_release_consuming_period_quota_flips_state(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    await pool.admin.update_resource(rid, quota_limit=1, reason="test")
    lease = await pool.acquire(AcquireRequest(consumer="c"))

    assert await pool.release(lease, used=1) is True
    resource = await pool.storage.get(pool.name, rid)
    assert resource is not None
    assert resource.quota is not None and resource.quota.exhausted
    assert resource.state is ResourceState.QUOTA_EXHAUSTED


async def test_release_draining_resource_archives_it(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    resource = await pool.storage.get(pool.name, rid)
    assert resource is not None
    assert await pool.storage.set_state(
        pool.name,
        rid,
        resource.version,
        expect_state=resource.state,
        patch=ResourcePatch(state=ResourceState.DRAINING),
    )

    assert await pool.release(lease) is True
    resource = await pool.storage.get(pool.name, rid)
    assert resource is not None
    assert resource.state is ResourceState.ARCHIVED


async def test_release_consumer_returns_every_lease(make_pool):
    pool = make_pool()
    rids = await seed(pool, n=3)
    first = await pool.acquire(AcquireRequest(consumer="alice"))
    second = await pool.acquire(AcquireRequest(consumer="alice"))
    other = await pool.acquire(AcquireRequest(consumer="bob"))

    released = await pool.release_consumer("alice", outcome="preempted")
    assert {lease.token for lease in released} == {first.token, second.token}
    used = sum([await _slots(pool, rid) for rid in rids])
    assert used == 1
    with pytest.raises(ConfigError):
        await pool.release_consumer("")
    with pytest.raises(ConfigError):
        await pool.release_consumer("alice", outcome="not-a-real-outcome")
    await pool.release(other)


async def test_release_hook_errors_are_guarded(make_pool, monkeypatch):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))

    async def boom(resource, target):
        raise RuntimeError("before_release exploded")

    monkeypatch.setattr(pool.adapter, "before_release", boom)
    assert await pool.release(lease) is True
    assert await _slots(pool, rid) == 0


# -- stateless borrows -----------------------------------------------------


async def test_stateless_borrow_is_remembered_and_released(make_pool):
    pool = make_pool(acquire_mode="stateless")
    [rid] = await seed(pool, n=1)
    seq = _seq(pool)

    lease = await pool.acquire(AcquireRequest(consumer="c", idempotency_key="sk"))
    assert lease.token in pool._lease._stateless_leases
    replay = await pool.acquire(AcquireRequest(consumer="c", idempotency_key="sk"))
    assert replay.token == lease.token
    assert await _slots(pool, rid) == 0

    assert await pool.release(lease, used=1) is True
    assert lease.token not in pool._lease._stateless_leases
    resource = await pool.storage.get(pool.name, rid)
    assert resource is not None and resource.used_count == 1
    released = [
        event for event in pool.purser.bus.history(after_seq=seq)
        if event.type == "lease_released"
    ]
    assert released[-1].data["tracked"] is False


async def test_stateless_directed_borrow_with_failing_hook(make_pool, monkeypatch):
    pool = make_pool(acquire_mode="stateless")
    [rid] = await seed(pool, n=1)

    async def boom(resource, target):
        raise RuntimeError("before_release exploded")

    monkeypatch.setattr(pool.adapter, "before_release", boom)
    lease = await pool.acquire(AcquireRequest(rid=rid, consumer="c"))
    assert lease.rid == rid
    assert await pool.release(lease) is True


async def test_stateless_commit_rejects_unfit_resources(make_pool):
    pool = make_pool(acquire_mode="stateless")
    [rid] = await seed(pool, n=1)
    plain = AcquireRequest(rid=rid)

    assert await pool._lease._commit(plain, "missing") is None
    assert await pool._lease._commit(
        AcquireRequest(rid=rid, tags=frozenset({"gold"})), rid
    ) is None
    assert await pool._lease._commit(
        AcquireRequest(rid=rid, required_tag="gold"), rid
    ) is None
    await pool.admin.admin_set_state(rid, target=ResourceState.SUSPENDED, reason="test")
    assert await pool._lease._commit(plain, rid) is None


# -- abandoning freshly acquired leases ------------------------------------


async def test_abandon_lease_paths(make_pool):
    pool = make_pool()
    [synthetic_rid] = await seed(pool, n=1)

    # Stateless borrow: only the local memory entry is dropped.
    stateless_pool = make_pool("stateless", acquire_mode="stateless")
    [stateless_rid] = await seed(stateless_pool, n=1)
    borrow = await stateless_pool.acquire(AcquireRequest(consumer="c"))
    await stateless_pool._lease._abandon_lease(borrow)
    assert borrow.token not in stateless_pool._lease._stateless_leases
    assert await _slots(stateless_pool, stateless_rid) == 0

    # Degraded token: local teardown only, never touches coordination.
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    await pool._coordination.unregister_lease(pool.name, lease.token)
    pool._lease._degraded_tokens.add(lease.token)
    await pool._lease._abandon_lease(lease)
    assert await _slots(pool, synthetic_rid) == 0
    assert lease.token not in pool._lease._degraded_tokens

    # Tracked token: unregister + return the slot + drop the consumer hold.
    tracked = await pool.acquire(AcquireRequest(consumer="tracked"))
    await pool._lease._abandon_lease(tracked)
    assert await _slots(pool, synthetic_rid) == 0
    holds = pool._lease.consumer_holds.get("tracked", set())
    assert tracked.rid not in holds


async def test_abandon_wait_handle_returns_delivered_lease(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)

    pending = pool._lease._enqueue_waiter(AcquireRequest(consumer="c"))
    pending.cancel_pending()
    await pool._lease._abandon_wait_handle(pending)
    assert await _slots(pool, rid) == 0

    lease = await pool.acquire(AcquireRequest(consumer="c"))
    delivered = pool._lease._enqueue_waiter(AcquireRequest(consumer="c"))
    delivered.future.set_result(lease)
    await pool._lease._abandon_wait_handle(delivered)
    assert await _slots(pool, rid) == 0


# -- wait queue ------------------------------------------------------------


async def test_serve_waiters_defers_failed_allocations(make_pool, monkeypatch):
    pool = make_pool()
    await seed(pool, n=1)
    handle = pool._lease._enqueue_waiter(AcquireRequest(consumer="c"))

    async def not_ready(_req):
        raise BadState("not ready")

    monkeypatch.setattr(pool._lease, "_try_allocate", not_ready)
    assert await pool._lease.serve_waiters() == 0
    assert handle in pool._waiters
    handle.cancel_pending()


async def test_serve_waiters_repushes_after_unexpected_error(make_pool, monkeypatch):
    pool = make_pool()
    await seed(pool, n=1)
    handle = pool._lease._enqueue_waiter(AcquireRequest(consumer="c"))

    async def broken(_req):
        raise RuntimeError("allocator exploded")

    monkeypatch.setattr(pool._lease, "_try_allocate", broken)
    with pytest.raises(RuntimeError):
        await pool._lease.serve_waiters()
    assert handle in pool._waiters
    handle.cancel_pending()


async def test_serve_waiters_abandons_lease_for_cancelled_waiter(make_pool, monkeypatch):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    held = await pool.acquire(AcquireRequest(consumer="holder"))
    handle = pool._lease._enqueue_waiter(AcquireRequest(consumer="c"))

    async def allocate_then_vanish(_req):
        handle.cancel_pending()
        return held

    monkeypatch.setattr(pool._lease, "_try_allocate", allocate_then_vanish)
    assert await pool._lease.serve_waiters() == 0
    assert await _slots(pool, rid) == 0


async def test_serve_waiters_honours_limit(make_pool):
    pool = make_pool()
    await seed(pool, n=2)
    handles = [
        pool._lease._enqueue_waiter(AcquireRequest(consumer=f"c{i}"))
        for i in range(2)
    ]
    assert await pool._lease.serve_waiters(limit=1) == 1
    first = handles[0].future.result()
    await pool.release(first)
    # Releasing wakes the remaining waiter through the normal waker path.
    assert handles[1].future.done()
    second = handles[1].future.result()
    await pool.release(second)


# -- reap / expiry / loss --------------------------------------------------


async def test_reap_releases_expired_lease(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    seq = _seq(pool)

    lease.expires_at = time.time() - 1
    expired = await pool._leases.reap()
    assert [item.token for item in expired] == [lease.token]
    assert await _slots(pool, rid) == 0
    assert pool._leases.get(lease.token) is None
    assert "lease_expired" in _events(pool, seq)


async def test_reap_callback_errors_are_swallowed(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    lease.expires_at = time.time() - 1

    async def boom(_lease):
        raise RuntimeError("watchdog exploded")

    pool._leases.on_expire = boom
    assert [item.token for item in await pool._leases.reap()] == [lease.token]
    pool._leases.unregister(lease.token)
    await pool.storage.reset_usage(pool.name, rid, 0)


async def test_on_expired_when_token_already_gone(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    seq = _seq(pool)
    await pool._coordination.unregister_lease(pool.name, lease.token)

    await pool._lease.on_expired(lease)
    assert "lease_lost" in _events(pool, seq)
    assert await _slots(pool, rid) == 1
    await pool.storage.reset_usage(pool.name, rid, 0)


async def test_on_expired_when_slots_already_returned(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    seq = _seq(pool)
    assert await pool.storage.reset_usage(pool.name, rid, 0)

    await pool._lease.on_expired(lease)
    assert "lease_lost" in _events(pool, seq)


async def test_on_lost_returns_slots_and_notifies_adapter(make_pool, monkeypatch):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    seq = _seq(pool)
    lost: list[str] = []

    async def record(resource, target):
        lost.append(resource.id)

    monkeypatch.setattr(pool.adapter, "on_lease_lost", record)
    await pool._lease.on_lost(lease)
    assert lost == [rid]
    assert await _slots(pool, rid) == 0
    assert "lease_lost" in _events(pool, seq)


async def test_on_lost_when_token_already_gone_keeps_slots(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    await pool._coordination.unregister_lease(pool.name, lease.token)

    await pool._lease.on_lost(lease)
    assert await _slots(pool, rid) == 1
    await pool.storage.reset_usage(pool.name, rid, 0)


async def test_on_lost_hook_errors_are_guarded(make_pool, monkeypatch):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))

    async def boom(resource, target):
        raise RuntimeError("hook exploded")

    monkeypatch.setattr(pool.adapter, "on_lease_lost", boom)
    await pool._lease.on_lost(lease)
    assert await _slots(pool, rid) == 0


async def test_reap_loop_logs_errors_and_reraises_cancel(make_pool, monkeypatch, caplog):
    pool = make_pool()
    calls = {"n": 0}

    async def fake_sleep(_delay):
        calls["n"] += 1
        if calls["n"] > 2:
            raise asyncio.CancelledError

    async def reap_once(self):
        if calls["n"] == 1:
            raise RuntimeError("reap failed")
        raise asyncio.CancelledError

    monkeypatch.setattr(leases_module.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(type(pool._leases), "reap", reap_once)
    with caplog.at_level(logging.ERROR, logger="purser"):
        with pytest.raises(asyncio.CancelledError):
            await pool._lease.reap_loop()
    assert "reaper loop failed" in caplog.text


# -- idempotent acquisition ------------------------------------------------


async def test_idempotent_acquire_replays_then_reissues(make_pool):
    pool = make_pool()
    await seed(pool, n=1)
    first = await pool.acquire(AcquireRequest(consumer="c", idempotency_key="k"))
    second = await pool.acquire(AcquireRequest(consumer="c", idempotency_key="k"))
    assert second.token == first.token
    await pool.release(first)

    third = await pool.acquire(AcquireRequest(consumer="c", idempotency_key="k"))
    assert third.token != first.token
    await pool.release(third)


async def test_idempotent_acquire_shields_inflight_request(make_pool):
    pool = make_pool()
    await seed(pool, n=1)
    request = AcquireRequest(consumer="c", idempotency_key="k")
    first, second = await asyncio.gather(pool.acquire(request), pool.acquire(request))
    assert first.token == second.token
    await pool.release(first)


async def test_idempotent_acquire_does_not_cache_failures(make_pool):
    pool = make_pool()
    request = AcquireRequest(rid="missing", idempotency_key="k")
    for _ in range(2):
        with pytest.raises(ResourceNotFound):
            await pool.acquire(request)


# -- rate limiting ---------------------------------------------------------


async def test_rate_limit_rejects_after_burst(make_pool):
    pool = make_pool(scheduler="token_bucket", rate_limit=0.01, rate_burst=1)
    await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c", timeout=0))

    with pytest.raises(PoolExhausted) as exc:
        await pool.acquire(AcquireRequest(consumer="c", timeout=0))
    assert "rate limited" in str(exc.value)
    pool._lease.reset_rate_limits()
    assert not pool._lease._rate_buckets
    await pool.release(lease)


async def test_rate_buckets_are_bounded(make_pool):
    pool = make_pool()
    for index in range(4100):
        pool._lease._rate_bucket(f"consumer-{index}")
    assert len(pool._lease._rate_buckets) == 4096


# -- directed acquisition --------------------------------------------------


async def test_directed_acquire_reports_missing_and_unvalidated(make_pool):
    pool = make_pool()
    with pytest.raises(ResourceNotFound):
        await pool.acquire(AcquireRequest(rid="missing"))

    await pool.import_resources(
        [{"id": "new-1", "host": "127.0.0.1"}], import_validate="none"
    )
    with pytest.raises(PoolExhausted) as exc:
        await pool.acquire(AcquireRequest(rid="new-1", timeout=0))
    assert "never validated" in str(exc.value)


async def test_directed_acquire_reports_validating_resource(make_pool):
    pool = make_pool()
    await pool.import_resources(
        [{"id": "v-1", "host": "127.0.0.1"}], import_validate="none"
    )
    resource = await pool.storage.get(pool.name, "v-1")
    assert resource is not None
    assert await pool.storage.set_state(
        pool.name,
        "v-1",
        resource.version,
        expect_state=resource.state,
        patch=ResourcePatch(state=ResourceState.VALIDATING),
    )
    with pytest.raises(PoolExhausted) as exc:
        await pool.acquire(AcquireRequest(rid="v-1", timeout=0))
    assert "still validating" in str(exc.value)


async def test_directed_acquire_reports_suspended_state(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    await pool.admin.admin_set_state(rid, target=ResourceState.SUSPENDED, reason="test")

    with pytest.raises(BadState):
        await pool.acquire(AcquireRequest(rid=rid))


async def test_directed_acquire_returns_consumer_hold(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    first = await pool.acquire(AcquireRequest(consumer="c", rid=rid))

    second = await pool.acquire(AcquireRequest(consumer="c", rid=rid))
    assert second.token == first.token
    assert second.payload is not None
    await pool.release(first)


async def test_directed_acquire_busy_times_out(make_pool):
    pool = make_pool()
    [rid] = await seed(pool, n=1)
    holder = await pool.acquire(AcquireRequest(consumer="a", rid=rid))

    with pytest.raises(ResourceBusy):
        await pool.acquire(
            AcquireRequest(consumer="b", rid=rid, timeout=0.05)
        )
    await pool.release(holder)


# -- preemption ------------------------------------------------------------


async def test_preemption_revokes_lower_priority_holder(make_pool):
    pool = make_pool(preemptible=True, preempt_threshold=5)
    [rid] = await seed(pool, n=1)
    seq = _seq(pool)
    low = await pool.acquire(AcquireRequest(consumer="low", priority=1))

    high = await pool.acquire(AcquireRequest(consumer="high", priority=10))
    assert high.token != low.token
    assert "lease_preempted" in _events(pool, seq)
    assert await _slots(pool, rid) == 1
    await pool.release(high)


@pytest.mark.parametrize("blocker", ["tags", "required_tag"])
async def test_preemption_skips_unmatched_victims(make_pool, blocker):
    pool = make_pool(preemptible=True, preempt_threshold=1)
    await seed(pool, n=1)
    low = await pool.acquire(AcquireRequest(consumer="low"))
    filters = (
        {"tags": frozenset({"gold"})}
        if blocker == "tags"
        else {"required_tag": "gold"}
    )

    with pytest.raises(PoolExhausted):
        await pool.acquire(
            AcquireRequest(consumer="high", priority=10, timeout=0, **filters)
        )
    assert await pool.renew(low) > 0
    await pool.release(low)


async def test_preemption_skips_non_allocatable_victim(make_pool):
    pool = make_pool(preemptible=True, preempt_threshold=1)
    [rid] = await seed(pool, n=1)
    low = await pool.acquire(AcquireRequest(consumer="low"))
    resource = await pool.storage.get(pool.name, rid)
    assert resource is not None
    assert await pool.storage.set_state(
        pool.name,
        rid,
        resource.version,
        expect_state=resource.state,
        patch=ResourcePatch(state=ResourceState.DRAINING),
    )

    with pytest.raises(PoolExhausted):
        await pool.acquire(AcquireRequest(consumer="high", priority=10, timeout=0))
    await pool.storage.reset_usage(pool.name, rid, 0)
    pool._leases.unregister(low.token)


async def test_preemption_revoke_failure_is_tolerated(make_pool, monkeypatch, caplog):
    pool = make_pool(preemptible=True, preempt_threshold=1)
    await seed(pool, n=1)
    low = await pool.acquire(AcquireRequest(consumer="low"))

    async def boom(*args, **kwargs):
        raise RuntimeError("revoke failed")

    monkeypatch.setattr(pool._lease, "_revoker", boom)
    with caplog.at_level(logging.WARNING, logger="purser"):
        with pytest.raises(PoolExhausted):
            await pool.acquire(
                AcquireRequest(consumer="high", priority=10, timeout=0)
            )
    assert "preemption of" in caplog.text
    monkeypatch.undo()
    await pool.release(low)


# -- request-level checks --------------------------------------------------


async def test_request_check_verdicts_are_cached(make_pool, monkeypatch):
    pool = make_pool()
    await seed(pool, n=1)
    calls = {"n": 0}

    async def passing(resource, request):
        calls["n"] += 1
        return CheckVerdict.PASS

    monkeypatch.setattr(pool.adapter, "on_request_check", passing)
    first = await pool.acquire(AcquireRequest(consumer="c", target="site"))
    await pool.release(first)
    second = await pool.acquire(AcquireRequest(consumer="c", target="site"))
    assert calls["n"] == 1
    await pool.release(second)


async def test_request_check_error_becomes_fail(make_pool, monkeypatch):
    pool = make_pool()
    await seed(pool, n=1)

    async def exploding(resource, request):
        raise RuntimeError("check exploded")

    monkeypatch.setattr(pool.adapter, "on_request_check", exploding)
    with pytest.raises(NoSuitableResource):
        await pool.acquire(AcquireRequest(consumer="c", target="site", timeout=0))


async def test_request_check_ban_takes_resource_out_of_service(make_pool, monkeypatch):
    pool = make_pool()
    [rid] = await seed(pool, n=1)

    async def banning(resource, request):
        return CheckVerdict.BAN

    monkeypatch.setattr(pool.adapter, "on_request_check", banning)
    with pytest.raises(NoSuitableResource):
        await pool.acquire(AcquireRequest(consumer="c", target="site", timeout=0))
    resource = await pool.storage.get(pool.name, rid)
    assert resource is not None
    assert resource.state is not ResourceState.IDLE


async def test_request_check_cache_is_swept(make_pool, monkeypatch):
    pool = make_pool()
    await seed(pool, n=1)
    now = time.time()
    pool._lease._check_cache = {
        ("", "site", f"stale-{index}"): (CheckVerdict.PASS, now - 1.0)
        for index in range(2050)
    }

    async def passing(resource, request):
        return CheckVerdict.PASS

    monkeypatch.setattr(pool.adapter, "on_request_check", passing)
    lease = await pool.acquire(AcquireRequest(consumer="c", target="site"))
    assert len(pool._lease._check_cache) == 1
    await pool.release(lease)


# -- commit rollbacks ------------------------------------------------------


async def test_commit_rejects_consumer_limit(make_pool):
    pool = make_pool(max_leases_per_consumer=1)
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))

    assert await pool._lease._commit(AcquireRequest(consumer="c"), rid) is None
    await pool.release(lease)


async def test_commit_rejects_mutex_group_conflict(make_pool):
    pool = make_pool()
    rids = await seed(pool, n=2)
    for rid in rids:
        await pool.admin.update_resource(rid, mutex_group="g", reason="test")
    held = await pool.acquire(AcquireRequest(consumer="c"))
    other = next(rid for rid in rids if rid != held.rid)

    assert await pool._lease._commit(AcquireRequest(consumer="c"), other) is None
    await pool.release(held)


async def test_acquire_cancelled_during_register_rolls_back(make_pool, monkeypatch):
    pool = make_pool()
    [rid] = await seed(pool, n=1)

    async def cancelled(*args, **kwargs):
        raise asyncio.CancelledError

    monkeypatch.setattr(pool._coordination, "register_lease", cancelled)
    with pytest.raises(asyncio.CancelledError):
        await pool.acquire(AcquireRequest())
    assert await _slots(pool, rid) == 0


async def test_acquire_cancelled_after_register_rolls_back(make_pool, monkeypatch):
    pool = make_pool()
    [rid] = await seed(pool, n=1)

    def cancelled(self, lease, renew):
        raise asyncio.CancelledError

    monkeypatch.setattr(type(pool._leases), "register", cancelled)
    with pytest.raises(asyncio.CancelledError):
        await pool.acquire(AcquireRequest())
    assert await _slots(pool, rid) == 0


async def test_register_backend_unavailable_fail_closed(make_pool, monkeypatch):
    pool = make_pool()
    [rid] = await seed(pool, n=1)

    async def down(*args, **kwargs):
        raise BackendUnavailable("coordination down")

    monkeypatch.setattr(pool._coordination, "register_lease", down)
    with pytest.raises(PoolExhausted):
        await pool.acquire(AcquireRequest(timeout=0))
    assert await _slots(pool, rid) == 0


async def test_register_backend_unavailable_fail_open_degrades(make_pool, monkeypatch):
    pool = make_pool(fail_open=True)
    [rid] = await seed(pool, n=1)
    seq = _seq(pool)

    async def down(*args, **kwargs):
        raise BackendUnavailable("coordination down")

    monkeypatch.setattr(pool._coordination, "register_lease", down)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    assert pool._lease.is_degraded(lease.token)
    assert "coordination_degraded" in _events(pool, seq)

    assert await pool.release(lease) is True
    assert await _slots(pool, rid) == 0
    assert not pool._lease.is_degraded(lease.token)


async def test_register_unexpected_error_rolls_back(make_pool, monkeypatch):
    pool = make_pool()
    [rid] = await seed(pool, n=1)

    async def broken(*args, **kwargs):
        raise RuntimeError("coordination broken")

    monkeypatch.setattr(pool._coordination, "register_lease", broken)
    with pytest.raises(PoolExhausted):
        await pool.acquire(AcquireRequest(timeout=0))
    assert await _slots(pool, rid) == 0


@pytest.mark.parametrize("mode", ["lease", "stateless"])
async def test_on_acquire_hook_errors_are_guarded(make_pool, monkeypatch, mode):
    pool = make_pool(acquire_mode=mode)
    await seed(pool, n=1)

    async def boom(resource, lease):
        raise RuntimeError("on_acquire exploded")

    monkeypatch.setattr(pool.adapter, "on_acquire", boom)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    assert lease.token
    assert await pool.release(lease) is True


# -- bounded bookkeeping ---------------------------------------------------


async def test_affinity_history_is_bounded(make_pool):
    pool = make_pool()
    for index in range(66):
        pool._lease._record_affinity("c", f"r-{index}")
    assert len(pool._lease.affinity["c"]) == 64
    assert "r-0" not in pool._lease.affinity["c"]
    assert await pool._lease._affinity_scores(None, []) == {}
    assert await pool._lease._affinity_scores("unknown", []) == {}
    scores = await pool._lease._affinity_scores(
        "c", [_resource(id="r-65", tags=frozenset({"gold"}))]
    )
    assert scores == {"r-65": 1.0}


async def test_stateless_memory_is_bounded(make_pool):
    pool = make_pool(acquire_mode="stateless")
    oldest = None
    for index in range(2050):
        lease = Lease.new(pool.name, f"r-{index}")
        if oldest is None:
            oldest = lease.token
        pool._lease._remember_stateless(lease)
    assert len(pool._lease._stateless_leases) == 2048
    assert oldest is not None and oldest not in pool._lease._stateless_leases


# -- diagnostics -----------------------------------------------------------


async def test_skip_reasons_cover_every_blocker(make_pool):
    pool = make_pool()
    rids = await seed(pool, n=2)
    now = time.time()
    service = pool._lease
    plain = AcquireRequest()

    busy = Slots(max_slots=1, used_slots=1)
    assert await service._skip_reason(
        _resource(slots=busy), AcquireRequest(slots=2), now
    ) == "slots"
    assert await service._skip_reason(
        _resource(tags=frozenset()), AcquireRequest(tags=frozenset({"gold"})), now
    ) == "tags"
    assert await service._skip_reason(
        _resource(tags=frozenset()), AcquireRequest(required_tag="gold"), now
    ) == "required_tag"

    closed_at = (dt.datetime.fromtimestamp(now) + dt.timedelta(minutes=5)).strftime(
        "%H:%M"
    )
    assert await service._skip_reason(
        _resource(schedule_window=(closed_at, closed_at)), plain, now
    ) == "schedule_window"
    assert await service._skip_reason(
        _resource(expires_at=now - 1), plain, now
    ) == "expired"
    assert await service._skip_reason(
        _resource(next_available_at=now + 60), plain, now
    ) == "cooldown"
    assert await service._skip_reason(
        _resource(site_failures={"site": SiteFailure("site", until=now + 60)}),
        AcquireRequest(target="site"),
        now,
    ) == "site_failure"
    assert await service._skip_reason(
        _resource(usage_quota=1, used_count=1), plain, now
    ) == "usage_quota"
    assert await service._skip_reason(
        _resource(quota=Quota(limit=1, used=1)), plain, now
    ) == "quota"
    assert await service._skip_reason(
        _resource(id="plain", tags=frozenset({"gold"})), plain, now
    ) == "contention"

    for rid in rids:
        await pool.admin.update_resource(rid, mutex_group="g", reason="test")
    held = await pool.acquire(AcquireRequest(consumer="c"))
    other = next(rid for rid in rids if rid != held.rid)
    stored = await pool.storage.get(pool.name, other)
    assert stored is not None
    assert await service._skip_reason(
        stored, AcquireRequest(consumer="c"), now
    ) == "mutex_group"
    assert await service._holds_group("c", "g", held.rid) is False

    service.consumer_holds["c"].add(held.rid)
    same_id = _resource(id=held.rid, tags=frozenset({"gold"}))
    assert await service._skip_reason(
        same_id, AcquireRequest(consumer="c"), now
    ) == "consumer_hold"
    await pool.release(held)


async def test_exhausted_message_reports_unvalidated(make_pool):
    pool = make_pool()
    await seed(pool, n=1)
    await pool.import_resources(
        [{"id": "new-1", "host": "127.0.0.1"}], import_validate="none"
    )
    holder = await pool.acquire(AcquireRequest())

    with pytest.raises(PoolExhausted) as exc:
        await pool.acquire(AcquireRequest(timeout=0))
    assert "not validated yet" in str(exc.value)
    await pool.release(holder)


async def test_exhausted_message_lists_filters_and_truncation(make_pool, monkeypatch):
    pool = make_pool()
    await seed(pool, n=2)
    monkeypatch.setattr(pool._lease, "_SKIP_SCAN_LIMIT", 1)

    with pytest.raises(PoolExhausted) as exc:
        await pool.acquire(
            AcquireRequest(
                tags=frozenset({"gold"}), required_tag="gold", timeout=0
            )
        )
    message = str(exc.value)
    assert "tags=" in message
    assert "required_tag='gold'" in message
    assert "(first 1 scanned)" in message


async def test_acquire_explains_running_validate_job(make_pool, monkeypatch):
    pool = make_pool()
    await pool.import_resources(
        [{"id": "new-1", "host": "127.0.0.1"}], import_validate="none"
    )
    monkeypatch.setattr(
        pool._validation, "_spawn", lambda coro: (coro.close(), None)[1]
    )
    job = pool._start_validate_job("job-1", ["new-1"], "system")
    assert job["status"] == "running"

    with pytest.raises(PoolExhausted) as exc:
        await pool.acquire(AcquireRequest(timeout=0))
    assert "validate job job-1 is 0/1 done" in str(exc.value)


async def test_acquire_reports_validating_counts(make_pool):
    pool = make_pool()
    await pool.import_resources(
        [{"id": "v-1", "host": "127.0.0.1"}], import_validate="none"
    )
    resource = await pool.storage.get(pool.name, "v-1")
    assert resource is not None
    assert await pool.storage.set_state(
        pool.name,
        "v-1",
        resource.version,
        expect_state=resource.state,
        patch=ResourcePatch(state=ResourceState.VALIDATING),
    )
    with pytest.raises(PoolExhausted) as exc:
        await pool.acquire(AcquireRequest(timeout=0))
    assert "validating=1" in str(exc.value)


async def test_active_validate_job_skips_finished_jobs(make_pool):
    pool = make_pool()
    now = time.time()
    base = {
        "pool": pool.name,
        "total": 1,
        "done": 1,
        "alive": 1,
        "failed": 0,
        "skipped": 0,
        "errors": [],
        "created_at": now,
        "updated_at": now,
        "operator": "system",
        "cancelled": False,
    }
    pool._validation._jobs.put(
        "run", {**base, "id": "run", "status": "running", "created_at": now}
    )
    pool._validation._jobs.put(
        "done", {**base, "id": "done", "status": "completed", "created_at": now + 1}
    )
    active = pool._lease._active_validate_job()
    assert active is not None and active["id"] == "run"


async def test_consumer_limit_short_circuits_skip_reasons(make_pool):
    pool = make_pool(max_leases_per_consumer=1)
    await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))

    with pytest.raises(PoolExhausted) as exc:
        await pool.acquire(AcquireRequest(consumer="c", timeout=0))
    assert "consumer_limit=1" in str(exc.value)
    await pool.release(lease)
