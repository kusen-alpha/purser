"""Contract refinements: outcome coercion, error taxonomy, remote typing."""

from __future__ import annotations

import time

import pytest
from conftest import FakeChecker, seed

from purser import (
    BackendUnavailable,
    ConfigError,
    LeaseView,
    NoSuitableResource,
    NotStarted,
    PoolConfig,
    PoolExhausted,
    ProxyResource,
    PurserError,
    QuotaExceeded,
    Resource,
    ResourceBusy,
    SyncPurser,
    UsageOutcome,
    connect,
    error_by_code,
    is_retryable,
)
from purser.local import LocalScheduler, LocalSchedulerClient
from purser.local.errors import remote_error

# -- outcome coercion -----------------------------------------------------


async def test_async_release_accepts_wire_outcome(purser, make_pool):
    pool = make_pool()
    await seed(pool, 1)
    lease = await pool.acquire()
    assert await pool.release(lease, outcome="captcha") is True


async def test_async_release_rejects_unknown_outcome(purser, make_pool):
    pool = make_pool()
    await seed(pool, 1)
    lease = await pool.acquire()
    try:
        with pytest.raises(ConfigError):
            await pool.release(lease, outcome="not-a-real-outcome")
    finally:
        await pool.release(lease)


def test_sync_release_accepts_wire_outcome():
    with SyncPurser(maintenance_enabled=False) as ps:
        ps.register_adapter(FakeChecker())
        pool = ps.create_pool("p", "fakecheck")
        pool.import_resources(
            [{"id": "r1", "host": "127.0.0.1", "port": 8080}],
            import_validate="on_import",
        )
        lease = pool.acquire(timeout=5)
        assert pool.release(lease, outcome="timeout") is True


# -- error taxonomy -------------------------------------------------------


def test_error_codes_come_from_the_class_hierarchy():
    assert error_by_code("backend_unavailable") is BackendUnavailable
    assert error_by_code("quota_exceeded") is QuotaExceeded
    assert error_by_code("pool_exhausted") is PoolExhausted
    assert error_by_code("nope") is None


def test_error_registry_is_complete_and_late_bindings_work():
    from purser.local.errors import (
        SchedulerProtocolError,
        SchedulerTimeout,
        SchedulerUnavailable,
    )

    assert error_by_code(ConfigError.code) is ConfigError
    assert error_by_code(NotStarted.code) is NotStarted
    assert error_by_code(SchedulerUnavailable.code) is SchedulerUnavailable
    assert error_by_code(SchedulerTimeout.code) is SchedulerTimeout
    assert error_by_code(SchedulerProtocolError.code) is SchedulerProtocolError

    class LateError(PurserError):
        code = "late_error"

    assert error_by_code("late_error") is LateError

    class DuplicateLateError(PurserError):
        code = "late_error"

    assert error_by_code("late_error") is LateError


def test_retryable_classification():
    assert is_retryable(PoolExhausted())
    assert is_retryable(BackendUnavailable())
    assert not is_retryable(QuotaExceeded())
    assert not is_retryable(NoSuitableResource("target", checked=2))


def test_remote_error_preserves_details():
    error = remote_error(
        "no_suitable_resource",
        "no suitable resource for target='t'",
        {"target": "t", "checked": 3},
    )
    assert isinstance(error, NoSuitableResource)
    assert error.target == "t" and error.checked == 3

    not_started = remote_error(
        "not_started",
        "cannot acquire: pool 'p' is not started",
        {"pool": "p", "operation": "acquire"},
    )
    assert not_started.pool == "p" and not_started.operation == "acquire"


def test_short_ttl_default_heartbeat_stays_below_ttl():
    cfg = PoolConfig(kind="fakecheck", lease_ttl=0.3)
    assert cfg.heartbeat_interval == pytest.approx(0.1)
    assert cfg.heartbeat_interval < cfg.lease_ttl


# -- remote typing / connect / auto-renew ---------------------------------


def _local_scheduler() -> LocalScheduler:
    return LocalScheduler(adapters=[FakeChecker()], maintenance_enabled=False)


def _seed_scheduler(sched: LocalScheduler, **config) -> None:
    sched.create_pool("p", "fakecheck", config=config)
    sched.import_resources(
        "p",
        [{"id": "r1", "host": "1.2.3.4", "port": 8080}],
        import_validate="on_import",
    )


def test_remote_lease_can_restore_a_typed_resource():
    with _local_scheduler() as sched:
        _seed_scheduler(sched)
        client = LocalSchedulerClient(
            sched.address,
            resource_types={"fakecheck": ProxyResource},
        )
        try:
            lease = client.acquire("p", consumer="c", timeout=5)
            assert isinstance(lease.resource, ProxyResource)
            assert lease.resource.host == "1.2.3.4"
            assert lease.resource.port == 8080
            lease.release()
        finally:
            client.close()


def test_remote_error_types_survive_ipc():
    with _local_scheduler() as sched:
        _seed_scheduler(sched)
        client = LocalSchedulerClient(sched.address)
        try:
            lease = client.acquire("p", consumer="c", timeout=5)
            try:
                with pytest.raises(ResourceBusy) as info:
                    client.acquire(
                        "p",
                        consumer="c2",
                        resource_id="r1",
                        timeout=0.1,
                    )
                assert info.value.retryable is True
            finally:
                lease.release()
        finally:
            client.close()


def test_scheduler_heartbeat_keeps_short_ttl_lease_alive():
    with _local_scheduler() as sched:
        _seed_scheduler(sched, lease_ttl=0.3)
        client = LocalSchedulerClient(sched.address)
        try:
            lease = client.acquire("p", consumer="c", timeout=5)
            time.sleep(0.8)
            # The scheduler process renews on its own; a stopped heartbeat
            # would make this renewal fail with LeaseConflict.
            expires_at = client.renew("p", lease)
            assert expires_at > time.time()
            lease.release()
        finally:
            client.close()


async def test_connect_exposes_an_async_pool_facade():
    with _local_scheduler() as sched:
        _seed_scheduler(sched)
        pool = connect(
            "p",
            sched.address,
            resource_types={"fakecheck": ProxyResource},
        )
        try:
            async with pool.lease(consumer="c", timeout=5) as lease:
                assert isinstance(lease.resource, ProxyResource)
                assert lease.resource.host == "1.2.3.4"
                assert isinstance(lease, LeaseView)
            stats = await pool.stats()
            assert isinstance(stats, dict)

            await pool.admin.ban("r1", reason="contract")
            banned = await pool.stats()
            assert banned["counts"]["banned"] == 1
            await pool.admin.unban("r1")
            unbanned = await pool.stats()
            assert unbanned["counts"].get("banned", 0) == 0
        finally:
            await pool.aclose()


# -- LeaseView ------------------------------------------------------------


async def test_async_lease_matches_lease_view(purser, make_pool):
    pool = make_pool()
    await seed(pool, 1)
    lease = await pool.acquire()
    try:
        assert isinstance(lease, LeaseView)
        assert isinstance(lease.resource, Resource)
    finally:
        await pool.release(lease)


def test_sync_lease_matches_lease_view():
    with SyncPurser(maintenance_enabled=False) as ps:
        ps.register_adapter(FakeChecker())
        pool = ps.create_pool("p", "fakecheck")
        pool.import_resources(
            [{"id": "r1", "host": "127.0.0.1", "port": 8080}],
            import_validate="on_import",
        )
        lease = pool.acquire(timeout=5)
        try:
            assert isinstance(lease, LeaseView)
            assert lease.outcome is UsageOutcome.OK
        finally:
            pool.release(lease)
