"""P1-5: release-time usage feedback survives CAS contention and is not lost."""

from __future__ import annotations

import logging

import pytest
from conftest import seed

from purser.core.resource import AcquireRequest, UsageOutcome


def _flaky_set_state(pool, *, fail_first: int = 0, fail_forever: bool = False):
    original = pool.storage.set_state
    calls = {"n": 0}

    async def flaky(*args, **kwargs):
        calls["n"] += 1
        if fail_forever or calls["n"] <= fail_first:
            return False
        return await original(*args, **kwargs)

    pool.storage.set_state = flaky
    return calls


async def test_cas_retry_does_not_double_count(make_pool):
    pool = make_pool(cooldown_after_release=0.0)
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    calls = _flaky_set_state(pool, fail_first=1)

    await pool.release(lease, outcome=UsageOutcome.OK, used=1)

    res = await pool.get(rid)
    assert calls["n"] == 2
    assert res.used_count == 1


async def test_cas_retry_does_not_stack_health_updates(make_pool):
    pool = make_pool(cooldown_after_release=0.0)
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c", target="site.com"))
    before = await pool.get(rid)
    samples_before = before.health.samples
    _flaky_set_state(pool, fail_first=1)

    await pool.release(lease, outcome=UsageOutcome.NETWORK_ERROR, used=1)

    res = await pool.get(rid)
    assert res.health.samples == samples_before + 1
    assert res.health.consecutive_failures == 1
    failure = res.site_failures["site.com"]
    assert failure.count == 1


async def test_cas_retry_does_not_double_quota(make_pool):
    pool = make_pool(cooldown_after_release=0.0)
    [rid] = await seed(pool, n=1)
    from purser.backends.base import ResourcePatch
    from purser.core.resource import Quota

    res = await pool.get(rid)
    await pool.storage.set_state(
        pool.name,
        rid,
        res.version,
        expect_state=res.state,
        patch=ResourcePatch(quota=Quota(limit=10, used=0)),
    )
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    _flaky_set_state(pool, fail_first=1)

    await pool.release(lease, outcome=UsageOutcome.OK, used=1)

    res = await pool.get(rid)
    assert res.quota is not None and res.quota.used == 1


async def test_exhausted_retries_are_counted_and_logged(make_pool, caplog):
    pool = make_pool(cooldown_after_release=0.0)
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    before = (await pool.get(rid)).used_count
    _flaky_set_state(pool, fail_forever=True)

    with caplog.at_level(logging.WARNING, logger="purser"):
        await pool.release(lease, outcome=UsageOutcome.OK, used=1)

    assert (await pool.get(rid)).used_count == before  # nothing committed
    counter = pool.purser.metrics.counters[f"pool.{pool.name}.feedback_dropped_total"]
    assert counter == 1
    assert any("usage feedback dropped" in rec.message for rec in caplog.records)


async def test_release_still_succeeds_when_feedback_is_dropped(make_pool):
    pool = make_pool(cooldown_after_release=0.0)
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    _flaky_set_state(pool, fail_forever=True)

    assert await pool.release(lease, outcome=UsageOutcome.OK) is True
    res = await pool.get(rid)
    assert res.concurrency.used_slots == 0


async def test_ok_release_feeds_latency_into_health(make_pool):
    pool = make_pool(cooldown_after_release=0.0)
    [rid] = await seed(pool, n=1)
    before = await pool.get(rid)
    samples_before = before.health.samples
    score_before = before.health.score
    lease = await pool.acquire(AcquireRequest(consumer="c"))

    await pool.release(lease, outcome=UsageOutcome.OK, used=1, latency_ms=42.5)

    res = await pool.get(rid)
    assert res.health.samples == samples_before + 1
    assert res.health.last_latency_ms == 42.5
    assert res.health.consecutive_failures == 0
    assert res.health.score > score_before


async def test_latency_is_ignored_for_failure_outcomes(make_pool):
    pool = make_pool(cooldown_after_release=0.0)
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))

    await pool.release(
        lease, outcome=UsageOutcome.NETWORK_ERROR, used=1, latency_ms=5.0
    )

    res = await pool.get(rid)
    assert res.health.last_error == "network_error"
    assert res.health.consecutive_failures == 1


async def test_negative_latency_rejected(make_pool):
    from purser.errors import ConfigError

    pool = make_pool(cooldown_after_release=0.0)
    [rid] = await seed(pool, n=1)
    lease = await pool.acquire(AcquireRequest(consumer="c"))
    with pytest.raises(ConfigError):
        await pool.release(lease, latency_ms=-1.0)
    await pool.release(lease)
