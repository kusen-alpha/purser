"""EWMA scoring, quarantine policy, probe guard and validation gating."""

from __future__ import annotations

import time

import pytest

from purser.core.health import HealthReport, ProbeGuard, ValidationGate
from purser.core.quarantine import QuarantinePolicy
from purser.core.resource import HealthStats, Resource


def test_ewma_cold_start_converges_fast():
    health = HealthStats()  # neutral 0.5
    health.update(alive=True, now=0.0)
    # alpha = 0.6 on first sample: 0.5*0.4 + 1.0*0.6 = 0.8
    assert health.score == pytest.approx(0.8)
    health.update(alive=False, now=1.0)
    # alpha = 0.3 afterwards: 0.8*0.7 + 0.0*0.3 = 0.56
    assert health.score == pytest.approx(0.56)
    assert health.consecutive_failures == 1


def test_health_stats_tracks_consecutive_failures():
    health = HealthStats(score=0.9, samples=10)
    health.update(alive=False, now=0.0)
    health.update(alive=False, now=1.0)
    assert health.consecutive_failures == 2
    health.update(alive=True, now=2.0)
    assert health.consecutive_failures == 0


def test_quarantine_backoff_with_cap_and_jitter():
    policy = QuarantinePolicy(base=30.0, cap=1800.0, jitter=10.0)
    durations = sorted(policy.duration(n) for n in (1, 2, 3, 7))
    assert durations[0] <= 30.0 + 10.0
    assert durations[-1] <= 1800.0 + 10.0
    assert durations[-1] >= 1800.0


def test_threshold_trial_protection():
    now = time.time()
    policy = QuarantinePolicy(max_failures=3)
    fresh = now - 60  # inside trial window
    old = now - 48 * 3600  # outside
    assert policy.threshold_for(fresh, now) == 2
    assert policy.threshold_for(old, now) == 3
    assert policy.first_failure_ban(fresh, now) is False
    assert policy.first_failure_ban(old, now) is False
    strict = QuarantinePolicy(max_failures=1)
    assert strict.threshold_for(old, now) == 1
    assert strict.first_failure_ban(old, now) is True


def test_probe_guard_disables_flaky_probe():
    guard = ProbeGuard(fail_threshold=2, cooldown=60.0)
    assert guard.enabled("p1")
    guard.record("p1", ok=False)
    guard.record("p1", ok=False)
    assert not guard.enabled("p1")
    assert guard.all_down(["p1"]) is True
    guard.record("p2", ok=True)
    assert guard.all_down(["p1", "p2"]) is False  # p2 is healthy again


def test_validation_gate_sampling_is_deterministic():
    gate = ValidationGate(stale_after=0.0, sample=0.5)
    a = Resource(id="aaaa", kind="x", payload={}, health=HealthStats(last_checked_at=0.0))
    assert bool(gate.due_background(a)) is bool(gate.due_background(a))
    assert isinstance(gate.due_background(a), bool)
    assert gate.due_background(a) == gate.due_background(
        Resource(id="aaaa", kind="x", payload={}, health=HealthStats(last_checked_at=0.0))
    )


def test_needs_validate_bypasses_staleness_gate():
    gate = ValidationGate(stale_after=3600.0, sample=0.0)
    fresh = Resource(
        id="a", kind="x", payload={}, health=HealthStats(last_checked_at=time.time())
    )
    assert not gate.due_background(fresh)
    fresh.meta["needs_validate"] = time.time()
    assert gate.due_background(fresh)


def test_stale_after_gate():
    gate = ValidationGate(stale_after=10.0, sample=1.0)
    fresh = Resource(id="a", kind="x", payload={}, health=HealthStats(last_checked_at=time.time()))
    stale = Resource(id="b", kind="x", payload={}, health=HealthStats(last_checked_at=time.time() - 60))
    assert not gate.stale(fresh)
    assert gate.stale(stale)


def test_health_report_ok_and_fail():
    ok = HealthReport.ok(latency_ms=5.0)
    assert ok.alive and ok.action.value == "ok" and ok.latency_ms == 5.0
    bad = HealthReport.fail("refused")
    assert not bad.alive and bad.error == "refused"
