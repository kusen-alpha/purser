"""usage_quota + quota_cooldown: N uses then a short auto-recovery window."""

from __future__ import annotations

import asyncio
import time

import pytest
from test_bundle import SimpleAdapter

from purser import ConfigError, PoolConfig, Purser, ResourceState


async def _cooldown_pool(quota_cooldown: float):
    ps = Purser(maintenance_enabled=False)
    ps.register_adapter(SimpleAdapter())
    pool = ps.create_pool("q", "sim", config={"quota_cooldown": quota_cooldown})
    await pool.import_resources(
        [{"id": "r1", "usage_quota": 2}], import_validate="on_import"
    )
    await ps.start()
    return ps, pool


async def test_quota_cooldown_gates_reset_and_then_recovers():
    ps, pool = await _cooldown_pool(1.0)
    try:
        for _ in range(2):
            lease = await pool.acquire()
            await pool.release(lease)
        now = time.time()
        res = await pool.get("r1")
        assert res.state is ResourceState.QUOTA_EXHAUSTED
        assert res.used_count == 2
        assert res.next_available_at is not None
        assert now <= res.next_available_at <= now + 1.6

        await pool.run_maintenance("quota_reset")
        assert (await pool.get("r1")).state is ResourceState.QUOTA_EXHAUSTED

        await asyncio.sleep(1.3)
        await pool.run_maintenance("quota_reset")
        res = await pool.get("r1")
        assert res.state is ResourceState.IDLE
        assert res.used_count == 0
        assert res.next_available_at is None
    finally:
        await ps.close()


async def test_quota_cooldown_zero_preserves_immediate_reset():
    ps, pool = await _cooldown_pool(0.0)
    try:
        for _ in range(2):
            lease = await pool.acquire()
            await pool.release(lease)
        assert (await pool.get("r1")).state is ResourceState.QUOTA_EXHAUSTED
        await pool.run_maintenance("quota_reset")
        res = await pool.get("r1")
        assert res.state is ResourceState.IDLE
        assert res.used_count == 0
    finally:
        await ps.close()


async def test_quota_cooldown_can_be_acquired_again():
    ps, pool = await _cooldown_pool(1.0)
    try:
        for _ in range(2):
            lease = await pool.acquire()
            await pool.release(lease)
        await asyncio.sleep(1.3)
        await pool.run_maintenance("quota_reset")
        lease = await pool.acquire()
        assert lease.resource_id == "r1"
        await pool.release(lease)
    finally:
        await ps.close()


def test_quota_cooldown_validation():
    with pytest.raises(ConfigError, match="quota_cooldown"):
        PoolConfig(kind="k", quota_cooldown=-1)
