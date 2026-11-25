"""P2-7: one heartbeat ticker renews every lease of a pool."""

from __future__ import annotations

import asyncio

from purser.core.lease import LeaseManager
from purser.core.resource import Lease


async def test_single_ticker_covers_many_leases():
    manager = LeaseManager(heartbeat_interval=0.02)
    renewed: dict[str, int] = {}

    def renew_fn(lease: Lease):
        async def renew(item: Lease) -> bool:
            renewed[item.token] = renewed.get(item.token, 0) + 1
            return True

        return renew

    leases = [Lease.new("p", f"r{i}", ttl=60.0) for i in range(20)]
    try:
        for lease in leases:
            manager.register(lease, renew_fn(lease))

        assert manager._ticker is not None

        for _ in range(200):
            if len([t for t, n in renewed.items() if n >= 1]) == 20:
                break
            await asyncio.sleep(0.02)

        assert len(renewed) == 20
        assert manager._ticker is not None  # one shared ticker, not one per lease
    finally:
        await manager.close()
        assert manager._ticker is None


async def test_unregistered_lease_stops_renewing():
    manager = LeaseManager(heartbeat_interval=0.02)
    calls: list[str] = []

    async def renew(lease: Lease) -> bool:
        calls.append(lease.token)
        return True

    keep, drop = Lease.new("p", "r1", ttl=60.0), Lease.new("p", "r2", ttl=60.0)
    try:
        manager.register(keep, renew)
        manager.register(drop, renew)
        for _ in range(200):
            if keep.token in calls:
                break
            await asyncio.sleep(0.02)

        manager.unregister(drop.token)
        calls.clear()
        for _ in range(200):
            if keep.token in calls:
                break
            await asyncio.sleep(0.02)
        assert keep.token in calls
        assert drop.token not in calls
    finally:
        await manager.close()
