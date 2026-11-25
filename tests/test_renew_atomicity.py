"""P1-6: atomic lease renewal and an explicit clock authority for liveness."""

from __future__ import annotations

import time

import fakeredis

from purser.backends.coordination import RedisCoordination
from purser.core.resource import Lease


def _redis() -> RedisCoordination:
    server = fakeredis.FakeServer()
    return RedisCoordination(client=fakeredis.aioredis.FakeRedis(server=server))


def _lease(rid: str = "r1", token: str = "t1", *, expires_in: float = 60.0) -> Lease:
    now = time.time()
    return Lease(
        token=token,
        pool="p",
        resource_id=rid,
        consumer="c",
        started_at=now,
        expires_at=now + expires_in,
    )


async def test_renew_refuses_to_resurrect_a_released_lease(monkeypatch):
    coord = _redis()
    try:
        lease = _lease()
        await coord.register_lease("p", lease)
        real_hget = coord._client.hget

        async def racing_hget(key, token):
            raw = await real_hget(key, token)
            # A release on another host removes the token between our read
            # and the renewal write.
            await coord.unregister_lease("p", lease.token)
            return raw

        # fakeredis exposes a writable client object; route only this call.
        monkeypatch.setattr(coord._client, "hget", racing_hget, raising=False)

        assert await coord.renew_lease("p", "r1", lease.token, time.time() + 60) is False
        assert await coord.get_lease("p", lease.token) is None
        assert await coord.leases_for("p", "r1") == []
    finally:
        await coord.close()


async def test_renew_updates_without_hset(monkeypatch):
    coord = _redis()
    try:
        lease = _lease()
        await coord.register_lease("p", lease)

        async def forbidden(*_a, **_k):
            raise AssertionError("renew must not issue a bare HSET")

        monkeypatch.setattr(coord._client, "hset", forbidden, raising=False)
        expires_at = time.time() + 30
        assert await coord.renew_lease("p", "r1", lease.token, expires_at) is True

        stored = await coord.get_lease("p", lease.token)
        assert stored is not None
        assert stored.expires_at == expires_at
    finally:
        await coord.close()
