"""Opt-in bundle state: cross-pool reservations and fairness markers (v2).

This store is only constructed when ``bundle_mode="two_phase"`` or
``bundle_fairness`` is configured.  Plain leases and the per-pool
coordination plane never read these keys, so the default flow is untouched.
"""

from __future__ import annotations

import functools
import json
import time
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from typing import Any, TypeVar

from redis import asyncio as aioredis
from redis.exceptions import RedisError
from typing_extensions import ParamSpec

from purser.errors import BackendUnavailable, ConfigError


@dataclass(frozen=True)
class ReservationMember:
    """One member lease bookmarked on a bundle reservation."""

    pool: str
    rid: str
    token: str


@dataclass
class Reservation:
    """One bundle's cross-pool state on the coordination plane."""

    bundle_id: str
    consumer: str | None
    pools: tuple[str, ...] = ()
    status: str = "fair"  # "fair" | "assembling" | "committed"
    members: dict[str, ReservationMember] = field(default_factory=dict)
    expires_at: float = 0.0
    seq: int = 0

    def active(self, now: float) -> bool:
        # Assembling counts too: a bundle mid-construction still claims its
        # consumer/pool quota, so concurrent admission cannot over-subscribe.
        return (
            self.status in ("fair", "committed", "assembling")
            and self.expires_at > now
        )


def _now() -> float:
    return time.time()


_P = ParamSpec("_P")
_R = TypeVar("_R")


def _fail_closed(
    fn: Callable[_P, Coroutine[Any, Any, _R]],
) -> Callable[_P, Coroutine[Any, Any, _R]]:
    @functools.wraps(fn)
    async def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        try:
            return await fn(*args, **kwargs)
        except RedisError as exc:
            raise BackendUnavailable(f"bundle redis error: {exc}") from exc

    return wrapper


class BundleStore:
    """Reservation + fairness markers for opt-in bundle modes."""

    distributed: bool = False

    async def next_seq(self) -> int:
        raise NotImplementedError

    async def admit(
        self, bundle_id: str, consumer: str | None, pools: tuple[str, ...], ttl: float
    ) -> None:
        raise NotImplementedError

    async def begin_members(self, bundle_id: str, ttl: float) -> None:
        raise NotImplementedError

    async def add_member(
        self, bundle_id: str, member: ReservationMember
    ) -> None:
        raise NotImplementedError

    async def confirm(self, bundle_id: str, ttl: float) -> None:
        raise NotImplementedError

    async def get(self, bundle_id: str) -> Reservation | None:
        raise NotImplementedError

    async def active(self, now: float) -> list[Reservation]:
        raise NotImplementedError

    async def expired(self, now: float) -> list[Reservation]:
        raise NotImplementedError

    async def release(self, bundle_id: str) -> None:
        raise NotImplementedError

    async def refresh(self, bundle_id: str, ttl: float) -> None:
        raise NotImplementedError


class MemoryBundleStore(BundleStore):
    """In-process reservation store (single scheduler process)."""

    def __init__(self) -> None:
        self._reservations: dict[str, Reservation] = {}
        self._seq = 0

    async def next_seq(self) -> int:
        self._seq += 1
        return self._seq

    async def admit(
        self, bundle_id: str, consumer: str | None, pools: tuple[str, ...], ttl: float
    ) -> None:
        existing = self._reservations.get(bundle_id)
        if existing is None:
            existing = Reservation(bundle_id=bundle_id, consumer=consumer, pools=pools)
            self._reservations[bundle_id] = existing
        existing.consumer = consumer
        existing.pools = pools
        existing.status = "fair"
        existing.expires_at = _now() + ttl

    async def begin_members(self, bundle_id: str, ttl: float) -> None:
        res = self._require(bundle_id)
        res.status = "assembling"
        res.members.clear()
        res.expires_at = _now() + ttl

    async def add_member(
        self, bundle_id: str, member: ReservationMember
    ) -> None:
        res = self._require(bundle_id)
        res.members[member.pool] = member

    async def confirm(self, bundle_id: str, ttl: float) -> None:
        res = self._require(bundle_id)
        res.status = "committed"
        res.expires_at = _now() + ttl

    async def get(self, bundle_id: str) -> Reservation | None:
        return self._reservations.get(bundle_id)

    async def active(self, now: float) -> list[Reservation]:
        return [res for res in self._reservations.values() if res.active(now)]

    async def expired(self, now: float) -> list[Reservation]:
        return [
            res for res in self._reservations.values() if res.expires_at <= now
        ]

    async def release(self, bundle_id: str) -> None:
        self._reservations.pop(bundle_id, None)

    async def refresh(self, bundle_id: str, ttl: float) -> None:
        res = self._reservations.get(bundle_id)
        if res is not None:
            res.expires_at = _now() + ttl

    def _require(self, bundle_id: str) -> Reservation:
        res = self._reservations.get(bundle_id)
        if res is None:
            raise ConfigError(f"bundle {bundle_id!r} has no reservation")
        return res


class RedisBundleStore(BundleStore):
    """Redis reservation store shared by every instance on the plane."""

    distributed: bool = True

    def __init__(
        self,
        *,
        client: aioredis.Redis | None = None,
        url: str | None = None,
        key_prefix: str = "purser",
    ) -> None:
        if client is None and url is None:
            raise ConfigError("RedisBundleStore requires a client or url")
        self._client = client or aioredis.Redis.from_url(
            url,
            protocol=2,
            socket_connect_timeout=5.0,
            socket_timeout=5.0,
        )
        self.kp = key_prefix

    def _meta(self, bundle_id: str) -> str:
        return f"{self.kp}:bundle:v2:{bundle_id}"

    def _dates(self) -> str:
        return f"{self.kp}:bundle:v2:dates"

    def _seq_key(self) -> str:
        return f"{self.kp}:bundle:v2:seq"

    @_fail_closed
    async def next_seq(self) -> int:
        raw = await self._client.incr(self._seq_key())
        return int(raw)

    async def _write(
        self,
        bundle_id: str,
        *,
        status: str,
        consumer: str | None,
        pools: tuple[str, ...],
        ttl: float,
    ) -> None:
        key = self._meta(bundle_id)
        pipe = self._client.pipeline()
        pipe.hset(key, "status", status)
        pipe.hset(key, "consumer", consumer or "")
        pipe.hset(key, "pools", json.dumps(list(pools)))
        pipe.zadd(self._dates(), {bundle_id: _now() + ttl})
        pipe.pexpire(key, int(ttl * 1000))
        await pipe.execute()

    @_fail_closed
    async def admit(
        self, bundle_id: str, consumer: str | None, pools: tuple[str, ...], ttl: float
    ) -> None:
        await self._write(
            bundle_id, status="fair", consumer=consumer, pools=pools, ttl=ttl
        )

    @_fail_closed
    async def begin_members(self, bundle_id: str, ttl: float) -> None:
        key = self._meta(bundle_id)
        existing = await self._client.hgetall(key)
        member_fields = [
            field_name for field_name in existing if field_name.startswith(b"m:")
        ]
        pipe = self._client.pipeline()
        if member_fields:
            pipe.hdel(key, *member_fields)
        pipe.hset(key, "status", "assembling")
        pipe.zadd(self._dates(), {bundle_id: _now() + ttl})
        pipe.pexpire(key, int(ttl * 1000))
        await pipe.execute()

    @_fail_closed
    async def add_member(
        self, bundle_id: str, member: ReservationMember
    ) -> None:
        key = self._meta(bundle_id)
        await self._client.hset(
            key, f"m:{member.pool}", json.dumps([member.rid, member.token])
        )

    @_fail_closed
    async def confirm(self, bundle_id: str, ttl: float) -> None:
        key = self._meta(bundle_id)
        existing = await self._client.hgetall(key)
        consumer = (existing.get(b"consumer") or b"").decode()
        pools_raw = existing.get(b"pools") or b"[]"
        pools = tuple(json.loads(pools_raw.decode()))
        await self._write(
            bundle_id,
            status="committed",
            consumer=consumer or None,
            pools=pools,
            ttl=ttl,
        )

    @_fail_closed
    async def get(self, bundle_id: str) -> Reservation | None:
        key = self._meta(bundle_id)
        raw = await self._client.hgetall(key)
        if not raw:
            return None
        status = (raw.get(b"status") or b"fair").decode()
        consumer = (raw.get(b"consumer") or b"").decode() or None
        pools = tuple(json.loads((raw.get(b"pools") or b"[]").decode()))
        members: dict[str, ReservationMember] = {}
        for field_name, value in raw.items():
            name = field_name.decode()
            if not name.startswith("m:"):
                continue
            pool = name[2:]
            rid, token = json.loads(value.decode())
            members[pool] = ReservationMember(pool=pool, rid=rid, token=token)
        score = await self._client.zscore(self._dates(), bundle_id)
        return Reservation(
            bundle_id=bundle_id,
            consumer=consumer,
            pools=pools,
            status=status,
            members=members,
            expires_at=float(score or 0.0),
        )

    async def _scan(self, *, active: bool, now: float) -> list[Reservation]:
        if active:
            ids = await self._client.zrangebyscore(self._dates(), now, "+inf")
        else:
            ids = await self._client.zrangebyscore(self._dates(), "-inf", now)
        out: list[Reservation] = []
        for raw_id in ids:
            bundle_id = raw_id.decode() if isinstance(raw_id, bytes) else raw_id
            res = await self.get(bundle_id)
            if res is None:
                continue
            if active and not res.active(now):
                continue
            out.append(res)
        return out

    async def active(self, now: float) -> list[Reservation]:
        return await self._scan(active=True, now=now)

    async def expired(self, now: float) -> list[Reservation]:
        return await self._scan(active=False, now=now)

    @_fail_closed
    async def release(self, bundle_id: str) -> None:
        pipe = self._client.pipeline()
        pipe.delete(self._meta(bundle_id))
        pipe.zrem(self._dates(), bundle_id)
        await pipe.execute()

    @_fail_closed
    async def refresh(self, bundle_id: str, ttl: float) -> None:
        key = self._meta(bundle_id)
        if not await self._client.exists(key):
            return
        pipe = self._client.pipeline()
        pipe.zadd(self._dates(), {bundle_id: _now() + ttl})
        pipe.pexpire(key, int(ttl * 1000))
        await pipe.execute()
