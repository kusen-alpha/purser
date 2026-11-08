"""Transient lease / wake-up plane: memory and redis (S10 / F2).

Leases, heartbeats and wake-ups live here and are rebuildable: losing them on
an abnormal restart only costs scheduling accuracy.  The redis implementation
uses hash keys per resource plus a token index, so cross-instance revoke and
listing work without the blocking ``KEYS`` command.
"""

from __future__ import annotations

import functools
import json
import time
from collections.abc import Callable, Coroutine
from typing import Any, TypeVar

from redis import asyncio as aioredis
from redis.exceptions import RedisError
from typing_extensions import ParamSpec

from purser.backends.base import Coordination
from purser.core.resource import Lease
from purser.errors import BackendUnavailable, ConfigError


def _encode_lease(lease: Lease) -> str:
    # ``resource`` is a process-local typed snapshot, not coordination state.
    return json.dumps(
        {k: v for k, v in vars(lease).items() if k not in ("payload", "resource")}
    )


def _decode_lease(raw: str) -> Lease:
    item = json.loads(raw)
    item["token"] = item.get("token")
    return Lease(**item)


_P = ParamSpec("_P")
_R = TypeVar("_R")

# KEYS: ids set, token -> rid hash.  ARGV: token, lease-key prefix.
# The lease key is derived inside the script so lookup + removal is one step;
# all derived keys share the ``{pool}`` hash tag with KEYS.
_UNREGISTER_LUA = """
local rid = redis.call('HGET', KEYS[2], ARGV[1])
if not rid then return nil end
local lease_key = ARGV[2] .. rid
local raw = redis.call('HGET', lease_key, ARGV[1])
redis.call('HDEL', lease_key, ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
if redis.call('HLEN', lease_key) == 0 then
  redis.call('SREM', KEYS[1], rid)
end
return raw
"""


def _fail_closed(
    fn: Callable[_P, Coroutine[Any, Any, _R]],
) -> Callable[_P, Coroutine[Any, Any, _R]]:
    @functools.wraps(fn)
    async def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        try:
            return await fn(*args, **kwargs)
        except RedisError as exc:
            raise BackendUnavailable(f"redis error: {exc}") from exc

    return wrapper


class MemoryCoordination(Coordination):
    """In-process coordination.  Suitable for single-node deployments."""

    def __init__(self) -> None:
        self._by_token: dict[str, dict[str, str]] = {}      # pool -> token -> rid
        self._by_rid: dict[str, dict[str, dict[str, Lease]]] = {}  # pool -> rid -> token -> lease

    def _tokens(self, pool: str) -> dict[str, str]:
        return self._by_token.setdefault(pool, {})

    def _leases(self, pool: str, rid: str) -> dict[str, Lease]:
        return self._by_rid.setdefault(pool, {}).setdefault(rid, {})

    async def register_lease(self, pool: str, lease: Lease) -> None:
        self._tokens(pool)[lease.token] = lease.resource_id
        self._leases(pool, lease.resource_id)[lease.token] = lease

    async def unregister_lease(self, pool: str, token: str) -> Lease | None:
        rid = self._tokens(pool).pop(token, None)
        if rid is None:
            return None
        return self._leases(pool, rid).pop(token, None)

    async def get_lease(self, pool: str, token: str) -> Lease | None:
        rid = self._tokens(pool).get(token)
        if rid is None:
            return None
        return self._leases(pool, rid).get(token)

    async def leases_for(self, pool: str, rid: str) -> list[Lease]:
        return list(self._by_rid.get(pool, {}).get(rid, {}).values())

    async def renew_lease(
        self, pool: str, rid: str, token: str, expires_at: float
    ) -> bool:
        lease = self._leases(pool, rid).get(token)
        if lease is None:
            return False
        lease.expires_at = expires_at
        lease.heartbeat_at = time.time()
        return True

    async def active(self, pool: str) -> list[Lease]:
        out: list[Lease] = []
        for rid_leases in self._by_rid.get(pool, {}).values():
            out.extend(rid_leases.values())
        return out

    async def flush(self, pool: str) -> None:
        self._by_token.pop(pool, None)
        self._by_rid.pop(pool, None)

    async def close(self) -> None:
        return None


class RedisCoordination(Coordination):
    """Redis coordination: per-resource lease hashes plus a token index."""

    def __init__(
        self,
        client: aioredis.Redis | None = None,
        *,
        url: str | None = None,
        key_prefix: str = "purser",
    ) -> None:
        if client is None and url is None:
            raise ConfigError("RedisCoordination requires a client or url")
        # Force RESP2 so the client also works against Redis < 6 servers that
        # do not speak HELLO/RESP3 (redis-py >= 8 negotiates RESP3 by default).
        self._client = client or aioredis.Redis.from_url(
            url,
            protocol=2,
            socket_connect_timeout=5.0,
            socket_timeout=5.0,
        )
        self.key_prefix = key_prefix

    def _lease_key(self, pool: str, rid: str) -> str:
        return f"{self.key_prefix}:{{{pool}}}:lease:{rid}"

    def _ids_key(self, pool: str) -> str:
        return f"{self.key_prefix}:{{{pool}}}:leaseids"

    def _token_key(self, pool: str) -> str:
        return f"{self.key_prefix}:{{{pool}}}:leasetoken"

    @_fail_closed
    async def register_lease(self, pool: str, lease: Lease) -> None:
        key = self._lease_key(pool, lease.resource_id)
        await self._client.hset(key, lease.token, _encode_lease(lease))
        await self._client.hset(self._token_key(pool), lease.token, lease.resource_id)
        await self._client.sadd(self._ids_key(pool), lease.resource_id)
        await self._client.pexpire(key, self._lease_ttl(lease) * 1000)

    @_fail_closed
    async def unregister_lease(self, pool: str, token: str) -> Lease | None:
        # Remove the token and its lease atomically.  Two concurrent removers
        # (release vs force_revoke, or two releases) must not both observe the
        # same lease: the storage slot is decremented only by the one that won.
        raw = await self._client.eval(
            _UNREGISTER_LUA,
            2,
            self._ids_key(pool),
            self._token_key(pool),
            token,
            f"{self.key_prefix}:{{{pool}}}:lease:",
        )
        return _decode_lease(raw.decode()) if raw else None

    @_fail_closed
    async def get_lease(self, pool: str, token: str) -> Lease | None:
        rid = await self._client.hget(self._token_key(pool), token)
        if not rid:
            return None
        raw = await self._client.hget(self._lease_key(pool, rid.decode()), token)
        return _decode_lease(raw.decode()) if raw else None

    @_fail_closed
    async def leases_for(self, pool: str, rid: str) -> list[Lease]:
        items = await self._client.hgetall(self._lease_key(pool, rid))
        return [_decode_lease(raw.decode()) for raw in items.values()]

    @_fail_closed
    async def renew_lease(
        self, pool: str, rid: str, token: str, expires_at: float
    ) -> bool:
        key = self._lease_key(pool, rid)
        raw = await self._client.hget(key, token)
        if not raw:
            return False
        lease = _decode_lease(raw.decode())
        lease.expires_at = expires_at
        lease.heartbeat_at = expires_at
        await self._client.hset(key, token, _encode_lease(lease))
        await self._client.pexpire(
            key, max(1, int(expires_at - time.time()) + 5) * 1000
        )
        return True

    @_fail_closed
    async def active(self, pool: str) -> list[Lease]:
        rids = await self._client.smembers(self._ids_key(pool))
        out: list[Lease] = []
        for rid_raw in rids:
            rid = rid_raw.decode()
            leases = await self.leases_for(pool, rid)
            if not leases:
                # TTL expiry removed the whole hash: stop the ids set from
                # growing forever on churned resources.
                await self._client.srem(self._ids_key(pool), rid)
                continue
            out.extend(leases)
        # TTL expiry also leaves stale token -> rid entries: drop any token
        # whose lease hash no longer contains it.
        stale = [
            token_raw
            for token_raw, rid_raw in (
                await self._client.hgetall(self._token_key(pool))
            ).items()
            if not await self._client.hexists(
                self._lease_key(pool, rid_raw.decode()), token_raw
            )
        ]
        if stale:
            await self._client.hdel(self._token_key(pool), *stale)
        return out

    @_fail_closed
    async def flush(self, pool: str) -> None:
        rids = await self._client.smembers(self._ids_key(pool))
        for rid in rids:
            await self._client.delete(self._lease_key(pool, rid.decode()))
        await self._client.delete(self._ids_key(pool), self._token_key(pool))

    @_fail_closed
    async def close(self) -> None:
        return None
