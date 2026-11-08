"""Write idempotency store (D1): return the first result for repeated keys."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any


@dataclass
class IdempotencyStore:
    """Process-local store.  Use :class:`RedisIdempotencyStore` when several
    API instances share the same clients."""

    ttl: float = 600.0
    _items: dict[str, tuple[float, dict[str, Any]]] = field(default_factory=dict)

    def get(self, key: str) -> dict[str, Any] | None:
        item = self._items.get(key)
        if item is None:
            return None
        expires, result = item
        if expires < time.time():
            self._items.pop(key, None)
            return None
        return result

    def put(self, key: str, result: dict[str, Any]) -> None:
        self._items[key] = (time.time() + self.ttl, result)

    def sweep(self, now: float | None = None) -> int:
        now = now or time.time()
        stale = [k for k, (exp, _) in self._items.items() if exp < now]
        for k in stale:
            self._items.pop(k, None)
        return len(stale)

    async def aget(self, key: str) -> dict[str, Any] | None:
        return self.get(key)

    async def aput(self, key: str, result: dict[str, Any]) -> None:
        self.put(key, result)


class RedisIdempotencyStore:
    """Shared idempotency keys for multi-instance deployments (D1)."""

    def __init__(
        self,
        *,
        url: str | None = None,
        client: Any = None,
        ttl: float = 600.0,
        prefix: str = "purser:idem:",
    ) -> None:
        self.ttl = ttl
        self._url = url
        self._client = client
        self._prefix = prefix

    async def _redis(self) -> Any:
        if self._client is None:
            if not self._url:
                raise RuntimeError("RedisIdempotencyStore needs url or client")
            import redis.asyncio as aioredis

            self._client = aioredis.from_url(self._url, decode_responses=True)
        return self._client

    async def aget(self, key: str) -> dict[str, Any] | None:
        client = await self._redis()
        raw = await client.get(self._prefix + key)
        if raw is None:
            return None
        result: dict[str, Any] = json.loads(raw)
        return result

    async def aput(self, key: str, result: dict[str, Any]) -> None:
        client = await self._redis()
        await client.set(
            self._prefix + key, json.dumps(result), ex=max(1, int(self.ttl))
        )

    async def aclose(self) -> None:
        if self._client is not None:
            close = getattr(self._client, "aclose", None) or getattr(
                self._client, "close", None
            )
            if close is not None:
                await close()
            self._client = None
