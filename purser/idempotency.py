"""Write idempotency store (D1): return the first result for repeated keys."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any


@dataclass
class IdempotencyStore:
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
