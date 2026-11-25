"""Allocator name registry shared by config validation and the plugin API.

Kept dependency-free so ``config`` / ``config_schema`` can import it without
pulling in ``core.allocator`` (which imports ``config`` for ``Weights``).
"""

from __future__ import annotations

DEFAULT_ALLOCATOR = "health_weighted"

ALLOCATOR_NAMES: set[str] = {
    "health_weighted",
    "round_robin",
    "least_loaded",
    "static_priority",
    "random",
}


def register_allocator_name(name: str) -> None:
    """Let ``PoolConfig(allocator=...)`` accept a custom allocator name."""

    ALLOCATOR_NAMES.add(name)
