"""Freeze the public export surface so accidental removals fail loudly.

When an export is intentionally added or removed, update ``EXPECTED_EXPORTS_SHA256``
(and ``EXPECTED_EXPORTS_COUNT``) via::

    python -c "import hashlib, purser; \
print(len(purser.__all__)); \
print(hashlib.sha256('\n'.join(sorted(purser.__all__)).encode()).hexdigest())"
"""

from __future__ import annotations

import hashlib
import inspect
import types

import purser

EXPECTED_EXPORTS_COUNT = 104
EXPECTED_EXPORTS_SHA256 = (
    "a50e7374114e947387b2fd3274fd6a6c6f8553e15121628c5c1d850704900acd"
)


def _surface_digest() -> str:
    joined = "\n".join(sorted(purser.__all__))
    return hashlib.sha256(joined.encode()).hexdigest()


def test_export_surface_is_frozen():
    assert len(purser.__all__) == EXPECTED_EXPORTS_COUNT
    assert _surface_digest() == EXPECTED_EXPORTS_SHA256


def test_every_export_resolves_to_a_non_module_object():
    missing = [name for name in purser.__all__ if not hasattr(purser, name)]
    assert not missing, missing
    modules = [name for name in purser.__all__ if isinstance(getattr(purser, name), types.ModuleType)]
    assert not modules, modules


def test_no_duplicate_exports():
    assert len(set(purser.__all__)) == len(purser.__all__)


def test_facade_entry_points_remain_callables():
    for name in ("Purser", "ResourcePool", "AcquireRequest"):
        assert inspect.isclass(getattr(purser, name)), name
