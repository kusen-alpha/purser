"""Importable factory specification for registering adapters remotely.

The local scheduler speaks JSON, so an adapter instance created by the caller
cannot be sent over IPC.  :class:`AdapterSpec` names an importable class (or
factory function) plus JSON-serializable constructor arguments; the scheduler
process imports it and calls ``register_adapter`` on the kernel it owns.
"""

from __future__ import annotations

import importlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from purser.errors import ConfigError

__all__ = ["AdapterSpec", "load_adapter"]


def _object_path(target: type) -> str:
    module = getattr(target, "__module__", "")
    qualname = getattr(target, "__qualname__", "")
    if not module or not qualname:
        raise ConfigError("adapter class must have a module and qualname")
    if "<locals>" in qualname:
        raise ConfigError(
            "adapter class must be importable; classes defined inside a function "
            "cannot be registered with a scheduler process"
        )
    return f"{module}:{qualname}"


@dataclass(frozen=True, slots=True)
class AdapterSpec:
    """Serializable description of an adapter class or factory."""

    path: str
    kwargs: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def of(cls, target: type, **kwargs: Any) -> AdapterSpec:
        if not isinstance(target, type):
            raise ConfigError("AdapterSpec.of expects an adapter class")
        try:
            json.dumps(kwargs)
        except (TypeError, ValueError) as exc:
            raise ConfigError(
                "AdapterSpec kwargs must be JSON-serializable; pass importable "
                "values only"
            ) from exc
        return cls(_object_path(target), dict(kwargs))

    def to_dict(self) -> dict[str, Any]:
        return {"path": self.path, "kwargs": dict(self.kwargs)}


def _split_path(path: str) -> tuple[str, str]:
    module_name, sep, qualname = path.partition(":")
    if not sep or not module_name or not qualname:
        raise ConfigError(f"adapter path must be 'module:qualname'; got {path!r}")
    if "<locals>" in qualname:
        raise ConfigError(
            f"adapter path {path!r} is not importable; move the class to module scope"
        )
    return module_name, qualname


def load_adapter(spec: AdapterSpec | Mapping[str, Any]) -> Any:
    """Import and instantiate the adapter described by ``spec``."""

    if isinstance(spec, AdapterSpec):
        path, kwargs = spec.path, dict(spec.kwargs)
    elif isinstance(spec, Mapping):
        raw_path = spec.get("path")
        if not isinstance(raw_path, str) or not raw_path:
            raise ConfigError("adapter spec requires a 'path' string")
        raw_kwargs = spec.get("kwargs")
        if raw_kwargs is None:
            raw_kwargs = {}
        if not isinstance(raw_kwargs, Mapping):
            raise ConfigError("adapter spec 'kwargs' must be an object")
        path, kwargs = raw_path, dict(raw_kwargs)
    else:
        raise ConfigError("adapter spec must be an AdapterSpec or mapping")

    module_name, qualname = _split_path(path)
    target: Any = importlib.import_module(module_name)
    for part in qualname.split("."):
        target = getattr(target, part)
    adapter = target(**kwargs)
    if not getattr(adapter, "kind", None):
        raise ConfigError(f"adapter from {path!r} does not define a 'kind'")
    return adapter
