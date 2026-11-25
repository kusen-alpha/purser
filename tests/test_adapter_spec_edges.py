"""AdapterSpec validation and ``load_adapter`` contract edges.

The spec is what crosses process boundaries, so every rejection path returns a
``ConfigError`` instead of leaking ``TypeError``/``ImportError``/``AttributeError``
to the caller.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from purser import AdapterSpec, load_adapter
from purser.adapters.base import ResourceAdapter
from purser.core.health import HealthReport
from purser.core.resource import Resource
from purser.errors import ConfigError


class SpecAdapter(ResourceAdapter):
    """Importable adapter with an optional constructor argument."""

    kind = "specedge"

    def __init__(self, prefix: str = "p") -> None:
        self.prefix = prefix

    async def generate(self, spec: dict) -> Resource:
        return Resource(id=spec.get("id") or f"{self.prefix}-1", kind=self.kind)

    async def validate(self, resource: Resource) -> HealthReport:
        return HealthReport.ok()


class KindlessAdapter:
    """Importable callable without a ``kind`` attribute."""

    def __init__(self) -> None:
        self.marker = True


class Outer:
    """Container for the nested-qualname path."""

    class Inner(SpecAdapter):
        kind = "specinner"


class _NoNameMeta(type):
    def __getattribute__(cls, name: str):
        if name in ("__module__", "__qualname__"):
            raise AttributeError(name)
        return super().__getattribute__(name)


class _Nameless(metaclass=_NoNameMeta):
    pass


def test_of_rejects_non_class_targets():
    with pytest.raises(ConfigError, match="adapter class"):
        AdapterSpec.of(SpecAdapter())  # type: ignore[arg-type]
    with pytest.raises(ConfigError):
        AdapterSpec.of("text")  # type: ignore[arg-type]


def test_of_rejects_class_without_module_or_qualname():
    with pytest.raises(ConfigError, match="module and qualname"):
        AdapterSpec.of(_Nameless)


def test_of_keeps_json_kwargs_and_is_frozen():
    spec = AdapterSpec.of(SpecAdapter, prefix="x")
    assert spec.kwargs == {"prefix": "x"}
    assert spec.to_dict() == {"path": spec.path, "kwargs": {"prefix": "x"}}
    with pytest.raises(FrozenInstanceError):
        spec.path = "other:Path"  # type: ignore[misc]


@pytest.mark.parametrize("path", ["noseparator", ":qualname", "module:"])
def test_split_path_rejects_malformed_paths(path: str):
    with pytest.raises(ConfigError, match="module:qualname"):
        load_adapter({"path": path})


def test_split_path_rejects_function_local_path():
    with pytest.raises(ConfigError, match="not importable"):
        load_adapter({"path": "tests.module:build.<locals>.Adapter"})


def test_load_adapter_requires_a_path_string():
    with pytest.raises(ConfigError, match="'path' string"):
        load_adapter({})
    with pytest.raises(ConfigError, match="'path' string"):
        load_adapter({"path": ""})
    with pytest.raises(ConfigError, match="'path' string"):
        load_adapter({"path": 3})


def test_load_adapter_rejects_non_mapping_kwargs():
    with pytest.raises(ConfigError, match="'kwargs' must be an object"):
        load_adapter({"path": "test_adapter_spec_edges:SpecAdapter", "kwargs": [1]})


@pytest.mark.parametrize("kwargs", [[], "", 0, False])
def test_load_adapter_rejects_falsy_non_mapping_kwargs(kwargs):
    with pytest.raises(ConfigError, match="'kwargs' must be an object"):
        load_adapter({"path": "test_adapter_spec_edges:SpecAdapter", "kwargs": kwargs})


def test_load_adapter_treats_null_kwargs_as_empty():
    adapter = load_adapter(
        {"path": "test_adapter_spec_edges:SpecAdapter", "kwargs": None}
    )
    assert adapter.prefix == "p"


def test_load_adapter_rejects_unsupported_spec_type():
    with pytest.raises(ConfigError, match="AdapterSpec or mapping"):
        load_adapter(object())  # type: ignore[arg-type]


def test_load_adapter_builds_adapter_from_spec_object():
    spec = AdapterSpec.of(SpecAdapter, prefix="from-spec")
    adapter = load_adapter(spec)
    assert isinstance(adapter, SpecAdapter)
    assert adapter.prefix == "from-spec"


def test_load_adapter_builds_adapter_from_mapping_and_nested_qualname():
    adapter = load_adapter(
        {
            "path": "test_adapter_spec_edges:Outer.Inner",
            "kwargs": {"prefix": "nested"},
        }
    )
    assert isinstance(adapter, Outer.Inner)
    assert adapter.prefix == "nested"


def test_load_adapter_requires_kind():
    with pytest.raises(ConfigError, match="does not define a 'kind'"):
        load_adapter({"path": "test_adapter_spec_edges:KindlessAdapter"})


def test_load_adapter_raises_attribute_error_for_unknown_attribute():
    # Documented behaviour: a bad qualname surfaces as AttributeError so the
    # caller can tell a typo apart from a malformed spec.
    with pytest.raises(AttributeError):
        load_adapter({"path": "test_adapter_spec_edges:Missing"})
