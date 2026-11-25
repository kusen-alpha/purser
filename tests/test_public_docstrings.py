"""Guards against internal ticket IDs and undocumented pool-facade methods."""

from __future__ import annotations

import ast
import inspect
import pathlib
import re
from typing import Any

import pytest

import purser
from purser import Purser, ResourcePool
from purser.remote import RemotePool
from purser.sync_pool import SyncPool
from purser.thread import ThreadPool

_TICKET = re.compile(
    r"\((?:[A-Z]{1,3}\d+|\d+\.\d+)(?:\s*/\s*[A-Z]{0,3}\d+)*\)"
)

_FACADES = (ResourcePool, SyncPool, ThreadPool, RemotePool)

_POOL_METHODS = (
    "name",
    "kind",
    "profile",
    "acquire",
    "acquire_many",
    "release",
    "renew",
    "lease",
    "stats",
    "import_resources",
    "generate",
    "query",
    "counts",
    "get",
    "require",
    "count_matching",
    "batches",
    "leases",
    "describe",
    "ban",
    "unban",
)


def _public_docstrings(cls: type) -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []
    for name, member in inspect.getmembers(cls):
        if name.startswith("_"):
            continue
        obj: Any = member.fget if isinstance(member, property) else member
        doc = inspect.getdoc(obj) if obj is not None else None
        if doc:
            found.append((name, doc))
    return found


def _docstring_nodes(tree: ast.AST):
    for node in ast.walk(tree):
        if not isinstance(
            node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
        ):
            continue
        doc = ast.get_docstring(node, clean=False)
        if doc:
            name = getattr(node, "name", "<module>")
            yield name, doc


@pytest.mark.parametrize("cls", [Purser, ResourcePool])
def test_public_docstrings_have_no_ticket_ids(cls: type) -> None:
    offenders = [
        f"{cls.__name__}.{name}: {_TICKET.search(doc).group(0)}"  # type: ignore[union-attr]
        for name, doc in _public_docstrings(cls)
        if _TICKET.search(doc)
    ]
    assert not offenders, offenders


@pytest.mark.parametrize("cls", _FACADES)
def test_pool_facade_methods_are_documented(cls: type) -> None:
    missing = [
        f"{cls.__name__}.{name}"
        for name in _POOL_METHODS
        if hasattr(cls, name) and not inspect.getdoc(getattr(cls, name))
    ]
    assert not missing, missing


def test_create_pool_example_is_synchronous() -> None:
    doc = inspect.getdoc(Purser.create_pool) or ""
    assert "await ps.create_pool" not in doc
    assert "ps.create_pool(" in doc


def test_no_ticket_ids_in_package_docstrings() -> None:
    package = pathlib.Path(purser.__file__).parent
    offenders: list[str] = []
    for path in package.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for name, doc in _docstring_nodes(tree):
            match = _TICKET.search(doc)
            if match:
                offenders.append(
                    f"{path.relative_to(package)}:{name}: {match.group(0)}"
                )
    assert not offenders, offenders
