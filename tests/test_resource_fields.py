"""Typed payload field metadata, normalization and class validation."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import ClassVar

import pytest

from purser import Purser
from purser.core.resource import PayloadField, Resource
from purser.types.account import AccountResource, AccountType
from purser.types.base import ResourceType
from purser.types.proxy import ProxyResource, ProxyType


def test_payload_fields_drive_typed_names_and_secrets():
    assert ProxyResource.typed_payload_fields == (
        "host",
        "port",
        "protocol",
        "username",
        "password",
        "country",
        "province",
        "city",
        "district",
    )
    assert ProxyType.secret_fields == ("password",)
    assert AccountResource.typed_payload_fields == (
        "username",
        "email",
        "password",
        "cookie",
        "access_token",
        "refresh_token",
    )
    assert AccountType.secret_fields == (
        "password",
        "cookie",
        "access_token",
        "refresh_token",
    )


def test_constructor_coerces_declared_fields():
    resource = ProxyResource(
        id="p", payload={"host": "h", "port": "8080", "custom": "kept"}
    )
    assert resource.port == 8080
    assert resource.payload == {"host": "h", "port": 8080, "custom": "kept"}
    assert resource.extra == {"custom": "kept"}
    assert resource.protocol is None

    direct = ProxyResource(id="d", host="h", port="8080")
    assert direct.port == 8080
    copied = direct.copy_with(port="3128")
    assert copied.port == 3128


def test_normalize_payload_fills_defaults_and_checks_required():
    resource = ProxyResource(id="p", payload={"host": "h", "port": "1080"})
    resource.normalize_payload(require=True)
    assert resource.port == 1080
    assert resource.protocol == "http"

    with pytest.raises(ValueError, match="host"):
        ProxyResource(id="p").normalize_payload(require=True)
    with pytest.raises(ValueError, match="invalid 'port'"):
        ProxyResource(id="p", payload={"host": "h", "port": "not-a-port"})


def test_hydrate_normalizes_without_requiring_fields():
    typed = ProxyType().hydrate(
        Resource(id="p", kind="proxy", payload={"host": "h", "port": "81"})
    )
    assert isinstance(typed, ProxyResource)
    assert typed.port == 81
    assert typed.protocol == "http"

    sparse = ProxyType().hydrate(
        Resource(id="p", kind="proxy", payload={"port": 81})
    )
    assert isinstance(sparse, ProxyResource)
    assert sparse.host is None


async def test_proxy_generate_keeps_custom_payload():
    resource = await ProxyType().generate(
        {
            "id": "p-custom",
            "host": "1.2.3.4",
            "port": 8080,
            "country": "US",
            "isp": "CT",
            "note": "x",
        }
    )
    assert isinstance(resource, ProxyResource)
    assert resource.payload == {
        "host": "1.2.3.4",
        "port": 8080,
        "protocol": "http",
        "country": "US",
        "isp": "CT",
        "note": "x",
    }
    assert resource.extra == {"isp": "CT", "note": "x"}


async def test_import_enforces_required_fields(purser):
    pool = purser.create_pool("proxy-required", "proxy")
    report = await pool.import_resources(
        [Resource(id="bad", kind="proxy", payload={"host": "1.1.1.1"})]
    )
    assert report.imported == 0
    assert report.errors and "port" in report.errors[0]["error"]


def test_payload_fields_merge_across_inheritance():
    @dataclass
    class MyProxy(ProxyResource):
        payload_fields = (PayloadField("isp"),)
        isp: str | None = None

    assert MyProxy.typed_payload_fields == (
        *ProxyResource.typed_payload_fields,
        "isp",
    )
    resource = MyProxy(
        id="m", payload={"host": "h", "port": "8080", "isp": "CT", "note": "x"}
    )
    assert (resource.host, resource.port, resource.isp) == ("h", 8080, "CT")
    assert resource.extra == {"note": "x"}


def test_payload_fields_merge_multiple_bases():
    @dataclass
    class Left(Resource):
        kind: str = "left"
        payload_fields = (PayloadField("left"),)
        left: str | None = None

    @dataclass
    class Right(Resource):
        kind: str = "right"
        payload_fields = (PayloadField("right"),)
        right: str | None = None

    @dataclass
    class Both(Left, Right):
        payload_fields = (PayloadField("both"),)
        both: str | None = None

    assert set(Both.typed_payload_fields) == {"left", "right", "both"}
    resource = Both(id="b", payload={"left": "l", "right": "r", "both": "b"})
    assert (resource.left, resource.right, resource.both) == ("l", "r", "b")
    assert resource.extra == {}


def test_payload_default_is_copied_per_instance():
    @dataclass
    class ListResource(Resource):
        kind: str = "list"
        payload_fields = (PayloadField("items", default=[]),)
        items: list | None = None

    first, second = ListResource(id="a"), ListResource(id="b")
    first.normalize_payload()
    second.normalize_payload()
    assert first.items is not second.items
    first.items.append("x")
    assert second.items == []


def test_payload_field_cannot_be_classvar():
    with pytest.raises(TypeError, match="flag"):
        class Bad(Resource):
            kind: str = "bad"
            payload_fields = (PayloadField("flag"),)
            flag: ClassVar[str] = "x"


def test_extra_is_a_deep_copy():
    resource = ProxyResource(
        id="p", payload={"host": "h", "port": 1, "nested": {"k": "v"}}
    )
    extra = resource.extra
    extra["nested"]["k"] = "changed"
    assert resource.extra["nested"]["k"] == "v"


def test_payload_is_a_detached_view():
    resource = ProxyResource(
        id="p", payload={"host": "h", "port": 1, "nested": {"k": "v"}}
    )
    view = resource.payload
    view["nested"]["k"] = "changed"
    view["port"] = 2
    assert resource.payload == {
        "host": "h",
        "port": 1,
        "nested": {"k": "v"},
    }


def test_copy_with_replaces_payload_and_typed_fields():
    resource = ProxyResource(
        id="p", payload={"host": "h", "port": 8080, "note": "old"}
    )

    changed = resource.copy_with(port=3128)
    assert changed.port == 3128
    assert changed.extra == {"note": "old"}

    replaced = resource.copy_with(
        payload={"host": "h2", "port": "81", "note": "new"}
    )
    assert replaced.host == "h2"
    assert replaced.port == 81
    assert replaced.protocol is None
    assert replaced.extra == {"note": "new"}


def test_to_dict_folds_typed_fields_into_payload():
    resource = ProxyResource(
        id="p", payload={"host": "h", "port": "8080", "note": "x"}
    )
    data = resource.to_dict()
    assert data["id"] == "p"
    assert data["state"] == "new"
    assert data["tags"] == []
    assert data["payload"] == {"host": "h", "port": 8080, "note": "x"}
    assert "host" not in data
    assert "_payload" not in data


def test_required_rejects_empty_but_accepts_zero():
    @dataclass
    class CounterResource(Resource):
        kind: str = "counter"
        payload_fields = (PayloadField("count", required=True, coerce=int),)
        count: int | None = None

    present = CounterResource(id="c", payload={"count": 0})
    present.normalize_payload(require=True)
    assert present.count == 0

    with pytest.raises(ValueError, match="count"):
        CounterResource(id="c").normalize_payload(require=True)
    with pytest.raises(ValueError, match="host"):
        ProxyResource(
            id="p", payload={"host": "", "port": 8080}
        ).normalize_payload(require=True)


async def test_proxy_password_is_encrypted_at_rest(tmp_path):
    db = str(tmp_path / "proxy.db")
    ps = Purser(
        maintenance_enabled=False,
        storage="sqlite",
        coordination="memory",
        sqlite_path=db,
    )
    try:
        pool = ps.create_pool("proxies", "proxy")
        await pool.import_resources(
            [{"id": "p-1", "host": "h", "port": 8080, "password": "hunter2"}]
        )
        conn = sqlite3.connect(db)
        try:
            blob = conn.execute(
                "SELECT payload FROM resources WHERE pool = 'proxies' AND id = 'p-1'"
            ).fetchone()[0]
        finally:
            conn.close()
        assert "hunter2" not in blob
        assert "password" in json.loads(blob)["_enc"]
        loaded = await pool.get("p-1")
        assert loaded is not None
        assert loaded.password == "hunter2"
    finally:
        await ps.close()


def test_payload_fields_must_name_dataclass_fields():
    with pytest.raises(TypeError, match="not dataclass fields"):
        class Broken(Resource):
            kind: str = "broken"
            payload_fields = (PayloadField("missing"),)

    with pytest.raises(TypeError, match="duplicate"):
        class Duplicated(Resource):
            kind: str = "duplicated"
            payload_fields = (PayloadField("value"), PayloadField("value"))
            value: str | None = None


def test_custom_type_derives_secrets_and_hydrates():
    @dataclass
    class TokenResource(Resource):
        kind: str = "token"
        payload_fields = (PayloadField("token", secret=True),)
        token: str | None = None

    class TokenType(ResourceType):
        kind = "token"
        resource_class = TokenResource

        async def generate(self, spec):
            raise NotImplementedError

        async def validate(self, resource):
            raise NotImplementedError

    assert TokenType.secret_fields == ("token",)
    typed = TokenType().hydrate(
        Resource(id="t", kind="token", payload={"token": "abc"})
    )
    assert isinstance(typed, TokenResource)
    assert typed.token == "abc"
