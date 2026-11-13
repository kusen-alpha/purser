"""Typed payload field metadata, normalization and class validation."""

from __future__ import annotations

import json
import sqlite3
import sys
import types
from dataclasses import dataclass
from typing import ClassVar

import pytest
from typing_extensions import Annotated

from purser import HealthReport, PoolConfig, Purser, ResourceState
from purser.adapters.account import AccountAdapter, AccountResource
from purser.adapters.base import ResourceAdapter
from purser.adapters.proxy import ProxyAdapter, ProxyResource
from purser.core.resource import PayloadField, Resource


def test_declared_fields_drive_typed_names_and_secrets():
    assert not hasattr(ProxyResource, "payload_fields")
    assert not hasattr(AccountResource, "payload_fields")
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
    assert ProxyAdapter.secret_fields == ("password",)
    assert AccountResource.typed_payload_fields == (
        "username",
        "email",
        "password",
        "cookie",
        "access_token",
        "refresh_token",
    )
    assert AccountAdapter.secret_fields == (
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
    typed = ProxyAdapter().hydrate(
        Resource(id="p", kind="proxy", payload={"host": "h", "port": "81"})
    )
    assert isinstance(typed, ProxyResource)
    assert typed.port == 81
    assert typed.protocol == "http"

    sparse = ProxyAdapter().hydrate(
        Resource(id="p", kind="proxy", payload={"port": 81})
    )
    assert isinstance(sparse, ProxyResource)
    assert sparse.host is None


async def test_proxy_generate_keeps_custom_payload():
    resource = await ProxyAdapter().generate(
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


def test_declared_fields_merge_across_inheritance():
    @dataclass
    class MyProxy(ProxyResource):
        isp: Annotated[str | None, PayloadField()] = None

    assert MyProxy.typed_payload_fields == (
        *ProxyResource.typed_payload_fields,
        "isp",
    )
    resource = MyProxy(
        id="m", payload={"host": "h", "port": "8080", "isp": "CT", "note": "x"}
    )
    assert (resource.host, resource.port, resource.isp) == ("h", 8080, "CT")
    assert resource.extra == {"note": "x"}


def test_declared_fields_merge_multiple_bases():
    @dataclass
    class Left(Resource):
        kind: str = "left"
        left: Annotated[str | None, PayloadField()] = None

    @dataclass
    class Right(Resource):
        kind: str = "right"
        right: Annotated[str | None, PayloadField()] = None

    @dataclass
    class Both(Left, Right):
        both: Annotated[str | None, PayloadField()] = None

    assert set(Both.typed_payload_fields) == {"left", "right", "both"}
    resource = Both(id="b", payload={"left": "l", "right": "r", "both": "b"})
    assert (resource.left, resource.right, resource.both) == ("l", "r", "b")
    assert resource.extra == {}


def test_payload_default_is_copied_per_instance():
    @dataclass
    class ListResource(Resource):
        kind: str = "list"
        items: Annotated[list | None, PayloadField(default=[])] = None

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
            flag: Annotated[ClassVar[str], PayloadField()] = "x"


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
        count: Annotated[int | None, PayloadField(required=True, coerce=int)] = None

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


def test_custom_type_derives_secrets_and_hydrates():
    @dataclass
    class TokenResource(Resource):
        kind: str = "token"
        token: Annotated[str | None, PayloadField(secret=True)] = None

    class TokenAdapter(ResourceAdapter):
        kind = "token"
        resource_class = TokenResource

        async def generate(self, spec):
            raise NotImplementedError

        async def validate(self, resource):
            raise NotImplementedError

    assert TokenAdapter.secret_fields == ("token",)
    typed = TokenAdapter().hydrate(
        Resource(id="t", kind="token", payload={"token": "abc"})
    )
    assert isinstance(typed, TokenResource)
    assert typed.token == "abc"


async def test_default_generate_maps_spec_one_to_one():
    @dataclass
    class CardResource(Resource):
        kind: str = "card"
        number: Annotated[str | None, PayloadField(required=True, coerce=str)] = None
        cvv: Annotated[str | None, PayloadField(secret=True)] = None
        attempts: Annotated[int | None, PayloadField(coerce=int, default=0)] = None

    class CardAdapter(ResourceAdapter):
        kind = "card"
        resource_class = CardResource

        async def validate(self, resource):
            return HealthReport.ok()

    adapter = CardAdapter()
    made = await adapter.generate(
        {
            "id": "c1",
            "number": "4111",
            "cvv": "123",
            "attempts": "3",
            "note": "extra",
            "tags": ["vip", "us"],
        }
    )
    assert isinstance(made, CardResource)
    assert made.id == "c1"
    assert made.kind == "card"
    assert made.number == "4111"
    assert made.cvv == "123"
    assert made.attempts == 3
    assert made.extra == {"note": "extra"}
    assert made.tags == frozenset({"vip", "us"})

    with pytest.raises(ValueError, match="number"):
        await adapter.generate({"id": "c2", "cvv": "000"})


async def test_default_generate_derives_stable_id():
    @dataclass
    class CardResource(Resource):
        kind: str = "card"
        number: Annotated[str | None, PayloadField(required=True)] = None
        cvv: Annotated[str | None, PayloadField()] = None

    class CardAdapter(ResourceAdapter):
        kind = "card"
        resource_class = CardResource

        async def validate(self, resource):
            return HealthReport.ok()

    adapter = CardAdapter()
    a = await adapter.generate({"number": "4111", "cvv": "123"})
    b = await adapter.generate({"number": "4111", "cvv": "123"})
    c = await adapter.generate({"number": "4111", "cvv": "456"})
    assert a.id == b.id
    assert a.id.startswith("card-")
    assert a.id != c.id


async def test_default_generate_imports_through_pool():
    @dataclass
    class CardResource(Resource):
        kind: str = "card"
        number: Annotated[str | None, PayloadField(required=True)] = None
        attempts: Annotated[int | None, PayloadField(coerce=int)] = None

    class CardAdapter(ResourceAdapter):
        kind = "card"
        resource_class = CardResource

        async def validate(self, resource):
            return HealthReport.ok()

    ps = Purser(maintenance_enabled=False)
    try:
        ps.register_adapter(CardAdapter())
        pool = ps.create_pool("cards", "card")
        report = await pool.import_resources(
            [{"id": "c1", "number": "4111", "attempts": "2"}],
            import_validate="on_import",
        )
        assert report.imported == 1
        loaded = await pool.get("c1")
        assert isinstance(loaded, CardResource)
        assert loaded.number == "4111"
        assert loaded.attempts == 2
    finally:
        await ps.close()


def test_annotated_fields_declare_schema():
    @dataclass
    class AnnotatedCard(Resource):
        kind: str = "card"
        number: Annotated[str | None, PayloadField(required=True, coerce=str)] = None
        cvv: Annotated[str | None, PayloadField(secret=True)] = None
        attempts: Annotated[int | None, PayloadField(coerce=int, default=0)] = None
        plain: str | None = None

    class AnnotatedCardAdapter(ResourceAdapter):
        kind = "card"
        resource_class = AnnotatedCard

        async def validate(self, resource):
            return HealthReport.ok()

    assert AnnotatedCard.typed_payload_fields == ("number", "cvv", "attempts")
    assert "plain" not in AnnotatedCard.typed_payload_fields
    assert AnnotatedCardAdapter.secret_fields == ("cvv",)

    r = AnnotatedCard(
        id="a", number="4111", cvv="123", attempts="3", payload={"plain": "x"}
    )
    assert r.number == "4111"
    assert r.cvv == "123"
    assert r.attempts == 3
    assert r.extra == {"plain": "x"}
    assert r.to_dict()["payload"] == {
        "number": "4111",
        "cvv": "123",
        "attempts": 3,
        "plain": "x",
    }

    with pytest.raises(ValueError, match="number"):
        AnnotatedCard(id="b", cvv="1").normalize_payload(require=True)


def test_annotated_fields_with_string_annotations():
    mod = types.ModuleType("_annotated_string_mod")
    mod.Annotated = Annotated
    mod.PayloadField = PayloadField
    mod.Resource = Resource
    mod.dataclass = dataclass
    sys.modules["_annotated_string_mod"] = mod
    try:
        ns = {"__name__": "_annotated_string_mod"}
        exec(
            "from __future__ import annotations\n"
            "from dataclasses import dataclass\n"
            "from typing_extensions import Annotated\n"
            "from purser.core.resource import Resource, PayloadField\n"
            "@dataclass\n"
            "class SmsCard(Resource):\n"
            "    kind: str = 'sms'\n"
            "    phone: Annotated[str | None, PayloadField(required=True, coerce=str)] = None\n"
            "    retries: Annotated[int | None, PayloadField(coerce=int)] = None\n",
            ns,
        )
        cls = ns["SmsCard"]
        assert cls.typed_payload_fields == ("phone", "retries")
        made = cls(id="s1", phone="+86138", retries="2")
        assert made.phone == "+86138"
        assert made.retries == 2
    finally:
        sys.modules.pop("_annotated_string_mod", None)


def test_annotated_fields_merge_across_bases():
    @dataclass
    class BaseCard(Resource):
        kind: str = "card"
        pan: Annotated[str | None, PayloadField(required=True, coerce=str)] = None

    @dataclass
    class VipCard(BaseCard):
        note: Annotated[str | None, PayloadField(doc="vip note")] = None

    assert BaseCard.typed_payload_fields == ("pan",)
    assert VipCard.typed_payload_fields == ("pan", "note")


async def test_annotated_fields_end_to_end_import(tmp_path):
    @dataclass
    class TokenCard(Resource):
        kind: str = "tcard"
        token: Annotated[str | None, PayloadField(required=True, secret=True)] = None
        note: str | None = None

    class TokenCardAdapter(ResourceAdapter):
        kind = "tcard"
        resource_class = TokenCard

        async def validate(self, resource):
            return HealthReport.ok()

    db = str(tmp_path / "cards.db")
    ps = Purser(
        maintenance_enabled=False,
        storage="sqlite",
        coordination="memory",
        sqlite_path=db,
    )
    try:
        ps.register_adapter(TokenCardAdapter())
        pool = ps.create_pool("cards", "tcard")
        report = await pool.import_resources(
            [{"id": "t1", "token": "hunter2", "note": "extra"}],
            import_validate="on_import",
        )
        assert report.imported == 1
        loaded = await pool.get("t1")
        assert isinstance(loaded, TokenCard)
        assert loaded.token == "hunter2"
        assert loaded.extra == {"note": "extra"}

        conn = sqlite3.connect(db)
        try:
            blob = conn.execute(
                "SELECT payload FROM resources WHERE pool = 'cards' AND id = 't1'"
            ).fetchone()[0]
        finally:
            conn.close()
        assert "hunter2" not in blob
        assert "token" in json.loads(blob)["_enc"]
    finally:
        await ps.close()


async def test_validate_defaults_to_ok():
    adapter = ResourceAdapter()
    report = await adapter.validate(Resource(id="r", kind="x"))
    assert report.alive is True
    assert report.latency_ms == 1.0


async def test_minimal_type_without_hooks_works_end_to_end():
    class BareAdapter(ResourceAdapter):
        kind = "bare"

    ps = Purser(maintenance_enabled=False)
    try:
        ps.register_adapter(BareAdapter())
        pool = ps.create_pool("bare", "bare")
        report = await pool.import_resources(
            [{"id": "b1", "host": "1.2.3.4"}],
            import_validate="on_import",
        )
        assert report.imported == 1
        await ps.start()
        async with pool.lease(consumer="c") as lease:
            assert lease.resource_id == "b1"
        counts = await pool.counts()
        assert counts[ResourceState.IDLE] == 1
    finally:
        await ps.close()


def test_adapter_fields_public_contract():
    fields = {spec.name: spec for spec in ProxyAdapter().fields()}
    assert fields["host"].required is True
    assert fields["port"].coerce is int
    assert fields["port"].required is True
    assert fields["password"].secret is True
    assert set(fields) >= {"host", "port", "protocol", "username", "password"}


def test_adapter_defaults_public_contract():
    account = AccountAdapter().defaults()
    assert account.max_slots == 4
    assert set(account.secret_fields) == {
        "password",
        "cookie",
        "access_token",
        "refresh_token",
    }
    proxy = ProxyAdapter().defaults()
    assert proxy.max_slots == 1
    assert proxy.secret_fields == ("password",)


def test_pool_config_default_for_consumes_adapter():
    cfg = PoolConfig.default_for("account", adapter=AccountAdapter())
    assert cfg.max_slots == 4
    assert set(cfg.secret_fields) == {
        "password",
        "cookie",
        "access_token",
        "refresh_token",
    }
    overridden = PoolConfig.default_for(
        "account", adapter=AccountAdapter(), max_slots=8
    )
    assert overridden.max_slots == 8
    bare = PoolConfig.default_for("account")
    assert bare.max_slots is None
    assert bare.secret_fields == ()
