"""Wire helpers and failure paths of the local scheduler IPC client.

The scheduler itself is covered by ``test_local_scheduler.py``; here the
transport is replaced by an in-memory stub so malformed messages, timeouts
and connection drops are deterministic and never touch a real process.
"""

from __future__ import annotations

import os

import pytest

from purser import ConfigError, PoolExhausted, Resource, UsageOutcome
from purser.adapters import ProxyResource
from purser.core.resource import (
    AcquireRequest,
    HealthStats,
    Quota,
    SiteFailure,
    Slots,
)
from purser.core.state import ResourceState
from purser.local import (
    LocalSchedulerClient,
    RemoteLease,
    SchedulerProtocolError,
    SchedulerTimeout,
    SchedulerUnavailable,
    client as client_mod,
    close_client,
    get_client,
    init_worker,
    set_default_address,
)
from purser.local.protocol import (
    ENV_VAR,
    SchedulerAddress,
    decode_message,
    default_address,
    encode_message,
)

# -- stub transport -------------------------------------------------------


class StubConn:
    """In-memory ``multiprocessing.connection`` stand-in.

    ``reply`` returns the response payload (encoded here), raw bytes to send
    verbatim, or an exception to raise from ``recv_bytes``.
    """

    def __init__(self, reply) -> None:
        self._reply = reply
        self._pending: bytes | BaseException = b""
        self.messages: list[dict] = []
        self.closed = False

    def send_bytes(self, raw: bytes) -> None:
        message = decode_message(raw)
        self.messages.append(message)
        outgoing = self._reply(message)
        if isinstance(outgoing, BaseException):
            self._pending = outgoing
        elif isinstance(outgoing, bytes):
            self._pending = outgoing
        else:
            self._pending = encode_message(outgoing)

    def poll(self, timeout: float | None = None) -> bool:
        return True

    def recv_bytes(self, maxlength: int | None = None) -> bytes:
        pending, self._pending = self._pending, b""
        if isinstance(pending, BaseException):
            raise pending
        return pending

    def close(self) -> None:
        self.closed = True


class SilentConn(StubConn):
    """Never answers, so ``recv_message`` hits its timeout branch."""

    def poll(self, timeout: float | None = None) -> bool:
        return False


class BrokenSendConn(StubConn):
    def send_bytes(self, raw: bytes) -> None:
        raise OSError("pipe gone")


def _with_conn(conn) -> LocalSchedulerClient:
    client = LocalSchedulerClient(default_address())
    client._local.conn = conn
    client._conns.append(conn)
    return client


def _ok_reply(result):
    return lambda message: {"id": message["id"], "ok": True, "result": result}


def _client_with(result=None, reply=None) -> tuple[LocalSchedulerClient, StubConn]:
    conn = StubConn(reply or _ok_reply({} if result is None else result))
    return _with_conn(conn), conn


# -- conversion helpers ---------------------------------------------------


def test_config_payload_and_dataclass_helpers():
    from purser import PoolConfig

    data = client_mod._config_payload(PoolConfig(kind="proxy"), {"max_slots": 2})
    assert data["kind"] == "proxy" and data["max_slots"] == 2
    assert client_mod._config_payload(None, {"a": 1}) == {"a": 1}
    assert client_mod._config_payload({"b": 2}, {"a": 1}) == {"a": 1, "b": 2}
    with pytest.raises(ConfigError):
        client_mod._config_payload(object(), {})

    built = client_mod._build_dataclass(
        Slots, {"max_slots": 3, "unknown": "x"}
    )
    assert built == Slots(max_slots=3)
    assert client_mod._build_dataclass(Slots, None) is None
    assert client_mod._build_dataclass(Slots, "nope") is None
    assert client_mod._optional_dataclass(Quota, None) is None
    assert client_mod._optional_dataclass(Quota, {"limit": 5}) == Quota(limit=5)


def test_resource_from_dict_rejects_non_mapping_and_unknown_state():
    assert client_mod._resource_from_dict(None) is None
    assert client_mod._resource_from_dict("r1") is None
    resource = client_mod._resource_from_dict(
        {"id": "r1", "kind": "fakecheck", "state": "not-a-state"}
    )
    assert resource.state is ResourceState.NEW


def test_resource_from_dict_restores_every_wire_field():
    resource = client_mod._resource_from_dict(
        {
            "id": "r1",
            "kind": "fakecheck",
            "payload": {"host": "1.2.3.4"},
            "state": ResourceState.IDLE.value,
            "version": "2",
            "tags": ["eu"],
            "slots": {"max_slots": 2},
            "health": {"score": 0.9},
            "site_failures": {
                "site-a": {"site": "site-a", "count": 2},
                "site-b": None,
            },
            "quota": {"limit": 4},
            "usage_quota": 10,
            "used_count": "1",
            "schedule_window": ["08:00", "20:00"],
            "meta": {"note": "x"},
        }
    )
    assert resource.id == "r1" and resource.version == 2
    assert resource.tags == frozenset({"eu"})
    assert resource.slots == Slots(max_slots=2)
    assert resource.health == HealthStats(score=0.9)
    assert set(resource.site_failures) == {"site-a"}
    assert isinstance(resource.site_failures["site-a"], SiteFailure)
    assert resource.quota == Quota(limit=4)
    assert resource.schedule_window == ("08:00", "20:00")

    empty = client_mod._resource_from_dict(
        {"id": "r2", "kind": "fakecheck", "schedule_window": []}
    )
    assert empty.schedule_window is None

    class NeedsPositional(Resource):
        def __init__(self, required):
            raise AssertionError("subclass constructor must not be reached")

    fallback = client_mod._resource_from_dict(
        {"id": "r3", "kind": "fakecheck"}, NeedsPositional
    )
    assert type(fallback) is Resource


def test_request_and_record_payloads_accept_documented_types():
    assert client_mod._request_payload(None) == {}
    assert client_mod._request_payload({"consumer": "c"}) == {"consumer": "c"}
    payload = client_mod._request_payload(
        AcquireRequest(consumer="c", tags={"b", "a"})
    )
    assert payload["tags"] == ["a", "b"] and payload["consumer"] == "c"

    resource = Resource(id="r1", kind="fakecheck")
    assert client_mod._record_payload(resource)["id"] == "r1"
    source = {"id": "r2"}
    copied = client_mod._record_payload(source)
    copied["id"] = "changed"
    assert source["id"] == "r2"


def test_request_and_record_payloads_reject_other_types():
    with pytest.raises(TypeError):
        client_mod._request_payload(42)
    with pytest.raises(TypeError):
        client_mod._record_payload(42)


def test_coerce_address_handles_every_documented_form():
    address = default_address()
    assert client_mod._coerce_address(None) is None
    assert client_mod._coerce_address(address) is address
    assert client_mod._coerce_address(address.to_dict()) == address
    assert client_mod._coerce_address(address.to_env()) == address
    assert client_mod.resolve_address(address.to_dict()) == address
    with pytest.raises(TypeError):
        client_mod._coerce_address(42)


def test_resolve_address_requires_an_address(monkeypatch):
    monkeypatch.delenv(ENV_VAR, raising=False)
    with pytest.raises(ConfigError, match="no scheduler address"):
        client_mod.resolve_address(None)


# -- client construction / registry ---------------------------------------


def test_register_resource_type_validates_input():
    client = LocalSchedulerClient(default_address())
    with pytest.raises(ConfigError):
        client.register_resource_type("", ProxyResource)
    with pytest.raises(ConfigError):
        client.register_resource_type("proxy", 42)
    with pytest.raises(ConfigError):
        client.register_resource_type("proxy", str)
    client.register_resource_type("proxy", ProxyResource)
    assert client._resource_types["proxy"] is ProxyResource


def test_resource_cls_for_prefers_explicit_then_registered_kind():
    client = LocalSchedulerClient(
        default_address(), resource_types={"proxy": ProxyResource}
    )
    assert client._resource_cls_for(None, Resource) is Resource
    lease = {"resource": {"kind": "proxy"}}
    assert client._resource_cls_for(lease, None) is ProxyResource
    assert client._resource_cls_for({"resource": "flat"}, None) is None
    assert client._resource_cls_for(None, None) is None


def test_client_close_is_idempotent_and_blocks_later_requests():
    client = LocalSchedulerClient(default_address())
    client.close()
    client.close()
    with pytest.raises(SchedulerUnavailable):
        client.health()

    with LocalSchedulerClient(default_address()) as entered:
        assert not entered._closed
    assert entered._closed


def test_drop_conn_and_pid_reset_tolerate_stale_connections():
    client = LocalSchedulerClient(default_address())
    client._drop_conn(object())
    client._pid = -1
    client._closed = True
    client._conns.append(object())
    client._ensure_pid()
    assert client._pid == os.getpid()
    assert client._conns == []
    assert client._closed is False


# -- transport failure paths ----------------------------------------------


def test_request_rejects_mismatched_response_id_and_drops_conn():
    conn = StubConn(lambda message: {"id": message["id"] + 1, "ok": True, "result": {}})
    client = _with_conn(conn)
    with pytest.raises(SchedulerProtocolError):
        client.health()
    assert client._local.conn is None and conn not in client._conns
    assert conn.closed


def test_request_rejects_malformed_error_payload():
    conn = StubConn(lambda message: {"id": message["id"], "ok": False, "error": "x"})
    client = _with_conn(conn)
    with pytest.raises(SchedulerProtocolError):
        client.health()


def test_request_rebuilds_typed_remote_errors():
    conn = StubConn(
        lambda message: {
            "id": message["id"],
            "ok": False,
            "error": {"code": "pool_exhausted", "message": "no slots"},
        }
    )
    client = _with_conn(conn)
    with pytest.raises(PoolExhausted):
        client.health()


def test_request_rejects_non_object_result():
    conn = StubConn(lambda message: {"id": message["id"], "ok": True, "result": []})
    client = _with_conn(conn)
    with pytest.raises(SchedulerProtocolError):
        client.health()


def test_recv_timeout_raises_scheduler_timeout_and_drops_conn():
    conn = SilentConn(_ok_reply({}))
    client = _with_conn(conn)
    with pytest.raises(SchedulerTimeout):
        client.health()
    assert client._local.conn is None


def test_recv_eof_surfaces_as_unavailable():
    conn = StubConn(lambda message: EOFError("peer closed"))
    client = _with_conn(conn)
    with pytest.raises(SchedulerUnavailable):
        client.health()
    assert client._local.conn is None


def test_send_failure_surfaces_as_unavailable():
    client = _with_conn(BrokenSendConn(_ok_reply({})))
    with pytest.raises(SchedulerUnavailable):
        client.health()


def test_invalid_json_response_is_a_protocol_error():
    conn = StubConn(lambda message: b"not json")
    client = _with_conn(conn)
    with pytest.raises(SchedulerProtocolError):
        client.pool_names()


def test_result_accessors_validate_wire_shape():
    client, _ = _client_with({"pools": "p"})
    with pytest.raises(SchedulerProtocolError):
        client.pool_names()

    client, _ = _client_with({"pool": "not-a-dict"})
    with pytest.raises(SchedulerProtocolError):
        client.create_pool("p", "fakecheck")

    client, _ = _client_with({"config": []})
    with pytest.raises(SchedulerProtocolError):
        client.update_pool("p", max_slots=1)

    client, _ = _client_with({"stats": ["x"]})
    with pytest.raises(SchedulerProtocolError):
        client.stats("p")

    client, _ = _client_with({"lease": "token"})
    with pytest.raises(SchedulerProtocolError):
        client.acquire("p")


def test_release_rejects_unknown_outcome_before_ipc():
    client, conn = _client_with({"ok": True})
    with pytest.raises(ConfigError):
        client.release("p", "token", outcome="not-real")
    assert conn.messages == []


def test_release_and_renew_forward_wire_fields():
    client, conn = _client_with({"ok": True, "expires_at": 123.5})
    assert client.release("p", "token", outcome="captcha", used=2) is True
    assert conn.messages[-1]["outcome"] == "captcha"
    assert conn.messages[-1]["used"] == 2

    assert client.renew("p", "token", used=3) == pytest.approx(123.5)
    assert conn.messages[-1]["used"] == 3
    # One cached connection serves every request.
    assert client._conns == [conn] and len(conn.messages) == 2


def test_shutdown_sends_a_request_until_the_client_closes():
    client, conn = _client_with({})
    client.shutdown()
    assert conn.messages[-1]["op"] == "shutdown"
    client.close()
    client.shutdown()
    assert len(conn.messages) == 1


def test_shutdown_is_idempotent_even_before_close():
    client, conn = _client_with({})
    client.shutdown()
    client.shutdown()
    assert [m["op"] for m in conn.messages] == ["shutdown"]


def test_shutdown_retries_when_the_request_never_reached_the_server():
    client = _with_conn(BrokenSendConn(_ok_reply({})))
    with pytest.raises(SchedulerUnavailable):
        client.shutdown()
    assert client._shutdown_sent is False


def test_connection_failure_is_reported_as_unavailable():
    base = default_address()
    missing = SchedulerAddress(
        family=base.family, address=base.address + "-missing", authkey=base.authkey
    )
    client = LocalSchedulerClient(missing)
    try:
        with pytest.raises(SchedulerUnavailable):
            client.health()
    finally:
        client.close()


def test_release_and_renew_accept_a_remote_lease():
    client, conn = _client_with({"ok": True, "expires_at": 12.5})
    lease = RemoteLease(
        client,
        {
            "token": "t1",
            "pool": "p",
            "rid": "r1",
            "used": 2,
            "outcome": "captcha",
        },
    )
    assert client.release("p", lease) is True
    assert conn.messages[-1]["token"] == "t1"
    assert conn.messages[-1]["used"] == 2
    assert conn.messages[-1]["outcome"] == "captcha"
    assert client.renew("p", lease) == pytest.approx(12.5)
    assert conn.messages[-1]["used"] == 2

    def reply(message):
        if message["op"] == "acquire":
            lease = {"token": "t2", "pool": "p", "rid": "r1"}
            return {"id": message["id"], "ok": True, "result": {"lease": lease}}
        return {"id": message["id"], "ok": True, "result": {"ok": True}}

    ctx_client, ctx_conn = _client_with(reply=reply)
    with ctx_client.lease("p", consumer="c") as captured:
        assert captured.token == "t2"
    assert [message["op"] for message in ctx_conn.messages] == ["acquire", "release"]
    assert captured.released


def test_pool_call_and_bulk_operations_return_values():
    client, conn = _client_with({"value": 7})
    assert client.pool_call("p", "leases") == 7
    assert conn.messages[-1]["method"] == "leases"

    client, conn = _client_with({"imported": 1})
    assert client.import_resources("p", [{"id": "r1"}]) == {"imported": 1}
    assert conn.messages[-1]["op"] == "import"

    client, conn = _client_with({"generated": 2})
    assert client.generate("p", [{"id": "r1"}]) == {"generated": 2}
    assert conn.messages[-1]["op"] == "generate"


def test_acquire_honours_explicit_resource_type_and_pool_replies():
    results = {
        "pool_names": {"pools": [1]},
        "create_pool": {"pool": {"name": "p"}},
        "update_pool": {"config": {"max_slots": 2}},
        "stats": {"stats": {"total": 0}},
        "delete_pool": {"removed": 1},
    }

    def reply(message):
        if message["op"] == "acquire":
            resource = {
                "id": "r1",
                "kind": "proxy",
                "payload": {"host": "1.2.3.4", "port": 8080},
            }
            lease = {
                "token": "t1",
                "pool": "p",
                "rid": "r1",
                "resource": resource,
            }
            return {"id": message["id"], "ok": True, "result": {"lease": lease}}
        return {"id": message["id"], "ok": True, "result": results[message["op"]]}

    client, _ = _client_with(reply=reply)
    lease = client.acquire("p", resource_cls=ProxyResource, timeout=5)
    assert isinstance(lease.resource, ProxyResource)
    assert client.pool_names() == ["1"]
    assert client.create_pool("p", "proxy") == {"name": "p"}
    assert client.update_pool("p", max_slots=2) == {"max_slots": 2}
    assert client.stats("p") == {"total": 0}
    assert client.delete_pool("p", force=True) == {"removed": 1}


def test_acquire_rejects_bad_request_and_records():
    client = LocalSchedulerClient(default_address())
    with pytest.raises(TypeError):
        client.acquire("p", req=42)
    with pytest.raises(TypeError):
        client.import_resources("p", [42])
    with pytest.raises(ConfigError):
        client.register_adapter(42)
    client.close()

    from purser import AdapterSpec

    stub, conn = _client_with({})
    stub.register_adapter(AdapterSpec("fakemod:Cls", {"flag": 1}))
    stub.register_adapter({"path": "fakemod:Cls"})
    assert conn.messages[0]["spec"] == {"path": "fakemod:Cls", "kwargs": {"flag": 1}}
    assert conn.messages[1]["spec"] == {"path": "fakemod:Cls"}


# -- process-wide worker helpers ------------------------------------------


def test_worker_helpers_require_a_usable_address(monkeypatch):
    monkeypatch.delenv(ENV_VAR, raising=False)
    monkeypatch.setattr(client_mod, "_default_address", None)
    with pytest.raises(ConfigError):
        get_client()
    with pytest.raises(ConfigError):
        init_worker(None)
    with pytest.raises(TypeError):
        init_worker(42)


def test_worker_helpers_share_one_client_per_address(monkeypatch):
    monkeypatch.delenv(ENV_VAR, raising=False)
    monkeypatch.setattr(client_mod, "_default_address", None)
    address = default_address()
    try:
        client = init_worker(address)
        assert get_client() is client
        close_client()
        close_client()  # already closed: no-op
        assert client._closed
        second = get_client(address)
        assert second is not client
        set_default_address(address)
        assert get_client() is second
    finally:
        close_client()
        monkeypatch.setattr(client_mod, "_default_address", None)


# -- RemoteLease snapshot -------------------------------------------------


class FakeLeaseClient:
    def __init__(self, *, release_ok: bool = True, expires_at: float = 999.0) -> None:
        self.release_ok = release_ok
        self.expires_at = expires_at
        self.calls: list[tuple] = []

    def release(self, pool, token, *, outcome=None, used=None, idempotency_key=None):
        self.calls.append(("release", pool, token, outcome, used, idempotency_key))
        return self.release_ok

    def renew(self, pool, token, *, used=None):
        self.calls.append(("renew", pool, token, used))
        return self.expires_at


def _lease(client=None) -> RemoteLease:
    return RemoteLease(
        client or FakeLeaseClient(),
        {
            "token": "t1",
            "pool": "p",
            "rid": "r1",
            "slots": 2,
            "priority": 3,
            "use_limit": 5,
            "used": 1,
            "trace_id": "trace",
            "target": "site",
            "consumer": "crawler",
            "replayed": True,
            "resource": {"id": "r1", "kind": "fakecheck"},
        },
    )


def test_remote_lease_exposes_wire_snapshot():
    lease = _lease()
    assert (lease.token, lease.pool, lease.rid) == ("t1", "p", "r1")
    assert (lease.slots, lease.priority, lease.use_limit, lease.used) == (2, 3, 5, 1)
    assert (lease.consumer, lease.trace_id, lease.target) == ("crawler", "trace", "site")
    assert lease.replayed is True
    assert lease.resource is not None and lease.resource.id == "r1"
    assert "token='t1'" in repr(lease)
    assert lease.expired(now=lease.expires_at + 1) is True


def test_remote_lease_setters_validate_input():
    lease = _lease()
    lease.payload = {"k": "v"}
    assert lease.payload == {"k": "v"}
    lease.outcome = "timeout"
    assert lease.outcome is UsageOutcome.TIMEOUT
    lease.used = 2
    assert lease.used == 2
    with pytest.raises(ValueError):
        lease.used = -1
    with pytest.raises(ValueError):
        lease.used = True


def test_remote_lease_release_is_idempotent_and_forwards():
    client = FakeLeaseClient()
    lease = _lease(client)
    lease.release(outcome=UsageOutcome.CAPTCHA, used=4)
    lease.release()
    assert lease.released
    assert client.calls == [("release", "p", "t1", UsageOutcome.CAPTCHA, 4, None)]


def test_remote_lease_renew_updates_snapshot():
    client = FakeLeaseClient(expires_at=4242.0)
    lease = _lease(client)
    assert lease.renew() == pytest.approx(4242.0)
    assert lease.expires_at == pytest.approx(4242.0)
    assert lease.heartbeat_at >= lease.started_at


def test_remote_lease_context_manager_releases():
    client = FakeLeaseClient()
    with _lease(client) as lease:
        assert lease.token == "t1"
    assert lease.released and client.calls[0][0] == "release"
