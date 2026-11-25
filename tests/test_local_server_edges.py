"""In-process edges of the local scheduler service.

The service normally lives in a spawned process, but every branch here is
transport-level: fake listeners and connections let the accept loop, protocol
error paths and lease-owner bookkeeping run deterministically without sockets.
"""

from __future__ import annotations

import queue
import threading
from collections.abc import Iterator

import pytest

from purser.errors import ConfigError
from purser.local import server as server_mod
from purser.local.errors import SchedulerProtocolError
from purser.local.protocol import SchedulerAddress
from purser.local.server import _scheduler_main, _Service
from purser.sync import SyncPurser


class FakeConn:
    def __init__(self, incoming: list[object] | None = None) -> None:
        self.incoming: Iterator[object] = iter(incoming or [])
        self.sent: list[dict] = []
        self.closed = False

    def close(self) -> None:
        self.closed = True


class FakeListener:
    def __init__(self, results: list[object]) -> None:
        self.results = list(results)
        self.closed = False

    def accept(self):
        item = self.results.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item

    def close(self) -> None:
        self.closed = True


def _purser() -> SyncPurser:
    return SyncPurser(maintenance_enabled=False)


def _service(listener=None) -> _Service:
    return _Service(_purser(), listener or FakeListener([]))


def _request(**fields):
    return {"id": 1, **fields}


def test_scheduler_main_reports_startup_failure():
    errors: queue.Queue[str] = queue.Queue()
    ready = threading.Event()
    address = SchedulerAddress(
        family="AF_INET", address=("127.0.0.1", 1), authkey=b"k"
    )
    _scheduler_main(address, {"storage": "bogus"}, ready, errors)
    assert ready.is_set()
    detail = errors.get(timeout=1)
    assert "bogus" in detail or "ConfigError" in detail


def test_scheduler_main_reports_listener_failure():
    errors: queue.Queue[str] = queue.Queue()
    ready = threading.Event()
    address = SchedulerAddress(
        family="AF_INET", address=("127.0.0.1", 70000), authkey=b"k"
    )
    _scheduler_main(address, {"maintenance_enabled": False}, ready, errors)
    assert ready.is_set()
    assert not errors.empty()


def test_handle_request_health_and_pool_names():
    with _purser() as ps:
        service = _Service(ps, FakeListener([]))
        health = service._handle_request(_request(op="health"))
        assert health["ok"] is True
        assert health["result"]["protocol"] == 1
        names = service._handle_request(_request(op="pool_names"))
        assert names["result"] == {"pools": []}


def test_handle_request_errors_are_mapped():
    with _purser() as ps:
        service = _Service(ps, FakeListener([]))
        missing_op = service._handle_request(_request())
        assert missing_op["ok"] is False
        assert "op" in missing_op["error"]["message"]

        unknown = service._handle_request(_request(op="nope"))
        assert unknown["ok"] is False
        assert "unknown scheduler operation" in unknown["error"]["message"]

        # KeyError from a malformed payload must not escape either.
        no_pool = service._handle_request(_request(op="stats"))
        assert no_pool["ok"] is False
        assert no_pool["error"]["details"] is None


def test_handle_request_rereaises_keyboard_interrupt(monkeypatch):
    service = _service()

    def die():
        raise KeyboardInterrupt

    monkeypatch.setattr(service, "_health", die)
    with pytest.raises(KeyboardInterrupt):
        service._handle_request(_request(op="health"))


def test_discard_response_variants(monkeypatch):
    service = _service()
    released: list[tuple] = []
    monkeypatch.setattr(
        service, "_release_token", lambda *args: released.append(args)
    )

    service._discard_response({"result": "not-a-dict"})
    service._discard_response({"result": {"lease": "not-a-dict"}})
    service._discard_response({"result": {"lease": {"pool": "p"}}})
    assert released == []

    service._discard_response(
        {"result": {"lease": {"pool": "p", "token": "tok"}}}
    )
    assert released == [("p", "tok", None, None)]


def test_discard_response_swallows_release_errors(monkeypatch):
    service = _service()

    def boom(*args):
        raise RuntimeError("release failed")

    monkeypatch.setattr(service, "_release_token", boom)
    service._discard_response(
        {"result": {"lease": {"pool": "p", "token": "tok"}}}
    )


def test_lease_owner_bookkeeping(monkeypatch):
    service = _service()
    released: list[tuple] = []
    monkeypatch.setattr(
        service, "_release_token", lambda *args: released.append(args)
    )
    first, second = FakeConn(), FakeConn()

    service._track_lease(first, "p", "tok")
    service._track_lease(second, "p", "tok")
    service._release_conn_leases(first)
    assert released == []
    assert service._lease_owners["tok"][1] == {second}

    service._release_conn_leases(second)
    assert released == [("p", "tok", None, None)]
    assert "tok" not in service._lease_owners

    service._untrack_lease("tok")


def test_release_conn_leases_swallows_release_errors(monkeypatch):
    service = _service()
    conn = FakeConn()

    def boom(*args):
        raise RuntimeError("release failed")

    monkeypatch.setattr(service, "_release_token", boom)
    service._track_lease(conn, "p", "tok")
    service._release_conn_leases(conn)


def test_accept_loop_registers_and_stops_connections():
    conn = FakeConn([EOFError()])
    listener = FakeListener([conn, EOFError()])
    service = _service(listener)
    thread = threading.Thread(target=service.accept_loop)
    thread.start()
    thread.join(5)
    assert not thread.is_alive()
    assert conn.closed

    service._conns.add(conn)
    service.stop()
    assert service._stop.is_set()
    assert listener.closed
    assert conn.closed


def _fake_recv(monkeypatch, *, error: BaseException | None = None) -> None:
    def recv(conn):
        if error is not None:
            raise error
        item = next(conn.incoming)
        if isinstance(item, BaseException):
            raise item
        return item

    monkeypatch.setattr(server_mod, "recv_message", recv)


def test_accept_loop_closes_connection_when_already_stopping():
    conn = FakeConn()
    listener = FakeListener([conn])
    service = _service(listener)

    def stop_on_accept():
        service._stop.set()
        return listener.results.pop(0)

    listener.accept = stop_on_accept
    service.accept_loop()
    assert conn.closed


def test_join_handlers_returns_when_deadline_passed():
    service = _service()
    service._handlers.add(object())
    service.join_handlers(0)


def test_serve_connection_reports_protocol_error(monkeypatch):
    service = _service()
    conn = FakeConn()
    sent: list[dict] = []
    _fake_recv(monkeypatch, error=SchedulerProtocolError("bad frame"))
    monkeypatch.setattr(
        server_mod, "send_message", lambda _conn, payload: sent.append(payload)
    )
    service._serve_connection(conn)
    assert conn.closed
    assert sent[0]["error"]["code"] == "scheduler_protocol"


def test_serve_connection_breaks_on_generic_recv_error(monkeypatch):
    service = _service()
    conn = FakeConn()
    _fake_recv(monkeypatch, error=RuntimeError("pipe gone"))
    service._serve_connection(conn)
    assert conn.closed


def test_serve_connection_sends_result_and_honours_shutdown(monkeypatch):
    service = _service()
    sent: list[dict] = []
    _fake_recv(monkeypatch)
    monkeypatch.setattr(
        server_mod, "send_message", lambda _conn, payload: sent.append(payload)
    )
    health = FakeConn([_request(op="health"), EOFError()])
    service._serve_connection(health)
    assert sent and sent[0]["ok"] is True

    closing = FakeConn([_request(op="shutdown")])
    service._serve_connection(closing)
    assert service.shutdown_ack.is_set()
    assert closing.closed


def test_serve_connection_discards_response_on_send_failure(monkeypatch):
    service = _service()
    discarded: list[dict] = []
    monkeypatch.setattr(service, "_discard_response", discarded.append)
    _fake_recv(monkeypatch)

    def broken_send(_conn, _payload):
        raise SchedulerProtocolError("broken pipe")

    monkeypatch.setattr(server_mod, "send_message", broken_send)
    conn = FakeConn([_request(op="health")])
    service._serve_connection(conn)
    assert len(discarded) == 1
    assert conn.closed


def test_register_adapter_requires_spec_object():
    with _purser() as ps:
        service = _Service(ps, FakeListener([]))
        with pytest.raises(ConfigError, match="spec object"):
            service._register_adapter({"spec": "nope"})
