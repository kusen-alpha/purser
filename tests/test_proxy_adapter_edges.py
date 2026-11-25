"""Proxy adapter edges: parsing, malformed specs and probe handshakes.

The happy-path HTTP/SOCKS5 probes live in ``test_proxy_probe.py``; these
tests pin the rejected-handshake, auth and fallback branches of the SOCKS4/5
clients plus the request-time probe verdicts.  Every server binds to
``127.0.0.1:0`` and is closed by the test, so nothing touches the network.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

import pytest

from purser.adapters.proxy import ProxyAdapter, parse_endpoint
from purser.core.resource import AcquireRequest, CheckVerdict, Resource

Handler = Callable[[asyncio.StreamReader, asyncio.StreamWriter], Awaitable[None]]


@asynccontextmanager
async def _serve(handler: Handler) -> AsyncIterator[int]:
    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    try:
        yield server.sockets[0].getsockname()[1]
    finally:
        server.close()
        await server.wait_closed()


def _close(handler: Handler) -> Handler:
    async def wrapped(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            await handler(reader, writer)
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()

    return wrapped


def _http_handler(reply: bytes) -> Handler:
    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        await reader.readline()  # request line
        while True:
            line = await reader.readline()
            if line in (b"\r\n", b""):
                break
        writer.write(reply)
        await writer.drain()
        await reader.read()  # wait for the adapter to close

    return _close(handle)


def _socks5_handler(
    *,
    method: bytes = b"\x05\x00",
    auth_reply: bytes | None = None,
    connect_reply: bytes = b"\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00",
) -> Handler:
    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        _version, count = await reader.readexactly(2)
        await reader.readexactly(count)
        writer.write(method)
        await writer.drain()
        if method[1] == 2:
            if auth_reply is None:
                return
            await reader.readexactly(1)  # auth version
            ulen = (await reader.readexactly(1))[0]
            await reader.readexactly(ulen)
            plen = (await reader.readexactly(1))[0]
            await reader.readexactly(plen)
            writer.write(auth_reply)
            await writer.drain()
            if auth_reply != b"\x01\x00":
                return
        head = await reader.readexactly(4)
        atyp = head[3]
        if atyp == 1:
            await reader.readexactly(4)
        elif atyp == 3:
            length = (await reader.readexactly(1))[0]
            await reader.readexactly(length)
        elif atyp == 4:
            await reader.readexactly(16)
        await reader.readexactly(2)  # port
        writer.write(connect_reply)
        await writer.drain()
        await reader.read()

    return _close(handle)


def _socks4_handler(reply: bytes) -> Handler:
    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        await reader.readexactly(8)
        await reader.readuntil(b"\x00")  # user id terminator
        writer.write(reply)
        await writer.drain()
        await reader.read()

    return _close(handle)


def _resource(host: str, port: int, protocol: str = "http", **payload) -> Resource:
    data = {"host": host, "port": port, "protocol": protocol, **payload}
    return Resource(id=f"{protocol}://{host}:{port}", kind="proxy", payload=data)


@pytest.mark.parametrize(
    ("target", "expected"),
    [
        ("http://example.com:8080", ("example.com", 8080)),
        ("example.com:443", ("example.com", 443)),
        ("example.com", ("example.com", 80)),
        ("http://example.com", ("example.com", 80)),
        ("example.com:notaport", ("example.com:notaport", 80)),
    ],
)
def test_parse_endpoint_variants(target, expected):
    assert parse_endpoint(target) == expected


async def test_generate_builds_typed_proxy_and_rejects_incomplete_specs():
    adapter = ProxyAdapter()
    resource = await adapter.generate(
        {
            "host": "1.2.3.4",
            "port": "8080",
            "protocol": "socks5",
            "tags": ["eu"],
            "source": "feed",
            "session": "s-1",
            "ignored": None,
        }
    )
    assert resource.id == "proxy://1.2.3.4:8080"
    assert resource.kind == "proxy"
    assert resource.tags == frozenset({"eu"})
    assert resource.meta["source"] == "feed"
    assert resource.payload["session"] == "s-1"
    assert "ignored" not in resource.payload

    for spec in ({"port": 80}, {"host": "1.2.3.4"}, {"host": "h", "port": 0}):
        with pytest.raises(ValueError, match="requires host and port"):
            await adapter.generate(spec)


async def test_validate_rejects_missing_host_and_unknown_protocol():
    adapter = ProxyAdapter(probe_targets=["http://127.0.0.1:1"], timeout=0.5)
    missing = await adapter.validate(Resource(kind="proxy", id="x", payload={"port": 80}))
    assert not missing.alive and "missing proxy host" in (missing.error or "")

    unsupported = await adapter.validate(
        _resource("127.0.0.1", 1, protocol="ftp")
    )
    assert not unsupported.alive
    assert "unsupported protocol" in (unsupported.error or "")


async def test_http_connect_non_200_and_malformed_status_lines():
    adapter = ProxyAdapter(probe_targets=["http://example.com:80"], timeout=2.0)
    async with _serve(_http_handler(b"HTTP/1.1 407 Proxy Auth Required\r\n\r\n")) as port:
        report = await adapter.validate(_resource("127.0.0.1", port))
    assert not report.alive
    assert "probe failed" in (report.error or "")
    assert "407" in (report.error or "")

    async with _serve(_http_handler(b"GARBAGE\r\n\r\n")) as port:
        report = await adapter.validate(_resource("127.0.0.1", port))
    assert not report.alive
    assert "GARBAGE" in (report.error or "")


async def test_http_connect_sends_basic_auth_header():
    adapter = ProxyAdapter(probe_targets=["http://example.com:80"], timeout=2.0)
    async with _serve(
        _http_handler(b"HTTP/1.1 200 Connection Established\r\n\r\n")
    ) as port:
        report = await adapter.validate(
            _resource("127.0.0.1", port, username="u", password="p")
        )
    assert report.alive


async def test_validate_pauses_judgement_when_guard_reports_all_down():
    class _PausedGuard:
        def enabled(self, target: str) -> bool:
            return True

        def record(self, target: str, ok: bool) -> None:
            pass

        def all_down(self, probes: list[str]) -> bool:
            return True

    adapter = ProxyAdapter(probe_targets=["http://example.com:80"], probe_guard=_PausedGuard())

    async def fail_probe(*args, **kwargs):
        return False, None, "down"

    adapter._probe = fail_probe  # type: ignore[method-assign]
    report = await adapter.validate(_resource("127.0.0.1", 1))
    assert report.alive
    assert "paused" in (report.error or "")


async def test_socks5_auth_success_and_failure():
    adapter = ProxyAdapter(probe_targets=["http://example.com:80"], timeout=2.0)
    async with _serve(
        _socks5_handler(method=b"\x05\x02", auth_reply=b"\x01\x00")
    ) as port:
        ok = await adapter.validate(
            _resource("127.0.0.1", port, protocol="socks5", username="u", password="p")
        )
    assert ok.alive

    async with _serve(
        _socks5_handler(method=b"\x05\x02", auth_reply=b"\x01\x01")
    ) as port:
        bad = await adapter.validate(
            _resource("127.0.0.1", port, protocol="socks5", username="u", password="p")
        )
    assert not bad.alive and "auth failed" in (bad.error or "")


async def test_socks5_server_selecting_auth_without_credentials_is_rejected():
    adapter = ProxyAdapter(probe_targets=["http://example.com:80"], timeout=2.0)
    async with _serve(_socks5_handler(method=b"\x05\x02")) as port:
        report = await adapter.validate(_resource("127.0.0.1", port, protocol="socks5"))
    assert not report.alive
    assert "requires auth" in (report.error or "")


async def test_socks5_rejects_bad_version_and_connect_reply():
    adapter = ProxyAdapter(probe_targets=["http://example.com:80"], timeout=2.0)
    async with _serve(_socks5_handler(method=b"\x04\x00")) as port:
        report = await adapter.validate(_resource("127.0.0.1", port, protocol="socks5"))
    assert not report.alive and "handshake rejected" in (report.error or "")

    rejected = b"\x05\x01\x00\x01\x00\x00\x00\x00\x00\x00"
    async with _serve(_socks5_handler(connect_reply=rejected)) as port:
        report = await adapter.validate(_resource("127.0.0.1", port, protocol="socks5"))
    assert not report.alive and "rep=1" in (report.error or "")


@pytest.mark.parametrize(
    "reply",
    [
        b"\x05\x00\x00\x04" + b"\x00" * 16,
        b"\x05\x00\x00\x05",  # unknown address type: no extra bytes to read
    ],
    ids=["ipv6", "unknown-atyp"],
)
async def test_socks5_alternate_address_types_are_accepted(reply):
    adapter = ProxyAdapter(probe_targets=["http://example.com:80"], timeout=2.0)
    async with _serve(_socks5_handler(connect_reply=reply)) as port:
        report = await adapter.validate(_resource("127.0.0.1", port, protocol="socks5"))
    assert report.alive


async def test_socks5_request_encodes_numeric_and_domain_targets():
    numeric = ProxyAdapter._socks5_request("1.2.3.4", 80)
    assert numeric[:4] == b"\x05\x01\x00\x01"
    assert numeric[-2:] == (80).to_bytes(2, "big")

    domain = ProxyAdapter._socks5_request("example.com", 443)
    assert domain[:4] == b"\x05\x01\x00\x03"
    assert domain[4] == len(b"example.com")


async def test_socks4_success_and_failure_codes():
    adapter = ProxyAdapter(probe_targets=["http://127.0.0.1:80"], timeout=2.0)
    async with _serve(_socks4_handler(b"\x00\x5a\x00\x00\x00\x00\x00\x00")) as port:
        ok = await adapter.validate(_resource("127.0.0.1", port, protocol="socks4"))
    assert ok.alive

    async with _serve(_socks4_handler(b"\x00\x5b\x00\x00\x00\x00\x00\x00")) as port:
        bad = await adapter.validate(_resource("127.0.0.1", port, protocol="socks4"))
    assert not bad.alive and "code=91" in (bad.error or "")


async def test_socks4_resolution_and_connection_failures(monkeypatch):
    adapter = ProxyAdapter(probe_targets=["http://127.0.0.1:80"], timeout=0.5)

    async def no_dns(*args, **kwargs):
        raise OSError("dns down")

    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "getaddrinfo", no_dns)
    ok, latency, error = await adapter._probe_socks4(
        "127.0.0.1", 1, "example.com", 80, None, 0.5
    )
    assert not ok and latency is None
    assert (error or "").startswith("resolve failed")
    monkeypatch.undo()

    ok, latency, error = await adapter._probe_socks4(
        "127.0.0.1", 1, "127.0.0.1", 80, None, 0.5
    )
    assert not ok and latency is None and isinstance(error, str)


async def test_on_request_check_verdicts(monkeypatch):
    adapter = ProxyAdapter(probe_targets=["http://example.com:80"], timeout=0.5)
    resource = _resource("127.0.0.1", 1)

    assert (
        await adapter.on_request_check(resource, AcquireRequest())
        is CheckVerdict.PASS
    )

    no_host = Resource(kind="proxy", id="x", payload={"port": 80})
    assert (
        await adapter.on_request_check(no_host, AcquireRequest(probe="http://a:80"))
        is CheckVerdict.FAIL
    )

    assert (
        await adapter.on_request_check(
            resource, AcquireRequest(probe="http://127.0.0.1:1")
        )
        is CheckVerdict.FAIL
    )

    async def ok_probe(*args, **kwargs):
        return True, 1.0, None

    monkeypatch.setattr(adapter, "_probe", ok_probe)
    assert (
        await adapter.on_request_check(
            resource, AcquireRequest(probe="http://127.0.0.1:1")
        )
        is CheckVerdict.PASS
    )

    async def boom_probe(*args, **kwargs):
        raise RuntimeError("probe crashed")

    monkeypatch.setattr(adapter, "_probe", boom_probe)
    assert (
        await adapter.on_request_check(
            resource, AcquireRequest(probe="http://127.0.0.1:1")
        )
        is CheckVerdict.FAIL
    )
