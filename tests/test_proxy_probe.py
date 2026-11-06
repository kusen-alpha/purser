"""Real network probes against local fake proxy servers (S2)."""

from __future__ import annotations

import asyncio

from purser.core.resource import Resource
from purser.types.proxy import ProxyType


async def start_connect_server() -> tuple[asyncio.Server, int]:
    """Fake HTTP CONNECT proxy that always answers 200."""

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            first = await asyncio.wait_for(reader.readline(), 2.0)
            if not first.upper().startswith(b"CONNECT"):
                writer.write(b"HTTP/1.1 400 Bad Request\r\n\r\n")
            else:
                # Consume request headers, then reply 200.
                while True:
                    line = await asyncio.wait_for(reader.readline(), 2.0)
                    if line in (b"\r\n", b""):
                        break
                writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            await writer.drain()
            await asyncio.sleep(5)  # keep socket open for latency measurement
            writer.close()
        except Exception:
            writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    return server, server.sockets[0].getsockname()[1]


async def start_socks5_server(
    reply: bytes = b"\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00",
) -> tuple[asyncio.Server, int]:
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            version = (await asyncio.wait_for(reader.readexactly(1), 2.0))[0]
            nmethods = (await asyncio.wait_for(reader.readexactly(1), 2.0))[0]
            await reader.readexactly(nmethods)
            assert version == 5
            writer.write(b"\x05\x00")  # no-auth chosen
            await writer.drain()
            head = await asyncio.wait_for(reader.readexactly(4), 2.0)
            atyp = head[3]
            if atyp == 1:
                await reader.readexactly(4)
            elif atyp == 3:
                length = (await asyncio.wait_for(reader.readexactly(1), 2.0))[0]
                await reader.readexactly(length)
            else:
                await reader.readexactly(16)
            await reader.readexactly(2)  # port
            writer.write(reply)
            await writer.drain()
            await asyncio.sleep(5)
            writer.close()
        except Exception:
            writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    return server, server.sockets[0].getsockname()[1]


def proxy_resource(host: str, port: int, protocol: str = "http") -> Resource:
    return Resource(
        id=f"{protocol}://{host}:{port}",
        kind="proxy",
        payload={"host": host, "port": port, "protocol": protocol},
    )


async def test_http_connect_probe_ok():
    server, port = await start_connect_server()
    rtype = ProxyType(probe_targets=["http://example.com:80"], timeout=3.0)
    report = await rtype.validate(proxy_resource("127.0.0.1", port))
    assert report.alive
    assert report.latency_ms is not None and report.latency_ms >= 0
    server.close()
    await server.wait_closed()


async def test_http_connect_probe_fails_on_refused():
    rtype = ProxyType(probe_targets=["http://example.com:80"], timeout=1.0)
    report = await rtype.validate(proxy_resource("127.0.0.1", 1))  # closed port
    assert not report.alive
    assert report.error is not None


async def test_socks5_probe_ok():
    server, port = await start_socks5_server()
    rtype = ProxyType(
        probe_targets=["http://example.com:80"], timeout=3.0
    )
    report = await rtype.validate(proxy_resource("127.0.0.1", port, protocol="socks5"))
    assert report.alive
    server.close()
    await server.wait_closed()


async def test_socks5_probe_domain_bnd_ok():
    server, port = await start_socks5_server(reply=b"\x05\x00\x00\x03\x03foo\x00\x50")
    rtype = ProxyType(probe_targets=["http://example.com:80"], timeout=3.0)
    report = await rtype.validate(proxy_resource("127.0.0.1", port, protocol="socks5"))
    assert report.alive
    server.close()
    await server.wait_closed()


async def test_probe_guard_pauses_judgement_when_all_down():
    from purser.core.health import ProbeGuard

    guard = ProbeGuard(fail_threshold=1, cooldown=600.0)
    guard.record("http://example.com:80", ok=False)
    rtype = ProxyType(
        probe_targets=["http://example.com:80"], timeout=1.0, probe_guard=guard
    )
    report = await rtype.validate(proxy_resource("127.0.0.1", 1))
    assert report.alive  # paused, never banned (X1)
    assert "disabled" in (report.error or "")
