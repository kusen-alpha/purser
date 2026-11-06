"""Proxy resource type with real HTTP CONNECT / SOCKS probes (S2)."""

from __future__ import annotations

import asyncio
import base64
import math
import socket
import time
from typing import Any

from purser.core.health import HealthReport, ProbeGuard
from purser.core.resource import (
    AcquireRequest,
    CheckVerdict,
    Resource,
)
from purser.types.base import ResourceType

DEFAULT_PROBE_TARGETS = ["http://example.com:80"]


def parse_endpoint(target: str) -> tuple[str, int]:
    """Accept ``http://host:port``, ``host:port`` or bare ``host``."""

    rest = target
    if "://" in target:
        rest = target.split("://", 1)[1]
    host, sep, port = rest.rpartition(":")
    if sep and port.isdigit():
        return host, int(port)
    return rest, 80


class ProxyType(ResourceType):
    kind = "proxy"
    default_concurrency = 1

    def __init__(
        self,
        *,
        probe_targets: list[str] | None = None,
        timeout: float = 8.0,
        probe_guard: ProbeGuard | None = None,
    ) -> None:
        self.probe_targets = list(probe_targets or DEFAULT_PROBE_TARGETS)
        self.timeout = timeout
        self.probe_guard = probe_guard

    async def generate(self, spec: dict[str, Any]) -> Resource:
        host = spec.get("host")
        port = spec.get("port")
        if not host or not port:
            raise ValueError("proxy spec requires host and port")
        payload = {
            "host": host,
            "port": int(port),
            "protocol": spec.get("protocol", "http"),
        }
        if spec.get("username"):
            payload["username"] = spec["username"]
        if spec.get("password"):
            payload["password"] = spec["password"]
        tags = frozenset(spec.get("tags") or ())
        rid = spec.get("id") or f"proxy://{host}:{port}"
        return Resource(
            id=rid,
            kind=self.kind,
            payload=payload,
            tags=tags,
            meta={"source": spec.get("source", "manual")},
        )

    async def validate(self, resource: Resource) -> HealthReport:
        payload = resource.payload
        host = payload.get("host")
        if not isinstance(host, str) or not host:
            return HealthReport.fail("missing proxy host")
        port = int(payload.get("port", 0))
        protocol = (payload.get("protocol") or "http").lower()
        if protocol not in ("http", "https", "socks4", "socks5"):
            return HealthReport.fail(f"unsupported protocol {protocol!r}")
        auth = None
        if payload.get("username") is not None:
            auth = (str(payload["username"]), str(payload.get("password") or ""))

        results: list[tuple[bool, float | None, str | None]] = []
        probes_run = 0
        for target in self.probe_targets:
            if self.probe_guard is not None and not self.probe_guard.enabled(target):
                continue
            probes_run += 1
            ok, latency, error = await self._probe(
                protocol, host, port, target, auth, self.timeout
            )
            if self.probe_guard is not None:
                self.probe_guard.record(target, ok)
            results.append((ok, latency, error))

        if not results:
            # Every probe is on cooldown; we cannot judge, so never ban (X1).
            return HealthReport(alive=True, error="all probes disabled")
        if self.probe_guard is not None and self.probe_guard.all_down(self.probe_targets):
            return HealthReport(alive=True, error="all probes down; judgement paused")

        alive = [r for r in results if r[0]]
        quorum = max(1, math.ceil(2 * len(results) / 3))
        ok = len(alive) >= quorum
        latencies = [r[1] for r in alive if r[1] is not None]
        latency = min(latencies) if latencies else None
        if ok:
            return HealthReport.ok(latency)
        detail = " | ".join(e or "no response" for _, _, e in results[:3])
        return HealthReport.fail(f"probe failed: {detail}")

    async def _probe(
        self,
        protocol: str,
        host: str,
        port: int,
        target: str,
        auth: tuple[str, str] | None,
        timeout: float,
    ) -> tuple[bool, float | None, str | None]:
        target_host, target_port = parse_endpoint(target)
        if protocol in ("http", "https"):
            return await self._probe_http_connect(
                host, port, target_host, target_port, auth, timeout
            )
        if protocol == "socks5":
            return await self._probe_socks5(
                host, port, target_host, target_port, auth, timeout
            )
        return await self._probe_socks4(host, port, target_host, target_port, auth, timeout)

    async def _probe_http_connect(
        self,
        host: str,
        port: int,
        target_host: str,
        target_port: int,
        auth: tuple[str, str] | None,
        timeout: float,
    ) -> tuple[bool, float | None, str | None]:
        started = time.monotonic()
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout
            )
        except Exception as exc:
            return False, None, str(exc)
        try:
            lines = [
                f"CONNECT {target_host}:{target_port} HTTP/1.1",
                f"Host: {target_host}:{target_port}",
            ]
            if auth:
                token = base64.b64encode(f"{auth[0]}:{auth[1]}".encode()).decode()
                lines.append(f"Proxy-Authorization: Basic {token}")
            request = "\r\n".join(lines) + "\r\n\r\n"
            writer.write(request.encode("latin-1"))
            await asyncio.wait_for(writer.drain(), timeout)
            line = await asyncio.wait_for(reader.readline(), timeout)
            latency = (time.monotonic() - started) * 1000.0
            parts = line.decode("latin-1", "replace").split()
            if len(parts) >= 2 and parts[1] == "200":
                # Consume remaining headers so the connection can be reused.
                while True:
                    header = await asyncio.wait_for(reader.readline(), timeout)
                    if header in (b"", b"\r\n"):
                        break
                return True, latency, None
            return False, latency, " ".join(parts[:3])
        except Exception as exc:
            return False, (time.monotonic() - started) * 1000.0, str(exc)
        finally:
            writer.close()
            try:
                await asyncio.wait_for(writer.wait_closed(), 2.0)
            except Exception:
                pass

    async def _probe_socks5(
        self,
        host: str,
        port: int,
        target_host: str,
        target_port: int,
        auth: tuple[str, str] | None,
        timeout: float,
    ) -> tuple[bool, float | None, str | None]:
        started = time.monotonic()
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout
            )
        except Exception as exc:
            return False, None, str(exc)
        try:
            if auth:
                writer.write(bytes([5, 2, 0, 2]))
            else:
                writer.write(bytes([5, 1, 0]))
            await asyncio.wait_for(writer.drain(), timeout)
            version, method = await asyncio.wait_for(reader.readexactly(2), timeout)
            if version != 5 or method not in (0, 2):
                return False, None, "socks5 handshake rejected"
            if method == 2:
                if not auth:
                    return False, None, "socks5 requires auth"
                user, pwd = auth[0].encode(), auth[1].encode()
                writer.write(bytes([1, len(user)]) + user + bytes([len(pwd)]) + pwd)
                await asyncio.wait_for(writer.drain(), timeout)
                if await asyncio.wait_for(reader.readexactly(2), timeout) != b"\x01\x00":
                    return False, None, "socks5 auth failed"
            request = self._socks5_request(target_host, target_port)
            writer.write(request)
            await asyncio.wait_for(writer.drain(), timeout)
            head = await asyncio.wait_for(reader.readexactly(4), timeout)
            latency = (time.monotonic() - started) * 1000.0
            if head[1] != 0:
                return False, latency, f"socks5 connect failed rep={head[1]}"
            atyp = head[3]
            extra = 6 if atyp == 1 else (16 if atyp == 4 else -1)
            if atyp == 3:
                domain_len = await asyncio.wait_for(reader.readexactly(1), timeout)
                extra = 1 + domain_len[0]
            if extra >= 0:
                await asyncio.wait_for(reader.readexactly(extra), timeout)
            return True, latency, None
        except Exception as exc:
            return False, None, str(exc)
        finally:
            writer.close()
            try:
                await asyncio.wait_for(writer.wait_closed(), 2.0)
            except Exception:
                pass

    @staticmethod
    def _socks5_request(host: str, port: int) -> bytes:
        try:
            packed = socket.inet_aton(host)
            atyp = 1
            addr = packed
        except OSError:
            raw = host.encode()
            atyp = 3
            addr = bytes([len(raw)]) + raw
        return bytes([5, 1, 0, atyp]) + addr + port.to_bytes(2, "big")

    async def _probe_socks4(
        self,
        host: str,
        port: int,
        target_host: str,
        target_port: int,
        auth: tuple[str, str] | None,
        timeout: float,
    ) -> tuple[bool, float | None, str | None]:
        started = time.monotonic()
        try:
            loop = asyncio.get_running_loop()
            infos = await loop.getaddrinfo(target_host, target_port, family=socket.AF_INET)
            ip = infos[0][4][0]
        except Exception as exc:
            return False, None, f"resolve failed: {exc}"
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout
            )
        except Exception as exc:
            return False, None, str(exc)
        try:
            user = (auth[0] if auth else "").encode()
            request = (
                bytes([4, 1])
                + target_port.to_bytes(2, "big")
                + socket.inet_aton(str(ip))
                + user
                + b"\x00"
            )
            writer.write(request)
            await asyncio.wait_for(writer.drain(), timeout)
            response = await asyncio.wait_for(reader.readexactly(8), timeout)
            latency = (time.monotonic() - started) * 1000.0
            if response[0] == 0 and response[1] == 90:
                return True, latency, None
            return False, latency, f"socks4 connect failed code={response[1]}"
        except Exception as exc:
            return False, None, str(exc)
        finally:
            writer.close()
            try:
                await asyncio.wait_for(writer.wait_closed(), 2.0)
            except Exception:
                pass

    async def on_request_check(
        self, resource: Resource, req: AcquireRequest
    ) -> CheckVerdict:
        # Base implementation already PASSes; proxies are validated on acquire
        # via the selected candidate's short-lived check cache in the pool.
        return CheckVerdict.PASS
