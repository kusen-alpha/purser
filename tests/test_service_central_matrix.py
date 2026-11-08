"""E-mode acceptance: one central REST service owns the resource set.

Each storage x coordination shape is exercised through a real ``uvicorn``
process over HTTP.  Clients never open a backend themselves; the chosen shape
only decides what the service uses internally, so even memory backends become
shareable once the service is the single authority.

Real Redis/MySQL are required for the corresponding shapes; without them those
tests skip rather than falling back to fakeredis.  Redis DB isolation is handled
by the autouse fixture in ``conftest.py``; MySQL pools are deleted by name.
"""

from __future__ import annotations

import contextlib
import os
import socket
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

import httpx
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
REDIS_URL = os.environ.get("PURSER_TEST_REDIS_URL")
MYSQL_URL = os.environ.get("PURSER_TEST_MYSQL_URL")

SHAPES = [
    ("memory", "memory"),
    ("memory", "redis"),
    ("sqlite", "memory"),
    ("sqlite", "redis"),
    ("mysql", "memory"),
    ("mysql", "redis"),
]

API_KEY = "service-matrix-key"


def _skip_reason(storage: str, coordination: str) -> str | None:
    if coordination == "redis" and not REDIS_URL:
        return "PURSER_TEST_REDIS_URL not set (not tested)"
    if storage == "mysql" and not MYSQL_URL:
        return "PURSER_TEST_MYSQL_URL not set (not tested)"
    return None


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@contextlib.contextmanager
def _service(shape: tuple[str, str], tmp_path: Path):
    storage, coordination = shape
    port = _free_port()
    cmd = [
        sys.executable,
        "-m",
        "purser.cli",
        "serve",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--storage",
        storage,
        "--coordination",
        coordination,
    ]
    if storage == "sqlite":
        cmd += ["--sqlite-path", str(tmp_path / "service.db")]
    if storage == "mysql":
        cmd += ["--mysql-url", MYSQL_URL]
    if coordination == "redis":
        cmd += ["--redis-url", REDIS_URL]
    env = os.environ.copy()
    env["PURSER_API_KEY"] = API_KEY
    env.pop("PURSER_READONLY_KEY", None)
    if storage == "mysql":
        # MySQL pools with secret fields refuse an ephemeral key; a fixed test
        # key keeps restarts and multiple service runs readable.
        env["PURSER_SECRET_KEY"] = "0123456789abcdef0123456789abcdef"

    log_path = tmp_path / "service.log"
    log = open(log_path, "wb")
    proc = subprocess.Popen(
        cmd,
        cwd=REPO_ROOT,
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    base = f"http://127.0.0.1:{port}"
    try:
        deadline = time.time() + 30.0
        ready = False
        while time.time() < deadline:
            if proc.poll() is not None:
                break
            try:
                if httpx.get(f"{base}/healthz", timeout=0.5).status_code == 200:
                    ready = True
                    break
            except httpx.HTTPError:
                pass
            time.sleep(0.1)
        if not ready:
            log.flush()
            detail = log_path.read_text(encoding="utf-8", errors="replace")
            raise RuntimeError(f"service did not start for {shape}:\n{detail}")
        yield base
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10.0)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        log.close()


def _auth() -> dict[str, str]:
    return {"Authorization": f"Bearer {API_KEY}"}


@pytest.mark.parametrize("shape", SHAPES, ids=[f"{s}-{c}" for s, c in SHAPES])
def test_service_owns_one_shared_resource_set(shape, tmp_path):
    reason = _skip_reason(*shape)
    if reason:
        pytest.skip(reason)

    pool = f"svc-{uuid.uuid4().hex[:10]}"
    pool_url = f"/pools/{pool}"

    with _service(shape, tmp_path) as base:
        with httpx.Client(base_url=base, headers=_auth(), timeout=30.0) as client:
            try:
                status = client.get("/service").json()
                assert (status["storage"], status["coordination"]) == shape

                created = client.post(
                    "/pools", json={"name": pool, "kind": "account"}
                )
                assert created.status_code == 200, created.text

                imported = client.post(
                    f"{pool_url}/resources",
                    json={
                        "records": [
                            {"id": "r0", "username": "u0", "max_slots": 1}
                        ],
                        "import_validate": "on_import",
                    },
                )
                assert imported.status_code == 200, imported.text
                assert imported.json()["imported"] == 1

                acquire_body = {
                    "consumer": "a",
                    "resource_id": "r0",
                    "slots": 1,
                    "timeout": 1.0,
                }
                first = client.post(
                    f"{pool_url}/leases",
                    json=acquire_body,
                    headers={"Idempotency-Key": "acquire-r0"},
                )
                assert first.status_code == 200, first.text
                token = first.json()["leases"][0]["token"]

                replay = client.post(
                    f"{pool_url}/leases",
                    json=acquire_body,
                    headers={"Idempotency-Key": "acquire-r0"},
                )
                assert replay.status_code == 200
                assert replay.json()["leases"][0]["token"] == token
                assert replay.json()["replayed"] is True

                resource = client.get(f"{pool_url}/resources/r0").json()
                assert resource["used_slots"] == 1
                assert client.get(pool_url).json()["active_leases"] == 1

                blocked = client.post(
                    f"{pool_url}/leases",
                    json={
                        "consumer": "b",
                        "resource_id": "r0",
                        "slots": 1,
                        "timeout": 0.3,
                    },
                )
                assert blocked.status_code == 409, blocked.text

                result: dict = {}

                def wait_for_slot() -> None:
                    with httpx.Client(
                        base_url=base, headers=_auth(), timeout=30.0
                    ) as waiter:
                        response = waiter.post(
                            f"{pool_url}/leases",
                            json={
                                "consumer": "c",
                                "resource_id": "r0",
                                "slots": 1,
                                "timeout": 5.0,
                            },
                        )
                        result["status"] = response.status_code
                        result["body"] = response.json()

                thread = threading.Thread(target=wait_for_slot)
                thread.start()
                time.sleep(0.35)
                released = client.request(
                    "DELETE",
                    f"{pool_url}/leases/{token}",
                    json={"outcome": "ok"},
                )
                assert released.status_code == 200, released.text
                thread.join(timeout=10.0)
                assert not thread.is_alive()
                assert result["status"] == 200, result.get("body")
                second_token = result["body"]["leases"][0]["token"]
                assert second_token != token

                history = client.get(
                    f"{pool_url}/events/history",
                    params={"type": "lease_acquired"},
                ).json()
                assert any(event["rid"] == "r0" for event in history["events"])

                assert (
                    client.delete(f"{pool_url}/leases/{second_token}").status_code
                    == 200
                )
                resource = client.get(f"{pool_url}/resources/r0").json()
                assert resource["used_slots"] == 0
                assert client.get(pool_url).json()["active_leases"] == 0
            finally:
                with contextlib.suppress(httpx.HTTPError):
                    client.delete(pool_url, params={"force": "true"})
