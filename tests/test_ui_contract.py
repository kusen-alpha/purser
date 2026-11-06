"""Console UI contract: `/` serves the shell and every fetch target exists."""

from __future__ import annotations

from fastapi.testclient import TestClient

from purser import Purser
from purser.ui.server import create_app


def test_console_served_and_endpoints_exist():
    app = create_app(Purser(maintenance_enabled=False))
    client = TestClient(app)
    resp = client.get("/")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]
    assert "purSer" in resp.text

    expected = [
        ("GET", "/pools"),
        ("POST", "/pools"),
        ("GET", "/pools/{pool_name}"),
        ("GET", "/pools/{pool_name}/resources"),
        ("POST", "/pools/{pool_name}/resources"),
        ("POST", "/pools/{pool_name}/resources/{rid}/{action}"),
        ("POST", "/pools/{pool_name}/leases"),
        ("GET", "/pools/{pool_name}/leases"),
        ("DELETE", "/pools/{pool_name}/leases/{token}"),
        ("GET", "/pools/{pool_name}/events"),
    ]
    methods_by_path: dict[str, set[str]] = {}
    for route in app.routes:
        methods_by_path.setdefault(route.path, set()).update(
            set(getattr(route, "methods", set()) or set())
        )
    for method, path in expected:
        assert path in methods_by_path, f"missing route {path}"
        assert method in methods_by_path[path], (method, path)
