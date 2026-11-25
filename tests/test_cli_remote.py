"""CLI remote handlers exercised against an in-process REST service.

The CLI speaks HTTP only, so the tests swap ``httpx.Client`` for the
``TestClient`` returned by the ``make_client`` fixture and point ``PURSER_URL``
at the service.  No sockets are opened.
"""

from __future__ import annotations

import io
import json

import pytest

from purser import cli

ADMIN = {"Authorization": "Bearer admin-key"}
BASE_URL = "http://service.local:9000"


def _dump(body) -> str:
    return json.dumps(body)


class _StubResponse:
    def __init__(
        self, status_code: int, body=None, text: str = "", lines: list[str] | None = None
    ) -> None:
        self.status_code = status_code
        self._body = body
        self.text = text
        self._lines = list(lines or ())

    def json(self):
        if isinstance(self._body, Exception):
            raise self._body
        return self._body

    def iter_lines(self):
        return iter(self._lines)


class _StubStream:
    def __init__(self, response: _StubResponse) -> None:
        self._response = response

    def __enter__(self) -> _StubResponse:
        return self._response

    def __exit__(self, *exc) -> bool:
        return False


@pytest.fixture
def service(make_client, monkeypatch):
    """Authorized service holding one tagged pool with two idle resources."""

    client, ps = make_client()
    assert (
        client.post("/pools", json={"name": "px", "kind": "fakecheck"}, headers=ADMIN)
        .status_code
        == 200
    )
    assert (
        client.post(
            "/pools/px/resources",
            json={
                "records": [
                    {"id": "r-0", "host": "127.0.0.1", "port": 8000, "tags": ["us"]},
                    {
                        "id": "r-1",
                        "host": "127.0.0.1",
                        "port": 8001,
                        "tags": ["eu"],
                        "usage_quota": 3,
                    },
                ],
                "import_validate": "on_import",
            },
            headers=ADMIN,
        ).status_code
        == 200
    )
    seen: dict = {}

    def factory(base_url="", headers=None, timeout=None, trust_env=True):
        seen["base_url"] = str(base_url)
        seen["headers"] = dict(headers or {})
        client.headers.update(headers or {})
        return client

    monkeypatch.setattr(cli.httpx, "Client", factory)
    monkeypatch.setenv("PURSER_URL", BASE_URL)
    monkeypatch.setenv("PURSER_API_KEY", "admin-key")
    return client, ps, seen


def _state(client, rid: str) -> str:
    resp = client.get(f"/pools/px/resources/{rid}", headers=ADMIN)
    assert resp.status_code == 200, resp.text
    return resp.json()["state"]


def test_status_uses_env_url_and_api_key(service, capsys):
    _, _, seen = service
    cli.main(["status"])
    out = capsys.readouterr().out
    assert seen["base_url"] == BASE_URL
    assert seen["headers"] == {"Authorization": "Bearer admin-key"}
    assert "资源池概览" in out
    assert "px" in out and "fakecheck" in out


def test_status_base_url_flag_overrides_env(service):
    _, _, seen = service
    cli.main(["status", "--base-url", "http://override.local:1"])
    assert seen["base_url"] == "http://override.local:1"


def test_status_failure_exits_nonzero(service, capsys, monkeypatch):
    client, _, _ = service

    def failing_get(url, params=None, **kwargs):
        return _StubResponse(503, {"detail": "backend unavailable"})

    monkeypatch.setattr(client, "get", failing_get)
    with pytest.raises(SystemExit) as exc:
        cli.main(["status"])
    assert exc.value.code == 1
    assert "503" in capsys.readouterr().out


def test_fail_falls_back_to_plain_text_body(capsys):
    class _Client:
        def get(self, url):
            return _StubResponse(500, ValueError("no json"), "upstream down")

    with pytest.raises(SystemExit) as exc:
        cli.cmd_status(type("NS", (), {})(), _Client())
    assert exc.value.code == 1
    assert "upstream down" in capsys.readouterr().out


def test_ls_renders_rows_and_forwards_filters(service, capsys, monkeypatch):
    client, _, _ = service
    captured: dict = {}
    original = client.get

    def spy(url, params=None, **kwargs):
        captured["params"] = dict(params or {})
        return original(url, params=params, **kwargs)

    monkeypatch.setattr(client, "get", spy)
    cli.main(["ls", "px", "--state", "idle", "--tag", "us"])
    out = capsys.readouterr().out
    assert captured["params"] == {"state": "idle", "tag": "us"}
    assert "r-0" in out
    assert "%" in out  # health score rendered as a percentage
    assert "0/1" in out  # used_slots / max_slots
    assert "us" in out


def test_ls_without_filters_sends_no_params(service, capsys, monkeypatch):
    client, _, _ = service
    captured: dict = {}
    original = client.get

    def spy(url, params=None, **kwargs):
        captured["params"] = params
        return original(url, params=params, **kwargs)

    monkeypatch.setattr(client, "get", spy)
    cli.main(["ls", "px"])
    assert captured["params"] == {}
    assert "r-1" in capsys.readouterr().out


def test_validate_ban_suspend_resume_actions(service, capsys):
    client, _, _ = service
    cli.main(["validate", "px", "r-1", "--reason", "smoke"])
    assert "完成" in capsys.readouterr().out
    assert _state(client, "r-1") == "idle"

    cli.main(["ban", "px", "r-0", "--reason", "risk"])
    assert "完成" in capsys.readouterr().out
    assert _state(client, "r-0") == "banned"

    cli.main(["unban", "px", "r-0", "--reason", "cleared"])
    assert _state(client, "r-0") in ("validating", "idle")

    cli.main(["suspend", "px", "r-1", "--reason", "hold"])
    assert _state(client, "r-1") == "suspended"
    cli.main(["resume", "px", "r-1"])
    assert _state(client, "r-1") == "idle"


def test_top_up_action_forwards_amount(service, capsys):
    client, _, _ = service
    cli.main(["top-up", "px", "r-1", "--amount", "2", "--reason", "credit"])
    assert "top-up r-1 完成" in capsys.readouterr().out
    body = client.get("/pools/px/resources/r-1", headers=ADMIN).json()
    assert body["used_count"] == 0 and body["state"] == "idle"


def test_top_up_without_quota_reports_conflict(service, capsys):
    with pytest.raises(SystemExit) as exc:
        cli.main(["top-up", "px", "r-0", "--amount", "1"])
    assert exc.value.code == 1
    assert "409" in capsys.readouterr().out


def test_action_on_missing_resource_exits_nonzero(service, capsys):
    with pytest.raises(SystemExit) as exc:
        cli.main(["ban", "px", "ghost"])
    assert exc.value.code == 1
    assert "error" in capsys.readouterr().out


def test_action_prints_state_transition(service, capsys):
    cli.main(["drain", "px", "r-0", "--reason", "rotate"])
    out = capsys.readouterr().out
    assert "drain r-0 完成" in out and "draining" in out


def test_lease_and_release_roundtrip(service, capsys, monkeypatch):
    client, _, _ = service
    cli.main(["lease", "px", "--consumer", "c1", "--target", "example.com"])
    out = capsys.readouterr().out
    assert "已获取" in out
    assert "提示：使用 `purser release" in out  # hint shown unless --keep
    leases = client.get("/pools/px/leases", headers=ADMIN).json()["leases"]
    assert len(leases) == 1
    token = leases[0]["token"]

    # ``httpx.Client.delete`` accepts ``json=`` but the TestClient helper does
    # not, so translate the body into an explicit request for this call.
    def delete(url, json=None, **kwargs):
        kwargs.pop("json", None)
        return client.request(
            "DELETE",
            url,
            content=_dump(json),
            headers={"Content-Type": "application/json"},
            **kwargs,
        )

    monkeypatch.setattr(client, "delete", delete)
    cli.main(["release", "px", token, "--outcome", "ok", "--used", "2"])
    assert "完成" in capsys.readouterr().out
    assert client.get("/pools/px/leases", headers=ADMIN).json()["count"] == 0


def test_lease_with_keep_suppresses_hint(service, capsys):
    cli.main(["lease", "px", "--consumer", "c2", "--keep"])
    assert "purser release" not in capsys.readouterr().out


def test_lease_failure_exits_nonzero(service, capsys):
    # Unknown pool -> 404 from the REST layer.
    with pytest.raises(SystemExit) as exc:
        cli.main(["lease", "ghost"])
    assert exc.value.code == 1
    assert "error 404" in capsys.readouterr().out


def test_events_stream_prints_typed_lines(service, capsys, monkeypatch):
    client, _, _ = service
    lines = [
        'data: {"type": "lease_acquired", "rid": "r-0", "pool": "px"}',
        "data: not-json",  # unparsable payload is skipped
        ": keepalive",  # no data prefix -> ignored
        'data: {"type": "lease_released", "pool": "px"}',
    ]
    response = _StubResponse(200, {}, lines=lines)
    monkeypatch.setattr(client, "stream", lambda *a, **kw: _StubStream(response))

    cli.main(["events", "px"])
    out = capsys.readouterr().out
    assert "lease_acquired" in out
    assert "r-0" in out
    assert "lease_released" in out and "px" in out


def test_events_stream_ignores_non_dict_payloads(service, monkeypatch, capsys):
    client, _, _ = service
    response = _StubResponse(200, {}, lines=["data: null", "data: []"])
    monkeypatch.setattr(client, "stream", lambda *a, **kw: _StubStream(response))
    cli.main(["events", "px"])
    assert capsys.readouterr().out == ""


def test_events_stream_reports_http_error(service, monkeypatch, capsys):
    client, _, _ = service
    response = _StubResponse(404, {"detail": "pool 'ghost' not found"})
    monkeypatch.setattr(client, "stream", lambda *a, **kw: _StubStream(response))
    with pytest.raises(SystemExit) as exc:
        cli.main(["events", "ghost"])
    assert exc.value.code == 1
    assert "pool 'ghost' not found" in capsys.readouterr().out


def test_import_reads_stdin_lines(service, monkeypatch, capsys):
    client, _, _ = service
    stdin = io.StringIO(
        "# comment\n"
        "1.2.3.4 8080\n"
        "px3 https 9.9.9.9 notaport\n"  # ValueError -> skipped line
        "onlyone\n"  # too few parts -> skipped
    )
    monkeypatch.setattr("sys.stdin", stdin)
    cli.main(["import", "px", "--import-validate", "none"])
    out = capsys.readouterr().out
    assert "导入完成" in out and "新增 1" in out
    assert "跳过无法解析的行" in out
    assert client.get("/pools/px/resources/1.2.3.4:8080", headers=ADMIN).status_code == 200


def test_import_accepts_json_batch_line(service, monkeypatch, capsys):
    client, _, _ = service
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO('[{"id": "j1", "host": "127.0.0.1", "port": 80}]\n'),
    )
    cli.main(["import", "px", "--import-validate", "none"])
    assert "新增 1" in capsys.readouterr().out
    assert client.get("/pools/px/resources/j1", headers=ADMIN).status_code == 200


def test_import_without_records_prints_hint(service, monkeypatch, capsys):
    monkeypatch.setattr("sys.stdin", io.StringIO("# nothing here\n"))
    cli.main(["import", "px"])
    assert "没有可导入的记录" in capsys.readouterr().out


def test_import_missing_file_exits(service, tmp_path, capsys):
    missing = tmp_path / "nope.txt"
    with pytest.raises(SystemExit) as exc:
        cli.main(["import", "px", str(missing)])
    assert exc.value.code == 1
    assert "无法打开文件" in capsys.readouterr().out


def test_import_prints_report_errors(service, monkeypatch, capsys):
    monkeypatch.setattr("sys.stdin", io.StringIO('["not-a-record"]\n'))
    cli.main(["import", "px"])
    out = capsys.readouterr().out
    assert "错误 1" in out
    assert "dict" in out


def test_import_failure_exits_nonzero(service, monkeypatch, capsys):
    monkeypatch.setattr("sys.stdin", io.StringIO("1.2.3.4 8080\n"))
    with pytest.raises(SystemExit) as exc:
        cli.main(["import", "ghost", "--import-validate", "none"])
    assert exc.value.code == 1
    assert "error 404" in capsys.readouterr().out


def test_import_idempotency_key_replays_first_report(service, monkeypatch, capsys):
    monkeypatch.setattr("sys.stdin", io.StringIO("9.9.9.9 1080\n"))
    cli.main(["import", "px", "--idempotency-key", "nightly-1"])
    assert "新增 1" in capsys.readouterr().out
    # Same key -> the cached report is replayed instead of re-importing.
    monkeypatch.setattr("sys.stdin", io.StringIO("9.9.9.9 1080\n"))
    cli.main(["import", "px", "--idempotency-key", "nightly-1"])
    assert "新增 1" in capsys.readouterr().out


def test_serve_dispatches_to_ui_server(monkeypatch):
    captured: dict = {}

    def fake_serve(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr("purser.ui.server.serve", fake_serve)
    cli.main(
        [
            "serve",
            "--host",
            "0.0.0.0",
            "--port",
            "9999",
            "--storage",
            "sqlite",
            "--sqlite-path",
            "purser.db",
        ]
    )
    assert captured["host"] == "0.0.0.0"
    assert captured["port"] == 9999
    assert captured["storage"] == "sqlite"
    assert captured["sqlite_path"] == "purser.db"
    assert captured["coordination"] is None


def test_parser_requires_a_subcommand():
    with pytest.raises(SystemExit):
        cli.main([])


def test_client_env_key_becomes_bearer_header(monkeypatch):
    monkeypatch.setenv("PURSER_API_KEY", "secret-key")
    with cli._client("http://127.0.0.1:1") as client:
        assert client.headers["Authorization"] == "Bearer secret-key"


def test_ok_or_fail_accepts_plain_bodies(capsys):
    cli._ok_or_fail(_StubResponse(200, {"ok": True}), "release tok")
    assert "release tok 完成" in capsys.readouterr().out


def test_ok_or_fail_maps_errors_to_exit(capsys):
    with pytest.raises(SystemExit) as exc:
        cli._ok_or_fail(_StubResponse(409, {"detail": "busy"}), "release tok")
    assert exc.value.code == 1
    assert "busy" in capsys.readouterr().out


def test_import_from_file_keeps_explicit_file_path(tmp_path, capsys):
    path = tmp_path / "proxies.txt"
    path.write_text("1.1.1.1 8080\n", encoding="utf-8")
    args = type(
        "NS",
        (),
        {
            "pool": "px",
            "file": str(path),
            "import_validate": "none",
            "idempotency_key": None,
        },
    )()
    seen: list[dict] = []

    class _Client:
        def post(self, url, json=None, headers=None):
            seen.append(json)
            return _StubResponse(
                200, {"imported": 1, "updated": 0, "skipped": 0, "errors": []}
            )

    cli.cmd_import(args, _Client())
    assert seen[0]["records"][0]["id"] == "1.1.1.1:8080"
    assert "导入完成" in capsys.readouterr().out
    # The file is closed by the handler, so the original content is intact.
    assert path.read_text(encoding="utf-8") == "1.1.1.1 8080\n"
