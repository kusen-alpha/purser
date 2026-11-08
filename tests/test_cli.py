"""CLI helpers: import line parsing and file reading."""

from __future__ import annotations

from purser import cli


class _Response:
    status_code = 200

    def json(self):
        return {"imported": 1, "updated": 0, "skipped": 0, "errors": []}


class _Client:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict, dict | None]] = []

    def post(self, url: str, json=None, headers=None):
        self.calls.append((url, json, headers))
        return _Response()


def _args(file, *, idempotency_key=None):
    return type(
        "NS",
        (),
        {
            "pool": "proxies",
            "file": file,
            "import_validate": "none",
            "idempotency_key": idempotency_key,
        },
    )()


def test_import_validates_in_background_by_default():
    args = cli._parser().parse_args(["import", "proxies", "p.txt"])
    assert args.import_validate == "batch"


def test_serve_parser_accepts_persistence_options():
    args = cli._parser().parse_args(
        [
            "serve",
            "--storage",
            "sqlite",
            "--sqlite-path",
            "purser.db",
            "--coordination",
            "redis",
            "--redis-url",
            "redis://localhost:6379",
            "--mysql-url",
            "mysql://user:pass@localhost/db",
            "--audit-path",
            "audit.db",
        ]
    )
    assert args.storage == "sqlite"
    assert args.sqlite_path == "purser.db"
    assert args.coordination == "redis"
    assert args.redis_url == "redis://localhost:6379"
    assert args.mysql_url == "mysql://user:pass@localhost/db"
    assert args.audit_path == "audit.db"


def test_cmd_import_parses_formats(tmp_path):
    f = tmp_path / "proxies.txt"
    f.write_text(
        "# comment\n"
        "1.2.3.4 8080\n"
        "px2 5.6.7.8 3128\n"
        "px3 https 9.9.9.9 443 tag1\n",
        encoding="utf-8",
    )
    client = _Client()
    cli.cmd_import(_args(str(f)), client)
    _, body, _ = client.calls[0]
    records = body["records"]
    assert records[0] == {
        "id": "1.2.3.4:8080",
        "protocol": "http",
        "host": "1.2.3.4",
        "port": 8080,
        "tags": [],
    }
    assert records[1] == {
        "id": "px2",
        "protocol": "http",
        "host": "5.6.7.8",
        "port": 3128,
        "tags": [],
    }
    assert records[2] == {
        "id": "px3",
        "protocol": "https",
        "host": "9.9.9.9",
        "port": 443,
        "tags": ["tag1"],
    }
    assert client.calls[0][2] == {}


def test_cmd_import_only_sends_explicit_idempotency_key(tmp_path):
    f = tmp_path / "proxies.txt"
    f.write_text("1.2.3.4 8080\n", encoding="utf-8")

    implicit = _Client()
    cli.cmd_import(_args(str(f)), implicit)
    assert implicit.calls[0][2] == {}

    explicit = _Client()
    cli.cmd_import(_args(str(f), idempotency_key="nightly-1"), explicit)
    assert explicit.calls[0][2] == {"Idempotency-Key": "nightly-1"}


def test_cmd_lease_and_release_handlers():
    calls: list[tuple[str, str, dict]] = []

    class _LeaseResp:
        status_code = 200

        def json(self):
            return {
                "leases": [
                    {
                        "resource_id": "r-0",
                        "token": "tok",
                        "payload": {"host": "1.1.1.1"},
                        "expires_at": 123.0,
                    }
                ]
            }

    class _OkResp:
        status_code = 200

        def json(self):
            return {"state": "idle"}

    class _Client:
        def post(self, url: str, json=None, headers=None):
            calls.append(("post", url, json))
            return _LeaseResp()

        def delete(self, url: str, json=None, headers=None):
            calls.append(("delete", url, json))
            return _OkResp()

    client = _Client()
    lease_args = type(
        "NS",
        (),
        {
            "pool": "p",
            "consumer": "c",
            "target": None,
            "timeout": 5.0,
            "keep": True,
        },
    )()
    cli.cmd_lease(lease_args, client)
    assert calls[0][0] == "post"
    assert calls[0][2]["consumer"] == "c"
    assert calls[0][2]["include_secrets"] is True

    release_args = type(
        "NS", (), {"pool": "p", "token": "tok", "outcome": "ok", "used": 1}
    )()
    cli.cmd_release(release_args, client)
    assert calls[1][0] == "delete"
    assert calls[1][1].endswith("/leases/tok")
    assert calls[1][2].get("outcome") == "ok"
