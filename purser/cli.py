"""Purser CLI: operate a running service (8.3)."""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

import httpx
from rich.console import Console
from rich.table import Table

console = Console()


def _client(base: str) -> httpx.Client:
    headers = {}
    key = os.environ.get("PURSER_API_KEY")
    if key:
        headers["Authorization"] = f"Bearer {key}"
    return httpx.Client(base_url=base, headers=headers, timeout=30.0)


def _fail(resp: httpx.Response) -> None:
    try:
        detail = resp.json().get("detail", resp.text)
    except Exception:
        detail = resp.text
    console.print(f"[bold red]error[/] {resp.status_code}: {detail}")
    raise SystemExit(1)


def cmd_status(args: argparse.Namespace, client: httpx.Client) -> None:
    resp = client.get("/pools")
    if resp.status_code != 200:
        _fail(resp)
    table = Table(title="资源池概览")
    for col in ("池", "类型", "总量", "可分配", "活跃租约", "等待", "隔离", "封禁"):
        table.add_column(col)
    for pool in resp.json().get("pools", []):
        table.add_row(
            pool["name"],
            pool["kind"],
            str(pool["total"]),
            str(pool["allocatable"]),
            str(pool["active_leases"]),
            str(pool["waiting"]),
            str(pool["counts"].get("quarantined", 0)),
            str(pool["counts"].get("banned", 0)),
        )
    console.print(table)


def cmd_ls(args: argparse.Namespace, client: httpx.Client) -> None:
    params = {}
    if args.state:
        params["state"] = args.state
    if args.tag:
        params["tag"] = args.tag
    resp = client.get(f"/pools/{args.pool}/resources", params=params)
    if resp.status_code != 200:
        _fail(resp)
    data = resp.json()
    table = Table(title=f"资源（{args.pool}）")
    for col in ("ID", "状态", "健康", "槽位", "标签"):
        table.add_column(col)
    for r in data.get("resources", []):
        table.add_row(
            r["id"],
            r["state"],
            f"{r['health']['score'] * 100:.0f}%",
            f"{r['used_slots']}/{r['max_slots']}",
            ",".join(r.get("tags") or []),
        )
    console.print(table)


def cmd_validate(args: argparse.Namespace, client: httpx.Client) -> None:
    resp = client.post(
        f"/pools/{args.pool}/resources/{args.rid}/validate", json={"reason": "cli"}
    )
    _ok_or_fail(resp, f"validate {args.rid}")


def cmd_action(args: argparse.Namespace, client: httpx.Client) -> None:
    body = {"reason": args.reason or "cli"}
    if args.amount is not None:
        body["amount"] = args.amount
    resp = client.post(
        f"/pools/{args.pool}/resources/{args.rid}/{args.action}", json=body
    )
    _ok_or_fail(resp, f"{args.action} {args.rid}")


def cmd_lease(args: argparse.Namespace, client: httpx.Client) -> None:
    body = {
        "consumer": args.consumer,
        "target": args.target,
        "timeout": args.timeout,
        "include_secrets": True,
    }
    resp = client.post(f"/pools/{args.pool}/leases", json=body)
    if resp.status_code != 200:
        _fail(resp)
    lease = resp.json()["leases"][0]
    console.print(f"[bold green]已获取[/] {lease['resource_id']}")
    console.print(f"token: {lease['token']}")
    console.print(f"payload: {json.dumps(lease.get('payload'), ensure_ascii=False)}")
    console.print(f"expires_at: {lease['expires_at']:.1f}")
    if not args.keep:
        console.print("[yellow]提示：使用 `purser release <pool> <token>` 归还。[/]")


def cmd_release(args: argparse.Namespace, client: httpx.Client) -> None:
    resp = client.delete(
        f"/pools/{args.pool}/leases/{args.token}",
        json={"outcome": args.outcome, "used": args.used},
    )
    _ok_or_fail(resp, f"release {args.token}")


def cmd_import(args: argparse.Namespace, client: httpx.Client) -> None:
    records: list[dict[str, Any]] = []
    source = sys.stdin
    if args.file:
        try:
            source = open(args.file, encoding="utf-8")
        except OSError as exc:
            console.print(f"[bold red]无法打开文件[/] {args.file}: {exc}")
            raise SystemExit(1) from None
    try:
        for line in source:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("["):
                records = json.loads(line)
                break
            parts = line.split()
            try:
                if len(parts) >= 4:
                    rid, protocol, host = parts[0], parts[1], parts[2]
                    port = int(parts[3])
                    tags = parts[4:]
                elif len(parts) == 3:
                    rid, host, port = parts[0], parts[1], int(parts[2])
                    protocol, tags = "http", []
                elif len(parts) == 2:
                    host, port = parts[0], int(parts[1])
                    rid, protocol, tags = f"{host}:{port}", "http", []
                else:
                    continue
            except ValueError:
                console.print(f"[yellow]跳过无法解析的行: {line}[/]")
                continue
            records.append(
                {
                    "id": rid,
                    "protocol": protocol,
                    "host": host,
                    "port": port,
                    "tags": tags,
                }
            )
    finally:
        if source is not sys.stdin:
            source.close()
    if not records:
        console.print("[yellow]没有可导入的记录[/]")
        return
    headers = {}
    if args.idempotency_key:
        headers["Idempotency-Key"] = args.idempotency_key
    resp = client.post(
        f"/pools/{args.pool}/resources",
        json={"records": records, "import_validate": args.import_validate},
        headers=headers,
    )
    if resp.status_code != 200:
        _fail(resp)
    rep = resp.json()
    console.print(
        f"[bold green]导入完成[/] 新增 {rep['imported']} · 更新 {rep['updated']} · "
        f"跳过 {rep['skipped']} · 错误 {len(rep['errors'])}"
    )
    for error in rep["errors"]:
        console.print(f"  [red]{error}[/]")


def cmd_events(args: argparse.Namespace, client: httpx.Client) -> None:
    with client.stream("GET", f"/pools/{args.pool}/events", params={"after_seq": 0}) as resp:
        if resp.status_code != 200:
            _fail(resp)
        for line in resp.iter_lines():
            if line.startswith("data: "):
                try:
                    ev = json.loads(line[6:])
                    console.print(f"[cyan]{ev['type']}[/] {ev.get('rid') or ''} {ev.get('pool')}")
                except Exception:
                    pass


def _ok_or_fail(resp: httpx.Response, what: str) -> None:
    if resp.status_code != 200:
        _fail(resp)
    body = resp.json()
    state = body.get("state")
    console.print(f"[bold green]{what} 完成[/]" + (f" → {state}" if state else ""))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="purser", description="通用采集资源调度库 CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    def add_common(p: argparse.ArgumentParser) -> None:
        p.add_argument("--base-url", default=os.environ.get("PURSER_URL", "http://127.0.0.1:8000"))

    p = sub.add_parser("status", help="池概览")
    add_common(p)
    p.set_defaults(fn=cmd_status)

    p = sub.add_parser("ls", help="资源列表")
    add_common(p)
    p.add_argument("pool")
    p.add_argument("--state")
    p.add_argument("--tag")
    p.set_defaults(fn=cmd_ls)

    for name in ("validate", "ban", "unban", "suspend", "resume", "top-up", "drain"):
        p = sub.add_parser(name, help=f"{name} 资源")
        add_common(p)
        p.add_argument("pool")
        p.add_argument("rid")
        p.add_argument("--reason")
        p.add_argument("--amount", type=int)
        p.add_argument("--action", default=name)
        p.set_defaults(fn=cmd_action)

    p = sub.add_parser("lease", help="获取租约")
    add_common(p)
    p.add_argument("pool")
    p.add_argument("--consumer")
    p.add_argument("--target")
    p.add_argument("--timeout", type=float, default=10.0)
    p.add_argument("--keep", action="store_true")
    p.set_defaults(fn=cmd_lease)

    p = sub.add_parser("release", help="归还租约")
    add_common(p)
    p.add_argument("pool")
    p.add_argument("token")
    p.add_argument("--outcome", default="ok")
    p.add_argument("--used", type=int)
    p.set_defaults(fn=cmd_release)

    p = sub.add_parser("import", help="批量导入")
    add_common(p)
    p.add_argument("pool")
    p.add_argument("file", nargs="?")
    p.add_argument(
        "--import-validate",
        default="batch",
        choices=("none", "on_import", "batch"),
        help="导入后验活策略；batch 交给后台任务（默认），none 保持 NEW 直到手动验活",
    )
    p.add_argument(
        "--idempotency-key",
        help="重复提交时复用首次结果；默认每次真正导入",
    )
    p.set_defaults(fn=cmd_import)

    p = sub.add_parser("events", help="订阅事件流")
    add_common(p)
    p.add_argument("pool")
    p.set_defaults(fn=cmd_events)

    p = sub.add_parser("serve", help="启动服务")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8000)
    p.add_argument("--storage", choices=["memory", "sqlite", "mysql"], default=None)
    p.add_argument("--sqlite-path", default=None)
    p.add_argument("--coordination", choices=["memory", "redis"], default=None)
    p.add_argument("--redis-url", default=None)
    p.add_argument("--mysql-url", default=None)
    p.add_argument("--audit-path", default=None)
    p.set_defaults(fn=lambda args, _client: _serve(args))
    return parser


def _serve(args: argparse.Namespace) -> None:
    from purser.ui.server import serve

    serve(
        host=args.host,
        port=args.port,
        storage=args.storage,
        sqlite_path=args.sqlite_path,
        coordination=args.coordination,
        redis_url=args.redis_url,
        mysql_url=args.mysql_url,
        audit_path=args.audit_path,
    )


def main(argv: list[str] | None = None) -> None:
    args = _parser().parse_args(argv)
    if args.command == "serve":
        args.fn(args, None)
        return
    client = _client(args.base_url)
    args.fn(args, client)


if __name__ == "__main__":
    main()
