"""Append-only audit log with a SHA-256 hash chain (O9 / D2).

In-memory by default; pass ``sqlite_path`` to persist every entry and to
reload the chain on restart so ``verify()`` still covers the full history.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from dataclasses import dataclass, field
from typing import Any


@dataclass
class AuditEntry:
    seq: int
    ts: float
    operator: str
    action: str
    pool: str | None = None
    rid: str | None = None
    reason: str | None = None
    trace_id: str | None = None
    detail: dict[str, Any] = field(default_factory=dict)
    prev_hash: str = ""
    digest: str = ""

    def _body(self) -> str:
        return json.dumps(
            {
                "seq": self.seq,
                "ts": self.ts,
                "operator": self.operator,
                "action": self.action,
                "pool": self.pool,
                "rid": self.rid,
                "reason": self.reason,
                "trace_id": self.trace_id,
                "detail": self.detail,
                "prev_hash": self.prev_hash,
            },
            sort_keys=True,
            separators=(",", ":"),
        )

    def compute_digest(self) -> str:
        return hashlib.sha256(self._body().encode("utf-8")).hexdigest()


class _AuditStore:
    """SQLite persistence for the audit chain."""

    def __init__(self, path: str) -> None:
        self._conn = sqlite3.connect(
            path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA busy_timeout = 5000")
        if path != ":memory:":
            self._conn.execute("PRAGMA journal_mode = WAL")
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS audit_log (
                seq INTEGER PRIMARY KEY,
                ts REAL NOT NULL,
                operator TEXT NOT NULL,
                action TEXT NOT NULL,
                pool TEXT,
                rid TEXT,
                reason TEXT,
                trace_id TEXT,
                detail TEXT NOT NULL DEFAULT '{}',
                prev_hash TEXT NOT NULL DEFAULT '',
                digest TEXT NOT NULL DEFAULT ''
            )
            """
        )

    def append(self, entry: AuditEntry) -> AuditEntry:
        """Allocate the next sequence and digest inside an immediate
        transaction, so concurrent writers cannot fork the chain."""

        for attempt in range(20):
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                row = self._conn.execute(
                    "SELECT seq, digest FROM audit_log ORDER BY seq DESC LIMIT 1"
                ).fetchone()
                entry.seq = row["seq"] + 1 if row is not None else 1
                entry.prev_hash = row["digest"] if row is not None else ""
                entry.digest = entry.compute_digest()
                self._conn.execute(
                    """
                    INSERT INTO audit_log (
                        seq, ts, operator, action, pool, rid, reason, trace_id,
                        detail, prev_hash, digest
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        entry.seq,
                        entry.ts,
                        entry.operator,
                        entry.action,
                        entry.pool,
                        entry.rid,
                        entry.reason,
                        entry.trace_id,
                        json.dumps(entry.detail, sort_keys=True),
                        entry.prev_hash,
                        entry.digest,
                    ),
                )
                self._conn.execute("COMMIT")
                return entry
            except sqlite3.OperationalError as exc:
                try:
                    self._conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                if "locked" not in str(exc).lower() or attempt == 19:
                    raise
                time.sleep(0.05 * (attempt + 1))
            except sqlite3.IntegrityError:
                try:
                    self._conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                if attempt == 19:
                    raise
                time.sleep(0.01)
        raise AssertionError("unreachable")  # pragma: no cover

    def last(self) -> tuple[int, str] | None:
        row = self._conn.execute(
            "SELECT seq, digest FROM audit_log ORDER BY seq DESC LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        return int(row["seq"]), str(row["digest"])

    def load(self, after_seq: int = 0) -> list[AuditEntry]:
        rows = self._conn.execute(
            "SELECT * FROM audit_log WHERE seq > ? ORDER BY seq", (after_seq,)
        ).fetchall()
        return [
            AuditEntry(
                seq=row["seq"],
                ts=row["ts"],
                operator=row["operator"],
                action=row["action"],
                pool=row["pool"],
                rid=row["rid"],
                reason=row["reason"],
                trace_id=row["trace_id"],
                detail=json.loads(row["detail"]) if row["detail"] else {},
                prev_hash=row["prev_hash"] or "",
                digest=row["digest"] or "",
            )
            for row in rows
        ]

    def close(self) -> None:
        try:
            self._conn.close()
        except sqlite3.Error:
            pass


class AuditLog:
    """Append-only store.  Each entry chains to the previous digest, so
    tampering is detectable via :meth:`verify`.
    """

    def __init__(self, sqlite_path: str | None = None) -> None:
        self._entries: list[AuditEntry] = []
        self._seq = 0
        self._store = _AuditStore(sqlite_path) if sqlite_path else None
        if self._store is not None:
            self._entries = self._store.load()
            self._seq = max((entry.seq for entry in self._entries), default=0)

    def _sync_from_store(self) -> None:
        """Adopt entries another writer appended since our last access."""

        if self._store is None:
            return
        last = self._store.last()
        if last is None or last[0] <= self._seq:
            return
        self._entries.extend(self._store.load(after_seq=self._seq))
        self._seq = self._entries[-1].seq

    def record(
        self,
        *,
        operator: str,
        action: str,
        pool: str | None = None,
        rid: str | None = None,
        reason: str | None = None,
        trace_id: str | None = None,
        detail: dict[str, Any] | None = None,
    ) -> AuditEntry:
        self._sync_from_store()
        prev = self._entries[-1].digest if self._entries else ""
        entry = AuditEntry(
            seq=self._seq + 1,
            ts=time.time(),
            operator=operator,
            action=action,
            pool=pool,
            rid=rid,
            reason=reason,
            trace_id=trace_id,
            detail=detail or {},
            prev_hash=prev,
        )
        if self._store is not None:
            self._store.append(entry)
        else:
            entry.digest = entry.compute_digest()
        self._seq = entry.seq
        self._entries.append(entry)
        return entry

    def close(self) -> None:
        if self._store is not None:
            self._store.close()
            self._store = None

    @staticmethod
    def _verify_chain(entries: list[AuditEntry]) -> bool:
        prev = ""
        for entry in entries:
            if entry.prev_hash != prev:
                return False
            if entry.compute_digest() != entry.digest:
                return False
            prev = entry.digest
        return True

    def verify(self) -> bool:
        """Recompute the chain; returns False if any entry was modified.

        When persisted, the on-disk chain is verified too so tampering by a
        process that does not share this instance's memory is still caught."""

        self._sync_from_store()
        if not self._verify_chain(self._entries):
            return False
        if self._store is not None:
            disk = self._store.load()
            if len(disk) != len(self._entries):
                return False
            return self._verify_chain(disk)
        return True

    def entries(self, *, limit: int | None = None, after_seq: int = 0) -> list[AuditEntry]:
        self._sync_from_store()
        items = [e for e in self._entries if e.seq > after_seq]
        if limit is not None:
            items = items[-limit:]
        return items

    def __len__(self) -> int:
        self._sync_from_store()
        return len(self._entries)
