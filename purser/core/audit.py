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


@dataclass(slots=True)
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
        self._conn = sqlite3.connect(path, check_same_thread=False)
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

    def insert(self, entry: AuditEntry) -> None:
        self._conn.execute(
            """
            INSERT OR REPLACE INTO audit_log (
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
        self._conn.commit()

    def load(self) -> list[AuditEntry]:
        rows = self._conn.execute("SELECT * FROM audit_log ORDER BY seq").fetchall()
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
        self._seq += 1
        prev = self._entries[-1].digest if self._entries else ""
        entry = AuditEntry(
            seq=self._seq,
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
        entry.digest = entry.compute_digest()
        self._entries.append(entry)
        if self._store is not None:
            self._store.insert(entry)
        return entry

    def close(self) -> None:
        if self._store is not None:
            self._store.close()
            self._store = None

    def verify(self) -> bool:
        """Recompute the chain; returns False if any entry was modified."""

        prev = ""
        for entry in self._entries:
            if entry.prev_hash != prev:
                return False
            if entry.compute_digest() != entry.digest:
                return False
            prev = entry.digest
        return True

    def entries(self, *, limit: int | None = None, after_seq: int = 0) -> list[AuditEntry]:
        items = [e for e in self._entries if e.seq > after_seq]
        if limit is not None:
            items = items[-limit:]
        return items

    def __len__(self) -> int:
        return len(self._entries)
