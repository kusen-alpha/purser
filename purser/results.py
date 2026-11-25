"""Result objects shared by the facades and the public protocols."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

__all__ = ["ImportReport"]


@dataclass(slots=True)
class ImportReport:
    """Outcome of ``import_resources`` / ``generate``."""

    imported: int = 0
    updated: int = 0
    skipped: int = 0
    errors: list[dict[str, Any]] = field(default_factory=list)
    batch_id: str | None = None
    validation: list[dict[str, Any]] = field(default_factory=list)

    @property
    def total(self) -> int:
        return self.imported + self.updated + self.skipped + len(self.errors)

    def to_dict(self) -> dict[str, Any]:
        return {
            "imported": self.imported,
            "updated": self.updated,
            "skipped": self.skipped,
            "errors": self.errors,
            "batch_id": self.batch_id,
            "validation": self.validation,
            "total": self.total,
        }
