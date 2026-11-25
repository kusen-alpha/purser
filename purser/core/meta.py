"""Typed, dict-compatible accessor for framework-owned resource meta keys."""

from __future__ import annotations

from typing import Any


class ResourceMeta(dict[str, Any]):
    """``dict`` subclass adding typed properties over scheduling keys.

    Storage is unchanged: keys live in the same bag, survive ``meta_patch`` /
    ``meta_clear`` untouched and serialize like any other entry.
    """

    __slots__ = ()

    @property
    def priority(self) -> int:
        value = self.get("priority")
        return 0 if value is None else int(value)

    @priority.setter
    def priority(self, value: int | None) -> None:
        if value is None:
            self.pop("priority", None)
        else:
            self["priority"] = int(value)

    @property
    def preference(self) -> float:
        value = self.get("preference")
        return 0.0 if value is None else float(value)

    @preference.setter
    def preference(self, value: float | None) -> None:
        if value is None:
            self.pop("preference", None)
        else:
            self["preference"] = float(value)

    @property
    def group(self) -> Any:
        """Group label looked up in ``group_priority``; returned uncoerced."""

        return self.get("group")

    @group.setter
    def group(self, value: Any) -> None:
        if value is None:
            self.pop("group", None)
        else:
            self["group"] = value

    @property
    def quarantine_until(self) -> float | None:
        value = self.get("quarantine_until")
        return None if value is None else float(value)

    @quarantine_until.setter
    def quarantine_until(self, value: float | None) -> None:
        if value is None:
            self.pop("quarantine_until", None)
        else:
            self["quarantine_until"] = float(value)
