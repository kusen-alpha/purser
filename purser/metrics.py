"""Lightweight counters and histograms with Prometheus text rendering (S14)."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class Metrics:
    counters: dict[str, int] = field(default_factory=lambda: OrderedDict())
    histograms: dict[str, list[float]] = field(default_factory=lambda: OrderedDict())

    def inc(self, name: str, delta: int = 1) -> None:
        self.counters[name] = self.counters.get(name, 0) + delta

    def observe(self, name: str, value: float) -> None:
        self.histograms.setdefault(name, []).append(value)
        if len(self.histograms[name]) > 10_000:  # bounded memory
            self.histograms[name] = self.histograms[name][-10_000:]

    def snapshot(self) -> dict[str, Any]:
        out: dict[str, Any] = {"counters": dict(self.counters)}
        out["histograms"] = {
            name: {
                "count": len(values),
                "sum": round(sum(values), 3),
                "min": round(min(values), 3) if values else None,
                "max": round(max(values), 3) if values else None,
                "avg": round(sum(values) / len(values), 3) if values else None,
            }
            for name, values in self.histograms.items()
        }
        return out

    def render_prometheus(self) -> str:
        lines = ["# HELP purser_metric Generic purser metric", "# TYPE purser_counter counter"]
        for name, value in self.counters.items():
            lines.append(f"purser_{name.replace('-', '_')} {value}")
        lines.append("# TYPE purser_histogram histogram")
        for name, values in self.histograms.items():
            safe = name.replace("-", "_")
            lines.append(f"purser_{safe}_count {len(values)}")
            lines.append(f"purser_{safe}_sum {sum(values):.3f}")
        return "\n".join(lines)
