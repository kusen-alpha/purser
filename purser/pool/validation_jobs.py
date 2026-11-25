"""Bounded store of asynchronous validation jobs."""

from __future__ import annotations

from typing import Any

_TERMINAL = ("completed", "cancelled")


class ValidationJobs:
    """Job records keyed by batch id; oldest terminal jobs are pruned first."""

    def __init__(self) -> None:
        self.jobs: dict[str, dict[str, Any]] = {}

    def __len__(self) -> int:
        return len(self.jobs)

    def get(self, job_id: str) -> dict[str, Any] | None:
        return self.jobs.get(job_id)

    def put(self, job_id: str, job: dict[str, Any]) -> None:
        self.jobs[job_id] = job

    def values(self) -> Any:
        return self.jobs.values()

    def terminal_ids(self) -> list[str]:
        return sorted(
            job_id
            for job_id, job in self.jobs.items()
            if job["status"] in _TERMINAL
        )

    def discard(self, job_id: str) -> None:
        self.jobs.pop(job_id, None)
