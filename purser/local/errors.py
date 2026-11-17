"""Errors raised by the local IPC scheduler client."""

from __future__ import annotations

from typing import Any

from purser.errors import PurserError, error_by_code


class SchedulerUnavailable(PurserError):
    """The local scheduler process is not reachable."""

    code = "scheduler_unavailable"
    retryable = True


class SchedulerTimeout(PurserError):
    """The scheduler did not answer before the caller's deadline."""

    code = "scheduler_timeout"
    retryable = True


class SchedulerProtocolError(PurserError):
    """The peer sent an invalid or mismatched protocol message."""

    code = "scheduler_protocol"


_LOCAL_TYPES: dict[str, type[PurserError]] = {
    SchedulerUnavailable.code: SchedulerUnavailable,
    SchedulerTimeout.code: SchedulerTimeout,
    SchedulerProtocolError.code: SchedulerProtocolError,
}


def remote_error(
    code: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> PurserError:
    """Rebuild a library error from the scheduler's structured error code."""

    error_type = _LOCAL_TYPES.get(code) or error_by_code(code)
    if error_type is None:
        error = PurserError(message)
        error.code = code
        return error
    return error_type.from_remote(message, details)
