"""Errors raised by the local IPC scheduler client."""

from __future__ import annotations

from purser.errors import (
    BadPayload,
    BadState,
    ConfigError,
    InvalidTransition,
    LeaseConflict,
    LeaseExpired,
    LeaseLimitReached,
    LeaseLostError,
    LeaseRevokedError,
    NoSuitableResource,
    NotStarted,
    PoolEmpty,
    PoolExhausted,
    PoolExists,
    PoolNotFound,
    PurserError,
    ResourceNotFound,
    VersionConflict,
)


class SchedulerUnavailable(PurserError):
    """The local scheduler process is not reachable."""


class SchedulerTimeout(PurserError):
    """The scheduler did not answer before the caller's deadline."""


class SchedulerProtocolError(PurserError):
    """The peer sent an invalid or mismatched protocol message."""


_ERROR_TYPES: dict[str, type[PurserError]] = {
    "bad_payload": BadPayload,
    "bad_state": BadState,
    "config_error": ConfigError,
    "invalid_transition": InvalidTransition,
    "lease_conflict": LeaseConflict,
    "lease_expired": LeaseExpired,
    "lease_limit": LeaseLimitReached,
    "lease_lost": LeaseLostError,
    "lease_revoked": LeaseRevokedError,
    "no_suitable_resource": NoSuitableResource,
    "not_started": NotStarted,
    "pool_empty": PoolEmpty,
    "pool_exhausted": PoolExhausted,
    "pool_exists": PoolExists,
    "pool_not_found": PoolNotFound,
    "resource_not_found": ResourceNotFound,
    "scheduler_protocol": SchedulerProtocolError,
    "scheduler_timeout": SchedulerTimeout,
    "scheduler_unavailable": SchedulerUnavailable,
    "version_conflict": VersionConflict,
}


def remote_error(code: str, message: str) -> PurserError:
    """Rebuild a library error from the scheduler's structured error code."""

    error_type = _ERROR_TYPES.get(code)
    if error_type is None or error_type is NotStarted:
        error = PurserError(message)
        error.code = code
        return error
    if error_type is NoSuitableResource:
        return NoSuitableResource(message)
    return error_type(message)
