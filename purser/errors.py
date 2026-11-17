"""Exception hierarchy for purser.

Every error the library raises derives from :class:`PurserError` so callers can
catch one base type, while the concrete subclasses keep the error surface
sufficiently descriptive for automation.

Each subclass declares two class-level facts:

``code``
    Stable wire code used by the local scheduler IPC.  The scheduler and the
    client both read this attribute, so a new error type only needs to be
    declared once.
``retryable``
    ``True`` when repeating the same operation later can reasonably succeed
    without changing the request or the program state.  Callers can build a
    retry loop from :func:`is_retryable` instead of enumerating types.
"""

from __future__ import annotations

from typing import Any


class PurserError(Exception):
    """Base class for all purser errors."""

    code: str | None = None
    retryable: bool = False

    def details(self) -> dict[str, Any] | None:
        """Structured context for IPC round-trips (``None`` when absent)."""

        return None

    @classmethod
    def from_remote(
        cls,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> PurserError:
        """Rebuild an instance from the scheduler's wire representation."""

        return cls(message)


class ConfigError(PurserError):
    """Invalid pool or library configuration."""

    code = "config_error"


class IsolationConflict(ConfigError):
    """A Purser instance would be shared across owners or processes."""

    code = "isolation_conflict"


class NotStarted(PurserError):
    """Pool runtime is not started; acquire/lease need ``await ps.start()``."""

    code = "not_started"

    def __init__(self, pool: str | None = None, *, operation: str = "acquire") -> None:
        self.pool = pool
        self.operation = operation
        target = f"pool {pool!r}" if pool else "purser"
        super().__init__(
            f"cannot {operation}: {target} is not started; "
            "call `await ps.start()` or use `async with Purser() as ps`"
        )

    def details(self) -> dict[str, Any] | None:
        return {"pool": self.pool, "operation": self.operation}

    @classmethod
    def from_remote(
        cls,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> NotStarted:
        # ``message`` is already the fully formatted text; bypass __init__ so it
        # is not interpreted as a pool name.
        error = cls.__new__(cls)
        PurserError.__init__(error, message)
        data = details or {}
        error.pool = data.get("pool")
        error.operation = str(data.get("operation", "acquire"))
        return error


class PoolNotFound(PurserError):
    """Referenced pool does not exist."""

    code = "pool_not_found"


class PoolExists(PurserError):
    """Pool with the same name already exists."""

    code = "pool_exists"


class ResourceNotFound(PurserError):
    """Referenced resource does not exist."""

    code = "resource_not_found"


class PoolEmpty(PurserError):
    """Pool has no allocatable resources at all; no point waiting."""

    code = "pool_empty"


class PoolExhausted(PurserError):
    """Pool could not satisfy the request before the timeout."""

    code = "pool_exhausted"
    retryable = True


class NoSuitableResource(PurserError):
    """Candidates existed but every request-level check rejected them."""

    code = "no_suitable_resource"

    def __init__(self, target: str | None = None, *, checked: int = 0):
        self.target = target
        self.checked = checked
        detail = f"no suitable resource for target={target!r}" if target else "no suitable resource"
        super().__init__(detail)

    def details(self) -> dict[str, Any] | None:
        return {"target": self.target, "checked": self.checked}

    @classmethod
    def from_remote(
        cls,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> NoSuitableResource:
        data = details or {}
        target = data.get("target")
        try:
            checked = int(data.get("checked", 0))
        except (TypeError, ValueError):
            checked = 0
        return cls(None if target is None else str(target), checked=checked)


class BundleAcquireFailed(PurserError):
    """A cross-pool bundle could not be completed; members were rolled back."""

    code = "bundle_acquire_failed"


class BundleQuotaExceeded(PurserError):
    """Bundle admission rejected (fairness limit) before any member was taken."""

    code = "bundle_quota_exceeded"


class ResourceBusy(PurserError):
    """Directed acquire timed out waiting for the resource."""

    code = "resource_busy"
    retryable = True


class LeaseConflict(PurserError):
    """Token mismatch or optimistic-lock conflict while touching a lease."""

    code = "lease_conflict"


class LeaseExpired(PurserError):
    """Lease already expired; renew or re-acquire."""

    code = "lease_expired"


class LeaseRevokedError(PurserError):
    """Lease was force-revoked by an operator."""

    code = "lease_revoked"


class LeaseLostError(PurserError):
    """Lease vanished (expired or backend reset) while still held."""

    code = "lease_lost"


class LeaseLimitReached(PurserError):
    """Lease reached its use_limit or max_duration."""

    code = "lease_limit"


class BadState(PurserError):
    """Resource is not in a state that permits the requested operation."""

    code = "bad_state"


class InvalidTransition(BadState):
    """The state machine rejected the (state, event) pair."""

    code = "invalid_transition"


class QuotaExceeded(PurserError):
    """A quota (usage, period or target) rejected the operation."""

    code = "quota_exceeded"


class BadPayload(PurserError):
    """Resource payload failed validation or serialization."""

    code = "bad_payload"


class VersionConflict(PurserError):
    """Optimistic-lock conflict: the stored version no longer matches."""

    code = "version_conflict"
    retryable = True


class BackendUnavailable(PurserError):
    """Storage backend could not serve the request (fail-closed)."""

    code = "backend_unavailable"
    retryable = True


def _error_types() -> dict[str, type[PurserError]]:
    """Index every declared subclass by wire code (computed on demand)."""

    found: dict[str, type[PurserError]] = {}
    stack: list[type[PurserError]] = list(PurserError.__subclasses__())
    while stack:
        cls = stack.pop()
        stack.extend(cls.__subclasses__())
        code = getattr(cls, "code", None)
        if isinstance(code, str) and code and code not in found:
            found[code] = cls
    return found


def error_by_code(code: str) -> type[PurserError] | None:
    """Return the concrete class registered for a wire code."""

    return _error_types().get(code)


def is_retryable(exc: BaseException) -> bool:
    """Whether the same operation may succeed later without state changes."""

    return bool(getattr(exc, "retryable", False))
