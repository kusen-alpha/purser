"""Exception hierarchy for purser.

Every error the library raises derives from :class:`PurserError` so callers can
catch one base type, while the concrete subclasses keep the error surface
sufficiently descriptive for automation.
"""

from __future__ import annotations


class PurserError(Exception):
    """Base class for all purser errors."""

    code: str | None = None


class ConfigError(PurserError):
    """Invalid pool or library configuration."""


class IsolationConflict(ConfigError):
    """A Purser instance would be shared across owners or processes."""


class NotStarted(PurserError):
    """Pool runtime is not started; acquire/lease need ``await ps.start()``."""

    def __init__(self, pool: str | None = None, *, operation: str = "acquire") -> None:
        self.pool = pool
        self.operation = operation
        target = f"pool {pool!r}" if pool else "purser"
        super().__init__(
            f"cannot {operation}: {target} is not started; "
            "call `await ps.start()` or use `async with Purser() as ps`"
        )


class PoolNotFound(PurserError):
    """Referenced pool does not exist."""


class PoolExists(PurserError):
    """Pool with the same name already exists."""


class ResourceNotFound(PurserError):
    """Referenced resource does not exist."""


class PoolEmpty(PurserError):
    """Pool has no allocatable resources at all; no point waiting."""


class PoolExhausted(PurserError):
    """Pool could not satisfy the request before the timeout."""


class NoSuitableResource(PurserError):
    """Candidates existed but every request-level check rejected them."""

    def __init__(self, target: str | None = None, *, checked: int = 0):
        self.target = target
        self.checked = checked
        detail = f"no suitable resource for target={target!r}" if target else "no suitable resource"
        super().__init__(detail)


class BundleAcquireFailed(PurserError):
    """A cross-pool bundle could not be completed; members were rolled back."""


class BundleQuotaExceeded(PurserError):
    """Bundle admission rejected (fairness limit) before any member was taken."""


class ResourceBusy(PurserError):
    """Directed acquire timed out waiting for the resource."""


class LeaseConflict(PurserError):
    """Token mismatch or optimistic-lock conflict while touching a lease."""


class LeaseExpired(PurserError):
    """Lease already expired; renew or re-acquire."""


class LeaseRevokedError(PurserError):
    """Lease was force-revoked by an operator."""


class LeaseLostError(PurserError):
    """Lease vanished (expired or backend reset) while still held."""


class LeaseLimitReached(PurserError):
    """Lease reached its use_limit or max_duration."""


class BadState(PurserError):
    """Resource is not in a state that permits the requested operation."""


class InvalidTransition(BadState):
    """The state machine rejected the (state, event) pair."""


class QuotaExceeded(PurserError):
    """A quota (usage, period or target) rejected the operation."""


class BadPayload(PurserError):
    """Resource payload failed validation or serialization."""


class VersionConflict(PurserError):
    """Optimistic-lock conflict: the stored version no longer matches."""


class BackendUnavailable(PurserError):
    """Storage backend could not serve the request (fail-closed)."""
