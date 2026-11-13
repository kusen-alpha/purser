"""Cross-pool bundle leases (O8): atomic multi-pool acquisition with rollback."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from purser.core.resource import AcquireRequest, Lease
from purser.errors import ConfigError


@dataclass(frozen=True)
class MatchRule:
    """Derive a companion slot's hard tag from an earlier bundle member.

    ``anchor_pool`` names the already-acquired member to read from;
    ``from_field`` picks the value off that resource (typed attribute first,
    then payload key); the companion slot then acquires with ``required_tag``
    set to ``f"{to_tag}:{value}"``.
    """

    anchor_pool: str
    from_field: str
    to_tag: str


@dataclass(frozen=True)
class BundleSlot:
    """One bundle member: a single-pool acquire with optional binding."""

    pool: str
    resource_id: str | None = None  # O12 directed acquire (fixed binding)
    req: AcquireRequest | None = None  # per-pool filters
    match: MatchRule | None = None  # derive companion constraints


@dataclass(frozen=True)
class BundleRequest:
    """Desired combination of resources across pools (O8)."""

    slots: tuple[BundleSlot, ...]
    consumer: str | None = None
    mode: Literal["atomic", "best_effort"] = "atomic"
    order: Literal["scarce_first", "declared"] = "scarce_first"
    timeout: float | None = None
    bundle_id: str | None = None
    max_retries: int = 0
    strict: bool | None = None  # None = follow Purser(bundle_mode=...)
    priority: int = 0  # admission-queue priority when bundle_fairness is on

    def __post_init__(self) -> None:
        if not self.slots:
            raise ConfigError("bundle requires at least one slot")
        if self.mode not in ("atomic", "best_effort"):
            raise ConfigError("bundle mode must be 'atomic' or 'best_effort'")
        if self.order not in ("scarce_first", "declared"):
            raise ConfigError("bundle order must be 'scarce_first' or 'declared'")
        if self.max_retries < 0:
            raise ConfigError("bundle max_retries must be >= 0")
        if not isinstance(self.priority, int) or isinstance(self.priority, bool):
            raise ConfigError("bundle priority must be an integer")
        if self.strict is True and self.mode == "best_effort":
            raise ConfigError("strict (two-phase) bundles cannot use best_effort")
        pools = [slot.pool for slot in self.slots]
        if len(pools) != len(set(pools)):
            raise ConfigError("bundle slots must use distinct pools")
        for slot in self.slots:
            if slot.match is not None and not any(
                other.pool == slot.match.anchor_pool for other in self.slots
            ):
                raise ConfigError(
                    f"bundle match anchor pool {slot.match.anchor_pool!r} "
                    "must be one of the slots"
                )


@dataclass(frozen=True)
class BundleFairness:
    """Cross-pool admission limits for bundles (default off: ``None``)."""

    max_per_consumer: int | None = None
    max_total: int | None = None
    per_pool_headroom: int = 1
    wait: bool = True
    timeout: float | None = None

    def __post_init__(self) -> None:
        if self.per_pool_headroom < 0:
            raise ConfigError("bundle_fairness.per_pool_headroom must be >= 0")
        for name in ("max_per_consumer", "max_total"):
            value = getattr(self, name)
            if value is not None and (not isinstance(value, int) or value < 1):
                raise ConfigError(f"bundle_fairness.{name} must be a positive int or None")
        if self.timeout is not None and self.timeout < 0:
            raise ConfigError("bundle_fairness.timeout must be >= 0 or None")


@dataclass
class BundleLease:
    """Result of ``acquire_bundle``: one lease per member pool."""

    bundle_id: str
    consumer: str | None
    members: dict[str, Lease]  # pool name -> member lease

    def member(self, pool: str) -> Lease:
        return self.members[pool]

    def ids(self) -> dict[str, str]:
        return {pool: lease.resource_id for pool, lease in self.members.items()}


def bundle_request_from_dict(data: dict[str, Any]) -> BundleRequest:
    """Build a :class:`BundleRequest` from a JSON body (REST/console)."""

    slots_raw = data.get("slots")
    if not isinstance(slots_raw, list) or not slots_raw:
        raise ConfigError("bundle requires a non-empty slots list")
    slots: list[BundleSlot] = []
    for item in slots_raw:
        if not isinstance(item, dict):
            raise ConfigError("bundle slot must be an object")
        pool = item.get("pool")
        if not isinstance(pool, str) or not pool:
            raise ConfigError("bundle slot requires a pool name")
        req = None
        req_raw = item.get("req")
        if req_raw is not None:
            if not isinstance(req_raw, dict):
                raise ConfigError("bundle slot req must be an object")
            req = AcquireRequest(**req_raw)
        match = None
        match_raw = item.get("match")
        if match_raw is not None:
            if not isinstance(match_raw, dict):
                raise ConfigError("bundle slot match must be an object")
            for key in ("anchor_pool", "from_field", "to_tag"):
                if not isinstance(match_raw.get(key), str) or not match_raw[key]:
                    raise ConfigError(f"bundle slot match requires {key}")
            match = MatchRule(
                anchor_pool=match_raw["anchor_pool"],
                from_field=match_raw["from_field"],
                to_tag=match_raw["to_tag"],
            )
        resource_id = item.get("resource_id")
        if resource_id is not None and not isinstance(resource_id, str):
            raise ConfigError("bundle slot resource_id must be a string")
        slots.append(
            BundleSlot(
                pool=pool,
                resource_id=resource_id,
                req=req,
                match=match,
            )
        )
    mode = data.get("mode", "atomic")
    order = data.get("order", "scarce_first")
    priority = data.get("priority", 0)
    strict = data.get("strict")
    if mode not in ("atomic", "best_effort"):
        raise ConfigError("bundle mode must be 'atomic' or 'best_effort'")
    if order not in ("scarce_first", "declared"):
        raise ConfigError("bundle order must be 'scarce_first' or 'declared'")
    if not isinstance(priority, int) or isinstance(priority, bool):
        raise ConfigError("bundle priority must be an integer")
    if strict is not None and not isinstance(strict, bool):
        raise ConfigError("bundle strict must be a boolean or null")
    return BundleRequest(
        slots=tuple(slots),
        consumer=data.get("consumer"),
        mode=mode,
        order=order,
        timeout=data.get("timeout"),
        bundle_id=data.get("bundle_id"),
        max_retries=data.get("max_retries", 0),
        strict=strict,
        priority=priority,
    )
