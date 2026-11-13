"""ResourceAdapter base class and registry (4.4 / 4.5 / S13)."""

from __future__ import annotations

import hashlib
import inspect
import json
from collections.abc import Awaitable
from dataclasses import dataclass, fields
from typing import Any, Callable, ClassVar, TypeVar

from purser.core.health import HealthReport
from purser.core.resource import (
    AcquireRequest,
    CheckVerdict,
    Lease,
    Resource,
    UsageOutcome,
)
from purser.core.state import ResourceState
from purser.errors import ConfigError

ResourceT = TypeVar("ResourceT", bound=Resource)


@dataclass
class Validator:
    """One named validation check declared on a ResourceAdapter."""

    name: str
    fn: Callable[..., Awaitable[HealthReport]]
    both: bool = False


@dataclass(frozen=True)
class FieldSpec:
    """Public schema for one typed payload field (UI/CLI consume this)."""

    name: str
    coerce: Callable[[Any], Any] | None = None
    required: bool = False
    secret: bool = False
    default: Any = None
    doc: str = ""


@dataclass(frozen=True)
class ValidatorSpec:
    """Public schema for one named validation check."""

    name: str
    both: bool = False


@dataclass(frozen=True)
class AdapterDefaults:
    """Pool-level defaults a ResourceAdapter contributes at pool creation."""

    max_slots: int | None = None
    secret_fields: tuple[str, ...] = ()


# Marker registry: decorated function -> Validator, consumed at class creation
# by ``ResourceAdapter.__init_subclass__``.  Unlike ``_payload_field_map`` there is
# no per-instance state here; checks are plain bound method calls.
_VALIDATOR_MARKS: dict[Callable[..., Awaitable[HealthReport]], Validator] = {}


def validator(
    name: str | None = None, *, both: bool = False
) -> Callable[
    [Callable[..., Awaitable[HealthReport]]],
    Callable[..., Awaitable[HealthReport]],
]:
    """Declare a named validation check on a ResourceAdapter.

    ``both=True`` checks run as part of lifecycle validation, before the
    adapter's own ``validate()``; a failing check short-circuits and follows the
    normal quarantine flow.  ``both=False`` checks only run on demand by name
    and never touch health or state.  ``name`` defaults to the method name.
    All checks are listed under ``GET /adapters`` so the console can invoke any
    of them by name.
    """

    def decorate(
        fn: Callable[..., Awaitable[HealthReport]],
    ) -> Callable[..., Awaitable[HealthReport]]:
        check_name = name or getattr(fn, "__name__", None)
        if not check_name:
            raise ConfigError("validator requires a non-empty name")
        if not inspect.iscoroutinefunction(fn):
            raise TypeError(f"validator {check_name!r} must be an async method")
        _VALIDATOR_MARKS[fn] = Validator(name=check_name, fn=fn, both=both)
        return fn

    return decorate


def _collect_validators(cls: type[ResourceAdapter]) -> dict[str, Validator]:
    """Merge named validators down the MRO, nearer classes overriding by name."""

    merged: dict[str, Validator] = {}
    for base in reversed(cls.__mro__[1:]):
        inherited = base.__dict__.get("_validators")
        if inherited:
            merged.update(inherited)
    declared: dict[str, Validator] = {}
    for value in cls.__dict__.values():
        spec = _VALIDATOR_MARKS.get(value)
        if spec is None:
            continue
        if spec.name in declared:
            raise ConfigError(
                f"duplicate validator name {spec.name!r} on {cls.__name__}"
            )
        declared[spec.name] = spec
    merged.update(declared)
    return merged


def rehydrate(resource: Resource, cls: type[ResourceT]) -> ResourceT:
    """Rebuild a generic stored Resource as a typed subclass."""

    values = {
        f.name: getattr(resource, f.name)
        for f in fields(Resource)
        if f.name != "_payload"
    }
    return cls(payload=resource.payload, **values)


class ResourceAdapter:
    """One plugin per resource family.  The kernel has zero knowledge of
    proxies, accounts etc.; everything type-specific flows through here.
    """

    kind: str = ""
    default_concurrency: int = 1
    resource_class: ClassVar[type[Resource]] = Resource
    secret_fields: tuple[str, ...] = ()
    _validators: ClassVar[dict[str, Validator]] = {}

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Derive ``secret_fields`` from declared ``PayloadField(secret=True)``.

        Declare ``resource_class`` in the class body; assigning it after class
        creation is not observed.  An explicit ``secret_fields`` always wins.
        """

        super().__init_subclass__(**kwargs)
        cls._validators = _collect_validators(cls)
        if "secret_fields" in cls.__dict__:
            return
        resource_class = getattr(cls, "resource_class", Resource)
        cls.secret_fields = tuple(
            spec.name
            for spec in getattr(resource_class, "_payload_field_map", {}).values()
            if spec.secret
        )

    async def run_validators(self, resource: Resource) -> HealthReport:
        """Run every ``both=True`` named check in declaration order, serially.

        Short-circuits on the first failure; the failing check's name is
        prefixed onto the error so callers can tell which check failed.
        """

        for check in self._validators.values():
            if not check.both:
                continue
            report = await check.fn(self, resource)
            if report.alive:
                continue
            detail = report.error or "check failed"
            return HealthReport(
                alive=False,
                latency_ms=report.latency_ms,
                error=f"{check.name}: {detail}",
                action=report.action,
            )
        return HealthReport.ok(latency_ms=1.0)

    async def validate_with_checks(self, resource: Resource) -> HealthReport:
        """Lifecycle validation composed with auto-run named checks.

        Auto checks run first; a failing check short-circuits into the same
        quarantine flow as ``validate()`` failing.  Types without auto checks
        behave exactly as before.
        """

        report = await self.run_validators(resource)
        if not report.alive:
            return report
        return await self.validate(resource)

    async def run_validator(self, resource: Resource, name: str) -> HealthReport:
        """Run one named check on demand; never writes health or state."""

        try:
            check = self._validators[name]
        except KeyError:
            raise ConfigError(
                f"unknown validator {name!r} on resource adapter {self.kind!r}"
            ) from None
        report = await check.fn(self, resource)
        if not report.alive and report.error is None:
            return HealthReport(
                alive=False,
                latency_ms=report.latency_ms,
                error=f"{check.name}: check failed",
                action=report.action,
            )
        return report

    async def generate(self, spec: dict[str, Any]) -> Resource:
        """Default spec -> resource mapping driven by ``resource_class``.

        Each ``spec`` key matching a declared field becomes a typed attribute;
        unknown keys land in the custom payload.  ``id`` and ``tags`` are
        lifted from the spec, ``adopt`` applies coercion, defaults and required
        checks, and :meth:`derive_id` supplies a stable id when the spec omits
        one.  Override when the mapping is not one-to-one or the id must be
        derived differently.
        """

        spec = dict(spec)
        raw_tags = spec.get("tags", ()) or ()
        if isinstance(raw_tags, str):
            raise TypeError("spec['tags'] must be an iterable of strings, not a str")
        rid = spec.get("id")
        rid = str(rid).strip() if rid is not None else ""
        rid = rid or self.derive_id(spec)
        tags = frozenset(raw_tags)
        spec.pop("id", None)
        spec.pop("tags", None)
        typed: dict[str, Any] = {}
        for name in getattr(self.resource_class, "_payload_field_map", {}):
            if name in spec:
                typed[name] = spec.pop(name)
        return self.adopt(
            self.resource_class(
                id=rid,
                kind=self.kind,
                tags=tags,
                payload=spec or None,
                **typed,
            )
        )

    def derive_id(self, spec: dict[str, Any]) -> str:
        """Stable id for a spec that carries none (deterministic across runs)."""

        canonical = json.dumps(spec, sort_keys=True, ensure_ascii=False, default=str)
        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:12]
        return f"{self.kind}-{digest}"

    def _typed(self, resource: Resource) -> Resource:
        if self.resource_class is Resource or isinstance(
            resource, self.resource_class
        ):
            return resource
        return rehydrate(resource, self.resource_class)

    def adopt(self, resource: Resource) -> Resource:
        """Validate and normalize a caller-built Resource on the import path.

        Dict records go through :meth:`generate`; this is the equivalent
        boundary for ``Resource`` instances handed to ``import_resources``.
        Typed fields are rehydrated and required ones enforced here.
        """

        if resource.kind != self.kind:
            raise ValueError(
                f"resource kind {resource.kind!r} does not match type {self.kind!r}"
            )
        if not resource.id:
            raise ValueError("resource id must be non-empty")
        if not isinstance(resource.payload, dict):
            raise ValueError("resource payload must be a dict")
        resource.tags = frozenset(resource.tags or ())
        resource.meta = dict(resource.meta or {})
        typed = self._typed(resource)
        typed.normalize_payload(require=True)
        return typed

    def hydrate(self, resource: Resource) -> Resource:
        """Return the typed view of a stored resource (read boundary).

        Storage is generic; the declared ``resource_class`` rebuilds the
        typed subclass so ``get``/``require``/``query`` hand callers the
        same shape they imported.
        """

        typed = self._typed(resource)
        typed.normalize_payload()
        return typed

    async def validate(self, resource: Resource) -> HealthReport:
        """Lifecycle validation: is the resource alive right now (5.3)?

        Default trusts the resource (always alive); override with a real
        probe when the resource can go stale or get banned.
        """

        return HealthReport.ok(latency_ms=1.0)

    async def on_request_check(
        self, resource: Resource, req: AcquireRequest
    ) -> CheckVerdict:
        """Request-level check: can this resource serve *this* request (5.8)?
        Default is a pass-through; subclasses return FAIL/BAN as needed."""

        return CheckVerdict.PASS

    async def on_release(self, resource: Resource, outcome: UsageOutcome) -> None:  # noqa: B027
        """Called after a lease returns with the reported usage outcome."""

    async def on_acquire(self, resource: Resource, lease: Lease) -> None:  # noqa: B027
        """Called right after a lease is granted (post-commit)."""

    async def on_renew(  # noqa: B027
        self, resource: Resource | None, lease: Lease
    ) -> None:
        """Called on each successful renewal/heartbeat.

        ``resource`` is the acquire-time snapshot and may be ``None`` for
        leases owned by another instance sharing the coordination plane.
        """

    async def before_release(  # noqa: B027
        self, resource: Resource, lease: Lease
    ) -> None:
        """Called before the lease is returned while the holder still owns it."""

    async def on_revoke(  # noqa: B027
        self, resource: Resource, lease: Lease, reason: str
    ) -> None:
        """Called after an admin force-revokes this lease."""

    async def on_lease_lost(self, resource: Resource, lease: Lease) -> None:  # noqa: B027
        """Called when the kernel gives up tracking a lease.

        Fires on heartbeat rejection or a vanished coordination token.
        """

    async def switch(self, resource: Resource) -> Resource | None:  # noqa: B027
        """Produce a replacement for ``resource``; ``None`` means no switch.

        Reserved extension point.  Subclasses either mutate ``resource`` in
        place and return it, or return a brand-new Resource.  The pool then
        persists the result: the same id updates the row in place, a new id
        imports the replacement and archives the old row.
        """

        return None

    async def on_state_changed(  # noqa: B027
        self,
        resource: Resource,
        old: ResourceState,
        new: ResourceState,
        reason: str,
    ) -> None:
        """Read-only observer hook; never alters the flow."""

    async def on_error(self, context: str, error: Exception) -> None:  # noqa: B027
        """Fires when a hook raises; never propagates into the kernel."""

    def describe(self, resource: Resource) -> str:
        """Masked human-readable summary for UI/logs (A1)."""

        return f"{self.kind}:{resource.id} [{resource.state.value}]"

    def fields(self) -> list[FieldSpec]:
        """Declared typed payload fields in declaration order (public schema)."""

        return [
            FieldSpec(
                name=spec.name,
                coerce=spec.coerce,
                required=spec.required,
                secret=spec.secret,
                default=spec.default,
                doc=spec.doc,
            )
            for spec in getattr(self.resource_class, "_payload_field_map", {}).values()
        ]

    def validators(self) -> list[ValidatorSpec]:
        """Named validation checks in declaration order (public schema)."""

        return [
            ValidatorSpec(name=spec.name, both=spec.both)
            for spec in self._validators.values()
        ]

    def defaults(self) -> AdapterDefaults:
        """Pool-level defaults merged by create_pool / default_for."""

        return AdapterDefaults(
            max_slots=self.default_concurrency,
            secret_fields=self.secret_fields,
        )


class ResourceAdapterRegistry:
    """Runtime-registrable plugin registry (X7)."""

    def __init__(self) -> None:
        self._adapters: dict[str, ResourceAdapter] = {}

    def register(self, adapter: ResourceAdapter) -> None:
        if not adapter.kind:
            raise ConfigError("resource adapter must declare a non-empty kind")
        self._adapters[adapter.kind] = adapter

    def unregister(self, kind: str) -> None:
        self._adapters.pop(kind, None)

    def get(self, kind: str) -> ResourceAdapter:
        try:
            return self._adapters[kind]
        except KeyError:
            raise ConfigError(f"unknown resource adapter {kind!r}") from None

    def __contains__(self, kind: str) -> bool:
        return kind in self._adapters

    def names(self) -> list[str]:
        return list(self._adapters)


_AdapterFactory = Callable[[], ResourceAdapter]

# Process-wide default catalog: kind -> factory producing a ResourceAdapter
# instance.  The catalog is shared (one per process); each Purser instantiates
# its own copy at construction, so instances never leak across Pursers.
_DEFAULT_ADAPTERS: dict[str, _AdapterFactory] = {}


def register(
    target: type[ResourceAdapter] | None = None, **config: Any
) -> type[ResourceAdapter] | Callable[[type[ResourceAdapter]], type[ResourceAdapter]]:
    """Register a ResourceAdapter as a process-wide default (decorator).

    Every :class:`Purser` built afterwards instantiates its own copy from the
    registered factory, so type instances are never shared between Purser
    instances.  ``kind`` must be unique across defaults and may not collide
    with a builtin adapter.  Usable bare (``@register``) or with constructor
    config (``@register(probe_targets=[...])``).
    """

    def decorate(cls: type[ResourceAdapter]) -> type[ResourceAdapter]:
        kind = getattr(cls, "kind", "")
        if not kind:
            raise ConfigError("registered resource adapter must declare a non-empty kind")
        if kind in _DEFAULT_ADAPTERS:
            raise ConfigError(f"duplicate resource adapter kind {kind!r}")
        def build() -> ResourceAdapter:
            return cls(**config) if config else cls()

        _DEFAULT_ADAPTERS[kind] = build
        return cls

    if target is not None:
        return decorate(target)
    return decorate


def default_adapter_factories() -> dict[str, _AdapterFactory]:
    """Snapshot of the process-wide default adapter factories (kind -> builder)."""

    return dict(_DEFAULT_ADAPTERS)


def clear_default_adapters() -> None:
    """Drop every process-wide default adapter (test / hot-reload isolation)."""

    _DEFAULT_ADAPTERS.clear()
