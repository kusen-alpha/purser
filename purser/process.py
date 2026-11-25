"""Purser-shaped facade for the same-host scheduler process.

``ProcessPurser`` presents the same calling convention as the other facades —
synchronous management calls, asynchronous pool data plane, async
``start``/``close`` — while the kernel lives in a child process owned by
:class:`~purser.local.LocalScheduler` (or reached through an existing
:class:`~purser.local.protocol.SchedulerAddress` in attach mode).

The data-plane objects are :class:`~purser.remote.RemotePool` wrappers, so
the scheduler process stays the single authority for slots.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Iterable, Mapping
from typing import Any, TypeVar, overload

from purser.adapter_spec import AdapterSpec, load_adapter
from purser.core.resource import Resource
from purser.errors import ConfigError, IsolationConflict, PoolExists, PoolNotFound
from purser.isolation import PICKLE_MESSAGE
from purser.local.client import LocalSchedulerClient
from purser.local.errors import SchedulerUnavailable
from purser.local.protocol import SchedulerAddress
from purser.local.scheduler import LocalScheduler
from purser.protocols import MISSING
from purser.remote import RemotePool

_T = TypeVar("_T")

__all__ = ["ProcessPurser"]


def _resolve_address(
    address: SchedulerAddress | Mapping[str, Any] | str | None,
) -> SchedulerAddress:
    if isinstance(address, SchedulerAddress):
        resolved: SchedulerAddress | None = address
    elif isinstance(address, Mapping):
        resolved = SchedulerAddress.from_dict(dict(address))
    elif isinstance(address, str):
        resolved = SchedulerAddress.from_env(address)
    elif address is None:
        resolved = SchedulerAddress.from_env()
    else:
        raise TypeError("address must be SchedulerAddress, mapping, str or None")
    if resolved is None:
        raise ConfigError(
            "no scheduler address; pass address or set "
            "PURSER_LOCAL_SCHEDULER"
        )
    return resolved


def _require_zero_arg(adapter: Any) -> type:
    cls = type(adapter)
    signature = inspect.signature(cls.__init__)
    required = [
        name
        for name, param in signature.parameters.items()
        if name != "self"
        and param.default is param.empty
        and param.kind
        in (param.POSITIONAL_ONLY, param.POSITIONAL_OR_KEYWORD, param.KEYWORD_ONLY)
    ]
    if required:
        raise ConfigError(
            f"adapter {cls.__qualname__} needs constructor arguments "
            f"({', '.join(required)}) and cannot be registered remotely; "
            "pass AdapterSpec.of(cls, **kwargs) instead"
        )
    return cls


def _as_spec(target: Any) -> AdapterSpec:
    if isinstance(target, AdapterSpec):
        return target
    if isinstance(target, Mapping):
        path = target.get("path")
        if not isinstance(path, str) or not path:
            raise ConfigError("adapter spec requires a 'path' string")
        kwargs = target.get("kwargs")
        if kwargs is None:
            kwargs = {}
        if not isinstance(kwargs, Mapping):
            raise ConfigError("adapter spec 'kwargs' must be an object")
        return AdapterSpec(path, dict(kwargs))
    factory_spec = getattr(target, "factory_spec", None)
    if callable(factory_spec):
        spec = factory_spec()
        if not isinstance(spec, AdapterSpec):
            raise ConfigError("factory_spec() must return an AdapterSpec")
        return spec
    if isinstance(target, type):
        return AdapterSpec.of(target)
    return AdapterSpec.of(_require_zero_arg(target))


class ProcessPurser:
    """Purser-shaped facade over a local scheduler process.

    Owned mode (default) constructs and starts the scheduler process; attach
    mode takes an existing ``address`` and never shuts it down.  Attached
    instances are picklable (address plus resource types), which makes them
    usable as worker-side handles.
    """

    def __init__(
        self,
        *,
        address: SchedulerAddress | Mapping[str, Any] | str | None = None,
        storage: str = "memory",
        coordination: str = "memory",
        sqlite_path: str | None = None,
        mysql_url: str | None = None,
        redis_url: str | None = None,
        audit_path: str | None = None,
        secret_key: bytes | None = None,
        authkey: bytes | None = None,
        start_method: str | None = "spawn",
        startup_timeout: float = 30.0,
        close_timeout: float = 30.0,
        adapters: Iterable[Any] = (),
        maintenance_enabled: bool | None = None,
        default_adapters: str = "global",
        resource_types: Mapping[str, type[Resource]] | None = None,
    ) -> None:
        owned_args: dict[str, Any] = {
            "storage": storage,
            "coordination": coordination,
            "sqlite_path": sqlite_path,
            "mysql_url": mysql_url,
            "redis_url": redis_url,
            "audit_path": audit_path,
            "secret_key": secret_key,
            "authkey": authkey,
            "start_method": start_method,
            "startup_timeout": startup_timeout,
            "close_timeout": close_timeout,
            "adapters": tuple(adapters),
            "maintenance_enabled": maintenance_enabled,
            "default_adapters": default_adapters,
        }
        if address is not None:
            # ``storage``/``coordination`` default to "memory"; only explicit
            # non-defaults conflict with attach mode.  Timeouts and
            # ``start_method`` only configure the owned scheduler, so they are
            # ignored here.
            provided = [
                name
                for name, value in owned_args.items()
                if name
                not in (
                    "start_method",
                    "startup_timeout",
                    "close_timeout",
                    "default_adapters",
                )
                and (
                    (name in ("storage", "coordination") and value != "memory")
                    or (
                        name not in ("storage", "coordination")
                        and value not in (None, ())
                    )
                )
            ]
            if provided:
                raise ConfigError(
                    "attach mode (address=) cannot be combined with scheduler "
                    f"options: {', '.join(sorted(provided))}"
                )
            self._address: SchedulerAddress | None = _resolve_address(address)
            self._scheduler: LocalScheduler | None = None
        else:
            self._address = None
            resolved_adapters = [
                load_adapter(item) if isinstance(item, AdapterSpec) else item
                for item in adapters
            ]
            owned_args.pop("adapters")
            self._scheduler = LocalScheduler(
                adapters=resolved_adapters,
                **owned_args,
            )
        self._owned = self._scheduler is not None
        self._resource_types: dict[str, type[Resource]] = dict(resource_types or {})
        self._client: LocalSchedulerClient | None = None
        self._pools: dict[str, RemotePool] = {}
        self._started = False
        self._closed = False

    # -- state ------------------------------------------------------------
    def __getstate__(self) -> Any:
        if self._owned:
            raise IsolationConflict(PICKLE_MESSAGE)
        return {
            "address": self.address.to_dict(),
            "resource_types": dict(self._resource_types),
        }

    def __setstate__(self, state: Mapping[str, Any]) -> None:
        self._address = SchedulerAddress.from_dict(dict(state["address"]))
        self._scheduler = None
        self._owned = False
        self._resource_types = dict(state.get("resource_types") or {})
        self._client = None
        self._pools = {}
        self._started = False
        self._closed = False

    @property
    def address(self) -> SchedulerAddress:
        if self._scheduler is not None:
            return self._scheduler.address
        assert self._address is not None
        return self._address

    @property
    def owned(self) -> bool:
        return self._owned

    @property
    def started(self) -> bool:
        return self._started

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def client(self) -> LocalSchedulerClient:
        if self._client is None:
            if self._closed:
                raise SchedulerUnavailable("ProcessPurser is closed")
            # Management calls may run before ``start()`` (pcrawl creates
            # pools synchronously); an owned scheduler starts on first use.
            if self._scheduler is not None and self._scheduler.pid is None:
                self._scheduler.start()
            self._client = LocalSchedulerClient(
                self.address, resource_types=self._resource_types
            )
        return self._client

    # -- adapters ---------------------------------------------------------
    def register_adapter(self, adapter: Any) -> None:
        """Register an adapter in the scheduler process.

        Only importable descriptions cross the IPC boundary: an
        :class:`~purser.adapter_spec.AdapterSpec`, a raw ``{"path", "kwargs"}``
        mapping, a class with a zero-argument constructor, or a live instance
        whose class is importable and zero-argument constructible.  Adapters
        needing live state belong in ``adapters=`` at construction time.
        """

        self.client.register_adapter(_as_spec(adapter))

    # -- pools ------------------------------------------------------------
    def create_pool(
        self,
        name: str,
        kind: str,
        *,
        config: Any = None,
        profile: Any = None,
        **overrides: Any,
    ) -> RemotePool:
        self.client.create_pool(name, kind, config=config, profile=profile, **overrides)
        pool = RemotePool(
            self.client,
            name,
            resource_types=self._resource_types,
            kind=kind,
            profile=profile if isinstance(profile, str) else None,
        )
        self._pools[name] = pool
        return pool

    def ensure_pool(
        self,
        name: str,
        kind: str,
        *,
        config: Any = None,
        profile: Any = None,
        **overrides: Any,
    ) -> RemotePool:
        """Return pool ``name``, attaching when the scheduler already owns it."""

        existing = self._pools.get(name)
        if existing is not None:
            return existing
        try:
            return self.create_pool(
                name, kind, config=config, profile=profile, **overrides
            )
        except PoolExists:
            # Attach mode frequently reaches a scheduler that already owns the
            # pool; track a handle instead of failing.
            info = self.client.stats(name)
            pool = RemotePool(
                self.client,
                name,
                resource_types=self._resource_types,
                kind=info.get("kind"),
                profile=info.get("profile"),
            )
            self._pools[name] = pool
            return pool

    @overload
    def get_pool(self, name: str, *, default: _T) -> RemotePool | _T: ...

    @overload
    def get_pool(self, name: str, *, default: Any = MISSING) -> RemotePool: ...

    def get_pool(self, name: str, *, default: Any = MISSING) -> Any:
        """Return a tracked pool, or ``default`` / raise ``PoolNotFound``."""

        existing = self._pools.get(name)
        if existing is not None:
            return existing
        if default is MISSING:
            raise PoolNotFound(
                f"pool {name!r} is not tracked by this ProcessPurser; "
                "pass ensure_pool(name, kind) or default="
            )
        return default

    @property
    def pools(self) -> Mapping[str, RemotePool]:
        """Snapshot of locally tracked pool handles; scheduler state may differ."""

        return dict(self._pools)

    def get_pool_names(self) -> list[str]:
        """Names registered in the scheduler process (queried over IPC)."""

        return [str(name) for name in self.client.pool_names()]

    async def delete_pool(
        self, name: str, *, force: bool = False, operator: str = "system"
    ) -> dict[str, Any]:
        """Delete a pool in the scheduler process and drop the local handle."""

        result = await asyncio.to_thread(
            self.client.delete_pool, name, force=force, operator=operator
        )
        self._pools.pop(name, None)
        return result

    # -- lifecycle --------------------------------------------------------
    async def start(self) -> None:
        if self._closed:
            raise ConfigError("ProcessPurser is closed")
        if self._started:
            return
        if self._scheduler is not None and not self._scheduler.alive:
            await asyncio.to_thread(self._scheduler.start)
        await asyncio.to_thread(self.client.health)
        self._started = True

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._started = False
        for pool in self._pools.values():
            await pool.aclose()
        self._pools.clear()
        client, self._client = self._client, None
        if client is not None:
            client.close()
        if self._scheduler is not None:
            await asyncio.to_thread(self._scheduler.close)

    async def __aenter__(self) -> ProcessPurser:
        await self.start()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    def __repr__(self) -> str:
        mode = "owned" if self._owned else "attached"
        return f"ProcessPurser({mode}, address={self.address.address!r})"
