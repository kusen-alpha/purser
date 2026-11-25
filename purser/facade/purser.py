"""The :class:`Purser` composition root.

``Purser`` owns the pool registry, the adapter registry, cross-pool bundles,
backup/restore and the instance lifecycle (start/close).  The acquisition
pipeline, wait queue, heartbeat and reaping live in ``purser.pool``; bundle
orchestration lives in ``purser.facade.bundle``.
"""

from __future__ import annotations

import asyncio
import contextlib
import math
from collections.abc import (
    AsyncIterator,
    Mapping,
)
from typing import Any, overload

from purser.adapters.base import (
    ResourceAdapter,
    ResourceAdapterRegistry,
    builtin_adapter_factories,
    default_adapter_factories,
)
from purser.backends.base import Coordination, StorageBackend
from purser.backends.coordination import MemoryCoordination, RedisCoordination
from purser.backends.memory import MemoryStorage
from purser.backends.sqlite import SqliteStorage
from purser.config import (
    BundleMode,
    CoordinationKind,
    PoolConfig,
    StorageKind,
    _coerce_kind,
    apply_adapter_defaults,
    resolve_pool_config,
)
from purser.core.audit import AuditLog
from purser.core.bundle import (
    BundleFairness,
    BundleLease,
    BundleRequest,
)
from purser.core.bundle_store import BundleStore
from purser.core.events import EventBus
from purser.core.health import ProbeGuard
from purser.core.resource import UsageOutcome
from purser.core.usage import UsageLedger
from purser.core.views import (
    ActiveUsageGroup,
    PoolUsage,
    UsageReport,
)
from purser.crypto import SecretCodec, load_or_create_key
from purser.errors import (
    BadState,
    ConfigError,
    IsolationConflict,
    PoolExists,
    PoolNotFound,
)
from purser.facade._helpers import (
    _T,
    BACKUP_FORMAT,
    BACKUP_VERSION,
    _FairWaiter,
    _now,
)
from purser.facade.bundle import BundleService
from purser.idempotency import IdempotencyStore, RedisIdempotencyStore
from purser.isolation import (
    PICKLE_MESSAGE,
    IsolationGuard,
    mysql_connection_identity,
    mysql_identity,
    redis_client_identity,
    redis_identity,
    resource_adapter_identity,
    sqlite_identity,
)
from purser.metrics import Metrics
from purser.pool.pool import ResourcePool
from purser.protocols import MISSING
from purser.results import ImportReport as ImportReport


class Purser:
    """Top-level entry point: owns pools, plugin registry, events, audit."""

    def __init__(
        self,
        *,
        maintenance_enabled: bool = True,
        event_queue_size: int = 10_000,
        usage_ledger_size: int = 50_000,
        storage: StorageKind | str = StorageKind.MEMORY,
        coordination: CoordinationKind | str = CoordinationKind.MEMORY,
        mysql_url: str | None = None,
        mysql_connection: Any = None,
        redis_url: str | None = None,
        redis_client: Any = None,
        sqlite_path: str | None = None,
        audit_path: str | None = None,
        secret_key: bytes | None = None,
        secret_key_id: str = "1",
        previous_secret_keys: dict[str, bytes | str] | None = None,
        default_adapters: str = "global",
        bundle_mode: BundleMode | str = BundleMode.ROLLBACK,
        bundle_fairness: BundleFairness | None = None,
        reservation_ttl: float = 30.0,
        bundle_gc_interval: float = 30.0,
        fairness_ttl: float = 300.0,
    ) -> None:
        if default_adapters not in ("global", "builtin", "none"):
            raise ConfigError(
                "default_adapters must be 'global', 'builtin' or 'none'"
            )
        self.bundle_mode = _coerce_kind(BundleMode, bundle_mode, "bundle_mode")
        if reservation_ttl <= 0 or bundle_gc_interval <= 0 or fairness_ttl <= 0:
            raise ConfigError(
                "reservation_ttl / bundle_gc_interval / fairness_ttl must be > 0"
            )
        self._isolation = IsolationGuard(self)
        # Set by SyncPurser: the same kernel backs the blocking facade, whose
        # start/close are not awaitable.
        self._sync_mode = False
        self._isolation.claim_many(
            [
                redis_client_identity(redis_client)
                if redis_client is not None
                else redis_identity(redis_url)
            ]
        )
        self.adapters = ResourceAdapterRegistry()
        self.bus = EventBus(maxsize=event_queue_size)
        self.usage = UsageLedger(capacity=usage_ledger_size)
        self.audit = AuditLog(sqlite_path=audit_path)
        self.metrics = Metrics()
        self.idempotency = (
            RedisIdempotencyStore(url=redis_url, client=redis_client)
            if redis_url or redis_client is not None
            else IdempotencyStore()
        )
        self.probe_guard = ProbeGuard()
        self.maintenance_enabled = maintenance_enabled
        self.bundle_fairness = bundle_fairness
        self.reservation_ttl = reservation_ttl
        self.bundle_gc_interval = bundle_gc_interval
        self.fairness_ttl = fairness_ttl
        self.storage_kind = _coerce_kind(StorageKind, storage, "storage")
        self.coordination_kind = _coerce_kind(
            CoordinationKind, coordination, "coordination"
        )
        self.mysql_url = mysql_url
        self.mysql_connection = mysql_connection
        self.redis_url = redis_url
        self.redis_client = redis_client
        self.sqlite_path = sqlite_path
        self.audit_path = audit_path
        self.secret_key = secret_key
        key = secret_key
        if key is None and sqlite_path:
            key = load_or_create_key(sqlite_path)
        self._codec = SecretCodec(
            key,
            key_id=secret_key_id,
            previous_keys=dict(previous_secret_keys or {}),
        )
        self._pools: dict[str, ResourcePool] = {}
        self._bundles: dict[str, BundleLease] = {}
        self._bundle_store: BundleStore | None = None
        self._fair_heap: list[tuple[int, int, _FairWaiter]] = []
        self._fair_seq = 0
        self._bundle_gc_task: asyncio.Task[Any] | None = None
        self._bundle_service = BundleService(self)
        self._running = False
        self._install_default_adapters(default_adapters)

    def _install_default_adapters(self, mode: str) -> None:
        """Install the catalog subset selected by ``default_adapters``."""

        if mode == "none":
            return
        from purser.adapters.proxy import ProxyAdapter

        catalog = (
            default_adapter_factories()
            if mode == "global"
            else builtin_adapter_factories()
        )
        for kind, build in catalog.items():
            adapter = build()
            if kind == ProxyAdapter.kind and isinstance(adapter, ProxyAdapter):
                # ProbeGuard is per-Purser; catalog factories are process-wide.
                adapter.probe_guard = self.probe_guard
            self.adapters.register(adapter)

    def __getstate__(self) -> Any:
        raise IsolationConflict(PICKLE_MESSAGE)

    def _ensure_process(self) -> None:
        self._isolation.ensure_process()

    # -- plugins / pools ---------------------------------------------------

    def register_adapter(self, adapter: ResourceAdapter) -> None:
        self._ensure_process()
        identity = resource_adapter_identity(adapter)
        self._isolation.claim_many([identity])
        try:
            self.adapters.register(adapter)
        except BaseException:
            self._isolation.release_many([identity[0]])
            raise

    def _pool_isolation_entries(
        self, cfg: PoolConfig, adapter: ResourceAdapter
    ) -> list[Any]:
        """Scheduling-plane identities this pool would actually use."""

        entries: list[Any] = []
        if cfg.storage == "sqlite":
            entries.append(sqlite_identity(self.sqlite_path))
        elif cfg.storage == "mysql":
            entries.append(
                mysql_connection_identity(self.mysql_connection)
                if self.mysql_connection is not None
                else mysql_identity(self.mysql_url)
            )
        if cfg.coordination == "redis":
            entries.append(
                redis_client_identity(self.redis_client)
                if self.redis_client is not None
                else redis_identity(self.redis_url)
            )
        entries.append(resource_adapter_identity(adapter))
        return entries

    def _release_pool_claims(self, pool: ResourcePool) -> None:
        keys = pool._isolation_keys
        if keys:
            pool._isolation_keys = []
            self._isolation.release_many(keys)

    def _make_storage(self, cfg: PoolConfig) -> StorageBackend:
        if cfg.storage == "memory":
            return MemoryStorage()
        if cfg.storage == "sqlite":
            if not self.sqlite_path:
                raise ConfigError(
                    "storage='sqlite' requires sqlite_path; refusing to fall "
                    "back to a silent in-memory database"
                )
            return SqliteStorage(
                path=self.sqlite_path,
                codec=self._codec,
                secret_fields=cfg.secret_fields,
            )
        if cfg.storage == "mysql":
            from purser.backends.mysql import MySqlStorage

            if cfg.secret_fields and self._codec.ephemeral:
                raise ConfigError(
                    "storage='mysql' with secret_fields requires an explicit "
                    "secret_key (or PURSER_SECRET_KEY); an ephemeral key would "
                    "make encrypted payloads unreadable by other instances"
                )
            return MySqlStorage(
                url=self.mysql_url,
                connection=self.mysql_connection,
                codec=self._codec,
                secret_fields=cfg.secret_fields,
            )
        raise ConfigError(f"unknown storage {cfg.storage!r}")

    def _make_coordination(self, cfg: PoolConfig) -> Coordination:
        if cfg.coordination == "memory":
            return MemoryCoordination()
        if cfg.coordination == "redis":
            return RedisCoordination(
                url=self.redis_url,
                client=self.redis_client,
            )
        raise ConfigError(f"unknown coordination {cfg.coordination!r}")

    def create_pool(
        self,
        name: str,
        kind: str,
        *,
        config: PoolConfig | Mapping[str, Any] | None = None,
        profile: str | Mapping[str, Any] | None = None,
        **overrides: Any,
    ) -> ResourcePool:
        """Create a pool.

        Quick starts need no config object::

            ps.create_pool("proxies", "proxy", lease_ttl=120)
            ps.create_pool("proxies", "proxy", profile="validated")

        ``defaults`` (instance storage/coordination) → ``profile`` → ``config``
        → keyword ``overrides``; later sources win.
        """

        self._ensure_process()
        if name in self._pools:
            raise PoolExists(f"pool {name!r} already exists")
        adapter = self.adapters.get(kind)
        selected_profile = profile
        if selected_profile is None and isinstance(config, Mapping):
            selected_profile = config.get("profile")
        profile_label = selected_profile if isinstance(selected_profile, str) else None
        defaults = {
            "storage": self.storage_kind,
            "coordination": self.coordination_kind,
        }
        cfg = resolve_pool_config(
            kind,
            profile=profile,
            config=config,
            overrides=overrides,
            defaults=defaults,
        )
        cfg = apply_adapter_defaults(cfg, adapter)
        entries = self._pool_isolation_entries(cfg, adapter)
        self._isolation.claim_many(entries)
        keys = list(
            dict.fromkeys(entry[0] for entry in entries if entry is not None)
        )
        try:
            storage = self._make_storage(cfg)
            coordination = self._make_coordination(cfg)
            pool = ResourcePool(
                name,
                self,
                adapter,
                cfg,
                storage,
                coordination,
                profile=profile_label,
            )
        except BaseException:
            self._isolation.release_many(keys)
            raise
        pool._isolation_keys = keys
        self._pools[name] = pool
        if self._running:
            pool._start_task = asyncio.create_task(pool.start())
        self.audit.record(
            operator="system", action="create_pool", pool=name,
            detail={
                "kind": kind,
                "profile": profile_label,
                "storage": cfg.storage,
                "coordination": cfg.coordination,
            },
        )
        self.bus.publish("pool_created", name)
        return pool

    @overload
    def get_pool(self, name: str, *, default: _T) -> ResourcePool | _T: ...

    @overload
    def get_pool(self, name: str, *, default: Any = MISSING) -> ResourcePool: ...

    def get_pool(self, name: str, *, default: Any = MISSING) -> Any:
        """Return an existing pool, or ``default`` / raise ``PoolNotFound``.

        ``ps.get_pool("proxies")``               -> existing pool, else ``PoolNotFound``
        ``ps.get_pool("proxies", default=None)`` -> existing pool, else ``None``
        """

        self._ensure_process()
        existing = self._pools.get(name)
        if existing is not None:
            return existing
        if default is MISSING:
            raise PoolNotFound(f"pool {name!r} does not exist")
        return default

    def ensure_pool(
        self,
        name: str,
        kind: str,
        *,
        config: PoolConfig | Mapping[str, Any] | None = None,
        profile: str | Mapping[str, Any] | None = None,
        **overrides: Any,
    ) -> ResourcePool:
        """Return pool ``name``, creating it with ``kind`` when absent.

        Existing pools win: ``config`` / ``profile`` / ``overrides`` are ignored
        for a pool that already exists. Use :meth:`create_pool` when creating a
        duplicate should raise ``PoolExists``.
        """

        self._ensure_process()
        existing = self._pools.get(name)
        if existing is not None:
            return existing
        return self.create_pool(
            name, kind, config=config, profile=profile, **overrides
        )

    @property
    def pools(self) -> Mapping[str, ResourcePool]:
        """Snapshot of locally registered pools; later changes are not reflected."""

        return dict(self._pools)

    def get_pool_names(self) -> list[str]:
        return list(self._pools)

    # -- cross-pool bundles -----------------------------------------

    @contextlib.asynccontextmanager
    async def bundle(
        self,
        request: BundleRequest | dict[str, Any],
        **overrides: Any,
    ) -> AsyncIterator[BundleLease]:
        """Atomic bundle acquire/release context manager."""

        bundle_lease = await self.acquire_bundle(request, **overrides)
        try:
            yield bundle_lease
        finally:
            await self.release_bundle(bundle_lease)

    async def acquire_bundle(
        self,
        request: BundleRequest | dict[str, Any],
        **overrides: Any,
    ) -> BundleLease:
        """Acquire one lease per bundle slot across pools (opt-in v2).

        Default ``rollback`` mode: scarce/declared order, rollback + jittered
        retry, ``best_effort`` partials.  With ``bundle_mode="two_phase"`` (or
        ``strict=True``) members are assembled in a deterministic global order
        and confirmed atomically on the reservation store.  With
        ``bundle_fairness`` configured, admission is gated by quota and
        per-pool headroom before any member is taken.  All advanced paths are
        off by default.
        """

        return await self._bundle_service.acquire(request, **overrides)

    async def release_bundle(
        self,
        bundle: BundleLease,
        *,
        outcome_by_pool: dict[str, UsageOutcome] | None = None,
        used_by_pool: dict[str, int] | None = None,
    ) -> None:
        """Release every member lease of a bundle (reversed acquisition order)."""

        await self._bundle_service.release(
            bundle,
            outcome_by_pool=outcome_by_pool,
            used_by_pool=used_by_pool,
        )

    async def bundle_gc(self) -> dict[str, int]:
        """Keep live bundle markers fresh, then sweep expired reservations."""

        return await self._bundle_service.gc()

    async def _store(self) -> BundleStore:
        return await self._bundle_service.ensure_store()

    # -- usage report (R1) -------------------------------------------------

    async def report_usage(
        self,
        *,
        since: float | None = None,
        until: float | None = None,
        group_by: str = "consumer",
        pool: str | None = None,
        limit: int = 20,
    ) -> UsageReport:
        """Aggregate completed leases plus the current arena snapshot.

        History comes from the bounded in-memory usage ledger: one row per
        returned (or force-revoked) lease, evicted oldest-first at capacity.
        Active leases and pool load are read live, so the report mixes
        historical usage with the present bottleneck view.
        """

        self._ensure_process()
        if group_by not in ("consumer", "target", "pool", "kind"):
            raise ConfigError(
                f"unknown group_by {group_by!r}; use consumer/target/pool/kind"
            )
        if not isinstance(limit, int) or limit < 1:
            raise ConfigError("limit must be an integer >= 1")
        for name, value in (("since", since), ("until", until)):
            if value is not None and (
                not isinstance(value, (int, float)) or not math.isfinite(value)
            ):
                raise ConfigError(f"{name} must be a finite epoch")
        if since is not None and until is not None and since > until:
            raise ConfigError("since must be <= until")
        names = [pool] if pool else list(self._pools)
        for name in names:
            if name not in self._pools:
                raise PoolNotFound(name)

        snapshot = self.usage.snapshot(
            since=since, until=until, group_by=group_by, pool=pool, limit=limit
        )
        active_groups: dict[str, dict[str, Any]] = {}
        pools: list[PoolUsage] = []
        for name in names:
            item = self._pools[name]
            active = await item._coordination.active(name)
            for lease in active:
                if group_by == "consumer":
                    key = lease.consumer or "(anonymous)"
                elif group_by == "target":
                    key = lease.target or "(no target)"
                elif group_by == "pool":
                    key = name
                else:
                    key = item.cfg.kind
                row = active_groups.setdefault(
                    key, {"key": key, "leases": 0, "slots": 0}
                )
                row["leases"] += 1
                row["slots"] += lease.slots
            stats = await item.stats()
            pools.append(
                {
                    "name": name,
                    "kind": item.cfg.kind,
                    "total": sum(stats["counts"].values()),
                    "counts": stats["counts"],
                    "allocatable": stats["allocatable"],
                    "active_leases": stats["active_leases"],
                    "waiting": stats["waiting"],
                    "avg_health": stats["avg_health"],
                }
            )
        groups: list[ActiveUsageGroup] = [
            {
                "key": str(row["key"]),
                "leases": int(row["leases"]),
                "slots": int(row["slots"]),
            }
            for row in sorted(
                active_groups.values(), key=lambda row: (-row["slots"], str(row["key"]))
            )
        ]
        return {
            "window": snapshot["window"],
            "group_by": snapshot["group_by"],
            "totals": snapshot["totals"],
            "groups": snapshot["groups"],
            "groups_total": snapshot["groups_total"],
            "truncated": snapshot["truncated"],
            "active": {"group_by": group_by, "groups": groups},
            "pools": pools,
        }

    # -- backup / restore --------------------------------------------------

    async def backup(self, *, operator: str = "system") -> dict[str, Any]:
        """Snapshot pool configs and resource state, secrets included.

        Callers must be trusted: the document carries plaintext payloads so a
        restore can reproduce resources exactly.  Leases live on the
        coordination plane and are deliberately not exported.
        """

        self._ensure_process()
        pools: list[dict[str, Any]] = []
        resources_total = 0
        for name in self._pools:
            pool = self._pools[name]
            records = []
            async for resource in pool.storage.scan(name):
                item = resource.to_dict()
                item.pop("leases", None)
                records.append(item)
            records.sort(key=lambda item: str(item.get("id", "")))
            resources_total += len(records)
            active = await pool._coordination.active(name)
            pools.append(
                {
                    "name": name,
                    "kind": pool.cfg.kind,
                    "config": pool.cfg.to_dict(),
                    "resources": records,
                    "leases": [
                        {
                            "token": lease.token,
                            "resource_id": lease.resource_id,
                            "consumer": lease.consumer,
                            "slots": lease.slots,
                            "started_at": lease.started_at,
                            "expires_at": lease.expires_at,
                            "priority": lease.priority,
                            "use_limit": lease.use_limit,
                            "used": lease.used,
                            "target": lease.target,
                            "trace_id": lease.trace_id,
                            "outcome": lease.outcome.value,
                        }
                        for lease in active
                    ],
                }
            )
        document = {
            "format": BACKUP_FORMAT,
            "version": BACKUP_VERSION,
            "schema_version": 1,
            "created_at": _now(),
            "pools": pools,
        }
        self.audit.record(
            operator=operator,
            action="backup",
            pool="*",
            detail={"pools": len(pools), "resources": resources_total},
        )
        return document

    async def restore_backup(
        self,
        document: dict[str, Any],
        *,
        conflict: str = "skip",
        operator: str = "system",
    ) -> dict[str, Any]:
        """Recreate pools/resources from :meth:`backup` output.

        Occupancy states (new/validating/active/full/draining/lost) return as
        IDLE because leases are not migrated; banned/quarantined/suspended/
        quota-exhausted/archived states are preserved.  ``conflict="skip"``
        leaves existing resources untouched, ``"overwrite"`` applies backup
        fields.  Pool storage/coordination are deployment choices, so they are
        taken from this service rather than the backup.
        """

        self._ensure_process()
        if conflict not in ("skip", "overwrite"):
            raise ConfigError("conflict must be 'skip' or 'overwrite'")
        if not isinstance(document, dict):
            raise ConfigError("backup document must be an object")
        if document.get("format") != BACKUP_FORMAT:
            raise ConfigError("backup document has an unknown format")
        if document.get("version") != BACKUP_VERSION:
            raise ConfigError(
                f"unsupported backup version {document.get('version')!r}"
            )
        entries = document.get("pools")
        if not isinstance(entries, list):
            raise ConfigError("backup.pools must be a list")

        created_pools: list[str] = []
        reused_pools: list[str] = []
        errors: list[dict[str, Any]] = []
        imported = updated = skipped = normalized = leases_ignored = 0
        for item in entries:
            if not isinstance(item, dict):
                errors.append({"pool": None, "error": "pool entry must be an object"})
                continue
            name = str(item.get("name") or "").strip()
            kind = str(item.get("kind") or "").strip()
            records = item.get("resources", [])
            if not name or not kind or not isinstance(records, list):
                errors.append(
                    {
                        "pool": name or None,
                        "error": "pool entry needs name, kind and resources[]",
                    }
                )
                continue
            pool = self._pools.get(name)
            if pool is not None:
                if pool.cfg.kind != kind:
                    errors.append(
                        {
                            "pool": name,
                            "error": f"pool already exists with kind {pool.cfg.kind!r}",
                        }
                    )
                    continue
                reused_pools.append(name)
            else:
                raw = item.get("config")
                config = raw if isinstance(raw, dict) else {}
                config = {
                    key: value
                    for key, value in config.items()
                    if key not in ("kind", "storage", "coordination", "schema_version")
                }
                try:
                    pool = self.create_pool(name, kind, config=config)
                except Exception as exc:
                    errors.append({"pool": name, "error": str(exc)})
                    continue
                created_pools.append(name)
            if self._running:
                await pool._ensure_started("restore")
            stats = await pool._restore_resources(
                records, conflict=conflict, operator=operator
            )
            pool_leases = item.get("leases")
            if isinstance(pool_leases, list):
                leases_ignored += len(pool_leases)
            imported += stats["imported"]
            updated += stats["updated"]
            skipped += stats["skipped"]
            normalized += stats["normalized"]
            if stats["errors"]:
                errors.extend({"pool": name, **err} for err in stats["errors"])
            self.audit.record(
                operator=operator,
                action="restore",
                pool=name,
                detail={**stats, "conflict": conflict},
            )
        return {
            "ok": True,
            "format": BACKUP_FORMAT,
            "conflict": conflict,
            "created_pools": created_pools,
            "reused_pools": reused_pools,
            "imported": imported,
            "updated": updated,
            "skipped": skipped,
            "normalized": normalized,
            "leases_ignored": leases_ignored,
            "errors": errors,
        }

    async def delete_pool(
        self, name: str, *, force: bool = False, operator: str = "system"
    ) -> dict[str, Any]:
        """Remove a pool from the registry.

        A pool with live leases is never deleted, even with ``force``: release
        or revoke them first.  ``force`` allows deleting a pool that still has
        stored resources, and flushes the backing stores before closing.
        """

        self._ensure_process()
        pool = self.get_pool(name)
        active = await pool._coordination.active(name)
        if active:
            raise BadState(
                f"pool {name!r} still has {len(active)} active lease(s); "
                "release or revoke them before deleting"
            )
        counts = await pool.counts()
        total = sum(counts.values())
        if total and not force:
            raise BadState(
                f"pool {name!r} still contains {total} resource(s); "
                "pass force to delete it"
            )
        if force:
            await pool.storage.flush(name)
            await pool._coordination.flush(name)
        await pool._binding.forget()
        await pool.close()
        self._pools.pop(name, None)
        self.audit.record(
            operator=operator,
            action="delete_pool",
            pool=name,
            detail={"removed": total, "forced": force},
        )
        self.bus.publish(
            "pool_deleted", name, data={"removed": total, "forced": force}
        )
        return {"pool": name, "removed": total, "forced": force}

    # -- runtimes ----------------------------------------------------------

    @property
    def started(self) -> bool:
        """Whether ``start()`` has been called and ``close()`` has not."""

        return self._running

    async def start(self) -> None:
        self._ensure_process()
        if self._running:
            return
        self._running = True
        await self.bus.start()
        for pool in self._pools.values():
            await pool.start()
        if self.bundle_mode == "two_phase" or self.bundle_fairness is not None:
            self._bundle_gc_task = asyncio.create_task(
                self._bundle_service.gc_loop()
            )

    async def close(self) -> None:
        self._ensure_process()
        self._running = False
        try:
            task, self._bundle_gc_task = self._bundle_gc_task, None
            if task is not None:
                task.cancel()
                with contextlib.suppress(BaseException):
                    await task
            for pool in self._pools.values():
                await pool.close()
            self._bundles.clear()
            await self.bus.stop()
            self.audit.close()
        finally:
            self._isolation.release_all()

    async def __aenter__(self) -> Purser:
        await self.start()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()
