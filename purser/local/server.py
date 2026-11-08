"""Scheduler process for same-host multiprocessing.

The process owns the only ``SyncPurser`` and serves workers over a
``multiprocessing.connection`` listener.  It has no HTTP dependency.
"""

from __future__ import annotations

import contextlib
import logging
import os
import threading
import time
import traceback
from collections.abc import Callable
from concurrent.futures import Future
from multiprocessing.connection import Listener
from typing import Any

from purser.config import Weights
from purser.core.quarantine import QuarantinePolicy
from purser.core.resource import AcquireRequest, UsageOutcome
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
from purser.local.errors import SchedulerProtocolError
from purser.local.protocol import (
    SchedulerAddress,
    cleanup_address,
    recv_message,
    send_message,
)
from purser.sync import SyncLease, SyncPool, SyncPurser

logger = logging.getLogger("purser.local")

_IDEMPOTENCY_TTL = 600.0
_RELEASE_TOMBSTONE_TTL = 600.0


def _scheduler_main(
    address: SchedulerAddress,
    config: dict[str, Any],
    ready_event: Any,
    error_queue: Any,
    types: tuple[Any, ...] = (),
) -> None:
    """Module-level entry point (spawn-safe) for the scheduler process."""

    purser: SyncPurser | None = None
    listener: Listener | None = None
    service: _Service | None = None
    try:
        purser = SyncPurser(**config)
        for rtype in types:
            purser.register_type(rtype)
        purser.start()
        listener = Listener(
            address.address, family=address.family, authkey=address.authkey
        )
        service = _Service(purser, listener)
        thread = threading.Thread(
            target=service.accept_loop, name="purser-local-accept", daemon=True
        )
        thread.start()
        ready_event.set()
        service.wait()
        service.shutdown_ack.wait(1.0)
        service.stop()
        service.join_handlers(5.0)
        purser.close()
    except BaseException:
        error_queue.put(traceback.format_exc())
        ready_event.set()
        if service is not None:
            with contextlib.suppress(BaseException):
                service.stop()
        if listener is not None:
            with contextlib.suppress(BaseException):
                listener.close()
        if purser is not None:
            with contextlib.suppress(BaseException):
                purser.close()
    finally:
        cleanup_address(address)


class _Service:
    def __init__(self, purser: SyncPurser, listener: Listener) -> None:
        self._purser = purser
        self._listener = listener
        self._stop = threading.Event()
        self.shutdown_ack = threading.Event()
        self._lock = threading.Lock()
        self._results: dict[str, tuple[float, dict[str, Any]]] = {}
        self._inflight: dict[str, Future[dict[str, Any]]] = {}
        self._released: dict[str, tuple[float, dict[str, Any]]] = {}
        # token -> (pool, connections that know about it).  A crashed worker's
        # connection closes, so its leases are returned instead of being
        # heartbeated forever by the scheduler process.
        self._lease_owners: dict[str, tuple[str, set[Any]]] = {}
        self._conns: set[Any] = set()
        self._handlers: set[threading.Thread] = set()
        self._started_at = time.time()

    # -- lifecycle --------------------------------------------------------
    def accept_loop(self) -> None:
        while not self._stop.is_set():
            try:
                conn = self._listener.accept()
            except (OSError, EOFError):
                break
            if self._stop.is_set():
                with contextlib.suppress(Exception):
                    conn.close()
                break
            with self._lock:
                self._conns.add(conn)
            thread = threading.Thread(
                target=self._serve_connection,
                args=(conn,),
                name="purser-local-conn",
                daemon=True,
            )
            with self._lock:
                self._handlers.add(thread)
            thread.start()

    def wait(self) -> None:
        self._stop.wait()

    def stop(self) -> None:
        self._stop.set()
        with contextlib.suppress(OSError):
            self._listener.close()
        with self._lock:
            conns = list(self._conns)
        for conn in conns:
            with contextlib.suppress(Exception):
                conn.close()

    def join_handlers(self, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        with self._lock:
            handlers = list(self._handlers)
        for thread in handlers:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            thread.join(remaining)

    # -- connection -------------------------------------------------------
    def _serve_connection(self, conn: Any) -> None:
        try:
            while not self._stop.is_set():
                try:
                    request = recv_message(conn)
                except SchedulerProtocolError as exc:
                    with contextlib.suppress(Exception):
                        send_message(
                            conn,
                            {
                                "id": None,
                                "ok": False,
                                "error": {
                                    "code": "scheduler_protocol",
                                    "message": str(exc),
                                },
                            },
                        )
                    break
                except Exception:
                    break
                response = self._handle_request(request, conn)
                try:
                    send_message(conn, response)
                except SchedulerProtocolError as exc:
                    self._discard_response(response)
                    with contextlib.suppress(Exception):
                        send_message(
                            conn,
                            {
                                "id": response.get("id"),
                                "ok": False,
                                "error": {
                                    "code": "scheduler_protocol",
                                    "message": str(exc),
                                },
                            },
                        )
                    break
                except Exception:
                    self._discard_response(response)
                    break
                if request.get("op") == "shutdown":
                    self.shutdown_ack.set()
                    break
        finally:
            with contextlib.suppress(Exception):
                conn.close()
            self._release_conn_leases(conn)
            with self._lock:
                self._conns.discard(conn)
                self._handlers.discard(threading.current_thread())

    def _discard_response(self, response: dict[str, Any]) -> None:
        result = response.get("result")
        if not isinstance(result, dict):
            return
        lease = result.get("lease")
        if not isinstance(lease, dict):
            return
        pool_name = lease.get("pool")
        token = lease.get("token")
        if not isinstance(pool_name, str) or not isinstance(token, str):
            return
        with contextlib.suppress(Exception):
            self._release_token(pool_name, token, None, None)

    def _track_lease(self, conn: Any, pool_name: str, token: str) -> None:
        with self._lock:
            entry = self._lease_owners.get(token)
            if entry is None:
                self._lease_owners[token] = (pool_name, {conn})
            else:
                entry[1].add(conn)

    def _untrack_lease(self, token: str) -> None:
        with self._lock:
            self._lease_owners.pop(token, None)

    def _release_conn_leases(self, conn: Any) -> None:
        """Return leases no live connection can still use (worker crash/exit)."""

        pending: list[tuple[str, str]] = []
        with self._lock:
            for token, (pool_name, conns) in list(self._lease_owners.items()):
                if conn not in conns:
                    continue
                conns.discard(conn)
                if not conns:
                    self._lease_owners.pop(token, None)
                    pending.append((pool_name, token))
        for pool_name, token in pending:
            with contextlib.suppress(Exception):
                self._release_token(pool_name, token, None, None)

    # -- dispatch ---------------------------------------------------------
    def _handle_request(
        self, request: dict[str, Any], conn: Any = None
    ) -> dict[str, Any]:
        request_id = request.get("id")
        op = request.get("op")
        try:
            if not isinstance(op, str):
                raise ConfigError("request is missing a string 'op'")
            if op == "health":
                result = self._health()
            elif op == "shutdown":
                self._stop.set()
                result = {"ok": True}
            elif op == "pool_names":
                result = {"pools": self._purser.pool_names()}
            elif op == "create_pool":
                result = {"pool": self._create_pool(request)}
            elif op == "delete_pool":
                result = self._purser.delete_pool(
                    str(request["name"]), force=bool(request.get("force", False))
                )
            elif op == "stats":
                result = {"stats": self._stats(str(request["pool"]))}
            elif op == "import":
                result = self._import(request)
            elif op == "generate":
                result = self._generate(request)
            elif op == "acquire":
                result = self._acquire(request, conn)
            elif op == "release":
                result = self._release(request)
            elif op == "renew":
                result = self._renew(request)
            else:
                raise ConfigError(f"unknown scheduler operation {op!r}")
            return {"id": request_id, "ok": True, "result": result}
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            if not isinstance(exc, PurserError):
                logger.exception("local scheduler operation %r failed", op)
            return {
                "id": request_id,
                "ok": False,
                "error": {"code": _error_code(exc), "message": str(exc)},
            }

    # -- operations -------------------------------------------------------
    def _health(self) -> dict[str, Any]:
        from purser import __version__

        return {
            "version": __version__,
            "protocol": 1,
            "pid": os.getpid(),
            "started_at": self._started_at,
            "uptime_s": round(time.time() - self._started_at, 1),
            "storage": self._purser.storage_kind,
            "coordination": self._purser.coordination_kind,
            "pools": self._purser.pool_names(),
        }

    def _create_pool(self, request: dict[str, Any]) -> dict[str, Any]:
        name = str(request["name"])
        kind = str(request["kind"])
        config = request.get("config") or {}
        if not isinstance(config, dict):
            raise ConfigError("config must be an object")
        pool = self._purser.create_pool(
            name, kind, config=_clean_pool_config(config)
        )
        return self._pool_stats(pool)

    def _stats(self, name: str) -> dict[str, Any]:
        return self._pool_stats(self._purser.require_pool(name))

    @staticmethod
    def _pool_stats(pool: SyncPool) -> dict[str, Any]:
        stats = pool.stats()
        if not isinstance(stats, dict):
            raise SchedulerProtocolError("pool stats are not a mapping")
        return dict(stats)

    def _import(self, request: dict[str, Any]) -> dict[str, Any]:
        pool = self._purser.require_pool(str(request["pool"]))
        records = request.get("records")
        if not isinstance(records, list):
            raise ConfigError("records must be a list")
        report = pool.import_resources(
            records,
            dedup_key=str(request.get("dedup_key", "id")),
            source=str(request.get("source", "manual")),
            operator=str(request.get("operator", "system")),
            fail_fast=bool(request.get("fail_fast", False)),
            import_validate=request.get("import_validate"),
            batch_id=request.get("batch_id"),
        )
        return report.to_dict()

    def _generate(self, request: dict[str, Any]) -> dict[str, Any]:
        pool = self._purser.require_pool(str(request["pool"]))
        specs = request.get("specs")
        if not isinstance(specs, list):
            raise ConfigError("specs must be a list")
        report = pool.generate(
            specs,
            source=str(request.get("source", "builtin")),
            operator=str(request.get("operator", "system")),
        )
        return report.to_dict()

    def _acquire(self, request: dict[str, Any], conn: Any = None) -> dict[str, Any]:
        pool_name = str(request["pool"])
        raw_request = request.get("request") or {}
        if not isinstance(raw_request, dict):
            raise ConfigError("request must be an object")
        key = request.get("idempotency_key")
        idem_key = f"acquire:{pool_name}:{key}" if key else None

        def run() -> dict[str, Any]:
            pool = self._purser.require_pool(pool_name)
            lease = pool.acquire(_acquire_request(raw_request))
            return {"lease": _lease_payload(lease)}

        result = self._idempotent(idem_key, run, mark_replayed=True)
        lease_data = result.get("lease")
        if conn is not None and isinstance(lease_data, dict):
            token = lease_data.get("token")
            if isinstance(token, str):
                self._track_lease(conn, pool_name, token)
        return result

    def _release(self, request: dict[str, Any]) -> dict[str, Any]:
        pool_name = str(request["pool"])
        token = str(request["token"])
        now = time.time()
        with self._lock:
            cached = self._released.get(token)
        if cached is not None and cached[0] >= now:
            return dict(cached[1])
        key = request.get("idempotency_key")
        idem_key = f"release:{pool_name}:{key}" if key else None

        def run() -> dict[str, Any]:
            outcome = request.get("outcome")
            used = request.get("used")
            result = self._release_token(
                pool_name,
                token,
                None if outcome is None else UsageOutcome(str(outcome)),
                None if used is None else int(used),
            )
            with self._lock:
                self._released[token] = (
                    time.time() + _RELEASE_TOMBSTONE_TTL,
                    result,
                )
                self._prune_locked()
            return result

        return self._idempotent(idem_key, run)

    def _release_token(
        self,
        pool_name: str,
        token: str,
        outcome: UsageOutcome | None,
        used: int | None,
    ) -> dict[str, Any]:
        pool = self._purser.require_pool(pool_name)
        lease = _find_lease(pool, token)
        if lease is None:
            # Same contract as the in-process facade: releasing a token that a
            # revoke, expiry or retry already removed is a success.
            self._untrack_lease(token)
            return {
                "ok": True,
                "token": token,
                "outcome": (outcome or UsageOutcome.OK).value,
            }
        pool.release(lease, outcome=outcome, used=used)
        self._untrack_lease(token)
        return {
            "ok": True,
            "token": token,
            "outcome": (outcome or lease.outcome).value,
        }

    def _renew(self, request: dict[str, Any]) -> dict[str, Any]:
        pool_name = str(request["pool"])
        token = str(request["token"])
        pool = self._purser.require_pool(pool_name)
        lease = _find_lease(pool, token)
        if lease is None:
            raise LeaseConflict(f"lease {token!r} is not active on pool {pool_name!r}")
        if not pool.renew_manual(lease):
            raise LeaseConflict(f"lease {token!r} renewal was rejected")
        return {"ok": True, "token": token, "expires_at": lease.expires_at}

    # -- idempotency ------------------------------------------------------
    def _idempotent(
        self,
        key: str | None,
        fn: Callable[[], dict[str, Any]],
        *,
        mark_replayed: bool = False,
    ) -> dict[str, Any]:
        if not key:
            return fn()
        with self._lock:
            self._prune_locked()
            cached = self._results.get(key)
            if cached is not None and cached[0] >= time.time():
                result = dict(cached[1])
                if mark_replayed:
                    lease = result.get("lease")
                    if isinstance(lease, dict):
                        result["lease"] = {**lease, "replayed": True}
                return result
            future = self._inflight.get(key)
            owner = future is None
            if owner:
                future = Future()
                self._inflight[key] = future
        assert future is not None
        if not owner:
            return future.result()
        try:
            result = fn()
        except BaseException as exc:
            with self._lock:
                self._inflight.pop(key, None)
            future.set_exception(exc)
            future.exception()
            raise
        with self._lock:
            self._inflight.pop(key, None)
            self._results[key] = (time.time() + _IDEMPOTENCY_TTL, dict(result))
        future.set_result(result)
        return result

    def _prune_locked(self) -> None:
        now = time.time()
        stale = [key for key, (expires, _) in self._results.items() if expires < now]
        for key in stale:
            self._results.pop(key, None)
        stale = [key for key, (expires, _) in self._released.items() if expires < now]
        for key in stale:
            self._released.pop(key, None)


def _find_lease(pool: SyncPool, token: str) -> SyncLease | None:
    for lease in pool.leases():
        if lease.token == token:
            return lease
    return None


def _acquire_request(raw: dict[str, Any]) -> AcquireRequest:
    fields = AcquireRequest.__dataclass_fields__
    unknown = set(raw) - set(fields)
    if unknown:
        names = ", ".join(sorted(unknown))
        raise ConfigError(f"acquire() got unexpected field(s): {names}")
    data = dict(raw)
    if "tags" in data and data["tags"] is not None:
        tags = data["tags"]
        if isinstance(tags, str) or not isinstance(tags, (list, tuple, set, frozenset)):
            raise ConfigError("tags must be a list of strings")
        data["tags"] = frozenset(str(tag) for tag in tags)
    return AcquireRequest(**data)


def _clean_pool_config(raw: dict[str, Any]) -> dict[str, Any]:
    data = dict(raw)
    data.pop("kind", None)
    if "secret_fields" in data and isinstance(data["secret_fields"], list):
        data["secret_fields"] = tuple(data["secret_fields"])
    if isinstance(data.get("weights"), dict):
        data["weights"] = Weights.from_dict(data["weights"])
    if isinstance(data.get("quarantine"), dict):
        data["quarantine"] = QuarantinePolicy(**data["quarantine"])
    return data


def _lease_payload(lease: SyncLease) -> dict[str, Any]:
    resource = lease.resource
    return {
        "token": lease.token,
        "pool": lease.pool,
        "resource_id": lease.resource_id,
        "consumer": lease.consumer,
        "slots": lease.slots,
        "started_at": lease.started_at,
        "expires_at": lease.expires_at,
        "heartbeat_at": lease.heartbeat_at,
        "priority": lease.priority,
        "use_limit": lease.use_limit,
        "used": lease.used,
        "trace_id": lease.trace_id,
        "target": lease.target,
        "outcome": lease.outcome.value,
        "payload": lease.payload,
        "resource": None if resource is None else resource.to_dict(),
    }


def _error_code(exc: BaseException) -> str:
    if isinstance(exc, PoolNotFound):
        return "pool_not_found"
    if isinstance(exc, PoolExists):
        return "pool_exists"
    if isinstance(exc, ResourceNotFound):
        return "resource_not_found"
    if isinstance(exc, PoolEmpty):
        return "pool_empty"
    if isinstance(exc, PoolExhausted):
        return "pool_exhausted"
    if isinstance(exc, NoSuitableResource):
        return "no_suitable_resource"
    if isinstance(exc, LeaseExpired):
        return "lease_expired"
    if isinstance(exc, LeaseRevokedError):
        return "lease_revoked"
    if isinstance(exc, LeaseLostError):
        return "lease_lost"
    if isinstance(exc, LeaseLimitReached):
        return "lease_limit"
    if isinstance(exc, LeaseConflict):
        return "lease_conflict"
    if isinstance(exc, InvalidTransition):
        return "invalid_transition"
    if isinstance(exc, BadState):
        return "bad_state"
    if isinstance(exc, BadPayload):
        return "bad_payload"
    if isinstance(exc, VersionConflict):
        return "version_conflict"
    if isinstance(exc, NotStarted):
        return "not_started"
    if isinstance(exc, ConfigError):
        return "config_error"
    if isinstance(exc, TimeoutError):
        return "scheduler_timeout"
    if isinstance(exc, PurserError):
        return "purser_error"
    if isinstance(exc, (TypeError, ValueError)):
        return "config_error"
    return "internal_error"
