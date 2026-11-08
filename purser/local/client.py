"""Client side of the local IPC scheduler.

Workers should create one client per process (``get_client()`` or
``init_worker(address)``).  Connection objects are never pickled or shared
between processes; only :class:`~purser.local.protocol.SchedulerAddress` is.
"""

from __future__ import annotations

import contextlib
import itertools
import os
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from multiprocessing.connection import Client
from typing import Any

from purser.core.resource import AcquireRequest, Resource, UsageOutcome
from purser.core.state import ResourceState
from purser.errors import ConfigError
from purser.local.errors import (
    SchedulerProtocolError,
    SchedulerTimeout,
    SchedulerUnavailable,
    remote_error,
)
from purser.local.protocol import (
    SchedulerAddress,
    recv_message,
    send_message,
)

_DEFAULT_RESPONSE_TIMEOUT = 300.0
_ACQUIRE_RESPONSE_GRACE = 10.0


def _resource_from_dict(data: Any) -> Resource | None:
    if not isinstance(data, dict):
        return None
    try:
        state = ResourceState(str(data.get("state", ResourceState.NEW.value)))
    except ValueError:
        state = ResourceState.NEW
    return Resource(
        id=str(data["id"]),
        kind=str(data["kind"]),
        payload=data.get("payload") or {},
        state=state,
        version=int(data.get("version", 0)),
        tags=frozenset(data.get("tags") or ()),
    )


class RemoteLease:
    """A lease owned by the scheduler process and used by a worker process."""

    __slots__ = (
        "_client",
        "_token",
        "_pool",
        "_resource_id",
        "_consumer",
        "_slots",
        "_started_at",
        "_expires_at",
        "_heartbeat_at",
        "_priority",
        "_use_limit",
        "_used",
        "_trace_id",
        "_target",
        "_outcome",
        "_payload",
        "_resource",
        "_released",
        "_replayed",
    )

    def __init__(self, client: LocalSchedulerClient, data: dict[str, Any]) -> None:
        self._client = client
        self._token = str(data["token"])
        self._pool = str(data["pool"])
        self._resource_id = str(data["resource_id"])
        self._consumer = data.get("consumer")
        self._slots = int(data.get("slots", 1))
        self._started_at = float(data.get("started_at", time.time()))
        self._expires_at = float(data.get("expires_at", time.time()))
        self._heartbeat_at = float(data.get("heartbeat_at", self._started_at))
        self._priority = int(data.get("priority", 0))
        self._use_limit = data.get("use_limit")
        self._used = int(data.get("used", 0))
        self._trace_id = data.get("trace_id")
        self._target = data.get("target")
        self._outcome = UsageOutcome(str(data.get("outcome", UsageOutcome.OK.value)))
        self._payload = data.get("payload")
        self._resource = _resource_from_dict(data.get("resource"))
        self._released = False
        self._replayed = bool(data.get("replayed"))

    # -- identity ---------------------------------------------------------
    @property
    def token(self) -> str:
        return self._token

    @property
    def pool(self) -> str:
        return self._pool

    @property
    def resource_id(self) -> str:
        return self._resource_id

    @property
    def consumer(self) -> str | None:
        return self._consumer

    @property
    def slots(self) -> int:
        return self._slots

    @property
    def started_at(self) -> float:
        return self._started_at

    @property
    def expires_at(self) -> float:
        return self._expires_at

    @property
    def heartbeat_at(self) -> float:
        return self._heartbeat_at

    @property
    def priority(self) -> int:
        return self._priority

    @property
    def use_limit(self) -> int | None:
        return self._use_limit

    @property
    def trace_id(self) -> str | None:
        return self._trace_id

    @property
    def target(self) -> str | None:
        return self._target

    @property
    def payload(self) -> dict[str, Any] | None:
        return self._payload

    @payload.setter
    def payload(self, value: dict[str, Any] | None) -> None:
        self._payload = value

    @property
    def resource(self) -> Resource | None:
        return self._resource

    @property
    def released(self) -> bool:
        return self._released

    @property
    def replayed(self) -> bool:
        return self._replayed

    @property
    def used(self) -> int:
        return self._used

    @used.setter
    def used(self, value: int) -> None:
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise ValueError("used must be a non-negative integer")
        self._used = value

    @property
    def outcome(self) -> UsageOutcome:
        return self._outcome

    @outcome.setter
    def outcome(self, value: UsageOutcome | str) -> None:
        self._outcome = value if isinstance(value, UsageOutcome) else UsageOutcome(value)

    # -- operations -------------------------------------------------------
    def expired(self, now: float | None = None) -> bool:
        return (now or time.time()) >= self._expires_at

    def renew(self) -> bool:
        expires_at = self._client.renew(self._pool, self._token)
        self._expires_at = expires_at
        self._heartbeat_at = time.time()
        return True

    def release(
        self,
        *,
        outcome: UsageOutcome | None = None,
        used: int | None = None,
        idempotency_key: str | None = None,
    ) -> bool:
        if self._released:
            return True
        result = self._client.release(
            self._pool,
            self._token,
            outcome=self._outcome if outcome is None else outcome,
            used=self._used if used is None else used,
            idempotency_key=idempotency_key,
        )
        self._released = True
        return result

    def __enter__(self) -> RemoteLease:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.release()

    def __repr__(self) -> str:
        return (
            f"RemoteLease(pool={self._pool!r}, resource_id={self._resource_id!r}, "
            f"token={self._token!r}, used={self._used}, outcome={self._outcome.value!r})"
        )


class LocalSchedulerClient:
    """Blocking client for one local scheduler process.

    Each calling thread gets its own connection, so a long ``acquire`` does not
    serialize other threads.  The client itself is process-local; forked
    children detect the PID change and reconnect lazily.
    """

    def __init__(self, address: SchedulerAddress) -> None:
        if address.protocol != 1:
            raise ConfigError(f"unsupported scheduler protocol {address.protocol}")
        self.address = address
        self._local = threading.local()
        self._conns: list[Any] = []
        self._conns_lock = threading.Lock()
        self._ids = itertools.count(1)
        self._ids_lock = threading.Lock()
        self._pid = os.getpid()
        self._closed = False

    # -- transport --------------------------------------------------------
    def _ensure_pid(self) -> None:
        if os.getpid() == self._pid:
            return
        # A forked child inherited the parent's sockets.  Drop the references
        # without closing them: the parent still owns its copies.
        self._local = threading.local()
        self._conns = []
        self._pid = os.getpid()
        self._closed = False

    def _conn(self) -> Any:
        self._ensure_pid()
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            return conn
        try:
            conn = Client(
                self.address.address,
                family=self.address.family,
                authkey=self.address.authkey,
            )
        except (OSError, EOFError) as exc:
            raise SchedulerUnavailable(
                f"cannot connect to local scheduler at {self.address.address!r}"
            ) from exc
        self._local.conn = conn
        with self._conns_lock:
            self._conns.append(conn)
        return conn

    def _drop_conn(self, conn: Any) -> None:
        with contextlib.suppress(Exception):
            conn.close()
        if getattr(self._local, "conn", None) is conn:
            self._local.conn = None
        with self._conns_lock, contextlib.suppress(ValueError):
            self._conns.remove(conn)

    def _request(
        self,
        op: str,
        *,
        timeout: float | None = None,
        **payload: Any,
    ) -> dict[str, Any]:
        if self._closed:
            raise SchedulerUnavailable("local scheduler client is closed")
        conn = self._conn()
        with self._ids_lock:
            request_id = next(self._ids)
        message = {"id": request_id, "op": op, **payload}
        try:
            send_message(conn, message)
            response = recv_message(conn, timeout)
        except (SchedulerTimeout, SchedulerUnavailable, SchedulerProtocolError):
            self._drop_conn(conn)
            raise
        if response.get("id") != request_id:
            self._drop_conn(conn)
            raise SchedulerProtocolError("scheduler response id does not match request")
        if not response.get("ok"):
            error = response.get("error")
            if not isinstance(error, dict):
                raise SchedulerProtocolError("scheduler error response is malformed")
            raise remote_error(
                str(error.get("code", "purser_error")),
                str(error.get("message", "scheduler operation failed")),
            )
        result = response.get("result")
        if not isinstance(result, dict):
            raise SchedulerProtocolError("scheduler result must be a JSON object")
        return result

    # -- data plane -------------------------------------------------------
    def acquire(
        self,
        pool: str,
        req: AcquireRequest | dict[str, Any] | None = None,
        *,
        timeout: float | None = None,
        idempotency_key: str | None = None,
        **kwargs: Any,
    ) -> RemoteLease:
        request = _request_payload(req)
        request.update(kwargs)
        if timeout is not None:
            request["timeout"] = timeout
        response_timeout = None
        if request.get("timeout") is not None:
            response_timeout = float(request["timeout"]) + _ACQUIRE_RESPONSE_GRACE
        else:
            response_timeout = _DEFAULT_RESPONSE_TIMEOUT
        result = self._request(
            "acquire",
            pool=pool,
            request=request,
            idempotency_key=idempotency_key,
            timeout=response_timeout,
        )
        lease = result.get("lease")
        if not isinstance(lease, dict):
            raise SchedulerProtocolError("scheduler did not return a lease")
        return RemoteLease(self, lease)

    def release(
        self,
        pool: str,
        lease: RemoteLease | str,
        *,
        outcome: UsageOutcome | str | None = None,
        used: int | None = None,
        idempotency_key: str | None = None,
    ) -> bool:
        token = lease.token if isinstance(lease, RemoteLease) else str(lease)
        if outcome is None and isinstance(lease, RemoteLease):
            outcome = lease.outcome
        if used is None and isinstance(lease, RemoteLease):
            used = lease.used
        result = self._request(
            "release",
            pool=pool,
            token=token,
            outcome=None if outcome is None else UsageOutcome(outcome).value,
            used=used,
            idempotency_key=idempotency_key,
            timeout=30.0,
        )
        return bool(result.get("ok", False))

    def renew(self, pool: str, lease: RemoteLease | str) -> float:
        token = lease.token if isinstance(lease, RemoteLease) else str(lease)
        result = self._request("renew", pool=pool, token=token, timeout=30.0)
        return float(result["expires_at"])

    @contextmanager
    def lease(
        self,
        pool: str,
        req: AcquireRequest | dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Iterator[RemoteLease]:
        lease = self.acquire(pool, req, **kwargs)
        try:
            yield lease
        finally:
            lease.release()

    # -- pool / resource management --------------------------------------
    def health(self) -> dict[str, Any]:
        return self._request("health", timeout=10.0)

    def pool_names(self) -> list[str]:
        result = self._request("pool_names", timeout=10.0)
        names = result.get("pools")
        if not isinstance(names, list):
            raise SchedulerProtocolError("scheduler did not return a pool list")
        return [str(name) for name in names]

    def create_pool(
        self,
        name: str,
        kind: str,
        *,
        config: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        result = self._request(
            "create_pool", name=name, kind=kind, config=config or {}, timeout=30.0
        )
        pool = result.get("pool")
        if not isinstance(pool, dict):
            raise SchedulerProtocolError("scheduler did not return pool stats")
        return pool

    def delete_pool(self, name: str, *, force: bool = False) -> dict[str, Any]:
        return self._request("delete_pool", name=name, force=force, timeout=30.0)

    def stats(self, pool: str) -> dict[str, Any]:
        result = self._request("stats", pool=pool, timeout=10.0)
        stats = result.get("stats")
        if not isinstance(stats, dict):
            raise SchedulerProtocolError("scheduler did not return pool stats")
        return stats

    def import_resources(
        self,
        pool: str,
        records: Any,
        *,
        dedup_key: str = "id",
        source: str = "manual",
        operator: str = "system",
        fail_fast: bool = False,
        import_validate: str | None = None,
        batch_id: str | None = None,
    ) -> dict[str, Any]:
        payload = [_record_payload(record) for record in records]
        return self._request(
            "import",
            pool=pool,
            records=payload,
            dedup_key=dedup_key,
            source=source,
            operator=operator,
            fail_fast=fail_fast,
            import_validate=import_validate,
            batch_id=batch_id,
            timeout=120.0,
        )

    def generate(
        self,
        pool: str,
        specs: Any,
        *,
        source: str = "builtin",
        operator: str = "system",
    ) -> dict[str, Any]:
        payload = [_record_payload(spec) for spec in specs]
        return self._request(
            "generate",
            pool=pool,
            specs=payload,
            source=source,
            operator=operator,
            timeout=120.0,
        )

    # -- lifecycle --------------------------------------------------------
    def shutdown(self) -> None:
        if self._closed:
            return
        self._request("shutdown", timeout=10.0)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        with self._conns_lock:
            conns, self._conns = self._conns, []
        for conn in conns:
            with contextlib.suppress(Exception):
                conn.close()
        self._local = threading.local()

    def __enter__(self) -> LocalSchedulerClient:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


def _request_payload(req: AcquireRequest | dict[str, Any] | None) -> dict[str, Any]:
    if req is None:
        return {}
    if isinstance(req, dict):
        return dict(req)
    if isinstance(req, AcquireRequest):
        data: dict[str, Any] = {}
        for name in AcquireRequest.__dataclass_fields__:
            value = getattr(req, name)
            if name == "tags":
                data[name] = sorted(value)
            else:
                data[name] = value
        return data
    raise TypeError("req must be AcquireRequest, dict or None")


def _record_payload(record: Any) -> dict[str, Any]:
    if isinstance(record, Resource):
        return record.to_dict()
    if isinstance(record, dict):
        return dict(record)
    raise TypeError("records must contain dict or Resource values")


# -- process-wide worker convenience -------------------------------------
_default_address: SchedulerAddress | None = None
_client: LocalSchedulerClient | None = None
_client_pid: int | None = None
_client_lock = threading.Lock()


def _coerce_address(value: Any) -> SchedulerAddress | None:
    if value is None:
        return None
    if isinstance(value, SchedulerAddress):
        return value
    if isinstance(value, dict):
        return SchedulerAddress.from_dict(value)
    if isinstance(value, str):
        return SchedulerAddress.from_env(value)
    raise TypeError("address must be SchedulerAddress, dict, str or None")


def set_default_address(address: SchedulerAddress) -> None:
    global _default_address
    _default_address = address


def init_worker(address: SchedulerAddress | dict[str, Any] | str | None = None) -> LocalSchedulerClient:
    """Pool/executor initializer: bind this process to one scheduler."""

    resolved = _coerce_address(address) or SchedulerAddress.from_env()
    if resolved is None:
        raise ConfigError(
            "no scheduler address; pass SchedulerAddress or set "
            "PURSER_LOCAL_SCHEDULER"
        )
    set_default_address(resolved)
    return get_client(resolved)


def get_client(
    address: SchedulerAddress | dict[str, Any] | str | None = None,
) -> LocalSchedulerClient:
    global _client, _client_pid
    resolved = _coerce_address(address) or _default_address
    if resolved is None:
        resolved = SchedulerAddress.from_env()
    if resolved is None:
        raise ConfigError(
            "no local scheduler address; call init_worker(address), "
            "set_default_address(address), or set PURSER_LOCAL_SCHEDULER"
        )
    with _client_lock:
        pid = os.getpid()
        if _client is None or _client_pid != pid or _client.address != resolved:
            _client = LocalSchedulerClient(resolved)
            _client_pid = pid
        return _client


def close_client() -> None:
    global _client, _client_pid
    with _client_lock:
        client, _client = _client, None
        _client_pid = None
    if client is not None:
        client.close()
