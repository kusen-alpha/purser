"""Lifecycle manager for the same-host multiprocessing scheduler.

One :class:`LocalScheduler` owns exactly one scheduler process (and therefore
one ``SyncPurser``).  Worker processes never create their own purser; they
connect over IPC with :func:`~purser.local.client.get_client`.
"""

from __future__ import annotations

import contextlib
import multiprocessing
import os
import queue
import time
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from multiprocessing.process import BaseProcess
from typing import Any, cast

from purser.errors import PurserError
from purser.local.client import LocalSchedulerClient
from purser.local.errors import SchedulerUnavailable
from purser.local.protocol import (
    ENV_VAR,
    cleanup_address,
    default_address,
)
from purser.local.server import _scheduler_main


class LocalScheduler:
    """Own a scheduler process and expose a blocking client for its data plane.

    ``storage``/``coordination`` accept every combination the local library
    supports, including ``memory``/``memory``: coordination always lives in the
    scheduler process, so workers only need the IPC address.
    """

    def __init__(
        self,
        *,
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
    ) -> None:
        self.address = default_address(authkey)
        self._adapters = tuple(adapters)
        self._config: dict[str, Any] = {
            "storage": storage,
            "coordination": coordination,
            "startup_timeout": startup_timeout,
            "close_timeout": close_timeout,
        }
        if maintenance_enabled is not None:
            self._config["maintenance_enabled"] = maintenance_enabled
        if sqlite_path is not None:
            self._config["sqlite_path"] = sqlite_path
        if mysql_url is not None:
            self._config["mysql_url"] = mysql_url
        if redis_url is not None:
            self._config["redis_url"] = redis_url
        if audit_path is not None:
            self._config["audit_path"] = audit_path
        if secret_key is not None:
            self._config["secret_key"] = secret_key
        self._ctx = multiprocessing.get_context(start_method)
        self._startup_timeout = startup_timeout
        self._close_timeout = close_timeout
        self._process: BaseProcess | None = None
        self._client: LocalSchedulerClient | None = None
        self._ready = self._ctx.Event()
        self._errors = self._ctx.Queue()
        self._closed = False
        self._env_previous: str | None = None
        self._env_installed = False

    # -- lifecycle --------------------------------------------------------
    def start(self) -> LocalScheduler:
        if self._closed:
            raise SchedulerUnavailable("local scheduler is closed")
        if self._process is not None:
            return self
        process = cast(Any, self._ctx).Process(
            target=_scheduler_main,
            args=(self.address, self._config, self._ready, self._errors, self._adapters),
            name="purser-local-scheduler",
            daemon=True,
        )
        process.start()
        self._process = process
        try:
            if not self._ready.wait(self._startup_timeout):
                raise SchedulerUnavailable(
                    f"local scheduler did not start within {self._startup_timeout}s"
                )
            self._raise_startup_error()
            self._client = LocalSchedulerClient(self.address)
            self._client.health()
        except BaseException:
            self._terminate()
            raise
        return self

    def _raise_startup_error(self) -> None:
        try:
            error = self._errors.get(timeout=0.1)
        except queue.Empty:
            return
        raise SchedulerUnavailable(f"local scheduler failed to start:\n{error}")

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        client, self._client = self._client, None
        if client is not None:
            with contextlib.suppress(Exception):
                client.shutdown()
            client.close()
        self._uninstall_env()
        process = self._process
        if process is not None:
            process.join(self._close_timeout)
            if process.is_alive():
                process.terminate()
                process.join(5.0)
            if process.is_alive() and hasattr(process, "kill"):
                process.kill()
                process.join(5.0)
        self._drain_errors()
        with contextlib.suppress(Exception):
            self._errors.close()
        cleanup_address(self.address)

    def _terminate(self) -> None:
        process = self._process
        if process is not None and process.is_alive():
            process.terminate()
            process.join(5.0)
        self._drain_errors()
        cleanup_address(self.address)

    def _drain_errors(self) -> None:
        with contextlib.suppress(Exception):
            while True:
                self._errors.get_nowait()

    def __enter__(self) -> LocalScheduler:
        return self.start()

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # -- state ------------------------------------------------------------
    @property
    def client(self) -> LocalSchedulerClient:
        if self._client is None:
            raise SchedulerUnavailable("local scheduler is not running")
        return self._client

    @property
    def pid(self) -> int | None:
        return None if self._process is None else self._process.pid

    @property
    def alive(self) -> bool:
        return self._process is not None and self._process.is_alive()

    def health(self) -> dict[str, Any]:
        return self.client.health()

    # -- environment helpers ---------------------------------------------
    def install_env(self) -> None:
        """Publish the address so spawned/forked children can self-discover it."""

        if not self._env_installed:
            self._env_previous = os.environ.get(ENV_VAR)
            self._env_installed = True
        os.environ[ENV_VAR] = self.address.to_env()

    def _uninstall_env(self) -> None:
        if not self._env_installed:
            return
        self._env_installed = False
        if self._env_previous is None:
            os.environ.pop(ENV_VAR, None)
        else:
            os.environ[ENV_VAR] = self._env_previous

    @contextmanager
    def env(self) -> Iterator[LocalScheduler]:
        self.install_env()
        try:
            yield self
        finally:
            self._uninstall_env()

    # -- data plane (delegates to the scheduler process) ------------------
    def create_pool(
        self,
        name: str,
        kind: str,
        *,
        config: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self.client.create_pool(name, kind, config=config)

    def delete_pool(self, name: str, *, force: bool = False) -> dict[str, Any]:
        return self.client.delete_pool(name, force=force)

    def pool_names(self) -> list[str]:
        return self.client.pool_names()

    def stats(self, pool: str) -> dict[str, Any]:
        return self.client.stats(pool)

    def import_resources(self, pool: str, records: Any, **kwargs: Any) -> dict[str, Any]:
        return self.client.import_resources(pool, records, **kwargs)

    def generate(self, pool: str, specs: Any, **kwargs: Any) -> dict[str, Any]:
        return self.client.generate(pool, specs, **kwargs)

    def acquire(self, pool: str, req: Any = None, **kwargs: Any) -> Any:
        return self.client.acquire(pool, req, **kwargs)

    def release(self, pool: str, lease: Any, **kwargs: Any) -> bool:
        return self.client.release(pool, lease, **kwargs)

    def lease(self, pool: str, req: Any = None, **kwargs: Any) -> Any:
        return self.client.lease(pool, req, **kwargs)

    def wait_until_ready(self, timeout: float = 10.0) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        while True:
            try:
                return self.health()
            except (PurserError, OSError):
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.05)
