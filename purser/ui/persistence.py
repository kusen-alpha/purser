"""Pool registry persistence for console-created pools."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

from purser import Purser
from purser.config import parse_pool_config

POOLS_FILE_ENV = "PURSER_POOLS_FILE"

logger = logging.getLogger("purser.ui")


def _pools_file() -> Path | None:
    raw = os.environ.get(POOLS_FILE_ENV)
    return Path(raw) if raw else None


def _pool_specs(ps: Purser) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for name, pool in ps.pools.items():
        config = pool.cfg.to_dict()
        config.pop("kind", None)
        entry: dict[str, Any] = {
            "name": name,
            "kind": pool.cfg.kind,
            "config": config,
        }
        if pool.profile:
            entry["profile"] = pool.profile
        out.append(entry)
    return out


def save_pools(ps: Purser) -> None:
    """Write-through pool registry so console-created pools survive restarts."""

    path = _pools_file()
    if path is None:
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        payload = json.dumps({"pools": _pool_specs(ps)}, ensure_ascii=False, indent=2)
        tmp.write_text(payload, encoding="utf-8")
        tmp.replace(path)
    except OSError:
        logger.exception("failed to persist pool registry to %s", path)


def load_pools(ps: Purser) -> None:
    path = _pools_file()
    if path is None or not path.exists():
        return
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        logger.exception("failed to read pool registry %s", path)
        return
    specs = data.get("pools") if isinstance(data, dict) else None
    if not isinstance(specs, list):
        logger.warning("pool registry %s has no pools list", path)
        return
    for spec in specs:
        if not isinstance(spec, dict):
            continue
        name = spec.get("name")
        kind = spec.get("kind")
        if not name or not kind or ps.get_pool(name, default=None) is not None:
            continue
        try:
            ps.create_pool(
                name,
                kind,
                config=parse_pool_config(spec.get("config") or {}, mode="create"),
                profile=spec.get("profile"),
            )
        except Exception:
            logger.exception("failed to restore pool %r from %s", name, path)
