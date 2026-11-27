"""UI/API metadata for pool and resource scheduling fields."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from purser.allocator_registry import ALLOCATOR_NAMES
from purser.config_kinds import (
    AcquireMode,
    CoordinationKind,
    ExpirePolicy,
    ImportValidate,
    SchedulerKind,
    StorageKind,
    UnbanMode,
    _values,
)


@dataclass(frozen=True, slots=True)
class ConfigField:
    """UI/API metadata for one pool or resource scheduling field.

    Parsing still goes through ``PoolConfig`` / ``PoolConfig.__post_init__``;
    this metadata exists so every entry path can share labels, grouping,
    mutability and null/clear semantics without hard-coded form copies.
    """

    name: str
    kind: str
    group: str
    label: str
    choices: tuple[Any, ...] = ()
    minimum: float | None = None
    maximum: float | None = None
    mutable: bool = True
    nullable: bool = False
    hidden: bool = False
    unit: str = ""
    doc: str = ""
    default: Any = None
    fields: Mapping[str, ConfigField] = field(default_factory=dict)


def _meta(group: str, label: str, **extra: Any) -> dict[str, Any]:
    return {"group": group, "label": label, **extra}


_FIELD_META: dict[str, dict[str, Any]] = {
    "storage": _meta(
        "deployment", "存储后端", kind="enum", choices=_values(StorageKind), mutable=False
    ),
    "coordination": _meta(
        "deployment",
        "协调后端",
        kind="enum",
        choices=_values(CoordinationKind),
        mutable=False,
    ),
    "secret_fields": _meta(
        "deployment",
        "加密字段",
        kind="array",
        mutable=False,
        doc="落库前加密的 payload 字段名",
    ),
    "allocator": _meta(
        "scheduling", "分配器", kind="enum", choices=tuple(sorted(ALLOCATOR_NAMES))
    ),
    # priority_quota (default), fifo (arrival order) or token_bucket
    # (per-consumer rate_limit/rate_burst admission).
    "scheduler": _meta(
        "scheduling", "队列策略", kind="enum", choices=_values(SchedulerKind)
    ),
    "acquire_mode": _meta(
        "scheduling",
        "获取模式",
        kind="enum",
        choices=_values(AcquireMode),
        doc="lease=占有租约；stateless=无占用取样（拿完即用，无需归还）",
    ),
    "max_leases_per_consumer": _meta(
        "scheduling", "消费者并发上限", kind="int", minimum=1, nullable=True
    ),
    "rate_limit": _meta(
        "scheduling",
        "获取速率",
        kind="float",
        minimum=0.001,
        unit="req/s",
        nullable=True,
        doc="scheduler=token_bucket 时每个消费者每秒允许的获取次数",
    ),
    "rate_burst": _meta(
        "scheduling", "突发容量", kind="int", minimum=1, nullable=True
    ),
    "max_slots": _meta("scheduling", "每资源槽位", kind="int", minimum=1, nullable=True),
    "top_k": _meta("scheduling", "候选集大小", kind="int", minimum=1),
    "affinity_weight": _meta(
        "scheduling", "亲和权重", kind="float", minimum=0.0
    ),
    "weights": _meta("scheduling", "分配权重", kind="object"),
    "unban_mode": _meta(
        "lifecycle", "解禁模式", kind="enum", choices=_values(UnbanMode)
    ),
    "unban_after": _meta("lifecycle", "自动解禁时间", kind="float", minimum=0.0, unit="s"),
    "quarantine": _meta("lifecycle", "隔离策略", kind="object"),
    "expire_policy": _meta(
        "lifecycle", "过期策略", kind="enum", choices=_values(ExpirePolicy)
    ),
    "requalify_on_release": _meta("lifecycle", "隔离后重新验活", kind="bool"),
    "expires_in": _meta(
        "lifecycle", "默认有效期", kind="float", minimum=0.0, unit="s", nullable=True
    ),
    "archive_retention": _meta(
        "lifecycle", "归档保留", kind="float", minimum=0.0, unit="s"
    ),
    "lost_retention": _meta(
        "lifecycle", "失联保留", kind="float", minimum=0.0, unit="s"
    ),
    "expired_retention": _meta(
        "lifecycle", "过期保留", kind="float", minimum=0.0, unit="s", nullable=True
    ),
    "lease_ttl": _meta("lease", "租约 TTL", kind="float", minimum=0.001, unit="s"),
    "heartbeat_interval": _meta(
        "lease", "心跳间隔", kind="float", minimum=0.01, unit="s", nullable=True
    ),
    "max_lease_duration": _meta(
        "lease", "租约最长时间", kind="float", minimum=0.001, unit="s", nullable=True
    ),
    "wait_timeout": _meta("lease", "默认等待", kind="float", minimum=0.0, unit="s"),
    "cooldown_after_release": _meta(
        "quota", "释放后冷却", kind="float", minimum=0.0, unit="s"
    ),
    "cooldown_consumer_only": _meta("quota", "冷却仅限 consumer", kind="bool"),
    "quota_cooldown": _meta("quota", "配额冷却", kind="float", minimum=0.0, unit="s"),
    "usage_quota": _meta(
        "quota", "使用次数上限", kind="int", minimum=0, nullable=True
    ),
    "quota_limit": _meta(
        "quota", "周期配额上限", kind="int", minimum=1, nullable=True
    ),
    "import_validate": _meta(
        "validation", "导入验活", kind="enum", choices=_values(ImportValidate)
    ),
    "stale_after": _meta(
        "validation", "验活陈旧阈值", kind="float", minimum=0.0, unit="s"
    ),
    "background_interval": _meta(
        "validation", "后台验活间隔", kind="float", minimum=0.0, unit="s"
    ),
    "background_validate_sample": _meta(
        "validation",
        "后台验活采样率",
        kind="float",
        minimum=0.0,
        maximum=1.0,
    ),
    "validate_timeout": _meta(
        "validation", "验活超时", kind="float", minimum=0.0, unit="s"
    ),
    "validate_workers": _meta(
        "validation", "验活 worker 数", kind="int", minimum=1
    ),
    "site_failure_ttl": _meta(
        "resilience", "站点故障 TTL", kind="float", minimum=0.0, unit="s"
    ),
    "site_failure_max": _meta("resilience", "站点故障上限", kind="int", minimum=1),
    "check_cache_ttl": _meta(
        "resilience", "请求检查缓存", kind="float", minimum=0.0, unit="s"
    ),
    "timezone": _meta("advanced", "时区", kind="string"),
    "meta": _meta("advanced", "自定义元数据", kind="object", nullable=True),
    "scheduler_intervals": _meta(
        "maintenance", "维护任务间隔", kind="map", nullable=True
    ),
    "fail_open": _meta("advanced", "故障放行", kind="bool"),
    "preemptible": _meta("advanced", "可抢占", kind="bool"),
    "preempt_threshold": _meta("advanced", "抢占阈值", kind="int"),
    "drain_timeout": _meta("advanced", "排空超时", kind="float"),
    "schema_version": _meta(
        "advanced", "Schema 版本", kind="int", mutable=False, hidden=True
    ),
    "event_queue_size": _meta(
        "advanced", "事件队列长度", kind="int", mutable=False, hidden=True
    ),
}
