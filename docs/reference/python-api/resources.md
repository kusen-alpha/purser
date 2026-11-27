# Resource / ResourceAdapter

`Resource` 是存储与租约共同引用的领域对象；`ResourceAdapter` 是每类资源的插件，负责生成、验活、请求级校验与生命周期钩子。内置 `proxy` / `account` 的字段见[内置资源适配器](../resource-adapters.md)，自定义适配器的完整示例见[自定义资源适配器](../../guides/custom-resource-adapter.md)。

## `Resource` 字段

```python
Resource(
    id, kind, payload=None, state=ResourceState.NEW, version=0,
    tags=frozenset(), slots=Slots(), health=HealthStats(),
    site_failures={}, quota=None, usage_quota=None, used_count=0,
    expires_at=None, next_available_at=None, mutex_group=None,
    schedule_window=None, meta={},
)
```

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `id` / `kind` | `str` | 池内唯一 id 与资源类型 |
| `payload` | `dict`（派生视图） | 自定义键与类型化字段合并后的视图；写入会整体替换类型化视图 |
| `state` | `ResourceState` | 12 状态之一，见[状态机](../state-machine.md) |
| `version` | `int` | 存储层乐观并发版本，由后端递增 |
| `tags` | `frozenset[str]` | 获取时按“全部包含”匹配 |
| `slots` | `Slots` | `max_slots` / `used_slots` 槽位账本 |
| `health` | `HealthStats` | EWMA 健康分与延迟/错误记录 |
| `site_failures` | `dict[str, SiteFailure]` | 按目标站点的失败标记与退避窗口 |
| `quota` | <code>Quota &#124; None</code> | 周期配额（按日重置） |
| `usage_quota` | <code>int &#124; None</code> | 生命周期累计使用上限 |
| `used_count` | `int` | 累计使用量（配额与 `usage_quota` 的计量依据） |
| `expires_at` | <code>float &#124; None</code> | 绝对过期时刻（epoch 秒）；过期后不再参与分配 |
| `next_available_at` | <code>float &#124; None</code> | 冷却结束时刻；与消费者冷却、隔离合并为“最早可用时间” |
| `mutex_group` | <code>str &#124; None</code> | 互斥组；同一 consumer 同时只能持有组内一个资源 |
| `schedule_window` | <code>tuple[str, str] &#124; None</code> | `("HH:MM", "HH:MM")`，支持跨夜 |
| `meta` | `dict` | 业务自定义元数据 + 系统标记；`user_meta` / `system_meta` 按归属拆分。框架写入的 `priority` / `preference` / `group` / `switched_from` 归入 `system_meta`；`meta` 为 `ResourceMeta`，支持类型化读写 |

嵌套结构：

| 类型 | 字段 |
| --- | --- |
| `Slots` | `max_slots`、`used_slots`；属性 `available`、`load_ratio` |
| `HealthStats` | `score`（冷启动 `0.5`）、`samples`、`last_latency_ms`、`last_checked_at`、`last_error`、`consecutive_failures`；`score < 0.35` 视为降级 |
| `Quota` | `limit`、`used`、`reset_at`；`exhausted` 表示 `used >= limit` |
| `SiteFailure` | `site`、`count`、`since`、`until`；`active(now)` 判断失败窗口是否仍生效 |

### 派生属性

| 属性 | 返回 | 说明 |
| --- | --- | --- |
| `payload` | `dict` | 深拷贝合并视图；读取修改不会写回资源 |
| `extra` | `dict` | 未声明为类型化字段的自定义键深拷贝 |
| `available_slots` | `int` | `max_slots - used_slots` |
| `allocatable` | `bool` | `state ∈ {IDLE, ACTIVE}` 且仍有可用槽位 |
| `created_at` / `updated_at` / `last_used_at` | `float` / `float` / <code>float &#124; None</code> | 从 `meta` 读取的时间戳 |
| `user_meta` / `system_meta` | `dict` | 按业务/系统归属拆分的元数据视图 |

### `normalize_payload()` {#resource-normalize-payload}

**用途**：规范化类型化字段并填充默认值；批量直接赋值后调用。

**签名**

```python
def normalize_payload(self, *, require: bool = False) -> None: ...
```

**参数**：`require=True` 时，缺失或空字符串的必填字段抛 `ValueError`；`0` / `False` 合法。

**返回**：`None`。`adopt()` 使用 `require=True`，`hydrate()` 使用默认 `False`，保证“写入严格、读取宽容”。

### `to_dict()` {#resource-to-dict}

**用途**：导出存储形态的深拷贝。

**签名**：`def to_dict() -> dict[str, Any]`

**返回**：类型化字段折回 `payload`，状态与标签转为 JSON 友好值。适合持久化、备份和日志。

### `copy_with()` {#resource-copy-with}

**用途**：复制资源并应用基础字段或类型化字段修改，可整体替换 payload。

**签名**

```python
def copy_with(
    self, *, payload: dict[str, Any] | None = None, **changes: Any
) -> Resource: ...
```

**参数**：`payload` 非 `None` 时整体替换 payload；`changes` 传给 dataclass 复制入口。

**返回**：同类型资源副本。

**注意**：`dataclasses.asdict()` 会暴露 `_payload`，`dataclasses.replace(..., payload=...)` 无法正确应用类型化键，二者都不支持。请使用 `to_dict()` / `copy_with()`。

### `mark_updated()` {#resource-mark-updated}

**签名**：`def mark_updated(now: float | None = None) -> None`

**用途**：刷新 `meta["updated_at"]`；`now` 缺省取当前时间。

### `usable_at()` {#resource-usable-at}

**用途**：计算资源最早可分配时刻。

**签名**

```python
def usable_at(
    self, now: float | None = None, consumer: str | None = None
) -> float: ...
```

**参数**：`now` 为当前 epoch 秒；`consumer` 用于合并消费者级冷却。

**返回**：`next_available_at`、消费者冷却和隔离结束时间的最大值。绝对 `expires_at` 是截止时间，不参与“等待到”计算，由过滤层单独处理。

## 类型化 payload

```python
from dataclasses import dataclass
from typing import Annotated

from purser import PayloadField, Resource


@dataclass
class SmsResource(Resource):
    kind: str = "sms"
    phone: Annotated[str | None, PayloadField(required=True, doc="E.164 手机号")] = None
    retries: Annotated[int | None, PayloadField(coerce=int, default=0)] = None
    api_key: Annotated[str | None, PayloadField(secret=True)] = None
```

| `PayloadField` 字段 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `name` | `str` | `""` | `Annotated` 写法可省略，自动取字段名；平行表写法必须传且是 dataclass 字段名 |
| `coerce` | <code>Callable &#124; None</code> | `None` | 纯且幂等的规范化函数；失败统一转成 `ValueError` |
| `required` | `bool` | `False` | 仅写入边界强制“存在且非空字符串”；`0` / `False` 合法 |
| `secret` | `bool` | `False` | 推导 `secret_fields`，存储层对该键加密落库 |
| `default` | `Any` | `None` | 读取时进入派生视图，每次深拷贝；下一次写入才持久化 |
| `doc` | `str` | `""` | 供 UI/文档展示 |

- 字段规则写在注解的 `PayloadField` 元数据里。`__init_subclass__` 沿 MRO 合并声明，拒绝 `ClassVar` 字段，并生成 `typed_payload_fields`。派生结果在类创建时快照。
- 规范化发生在构造、`copy_with`、`payload` 赋值、`adopt`、`hydrate`。直接给类型化属性赋值不会被拦截，批量修改后调用 `normalize_payload()`。
- `resource.payload = {...}` 会替换整个类型化视图：未包含的声明字段被清空，未声明的键进入 `extra`。
- 存储后端始终通过合并后的 `payload` 读写；`secret=True` 的键在支持加密的后端中密文落库。

## `ResourceAdapter` 插件

```python
class ResourceAdapter:
    kind: str = ""
    resource_class: type[Resource] = Resource
    secret_fields: tuple[str, ...] = ()
    default_slots: int = 1
```

| 类属性 | 说明 |
| --- | --- |
| `kind` | 必填；建池与导入按它选择插件 |
| `resource_class` | 存储读回时重建的类型化子类；在类体中声明，类创建后赋值不会被观察 |
| `secret_fields` | 缺省由 `PayloadField(secret=True)` 推导；显式声明优先 |
| `default_slots` | 建池未给 `max_slots` 时的默认槽位数 |

### 生成与 id

#### `generate()` {#adapter-generate}

**签名**：`async def generate(self, spec: dict[str, Any]) -> Resource`

**用途**：默认按声明字段把 spec 映射成类型化字段，未知键进入自定义 payload。

**返回与异常**：返回通过 `adopt()` 的 `Resource`。`tags` 传字符串抛 `TypeError`；必填字段缺失或类型化失败抛 `ValueError`。需要派生 id 或非一对一映射时覆盖此方法。

#### `derive_id()` {#adapter-derive-id}

**签名**：`def derive_id(self, spec: dict[str, Any]) -> str`

**用途**：spec 未提供 id 时生成稳定 id。默认对规范化 spec 做 SHA-256，取前 12 位并加 `kind` 前缀。

### 读写边界

#### `adopt()` {#adapter-adopt}

**签名**：`def adopt(self, resource: Resource) -> Resource`

**用途**：导入/写入边界：重建 `resource_class`，校验 kind/id/payload，并执行 `normalize_payload(require=True)`。

**返回与异常**：返回类型化资源。kind 不匹配、id 为空、payload 非字典或必填字段缺失时抛 `ValueError`。

#### `hydrate()` {#adapter-hydrate}

**签名**：`def hydrate(self, resource: Resource) -> Resource`

**用途**：读取边界：重建类型化子类并执行 `normalize_payload()`，不强制 required。

**返回**：类型化资源。历史数据缺字段时仍可读取。

### 验活与请求校验

#### `validate()` {#adapter-validate}

**签名**：`async def validate(self, resource: Resource) -> HealthReport`

**用途**：生命周期验活。默认返回 `HealthReport.ok(latency_ms=1.0)`；需要真实探针时覆盖。

#### `on_request_check()` {#adapter-on-request-check}

**签名**

```python
async def on_request_check(
    self, resource: Resource, req: AcquireRequest
) -> CheckVerdict: ...
```

**用途**：请求级校验。`PASS` 允许候选，`FAIL` 跳过，`BAN` 走内核封禁路径。

### 生命周期钩子

| 方法 | 签名 | 调用时机与语义 |
| --- | --- | --- |
| `on_acquire` | `async def on_acquire(resource, lease) -> None` | 租约签发并提交后 |
| `on_renew` | `async def on_renew(resource, lease) -> None` | 每次续约/心跳成功后；`resource` 是签发时快照，跨实例租约可能为 `None` |
| `before_release` | `async def before_release(resource, lease) -> None` | 归还前，持有者仍拥有租约 |
| `on_release` | `async def on_release(resource, outcome) -> None` | 归还后，接收最终 `UsageOutcome` |
| `on_revoke` | `async def on_revoke(resource, lease, reason) -> None` | 管理面强制撤销后 |
| `on_lease_lost` | `async def on_lease_lost(resource, lease) -> None` | 心跳被拒或协调 token 消失，内核放弃跟踪时 |
| `on_state_changed` | `async def on_state_changed(resource, old, new, reason) -> None` | 状态迁移观察钩子，不得改变主流程 |
| `on_error` | `async def on_error(context, error) -> None` | 其他钩子抛错时由内核回调，不把异常传播回主流程 |
| `switch` | <code>async def switch(resource) -> Resource&#124;None</code> | 预留切换点；返回修改后的旧对象原地更新，返回新对象则导入并归档旧行，`None` 不切换 |

### 命名校验链

#### `@validator()` {#adapter-validator}

**用途**：声明适配器命名校验项，供生命周期验活或按名调用。

**签名**

```python
def validator(
    name: str | None = None, *, both: bool = False
) -> Callable[..., Awaitable[HealthReport]]: ...
```

**参数**：`name` 缺省取方法名；`both=True` 时随生命周期验活自动执行，失败会短路并走正常隔离流程；`both=False` 只按名调用，不改变健康或状态。装饰的方法必须是 async。

**示例**

```python
import time

from purser import HealthReport, ResourceAdapter, validator


class MyAdapter(ResourceAdapter):
    kind = "my-kind"

    @validator(both=True)
    async def check_expiry(self, resource):
        if resource.expires_at and resource.expires_at < time.time():
            return HealthReport.fail("expired")
        return HealthReport.ok()
```

#### `run_validators()` / `validate_with_checks()` / `run_validator()` {#adapter-run-validators}

| 方法 | 签名 | 说明 |
| --- | --- | --- |
| `run_validators` | `async def run_validators(resource) -> HealthReport` | 按声明顺序执行所有 `both=True` 检查，首个失败短路 |
| `validate_with_checks` | `async def validate_with_checks(resource) -> HealthReport` | 先执行自动校验，全部通过后再调用 `validate()` |
| `run_validator` | `async def run_validator(resource, name) -> HealthReport` | 按名执行单个检查，不写健康或状态；未知名称抛 `ConfigError` |

### 公开 schema 与摘要

| 方法 | 签名 | 返回 |
| --- | --- | --- |
| `describe` | `def describe(resource) -> str` | 面向日志的单行摘要，默认 `kind:id [state]` |
| `defaults` | `def defaults() -> AdapterDefaults` | 池级默认 `max_slots` / `secret_fields`，建池时合并 |
| `fields` | `def fields() -> list[FieldSpec]` | 声明字段的公开 schema：`name` / `coerce` / `required` / `secret` / `default` / `doc` |
| `validators` | `def validators() -> list[ValidatorSpec]` | 命名校验项 schema：`name` / `both` |

`FieldSpec` / `ValidatorSpec` / `AdapterDefaults` 可从 `purser.adapters` 导入；UI 与 CLI 只消费这三个公开描述，不依赖适配器内部实现。

## `HealthReport`

```python
HealthReport(alive, latency_ms=None, error=None, action=Action.OK)
```

| 类方法 | 签名 | 用途 |
| --- | --- | --- |
| `ok` | `HealthReport.ok(latency_ms=None) -> HealthReport` | 构造存活结果，`action=OK` |
| `fail` | `HealthReport.fail(error, *, action=Action.REVALIDATE, latency_ms=None) -> HealthReport` | 构造失败结果，可指定 `OK` / `REVALIDATE` / `BAN` / `DROP` 等动作 |

`CheckVerdict` 取 `pass` / `fail` / `ban`。`HealthReport` 与 `CheckVerdict` 均可从顶层 `purser` 导入。

## 注册与存储关系

```python
ps.register_adapter(adapter=MyAdapter())
pool = ps.create_pool(name="demo", kind="my-kind")
```

同一 `kind` 重复注册会覆盖旧适配器；已存在的池继续持有创建时的适配器实例。

- `storage` 只保存通用 `Resource` 形态与合并后的 payload；读取时由 `ResourceAdapter.hydrate()` 恢复类型化子类。
- `adopt()` 与 `hydrate()` 的分工保证“写入严格、读取宽容”：导入时缺必填字段立即失败，历史数据读取时只规范化而不拒绝。
- 后端如何编解码 payload、密文字段与版本见[StorageBackend / Coordination](backends.md)。
