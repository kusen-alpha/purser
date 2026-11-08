# Resource / ResourceType

`Resource` 是存储与租约共同引用的领域对象；`ResourceType` 是每类资源的插件，负责生成、验活、请求级校验与生命周期钩子。内置 `proxy` / `account` 的字段见[内置资源类型](../resource-types.md)，自定义类型的完整示例见[自定义资源类型](../../guides/custom-resource-type.md)。

## `Resource` 字段

```python
Resource(
    id, kind, payload=None, state=ResourceState.NEW, version=0,
    tags=frozenset(), concurrency=Concurrency(), health=HealthStats(),
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
| `concurrency` | `Concurrency` | `max_slots` / `used_slots` 槽位账本 |
| `health` | `HealthStats` | EWMA 健康分与延迟/错误记录 |
| `site_failures` | `dict[str, SiteFailure]` | 按目标站点的失败标记与退避窗口 |
| `quota` | `Quota \| None` | 周期配额（按日重置） |
| `usage_quota` | `int \| None` | 生命周期累计使用上限 |
| `used_count` | `int` | 累计使用量（配额与 `usage_quota` 的计量依据） |
| `expires_at` | `float \| None` | 绝对过期时刻（epoch 秒）；过期后不再参与分配 |
| `next_available_at` | `float \| None` | 冷却结束时刻；与消费者冷却、隔离合并为“最早可用时间” |
| `mutex_group` | `str \| None` | 互斥组；同一 consumer 同时只能持有组内一个资源 |
| `schedule_window` | `tuple[str, str] \| None` | `("HH:MM", "HH:MM")`，支持跨夜 |
| `meta` | `dict` | 批次、来源、创建/更新时间、冷却与隔离等内部标记 |

嵌套结构：

| 类型 | 字段 |
| --- | --- |
| `Concurrency` | `max_slots`、`used_slots`；属性 `available`、`load_ratio` |
| `HealthStats` | `score`（冷启动 `0.5`）、`samples`、`last_latency_ms`、`last_checked_at`、`last_error`、`consecutive_failures`；`score < 0.35` 视为降级 |
| `Quota` | `limit`、`used`、`reset_at`；`exhausted` 表示 `used >= limit` |
| `SiteFailure` | `site`、`count`、`since`、`until`；`active(now)` 判断失败窗口是否仍生效 |

### 属性与方法

| 成员 | 说明 |
| --- | --- |
| `payload` | 深拷贝合并视图；读取修改不会写回资源 |
| `extra` | 未声明为类型化字段的自定义键深拷贝 |
| `available_slots` | `max_slots - used_slots` |
| `allocatable` | `state ∈ {IDLE, ACTIVE}` 且仍有可用槽位 |
| `created_at` / `updated_at` / `last_used_at` | 从 `meta` 读取的时间戳 |
| `next_usable_at(now=None, consumer=None)` | 冷却、消费者冷却与隔离结束时间的最大值；绝对过期由过滤层单独处理 |
| `normalize_payload(require=False)` | 规范化类型化字段并填默认值；`require=True` 在写入边界强制必填 |
| `to_dict()` | 存储形态的深拷贝，类型化字段折回 `payload`，状态与标签转为 JSON 友好值 |
| `copy_with(payload=None, **changes)` | 受支持的复制入口，可整体替换 payload |
| `mark_updated(now=None)` | 刷新 `meta["updated_at"]` |

**序列化契约**：`dataclasses.asdict()` 会暴露 `_payload`，`dataclasses.replace(..., payload=...)` 无法应用类型化键，二者都不支持。请使用 `to_dict()` / `copy_with()`。

## 类型化 payload

```python
from dataclasses import dataclass

from purser import PayloadField, Resource


@dataclass
class SmsResource(Resource):
    kind: str = "sms"
    payload_fields = (
        PayloadField("phone", required=True, doc="E.164 手机号"),
        PayloadField("retries", coerce=int, default=0),
        PayloadField("api_key", secret=True),
    )
    phone: str | None = None
    retries: int | None = None
    api_key: str | None = None
```

| `PayloadField` 字段 | 默认 | 说明 |
| --- | --- | --- |
| `name` | — | 必须是 dataclass 字段名 |
| `coerce` | `None` | 纯且幂等的规范化函数；失败统一转成 `ValueError` |
| `required` | `False` | 仅写入边界（`adopt`）强制“存在且非空字符串”；`0` / `False` 合法 |
| `secret` | `False` | 推导 `secret_fields`，存储层对该键加密落库 |
| `default` | `None` | 读取时进入派生视图，每次应用深拷贝；只在下一次写入时持久化 |
| `doc` | `""` | 供 UI/文档展示 |

- 子类声明 `payload_fields` 时，`__init_subclass__` 沿 MRO 合并所有父类声明（近者覆盖远者），拒绝重复名、`ClassVar` 与非 dataclass 字段，并生成 `typed_payload_fields`。派生结果在类创建时快照，运行期再改 `payload_fields` 不生效。
- 规范化发生在：构造（`payload` 与类型化 kwargs 两条路径等价）、`copy_with`、`payload` 赋值、`adopt`、`hydrate`。直接给类型化属性赋值不会被拦截，批量修改后调用 `normalize_payload()`。
- `resource.payload = {...}` 会替换整个类型化视图：未包含的声明字段被清空，未声明的键进入 `extra`。
- 存储后端始终通过合并后的 `payload` 读写；`secret=True` 的键在支持加密的后端中密文落库。

## `ResourceType` 插件

```python
class ResourceType(ABC):
    kind: str = ""
    resource_class: type[Resource] = Resource
    secret_fields: tuple[str, ...] = ()
    default_concurrency: int = 1

    async def generate(self, spec: dict[str, Any]) -> Resource: ...
    async def validate(self, resource: Resource) -> HealthReport: ...
    async def on_request_check(self, resource: Resource, req: AcquireRequest) -> CheckVerdict: ...
    async def on_release(self, resource: Resource, outcome: UsageOutcome) -> None: ...
    async def on_state_changed(self, resource: Resource, old: ResourceState,
                               new: ResourceState, reason: str) -> None: ...
    async def on_error(self, context: str, error: Exception) -> None: ...
```

| 成员 | 说明 |
| --- | --- |
| `kind` | 必填；建池与导入按它选择插件 |
| `resource_class` | 存储读回时重建的类型化子类；在类体中声明，类创建后赋值不会被观察 |
| `secret_fields` | 缺省由 `resource_class.payload_fields` 中 `secret=True` 推导；显式声明优先 |
| `default_concurrency` | 建池未给 `max_slots` 时的默认槽位数 |
| `generate(spec)` | 必实现：把导入字典转成 `NEW` 资源 |
| `validate(resource)` | 必实现：返回 `HealthReport`，决定验活后的状态流转 |
| `on_request_check(resource, req)` | 请求级校验；`PASS` 允许、`FAIL` 跳过该候选、`BAN` 走内核封禁路径 |
| `on_release(resource, outcome)` | 归还后的观察钩子 |
| `on_state_changed(resource, old, new, reason)` | 状态迁移观察钩子，不得改变主流程 |
| `on_error(context, error)` | 钩子自身抛错时由内核回调，不会把异常传播回主流程 |
| `adopt(resource)` | 写入边界：重建 `resource_class`、校验 kind/id/payload、执行 `normalize_payload(require=True)` |
| `hydrate(resource)` | 读取边界：重建类型化子类并执行 `normalize_payload()`，不强制 required |
| `describe(resource)` | 面向日志的单行摘要，默认 `kind:id [state]` |

`HealthReport` 字段：`alive` / `latency_ms` / `error` / `action`；`action` 取 `ok` / `revalidate` / `ban` / `drop`。`CheckVerdict` 取 `pass` / `fail` / `ban`，均可从顶层 `purser` 导入（`HealthReport`、`CheckVerdict`）。

注册方式：

```python
ps.register_type(MyType())
pool = ps.create_pool("demo", "my-kind")
```

同一 `kind` 重复注册会覆盖旧插件；已存在的池继续持有创建时的类型实例。

## 与存储的关系

- `storage` 只保存通用 `Resource` 形态与合并后的 payload；读取时由 `ResourceType.hydrate()` 恢复类型化子类。
- `adopt()` 与 `hydrate()` 的分工保证了“写入严格、读取宽容”：导入时缺必填字段立即失败，历史数据读取时只做规范化而不拒绝。
- 后端如何编解码 payload、密文字段与版本见[StorageBackend / Coordination](backends.md)。
