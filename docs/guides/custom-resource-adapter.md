# 自定义资源适配器

内核不认识“代理”或“账号”，只认识 `ResourceAdapter` 插件。自定义适配器负责三件事：怎么生成、怎么验活、怎么在请求边界做校验；这三件事都有默认实现，只有需要定制时才覆盖对应钩子。

## 最小实现

```python
from purser import Purser, ResourceAdapter


class SmsAdapter(ResourceAdapter):
    kind = "sms"

ps = Purser()
ps.register_adapter(SmsAdapter())
pool = ps.create_pool("sms", "sms")
```

`generate` 与 `validate` 都有默认实现：前者按类型化字段把 spec 映射成 `Resource`（并自动补 `kind`），后者返回 `HealthReport.ok(latency_ms=1.0)` 视为存活。需要真实验活或特殊构造时再覆盖：

```python
from purser import HealthReport, Resource, ResourceAdapter


class SmsAdapter(ResourceAdapter):
    kind = "sms"
    default_concurrency = 3

    async def generate(self, spec):
        return Resource(id=spec["id"], kind=self.kind, payload=spec)

    async def validate(self, resource):
        return HealthReport.ok(latency_ms=5.0)
```

## 默认 generate 的字段映射

默认 `generate` 按声明出的字段表把 spec 键逐一对应为类型化字段，未声明的键进入 `payload`，`id` / `tags` 自动提取，缺 `id` 时用 `derive_id()` 生成稳定 id（`{kind}-{hash}`），并经过 `adopt` 执行必填校验、类型转换与默认值：

```python
class SmsAdapter(ResourceAdapter):
    kind = "sms"
    resource_class = SmsResource  # 声明了 phone / retries 等类型化字段

    async def validate(self, resource):
        return HealthReport.ok(latency_ms=5.0)
```

```python
await pool.import_resources([{"id": "sms-001", "phone": "+8613800138000", "retries": 2}])
```

`phone`、`retries` 会落在类型化字段上；若 spec 里出现未声明字段（如 `note`），会进入 `resource.extra`。需要自定义 id（如 `proxy://host:port`）时，覆盖 `derive_id(spec)` 或整个 `generate`。

## 类属性

| 属性 | 默认 | 说明 |
| --- | --- | --- |
| `kind` | — | 类型标识，建池时使用 |
| `resource_class` | `Resource` | 存储读回时重建的类型化子类 |
| `secret_fields` | `()` | 需要加密的 payload 字段；默认由声明字段里的 `PayloadField(secret=True)` 推导 |
| `default_concurrency` | `1` | 资源默认槽位数 |

## 生命周期钩子

```python
class SmsAdapter(ResourceAdapter):
    async def on_request_check(self, resource, req) -> CheckVerdict:
        """获取前的请求级校验；默认 PASS。"""
        return CheckVerdict.PASS

    async def on_acquire(self, resource, lease) -> None:
        """租约签发后回调。"""

    async def on_renew(self, resource, lease) -> None:
        """每次续租/心跳成功后回调。"""

    async def before_release(self, resource, lease) -> None:
        """归还前回调，此时仍持有租约。"""

    async def on_revoke(self, resource, lease, reason) -> None:
        """管理员强制撤销租约后回调。"""

    async def on_lease_lost(self, resource, lease) -> None:
        """内核放弃跟踪租约时回调（心跳被拒/协调凭据消失）。"""

    async def switch(self, resource) -> Resource | None:
        """切换/轮换预留点：返回 None 表示不切换；改旧对象返回则原地更新，返回新对象则替换。"""
        return None

    async def on_release(self, resource, outcome) -> None:
        """归还时回调。"""

    async def on_state_changed(self, resource, old, new, reason) -> None:
        """状态迁移后回调；old 是迁移前快照。"""

    async def on_error(self, context, error) -> None:
        """钩子异常保护，不破坏主流程。"""
```

钩子异常由 `on_error` 承接，不会破坏核心流程；不要在钩子里做长阻塞操作。

新增的 5 个租约级钩子默认都是空实现，按调度路径依次触发：`on_acquire`（签发）、`on_renew`（每次续租）、`before_release`（归还前，持约者还没放手）、`on_revoke`（`force_revoke` 踢人）、`on_lease_lost`（超时/心跳失败被回收）。它们只做观察与副作用，不改变内核的槽位与状态流转。

`switch` 是预留的切换/轮换扩展点（默认返回 `None`）。覆写时由你决定切换逻辑：直接改 `resource` 的数据并返回它，池会原地更新该行；或返回一个全新的 `Resource`，池会导入新行并把旧行归档。对应的池方法是 `switch_resource(rid, reason=...)`，忙（有活跃租约）或终态（archived/draining/banned/lost）会拒绝切换。

## 命名校验项（validator 装饰器）

除了覆写 `validate()`，还可以把校验逻辑拆成多个命名检查项，用 `@validator` 声明。每个检查项是 `async def check(self, resource) -> HealthReport`，`name` 缺省取方法名，`both` 默认 `False`：

```python
from purser import HealthReport, ResourceAdapter, validator


class SmsAdapter(ResourceAdapter):
    kind = "sms"

    @validator(name="credential", both=True)   # 自动并入验活流程
    async def check_credential(self, resource) -> HealthReport:
        if resource.payload.get("api_key"):
            return HealthReport.ok(latency_ms=2.0)
        return HealthReport.fail("missing api_key")

    @validator(both=False)                     # 只注册，按名手动触发
    async def check_expiry(self, resource) -> HealthReport:
        ...
```

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `name` | 方法名 | 检查项名称，适配器内唯一；同类重名在类创建时抛 `ConfigError` |
| `both` | `False` | 是否自动并入生命周期验活 |

- `both=True`：执行 `validate_now()` / `import_validate="on_import"` / 后台验活时，先按声明顺序逐个运行这些检查项，首个失败即短路；失败与 `validate()` 失败同流程，进入 `QUARANTINED`、连续失败升级 `BANNED`。组合发生在 `validate_with_checks()`，覆写 `validate()` 无需自己处理。
- `both=False`：只做注册（供 UI `GET /adapters` 展示），不自动执行；需要时用 `await pool.validate_check(rid, "check_expiry")` 按名运行单条，只返回 `HealthReport`，不写健康统计、不改状态。
- 检查项沿 MRO 合并，子类同名覆盖父类；必须是 `async` 方法，否则在类创建时报 `TypeError`。

UI 侧一致性：`GET /adapters` 会在每个适配器下返回 `validators: [{"name", "both"}]`；`POST /pools/{pool}/resources/{rid}/validate` 带 `{"check": "name"}` 触发单条检查（只出报告，审计动作 `validate_check`），不带 `check` 时仍是完整生命周期验活。

## 类型化字段

```python
from dataclasses import dataclass
from typing_extensions import Annotated
from purser import PayloadField, Resource


@dataclass
class SmsResource(Resource):
    kind: str = "sms"
    phone: Annotated[str | None, PayloadField(required=True, doc="E.164 手机号")] = None
    retries: Annotated[int | None, PayloadField(coerce=int)] = None
    api_key: Annotated[str | None, PayloadField(secret=True)] = None
```

字段规则直接挂在注解上，字段名只写一次；`Annotated` 里的 `PayloadField` 元数据会在类创建时被收进字段表，驱动内核的读写边界、加密与序列化。

| 参数 | 说明 |
| --- | --- |
| `coerce` | 规范化函数，必须纯且幂等 |
| `required` | 仅写入边界（`adopt`）强制；`None` / `""` 拒绝，`0` / `False` 合法 |
| `secret` | 推导 `secret_fields`，落库时加密 |
| `default` | 每次应用深拷贝，避免实例间共享 |
| `doc` | 控制台/文档展示 |

只有带 `PayloadField` 元数据的 `Annotated` 字段才算 schema 字段，普通注解不会进入 payload。声明结果在类创建时快照，沿 MRO 合并父类字段（近者覆盖远者），运行期修改无效。

## 读写边界

- `adopt()`（导入/写入）：重建 `resource_class`，执行 `normalize_payload(require=True)`。
- `hydrate()`（get/require/query 读取）：执行 `normalize_payload()`，不强制 `required`。
- 类型化属性是写入 API；直接整体赋值 `resource.payload = {...}` 会替换类型化视图，未包含的声明字段清空。
- `resource.extra` 返回未声明自定义键的深拷贝。
- `dataclasses.asdict` 会暴露 `_payload`，不支持；请用 `to_dict()` 与 `copy_with()`。

## 注册与建池

装饰器注册（推荐）：适配器定义时登记为进程级默认，之后每个 `Purser()` 构造时自动实例化装载，实例互不共享：

```python
from purser import register

@register                      # 无参适配器
class SmsAdapter(ResourceAdapter):
    kind = "sms"

@register(probe_targets=["http://example.com:80"])   # 带构造配置
class ProxyAdapter(ResourceAdapter):
    ...
```

```python
ps = Purser()                  # 自动装载 @register 的适配器
pool = ps.create_pool("sms", "sms", config={"max_slots": 2})
```

`kind` 在默认目录里重复、或与内置 `proxy` / `account` 冲突时会直接报错；测试/多实例隔离用 `Purser(use_globals=False)` 或 `clear_default_adapters()`。

手动注册（精确控制，优先于默认目录）：

```python
ps.register_adapter(SmsAdapter())
pool = ps.create_pool("sms", "sms", config={"max_slots": 2})
```

服务模式下自定义适配器需要自行组装：

```python
from purser.ui.server import create_app

ps = Purser()
ps.register_adapter(SmsAdapter())
app = create_app(ps)
```

`PURSER_POOLS_FILE` 只恢复池配置，不会注册适配器。需要按 `Purser` 注入运行时依赖（如 `ProxyAdapter(probe_guard=...)`）的适配器，用 `register_adapter` 手动注册，不要用 `@register`。

## 测试建议

- `validate()` 在测试中返回确定性结果，避免网络依赖。
- 用 `import_validate="on_import"` 覆盖成功/失败/异常三条路径。
- 断言状态迁移与钩子调用顺序；参考 `tests/test_resource_fields.py` 与 `tests/test_api.py`。
