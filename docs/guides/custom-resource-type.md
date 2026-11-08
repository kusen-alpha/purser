# 自定义资源类型

内核不认识“代理”或“账号”，只认识 `ResourceType` 插件。自定义类型负责三件事：怎么生成、怎么验活、怎么在请求边界做校验。

## 最小实现

```python
from purser import HealthReport, Purser, Resource, ResourceType


class SmsType(ResourceType):
    kind = "sms"
    default_concurrency = 3

    async def generate(self, spec):
        return Resource(id=spec["id"], kind=self.kind, payload=spec)

    async def validate(self, resource):
        return HealthReport.ok(latency_ms=5.0)


ps = Purser()
ps.register_type(SmsType())
pool = ps.create_pool("sms", "sms")
```

`generate` 与 `validate` 必须实现；其余钩子有默认空实现。

## 类属性

| 属性 | 默认 | 说明 |
| --- | --- | --- |
| `kind` | — | 类型标识，建池时使用 |
| `resource_class` | `Resource` | 存储读回时重建的类型化子类 |
| `secret_fields` | `()` | 需要加密的 payload 字段；默认由 `payload_fields(secret=True)` 推导 |
| `default_concurrency` | `1` | 资源默认槽位数 |

## 生命周期钩子

```python
class SmsType(ResourceType):
    async def on_request_check(self, resource, req) -> CheckVerdict:
        """获取前的请求级校验；默认 PASS。"""
        return CheckVerdict.PASS

    async def on_release(self, resource, outcome) -> None:
        """归还时回调。"""

    async def on_state_changed(self, resource, old, new, reason) -> None:
        """状态迁移后回调；old 是迁移前快照。"""

    async def on_error(self, context, error) -> None:
        """钩子异常保护，不破坏主流程。"""
```

钩子异常由 `on_error` 承接，不会破坏核心流程；不要在钩子里做长阻塞操作。

## 类型化字段

```python
from dataclasses import dataclass
from purser import PayloadField, Resource


@dataclass
class SmsResource(Resource):
    kind: str = "sms"
    payload_fields = (
        PayloadField("phone", required=True, doc="E.164 手机号"),
        PayloadField("retries", coerce=int),
        PayloadField("api_key", secret=True),
    )
    phone: str | None = None
    retries: int | None = None
    api_key: str | None = None
```

| 参数 | 说明 |
| --- | --- |
| `coerce` | 规范化函数，必须纯且幂等 |
| `required` | 仅写入边界（`adopt`）强制；`None` / `""` 拒绝，`0` / `False` 合法 |
| `secret` | 推导 `secret_fields`，落库时加密 |
| `default` | 每次应用深拷贝，避免实例间共享 |
| `doc` | 控制台/文档展示 |

子类声明 `payload_fields` 时会沿 MRO 合并父类声明（近者覆盖远者），并校验字段名确实是 dataclass 字段。派生结果在类创建时快照，运行期修改无效。

## 读写边界

- `adopt()`（导入/写入）：重建 `resource_class`，执行 `normalize_payload(require=True)`。
- `hydrate()`（get/require/query 读取）：执行 `normalize_payload()`，不强制 `required`。
- 类型化属性是写入 API；直接整体赋值 `resource.payload = {...}` 会替换类型化视图，未包含的声明字段清空。
- `resource.extra` 返回未声明自定义键的深拷贝。
- `dataclasses.asdict` 会暴露 `_payload`，不支持；请用 `to_dict()` 与 `copy_with()`。

## 注册与建池

```python
ps.register_type(SmsType())
pool = ps.create_pool("sms", "sms", config={"max_slots": 2})
```

服务模式下自定义类型需要自行组装：

```python
from purser.ui.server import create_app

ps = Purser()
ps.register_type(SmsType())
app = create_app(ps)
```

`PURSER_POOLS_FILE` 只恢复池配置，不会注册类型。

## 测试建议

- `validate()` 在测试中返回确定性结果，避免网络依赖。
- 用 `import_validate="on_import"` 覆盖成功/失败/异常三条路径。
- 断言状态迁移与钩子调用顺序；参考 `tests/test_resource_fields.py` 与 `tests/test_api.py`。
