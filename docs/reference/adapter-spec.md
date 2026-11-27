# AdapterSpec 与远程注册

`AdapterSpec` 是资源适配器的**可序列化描述**：一个模块路径加一组 JSON 构造参数。它解决的唯一问题是“调度进程不在当前进程里，如何将适配器传入调度进程”。

## 结构

```python
from purser import AdapterSpec

spec = AdapterSpec.of(MyAdapter, region="cn", timeout=8.0)
spec.path    # "myapp.adapters:MyAdapter"
spec.kwargs  # {"region": "cn", "timeout": 8.0}
spec.to_dict()
# {"path": "myapp.adapters:MyAdapter", "kwargs": {"region": "cn", "timeout": 8.0}}
```

| 字段 | 说明 |
| --- | --- |
| `path` | `module:qualname`，必须是模块顶层可导入对象 |
| `kwargs` | 构造参数，必须可 `json.dumps` |

`AdapterSpec.of()` 在构造时就校验：

- `target` 必须是类（不是实例）；
- `kwargs` 必须 JSON 可序列化；
- 路径不能包含 `<locals>`（即不能是函数内定义的类）。

## 加载

```python
from purser import load_adapter

adapter = load_adapter(spec)   # 等价于 import 后 MyAdapter(**kwargs)
```

`load_adapter()` 也接受映射 `{"path": ..., "kwargs": {...}}`。导入失败、路径格式错误、构造参数不匹配都会抛 `ConfigError`。

## 远程注册

```python
from purser import AdapterSpec

ps.register_adapter(adapter=AdapterSpec.of(MyAdapter, region="cn"))
```

`register_adapter()` 在远程/进程门面上接受四类目标：

| 目标 | 行为 |
| --- | --- |
| `AdapterSpec` | 原样序列化，调度进程内实例化 |
| `{"path": ..., "kwargs": {...}}` | 同上 |
| 零参类 | 自动转成 `AdapterSpec.of(cls)` |
| 零参可构造的实例 | 取其类；若构造需要参数则拒绝，提示改用 `AdapterSpec` |

带 live 状态的实例（连接、锁、缓存）不能跨 IPC 传递；请在创建调度进程时用 `adapters=[...]` 注入，或改成无状态 + 构造参数。

## 默认适配器目录

`@register_default_adapter` 注册的是**进程级默认目录**，之后创建的每个 `Purser` 会各自实例化一份：

```python
from purser import register_default_adapter

@register_default_adapter(probe_targets=["https://example.com"])
class MyProxyAdapter(ProxyAdapter):
    kind = "proxy"
```

- `kind` 必须全局唯一，不能与内置 `proxy` / `account` 或已注册条目重复。
- 可以裸用 `@register_default_adapter`，也可以带构造参数。
- `Purser(default_adapters=...)` 控制装载范围：`"global"`（默认）/ `"builtin"` / `"none"`。
- 注意：覆盖内置 kind 不能用默认目录注册（会报重复）；请用实例级 `ps.register_adapter(...)` 覆盖。

## 与服务模式的关系

`purser serve` 启动的是普通进程，默认只有内置与默认目录里的适配器。自定义适配器要进入服务：

1. **代码内组装**：`create_app(ps)`，先在 `ps` 上 `register_adapter()`；
2. **进程门面**：用 `AdapterSpec` 远程注册；
3. **默认目录**：在服务启动前 import 你的模块，让 `@register_default_adapter` 生效。

`PURSER_POOLS_FILE` 只恢复池配置，**不会**注册新适配器。

## 安全边界

IPC 已有 authkey，`AdapterSpec` 被视为可信输入；它本质上是“按路径 import 并执行构造函数”。不要用 `AdapterSpec` 加载不受信任的模块路径或参数。

## 相关页面

- 适配器插件契约：[Resource / ResourceAdapter](python-api/resources.md)
- 自定义适配器指南：[自定义资源适配器](../guides/custom-resource-adapter.md)
- 进程门面：[LocalScheduler](local-scheduler.md)、[门面族](../guides/facades.md)