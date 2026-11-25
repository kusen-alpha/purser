"""Resource adapters: all resource-specific behaviour lives here."""

from purser.adapters.account import AccountAdapter, AccountResource
from purser.adapters.base import (
    AdapterDefaults,
    FieldSpec,
    ResourceAdapter,
    ResourceAdapterRegistry,
    ValidatorSpec,
    builtin_adapter_factories,
    clear_default_adapters,
    install_builtin_adapter,
    register,
    register_default_adapter,
    validator,
)
from purser.adapters.proxy import ProxyAdapter, ProxyResource

install_builtin_adapter(ProxyAdapter)
install_builtin_adapter(AccountAdapter)

__all__ = [
    "AdapterDefaults",
    "FieldSpec",
    "ResourceAdapter",
    "ResourceAdapterRegistry",
    "ValidatorSpec",
    "register_default_adapter",
    "register",
    "validator",
    "builtin_adapter_factories",
    "clear_default_adapters",
    "ProxyAdapter",
    "ProxyResource",
    "AccountAdapter",
    "AccountResource",
]
