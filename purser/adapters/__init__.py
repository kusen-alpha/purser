"""Resource adapters: all resource-specific behaviour lives here."""

from purser.adapters.account import AccountAdapter, AccountResource
from purser.adapters.base import (
    AdapterDefaults,
    FieldSpec,
    ResourceAdapter,
    ResourceAdapterRegistry,
    ValidatorSpec,
    clear_default_adapters,
    register,
    validator,
)
from purser.adapters.proxy import ProxyAdapter, ProxyResource

__all__ = [
    "AdapterDefaults",
    "FieldSpec",
    "ResourceAdapter",
    "ResourceAdapterRegistry",
    "ValidatorSpec",
    "register",
    "validator",
    "clear_default_adapters",
    "ProxyAdapter",
    "ProxyResource",
    "AccountAdapter",
    "AccountResource",
]
