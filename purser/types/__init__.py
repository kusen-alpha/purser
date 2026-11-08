"""Resource type plugins: all resource-specific behaviour lives here."""

from purser.types.account import AccountResource, AccountType
from purser.types.base import ResourceType, ResourceTypeRegistry
from purser.types.proxy import ProxyResource, ProxyType

__all__ = [
    "ResourceType",
    "ResourceTypeRegistry",
    "ProxyType",
    "ProxyResource",
    "AccountType",
    "AccountResource",
]
