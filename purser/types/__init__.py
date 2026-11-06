"""Resource type plugins: all resource-specific behaviour lives here."""

from purser.types.account import AccountType
from purser.types.base import ResourceType, ResourceTypeRegistry
from purser.types.proxy import ProxyType

__all__ = ["ResourceType", "ResourceTypeRegistry", "ProxyType", "AccountType"]
