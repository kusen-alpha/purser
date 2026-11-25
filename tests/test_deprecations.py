"""Deprecated pre-0.6 spellings still resolve, but warn once per access."""

from __future__ import annotations

import warnings

import pytest

import purser
import purser.errors
from purser import LeaseLost, LeaseRevoked, Resource
from purser.adapters.base import register_default_adapter


def test_lease_error_aliases_warn_at_package_level() -> None:
    with pytest.warns(DeprecationWarning, match="LeaseRevokedError"):
        assert purser.LeaseRevokedError is LeaseRevoked
    with pytest.warns(DeprecationWarning, match="LeaseLostError"):
        assert purser.LeaseLostError is LeaseLost


def test_lease_error_aliases_warn_in_errors_module() -> None:
    with pytest.warns(DeprecationWarning, match="purser.errors.LeaseRevokedError"):
        assert purser.errors.LeaseRevokedError is LeaseRevoked
    with pytest.warns(DeprecationWarning, match="purser.errors.LeaseLostError"):
        assert purser.errors.LeaseLostError is LeaseLost


def test_register_alias_warns_and_points_at_the_new_decorator() -> None:
    with pytest.warns(DeprecationWarning, match="register_default_adapter"):
        alias = purser.register
    assert alias is register_default_adapter


def test_next_usable_at_alias_warns_and_delegates() -> None:
    res = Resource(id="r1", kind="proxy")
    with pytest.warns(DeprecationWarning, match="usable_at"):
        assert res.next_usable_at(0.0) == res.usable_at(0.0)


def test_unknown_attributes_still_raise() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        with pytest.raises(AttributeError):
            _ = purser.NotAThing  # type: ignore[attr-defined]
