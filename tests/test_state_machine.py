"""Exhaustive state machine tests generated from the transition table."""

from __future__ import annotations

import pytest

from purser.core.state import (
    ADMIN_ALLOWED,
    ALL_EVENTS,
    ALL_STATES,
    TRANSITIONS,
    Event,
    ResourceState,
    StateContext,
    StateMachine,
    _acquire_target,
    _release_target,
    _undrain_target,
    _validate_fail_target,
)
from purser.errors import InvalidTransition


def test_every_state_event_pair_is_total():
    """Every (state, event) either maps to a valid target or is rejected."""

    for state in ALL_STATES:
        for event in ALL_EVENTS:
            if StateMachine.can_trigger(state, event):
                if event is Event.ADMIN_SET_STATE:
                    target = StateMachine.trigger(
                        state, event, ctx=StateContext(target_state=ResourceState.IDLE)
                    )
                else:
                    target = StateMachine.trigger(state, event)
                assert target in ALL_STATES
            else:
                with pytest.raises(InvalidTransition):
                    StateMachine.trigger(state, event)


def test_table_rows_resolve_to_valid_states():
    for state, rows in TRANSITIONS.items():
        for event, spec in rows.items():
            assert spec.event is event
            if event is Event.ADMIN_SET_STATE:
                target = StateMachine.trigger(
                    state, event, ctx=StateContext(target_state=ResourceState.IDLE)
                )
            else:
                target = StateMachine.trigger(state, event)
            assert target in ALL_STATES


@pytest.mark.parametrize(
    ("state", "used", "slots", "max_slots", "expected"),
    [
        (ResourceState.IDLE, 0, 1, 1, ResourceState.FULL),
        (ResourceState.IDLE, 0, 1, 4, ResourceState.ACTIVE),
        (ResourceState.ACTIVE, 1, 1, 2, ResourceState.FULL),
        (ResourceState.ACTIVE, 2, 1, 4, ResourceState.ACTIVE),
    ],
)
def test_acquire_target_slot_math(state, used, slots, max_slots, expected):
    ctx = StateContext(used_slots=used, slots=slots, max_slots=max_slots)
    assert _acquire_target(state, ctx) is expected


@pytest.mark.parametrize(
    ("state", "used", "slots", "max_slots", "expected"),
    [
        (ResourceState.FULL, 1, 1, 1, ResourceState.IDLE),
        (ResourceState.FULL, 2, 1, 2, ResourceState.ACTIVE),
        (ResourceState.ACTIVE, 1, 1, 4, ResourceState.IDLE),
        (ResourceState.ACTIVE, 3, 1, 4, ResourceState.ACTIVE),
    ],
)
def test_release_target_slot_math(state, used, slots, max_slots, expected):
    ctx = StateContext(used_slots=used, slots=slots, max_slots=max_slots)
    assert _release_target(state, ctx) is expected


@pytest.mark.parametrize(
    ("used", "max_slots", "expected"),
    [
        (0, 1, ResourceState.IDLE),
        (1, 1, ResourceState.FULL),
        (1, 4, ResourceState.ACTIVE),
        (4, 4, ResourceState.FULL),
    ],
)
def test_undrain_target_keeps_current_load(used, max_slots, expected):
    ctx = StateContext(used_slots=used, slots=0, max_slots=max_slots)
    assert _undrain_target(ResourceState.DRAINING, ctx) is expected
    assert (
        StateMachine.trigger(ResourceState.DRAINING, Event.UNDRAIN, ctx=ctx) is expected
    )


def test_validate_fail_escalation():
    ctx = StateContext(fail_count=1, ban_threshold=3)
    assert _validate_fail_target(ResourceState.VALIDATING, ctx) is ResourceState.QUARANTINED
    ctx = StateContext(fail_count=3, ban_threshold=3)
    assert _validate_fail_target(ResourceState.VALIDATING, ctx) is ResourceState.BANNED


def test_admin_set_state_guards():
    for target in ADMIN_ALLOWED:
        result = StateMachine.trigger(
            ResourceState.IDLE, Event.ADMIN_SET_STATE, ctx=StateContext(target_state=target)
        )
        assert result is target
    with pytest.raises(InvalidTransition):
        StateMachine.trigger(
            ResourceState.IDLE,
            Event.ADMIN_SET_STATE,
            ctx=StateContext(target_state=ResourceState.BANNED),
        )


def test_key_lifecycle_path():
    state = ResourceState.NEW
    state = StateMachine.trigger(state, Event.VALIDATE_OK)
    assert state is ResourceState.IDLE
    state = StateMachine.trigger(state, Event.ACQUIRE, ctx=StateContext(max_slots=1, slots=1))
    assert state is ResourceState.FULL
    state = StateMachine.trigger(state, Event.RELEASE, ctx=StateContext(used_slots=1, slots=1, max_slots=1))
    assert state is ResourceState.IDLE


def test_illegal_transitions_are_rejected():
    assert not StateMachine.can_trigger(ResourceState.NEW, Event.RELEASE)
    assert not StateMachine.can_trigger(ResourceState.ARCHIVED, Event.ACQUIRE)
    assert not StateMachine.can_trigger(ResourceState.BANNED, Event.SUSPEND)
    assert not StateMachine.can_trigger(ResourceState.SUSPENDED, Event.SUSPEND)
    assert not StateMachine.can_trigger(ResourceState.QUOTA_EXHAUSTED, Event.BANNED)
