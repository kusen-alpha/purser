"""Event bus dispatch: slow listeners are paused instead of stalling the bus."""

from __future__ import annotations

import asyncio

from purser.core.events import EventBus


async def test_slow_listener_is_paused_after_repeated_timeouts():
    bus = EventBus(listener_timeout=0.05, listener_cooldown=0.1)
    await bus.start()
    slow_calls: list[str] = []
    fast_calls: list[str] = []

    async def slow(event) -> None:
        slow_calls.append(event.type)
        await asyncio.sleep(0.5)

    async def fast(event) -> None:
        fast_calls.append(event.type)

    bus.subscribe(slow)
    bus.subscribe(fast)
    try:
        bus.publish("a", "p")
        await asyncio.sleep(0.15)  # first timeout, one strike
        bus.publish("b", "p")
        await asyncio.sleep(0.15)  # second timeout -> disabled
        bus.publish("c", "p")
        await asyncio.sleep(0.15)
        assert "c" in fast_calls  # healthy listener keeps receiving
        assert "c" not in slow_calls  # slow listener is paused
    finally:
        await bus.stop()


async def test_bus_overflow_drops_oldest_never_blocks():
    bus = EventBus(maxsize=5)
    for i in range(20):
        bus.publish(f"e{i}", "p")
    assert bus.last_seq == 20
    assert bus.dropped == 15
    history = [e.seq for e in bus.history(after_seq=0)]
    assert history == [16, 17, 18, 19, 20]


async def test_broken_listener_does_not_block_others():
    bus = EventBus(listener_timeout=1.0)
    await bus.start()
    seen: list[str] = []

    def broken(event) -> None:
        raise RuntimeError("boom")

    async def listening(event) -> None:
        seen.append(event.type)

    bus.subscribe(broken)
    bus.subscribe(listening)
    try:
        for i in range(3):
            bus.publish(f"e{i}", "p")
        for _ in range(100):
            if len(seen) >= 3:
                break
            await asyncio.sleep(0.01)
        assert seen == ["e0", "e1", "e2"]
        assert bus.dropped >= 3
    finally:
        await bus.stop()


async def test_history_survives_dispatch():
    bus = EventBus(maxsize=10, listener_timeout=1.0)
    seen: list[str] = []

    async def listening(event) -> None:
        seen.append(event.type)

    bus.subscribe(listening)
    for i in range(3):
        bus.publish(f"e{i}", "p")
    await bus.start()
    try:
        for _ in range(100):
            if len(seen) >= 3:
                break
            await asyncio.sleep(0.01)
        assert seen == ["e0", "e1", "e2"]
        replay = bus.history(after_seq=0)
        assert [e.type for e in replay] == ["e0", "e1", "e2"]
    finally:
        await bus.stop()


async def test_unsubscribe_stops_delivery():
    bus = EventBus(listener_timeout=1.0)
    await bus.start()
    seen: list[str] = []

    async def listening(event) -> None:
        seen.append(event.type)

    unsub = bus.subscribe(listening)
    bus.publish("a", "p")
    for _ in range(50):
        if "a" in seen:
            break
        await asyncio.sleep(0.01)
    assert seen == ["a"]
    unsub()
    bus.publish("b", "p")
    await asyncio.sleep(0.1)
    assert seen == ["a"]
    await bus.stop()
