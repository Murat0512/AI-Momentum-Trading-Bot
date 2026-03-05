"""
tests/test_event_bus.py — Unit tests for events.bus and events.types.
"""

import pytest

from events.bus import EventBus
from events.sinks.test_sink import TestSink
from events.types import (
    DomainEvent,
    OrderCancelled,
    OrderFilled,
    OrderSubmitted,
    SlippageRecorded,
)


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def bus():
    """Fresh EventBus for each test (does not mutate the global singleton)."""
    return EventBus()


@pytest.fixture
def sink():
    return TestSink()


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────


class TestEventBusBasics:
    def test_sink_receives_published_events(self, bus, sink):
        bus.register(sink)
        evt = OrderSubmitted(
            cycle_id=1, order_id="ORD1", ticker="NVDA", side="buy", qty=100
        )
        bus.publish(evt)
        assert len(sink) == 1
        assert sink.events[0] is evt

    def test_multiple_sinks_all_receive(self, bus):
        s1, s2 = TestSink(), TestSink()
        bus.register(s1)
        bus.register(s2)
        bus.publish(OrderFilled(cycle_id=2, order_id="ORD2", ticker="AMD"))
        assert len(s1) == 1
        assert len(s2) == 1

    def test_of_type_filters_correctly(self, bus, sink):
        bus.register(sink)
        bus.publish(OrderSubmitted(cycle_id=1, order_id="A", ticker="NVDA"))
        bus.publish(OrderFilled(cycle_id=1, order_id="A", ticker="NVDA", filled_qty=50))
        bus.publish(SlippageRecorded(cycle_id=1, ticker="NVDA", action="SLIPPAGE_OK"))

        assert len(sink.of_type(OrderSubmitted)) == 1
        assert len(sink.of_type(OrderFilled)) == 1
        assert len(sink.of_type(SlippageRecorded)) == 1
        assert len(sink.of_type(OrderCancelled)) == 0

    def test_sink_failure_does_not_propagate(self, bus):
        """A crashing sink must not prevent other sinks from receiving events."""
        received = []

        def bad_sink(evt):
            raise RuntimeError("sink exploded")

        def good_sink(evt):
            received.append(evt)

        bus.register(bad_sink)
        bus.register(good_sink)
        bus.publish(OrderFilled(cycle_id=3, ticker="TSLA"))
        assert len(received) == 1  # good_sink still ran

    def test_event_is_frozen_immutable(self):
        evt = OrderSubmitted(cycle_id=1, order_id="X", ticker="NVDA")
        with pytest.raises((AttributeError, TypeError)):
            evt.ticker = "AMD"  # type: ignore[misc]

    def test_cycle_id_preserved(self, bus, sink):
        bus.register(sink)
        bus.publish(OrderFilled(cycle_id=42, order_id="Z", ticker="AAPL"))
        assert sink.events[0].cycle_id == 42

    def test_clear_sinks(self, bus, sink):
        bus.register(sink)
        bus.clear_sinks()
        bus.publish(OrderFilled(ticker="SPY"))
        assert len(sink) == 0

    def test_unregister_sink(self, bus, sink):
        bus.register(sink)
        bus.unregister(sink)
        bus.publish(OrderFilled(ticker="SPY"))
        assert len(sink) == 0


class TestTestSinkHelpers:
    def test_latest_raises_if_no_match(self):
        s = TestSink()
        with pytest.raises(IndexError):
            s.latest(OrderFilled)

    def test_latest_returns_most_recent(self):
        s = TestSink()
        s(OrderFilled(cycle_id=1, ticker="A"))
        s(OrderFilled(cycle_id=2, ticker="B"))
        assert s.latest(OrderFilled).ticker == "B"

    def test_clear_resets_events(self):
        s = TestSink()
        s(OrderFilled(ticker="X"))
        s.clear()
        assert len(s) == 0
        assert s.count() == 0

    def test_count_with_type_filter(self):
        s = TestSink()
        s(OrderFilled(ticker="A"))
        s(OrderFilled(ticker="B"))
        s(OrderSubmitted(ticker="C"))
        assert s.count(OrderFilled) == 2
        assert s.count(OrderSubmitted) == 1
        assert s.count() == 3
