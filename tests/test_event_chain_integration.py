"""
tests/test_event_chain_integration.py — Integration tests for the domain event chain.

Verifies that publishing flows from modules through the global event_bus
and that TestSink collects the right events.
"""

import pytest

from events.bus import event_bus
from events.sinks.test_sink import TestSink
from events.types import (
    HaltStateChange,
    IntegrityGateTrip,
    LifecycleTransition,
    OrderCancelled,
    OrderFilled,
    OrderPartial,
    OrderSubmitted,
    PortfolioGateResult,
    SlippageRecorded,
)


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _clean_bus():
    """Register a fresh TestSink for each test and remove it after."""
    sink = TestSink()
    event_bus.register(sink)
    yield sink
    event_bus.unregister(sink)


# ─────────────────────────────────────────────────────────────────────────────
# Tests — order_manager → bus
# ─────────────────────────────────────────────────────────────────────────────


class TestOrderManagerEvents:
    def test_submit_publishes_order_submitted(self, _clean_bus):
        from unittest.mock import MagicMock
        from execution.order_manager import OrderManager
        from config.constants import ORDER_SUBMITTED

        om = OrderManager()
        broker = MagicMock()
        result = MagicMock()
        result.success = True
        result.order_id = "B1"
        result.filled_price = 0.0
        result.filled_at = None
        broker.buy.return_value = result

        om.submit(broker, ticker="NVDA", side="buy", qty=100, limit_price=500.0)

        submitted = _clean_bus.of_type(OrderSubmitted)
        assert len(submitted) == 1
        assert submitted[0].ticker == "NVDA"

    def test_fill_publishes_order_filled(self, _clean_bus):
        from unittest.mock import MagicMock
        from execution.order_manager import OrderManager

        om = OrderManager()
        broker = MagicMock()
        result = MagicMock()
        result.success = True
        result.order_id = "B2"
        result.filled_price = 501.0
        result.filled_at = None
        broker.buy.return_value = result

        om.submit(broker, ticker="AMD", side="buy", qty=50, limit_price=200.0)

        fills = _clean_bus.of_type(OrderFilled)
        assert len(fills) == 1
        assert fills[0].ticker == "AMD"
        assert fills[0].filled_price == 501.0


# ─────────────────────────────────────────────────────────────────────────────
# Tests — slippage_monitor → bus
# ─────────────────────────────────────────────────────────────────────────────


class TestSlippageEvents:
    def test_record_fill_publishes_slippage_recorded(self, _clean_bus):
        from execution.slippage import SlippageMonitor

        monitor = SlippageMonitor()
        monitor.record_fill(
            ticker="TSLA",
            expected_price=200.0,
            fill_price=200.10,
            spread_pct=0.001,
            r_value=0.05,
        )

        events = _clean_bus.of_type(SlippageRecorded)
        assert len(events) == 1
        assert events[0].ticker == "TSLA"
        assert isinstance(events[0].action, str)


# ─────────────────────────────────────────────────────────────────────────────
# Tests — halt_machine → bus
# ─────────────────────────────────────────────────────────────────────────────


class TestHaltMachineEvents:
    def test_on_health_block_publishes_halt_state_change(self, _clean_bus):
        from execution.halt_machine import HaltStateMachine

        hm = HaltStateMachine()
        hm.on_health_block("NVDA", "test halt")

        events = _clean_bus.of_type(HaltStateChange)
        assert len(events) == 1
        assert events[0].ticker == "NVDA"
        assert "HALTED" in events[0].new_state

    def test_on_clean_tick_publishes_resuming(self, _clean_bus):
        from execution.halt_machine import HaltStateMachine, HALT_RESUMING

        hm = HaltStateMachine()
        hm.on_health_block("AMD", "test")
        _clean_bus.clear()  # clear the HALTED event
        hm.on_clean_tick("AMD")

        events = _clean_bus.of_type(HaltStateChange)
        assert len(events) == 1
        assert events[0].new_state == HALT_RESUMING


# ─────────────────────────────────────────────────────────────────────────────
# Tests — integrity_gate → bus
# ─────────────────────────────────────────────────────────────────────────────


class TestIntegrityGateEvents:
    def test_reject_loop_publishes_trip(self, _clean_bus):
        from execution.integrity_gate import IntegrityGate
        from config.settings import CONFIG

        gate = IntegrityGate()
        threshold = CONFIG.integrity_gate.broker_reject_threshold

        for _ in range(threshold):
            gate.record_reject()

        trips = _clean_bus.of_type(IntegrityGateTrip)
        assert len(trips) >= 1
        assert trips[-1].threshold == threshold


# ─────────────────────────────────────────────────────────────────────────────
# Tests — replay determinism
# ─────────────────────────────────────────────────────────────────────────────


class TestReplayDeterminism:
    def test_same_inputs_produce_same_events(self):
        """
        With identical inputs, evaluate_market_state + portfolio_controller
        must return identical results (pure function contracts).
        """
        from execution.supervisor import CycleMetrics, evaluate_market_state
        from risk.portfolio_controller import evaluate as pf_eval, OpenPosition, _sector

        metrics = CycleMetrics(
            bar_latency_seconds=10.0,
            missing_bar_pct=0.01,
            median_spread_pct=0.002,
            p90_spread_pct=0.004,
            rejection_rate_10c=0.05,
        )
        sv1 = evaluate_market_state(metrics)
        sv2 = evaluate_market_state(metrics)
        assert sv1 == sv2

        positions = [
            OpenPosition(
                ticker="AAPL", notional=5000, sector="bigtech", dollar_volume=1e9
            ),
        ]
        pf1 = pf_eval("NVDA", 100, 5000.0, positions)
        pf2 = pf_eval("NVDA", 100, 5000.0, positions)
        assert pf1 == pf2
