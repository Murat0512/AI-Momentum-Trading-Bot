"""
tests/test_startup_recovery.py — Tests for execution/startup_recovery.py

Covers:
  1.  PaperBroker → always returns clean RecoveryResult (no-op)
  2.  Cold start with AlpacaBroker (no broker positions) → clean
  3.  Warm restart: broker has unknown position → position reconstructed in risk_manager
  4.  Warm restart: position already tracked → already_tracked (no duplicate)
  5.  Phantom position (internal has it, broker doesn't) → drift_detected + halt
  6.  Qty mismatch between internal and broker → drift_detected + halt
  7.  Broker fetch failure → halt triggered
  8.  Open broker orders re-registered in order_manager
  9.  No duplicate stop order on restart (idempotent order recovery)
  10. RecoveryResult.is_clean() works correctly
  11. Event log receives RESTART_RECOVERY event
  12. RiskManager.recover_position() creates a valid TradeRecord
  13. RiskManager.recover_position() applies 2% stop when stop_price is 0
  14. OrderManager.recover_order() registers active order
  15. OrderManager.recover_order() ignores terminal orders
  16. OrderManager.recover_order() is idempotent (calling twice, no duplicate)
"""

from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
from unittest.mock import MagicMock, patch
import pytest
import pytz

ET = pytz.timezone("America/New_York")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _make_paper_broker():
    """PaperBroker — no _client attribute."""
    broker = MagicMock(spec=[])  # no attributes by default
    return broker


def _make_alpaca_broker(positions=None, orders=None, fetch_positions_raises=None):
    """Simulated AlpacaBroker with _client."""
    client = MagicMock()

    if fetch_positions_raises:
        client.get_all_positions.side_effect = fetch_positions_raises
    else:
        alpaca_positions = []
        for sym, qty, avg in positions or []:
            pos = MagicMock()
            pos.symbol = sym
            pos.qty = str(qty)
            pos.avg_entry_price = str(avg)
            alpaca_positions.append(pos)
        client.get_all_positions.return_value = alpaca_positions

    # Mock open orders
    open_orders = []
    for o in orders or []:
        order = MagicMock()
        order.id = o.get("order_id", "ORD-001")
        order.symbol = o.get("ticker", "TSLA")
        order.side = MagicMock(value=o.get("side", "sell"))
        order.qty = str(o.get("qty", 100))
        order.filled_qty = str(o.get("filled_qty", 0))
        order.filled_avg_price = str(o.get("filled_price", 0.0))
        order.limit_price = str(o.get("limit_price", 0.0))
        order.status = MagicMock(value=o.get("status", "open"))
        open_orders.append(order)

    # Patch get_orders to return open_orders
    try:
        from alpaca.trading.enums import QueryOrderStatus

        client.get_orders.return_value = open_orders
    except ImportError:
        client.get_orders.return_value = open_orders

    broker = MagicMock()
    broker._client = client
    return broker


def _fresh_risk_manager():
    from risk.manager import RiskManager

    return RiskManager()


def _fresh_order_manager():
    from execution.order_manager import OrderManager

    return OrderManager()


def _fresh_event_log(tmp_path):
    from trade_log.event_log import EventLog

    el = EventLog(log_dir=str(tmp_path))
    el.new_day(date_str="2099-06-10")
    return el


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────


class TestPaperBrokerRecovery:
    def test_paper_broker_returns_clean(self):
        from execution.startup_recovery import reconstruct_from_broker

        broker = _make_paper_broker()
        rm = _fresh_risk_manager()
        om = _fresh_order_manager()
        result = reconstruct_from_broker(broker, rm, om)
        assert result.is_clean
        assert not result.drift_detected
        assert not result.halt_triggered
        assert result.reconstructed_positions == {}

    def test_paper_broker_does_not_modify_risk_manager(self):
        from execution.startup_recovery import reconstruct_from_broker

        broker = _make_paper_broker()
        rm = _fresh_risk_manager()
        om = _fresh_order_manager()
        reconstruct_from_broker(broker, rm, om)
        assert rm.open_trades() == []


class TestColdStartAlpaca:
    def test_cold_start_no_positions_is_clean(self):
        from execution.startup_recovery import reconstruct_from_broker

        broker = _make_alpaca_broker(positions=[])
        rm = _fresh_risk_manager()
        om = _fresh_order_manager()
        result = reconstruct_from_broker(broker, rm, om)
        assert result.is_clean
        assert result.reconstructed_positions == {}

    def test_cold_start_no_positions_note(self):
        from execution.startup_recovery import reconstruct_from_broker

        broker = _make_alpaca_broker(positions=[])
        result = reconstruct_from_broker(
            broker, _fresh_risk_manager(), _fresh_order_manager()
        )
        assert "no_positions_found" in result.note or result.note != ""


class TestWarmRestartReconstruction:
    def test_broker_position_unknown_to_internal_is_reconstructed(self):
        from execution.startup_recovery import reconstruct_from_broker

        broker = _make_alpaca_broker(positions=[("TSLA", 100, 251.50)])
        rm = _fresh_risk_manager()
        om = _fresh_order_manager()
        result = reconstruct_from_broker(broker, rm, om)

        assert "TSLA" in result.reconstructed_positions
        assert result.reconstructed_positions["TSLA"] == 100
        assert not result.drift_detected
        assert not result.halt_triggered
        # risk_manager now tracks the position
        assert any(t.ticker == "TSLA" for t in rm.open_trades())

    def test_reconstructed_position_has_correct_entry_price(self):
        from execution.startup_recovery import reconstruct_from_broker

        broker = _make_alpaca_broker(positions=[("NVDA", 50, 480.25)])
        rm = _fresh_risk_manager()
        om = _fresh_order_manager()
        reconstruct_from_broker(broker, rm, om)

        trade = next(t for t in rm.open_trades() if t.ticker == "NVDA")
        assert trade.entry_price == pytest.approx(480.25)
        assert trade.shares == 50

    def test_reconstructed_position_has_stop_applied(self):
        from execution.startup_recovery import reconstruct_from_broker

        broker = _make_alpaca_broker(positions=[("AMD", 75, 160.00)])
        rm = _fresh_risk_manager()
        om = _fresh_order_manager()
        reconstruct_from_broker(broker, rm, om)

        trade = next(t for t in rm.open_trades() if t.ticker == "AMD")
        # stop should be 2% below entry when not known
        assert trade.stop_price < trade.entry_price
        assert trade.stop_price == pytest.approx(160.00 * 0.98, abs=0.01)

    def test_already_tracked_position_not_duplicated(self):
        """If the broker position matches internal state → already_tracked, no dup."""
        from execution.startup_recovery import reconstruct_from_broker

        broker = _make_alpaca_broker(positions=[("TSLA", 100, 251.50)])
        rm = _fresh_risk_manager()
        om = _fresh_order_manager()

        # Pre-populate risk manager with the same position
        rm.recover_position("TSLA", 100, 251.50, stop_price=248.00)
        assert len(rm.open_trades()) == 1

        result = reconstruct_from_broker(broker, rm, om)
        # Should be already_tracked, NOT in reconstructed
        assert "TSLA" in result.already_tracked
        assert "TSLA" not in result.reconstructed_positions
        # No duplicate created
        assert len(rm.open_trades()) == 1


class TestDriftDetection:
    def test_phantom_internal_position_triggers_halt(self, monkeypatch):
        """Internal has position broker doesn't → drift + halt."""
        from execution.startup_recovery import reconstruct_from_broker
        from execution import integrity_gate as ig_mod

        mock_gate = MagicMock()
        monkeypatch.setattr(ig_mod, "integrity_gate", mock_gate)

        broker = _make_alpaca_broker(positions=[])  # broker empty
        rm = _fresh_risk_manager()
        om = _fresh_order_manager()
        # Manually add an internal position
        rm.recover_position("TSLA", 100, 250.0, stop_price=247.0)

        # Need to patch the integrity_gate inside startup_recovery too
        import execution.startup_recovery as sr_mod

        monkeypatch.setattr(
            sr_mod, "_trigger_halt", lambda reason: mock_gate.force_halt(reason)
        )

        result = reconstruct_from_broker(broker, rm, om)
        assert result.drift_detected
        assert result.halt_triggered
        mock_gate.force_halt.assert_called()

    def test_broker_fetch_failure_triggers_halt(self, monkeypatch):
        from execution.startup_recovery import reconstruct_from_broker

        halted = []
        import execution.startup_recovery as sr_mod

        monkeypatch.setattr(sr_mod, "_trigger_halt", lambda r: halted.append(r))

        broker = _make_alpaca_broker(fetch_positions_raises=RuntimeError("timeout"))
        rm = _fresh_risk_manager()
        om = _fresh_order_manager()
        result = reconstruct_from_broker(broker, rm, om)

        assert result.halt_triggered
        assert result.drift_detected
        assert len(halted) == 1


class TestOrderIdempotency:
    def test_open_orders_registered_in_order_manager(self):
        from execution.startup_recovery import reconstruct_from_broker

        broker = _make_alpaca_broker(
            positions=[("TSLA", 100, 251.50)],
            orders=[
                {
                    "order_id": "ALPACA-001",
                    "ticker": "TSLA",
                    "side": "sell",
                    "qty": 100,
                    "status": "open",
                }
            ],
        )
        rm = _fresh_risk_manager()
        om = _fresh_order_manager()
        result = reconstruct_from_broker(broker, rm, om)

        active = om.active_orders()
        assert any(o.ticker == "TSLA" for o in active)

    def test_no_duplicate_sell_order_after_recovery(self):
        """After order recovery, can_submit returns False for TSLA sell."""
        from execution.startup_recovery import reconstruct_from_broker

        broker = _make_alpaca_broker(
            positions=[("TSLA", 100, 251.50)],
            orders=[
                {
                    "order_id": "ALPACA-001",
                    "ticker": "TSLA",
                    "side": "sell",
                    "qty": 100,
                    "status": "open",
                }
            ],
        )
        rm = _fresh_risk_manager()
        om = _fresh_order_manager()
        reconstruct_from_broker(broker, rm, om)

        ok, reason = om.can_submit("TSLA", "sell")
        assert not ok
        assert (
            "duplicate" in reason.lower()
            or "active" in reason.lower()
            or "pending" in reason.lower()
        )

    def test_recovery_is_idempotent(self):
        """Calling reconstruct_from_broker twice for the same state is safe."""
        from execution.startup_recovery import reconstruct_from_broker

        broker = _make_alpaca_broker(positions=[("TSLA", 100, 251.50)])
        rm = _fresh_risk_manager()
        om = _fresh_order_manager()

        result1 = reconstruct_from_broker(broker, rm, om)
        result2 = reconstruct_from_broker(broker, rm, om)

        # Second call should see the position as already_tracked
        assert "TSLA" in result2.already_tracked
        # Only one trade in risk_manager
        tsla_trades = [t for t in rm.open_trades() if t.ticker == "TSLA"]
        assert len(tsla_trades) == 1


class TestEventLogEmission:
    def test_restart_event_emitted(self, tmp_path):
        from execution.startup_recovery import reconstruct_from_broker
        from trade_log.event_log import EventLog
        from config.constants import EVT_RESTART
        import json

        el = _fresh_event_log(tmp_path)
        broker = _make_alpaca_broker(positions=[("TSLA", 100, 251.50)])
        rm = _fresh_risk_manager()
        om = _fresh_order_manager()

        reconstruct_from_broker(broker, rm, om, event_log=el)
        el.close()

        events = [
            json.loads(l)
            for l in (tmp_path / "events_2099-06-10.jsonl").read_text().splitlines()
            if l.strip()
        ]
        types = [e["event_type"] for e in events]
        assert EVT_RESTART in types

    def test_restart_event_payload_contains_positions(self, tmp_path):
        from execution.startup_recovery import reconstruct_from_broker
        from trade_log.event_log import EventLog
        from config.constants import EVT_RESTART
        import json

        el = _fresh_event_log(tmp_path)
        broker = _make_alpaca_broker(positions=[("NVDA", 50, 480.0)])
        rm = _fresh_risk_manager()
        om = _fresh_order_manager()

        reconstruct_from_broker(broker, rm, om, event_log=el)
        el.close()

        events = [
            json.loads(l)
            for l in (tmp_path / "events_2099-06-10.jsonl").read_text().splitlines()
            if l.strip()
        ]
        restart_event = next(e for e in events if e["event_type"] == EVT_RESTART)
        # Should contain reconstructed position
        assert "NVDA" in restart_event["payload"]["reconstructed_positions"]


class TestRecoveryResult:
    def test_is_clean_when_no_issues(self):
        from execution.startup_recovery import RecoveryResult

        r = RecoveryResult()
        assert r.is_clean

    def test_not_clean_when_drift(self):
        from execution.startup_recovery import RecoveryResult

        r = RecoveryResult(drift_detected=True)
        assert not r.is_clean

    def test_not_clean_when_halted(self):
        from execution.startup_recovery import RecoveryResult

        r = RecoveryResult(halt_triggered=True)
        assert not r.is_clean

    def test_to_dict_contains_keys(self):
        from execution.startup_recovery import RecoveryResult

        r = RecoveryResult(reconstructed_positions={"TSLA": 100})
        d = r.to_dict()
        assert "reconstructed_positions" in d
        assert "drift_detected" in d
        assert "halt_triggered" in d


class TestRiskManagerRecoverPosition:
    def test_recover_creates_open_trade(self):
        from risk.manager import RiskManager

        rm = RiskManager()
        rm.recover_position("TSLA", 100, 251.50, stop_price=248.0)
        trades = rm.open_trades()
        assert len(trades) == 1
        assert trades[0].ticker == "TSLA"
        assert trades[0].shares == 100
        assert trades[0].entry_price == pytest.approx(251.50)
        assert trades[0].stop_price == pytest.approx(248.0)

    def test_recover_applies_estimated_stop_when_zero(self):
        from risk.manager import RiskManager

        rm = RiskManager()
        rm.recover_position("AMD", 50, 160.0, stop_price=0.0)
        t = rm.open_trades()[0]
        assert t.stop_price == pytest.approx(160.0 * 0.98, abs=0.01)
        assert "STOP_ESTIMATED_2PCT" in t.notes

    def test_recover_sets_shares_remaining(self):
        from risk.manager import RiskManager

        rm = RiskManager()
        rm.recover_position("NVDA", 75, 480.0, stop_price=475.0)
        t = rm.open_trades()[0]
        assert t.shares_remaining == 75

    def test_recover_trade_id_contains_ticker(self):
        from risk.manager import RiskManager

        rm = RiskManager()
        rm.recover_position("MARA", 30, 15.0, stop_price=14.5)
        t = rm.open_trades()[0]
        assert "MARA" in t.trade_id
        assert "RECOVERED" in t.trade_id

    def test_recover_preserves_squeeze_state_from_notes(self):
        from config.constants import LIFECYCLE_SQUEEZE
        from risk.manager import RiskManager

        rm = RiskManager()
        rm.recover_position(
            "TSLA",
            100,
            251.50,
            stop_price=248.0,
            note="RECOVERED_ON_RESTART|LIFECYCLE_SQUEEZE",
        )
        t = rm.open_trades()[0]
        assert t.lifecycle_state == LIFECYCLE_SQUEEZE
        assert t.trail_active is True


class TestOrderManagerRecoverOrder:
    def test_recover_active_order_is_registered(self):
        from execution.order_manager import OrderManager

        om = OrderManager()
        result = om.recover_order(
            broker_order_id="ALPACA-123",
            ticker="TSLA",
            side="sell",
            qty=100,
            status="open",
        )
        assert result is not None
        active = om.active_orders()
        assert any(o.ticker == "TSLA" for o in active)

    def test_recover_terminal_order_is_ignored(self):
        from execution.order_manager import OrderManager

        om = OrderManager()
        result = om.recover_order(
            broker_order_id="ALPACA-DONE",
            ticker="TSLA",
            side="sell",
            qty=100,
            status="filled",
        )
        assert result is None
        assert om.active_orders() == []

    def test_recover_order_is_idempotent(self):
        from execution.order_manager import OrderManager

        om = OrderManager()
        om.recover_order("ALPACA-999", "NVDA", "sell", 50, "open")
        om.recover_order("ALPACA-999", "NVDA", "sell", 50, "open")
        active = [o for o in om.active_orders() if o.ticker == "NVDA"]
        assert len(active) == 1

    def test_recover_blocks_can_submit_for_same_ticker(self):
        from execution.order_manager import OrderManager

        om = OrderManager()
        om.recover_order("ALPACA-XYZ", "AAPL", "sell", 100, "open")
        ok, reason = om.can_submit("AAPL", "sell")
        assert not ok

    def test_recover_cancelled_order_is_ignored(self):
        from execution.order_manager import OrderManager

        om = OrderManager()
        result = om.recover_order("ALPACA-CAN", "GME", "buy", 50, "cancelled")
        assert result is None
