"""
tests/test_event_replay.py — Tests for the complete event taxonomy and replay.

Covers:
  1. All new EVT_* constants exist in config.constants
  2. Every new wrapper logs the correct event_type
  3. Replay reconstructs events in order with correct fields
  4. Decision evaluation records accepted/rejected states with full metadata
  5. Session start/end events carry correct metadata
  6. Restart recovery event carries position + order data
  7. RTH block event carries ticker and reason
  8. Promotion/demotion wrappers carry source, reason, score
"""

from __future__ import annotations

import json
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from trade_log.event_log import EventLog
from config.constants import (
    EVT_DECISION,
    EVT_SESSION_START,
    EVT_SESSION_END,
    EVT_RESTART,
    EVT_RTH_BLOCK,
    EVT_PROMOTION,
    EVT_DEMOTION,
    EVT_GOVERNOR_EVENT,
    EVT_ORDER_CANCELLED,
    EVT_ORDER_STUCK,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def elog(tmp_path):
    """Fresh EventLog writing to a temp directory."""
    el = EventLog(log_dir=str(tmp_path))
    el.new_day(date_str="2099-06-10", run_id="replay-test")
    yield el, tmp_path
    el.close()


def _read_events(tmp_path) -> list:
    path = tmp_path / "events_2099-06-10.jsonl"
    return [
        json.loads(line)
        for line in path.read_text().splitlines()
        if line.strip()
    ]


# ─────────────────────────────────────────────────────────────────────────────
# 1. EVT_* constants exist
# ─────────────────────────────────────────────────────────────────────────────

class TestNewConstants:
    def test_evt_decision_defined(self):
        assert EVT_DECISION == "DECISION_EVALUATED"

    def test_evt_session_start_defined(self):
        assert EVT_SESSION_START == "SESSION_START"

    def test_evt_session_end_defined(self):
        assert EVT_SESSION_END == "SESSION_END"

    def test_evt_restart_defined(self):
        assert EVT_RESTART == "RESTART_RECOVERY"

    def test_evt_rth_block_defined(self):
        assert EVT_RTH_BLOCK == "RTH_BLOCK"


# ─────────────────────────────────────────────────────────────────────────────
# 2. New wrapper functions emit correct event_type
# ─────────────────────────────────────────────────────────────────────────────

class TestWrapperEmissions:
    def test_log_decision_evaluation_accepted(self, elog):
        el, tmp = elog
        el.log_decision_evaluation(
            ticker="TSLA", accepted=True, reason="all_gates_passed",
            sqs=0.72, demand_score=0.85, regime="TREND",
            gate_results={"spread": True, "rvol": True},
        )
        el.close()
        events = _read_events(tmp)
        assert len(events) == 1
        e = events[0]
        assert e["event_type"] == EVT_DECISION
        assert e["ticker"] == "TSLA"
        assert e["payload"]["accepted"] is True
        assert e["payload"]["sqs"] == pytest.approx(0.72)
        assert e["payload"]["demand_score"] == pytest.approx(0.85)
        assert e["payload"]["regime"] == "TREND"
        assert e["payload"]["gate_results"]["spread"] is True

    def test_log_decision_evaluation_rejected(self, elog):
        el, tmp = elog
        el.log_decision_evaluation(
            ticker="AMC", accepted=False,
            reason="spread_too_wide",
            sqs=0.55, demand_score=0.60,
        )
        el.close()
        events = _read_events(tmp)
        assert events[0]["payload"]["accepted"] is False
        assert events[0]["payload"]["reason"] == "spread_too_wide"

    def test_log_promotion(self, elog):
        el, tmp = elog
        el.log_promotion("GME", source="news", reason="earnings_catalyst", score=0.30)
        el.close()
        events = _read_events(tmp)
        e = events[0]
        assert e["event_type"] == EVT_PROMOTION
        assert e["ticker"] == "GME"
        assert e["payload"]["source"] == "news"
        assert e["payload"]["score"] == pytest.approx(0.30)

    def test_log_demotion(self, elog):
        el, tmp = elog
        el.log_demotion("PLTR", reason="ttl_expired", last_score=0.22)
        el.close()
        events = _read_events(tmp)
        e = events[0]
        assert e["event_type"] == EVT_DEMOTION
        assert e["ticker"] == "PLTR"
        assert e["payload"]["reason"] == "ttl_expired"

    def test_log_governor_event_blocked(self, elog):
        el, tmp = elog
        el.log_governor_event(
            blocked=True, reason="daily_loss_cap_reached",
            ticker="AAPL", governor_name="daily_loss_cap",
        )
        el.close()
        events = _read_events(tmp)
        e = events[0]
        assert e["event_type"] == EVT_GOVERNOR_EVENT
        assert e["payload"]["blocked"] is True
        assert e["payload"]["governor_name"] == "daily_loss_cap"

    def test_log_order_cancelled_wrapper(self, elog):
        el, tmp = elog
        el.log_order_cancelled("NVDA", order_id="ORD-001", reason="ttl_expired")
        el.close()
        events = _read_events(tmp)
        e = events[0]
        assert e["event_type"] == EVT_ORDER_CANCELLED
        assert e["payload"]["order_id"] == "ORD-001"
        assert e["payload"]["reason"] == "ttl_expired"

    def test_log_order_stuck_wrapper(self, elog):
        el, tmp = elog
        el.log_order_stuck("AMD", order_id="ORD-002", age_seconds=95.5)
        el.close()
        events = _read_events(tmp)
        e = events[0]
        assert e["event_type"] == EVT_ORDER_STUCK
        assert e["payload"]["age_seconds"] == pytest.approx(95.5)

    def test_log_session_start(self, elog):
        el, tmp = elog
        el.log_session_start("2099-06-10", run_id="abc123", mode="paper")
        el.close()
        events = _read_events(tmp)
        e = events[0]
        assert e["event_type"] == EVT_SESSION_START
        assert e["payload"]["date"] == "2099-06-10"
        assert e["payload"]["mode"] == "paper"
        assert e["payload"]["run_id"] == "abc123"

    def test_log_session_end(self, elog):
        el, tmp = elog
        el.log_session_end(
            "2099-06-10", total_trades=4, daily_pnl=312.50, daily_pnl_r=3.2
        )
        el.close()
        events = _read_events(tmp)
        e = events[0]
        assert e["event_type"] == EVT_SESSION_END
        assert e["payload"]["total_trades"] == 4
        assert e["payload"]["daily_pnl"] == pytest.approx(312.50)

    def test_log_restart(self, elog):
        el, tmp = elog
        el.log_restart(
            reconstructed_positions={"TSLA": 100},
            open_orders=[{"order_id": "ORD-001", "ticker": "TSLA", "side": "sell"}],
            drift_detected=False,
            halt_triggered=False,
            note="warm_restart",
        )
        el.close()
        events = _read_events(tmp)
        e = events[0]
        assert e["event_type"] == EVT_RESTART
        assert e["payload"]["reconstructed_positions"]["TSLA"] == 100
        assert e["payload"]["drift_detected"] is False
        assert e["payload"]["halt_triggered"] is False

    def test_log_rth_block(self, elog):
        el, tmp = elog
        el.log_rth_block("MARA", reason="outside RTH (06:00 ET)", now_str="2099-06-10T06:00:00")
        el.close()
        events = _read_events(tmp)
        e = events[0]
        assert e["event_type"] == EVT_RTH_BLOCK
        assert e["ticker"] == "MARA"
        assert "outside RTH" in e["payload"]["reason"]


# ─────────────────────────────────────────────────────────────────────────────
# 3. Replay — full day stream reconstructable in order
# ─────────────────────────────────────────────────────────────────────────────

class TestFullDayReplay:
    def test_replay_order_matches_write_order(self, elog):
        el, tmp = elog
        # Simulate a mini trading day
        el.log_session_start("2099-06-10", run_id="replay-test", mode="paper")
        el.log_promotion("TSLA", source="scanner", reason="rvol_5x", score=0.88)
        el.log_decision_evaluation(
            "TSLA", accepted=True, reason="all_gates_passed",
            sqs=0.71, demand_score=0.88,
        )
        el.log_order_submitted("TSLA", "buy", 50, limit_price=251.50, order_id="ORD-A")
        el.log_order_filled("TSLA", "buy", 50, fill_price=251.55, order_id="ORD-A")
        el.log_position_opened(_FakeTrade())
        el.log_stop_adjusted("TSLA", "TRD-01", old_stop=248.0, new_stop=251.55,
                              reason="breakeven", lifecycle_state="PARTIAL1")
        el.log_position_closed(_FakeTrade(exit_price=265.0, pnl=675.0, pnl_r=2.0))
        el.log_session_end("2099-06-10", total_trades=1, daily_pnl=675.0, daily_pnl_r=2.0)
        el.close()

        events = list(EventLog.replay("2099-06-10", log_dir=str(tmp)))
        event_types = [e["event_type"] for e in events]

        assert event_types[0] == "SESSION_START"
        assert event_types[1] == "PROMOTION"
        assert event_types[2] == "DECISION_EVALUATED"
        assert event_types[3] == "ORDER_SUBMITTED"
        assert event_types[4] == "ORDER_FILLED"
        assert event_types[5] == "POSITION_OPENED"
        assert event_types[6] == "STOP_ADJUSTED"
        assert event_types[7] == "POSITION_CLOSED"
        assert event_types[8] == "SESSION_END"
        assert len(events) == 9

    def test_replay_ticker_field_preserved(self, elog):
        el, tmp = elog
        el.log_decision_evaluation("NVDA", accepted=False, reason="rvol_below_min",
                                   demand_score=0.42)
        el.close()
        events = list(EventLog.replay("2099-06-10", log_dir=str(tmp)))
        assert events[0]["ticker"] == "NVDA"

    def test_replay_run_id_on_all_events(self, elog):
        el, tmp = elog
        el.log_session_start("2099-06-10", run_id="replay-test")
        el.log_demotion("PLTR", reason="ttl_expired")
        el.close()
        events = list(EventLog.replay("2099-06-10", log_dir=str(tmp)))
        for e in events:
            assert e["run_id"] == "replay-test"


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

class _FakeTrade:
    def __init__(self, exit_price=0.0, pnl=0.0, pnl_r=0.0):
        self.trade_id           = "TRD-01"
        self.ticker             = "TSLA"
        self.entry_price        = 251.55
        self.stop_price         = 248.00
        self.shares             = 50
        self.regime             = "TREND"
        self.setup_name         = "MomentumV1"
        self.exit_price         = exit_price
        self.pnl                = pnl
        self.pnl_r              = pnl_r
        self.exit_reason        = "target_hit"
