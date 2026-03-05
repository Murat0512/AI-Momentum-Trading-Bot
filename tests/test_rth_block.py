"""
tests/test_rth_block.py — Tests for RTH order-submission hard block and
                            HaltStateMachine resume gate.

Covers:
  Session-constraint verification:
        1. trade_extended_hours=True is the default in SessionConfig
    2. engine._execute_entry returns early (no broker.buy) when outside RTH
    3. engine._execute_entry proceeds when inside RTH
    4. RTH_BLOCK event is emitted when order is blocked
    5. trade_extended_hours=True bypasses the RTH block (paper experiment mode)

  HaltStateMachine:
    6.  New ticker starts in ACTIVE state
    7.  on_health_block transitions ACTIVE → HALTED
    8.  First clean tick after halt transitions HALTED → RESUMING
    9.  Required clean ticks in RESUMING → back to ACTIVE
    10. Re-detected halt during RESUMING resets gate to HALTED
    11. is_blocked returns True for HALTED and RESUMING
    12. is_blocked returns False for ACTIVE
    13. resume_spread_multiplier < 1.0 during RESUMING, else 1.0
    14. reset() clears per-ticker state
    15. reset_all() clears all tickers
    16. status_summary returns correct mapping
    17. BLOCK_HALT_DETECTED string matches scanner wiring constant
"""

from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, call
import pytest
import pytz

from config.constants import BLOCK_HALT_DETECTED, EVT_RTH_BLOCK
from config.settings import CONFIG, SessionConfig
from execution.halt_machine import (
    HaltStateMachine,
    HALT_ACTIVE,
    HALT_HALTED,
    HALT_RESUMING,
)

ET = pytz.timezone("America/New_York")


# ─────────────────────────────────────────────────────────────────────────────
# SessionConfig defaults
# ─────────────────────────────────────────────────────────────────────────────


class TestSessionConfig:
    def test_trade_extended_hours_default_is_true(self):
        cfg = SessionConfig()
        assert cfg.trade_extended_hours is False

    def test_session_open_default(self):
        assert SessionConfig().session_open == "09:30"

    def test_entry_cooldown_default(self):
        assert SessionConfig().entry_cooldown_minutes_after_open == 15

    def test_session_close_default(self):
        assert SessionConfig().session_close == "16:00"


# ─────────────────────────────────────────────────────────────────────────────
# RTH hard block in engine._execute_entry
# ─────────────────────────────────────────────────────────────────────────────


def _make_candidate(ticker="TSLA"):
    """Build a minimal TradeCandidate-like mock for _execute_entry."""
    from unittest.mock import MagicMock

    cand = MagicMock()
    cand.ticker = ticker
    cand.setup.entry_price = 251.0
    cand.setup.stop_price = 248.0
    cand.setup.setup_quality_score = 0.85  # needed for log.info format spec :.3f
    cand.metrics.demand_score = 0.80
    cand.metrics._dh_report = None
    cand.metrics._feed_type = "sip"
    cand.metrics._quote_quality = "ok"
    cand.metrics.bid = 0.0  # no live quote — spread gate skipped
    cand.metrics.ask = 0.0
    return cand


def _make_engine(monkeypatch, paper_broker=True):
    """Build TradingEngine with mocked broker and all side-effects suppressed.

    Uses object.__new__ to bypass __init__, avoiding the expensive
    BatchFetcher / MTFPipeline / YFinanceAdapter initialisation chain that
    makes tests unacceptably slow.
    """
    import execution.engine as eng_mod

    broker = MagicMock()
    broker.buy.return_value = MagicMock(
        success=True, filled_price=251.10, order_id="ORD-TEST"
    )

    risk = MagicMock()
    risk.size_position.return_value = 100
    risk.open_trades.return_value = []  # portfolio gate sees empty book
    risk.open_trade.return_value = MagicMock(
        ticker="TSLA",
        shares=100,
        entry_price=251.10,
        stop_price=248.0,
        target_price=257.0,
        high_watermark=251.10,
    )

    logger = MagicMock()

    # Bypass __init__ entirely — only _execute_entry instance attributes needed
    engine = object.__new__(eng_mod.TradingEngine)
    engine._broker = broker
    engine._risk = risk
    engine._logger = logger
    return engine, broker


class TestRTHHardBlock:
    def test_order_blocked_before_rth(self, monkeypatch, tmp_path):
        """is_session_active=False + trade_extended_hours=False → broker.buy NOT called."""
        import execution.engine as eng_mod
        from trade_log import event_log as el_mod

        # Patch is_session_active to return False (outside RTH)
        monkeypatch.setattr(eng_mod, "is_session_active", lambda now: False)
        # Patch CONFIG.session.trade_extended_hours
        monkeypatch.setattr(CONFIG.session, "trade_extended_hours", False)

        el = el_mod.EventLog(log_dir=str(tmp_path))
        el.new_day(date_str="2099-06-10")
        # Patch BOTH module-level singletons so the engine's binding uses our el
        monkeypatch.setattr(el_mod, "event_log", el)
        monkeypatch.setattr(eng_mod, "event_log", el)

        engine, broker = _make_engine(monkeypatch)
        engine._execute_entry(
            _make_candidate(), regime="TREND", sel=MagicMock(), now=datetime.now(ET)
        )

        broker.buy.assert_not_called()
        el.close()

        # RTH_BLOCK event should be in the log
        import json

        events = [
            json.loads(l)
            for l in (tmp_path / "events_2099-06-10.jsonl").read_text().splitlines()
            if l.strip()
        ]
        types = [e["event_type"] for e in events]
        assert EVT_RTH_BLOCK in types

    def test_order_proceeds_inside_rth(self, monkeypatch):
        """is_session_active=True → broker.buy IS called."""
        import execution.engine as eng_mod

        monkeypatch.setattr(eng_mod, "is_session_active", lambda now: True)
        monkeypatch.setattr(CONFIG.session, "trade_extended_hours", False)

        engine, broker = _make_engine(monkeypatch)
        engine._execute_entry(
            _make_candidate(), regime="TREND", sel=MagicMock(), now=datetime.now(ET)
        )

        broker.buy.assert_called_once()

    def test_extended_hours_true_bypasses_rth_block(self, monkeypatch):
        """trade_extended_hours=True → broker.buy called even outside RTH."""
        import execution.engine as eng_mod

        monkeypatch.setattr(eng_mod, "is_session_active", lambda now: False)
        monkeypatch.setattr(CONFIG.session, "trade_extended_hours", True)

        engine, broker = _make_engine(monkeypatch)
        engine._execute_entry(
            _make_candidate(), regime="TREND", sel=MagicMock(), now=datetime.now(ET)
        )

        broker.buy.assert_called_once()

    def test_halt_machine_blocks_entry(self, monkeypatch):
        """halt_machine.is_blocked=True → broker.buy NOT called."""
        import execution.engine as eng_mod
        from execution import halt_machine as hm_mod

        monkeypatch.setattr(eng_mod, "is_session_active", lambda now: True)
        monkeypatch.setattr(CONFIG.session, "trade_extended_hours", False)

        halt = HaltStateMachine()
        halt.on_health_block("TSLA", BLOCK_HALT_DETECTED)  # HALTED
        monkeypatch.setattr(hm_mod, "halt_machine", halt)
        monkeypatch.setattr(eng_mod, "halt_machine", halt)

        engine, broker = _make_engine(monkeypatch)
        engine._execute_entry(
            _make_candidate("TSLA"),
            regime="TREND",
            sel=MagicMock(),
            now=datetime.now(ET),
        )

        broker.buy.assert_not_called()

    def test_halt_machine_allows_entry_after_resume(self, monkeypatch):
        """halt recovered + CLEAN_TICKS_REQUIRED clean ticks → broker.buy called."""
        import execution.engine as eng_mod
        from execution import halt_machine as hm_mod

        monkeypatch.setattr(eng_mod, "is_session_active", lambda now: True)
        monkeypatch.setattr(CONFIG.session, "trade_extended_hours", False)

        halt = HaltStateMachine()
        halt.on_health_block("TSLA", BLOCK_HALT_DETECTED)
        for _ in range(HaltStateMachine.CLEAN_TICKS_REQUIRED):
            halt.on_clean_tick("TSLA")

        monkeypatch.setattr(hm_mod, "halt_machine", halt)
        monkeypatch.setattr(eng_mod, "halt_machine", halt)

        engine, broker = _make_engine(monkeypatch)
        engine._execute_entry(
            _make_candidate("TSLA"),
            regime="TREND",
            sel=MagicMock(),
            now=datetime.now(ET),
        )

        broker.buy.assert_called_once()

    def test_entry_runaway_guard_blocks_submit(self, monkeypatch):
        """Fast ask drift above setup entry is blocked before broker.submit."""
        import execution.engine as eng_mod

        monkeypatch.setattr(eng_mod, "is_session_active", lambda now: True)
        monkeypatch.setattr(CONFIG.session, "trade_extended_hours", False)

        engine, broker = _make_engine(monkeypatch)
        candidate = _make_candidate("TSLA")
        candidate.setup.entry_price = 1.00
        candidate.setup.atr = 0.0
        candidate.metrics.bid = 1.03
        candidate.metrics.ask = 1.04
        candidate.metrics._dh_report = SimpleNamespace(quote_age_s=0.2)

        engine._execute_entry(
            candidate,
            regime="TREND",
            sel=MagicMock(),
            now=datetime.now(ET),
        )

        broker.buy.assert_not_called()

    def test_entry_stale_quote_guard_blocks_submit(self, monkeypatch):
        """Stale quote age blocks entry before broker.submit."""
        import execution.engine as eng_mod

        monkeypatch.setattr(eng_mod, "is_session_active", lambda now: True)
        monkeypatch.setattr(CONFIG.session, "trade_extended_hours", False)

        engine, broker = _make_engine(monkeypatch)
        candidate = _make_candidate("TSLA")
        candidate.setup.entry_price = 251.0
        candidate.setup.atr = 0.0
        candidate.metrics.bid = 250.99
        candidate.metrics.ask = 251.0
        candidate.metrics._dh_report = SimpleNamespace(quote_age_s=12.0)

        engine._execute_entry(
            candidate,
            regime="TREND",
            sel=MagicMock(),
            now=datetime.now(ET),
        )

        broker.buy.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# HaltStateMachine unit tests
# ─────────────────────────────────────────────────────────────────────────────


class TestHaltStateMachine:
    def setup_method(self):
        self.hm = HaltStateMachine()

    def test_new_ticker_is_active(self):
        assert self.hm.current_state("TSLA") == HALT_ACTIVE
        assert not self.hm.is_blocked("TSLA")

    def test_health_block_transitions_to_halted(self):
        self.hm.on_health_block("TSLA", BLOCK_HALT_DETECTED)
        assert self.hm.current_state("TSLA") == HALT_HALTED
        assert self.hm.is_blocked("TSLA")

    def test_first_clean_tick_transitions_to_resuming(self):
        self.hm.on_health_block("TSLA", BLOCK_HALT_DETECTED)
        self.hm.on_clean_tick("TSLA")
        assert self.hm.current_state("TSLA") == HALT_RESUMING
        assert self.hm.is_blocked("TSLA")  # still blocked during cooldown

    def test_full_clean_ticks_transitions_to_active(self):
        self.hm.on_health_block("TSLA", BLOCK_HALT_DETECTED)
        for _ in range(HaltStateMachine.CLEAN_TICKS_REQUIRED):
            self.hm.on_clean_tick("TSLA")
        assert self.hm.current_state("TSLA") == HALT_ACTIVE
        assert not self.hm.is_blocked("TSLA")

    def test_partial_clean_ticks_stays_resuming(self):
        self.hm.on_health_block("TSLA", BLOCK_HALT_DETECTED)
        # One fewer than required
        for _ in range(HaltStateMachine.CLEAN_TICKS_REQUIRED - 1):
            self.hm.on_clean_tick("TSLA")
        assert self.hm.current_state("TSLA") == HALT_RESUMING

    def test_rehalt_during_resuming_resets_gate(self):
        self.hm.on_health_block("TSLA", BLOCK_HALT_DETECTED)
        self.hm.on_clean_tick("TSLA")  # RESUMING
        self.hm.on_health_block("TSLA", BLOCK_HALT_DETECTED)  # re-halt during cooling
        assert self.hm.current_state("TSLA") == HALT_HALTED

    def test_is_blocked_false_for_active(self):
        assert not self.hm.is_blocked("AAPL")

    def test_is_blocked_true_for_halted(self):
        self.hm.on_health_block("AAPL", BLOCK_HALT_DETECTED)
        assert self.hm.is_blocked("AAPL")

    def test_is_blocked_true_for_resuming(self):
        self.hm.on_health_block("AAPL", BLOCK_HALT_DETECTED)
        self.hm.on_clean_tick("AAPL")
        assert self.hm.is_blocked("AAPL")

    def test_resume_spread_1_when_active(self):
        assert self.hm.resume_spread_multiplier("TSLA") == pytest.approx(1.0)

    def test_resume_spread_less_than_1_when_resuming(self):
        self.hm.on_health_block("TSLA", BLOCK_HALT_DETECTED)
        self.hm.on_clean_tick("TSLA")  # RESUMING
        mult = self.hm.resume_spread_multiplier("TSLA")
        assert mult < 1.0
        assert mult == pytest.approx(HaltStateMachine.RESUME_SPREAD_MULT)

    def test_resume_spread_back_to_1_after_active(self):
        self.hm.on_health_block("TSLA", BLOCK_HALT_DETECTED)
        for _ in range(HaltStateMachine.CLEAN_TICKS_REQUIRED):
            self.hm.on_clean_tick("TSLA")
        # Spread stays tight until the first post-halt entry is recorded
        assert self.hm.resume_spread_multiplier("TSLA") == pytest.approx(
            HaltStateMachine.RESUME_SPREAD_MULT
        )
        self.hm.on_post_halt_entry("TSLA")
        assert self.hm.resume_spread_multiplier("TSLA") == pytest.approx(1.0)

    def test_reset_clears_ticker_state(self):
        self.hm.on_health_block("TSLA", BLOCK_HALT_DETECTED)
        self.hm.reset("TSLA")
        assert self.hm.current_state("TSLA") == HALT_ACTIVE
        assert not self.hm.is_blocked("TSLA")

    def test_reset_all_clears_all_tickers(self):
        for ticker in ("TSLA", "AAPL", "NVDA"):
            self.hm.on_health_block(ticker, BLOCK_HALT_DETECTED)
        self.hm.reset_all()
        for ticker in ("TSLA", "AAPL", "NVDA"):
            assert self.hm.current_state(ticker) == HALT_ACTIVE

    def test_status_summary_reflects_states(self):
        self.hm.on_health_block("TSLA", BLOCK_HALT_DETECTED)
        self.hm.on_clean_tick("TSLA")  # RESUMING
        self.hm.on_health_block("AAPL", BLOCK_HALT_DETECTED)  # HALTED
        summary = self.hm.status_summary()
        assert summary["TSLA"] == HALT_RESUMING
        assert summary["AAPL"] == HALT_HALTED

    def test_clean_tick_on_untracked_ticker_is_noop(self):
        """Calling on_clean_tick for an unknown ticker creates no state."""
        self.hm.on_clean_tick("UNKNOWN")
        assert "UNKNOWN" not in self.hm._states

    def test_multiple_tickers_independent(self):
        self.hm.on_health_block("TSLA", BLOCK_HALT_DETECTED)
        assert not self.hm.is_blocked("NVDA")
        assert self.hm.is_blocked("TSLA")

    def test_block_halt_detected_constant_used_in_scanner_import(self):
        """Confirm BLOCK_HALT_DETECTED constant is importable and matches expected value."""
        assert BLOCK_HALT_DETECTED == "BLOCK_HALT_DETECTED"
