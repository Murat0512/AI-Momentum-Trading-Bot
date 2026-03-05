from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytz

from config.settings import CONFIG
from scanner.demand import DemandMetrics
from scanner.universe_manager import CandidateEntry
from selection.selector import SelectionResult, TradeCandidate


ET = pytz.timezone("America/New_York")


class _CfgRestore:
    def __init__(self):
        self._runtime = {
            k: getattr(CONFIG.runtime, k) for k in CONFIG.runtime.__dataclass_fields__
        }

    def restore(self):
        for k, v in self._runtime.items():
            setattr(CONFIG.runtime, k, v)


def test_tick_uses_decision_stack_when_enabled(monkeypatch):
    saved = _CfgRestore()
    try:
        CONFIG.runtime.use_decision_stack = True

        import execution.engine as eng_mod

        engine = object.__new__(eng_mod.TradingEngine)
        engine._cycle_count = 0
        engine._session_date = "2026-03-04"
        engine._risk = MagicMock()
        engine._logger = MagicMock()
        engine._broker = MagicMock()
        engine._fetcher = MagicMock()
        engine._pipeline = MagicMock()
        engine._selector = MagicMock()
        engine._setup_det = MagicMock()
        engine._lifecycle = MagicMock()
        engine._universe_mgr = MagicMock()
        engine._decision_engine = MagicMock()
        engine._pending_exit_orders = {}
        engine._execute_entry = MagicMock()

        now = datetime(2026, 3, 4, 10, 0, tzinfo=ET)

        dm = DemandMetrics(
            ticker="AAPL",
            last_price=10.0,
            dollar_volume=100_000_000.0,
            rvol=3.0,
            gap_pct=0.05,
            intraday_range_pct=0.03,
            volume_spike_z=2.0,
            demand_score=4.2,
            rank=1,
            bid=10.0,
            ask=10.01,
        )
        monkeypatch.setattr(eng_mod.UniverseScanner, "scan", lambda _self: [dm])

        entry = CandidateEntry(ticker="AAPL")
        entry.demand_score = 4.2
        entry.composite_score = 4.2
        entry.rank = 1
        entry.last_price = 10.0
        entry.dollar_volume = 100_000_000.0
        entry.rvol = 3.0
        entry.gap_pct = 0.05
        entry.feed_type = "alpaca_iex"
        engine._universe_mgr.top_n.return_value = [entry]

        candidate = TradeCandidate(
            metrics=dm,
            setup=SimpleNamespace(
                valid=True,
                setup_quality_score=0.8,
                spread_pct=0.001,
                entry_price=10.0,
                stop_price=9.8,
                break_level_name="PMH",
            ),
        )
        sel = SelectionResult(selected=candidate, selection_reason="decision-stack")
        engine._decision_engine.run.return_value = SimpleNamespace(
            selection=sel,
            reason="ok",
        )

        engine._fetcher.fetch_quotes.return_value = {
            "AAPL": {"bid": 10.0, "ask": 10.01, "timestamp": now}
        }

        monkeypatch.setattr(eng_mod, "now_et", lambda: now)
        monkeypatch.setattr(eng_mod, "is_session_active", lambda _now: True)
        monkeypatch.setattr(
            eng_mod.TradingEngine, "_update_regime", lambda _self: "TREND"
        )
        monkeypatch.setattr(eng_mod.TradingEngine, "_process_exits", lambda _self: None)
        monkeypatch.setattr(eng_mod.order_manager, "tick", lambda _broker, _now: [])

        engine._tick()

        assert engine._universe_mgr.update_from_metrics.called
        assert engine._decision_engine.run.called
        assert engine._execute_entry.called
    finally:
        saved.restore()


def test_halt_entries_still_processes_exits(monkeypatch):
    saved = _CfgRestore()
    try:
        import execution.engine as eng_mod
        import execution.supervisor as sup_mod

        engine = object.__new__(eng_mod.TradingEngine)
        engine._cycle_count = 0
        engine._session_date = "2026-03-04"
        engine._risk = MagicMock()
        engine._logger = MagicMock()
        engine._broker = MagicMock()
        engine._fetcher = MagicMock()
        engine._pipeline = MagicMock()
        engine._selector = MagicMock()
        engine._setup_det = MagicMock()
        engine._lifecycle = MagicMock()
        engine._universe_mgr = MagicMock()
        engine._decision_engine = MagicMock()
        engine._pending_exit_orders = {}

        process_exits_called = {"v": False}

        def _mark_exits(_self):
            process_exits_called["v"] = True

        now = datetime(2026, 3, 4, 10, 0, tzinfo=ET)
        monkeypatch.setattr(eng_mod, "now_et", lambda: now)
        monkeypatch.setattr(eng_mod, "is_session_active", lambda _now: True)
        monkeypatch.setattr(
            eng_mod.TradingEngine, "_update_regime", lambda _self: "TREND"
        )
        monkeypatch.setattr(eng_mod.TradingEngine, "_process_exits", _mark_exits)
        monkeypatch.setattr(eng_mod.order_manager, "tick", lambda _broker, _now: [])

        def _halt_eval(_metrics):
            return sup_mod.SupervisorOutput(
                state=sup_mod.MarketState.HALT_ENTRIES,
                size_mult=0.0,
                spread_mult=0.0,
                min_sqs=1.0,
                trigger="test-halt",
            )

        monkeypatch.setattr(sup_mod, "evaluate_market_state", _halt_eval)

        scan_called = {"v": False}

        def _scan(_self):
            scan_called["v"] = True
            return []

        monkeypatch.setattr(eng_mod.UniverseScanner, "scan", _scan)

        engine._tick()

        assert process_exits_called["v"] is True
        assert scan_called["v"] is False
    finally:
        saved.restore()
