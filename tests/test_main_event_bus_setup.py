from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import MagicMock

from events.bus import event_bus
from events.types import LegacyAuditEvent, OrderFilled, OrderSubmitted
from main import configure_event_bus
from config.settings import CONFIG


class _CfgRestore:
    def __init__(self):
        self._events = {
            k: getattr(CONFIG.events, k) for k in CONFIG.events.__dataclass_fields__
        }

    def restore(self):
        for k, v in self._events.items():
            setattr(CONFIG.events, k, v)


def test_configure_event_bus_registers_jsonl_sink_and_writes_event(
    tmp_path, monkeypatch
):
    saved = _CfgRestore()
    try:
        monkeypatch.chdir(tmp_path)
        CONFIG.events.enabled = True
        CONFIG.events.jsonl_enabled = True
        CONFIG.events.console_enabled = False

        configure_event_bus(debug=False)
        event_bus.publish(
            OrderFilled(
                cycle_id=1,
                order_id="OM-1",
                ticker="AAPL",
                filled_qty=10,
                filled_price=10.0,
            )
        )

        files = sorted((tmp_path / "logs").glob("events_*.jsonl"))
        assert files
        line = files[-1].read_text(encoding="utf-8").strip().splitlines()[-1]
        payload = json.loads(line)
        assert payload.get("_type") == "OrderFilled"
    finally:
        event_bus.clear_sinks()
        saved.restore()


def test_engine_cycle_writes_jsonl_when_events_enabled(tmp_path, monkeypatch):
    saved = _CfgRestore()
    try:
        monkeypatch.chdir(tmp_path)
        CONFIG.events.enabled = True
        CONFIG.events.jsonl_enabled = True
        CONFIG.events.console_enabled = False
        configure_event_bus(debug=False)

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

        now = datetime(2026, 3, 4, 10, 0, 0)
        monkeypatch.setattr(eng_mod, "now_et", lambda: now)
        monkeypatch.setattr(eng_mod, "is_session_active", lambda _now: True)
        monkeypatch.setattr(
            eng_mod.TradingEngine, "_update_regime", lambda _self: "TREND"
        )
        monkeypatch.setattr(eng_mod.TradingEngine, "_process_exits", lambda _self: None)
        monkeypatch.setattr(eng_mod.order_manager, "tick", lambda _broker, _now: [])
        monkeypatch.setattr(eng_mod.UniverseScanner, "scan", lambda _self: [])

        def _fake_eval(_metrics):
            return sup_mod.SupervisorOutput(
                state=sup_mod.MarketState.CAUTION,
                size_mult=1.0,
                spread_mult=1.0,
                min_sqs=0.0,
                trigger="test",
            )

        monkeypatch.setattr(sup_mod, "evaluate_market_state", _fake_eval)

        engine._tick()

        files = sorted((tmp_path / "logs").glob("events_*.jsonl"))
        assert files
        lines = files[-1].read_text(encoding="utf-8").strip().splitlines()
        assert lines
        rec = json.loads(lines[-1])
        assert rec.get("_type") == "SupervisorStateChange"
    finally:
        event_bus.clear_sinks()
        saved.restore()


def test_configure_event_bus_registers_csv_sinks_and_writes(tmp_path, monkeypatch):
    saved = _CfgRestore()
    try:
        monkeypatch.chdir(tmp_path)
        CONFIG.events.enabled = True
        CONFIG.events.jsonl_enabled = False
        CONFIG.events.console_enabled = False
        CONFIG.events.csv_enabled = True
        CONFIG.events.csv_orders_enabled = True

        configure_event_bus(debug=False)

        event_bus.publish(OrderSubmitted(cycle_id=3, order_id="OM-2", ticker="NVDA"))
        event_bus.publish(
            LegacyAuditEvent(
                cycle_id=3,
                event_type="POSITION_CLOSED",
                ticker="NVDA",
                payload={"trade_id": "T-1", "pnl_r": 1.2},
            )
        )

        order_files = sorted((tmp_path / "logs").glob("orders_*.csv"))
        trade_files = sorted((tmp_path / "logs").glob("trades_*.csv"))
        assert order_files
        assert trade_files

        order_lines = order_files[-1].read_text(encoding="utf-8").strip().splitlines()
        trade_lines = trade_files[-1].read_text(encoding="utf-8").strip().splitlines()
        assert len(order_lines) >= 2
        assert len(trade_lines) >= 2
    finally:
        event_bus.clear_sinks()
        saved.restore()
