"""
tests/test_position_sizing_caps.py — Prop-grade capital allocation cap tests.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from config.settings import CONFIG
from risk.portfolio_controller import ALLOW, ALLOW_WITH_MULTIPLIER, BLOCK, OpenPosition, evaluate, _sector
from risk.manager import RiskManager


@pytest.fixture(autouse=True)
def _restore_cfg():
    p = {k: getattr(CONFIG.portfolio, k) for k in CONFIG.portfolio.__dataclass_fields__}
    r = {k: getattr(CONFIG.risk, k) for k in CONFIG.risk.__dataclass_fields__}
    yield
    for k, v in p.items():
        setattr(CONFIG.portfolio, k, v)
    for k, v in r.items():
        setattr(CONFIG.risk, k, v)


def _open(ticker: str, notional: float, dvol: float = 50_000_000.0) -> OpenPosition:
    return OpenPosition(ticker=ticker, notional=notional, sector=_sector(ticker), dollar_volume=dvol)


def test_notional_cap_limits_qty():
    CONFIG.risk.account_size = 25_000.0
    CONFIG.portfolio.max_notional_per_trade_pct = 0.10

    # qty_base=1000 @ $10 => $10,000, but cap1 allows only $2,500 => 250 shares
    decision = evaluate(
        ticker="NVDA",
        planned_qty=1000,
        planned_notional=10_000.0,
        open_positions=[],
        entry_price=10.0,
        dollar_volume=100_000_000.0,
    )
    assert decision.action == ALLOW
    assert decision.qty_final == 250
    assert "max_notional_per_trade_pct" in decision.cap_reason


def test_liquidity_cap_limits_qty():
    CONFIG.risk.account_size = 25_000.0
    CONFIG.portfolio.max_notional_per_trade_pct = 1.0
    CONFIG.portfolio.max_trade_notional = 1_000_000.0
    CONFIG.portfolio.liquidity_notional_cap_pct_of_dvol = 0.01

    # dvol=100k, 1% => $1,000 notional cap => 100 shares at $10
    decision = evaluate(
        ticker="MULN",
        planned_qty=5_000,
        planned_notional=50_000.0,
        open_positions=[],
        entry_price=10.0,
        dollar_volume=100_000.0,
    )
    assert decision.qty_final == 100
    assert "liquidity_notional_cap_pct_of_dvol" in decision.cap_reason


def test_gross_exposure_blocks_or_reduces():
    CONFIG.risk.account_size = 25_000.0
    CONFIG.portfolio.max_gross_exposure_pct = 0.25  # $6,250
    CONFIG.portfolio.max_notional_per_trade_pct = 1.0
    CONFIG.portfolio.max_trade_notional = 100_000.0
    CONFIG.portfolio.min_trade_notional = 100.0

    # already open $6,000, only $250 room -> at $10 => fit 25 shares
    decision = evaluate(
        ticker="AMD",
        planned_qty=200,
        planned_notional=2_000.0,
        open_positions=[_open("AAPL", 6_000.0)],
        entry_price=10.0,
        dollar_volume=50_000_000.0,
    )
    assert decision.action == ALLOW
    assert decision.qty_final == 25
    assert "max_gross_exposure_pct" in decision.cap_reason

    # no room at all should block
    decision_block = evaluate(
        ticker="AMD",
        planned_qty=200,
        planned_notional=2_000.0,
        open_positions=[_open("AAPL", 6_300.0)],
        entry_price=10.0,
        dollar_volume=50_000_000.0,
    )
    assert decision_block.action == BLOCK


def test_deterministic_repeatability_same_inputs_same_qty():
    kwargs = dict(
        ticker="TSLA",
        planned_qty=777,
        planned_notional=31_080.0,
        open_positions=[_open("NVDA", 2_000.0)],
        entry_price=40.0,
        dollar_volume=3_000_000.0,
        recent_volume=50_000.0,
    )
    d1 = evaluate(**kwargs)
    d2 = evaluate(**kwargs)
    assert d1 == d2


def test_integration_order_submitted_qty_equals_capped_qty(monkeypatch):
    import execution.engine as eng_mod
    from events.bus import event_bus
    from events.sinks.test_sink import TestSink
    from events.types import OrderSubmitted

    # Tight cap so submitted qty should be strongly reduced.
    CONFIG.risk.account_size = 25_000.0
    CONFIG.portfolio.max_notional_per_trade_pct = 0.10
    CONFIG.portfolio.max_trade_notional = 100_000.0
    CONFIG.portfolio.max_gross_exposure_pct = 1.0

    sink = TestSink()
    event_bus.register(sink)

    try:
        engine = object.__new__(eng_mod.TradingEngine)
        engine._broker = MagicMock()
        engine._risk = MagicMock()
        engine._logger = MagicMock()

        broker_result = MagicMock()
        broker_result.success = True
        broker_result.order_id = "TEST-ORDER-1"
        broker_result.filled_price = 0.0
        broker_result.filled_at = None
        engine._broker.buy.return_value = broker_result

        eng_mod.order_manager.reset()

        engine._risk.size_position.return_value = 10_000  # large base qty
        engine._risk.open_trades.return_value = []
        engine._risk.open_trade.return_value = MagicMock(
            shares=250,
            ticker="TSLA",
            entry_price=10.0,
            stop_price=9.5,
            target_price=11.0,
            high_watermark=10.0,
        )

        monkeypatch.setattr(eng_mod.halt_machine, "is_blocked", lambda _t: False)
        monkeypatch.setattr(eng_mod.halt_machine, "resume_spread_multiplier", lambda _t: 1.0)
        monkeypatch.setattr(eng_mod.halt_machine, "resume_size_multiplier", lambda _t: 1.0)
        monkeypatch.setattr(eng_mod.halt_machine, "on_post_halt_entry", lambda _t: None)
        monkeypatch.setattr(eng_mod, "is_session_active", lambda _now: True)

        candidate = MagicMock()
        candidate.ticker = "TSLA"
        candidate.universe_rank = 1
        candidate.setup.entry_price = 10.0
        candidate.setup.stop_price = 9.5
        candidate.setup.setup_quality_score = 0.8
        candidate.setup.break_level_name = "PMH"
        candidate.metrics.demand_score = 0.9
        candidate.metrics.bid = 10.0
        candidate.metrics.ask = 10.01
        candidate.metrics.dollar_volume = 100_000_000.0
        candidate.metrics.volume_1m = 0.0
        candidate.metrics._dh_report = None
        candidate.metrics._feed_type = "alpaca_iex"

        sel = MagicMock()
        sel.chosen_over = []
        sel.selection_reason = "test"

        engine._execute_entry(candidate, "TREND", sel, now=eng_mod.now_et())

        submitted = sink.of_type(OrderSubmitted)
        assert len(submitted) >= 1
        expected = evaluate(
            ticker="TSLA",
            planned_qty=10_000,
            planned_notional=100_000.0,
            open_positions=[],
            entry_price=10.0,
            dollar_volume=100_000_000.0,
            recent_volume=0.0,
        )
        assert submitted[-1].qty == expected.qty_final
    finally:
        eng_mod.order_manager.reset()
        event_bus.unregister(sink)


def test_risk_manager_legacy_notional_cap_disabled_by_default():
    rm = RiskManager()

    CONFIG.risk.account_size = 25_000.0
    CONFIG.risk.risk_per_trade_pct = 1.0
    CONFIG.risk.legacy_notional_cap_enabled = False
    uncapped = rm.size_position(entry_price=100.0, stop_price=99.0, regime="TREND")

    CONFIG.risk.legacy_notional_cap_enabled = True
    capped = rm.size_position(entry_price=100.0, stop_price=99.0, regime="TREND")

    assert uncapped > capped
    assert capped == 50
