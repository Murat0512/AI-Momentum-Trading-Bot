from __future__ import annotations

from unittest.mock import MagicMock

from config.constants import (
    ORDER_FILLED,
    ORDER_PARTIAL,
    ORDER_REJECTED,
    LIFECYCLE_PARTIAL2,
)
from execution.engine import TradingEngine
from execution.lifecycle import LifecycleEvent
from execution.order_manager import ManagedOrder


class _Trade:
    def __init__(self) -> None:
        self.trade_id = "T-1"
        self.ticker = "AAPL"
        self.shares_remaining = 100
        self.lifecycle_state = "PARTIAL1"
        self.entry_price = 10.0


def _engine_stub() -> TradingEngine:
    engine = object.__new__(TradingEngine)
    engine._pending_exit_orders = {}
    engine._logger = MagicMock()
    return engine


def test_rejecting_exit_order_does_not_change_shares_remaining():
    engine = _engine_stub()
    trade = _Trade()
    evt = LifecycleEvent(
        trade_id=trade.trade_id,
        ticker=trade.ticker,
        event="PARTIAL_SELL",
        shares_to_sell=50,
        new_lifecycle_state=LIFECYCLE_PARTIAL2,
        reason="partial intent",
    )
    order = ManagedOrder(
        order_id="OM-1",
        ticker=trade.ticker,
        side="sell",
        qty=50,
        limit_price=10.0,
        status=ORDER_REJECTED,
        filled_qty=0,
    )

    engine._register_pending_exit_order(trade, evt, order, expected_price=10.0)
    engine._reconcile_pending_exit_orders([order])

    assert trade.shares_remaining == 100
    assert trade.lifecycle_state == "PARTIAL1"


def test_partial_fill_decrements_only_filled_qty():
    engine = _engine_stub()
    trade = _Trade()
    evt = LifecycleEvent(
        trade_id=trade.trade_id,
        ticker=trade.ticker,
        event="PARTIAL_SELL",
        shares_to_sell=50,
        new_lifecycle_state=LIFECYCLE_PARTIAL2,
        reason="partial intent",
    )
    submit_order = ManagedOrder(
        order_id="OM-2",
        ticker=trade.ticker,
        side="sell",
        qty=50,
        limit_price=10.0,
        status="SUBMITTED",
        filled_qty=0,
    )

    engine._register_pending_exit_order(trade, evt, submit_order, expected_price=10.0)

    partial = ManagedOrder(
        order_id="OM-2",
        ticker=trade.ticker,
        side="sell",
        qty=50,
        limit_price=10.0,
        status=ORDER_PARTIAL,
        filled_qty=20,
        filled_price=10.1,
    )
    engine._reconcile_pending_exit_orders([partial])

    assert trade.shares_remaining == 80
    assert trade.lifecycle_state == LIFECYCLE_PARTIAL2


def test_full_fill_decrements_correct_amount():
    engine = _engine_stub()
    trade = _Trade()
    evt = LifecycleEvent(
        trade_id=trade.trade_id,
        ticker=trade.ticker,
        event="PARTIAL_SELL",
        shares_to_sell=50,
        new_lifecycle_state=LIFECYCLE_PARTIAL2,
        reason="partial intent",
    )
    submit_order = ManagedOrder(
        order_id="OM-3",
        ticker=trade.ticker,
        side="sell",
        qty=50,
        limit_price=10.0,
        status="SUBMITTED",
        filled_qty=0,
    )

    engine._register_pending_exit_order(trade, evt, submit_order, expected_price=10.0)

    filled = ManagedOrder(
        order_id="OM-3",
        ticker=trade.ticker,
        side="sell",
        qty=50,
        limit_price=10.0,
        status=ORDER_FILLED,
        filled_qty=50,
        filled_price=10.2,
    )
    engine._reconcile_pending_exit_orders([filled])

    assert trade.shares_remaining == 50
    assert trade.lifecycle_state == LIFECYCLE_PARTIAL2
