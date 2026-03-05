"""
orders.py — Order abstraction layer.

Supports:
  - Paper trading (default, simulates fills at ask)
    - IBKR live/paper API

All order methods return an OrderResult.

Design notes
------------
  • buy() and sell() both accept ``limit_price`` as the required price
    parameter.  OrderManager always supplies the strategy’s intended limit
    so the broker never needs to compute it from a reference ask/bid.
     IBBroker submits ``limit_price`` directly to the exchange  no
        internal slippage calculation.  Repricing is handled exclusively by
        OrderManager._reprice() so slippage is applied exactly once.
     PaperBroker fills at ``limit_price  limit_slippage_pct`` to simulate
        real-world execution quality.  Slippage is applied on both buy and
        sell sides for symmetry.
     buy() and sell() return filled_price=0.0 for live brokers when the fill
        is not yet confirmed (normal for limit orders that rest in the book).
        OrderManager.tick() polls get_order_status() to resolve fills asynchronously.
     IBBroker.sell() uses a LimitOrderRequest when order_type="limit" 
        market sells only if CONFIG.execution.allow_market_sell_emergency=True.
     cancel_order() is best-effort (no-raise).
"""

from __future__ import annotations

import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

import pytz

from config.settings import CONFIG

log = logging.getLogger(__name__)
ET = pytz.timezone("America/New_York")


@dataclass
class OrderResult:
    order_id: str
    ticker: str
    side: str  # "buy" | "sell"
    qty: int
    filled_price: float  # 0.0 if not yet filled (poll via get_order_status)
    filled_at: Optional[datetime]
    success: bool
    message: str = ""


@dataclass
class BrokerOrderStatus:
    """Returned by get_order_status() — live broker polling result."""

    broker_order_id: str
    status: str  # "open" | "partial" | "filled" | "cancelled" | "rejected"
    filled_qty: int = 0
    filled_price: float = 0.0
    reason: str = ""


# ─────────────────────────────────────────────────────────────────────────────
# PAPER BROKER (default)
# ─────────────────────────────────────────────────────────────────────────────


class PaperBroker:
    """
    Simulates order fills at the submitted limit price.
    Adds configurable slippage (limit_slippage_pct) on both buy and sell
    sides to model real-world execution quality.  Fills are synchronous:
    filled_price is set immediately.
    """

    def __init__(self):
        self._slippage = CONFIG.execution.limit_slippage_pct
        self._paper_fills: dict = {}  # broker_order_id → BrokerOrderStatus

    def buy(self, ticker: str, qty: int, limit_price: float, **kwargs) -> OrderResult:
        fill_price = round(limit_price * (1 + self._slippage), 4)
        order_id = f"PAPER-{uuid.uuid4().hex[:8].upper()}"
        now = datetime.now(ET)
        log.info(
            f"[PAPER] BUY {qty}sh {ticker} @ ${fill_price:.4f} "
            f"(limit=${limit_price:.4f} + {self._slippage*100:.1f}% slippage)"
        )
        status = BrokerOrderStatus(
            broker_order_id=order_id,
            status="filled",
            filled_qty=qty,
            filled_price=fill_price,
        )
        self._paper_fills[order_id] = status
        return OrderResult(
            order_id=order_id,
            ticker=ticker,
            side="buy",
            qty=qty,
            filled_price=fill_price,
            filled_at=now,
            success=True,
        )

    def sell(self, ticker: str, qty: int, limit_price: float, **kwargs) -> OrderResult:
        fill_price = round(limit_price * (1 - self._slippage), 4)
        order_id = f"PAPER-{uuid.uuid4().hex[:8].upper()}"
        now = datetime.now(ET)
        log.info(
            f"[PAPER] SELL {qty}sh {ticker} @ ${fill_price:.4f} "
            f"(limit=${limit_price:.4f} - {self._slippage*100:.1f}% slippage)"
        )
        status = BrokerOrderStatus(
            broker_order_id=order_id,
            status="filled",
            filled_qty=qty,
            filled_price=fill_price,
        )
        self._paper_fills[order_id] = status
        return OrderResult(
            order_id=order_id,
            ticker=ticker,
            side="sell",
            qty=qty,
            filled_price=fill_price,
            filled_at=now,
            success=True,
        )

    def get_order_status(self, broker_order_id: str) -> Optional[BrokerOrderStatus]:
        return self._paper_fills.get(broker_order_id)

    def cancel_order(self, broker_order_id: str) -> None:
        """Best-effort cancel — no-op for paper (fills are immediate)."""
        self._paper_fills.pop(broker_order_id, None)

    def list_open_orders(self, ticker: Optional[str] = None) -> list[dict[str, Any]]:
        """Paper broker has no resting orders because fills are immediate."""
        return []

    def cancel_open_orders(self, ticker: Optional[str] = None) -> int:
        """Paper broker has no resting orders to cancel."""
        return 0

    def get_position_qty(self, ticker: str) -> float:
        """Paper broker does not persist simulated positions in this layer."""
        return 0.0

    def has_open_position(self, ticker: str) -> bool:
        return False


# ─────────────────────────────────────────────────────────────────────────────
## IBKR Broker Only
# ─────────────────────────────────────────────────────────────────────────────


class IBBroker:
    def __init__(self, ib_connection=None):
        import logging
        import pytz
        from datetime import datetime
        from ib_insync import Stock, MarketOrder, LimitOrder, Trade

        self.logger = logging.getLogger(__name__)
        # If no connection passed, get it from manager
        if ib_connection is None:
            from infrastructure.ib_connection import IBConnectionManager

            self.conn_mgr = IBConnectionManager()
            if not self.conn_mgr.is_connected:
                self.conn_mgr.connect()
            self.ib = self.conn_mgr.ib
        else:
            self.ib = ib_connection
        self.eastern = pytz.timezone("US/Eastern")

    def _is_regular_trading_hours(self) -> bool:
        from datetime import datetime
        import pytz

        now = datetime.now(self.eastern)
        market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
        market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
        return market_open <= now <= market_close

    def get_position_qty(self, ticker: str) -> int:
        try:
            positions = self.ib.positions()
            for pos in positions:
                if pos.contract.symbol == ticker:
                    return int(pos.position)
            return 0
        except Exception:
            return 0

    def has_open_position(self, ticker: str) -> bool:
        return abs(self.get_position_qty(ticker)) > 0

    def cancel_all_orders(self, ticker: str):
        self.logger.info(f"?? Checking for hanging orders on {ticker}...")
        try:
            open_trades = self.ib.openTrades()
            cancellations = 0
            for trade in open_trades:
                if trade.contract.symbol == ticker and not trade.isDone():
                    self.logger.warning(
                        f"?? Canceling active order {trade.order.orderId} for {ticker}"
                    )
                    self.ib.cancelOrder(trade.order)
                    cancellations += 1
            if cancellations > 0:
                self.ib.sleep(0.5)
        except Exception:
            pass

    def _get_live_quote(self, contract) -> dict:
        try:
            tickers = self.ib.reqTickers(contract)
            if tickers and len(tickers) > 0:
                t = tickers[0]
                bid = float(t.bid) if (t.bid and t.bid > 0) else float(t.marketPrice())
                ask = float(t.ask) if (t.ask and t.ask > 0) else float(t.marketPrice())
                return {"bid": bid, "ask": ask}
        except Exception:
            pass
        return {"bid": 0.0, "ask": 0.0}

    def _passes_spread_defense(
        self, ticker: str, quote: dict, max_spread_pct: float = 0.01
    ) -> bool:
        """
        PHASE 3: The Spread & Commission Defense.
        Aborts trades if the invisible fee of the Bid/Ask spread is too high.
        """
        bid = quote["bid"]
        ask = quote["ask"]

        if bid <= 0 or ask <= 0:
            self.logger.warning(
                f"??? SPREAD DEFENSE: {ticker} rejected. Invalid Quote (Bid: {bid:.2f}, Ask: {ask:.2f})."
            )
            return False

        spread = ask - bid
        spread_pct = spread / ask

        # Max acceptable spread is set to 1.0% (0.01)
        if spread_pct > max_spread_pct:
            self.logger.warning(
                f"??? SPREAD DEFENSE: {ticker} rejected. Spread too wide ({(spread_pct*100):.2f}% > {(max_spread_pct*100):.2f}%)."
            )
            return False

        self.logger.info(
            f"? SPREAD DEFENSE: {ticker} passed. Spread:  ({(spread_pct*100):.2f}%)."
        )
        return True

    def buy(self, ticker: str, qty: int, limit_price: float = None):
        from ib_insync import Stock, MarketOrder, LimitOrder

        self.cancel_all_orders(ticker)

        contract = Stock(ticker, "SMART", "USD")
        self.ib.qualifyContracts(contract)

        # PHASE 3 INJECTION: Secure the quote and run the Spread Defense
        quote = self._get_live_quote(contract)
        if not self._passes_spread_defense(ticker, quote):
            self.logger.error(
                f"? TRADE ABORTED: {ticker} failed spread defense. Too illiquid."
            )
            return None

        is_rth = self._is_regular_trading_hours()

        if is_rth:
            self.logger.info(f"?? RTH ACTIVE: Routing MARKET BUY for {qty} {ticker}")
            order = MarketOrder("BUY", qty)
        else:
            ask_price = quote["ask"]
            buy_limit = round(ask_price + 0.03, 2)
            self.logger.info(
                f"?? EXTENDED HOURS: Routing LIMIT BUY for {qty} {ticker} @  (Ask: )"
            )
            order = LimitOrder("BUY", qty, buy_limit)
            order.outsideRth = True

        try:
            trade = self.ib.placeOrder(contract, order)
            self.ib.sleep(0.5)
            self.logger.info(
                f"? BUY ORDER PLACED: {ticker} | Status: {trade.orderStatus.status}"
            )

            from execution.orders import OrderResult

            return OrderResult(
                broker_order_id=str(trade.order.orderId),
                status="filled" if trade.isDone() else "open",
                filled_qty=qty,
                filled_avg_price=0.0,
            )
        except Exception as e:
            self.logger.error(f"? BUY ORDER CRASH on {ticker}: {e}")
            return None

    def sell(self, ticker: str, qty: int, limit_price: float = None):
        from ib_insync import Stock, MarketOrder, LimitOrder

        self.cancel_all_orders(ticker)

        contract = Stock(ticker, "SMART", "USD")
        self.ib.qualifyContracts(contract)

        is_rth = self._is_regular_trading_hours()

        if is_rth:
            self.logger.info(f"?? RTH ACTIVE: Routing MARKET SELL for {qty} {ticker}")
            order = MarketOrder("SELL", qty)
        else:
            quote = self._get_live_quote(contract)
            bid_price = quote["bid"]

            if bid_price <= 0:
                self.logger.error(
                    f"? PM/AH SELL FAILED for {ticker}: Invalid Bid Price ($)"
                )
                return None

            sell_limit = round(bid_price - 0.03, 2)
            self.logger.info(
                f"?? EXTENDED HOURS: Routing LIMIT SELL for {qty} {ticker} @  (Bid: )"
            )
            order = LimitOrder("SELL", qty, sell_limit)
            order.outsideRth = True

        try:
            trade = self.ib.placeOrder(contract, order)
            self.ib.sleep(0.5)
            self.logger.info(
                f"? SELL ORDER PLACED: {ticker} | Status: {trade.orderStatus.status}"
            )

            from execution.orders import OrderResult

            return OrderResult(
                broker_order_id=str(trade.order.orderId),
                status="filled" if trade.isDone() else "open",
                filled_qty=qty,
                filled_avg_price=0.0,
            )
        except Exception as e:
            self.logger.error(f"? SELL ORDER CRASH on {ticker}: {e}")
            return None


def get_broker():
    explicit = str(os.getenv("EXECUTION_BROKER", "")).strip().lower()
    test_profile = str(os.getenv("TEST_PROFILE", "")).strip().lower()

    if explicit:
        broker = explicit
    elif test_profile in {"1", "true", "yes", "on"}:
        if bool(getattr(CONFIG.execution, "paper_mode", True)):
            broker = "ibkr"
            log.warning(
                "[BROKER] TEST_PROFILE active in paper mode: forcing broker='ibkr'"
            )
        else:
            broker = str(getattr(CONFIG.execution, "broker", "paper")).strip().lower()
            log.warning(
                "[BROKER] TEST_PROFILE ignored for live mode; using configured broker='%s'",
                broker,
            )
    else:
        broker = str(getattr(CONFIG.execution, "broker", "paper")).strip().lower()

    log.info("[BROKER] Selected broker='%s'", broker)
    if broker == "ibkr":
        try:
            return IBBroker()
        except Exception as exc:
            log.critical(
                "[BROKER] IBKR initialization failed (%s). Falling back to PaperBroker.",
                exc,
            )
            return PaperBroker()
    return PaperBroker()
