import re

with open('execution/orders.py', 'r', encoding='utf-8') as f:
    text = f.read()

new_class = '''class IBBroker:
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
        self.eastern = pytz.timezone('US/Eastern')

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
                    self.logger.warning(f"?? Canceling active order {trade.order.orderId} for {ticker}")
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

    def _passes_spread_defense(self, ticker: str, quote: dict, max_spread_pct: float = 0.01) -> bool:
        """
        PHASE 3: The Spread & Commission Defense.
        Aborts trades if the invisible fee of the Bid/Ask spread is too high.
        """
        bid = quote['bid']
        ask = quote['ask']

        if bid <= 0 or ask <= 0:
            self.logger.warning(f"??? SPREAD DEFENSE: {ticker} rejected. Invalid Quote (Bid: \, Ask: \).")
            return False

        spread = ask - bid
        spread_pct = spread / ask

        # Max acceptable spread is set to 1.0% (0.01)
        if spread_pct > max_spread_pct:
            self.logger.warning(f"??? SPREAD DEFENSE: {ticker} rejected. Spread too wide ({(spread_pct*100):.2f}% > {(max_spread_pct*100):.2f}%).")
            return False

        self.logger.info(f"? SPREAD DEFENSE: {ticker} passed. Spread: \ ({(spread_pct*100):.2f}%).")
        return True

    def buy(self, ticker: str, qty: int, limit_price: float = None):
        from ib_insync import Stock, MarketOrder, LimitOrder
        self.cancel_all_orders(ticker)
        
        contract = Stock(ticker, 'SMART', 'USD')
        self.ib.qualifyContracts(contract)

        # PHASE 3 INJECTION: Secure the quote and run the Spread Defense
        quote = self._get_live_quote(contract)
        if not self._passes_spread_defense(ticker, quote):
            self.logger.error(f"? TRADE ABORTED: {ticker} failed spread defense. Too illiquid.")
            return None

        is_rth = self._is_regular_trading_hours()

        if is_rth:
            self.logger.info(f"?? RTH ACTIVE: Routing MARKET BUY for {qty} {ticker}")
            order = MarketOrder('BUY', qty)
        else:
            ask_price = quote['ask']
            buy_limit = round(ask_price + 0.03, 2)
            self.logger.info(f"?? EXTENDED HOURS: Routing LIMIT BUY for {qty} {ticker} @ \ (Ask: \)")
            order = LimitOrder('BUY', qty, buy_limit)
            order.outsideRth = True

        try:
            trade = self.ib.placeOrder(contract, order)
            self.ib.sleep(0.5)
            self.logger.info(f"? BUY ORDER PLACED: {ticker} | Status: {trade.orderStatus.status}")
            
            from execution.orders import OrderResult
            return OrderResult(
                broker_order_id=str(trade.order.orderId),
                status="filled" if trade.isDone() else "open",
                filled_qty=qty,
                filled_avg_price=0.0
            )
        except Exception as e:
            self.logger.error(f"? BUY ORDER CRASH on {ticker}: {e}")
            return None

    def sell(self, ticker: str, qty: int, limit_price: float = None):
        from ib_insync import Stock, MarketOrder, LimitOrder
        self.cancel_all_orders(ticker)
        
        contract = Stock(ticker, 'SMART', 'USD')
        self.ib.qualifyContracts(contract)

        is_rth = self._is_regular_trading_hours()

        if is_rth:
            self.logger.info(f"?? RTH ACTIVE: Routing MARKET SELL for {qty} {ticker}")
            order = MarketOrder('SELL', qty)
        else:
            quote = self._get_live_quote(contract)
            bid_price = quote['bid']
            
            if bid_price <= 0:
                self.logger.error(f"? PM/AH SELL FAILED for {ticker}: Invalid Bid Price (\)")
                return None
            
            sell_limit = round(bid_price - 0.03, 2)
            self.logger.info(f"?? EXTENDED HOURS: Routing LIMIT SELL for {qty} {ticker} @ \ (Bid: \)")
            order = LimitOrder('SELL', qty, sell_limit)
            order.outsideRth = True

        try:
            trade = self.ib.placeOrder(contract, order)
            self.ib.sleep(0.5)
            self.logger.info(f"? SELL ORDER PLACED: {ticker} | Status: {trade.orderStatus.status}")
            
            from execution.orders import OrderResult
            return OrderResult(
                broker_order_id=str(trade.order.orderId),
                status="filled" if trade.isDone() else "open",
                filled_qty=qty,
                filled_avg_price=0.0
            )
        except Exception as e:
            self.logger.error(f"? SELL ORDER CRASH on {ticker}: {e}")
            return None
'''

# Standardize variables like before
new_class = new_class.replace('\\$', '$')

pattern = r"class IBBroker:.*?def get_broker\(\):"
text = re.sub(pattern, new_class + "\ndef get_broker():", text, flags=re.DOTALL)

with open('execution/orders.py', 'w', encoding='utf-8') as f:
    f.write(text)
