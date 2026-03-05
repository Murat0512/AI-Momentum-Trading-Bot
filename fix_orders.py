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
        """
        Determines if the current time is within standard US equity hours.
        """
        from datetime import datetime
        import pytz
        now = datetime.now(self.eastern)
        market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
        market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
        
        return market_open <= now <= market_close

    def get_position_qty(self, ticker: str) -> int:
        """
        Scans the current IBKR portfolio and returns the held quantity for a ticker.
        """
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
        """
        Wash Trade Protection: Clears any hanging orders before executing a new one.
        """
        self.logger.info(f"?? Checking for hanging orders on {ticker}...")
        open_trades = self.ib.openTrades()
        cancellations = 0
        
        for trade in open_trades:
            if trade.contract.symbol == ticker and not trade.isDone():
                self.logger.warning(f"?? Canceling active order {trade.order.orderId} for {ticker}")
                self.ib.cancelOrder(trade.order)
                cancellations += 1
                
        if cancellations > 0:
            self.ib.sleep(0.5)

    def _get_live_quote(self, contract) -> dict:
        """
        Fetches an instantaneous snapshot quote to calculate Limit Order pricing
        during extended hours where Market orders are rejected.
        """
        tickers = self.ib.reqTickers(contract)
        if tickers and len(tickers) > 0:
            t = tickers[0]
            bid = t.bid if (t.bid and t.bid > 0) else t.marketPrice()
            ask = t.ask if (t.ask and t.ask > 0) else t.marketPrice()
            return {"bid": bid, "ask": ask}
        return {"bid": 0.0, "ask": 0.0}

    def buy(self, ticker: str, qty: int, limit_price: float = None):
        """
        Intelligent Buy Router: Switches order types based on the exact time of day.
        """
        from ib_insync import Stock, MarketOrder, LimitOrder
        self.cancel_all_orders(ticker)
        
        contract = Stock(ticker, 'SMART', 'USD')
        self.ib.qualifyContracts(contract)

        is_rth = self._is_regular_trading_hours()

        if is_rth:
            self.logger.info(f"?? RTH ACTIVE: Routing MARKET BUY for {qty} {ticker}")
            order = MarketOrder('BUY', qty)
        else:
            # Extended Hours Logic
            quote = self._get_live_quote(contract)
            ask_price = quote['ask']
            
            if ask_price <= 0:
                self.logger.error(f"? PM/AH BUY FAILED for {ticker}: Invalid Ask Price ({ask_price})")
                return None
            
            # Add \.03 of allowable slippage to aggressively fill the limit order
            limit_price = round(ask_price + 0.03, 2)
            self.logger.info(f"?? EXTENDED HOURS: Routing LIMIT BUY for {qty} {ticker} @ \ (Ask: \)")
            
            order = LimitOrder('BUY', qty, limit_price)
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
        """
        Intelligent Sell Router: Secures exits based on time of day.
        """
        from ib_insync import Stock, MarketOrder, LimitOrder
        self.cancel_all_orders(ticker)
        
        contract = Stock(ticker, 'SMART', 'USD')
        self.ib.qualifyContracts(contract)

        is_rth = self._is_regular_trading_hours()

        if is_rth:
            self.logger.info(f"?? RTH ACTIVE: Routing MARKET SELL for {qty} {ticker}")
            order = MarketOrder('SELL', qty)
        else:
            # Extended Hours Logic
            quote = self._get_live_quote(contract)
            bid_price = quote['bid']
            
            if bid_price <= 0:
                self.logger.error(f"? PM/AH SELL FAILED for {ticker}: Invalid Bid Price ({bid_price})")
                return None
            
            # Subtract \.03 of allowable slippage to aggressively dump the shares
            limit_price = round(bid_price - 0.03, 2)
            self.logger.info(f"?? EXTENDED HOURS: Routing LIMIT SELL for {qty} {ticker} @ \ (Bid: \)")
            
            order = LimitOrder('SELL', qty, limit_price)
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

new_class = new_class.replace('\\$', '$')

pattern = r"class IBBroker:.*?def get_broker\(\):"
text = re.sub(pattern, new_class + "\ndef get_broker():", text, flags=re.DOTALL)

with open('execution/orders.py', 'w', encoding='utf-8') as f:
    f.write(text)
