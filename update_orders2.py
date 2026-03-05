import re

with open("execution/orders.py", "r", encoding="utf-8") as f:
    text = f.read()

ibkr_broker_code = """class IBBroker:
    \"\"\"
    Live or paper IBKR broker via ib_insync.
    Connects using infrastructure.ib_connection.IBConnectionManager.
    \"\"\"

    def __init__(self):
        from infrastructure.ib_connection import IBConnectionManager
        self.conn_mgr = IBConnectionManager()
        if not self.conn_mgr.is_connected:
            self.conn_mgr.connect()
        self.ib = self.conn_mgr.ib
        self.adapter_name = "ibkr"
        log.info("[IBKR] Initializing IBBroker")

    def _qualify_contract(self, ticker: str):
        from ib_insync import Stock
        contract = Stock(ticker, 'SMART', 'USD')
        try:
            self.ib.qualifyContracts(contract)
        except Exception as e:
            log.warning(f"Could not qualify {ticker} in IBBroker: {e}")
        return contract

    def buy(self, ticker: str, qty: int, limit_price: float, **kwargs) -> OrderResult:
        from ib_insync import MarketOrder, LimitOrder
        try:
            contract = self._qualify_contract(ticker)
            from config.settings import CONFIG
            if CONFIG.execution.order_type == "limit":
                order = LimitOrder('BUY', qty, round(limit_price, 2))
                order.outsideRth = True
            else:
                order = MarketOrder('BUY', qty)
            
            trade = self.ib.placeOrder(contract, order)
            self.ib.sleep(0.5)

            fill = float(trade.orderStatus.avgFillPrice) if trade.orderStatus.avgFillPrice else 0.0

            return OrderResult(
                order_id=str(trade.order.orderId),
                ticker=ticker,
                side="buy",
                qty=qty,
                filled_price=fill,
                filled_at=datetime.now(ET) if fill > 0 else None,
                success=True,
            )
        except Exception as exc:
            log.error(f"[IBKR] BUY {ticker} failed: {exc}")
            return OrderResult("", ticker, "buy", qty, 0.0, None, False, str(exc))

    def sell(self, ticker: str, qty: int, limit_price: float, **kwargs) -> OrderResult:
        from ib_insync import MarketOrder, LimitOrder
        try:
            contract = self._qualify_contract(ticker)
            from config.settings import CONFIG
            if getattr(CONFIG.execution, "order_type", "limit") == "limit":
                order = LimitOrder('SELL', qty, round(limit_price, 2))
                order.outsideRth = True
            elif getattr(CONFIG.execution, "allow_market_sell_emergency", False):
                order = MarketOrder('SELL', qty)
            else:
                order = LimitOrder('SELL', qty, round(limit_price, 2))
                order.outsideRth = True

            trade = self.ib.placeOrder(contract, order)
            self.ib.sleep(0.5)

            fill = float(trade.orderStatus.avgFillPrice) if trade.orderStatus.avgFillPrice else 0.0

            return OrderResult(
                order_id=str(trade.order.orderId),
                ticker=ticker,
                side="sell",
                qty=qty,
                filled_price=fill,
                filled_at=datetime.now(ET) if fill > 0 else None,
                success=True,
            )
        except Exception as exc:
            log.error(f"[IBKR] SELL {ticker} failed: {exc}")
            return OrderResult("", ticker, "sell", qty, 0.0, None, False, str(exc))

    def cancel_order(self, order_id: str) -> bool:
        try:
            for trade in self.ib.openTrades():
                if str(trade.order.orderId) == str(order_id):
                    self.ib.cancelOrder(trade.order)
                    return True
        except Exception as exc:
            log.warning(f"[IBKR] Could not cancel {order_id}: {exc}")
        return False

    def cancel_open_orders(self, ticker: str) -> bool:
        try:
            canceled_any = False
            for trade in self.ib.openTrades():
                if hasattr(trade.contract, "symbol") and trade.contract.symbol == ticker:
                    self.ib.cancelOrder(trade.order)
                    canceled_any = True
            return True
        except Exception:
            return False

    def get_order_status(self, order_id: str) -> Optional[BrokerOrderStatus]:
        try:
            for trade in self.ib.trades():
                if str(trade.order.orderId) == str(order_id):
                    status_str = trade.orderStatus.status
                    filled = trade.orderStatus.filled
                    price = trade.orderStatus.avgFillPrice

                    mapped = "open"
                    if status_str == "Filled": mapped = "filled"
                    elif status_str in ("Cancelled", "ApiCancelled"): mapped = "canceled"
                    elif status_str in ("PendingCancel", "PendingSubmit", "PreSubmitted", "Submitted"): mapped = "open"

                    return BrokerOrderStatus(
                        status=mapped,
                        filled_qty=int(filled),
                        filled_price=float(price),
                    )
        except Exception:
            pass
        return None

    def list_open_orders(self, ticker: str = "") -> List[Dict]:
        out = []
        try:
            for trade in self.ib.openTrades():
                sym = trade.contract.symbol if hasattr(trade.contract, "symbol") else ""
                if ticker and sym != ticker:
                    continue
                out.append({
                    "id": str(trade.order.orderId),
                    "ticker": sym,
                    "side": trade.order.action.lower(),
                })
        except Exception:
            pass
        return out

    def get_position_qty(self, ticker: str) -> float:
        try:
            for pos in self.ib.positions():
                sym = pos.contract.symbol if hasattr(pos.contract, "symbol") else ""
                if sym == ticker:
                    return float(pos.position)
            return 0.0
        except Exception:
            return 0.0

    def has_open_position(self, ticker: str) -> bool:
        return abs(self.get_position_qty(ticker)) > 0

def get_broker"""

text = re.sub(
    r"class AlpacaBroker:.*?def get_broker", ibkr_broker_code, text, flags=re.DOTALL
)

text = text.replace('broker = "alpaca"', 'broker = "ibkr"')
text = text.replace("broker='alpaca'", "broker='ibkr'")
text = text.replace('if broker == "alpaca":', 'if broker in ("ibkr", "alpaca"):')
text = text.replace("return AlpacaBroker()", "return IBBroker()")
text = text.replace("[BROKER] Alpaca init", "[BROKER] IBKR init")

with open("execution/orders.py", "w", encoding="utf-8") as f:
    f.write(text)

print("Orders.py updated!")
