import re

with open("data/fetcher.py", "r", encoding="utf-8") as f:
    text = f.read()

# 1. Update assert_live_feed
text = re.sub(
    r'if adapter_name != "alpaca":', r'if adapter_name not in ["alpaca", "ibkr"]:', text
)

# 2. Add imports
ib_imports = """
from infrastructure.ib_connection import IBConnectionManager
from ib_insync import Stock, util
import asyncio
"""
text = text.replace("import pytz\nimport requests", "import pytz\n" + ib_imports)

ibkr_adapter_code = """
class IBAdapter(BaseDataAdapter):
    adapter_name = "ibkr"

    @property
    def feed_type(self) -> str:
        return "ibkr_paper"

    def _qualify_contract(self, ticker: str):
        # Local import to prevent circular issues
        from infrastructure.ib_connection import IBConnectionManager
        if not hasattr(self, '_conn_mgr') or self._conn_mgr is None:
            self._conn_mgr = IBConnectionManager()
            self._conn_mgr.connect()
        ib = self._conn_mgr.ib
        contract = Stock(ticker, 'SMART', 'USD')
        try:
            ib.qualifyContracts(contract)
        except Exception as e:
            log.warning(f"Could not qualify {ticker}: {e}")
        return contract

    def fetch_1m(
        self,
        ticker: str,
        days: int,
        include_extended_hours: bool = True,
    ) -> pd.DataFrame:
        contract = self._qualify_contract(ticker)
        ib = self._conn_mgr.ib
        
        calc_days = max(1, int(days))
        duration = f"{calc_days} D"

        try:
            bars = ib.reqHistoricalData(
                contract,
                endDateTime='',
                durationStr=duration,
                barSizeSetting='1 min',
                whatToShow='TRADES',
                useRTH=not include_extended_hours,
                formatDate=1,
                keepUpToDate=False
            )
        except Exception as e:
            log.warning(f"[{ticker}] IBKR historical data error: {e}")
            return pd.DataFrame()

        if not bars:
            return pd.DataFrame()

        import pandas as pd
        df = util.df(bars)
        if df is None or df.empty:
            return pd.DataFrame()

        # Output Normalization
        if 'date' in df.columns:
            df.rename(columns={'date': 'timestamp'}, inplace=True)
        if 'timestamp' in df.columns:
            df.set_index('timestamp', inplace=True)
            
        return self._normalize(df)

    def fetch_quote(self, ticker: str) -> dict:
        contract = self._qualify_contract(ticker)
        ib = self._conn_mgr.ib

        tickers_data = ib.reqTickers(contract)
        if not tickers_data:
            return _null_quote(ticker, self.feed_type, quality="fetch_error")

        t = tickers_data[0]
        # Handle nans properly
        bid = float(t.bid) if t.bid == t.bid else 0.0
        ask = float(t.ask) if t.ask == t.ask else 0.0
        mp = t.marketPrice()
        last = float(mp) if mp == mp else 0.0

        return {
            "ticker": ticker,
            "bid": bid,
            "ask": ask,
            "last": last,
            "timestamp": datetime.now(ET),
            "feed": self.feed_type,
            "_quote_quality": "ok" if bid > 0 and ask > 0 else "invalid_bid_ask"
        }

    def fetch_quotes(self, tickers: list) -> dict:
        result = {}
        contracts = []
        valid_tickers = []
        for ticker in tickers:
            try:
                c = self._qualify_contract(ticker)
                contracts.append(c)
                valid_tickers.append(ticker)
            except Exception:
                result[ticker] = _null_quote(ticker, self.feed_type, "fetch_error")
                
        if contracts:
            ib = self._conn_mgr.ib
            try:
                tickers_data = ib.reqTickers(*contracts)
                for i, t in enumerate(tickers_data):
                    ticker = valid_tickers[i]
                    bid = float(t.bid) if t.bid == t.bid else 0.0
                    ask = float(t.ask) if t.ask == t.ask else 0.0
                    mp = t.marketPrice()
                    last = float(mp) if mp == mp else 0.0
                    result[ticker] = {
                        "ticker": ticker,
                        "bid": bid,
                        "ask": ask,
                        "last": last,
                        "timestamp": datetime.now(ET),
                        "feed": self.feed_type,
                        "_quote_quality": "ok" if bid > 0 and ask > 0 else "invalid_bid_ask"
                    }
            except Exception as e:
                log.warning(f"Batch quote failed: {e}")

        for ticker in tickers:
            if ticker not in result:
                result[ticker] = _null_quote(ticker, self.feed_type, "missing_after_bulk")
                
        return result
"""

al_start = text.find("class AlpacaAdapter(BaseDataAdapter):")
al_end = text.find("def get_live_adapter() -> BaseDataAdapter:")

if al_start != -1 and al_end != -1:
    text = text[:al_start] + ibkr_adapter_code + "\n\n# " + text[al_end - 15 :]
else:
    print("Could not find bounds")

text = text.replace(
    'if src == "alpaca":\n        return AlpacaAdapter()',
    'if src in ("alpaca", "ibkr"):\n        return IBAdapter()',
)

pacing_code = """        for ticker in tickers:
            # PACING SAFEGUARD to prevent IBKR Pacing Violations (Error 162)
            if hasattr(self._adapter, "feed_type") and "ibkr" in self._adapter.feed_type:
                try:
                    util.sleep(1.0)
                except Exception:
                    time.sleep(1.0)
"""
text = text.replace("        for ticker in tickers:", pacing_code)

with open("data/fetcher.py", "w", encoding="utf-8") as f:
    f.write(text)

print("SUCCESS: fetcher.py rewritten.")
