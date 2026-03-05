import pandas as pd
from datetime import datetime, timedelta
import pytz
import logging
from abc import ABC, abstractmethod

log = logging.getLogger(__name__)
ET = pytz.timezone("US/Eastern")


def _null_quote(ticker: str, feed: str, quality: str) -> dict:
    return {
        "ticker": ticker,
        "bid": 0.0,
        "ask": 0.0,
        "last": 0.0,
        "timestamp": datetime.now(ET),
        "feed": feed,
        "_quote_quality": quality,
    }


class BaseDataAdapter(ABC):
    @property
    @abstractmethod
    def feed_type(self) -> str:
        """Returns string like 'yfinance_dev', 'ibkr_paper'."""
        pass

    def _normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        # Essential formatting logic
        if "open" not in df.columns:
            df.columns = [c.lower() for c in df.columns]
        if "volume" not in df.columns:
            return pd.DataFrame()  # critical fail

        # Timezone check
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC").tz_convert(ET)
        else:
            df.index = df.index.tz_convert(ET)

        df = df[~df.index.duplicated(keep="last")].sort_index()
        df["feed"] = self.feed_type
        return df

    @abstractmethod
    def fetch_1m(
        self, ticker: str, days: int, include_extended_hours: bool = True
    ) -> pd.DataFrame:
        pass

    @abstractmethod
    def fetch_quote(self, ticker: str) -> dict:
        pass

    def fetch_quotes(self, tickers: list) -> dict:
        result = {}
        for t in tickers:
            result[t] = self.fetch_quote(t)
        return result


class YFinanceAdapter(BaseDataAdapter):
    adapter_name = "yfinance"

    @property
    def feed_type(self) -> str:
        return "yfinance_dev"

    def fetch_1m(
        self, ticker: str, days: int, include_extended_hours: bool = True
    ) -> pd.DataFrame:
        import yfinance as yf

        calc_days = max(1, min(7, int(days)))
        try:
            df = yf.download(
                tickers=ticker,
                period=f"{calc_days}d",
                interval="1m",
                prepost=include_extended_hours,
                progress=False,
                threads=False,
            )
        except Exception as e:
            log.warning(f"[{ticker}] YF historical error: {e}")
            return pd.DataFrame()

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(1)

        return self._normalize(df)

    def fetch_quote(self, ticker: str) -> dict:
        import yfinance as yf

        try:
            t = yf.Ticker(ticker)
            info = t.info
            # YF is extremely unreliable for live quotes, mapping roughly
            bid = info.get("bid", 0.0)
            ask = info.get("ask", 0.0)
            mp = info.get("currentPrice", 0.0)
            return {
                "ticker": ticker,
                "bid": bid,
                "ask": ask,
                "last": mp,
                "timestamp": datetime.now(ET),
                "feed": self.feed_type,
                "_quote_quality": "ok" if bid > 0 and ask > 0 else "degraded_delayed",
            }
        except Exception:
            return _null_quote(ticker, self.feed_type, "fetch_error")


from infrastructure.ib_connection import IBConnectionManager
from ib_insync import Stock, util
import asyncio


class IBAdapter(BaseDataAdapter):
    adapter_name = "ibkr"

    @property
    def feed_type(self) -> str:
        return "ibkr_paper"

    def _qualify_contract(self, ticker: str):
        from infrastructure.ib_connection import IBConnectionManager

        if not hasattr(self, "_conn_mgr") or self._conn_mgr is None:
            self._conn_mgr = IBConnectionManager()
            self._conn_mgr.connect()
        ib = self._conn_mgr.ib
        contract = Stock(ticker, "SMART", "USD")
        try:
            ib.qualifyContracts(contract)
        except Exception as e:
            log.warning(f"Could not qualify {ticker}: {e}")
        return contract

    def fetch_1m(
        self, ticker: str, days: int, include_extended_hours: bool = True
    ) -> pd.DataFrame:
        contract = self._qualify_contract(ticker)
        ib = self._conn_mgr.ib

        calc_days = max(1, int(days))
        duration = f"{calc_days} D"

        try:
            bars = ib.reqHistoricalData(
                contract,
                endDateTime="",
                durationStr=duration,
                barSizeSetting="1 min",
                whatToShow="TRADES",
                useRTH=not include_extended_hours,
                formatDate=1,
                keepUpToDate=False,
            )
        except Exception as e:
            log.warning(f"[{ticker}] IBKR historical data error: {e}")
            return pd.DataFrame()

        if not bars:
            return pd.DataFrame()

        df = util.df(bars)
        if df is None or df.empty:
            return pd.DataFrame()

        # Output Normalization
        if "date" in df.columns:
            df.rename(columns={"date": "timestamp"}, inplace=True)
        if "timestamp" in df.columns:
            df.set_index("timestamp", inplace=True)

        return self._normalize(df)

    def fetch_quote(self, ticker: str) -> dict:
        contract = self._qualify_contract(ticker)
        ib = self._conn_mgr.ib

        tickers_data = ib.reqTickers(contract)
        if not tickers_data:
            return _null_quote(ticker, self.feed_type, "fetch_error")

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
            "_quote_quality": "ok" if bid > 0 and ask > 0 else "invalid_bid_ask",
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
                        "_quote_quality": (
                            "ok" if bid > 0 and ask > 0 else "invalid_bid_ask"
                        ),
                    }
            except Exception as e:
                log.warning(f"Batch quote failed: {e}")

        for ticker in tickers:
            if ticker not in result:
                result[ticker] = _null_quote(
                    ticker, self.feed_type, "missing_after_bulk"
                )

        return result


def get_live_adapter(adapter_name: str) -> BaseDataAdapter:
    if adapter_name != "ibkr":
        raise ValueError(f"Unknown or unsupported data adapter: {adapter_name}")
    return IBAdapter()


class BatchFetcher:
    def __init__(self, adapter_name="yfinance"):
        self._adapter = get_live_adapter(adapter_name)

    def fetch_all(self, tickers: list, days: int = 2, **kwargs) -> dict:
        results = {}
        for ticker in tickers:
            # PACING SAFEGUARD to prevent IBKR Pacing Violations (Error 162)
            if (
                hasattr(self._adapter, "feed_type")
                and "ibkr" in self._adapter.feed_type
            ):
                try:
                    from ib_insync import util

                    util.sleep(1.0)
                except Exception:
                    import time

                    time.sleep(1.0)

            try:
                df = self._adapter.fetch_1m(ticker, days=days)
                if df is not None and not df.empty:
                    results[ticker] = df
            except Exception as e:
                log.error(f"Failed bulk historical fetch for {ticker}: {e}")
        return results

    def fetch_quotes(self, tickers: list) -> dict:
        return self._adapter.fetch_quotes(tickers)
