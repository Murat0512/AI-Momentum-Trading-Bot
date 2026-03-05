"""
pipeline.py — Multi-timeframe bar pipeline.

Takes 1-minute bars as source-of-truth and resamples to all required
timeframes in one pass. Zero external fetches per timeframe.

Output per ticker:
  {
    "1min":  pd.DataFrame,
    "5min":  pd.DataFrame,
    "15min": pd.DataFrame,
    "60min": pd.DataFrame,
    "4h":    pd.DataFrame,
    "1D":    pd.DataFrame,
  }
"""

from __future__ import annotations

import logging
from typing import Dict, Optional

import pandas as pd
import pytz

from config.constants import TF_1M, TF_5M, TF_15M, TF_1H, TF_4H, TF_1D, ALL_TIMEFRAMES
from config.settings import CONFIG

log = logging.getLogger(__name__)
ET = pytz.timezone("America/New_York")

# Map internal labels → pandas resample rule
_PANDAS_RULE: Dict[str, str] = {
    TF_1M: "1min",
    TF_5M: "5min",
    TF_15M: "15min",
    TF_1H: "60min",
    TF_4H: "240min",
    TF_1D: "1D",
}

# OHLCV aggregation
_OHLCV_AGG = {
    "open": "first",
    "high": "max",
    "low": "min",
    "close": "last",
    "volume": "sum",
}


def resample_bars(df_1m: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    """
    Resample a 1m DataFrame to a given timeframe.
    Uses regular-market-hours-aware offset for daily.
    """
    if df_1m.empty:
        return pd.DataFrame()

    rule = _PANDAS_RULE[timeframe]

    if timeframe == TF_4H:
        # 4h bars anchored to 09:30 ET
        resampled = df_1m.resample(
            rule,
            origin="start_day",
            offset="9h30min",
            label="left",
            closed="left",
        ).agg(_OHLCV_AGG)
    elif timeframe == TF_1D:
        # Anchor at 09:30 ET so each daily bar covers one RTH session
        # rather than midnight-to-midnight calendar days.
        resampled = df_1m.resample(rule, origin="start_day", offset="9h30min").agg(
            _OHLCV_AGG
        )
    else:
        resampled = df_1m.resample(rule, label="left", closed="left").agg(_OHLCV_AGG)

    resampled.dropna(subset=["close"], inplace=True)
    return resampled


class MTFPipeline:
    """
    Multi-Timeframe Pipeline.

    Usage:
        pipeline = MTFPipeline()
        bars = pipeline.build(ticker, df_1m)
        # bars["5min"] → 5-minute OHLCV DataFrame
    """

    def __init__(self, timeframes: list[str] = None):
        self._timeframes = timeframes or CONFIG.data.resample_timeframes

    def build(self, ticker: str, df_1m: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Create all timeframe DataFrames from raw 1m bars."""
        if df_1m is None or df_1m.empty:
            log.warning(f"[{ticker}] MTFPipeline: empty 1m bars")
            return {}

        result: Dict[str, pd.DataFrame] = {TF_1M: df_1m.copy()}

        for tf in self._timeframes:
            if tf == TF_1M:
                continue
            try:
                resampled = resample_bars(df_1m, tf)
                result[tf] = resampled
                log.debug(f"[{ticker}] {tf}: {len(resampled)} bars")
            except Exception as exc:
                log.warning(f"[{ticker}] resample {tf} failed: {exc}")
                result[tf] = pd.DataFrame()

        return result

    def build_all(
        self,
        bars_dict: Dict[str, pd.DataFrame],
    ) -> Dict[str, Dict[str, pd.DataFrame]]:
        """Build MTF bars for every ticker in bars_dict."""
        mtf_all: Dict[str, Dict[str, pd.DataFrame]] = {}
        for ticker, df_1m in bars_dict.items():
            mtf_all[ticker] = self.build(ticker, df_1m)
        log.info(f"MTFPipeline: built MTF bars for {len(mtf_all)} tickers")
        return mtf_all


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _ensure_et(df: pd.DataFrame) -> pd.DataFrame:
    """
    Guarantee that df.index is tz-aware and in America/New_York.

    - Naive index  → tz_localize(ET)
    - Non-ET aware → tz_convert(ET)

    Called by every day/session filter so downstream code never silently
    operates on wrong-tz timestamps.
    """
    if df.empty:
        return df
    idx = df.index
    if idx.tzinfo is None:
        df = df.copy()
        df.index = idx.tz_localize(ET)
    elif str(idx.tzinfo) != str(ET):
        df = df.copy()
        df.index = idx.tz_convert(ET)
    return df


def bars_today(df: pd.DataFrame, today: pd.Timestamp = None) -> pd.DataFrame:
    """
    Filter DataFrame rows to today's ET date only.

    ``today`` may be supplied explicitly (useful for tests / replay);
    defaults to the live ET calendar date.
    """
    if df.empty:
        return df
    df = _ensure_et(df)
    _today = (today or pd.Timestamp.now(tz=ET)).normalize()
    return df[df.index.normalize() == _today]


def session_bars(
    df: pd.DataFrame, start: str = "09:35", end: str = "15:30"
) -> pd.DataFrame:
    """
    Filter to a custom session window (ET).  Default 09:35–15:30 is the
    *trade window* (order eligibility).  Prefer the named helpers below for
    analytical calculations.
    """
    if df.empty:
        return df
    df = _ensure_et(df)
    mask = (df.index.time >= pd.Timestamp(f"2000-01-01 {start}").time()) & (
        df.index.time <= pd.Timestamp(f"2000-01-01 {end}").time()
    )
    return df[mask]


def rth_bars(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return true Regular Trading Hours bars: 09:30–16:00 ET.

    Use for VWAP anchoring, HOD, structure, ATR — any calculation that
    should include the opening minute and the close.
    """
    return session_bars(df, start="09:30", end="16:00")


def trade_window_bars(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return the *order-eligibility* window: 09:35–15:30 ET.

    Use for order timing checks only — not for VWAP or level calculations.
    """
    return session_bars(df, start="09:35", end="15:30")


def premarket_bars(
    df: pd.DataFrame,
    start: str = "04:00",
    end: str = "09:29",
    today: pd.Timestamp = None,
) -> pd.DataFrame:
    """
    Return today's pre-market bars (04:00–09:29 ET by default).

    Day-bounded so bars from prior sessions are never included, which
    prevents PMH from being inflated by old premarket highs.

    ``today`` may be supplied explicitly for tests / event replay.
    """
    if df.empty:
        return df
    # Restrict to today first, then apply the time-of-day mask.
    df = bars_today(df, today=today)
    return session_bars(df, start=start, end=end)


def bar_count_summary(mtf_bars: Dict[str, pd.DataFrame]) -> dict:
    """Return bar count per timeframe (for logging/debug)."""
    return {tf: len(df) for tf, df in mtf_bars.items() if df is not None}


def get_last_closed_low(ticker: str, timeframe: str = TF_1M) -> Optional[float]:
    """
    Return the last *closed* candle low from cache for ticker/timeframe.

    Uses iloc[-2] to avoid using the currently-forming bar.
    Returns None when insufficient bars are available.
    """
    try:
        from data.cache import bar_cache

        df = bar_cache.get_tf(ticker, timeframe)
        if df is None or len(df) < 2:
            return None
        return float(df["low"].iloc[-2])
    except Exception:
        return None
