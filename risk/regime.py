"""
regime.py — Market regime detection.

Regime is used ONLY to modulate risk, NOT to block trading.

  TREND  → normal risk parameters
  CHOP   → raise RVOL req, tighten spread, reduce size, faster time stop
  RANGE  → same as CHOP but slightly less aggressive reduction

Regime is determined from SPY's intraday behavior.
"""

from __future__ import annotations

import logging
from typing import Dict, Optional

import numpy as np
import pandas as pd
import pytz

from config.constants import REGIME_CHOP, REGIME_RANGE, REGIME_TREND, TF_5M
from config.settings import CONFIG
from data.pipeline import bars_today, session_bars

log = logging.getLogger(__name__)
ET  = pytz.timezone("America/New_York")


def calc_adx(df: pd.DataFrame, period: int = 14) -> float:
    """
    Average Directional Index (ADX) — measures trend strength.
    Returns 0.0 if insufficient data.
    """
    if df is None or len(df) < period + 2:
        return 0.0

    high  = df["high"]
    low   = df["low"]
    close = df["close"]

    # True Range
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low  - prev_close).abs(),
    ], axis=1).max(axis=1)

    # Directional Movement
    up_move   = high.diff()
    down_move = -low.diff()

    dm_plus  = pd.Series(np.where((up_move > down_move) & (up_move > 0), up_move,  0.0), index=df.index)
    dm_minus = pd.Series(np.where((down_move > up_move) & (down_move > 0), down_move, 0.0), index=df.index)

    # Smoothed TR and DM
    atr14    = tr.ewm(span=period, adjust=False).mean()
    di_plus  = 100 * dm_plus.ewm(span=period,  adjust=False).mean() / (atr14 + 1e-9)
    di_minus = 100 * dm_minus.ewm(span=period, adjust=False).mean() / (atr14 + 1e-9)

    dx  = 100 * (di_plus - di_minus).abs() / (di_plus + di_minus + 1e-9)
    adx = dx.ewm(span=period, adjust=False).mean()

    val = adx.iloc[-1]
    return float(val) if not np.isnan(val) else 0.0


def calc_spy_intraday_range_pct(df_spy_1m: pd.DataFrame) -> float:
    """SPY intraday range as fraction of open price."""
    today = session_bars(bars_today(df_spy_1m))
    if today.empty:
        return 0.0
    h = today["high"].max()
    l = today["low"].min()
    o = today["open"].iloc[0]
    if o <= 0:
        return 0.0
    return float((h - l) / o)


class RegimeDetector:
    """
    Determines current market regime from SPY 5m bars.

    Returns: "TREND" | "CHOP" | "RANGE"
    """

    def __init__(self, spy_mtf: Dict[str, pd.DataFrame] = None):
        self._spy_mtf = spy_mtf  # updated each cycle by engine

    def update(self, spy_mtf: Dict[str, pd.DataFrame]) -> None:
        self._spy_mtf = spy_mtf

    def detect(self) -> str:
        cfg = CONFIG.regime

        if self._spy_mtf is None:
            log.debug("RegimeDetector: no SPY bars, defaulting to TREND")
            return REGIME_TREND

        df_5m_spy = self._spy_mtf.get(TF_5M)

        # ADX-based detection
        adx = calc_adx(df_5m_spy) if df_5m_spy is not None and len(df_5m_spy) >= 16 else 0.0

        # SPY intraday range proxy
        df_1m_spy  = self._spy_mtf.get("1min")
        spy_range  = calc_spy_intraday_range_pct(df_1m_spy) if df_1m_spy is not None else 0.0

        log.debug(f"RegimeDetector: ADX={adx:.1f} SPY_range={spy_range*100:.2f}%")

        if adx >= cfg.adx_trend_threshold:
            regime = REGIME_TREND
        elif adx <= cfg.adx_chop_threshold or spy_range <= cfg.spy_range_chop_threshold:
            regime = REGIME_CHOP
        else:
            regime = REGIME_RANGE

        log.info(f"RegimeDetector → {regime} (ADX={adx:.1f}, spy_range={spy_range*100:.2f}%)")
        return regime


# Module-level singleton
regime_detector = RegimeDetector()
