"""
vwap.py — VWAP and Anchored VWAP calculations.

VWAP   = cumsum(typical_price × volume) / cumsum(volume)
Anchored VWAP can be anchored to any bar (open, pivot, gap, etc.)
"""

from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd
import pytz

from config.constants import EPSILON
from data.pipeline import bars_today, rth_bars

log = logging.getLogger(__name__)
ET = pytz.timezone("America/New_York")


def typical_price(df: pd.DataFrame) -> pd.Series:
    """(High + Low + Close) / 3"""
    return (df["high"] + df["low"] + df["close"]) / 3


def calc_vwap(df: pd.DataFrame) -> pd.Series:
    """
    Intraday session VWAP anchored to today's open (09:30 ET).

    Returns a Series aligned with df.index.
    """
    if df is None or df.empty:
        return pd.Series(dtype=float)

    # Anchor VWAP to true RTH open (09:30) so we capture the opening minute
    today_session = rth_bars(bars_today(df))
    if today_session.empty:
        return pd.Series(dtype=float)

    tp = typical_price(today_session)
    vol = today_session["volume"].replace(0, np.nan).fillna(EPSILON)

    cum_tpv = (tp * vol).cumsum()
    cum_vol = vol.cumsum()
    vwap = cum_tpv / cum_vol

    return vwap.rename("vwap")


def calc_anchored_vwap(df: pd.DataFrame, anchor_ts: pd.Timestamp) -> pd.Series:
    """
    VWAP anchored from a specific bar timestamp forward.
    Useful for gap-up anchored VWAP or post-halt restart.
    """
    if df is None or df.empty:
        return pd.Series(dtype=float)

    subset = df[df.index >= anchor_ts].copy()
    if subset.empty:
        return pd.Series(dtype=float)

    tp = typical_price(subset)
    vol = subset["volume"].replace(0, np.nan).fillna(EPSILON)

    vwap = (tp * vol).cumsum() / vol.cumsum()
    return vwap.rename("anchored_vwap")


def current_vwap(df_1m: pd.DataFrame) -> Optional[float]:
    """
    Return the most recent VWAP value (float) for current session.
    """
    vwap_series = calc_vwap(df_1m)
    if vwap_series.empty:
        return None
    return float(vwap_series.iloc[-1])


def price_above_vwap(last_close: float, df_1m: pd.DataFrame) -> bool:
    """True if last close is above current VWAP."""
    vwap = current_vwap(df_1m)
    if vwap is None:
        return False
    return last_close > vwap


def vwap_distance_pct(last_close: float, df_1m: pd.DataFrame) -> float:
    """Signed distance from VWAP as fraction of VWAP. Positive = above."""
    vwap = current_vwap(df_1m)
    if vwap is None or vwap <= EPSILON:
        return 0.0
    return (last_close - vwap) / vwap


def add_vwap_to_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convenience: attach a 'vwap' column to a session DataFrame in-place.
    Useful for backtesting or charting.
    """
    if df is None or df.empty:
        return df
    vwap = calc_vwap(df)
    df = df.copy()
    df["vwap"] = vwap
    return df
