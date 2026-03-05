"""
structure.py — Market structure detection.

Provides:
  - Higher High / Higher Low detection on any timeframe
  - Pivot high identification (premarket high, intraday HOD)
  - Breakout confirmation (close above level)
  - Structure clarity score (0–1) for SetupQualityScore
"""

from __future__ import annotations

import logging
from typing import Optional, Tuple

import numpy as np
import pandas as pd
import pytz

from config.settings import CONFIG
from data.pipeline import bars_today, premarket_bars, rth_bars

log = logging.getLogger(__name__)
ET = pytz.timezone("America/New_York")


# ─────────────────────────────────────────────────────────────────────────────
# HIGHER HIGHS / HIGHER LOWS
# ─────────────────────────────────────────────────────────────────────────────


def detect_hh_hl(df: pd.DataFrame, lookback: int = 4) -> Tuple[int, int]:
    """
    Count higher highs and higher lows in the last `lookback` bars.

    Returns: (hh_count, hl_count)
    """
    if df is None or len(df) < 2:
        return 0, 0

    tail = df.tail(lookback + 1)
    hh, hl = 0, 0

    for i in range(1, len(tail)):
        if tail["high"].iloc[i] > tail["high"].iloc[i - 1]:
            hh += 1
        if tail["low"].iloc[i] > tail["low"].iloc[i - 1]:
            hl += 1

    return hh, hl


def structure_score(df_5m: pd.DataFrame) -> float:
    """
    Returns a 0–1 score reflecting structural quality.

    Higher = cleaner uptrend with HH/HL pattern.
    """
    cfg = CONFIG.setup
    lookback = cfg.hh_hl_lookback
    required = cfg.min_hh_hl_count

    if df_5m is None or len(df_5m) < lookback + 1:
        return 0.0

    hh, hl = detect_hh_hl(df_5m, lookback)
    combo = hh + hl
    max_possible = lookback * 2
    return round(min(combo / max_possible, 1.0), 3)


def has_valid_structure(df_5m: pd.DataFrame) -> bool:
    """True if structure meets minimum HH/HL requirement."""
    cfg = CONFIG.setup
    hh, hl = detect_hh_hl(df_5m, cfg.hh_hl_lookback)
    return (hh + hl) >= cfg.min_hh_hl_count


# ─────────────────────────────────────────────────────────────────────────────
# PIVOT LEVELS
# ─────────────────────────────────────────────────────────────────────────────


def premarket_high(df_1m: pd.DataFrame, today: pd.Timestamp = None) -> Optional[float]:
    """
    Return today's premarket session high.

    ``today`` may be supplied for tests / replay; defaults to live ET date.
    Always day-bounded (see premarket_bars) so yesterday's PM highs are
    never included.
    """
    pm = premarket_bars(df_1m, today=today)
    if pm.empty:
        return None
    return float(pm["high"].max())


def premarket_low(df_1m: pd.DataFrame, today: pd.Timestamp = None) -> Optional[float]:
    pm = premarket_bars(df_1m, today=today)
    if pm.empty:
        return None
    return float(pm["low"].min())


def intraday_high(df_1m: pd.DataFrame, exclude_last: int = 1) -> Optional[float]:
    """
    Today's session high-of-day (HOD) using true RTH bars (09:30–16:00 ET).

    ``exclude_last`` strips that many trailing bars before taking the max,
    preventing the currently-open bar from self-confirming a breakout.
    Set to 0 for backtests where all bars are already closed.
    """
    today = rth_bars(bars_today(df_1m))
    if today.empty:
        return None
    if exclude_last > 0 and len(today) > exclude_last:
        today = today.iloc[:-exclude_last]
    return float(today["high"].max())


def intraday_pivot_high(df_5m: pd.DataFrame, n_bars: int = 10) -> Optional[float]:
    """
    Most recent *confirmed* pivot high on 5m chart.

    A pivot high is confirmed when the bar's high is strictly greater than
    all highs both to its left and right within the look-back window.
    Returns ``None`` if no confirmed pivot is found (callers treat None as
    "no level"), preventing weak pivots from acting as break triggers.
    """
    if df_5m is None or len(df_5m) < 3:
        return None

    window = df_5m.tail(n_bars)
    max_idx = window["high"].idxmax()
    pivot = window["high"].max()

    pos = window.index.get_loc(max_idx)
    if pos == 0 or pos == len(window) - 1:
        # Edge positions cannot be confirmed (no bars on one side)
        return None

    left = window["high"].iloc[:pos].max()
    right = window["high"].iloc[pos + 1 :].max()
    if left < pivot and right < pivot:
        return float(pivot)

    # Not confirmed — return None so callers don’t treat it as a break level
    return None


def key_resistance_levels(
    df_1m: pd.DataFrame,
    df_5m: pd.DataFrame,
    today: pd.Timestamp = None,
) -> dict:
    """
    Return all key levels for the setup check.

    {
      "pmh":      float | None,
      "pml":      float | None,
      "hod":      float | None,  # excludes current bar (self-confirm guard)
      "pivot_5m": float | None,  # None if not confirmed
    }
    ``today`` is threaded through to premarket_bars so tests / replay can
    pin the reference date.
    """
    return {
        "pmh": premarket_high(df_1m, today=today),
        "pml": premarket_low(df_1m, today=today),
        "hod": intraday_high(df_1m, exclude_last=1),
        "pivot_5m": intraday_pivot_high(df_5m),
    }


# ─────────────────────────────────────────────────────────────────────────────
# BREAKOUT DETECTION
# ─────────────────────────────────────────────────────────────────────────────


def broke_above(
    level: Optional[float], last_close: float, tolerance: float = 0.001
) -> bool:
    """
    True if last_close broke above `level` with no meaningful tolerance.
    tolerance = 0.1% buffer to avoid noise.
    """
    if level is None or level <= 0:
        return False
    return last_close >= level * (1 + tolerance)


def volume_expansion_on_bar(df_5m: pd.DataFrame, multiplier: float = None) -> bool:
    """
    True if the most recent 5m bar has volume >= multiplier × rolling average.
    """
    multiplier = multiplier or CONFIG.setup.volume_expansion_multiplier
    if df_5m is None or len(df_5m) < 5:
        return False

    recent_vol = df_5m["volume"].iloc[-1]
    avg_vol = df_5m["volume"].iloc[:-1].rolling(10).mean().iloc[-1]

    if np.isnan(avg_vol) or avg_vol <= 0:
        avg_vol = df_5m["volume"].iloc[:-1].mean()

    return recent_vol >= avg_vol * multiplier


def volume_expansion_ratio(df_5m: pd.DataFrame) -> float:
    """
    Return the actual expansion ratio of the last bar vs rolling-10 average.
    Consistent with volume_expansion_on_bar() which uses the same baseline.
    """
    if df_5m is None or len(df_5m) < 2:
        return 0.0
    recent = df_5m["volume"].iloc[-1]
    avg = df_5m["volume"].iloc[:-1].rolling(10).mean().iloc[-1]
    if np.isnan(avg) or avg <= 0:
        avg = df_5m["volume"].iloc[:-1].mean()
    if avg <= 0:
        return 0.0
    return round(recent / avg, 2)


# ─────────────────────────────────────────────────────────────────────────────
# ATR
# ─────────────────────────────────────────────────────────────────────────────


def calc_atr(df: pd.DataFrame, period: int = 14) -> float:
    """
    Average True Range over `period` bars.
    Falls back gracefully if insufficient data.
    """
    if df is None or len(df) < 2:
        return 0.0

    high = df["high"]
    low = df["low"]
    close = df["close"]

    prev_close = close.shift(1)
    tr = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    atr = tr.rolling(period).mean().iloc[-1]
    return float(atr) if not np.isnan(atr) else float(tr.mean())
