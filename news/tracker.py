"""
news/tracker.py — AlpacaTracker: tape-based promotion gateway.

For each ticker sourced from news or the discovery scan, the AlpacaTracker
verifies tape activity before promoting it into the live universe:

  1. Fetch the last 5–10 1m bars via Alpaca.
  2. Compute tape features (price velocity, volume acceleration, bar continuity).
  3. Run DATA_HEALTH check.
  4. If all thresholds pass → promote = True, source = SOURCE_TRACKER.

The tracker is NOT a scanner — it does not discover new names.
It acts as a confirmation gate for candidates already nominated by
the NewsIngestor or the discovery scanner.

Thresholds (all configurable via CONFIG.universe):
  tape_min_price_velocity_pct : price moved >= 0.5% in last 3 bars
  tape_min_vol_acceleration   : last bar vol >= 1.5 × 5-bar avg
  tape_min_bar_continuity     : >= 70% of expected 1m bars are present
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import pytz

from config.constants import (
    DH_BLOCK,
    EPSILON,
    FEED_ALPACA_IEX,
    SOURCE_TRACKER,
    TAPE_PROMOTED,
    TAPE_REJECTED_BARS,
    TAPE_REJECTED_HEALTH,
    TAPE_REJECTED_VELOCITY,
    TAPE_REJECTED_VOL,
)
from config.settings import CONFIG
from data.health import DataHealthValidator, DataHealthReport, current_session
from data.pipeline import MTFPipeline

log = logging.getLogger(__name__)
ET  = pytz.timezone("America/New_York")


# ─────────────────────────────────────────────────────────────────────────────
# RESULT TYPE
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class TapeResult:
    """Outcome of a single tape-tracking evaluation."""
    ticker:           str
    promoted:         bool        = False
    reason:           str         = ""
    price_velocity:   float       = 0.0    # (close[-1] - close[-3]) / close[-3]
    vol_acceleration: float       = 0.0    # close bar vol / mean(prev 5 bars)
    bar_continuity:   float       = 0.0    # fraction of expected 1m bars present
    spread_pct:       float       = 0.0    # latest quoted spread
    dh_status:        str         = ""
    dh_block_reason:  str         = ""
    feed_type:        str         = FEED_ALPACA_IEX
    evaluated_at:     Optional[datetime] = None
    bars_used:        int         = 0


# ─────────────────────────────────────────────────────────────────────────────
# TAPE FEATURE CALCULATIONS
# ─────────────────────────────────────────────────────────────────────────────

def _price_velocity(df_1m: pd.DataFrame, lookback: int = 3) -> float:
    """
    Fractional price change over last `lookback` 1m bars.
    Positive = upward momentum, negative = downward.
    """
    if df_1m is None or len(df_1m) < lookback + 1:
        return 0.0
    close = df_1m["close"]
    base  = close.iloc[-(lookback + 1)]
    if base <= EPSILON:
        return 0.0
    return float((close.iloc[-1] - base) / base)


def _vol_acceleration(df_1m: pd.DataFrame, lookback: int = 5) -> float:
    """
    Ratio of the most recent 1m bar volume to the mean of the prior `lookback` bars.
    > 1.0 = accelerating; < 1.0 = decelerating.
    """
    if df_1m is None or len(df_1m) < lookback + 1:
        return 0.0
    vol  = df_1m["volume"]
    last = float(vol.iloc[-1])
    avg  = float(vol.iloc[-(lookback + 1):-1].mean())
    if avg <= 0:
        return 1.0
    return round(last / (avg + EPSILON), 3)


def _bar_continuity(df_1m: pd.DataFrame, now: datetime, lookback_minutes: int = 15) -> float:
    """
    Fraction of expected 1m bars present in the last `lookback_minutes`.
    1.0 = no gaps, 0.0 = all bars missing.
    """
    if df_1m is None or df_1m.empty:
        return 0.0
    cutoff    = now - pd.Timedelta(minutes=lookback_minutes)
    recent    = df_1m[df_1m.index >= cutoff]
    if recent.empty:
        return 0.0
    expected  = lookback_minutes    # one bar per minute
    actual    = len(recent)
    return round(min(actual / expected, 1.0), 3)


# ─────────────────────────────────────────────────────────────────────────────
# ALPACA TRACKER
# ─────────────────────────────────────────────────────────────────────────────

class AlpacaTracker:
    """
    Validates tape activity for nominated tickers before promoting them
    into the live universe.

    Usage:
        tracker = AlpacaTracker()
        result  = tracker.evaluate("TSLA", df_1m, mtf_bars, quote)
    """

    def __init__(self) -> None:
        self._dh_validator = DataHealthValidator()
        self._cfg          = CONFIG.universe

    def evaluate(
        self,
        ticker:   str,
        df_1m:    pd.DataFrame,
        mtf_bars: Dict[str, pd.DataFrame],
        quote:    dict,
        feed_type: str = FEED_ALPACA_IEX,
        now:      datetime = None,
    ) -> TapeResult:
        """
        Evaluate a single ticker's tape and return a TapeResult.

        Args:
            ticker   : symbol to evaluate
            df_1m    : raw 1m bar DataFrame (multi-day, extended hours OK)
            mtf_bars : pre-built MTF dict from MTFPipeline.build()
            quote    : {'bid': float, 'ask': float, 'last': float, 'timestamp': datetime}
            feed_type: originating feed (used in DH check)
            now      : override for unit tests
        """
        now = now or datetime.now(ET)
        result = TapeResult(ticker=ticker, evaluated_at=now)

        if df_1m is None or df_1m.empty:
            result.reason = TAPE_REJECTED_BARS
            return result

        result.bars_used = len(df_1m)

        # ── Tape features ────────────────────────────────────────────────────
        velocity     = _price_velocity(df_1m, lookback=3)
        vol_accel    = _vol_acceleration(df_1m, lookback=5)
        continuity   = _bar_continuity(df_1m, now, lookback_minutes=15)
        bid          = quote.get("bid", 0.0)
        ask          = quote.get("ask", 0.0)
        mid          = (bid + ask) / 2 + EPSILON
        spread_pct   = (ask - bid) / mid

        result.price_velocity   = round(velocity,   4)
        result.vol_acceleration = round(vol_accel,  3)
        result.bar_continuity   = round(continuity, 3)
        result.spread_pct       = round(spread_pct, 5)
        result.feed_type        = feed_type

        # ── DATA_HEALTH check ────────────────────────────────────────────────
        dh: DataHealthReport = self._dh_validator.check(
            ticker            = ticker,
            mtf_bars          = mtf_bars,
            quote             = quote,
            prev_bar_count_1m = None,
            feed_type         = feed_type,
            now               = now,
        )
        result.dh_status      = dh.status
        result.dh_block_reason = dh.block_reason

        if dh.status == DH_BLOCK:
            result.reason = TAPE_REJECTED_HEALTH
            log.debug(
                f"[AlpacaTracker] {ticker} rejected — DATA_HEALTH BLOCK: {dh.block_reason}"
            )
            return result

        # ── Threshold gates ──────────────────────────────────────────────────
        cfg = self._cfg

        if continuity < cfg.tape_min_bar_continuity:
            result.reason = (
                f"{TAPE_REJECTED_BARS}(continuity={continuity:.0%} "
                f"< {cfg.tape_min_bar_continuity:.0%})"
            )
            return result

        # Velocity check: accept positive OR negative momentum (both signal)
        if abs(velocity) < cfg.tape_min_price_velocity_pct:
            result.reason = (
                f"{TAPE_REJECTED_VELOCITY}(|velocity|={abs(velocity):.3%} "
                f"< {cfg.tape_min_price_velocity_pct:.3%})"
            )
            return result

        if vol_accel < cfg.tape_min_vol_acceleration:
            result.reason = (
                f"{TAPE_REJECTED_VOL}(vol_accel={vol_accel:.2f} "
                f"< {cfg.tape_min_vol_acceleration:.2f})"
            )
            return result

        # ── Promoted ──────────────────────────────────────────────────────────
        result.promoted = True
        result.reason   = TAPE_PROMOTED
        log.debug(
            f"[AlpacaTracker] {ticker} PROMOTED "
            f"vel={velocity:.3%} vaccel={vol_accel:.2f} cont={continuity:.0%}"
        )
        return result

    def evaluate_batch(
        self,
        tickers:   List[str],
        bar_store: Dict[str, pd.DataFrame],          # ticker → df_1m
        mtf_store: Dict[str, Dict[str, pd.DataFrame]],  # ticker → mtf_bars
        quote_store: Dict[str, dict],                # ticker → quote
        feed_type:  str = FEED_ALPACA_IEX,
        now:        datetime = None,
    ) -> Dict[str, TapeResult]:
        """
        Evaluate a list of tickers.  Returns {ticker: TapeResult}.
        Skips missing data gracefully.
        """
        now    = now or datetime.now(ET)
        out: Dict[str, TapeResult] = {}

        for ticker in tickers:
            df_1m    = bar_store.get(ticker)
            mtf_bars = mtf_store.get(ticker, {})
            quote    = quote_store.get(ticker, {})

            out[ticker] = self.evaluate(
                ticker    = ticker,
                df_1m     = df_1m,
                mtf_bars  = mtf_bars,
                quote     = quote,
                feed_type = feed_type,
                now       = now,
            )

        n_promoted = sum(1 for r in out.values() if r.promoted)
        log.debug(
            f"[AlpacaTracker] batch: {n_promoted}/{len(tickers)} promoted"
        )
        return out


# ─────────────────────────────────────────────────────────────────────────────
# Module-level singleton
# ─────────────────────────────────────────────────────────────────────────────
alpaca_tracker = AlpacaTracker()
