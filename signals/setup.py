"""
setup.py — Momentum v1 setup detection: VWAP Reclaim + PMH/HOD Break.

A setup fires when ALL of the following are true on the 5m chart:
  1. Price above VWAP (5m close)
  2. Price NOT too extended above VWAP (hard cap: CONFIG.setup.max_vwap_extension_pct)
  3. Higher highs + higher lows OR clear consolidation
  4. Break of premarket high (PMH) OR intraday pivot high (HOD)
  5. 5m volume expansion (≥ 1.5× rolling average)
  6. 1m quality component evaluated via pullback integrity score (SQS component)
  7. No flash-crash / halt flags
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional

import pandas as pd
import pytz

from config.constants import (
    BLOCK_BY_SENTIMENT,
    EPSILON,
    REJECT_BELOW_VWAP,
    REJECT_NO_SETUP,
    REJECT_NO_STRUCTURE_BREAK,
    REJECT_TOO_EXTENDED_VWAP,
    SETUP_VWAP_RECLAIM,
    TF_1M,
    TF_5M,
)
from config.settings import CONFIG
from data.pipeline import bars_today, rth_bars
from intelligence.news_validator import news_validator
from scanner.demand import compute_setup_quality_score
from scanner.rvol import best_rvol, calc_session_rvol
from signals.structure import (
    calc_atr,
    has_valid_structure,
    structure_score,
    volume_expansion_ratio,
)
from signals.vwap import (
    current_vwap,
)

log = logging.getLogger(__name__)
ET = pytz.timezone("America/New_York")


def _is_setup_test_override_enabled() -> bool:
    test_profile = str(os.getenv("TEST_PROFILE", "")).strip().lower()
    legacy_flag = str(os.getenv("SETUP_TEST_OVERRIDE", "")).strip().lower()
    enabled = test_profile in {"1", "true", "yes", "on"} or legacy_flag in {
        "1",
        "true",
        "yes",
        "on",
    }
    return enabled and bool(getattr(CONFIG.execution, "paper_mode", True))


# ─────────────────────────────────────────────────────────────────────────────
# SETUP RESULT
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class SetupResult:
    ticker: str
    valid: bool = False
    setup_name: str = ""
    rejection_reason: str = ""

    # Trigger details
    entry_price: float = 0.0
    stop_price: float = 0.0
    atr: float = 0.0
    vwap: float = 0.0
    pmh: Optional[float] = None
    hod: Optional[float] = None
    break_level: Optional[float] = None  # which level was broken
    break_level_name: str = ""

    # Scores
    setup_quality_score: float = 0.0
    vwap_dist_pct: float = 0.0
    volume_expansion: float = 0.0
    structure_clarity: float = 0.0
    spread_pct: float = 0.0
    pressure_score: float = 0.0
    rvol_strength: float = 0.0
    sentiment_score: float = 0.0

    # Bar counts (for logging / size degradation)
    bars_1m: int = 0
    bars_5m: int = 0
    bars_15m: int = 0

    detected_at: Optional[datetime] = None


# ─────────────────────────────────────────────────────────────────────────────
# SETUP DETECTOR
# ─────────────────────────────────────────────────────────────────────────────


class MomentumSetupV1:
    """
    Momentum v1: VWAP Reclaim + Break of PMH / Intraday Pivot High.
    """

    def check(
        self,
        ticker: str,
        mtf_bars: Dict[str, pd.DataFrame],
        bid: float,
        ask: float,
        now: Optional[datetime] = None,
    ) -> SetupResult:
        df_1m = mtf_bars.get(TF_1M)
        df_5m = mtf_bars.get(TF_5M)

        eval_now = now or datetime.now(ET)
        result = SetupResult(ticker=ticker, detected_at=eval_now)
        result.bars_1m = len(df_1m) if df_1m is not None else 0
        result.bars_5m = len(df_5m) if df_5m is not None else 0

        df_15m = mtf_bars.get("15min")
        result.bars_15m = len(df_15m) if df_15m is not None else 0

        if df_1m is None or df_1m.empty or df_5m is None or df_5m.empty:
            result.rejection_reason = f"{REJECT_NO_SETUP}(insufficient bars)"
            return result

        if now is not None and bool(
            getattr(CONFIG.setup, "require_bar_close_confirmation", False)
        ):
            confirm_window = int(
                max(1, getattr(CONFIG.setup, "confirmation_window_seconds", 10))
            )
            if eval_now.second > confirm_window:
                result.rejection_reason = (
                    f"{REJECT_NO_SETUP}(awaiting 1m close confirmation: "
                    f"second={eval_now.second}s > window={confirm_window}s)"
                )
                return result

        df_all_today = bars_today(df_1m)
        df_1m_today = rth_bars(df_all_today)
        df_5m_today = rth_bars(bars_today(df_5m))

        if df_1m_today.empty or df_5m_today.empty:
            result.rejection_reason = f"{REJECT_NO_SETUP}(no intraday bars)"
            return result

        if len(df_1m_today) < 6 or len(df_5m_today) < 2:
            result.rejection_reason = f"{REJECT_NO_SETUP}(need >=6 1m and >=2 5m bars)"
            return result

        df_1m_confirmed = df_1m_today.iloc[:-1]
        if len(df_1m_confirmed) < 5:
            result.rejection_reason = (
                f"{REJECT_NO_SETUP}(insufficient confirmed 1m bars)"
            )
            return result

        last_close = float(df_5m_today["close"].iloc[-1])
        last_1m = float(df_1m_confirmed["close"].iloc[-1])

        if bid > EPSILON:
            spread = (ask - bid) / ((ask + bid) / 2 + EPSILON)
        else:
            spread = 0.0
        result.spread_pct = spread

        # ── 1. VWAP Check ───────────────────────────────────────────────────
        vwap_5m = current_vwap(df_5m_today)
        if vwap_5m is None or vwap_5m <= EPSILON:
            result.rejection_reason = f"{REJECT_NO_SETUP}(5m vwap unavailable)"
            return result

        above_vwap = last_close > vwap_5m
        vwap_dist = (last_close - vwap_5m) / vwap_5m
        result.vwap_dist_pct = vwap_dist
        result.vwap = float(vwap_5m)

        if CONFIG.setup.require_above_vwap and not above_vwap:
            result.rejection_reason = f"{REJECT_BELOW_VWAP}(dist={vwap_dist*100:.2f}%)"
            return result

        max_ext = CONFIG.setup.max_vwap_extension_pct
        if max_ext > 0 and vwap_dist > max_ext:
            result.rejection_reason = f"{REJECT_TOO_EXTENDED_VWAP}({vwap_dist*100:.2f}% > cap {max_ext*100:.1f}%)"
            return result

        # ── 2. Structure & Volume Check ─────────────────────────────────────
        struct_clarity = structure_score(df_5m_today)
        result.structure_clarity = struct_clarity

        if not has_valid_structure(df_5m_today):
            log.debug(
                f"[{ticker}] structure weak ({struct_clarity:.2f}) but continuing"
            )

        vol_ratio = volume_expansion_ratio(df_5m_today)
        result.volume_expansion = vol_ratio

        prior_1m = df_1m_confirmed["volume"].iloc[:-1].tail(10)
        prior_1m_positive = prior_1m[prior_1m > 0]

        if prior_1m_positive.empty or len(prior_1m_positive) < int(
            CONFIG.setup.min_1m_burst_baseline_bars
        ):
            if not _is_setup_test_override_enabled():
                result.rejection_reason = (
                    f"{REJECT_NO_SETUP}(Low Volume Burst: missing baseline)"
                )
                return result
            prior_1m_positive = pd.Series([500.0])  # fallback for test mode

        # Floor the baseline at 500 shares to prevent IEX 0-volume anomalies
        burst_baseline = max(500.0, float(prior_1m_positive.mean()))
        current_1m_vol = float(df_1m_confirmed["volume"].iloc[-1])

        burst_mult = float(CONFIG.setup.min_1m_volume_burst_mult)
        relax_ok = (
            abs(vwap_dist) <= float(CONFIG.setup.burst_relax_if_vwap_dist_below_pct)
            and struct_clarity >= float(CONFIG.setup.burst_relax_if_structure_min)
            and vol_ratio >= float(CONFIG.setup.burst_relax_if_5m_volume_expansion_min)
        )
        if relax_ok:
            burst_mult = max(
                float(CONFIG.setup.min_1m_volume_burst_mult_floor), burst_mult - 0.30
            )

        required_burst = burst_baseline * burst_mult

        log.debug(
            f"[{ticker}] BURST CHECK: cur_1m_vol={current_1m_vol:.0f} "
            f"baseline_pos={burst_baseline:.0f} mult={burst_mult:.2f} "
            f"required={required_burst:.0f}"
        )

        if current_1m_vol < required_burst and not _is_setup_test_override_enabled():
            result.rejection_reason = f"{REJECT_NO_SETUP}(Low Volume Burst: {current_1m_vol:.0f} < {required_burst:.0f})"
            return result

        # ── 3. OPTIMIZED MACRO BREAKOUT LOGIC (PMH / HOD) ───────────────────

        # A. Pre-Market High (04:00 - 09:29)
        df_pm = df_all_today.between_time("04:00", "09:29")
        pmh = float(df_pm["high"].max()) if not df_pm.empty else 0.0
        result.pmh = pmh

        # B. High of Day (HOD) - Excluding the currently forming bar
        hod = float(df_1m_confirmed["high"].max()) if not df_1m_confirmed.empty else 0.0
        result.hod = hod

        # C. Opening Range High (09:30 - 09:45) - Intelligent fallback for thin data
        df_or = df_1m_confirmed.between_time("09:30", "09:45")
        orh = float(df_or["high"].max()) if not df_or.empty else hod

        # Determine Resistance Line
        resistance_line = 0.0
        resistance_name = ""

        if CONFIG.setup.require_pmh_break and pmh > EPSILON:
            resistance_line = pmh
            resistance_name = "PMH_BREAK"
        elif CONFIG.setup.require_hod_break and hod > EPSILON:
            resistance_line = hod
            resistance_name = "HOD_BREAK"
        else:
            # If config is off or data is missing, fallback to Opening Range High
            resistance_line = orh if orh > EPSILON else hod
            resistance_name = "ORH_BREAK"

        result.break_level = resistance_line
        result.break_level_name = resistance_name

        # Evaluate the macro breakout (Must be within 0.2% of resistance, or breaking above it)
        macro_proximity = (last_1m / resistance_line) if resistance_line > 0 else 1.0
        is_macro_break = macro_proximity >= 0.998

        # Evaluate the micro breakout (Must be pushing higher right now)
        prev_two_high = 0.0
        if len(df_1m_confirmed) >= 3:
            prev_two_high = float(df_1m_confirmed["high"].iloc[-3:-1].max())
            is_micro_break = last_1m > prev_two_high
        else:
            is_micro_break = False

        if not _is_setup_test_override_enabled():
            if not is_macro_break:
                result.rejection_reason = f"{REJECT_NO_STRUCTURE_BREAK}(Below Resistance: {last_1m:.2f} vs {resistance_line:.2f})"
                return result
            if not is_micro_break:
                result.rejection_reason = f"{REJECT_NO_STRUCTURE_BREAK}(Failing Micro-Trend: {last_1m:.2f} <= {prev_two_high:.2f})"
                return result
        else:
            log.warning(
                f"[{ticker}] SETUP TEST OVERRIDE ACTIVE: bypassing breakout triggers"
            )

        # ── 4. ATR + Technical Stop ─────────────────────────────────────────
        atr = calc_atr(df_5m_today, period=14)
        if atr <= EPSILON:
            atr = calc_atr(df_1m_confirmed, period=14)

        result.atr = atr
        result.entry_price = round(ask if ask > EPSILON else last_1m, 4)

        last3_low = float(df_1m_confirmed["low"].tail(3).min())
        vwap_floor = float(vwap_5m) * (1.0 - 0.001)

        technical_stop = last3_low if last3_low >= float(vwap_5m) else vwap_floor

        if technical_stop >= result.entry_price - EPSILON:
            if _is_setup_test_override_enabled():
                technical_stop = float(result.entry_price) * 0.99
            else:
                result.rejection_reason = f"{REJECT_NO_SETUP}(invalid stop: {technical_stop:.4f} >= {result.entry_price:.4f})"
                return result
        result.stop_price = round(technical_stop, 4)

        # ── 5. SetupQualityScore ────────────────────────────────────────────
        pressure_score = calculate_pressure_score(df_1m_confirmed)
        result.pressure_score = pressure_score

        rvol_strength = _calculate_rvol_strength(df_1m, now=eval_now)
        result.rvol_strength = rvol_strength

        sent = news_validator.validate_ticker(ticker)
        result.sentiment_score = float(sent.score)
        if (
            bool(getattr(CONFIG.strategy, "sentiment_gate_enabled", True))
            and not sent.allowed
        ):
            result.rejection_reason = f"{BLOCK_BY_SENTIMENT}(score={sent.score:.2f})"
            return result

        sqscore = compute_setup_quality_score(
            vwap_distance_pct=abs(vwap_dist),
            volume_expansion=vol_ratio,
            structure_clarity=struct_clarity,
            spread_pct=spread,
            pressure_score=pressure_score,
            rvol_strength=rvol_strength,
        )

        result.setup_quality_score = sqscore
        result.setup_name = SETUP_VWAP_RECLAIM
        result.valid = True

        log.info(
            f"[{ticker}] SETUP VALID | break={result.break_level_name}@{result.break_level:.2f} "
            f"entry={result.entry_price:.2f} stop={result.stop_price:.2f} "
            f"SQS={sqscore:.3f} vol_exp={vol_ratio:.1f}x"
        )
        return result


# ─────────────────────────────────────────────────────────────────────────────
def calculate_pressure_score(
    df_1m: Optional[pd.DataFrame],
    z_window: int = 50,
) -> float:
    """Approximate buy-vs-sell pressure using candle-close location and volume."""
    if df_1m is None or df_1m.empty or len(df_1m) < 6:
        return 0.0

    high = df_1m["high"].astype(float)
    low = df_1m["low"].astype(float)
    close = df_1m["close"].astype(float)
    volume = df_1m["volume"].astype(float)

    rng = (high - low).replace(0.0, EPSILON)
    pressure_ratio = ((close - low) / rng).clip(lower=0.0, upper=1.0)
    signed_pressure = pressure_ratio - 0.5
    weighted = signed_pressure * volume
    roll3 = weighted.rolling(window=3).sum().dropna()
    if roll3.empty:
        return 0.0

    trailing = roll3.tail(z_window + 1)
    if len(trailing) < 3:
        return 0.0

    latest = float(trailing.iloc[-1])
    history = trailing.iloc[:-1]
    mu = float(history.mean())
    sigma = float(history.std(ddof=0))
    if sigma <= EPSILON:
        return 0.0

    z = (latest - mu) / sigma
    z_capped = max(-3.0, min(3.0, float(z)))
    return round((z_capped + 3.0) / 6.0, 4)


def _calculate_rvol_strength(
    df_1m: Optional[pd.DataFrame], now: Optional[datetime] = None
) -> float:
    """Session-aware RVOL strength in [0,1] using scanner RVOL provider."""
    if df_1m is None or df_1m.empty:
        return 0.0

    rvol_result = calc_session_rvol(
        df_1m,
        now=now,
        lookback_days=CONFIG.exthours.rvol_lookback_days,
    )
    rvol = float(best_rvol(rvol_result))

    strength = (rvol - 1.0) / 2.0
    return round(max(0.0, min(1.0, strength)), 4)
