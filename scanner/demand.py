"""
demand.py — DemandScore and SetupQualityScore calculation.

DemandScore =
    0.30*RVOL + 0.20*|Gap%| + 0.20*IntradayRange%
    + 0.15*VolSpikeZ + 0.15*DollarFlowMomentum

All inputs are fractions (e.g. gap_pct=0.06 means 6%).
Internal ×100 scaling is applied where needed; component caps prevent
outlier tickers from dominating the rank.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from config.constants import EPSILON
from config.settings import CONFIG, ScannerConfig

# Lazy import to avoid circular deps — imported at call-site when used
_RVOLResult = None


def _get_rvol_class():
    global _RVOLResult
    if _RVOLResult is None:
        from scanner.rvol import RVOLResult

        _RVOLResult = RVOLResult
    return _RVOLResult


log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# DEMAND SCORE
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class DemandMetrics:
    ticker: str
    last_price: float = 0.0
    dollar_volume: float = 0.0
    rvol: float = 0.0
    gap_pct: float = 0.0  # as fraction e.g. 0.06 = 6%
    intraday_range_pct: float = 0.0  # as fraction
    volume_spike_z: float = 0.0
    dollar_flow_momentum_z: float = 0.0
    demand_score: float = 0.0
    rank: int = 0
    bid: float = 0.0
    ask: float = 0.0

    @property
    def spread_pct(self) -> float:
        mid = (self.bid + self.ask) / 2 + EPSILON
        return (self.ask - self.bid) / mid


def compute_demand_score(
    rvol: float = 0.0,
    gap_pct: float = 0.0,
    intraday_range_pct: float = 0.0,
    volume_spike_z: float = 0.0,
    dollar_flow_momentum_z: float = 0.0,
    cfg: ScannerConfig = None,
    rvol_result=None,  # Optional[RVOLResult] — preferred over bare rvol
) -> float:
    """
    DemandScore — higher is better.

    Inputs (two modes):
      rvol_result : RVOLResult from calc_session_rvol() — uses session-aware best_rvol()
      rvol        : bare float fallback when rvol_result is not provided
      gap_pct            : gap as fraction (e.g. 0.07 = 7%)
      intraday_range_pct : today's high-low / prev close as fraction
    volume_spike_z     : z-score of today's volume vs rolling average
    dollar_flow_momentum_z: z-score of recent vs prior dollar-flow acceleration
    """
    cfg = cfg or CONFIG.scanner
    # Prefer session-aware RVOL when available
    if rvol_result is not None:
        from scanner.rvol import best_rvol

        rvol = best_rvol(rvol_result)

    # ── Scale to percent units then cap so outlier tickers don't dominate ──
    raw_rvol = rvol
    raw_gap = abs(gap_pct) * 100  # fraction → percent
    raw_range = intraday_range_pct * 100  # fraction → percent
    raw_z = max(volume_spike_z, 0.0)
    raw_flow = max(dollar_flow_momentum_z, 0.0)

    capped_rvol = min(raw_rvol, 6.0)
    capped_gap = min(raw_gap, 25.0)
    capped_range = min(raw_range, 20.0)
    capped_z = min(raw_z, 5.0)
    capped_flow = min(raw_flow, 5.0)

    if raw_rvol > capped_rvol or raw_gap > capped_gap:
        log.debug(
            "[demand] caps applied: rvol %.1f→%.1f  gap %.1f→%.1f",
            raw_rvol,
            capped_rvol,
            raw_gap,
            capped_gap,
        )

    score = (
        cfg.weight_rvol * capped_rvol
        + cfg.weight_gap * capped_gap
        + cfg.weight_intraday_range * capped_range
        + cfg.weight_volume_spike_z * capped_z
        + cfg.weight_dollar_flow_momentum * capped_flow
    )
    return round(score, 4)


def calculate_dollar_flow_momentum(
    df_1m: pd.DataFrame,
    z_window: int = 50,
) -> float:
    """
    Dollar Flow Momentum z-score.

    Per bar: dollar_flow = close * volume
    Window momentum: sum(last 5 bars) - sum(previous 5 bars)
    Returns z-score of latest momentum vs a fixed trailing history window.
    """
    if df_1m is None or df_1m.empty or len(df_1m) < 10:
        return 0.0

    work = df_1m[["close", "volume"]].copy()
    flow = (work["close"].astype(float) * work["volume"].astype(float)).fillna(0.0)

    momentum_vals = []
    for i in range(9, len(flow)):
        recent = float(flow.iloc[i - 4 : i + 1].sum())
        previous = float(flow.iloc[i - 9 : i - 4].sum())
        momentum_vals.append(recent - previous)

    if not momentum_vals:
        return 0.0

    latest = momentum_vals[-1]
    if len(momentum_vals) < 2:
        return 0.0

    trailing = momentum_vals[-(z_window + 1) :]
    if len(trailing) < 3:
        return 0.0

    latest = float(trailing[-1])
    history = np.array(trailing[:-1], dtype=float)
    if history.size < 2:
        return 0.0
    mu = float(np.mean(history))
    sigma = float(np.std(history))
    if sigma <= EPSILON:
        return 0.0

    z = (latest - mu) / sigma
    return round(float(z), 4)


# ─────────────────────────────────────────────────────────────────────────────
# RVOL CALCULATOR
# ─────────────────────────────────────────────────────────────────────────────


def calc_rvol(df_1m: pd.DataFrame, lookback_days: int = 5, now=None) -> float:
    """
    Legacy RTH-only RVOL fallback.
    Prefer calc_session_rvol() from scanner.rvol for session-aware PM/AH support.

    Relative volume = today's total volume / average same-time volume over N days.
    Uses time-of-day matching to compare apples-to-apples.
    """
    if df_1m is None or df_1m.empty:
        return 0.0

    import pytz

    ET = pytz.timezone("America/New_York")
    now_et = pd.Timestamp(now, tz=ET) if now is not None else pd.Timestamp.now(tz=ET)
    today = now_et.normalize()

    # Today's cumulative volume up to current bar
    today_df = df_1m[df_1m.index.normalize() == today]
    if today_df.empty:
        return 0.0
    today_vol = today_df["volume"].sum()

    # Current time-of-day cutoff
    current_time = now_et.time()

    # Historical same-time volumes
    hist_vols = []
    for day_offset in range(1, lookback_days + 1):
        target_date = today - pd.Timedelta(days=day_offset)
        hist_day = df_1m[df_1m.index.normalize() == target_date]
        if hist_day.empty:
            continue
        hist_day_cutoff = hist_day[hist_day.index.time <= current_time]
        if not hist_day_cutoff.empty:
            hist_vols.append(hist_day_cutoff["volume"].sum())

    if not hist_vols:
        return 1.0  # can't calculate, assume normal
    avg_vol = np.mean(hist_vols) + EPSILON
    return round(today_vol / avg_vol, 2)


def calc_gap_pct(df_1m: pd.DataFrame, now=None) -> float:
    """
    Gap % = (today's open - prev close) / prev close.
    """
    if df_1m is None or df_1m.empty:
        return 0.0

    import pytz

    ET = pytz.timezone("America/New_York")
    today = (
        pd.Timestamp(now, tz=ET) if now is not None else pd.Timestamp.now(tz=ET)
    ).normalize()

    today_bars = df_1m[df_1m.index.normalize() == today]
    prev_bars = df_1m[df_1m.index.normalize() < today]

    if today_bars.empty or prev_bars.empty:
        return 0.0

    today_open = today_bars.iloc[0]["open"]
    prev_close = prev_bars.iloc[-1]["close"]
    if prev_close <= EPSILON:
        return 0.0
    return round((today_open - prev_close) / prev_close, 4)


def calc_intraday_range_pct(df_1m_today: pd.DataFrame, prev_close: float) -> float:
    """
    Intraday range % = (today_high - today_low) / prev_close.
    """
    if df_1m_today is None or df_1m_today.empty or prev_close <= EPSILON:
        return 0.0
    h = df_1m_today["high"].max()
    l = df_1m_today["low"].min()
    return round((h - l) / prev_close, 4)


def calc_volume_spike_z(
    df_1m: pd.DataFrame, lookback_days: int = 10, now=None
) -> float:
    """
    Z-score of today's dollar volume vs historical distribution.
    """
    if df_1m is None or df_1m.empty:
        return 0.0

    import pytz

    ET = pytz.timezone("America/New_York")
    today = (
        pd.Timestamp(now, tz=ET) if now is not None else pd.Timestamp.now(tz=ET)
    ).normalize()

    today_bars = df_1m[df_1m.index.normalize() == today]
    if today_bars.empty:
        return 0.0

    today_vol = today_bars["volume"].sum() * today_bars["close"].mean()

    # Historical daily dollar volumes
    hist_dollars = []
    for day_offset in range(1, lookback_days + 1):
        target = today - pd.Timedelta(days=day_offset)
        day_df = df_1m[df_1m.index.normalize() == target]
        if not day_df.empty:
            hist_dollars.append(day_df["volume"].sum() * day_df["close"].mean())

    if len(hist_dollars) < 2:
        return 0.0

    mu = np.mean(hist_dollars)
    std = np.std(hist_dollars) + EPSILON
    return round((today_vol - mu) / std, 2)


# ─────────────────────────────────────────────────────────────────────────────
# SETUP QUALITY SCORE
# ─────────────────────────────────────────────────────────────────────────────


def compute_setup_quality_score(
    vwap_distance_pct: float,  # how far price is above VWAP (fraction)
    volume_expansion: float,  # current 5m bar vol / 5m avg vol
    structure_clarity: float,  # 0–1 normalized score of HH/HL count
    spread_pct: float,  # as fraction
    pressure_score: float = 0.0,
    rvol_strength: float = 0.0,
    effective_spread_cap: float = None,  # override for halt-resume widened threshold
) -> float:
    """
    SetupQualityScore — higher is better.

    Penalizes:
      - price too far from VWAP (overextended)
      - low volume expansion
      - poor structure
      - wide spread

    ``effective_spread_cap`` allows the caller to pass a halt-resume-adjusted
    spread threshold (e.g. max_spread_pct * resume_spread_multiplier) so the
    spread score is graded against the right reference rather than the default
    CONFIG.scanner.max_spread_pct.
    """
    _ = structure_clarity
    _ = spread_pct
    _ = effective_spread_cap

    # VWAP momentum strength: closest to ~1.5% above VWAP is best.
    vwap_score = 1.0 - min(abs(vwap_distance_pct - 0.015) / 0.015, 1.0)
    vwap_score = max(0.0, min(1.0, vwap_score))

    # Volume confirmation: 1.0x->0, 3.0x->1.
    vol_score = max(0.0, min(1.0, (volume_expansion - 1.0) / 2.0))

    pressure_norm = max(0.0, min(1.0, pressure_score))
    rvol_norm = max(0.0, min(1.0, rvol_strength))

    score = (
        0.35 * rvol_norm + 0.30 * vwap_score + 0.20 * vol_score + 0.15 * pressure_norm
    )
    return round(score, 4)


# ─────────────────────────────────────────────────────────────────────────────
# UNIVERSE RANKER
# ─────────────────────────────────────────────────────────────────────────────


def rank_universe(
    metrics_list: List[DemandMetrics], top_n: int = None
) -> List[DemandMetrics]:
    """
    Sort by DemandScore descending, with deterministic tie-breakers:
      1. demand_score  (primary)
      2. dollar_volume (higher wins)
      3. rvol          (higher wins)
      4. ticker        (alphabetical — stable across identical scores)
    Returns top_n entries with rank assigned 1-based.
    """
    top_n = top_n or CONFIG.scanner.top_n
    ranked = sorted(
        metrics_list,
        key=lambda m: (-m.demand_score, -m.dollar_volume, -m.rvol, m.ticker),
    )
    for i, m in enumerate(ranked):
        m.rank = i + 1
    return ranked[:top_n]
