"""
rvol.py — Session-aware Relative Volume calculation.

Three distinct RVOL variants — never mix session periods:

  RVOL_RTH(t) = RTH_vol_today(09:30→t) / avg_RTH_vol_lastN(09:30→t)
  RVOL_PM(t)  = PM_vol_today(04:00→t)  / avg_PM_vol_lastN(04:00→t)
  RVOL_AH(t)  = AH_vol_today(16:00→t)  / avg_AH_vol_lastN(16:00→t)

Each variant anchors to its own session start so the comparison is
apples-to-apples at the same time-of-day within that session.

Key principle: do NOT compare PM volume to full-day averages.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, time
from typing import Optional

import numpy as np
import pandas as pd
import pytz

from config.constants import EPSILON, RVOL_AH, RVOL_PM, RVOL_RTH
from config.settings import CONFIG

log = logging.getLogger(__name__)
ET  = pytz.timezone("America/New_York")


# ─────────────────────────────────────────────────────────────────────────────
# SESSION WINDOWS (parsed from config at import time)
# ─────────────────────────────────────────────────────────────────────────────

def _t(s: str) -> time:
    h, m = s.split(":")
    return time(int(h), int(m))


@dataclass
class RVOLResult:
    rvol_rth: float = 0.0    # primary (RTH, time-of-day matched)
    rvol_pm:  float = 0.0    # premarket RVOL (0 outside PM session)
    rvol_ah:  float = 0.0    # after-hours RVOL (0 outside AH session)

    @property
    def active(self) -> float:
        """Return whichever RVOL variant is non-zero for the current session."""
        if self.rvol_pm > 0:
            return self.rvol_pm
        if self.rvol_ah > 0:
            return self.rvol_ah
        return self.rvol_rth

    def log_str(self) -> str:
        parts = [f"RTH={self.rvol_rth:.2f}x"]
        if self.rvol_pm  > 0: parts.append(f"PM={self.rvol_pm:.2f}x")
        if self.rvol_ah  > 0: parts.append(f"AH={self.rvol_ah:.2f}x")
        return " | ".join(parts)


# ─────────────────────────────────────────────────────────────────────────────
# CORE SESSION RVOL CALCULATOR
# ─────────────────────────────────────────────────────────────────────────────

def _session_rvol(
    df_1m:      pd.DataFrame,
    session_start: time,
    session_end:   time,
    today:      pd.Timestamp,
    lookback_days: int,
    now_et:     datetime,
) -> float:
    """
    Generic session-aware RVOL.

    Computes today's cumulative volume from session_start up to now,
    then compares to the same time-of-day window across lookback_days.
    """
    if df_1m is None or df_1m.empty:
        return 0.0

    current_time = now_et.time()

    # ── Today's session volume up to current bar ─────────────────────────
    today_session = df_1m[
        (df_1m.index.normalize() == today)
        & (df_1m.index.time >= session_start)
        & (df_1m.index.time <= min(current_time, session_end))
    ]
    if today_session.empty:
        return 0.0
    today_vol = today_session["volume"].sum()

    # ── Historical same-period volumes ───────────────────────────────────
    hist_vols = []
    for day_offset in range(1, lookback_days + 1):
        target = today - pd.Timedelta(days=day_offset)
        hist_day = df_1m[
            (df_1m.index.normalize() == target)
            & (df_1m.index.time >= session_start)
            & (df_1m.index.time <= min(current_time, session_end))
        ]
        if not hist_day.empty:
            hist_vols.append(hist_day["volume"].sum())

    if not hist_vols:
        log.debug(f"  RVOL: no historical bars for session "
                  f"{session_start}–{session_end}")
        return 1.0  # can't compute — treat as normal

    avg_vol = np.mean(hist_vols) + EPSILON
    rvol    = today_vol / avg_vol
    return round(float(rvol), 2)


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC API
# ─────────────────────────────────────────────────────────────────────────────

def calc_session_rvol(
    df_1m:         pd.DataFrame,
    now:           datetime = None,
    lookback_days: int      = None,
) -> RVOLResult:
    """
    Compute all three session RVOL variants at once.
    Only populates the variants relevant to the current session.

    Args:
        df_1m:         Full 1m bar history (multi-day, extended hours included)
        now:           Current ET datetime (defaults to now)
        lookback_days: Override CONFIG.exthours.rvol_lookback_days
    """
    now  = now or datetime.now(ET)
    days = lookback_days or CONFIG.exthours.rvol_lookback_days
    cfg  = CONFIG.exthours

    pm_start  = _t(cfg.pm_start)
    pm_end    = _t(cfg.pm_end)
    rth_open  = _t(cfg.rth_open)
    rth_close = _t(cfg.rth_close)
    ah_start  = _t(cfg.ah_start)
    ah_end    = _t(cfg.ah_end)

    today     = pd.Timestamp(now).normalize()

    result = RVOLResult()

    current_t = now.time()

    # RTH RVOL — always computed (used in demand score)
    # Only meaningful from 09:30 onwards; before that, returns 0
    if current_t >= rth_open:
        result.rvol_rth = _session_rvol(
            df_1m, rth_open, rth_close, today, days, now
        )

    # PM RVOL — only during premarket session
    if pm_start <= current_t < rth_open:
        result.rvol_pm = _session_rvol(
            df_1m, pm_start, pm_end, today, days, now
        )

    # AH RVOL — only during after-hours
    if ah_start <= current_t <= ah_end:
        result.rvol_ah = _session_rvol(
            df_1m, ah_start, ah_end, today, days, now
        )

    return result


def best_rvol(result: RVOLResult) -> float:
    """
    Return the single most relevant RVOL value for the current moment.
    Used in DemandScore when only one value is needed.
    Falls back to 1.0 (neutral) when no session data is available.
    """
    v = result.active
    return v if v > 0.0 else 1.0
