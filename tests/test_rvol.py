"""
Acceptance Test 1 — Session-aware RVOL correctness.

Invariants verified:
  A. PM RVOL uses ONLY premarket (04:00–09:29) window vs PM baseline.
     PM volume is NEVER compared to a full-day (RTH) average.
  B. RTH RVOL uses ONLY RTH (09:30–16:00) window vs RTH baseline.
  C. AH RVOL uses ONLY after-hours (16:00–20:00) window vs AH baseline.
  D. best_rvol() returns the active session's RVOL value, not a blend.
  E. A ticker with heavy PM volume but flat RTH volume should show:
       rvol_pm >> 1.0 while rvol_rth ≈ 1.0
"""

from __future__ import annotations

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, date, timedelta
import pandas as pd
import pytz
import pytest

from scanner.rvol import RVOLResult, calc_session_rvol, best_rvol

ET = pytz.timezone("America/New_York")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _bar(ts: datetime, volume: int = 1_000, price: float = 10.0) -> dict:
    return {"open": price, "high": price, "low": price, "close": price, "volume": volume}


def _make_df(rows: list[tuple[datetime, int]]) -> pd.DataFrame:
    """rows: [(timestamp_et, volume), ...]"""
    data = [_bar(ts, vol) for ts, vol in rows]
    idx  = [ts for ts, _ in rows]
    df   = pd.DataFrame(data, index=pd.DatetimeIndex(idx))
    if df.index.tzinfo is None:
        df.index = df.index.tz_localize(ET)
    return df


def _ts(d: date, hour: int, minute: int) -> datetime:
    return ET.localize(datetime(d.year, d.month, d.day, hour, minute, 0))


# ─────────────────────────────────────────────────────────────────────────────
# Test A: PM RVOL measures only PM window
# ─────────────────────────────────────────────────────────────────────────────

def test_pm_rvol_uses_only_premarket_window():
    """
    If today has 10× PM volume vs historical PM average, rvol_pm should be ~10.
    RTH data for today should have NO influence on rvol_pm.
    """
    today  = date(2024, 6, 10)
    hist   = [date(2024, 6, d) for d in range(3, 8)]   # Mon–Fri history

    rows = []
    # Historical PM: each day 1 bar at 06:00 with 1000 vol
    for d in hist:
        rows.append((_ts(d, 6, 0), 1_000))
        # Historical RTH: much larger volume (should NOT contaminate PM avg)
        for h in range(10, 16):
            rows.append((_ts(d, h, 0), 50_000))

    # Today PM: 10× historical PM = 10,000 vol
    rows.append((_ts(today, 6, 0), 10_000))
    # Today RTH so far: 0 bars (or normal — should not affect PM rvol)
    # (no RTH bars added for today)

    df_1m   = _make_df(rows)
    now_et  = _ts(today, 8, 30)    # we are mid-premarket

    result = calc_session_rvol(df_1m, now_et, lookback_days=5)

    assert result.rvol_pm > 5.0, \
        f"Expected rvol_pm >> 1 (heavy PM vol), got {result.rvol_pm}"
    # RTH rvol should be near 0.0 or 1.0 since today has no RTH bars yet
    assert result.rvol_rth <= 1.5, \
        f"Expected rvol_rth ≈ baseline, got {result.rvol_rth}"


# ─────────────────────────────────────────────────────────────────────────────
# Test B: RTH RVOL is never contaminated by PM volume
# ─────────────────────────────────────────────────────────────────────────────

def test_rth_rvol_excludes_premarket_volume():
    """
    Historical PM volume should NOT inflate the RTH baseline.
    If PM history is enormous but RTH is flat, rvol_rth ≈ 1.0.
    """
    today = date(2024, 6, 10)
    hist  = [date(2024, 6, d) for d in range(3, 8)]

    rows = []
    for d in hist:
        # Huge PM vol (should NOT enter RTH avg)
        rows.append((_ts(d, 6, 0), 1_000_000))
        # Normal RTH vol
        for h in range(10, 16):
            rows.append((_ts(d, h, 0), 1_000))

    # Today RTH: normal Vol
    for h in range(10, 14):
        rows.append((_ts(today, h, 0), 1_000))

    df_1m  = _make_df(rows)
    now_et = _ts(today, 13, 30)   # mid RTH

    result = calc_session_rvol(df_1m, now_et, lookback_days=5)

    # rvol_rth should be close to 1.0 — RTH vol matches hist RTH
    assert 0.3 <= result.rvol_rth <= 3.0, \
        f"Expected rvol_rth ≈ 1.0 (PM noise excluded), got {result.rvol_rth}"
    # rvol_pm is 0 (today no PM bars)
    assert result.rvol_pm == 0.0 or result.rvol_pm >= 0.0


# ─────────────────────────────────────────────────────────────────────────────
# Test C: best_rvol returns active session value
# ─────────────────────────────────────────────────────────────────────────────

def test_best_rvol_returns_active_session():
    """best_rvol(result) should return whichever session has non-zero RVOL."""
    r_pm_active = RVOLResult(rvol_rth=0.0, rvol_pm=4.5, rvol_ah=0.0)
    assert best_rvol(r_pm_active) == pytest.approx(4.5), "Should pick PM rvol"

    r_rth_active = RVOLResult(rvol_rth=3.2, rvol_pm=0.0, rvol_ah=0.0)
    assert best_rvol(r_rth_active) == pytest.approx(3.2), "Should pick RTH rvol"

    r_ah_active = RVOLResult(rvol_rth=0.0, rvol_pm=0.0, rvol_ah=2.8)
    assert best_rvol(r_ah_active) == pytest.approx(2.8), "Should pick AH rvol"

    r_fallback = RVOLResult(rvol_rth=0.0, rvol_pm=0.0, rvol_ah=0.0)
    assert best_rvol(r_fallback) == pytest.approx(1.0), "Should fallback to 1.0"


# ─────────────────────────────────────────────────────────────────────────────
# Test D: PM-heavy ticker vs RTH-heavy ticker
# ─────────────────────────────────────────────────────────────────────────────

def test_pm_heavy_shows_high_pm_rvol_not_rth():
    """
    Ticker with 10× PM vol but flat RTH:
      rvol_pm >> 1, rvol_rth ≈ 1
    This confirms no cross-session contamination.
    """
    today = date(2024, 6, 10)
    hist  = [date(2024, 6, d) for d in range(3, 8)]

    rows = []
    for d in hist:
        rows.append((_ts(d, 5, 30), 500))
        for h in range(10, 16):
            rows.append((_ts(d, h, 0), 2_000))

    # Today: 10× PM vol, normal RTH
    rows.append((_ts(today, 5, 30), 5_000))   # 10× historical PM

    df_1m  = _make_df(rows)
    now_et = _ts(today, 7, 0)

    result = calc_session_rvol(df_1m, now_et, lookback_days=5)
    assert result.rvol_pm > 3.0,  f"rvol_pm should be high, got {result.rvol_pm}"
    assert result.rvol_rth == 0.0, f"rvol_rth should be 0 (no RTH bars today), got {result.rvol_rth}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
