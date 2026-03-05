"""
utils/spread_policy.py — Single source of truth for effective spread caps.

Problem this solves
-------------------
Multiple modules previously hard-referenced their own spread thresholds
(ScannerConfig.max_spread_pct, DecisionConfig.max_spread_pct_entry,
ExtendedHoursConfig.max_pm_spread_pct, DataHealthConfig.max_spread_hard_block_pct,
RegimeConfig.chop_spread_multiplier, DecisionConfig.chop_spread_multiplier).

This led to divergent gating: different code paths silently applied different
thresholds for the same situation.

Solution
--------
Call `effective_spread_cap(session, regime, phase)` everywhere a spread
threshold is needed.  The function:
  1. Picks the base threshold for the session (PM / RTH / AH).
  2. Applies the phase modifier (scan vs entry — entry is tighter).
  3. Applies the regime multiplier when in CHOP mode within RTH.

The hard-block threshold (DataHealthConfig.max_spread_hard_block_pct) is
intentionally NOT routed through this function — it is a data-integrity wall,
not a trading-decision threshold, and should always be evaluated independently.

Usage
-----
    from utils.spread_policy import effective_spread_cap
    from config.constants import SESSION_RTH, REGIME_CHOP

    cap = effective_spread_cap("RTH", regime, phase="entry")
    if spread_pct > cap:
        ...  # reject
"""

from __future__ import annotations

from config.constants import SESSION_PREMARKET, SESSION_AFTERHOURS, REGIME_CHOP
from config.settings import CONFIG


def effective_spread_cap(
    session: str = "RTH",
    regime:  str = "TREND",
    phase:   str = "entry",
) -> float:
    """
    Return the maximum allowed spread (as a fraction, e.g. 0.005 = 0.5%)
    for the given session / regime / phase combination.

    Parameters
    ----------
    session : str
        One of SESSION_PREMARKET ("PREMARKET"), SESSION_RTH ("RTH"),
        SESSION_AFTERHOURS ("AFTERHOURS").  Case-insensitive.
    regime : str
        "TREND" or "CHOP".  Chop mode tightens the RTH cap.
        Has no effect on PM/AH caps (no regime detection outside RTH).
    phase : str
        "scan"  — universe scanner threshold (looser; used in filters.py).
        "entry" — pre-order decision gate (tighter; used in decision/engine.py).

    Returns
    -------
    float
        The effective spread cap.  Multiply by 100 for percent.

    Examples
    --------
    >>> effective_spread_cap("RTH",       "TREND", "scan")   # 0.005
    >>> effective_spread_cap("RTH",       "TREND", "entry")  # 0.005
    >>> effective_spread_cap("RTH",       "CHOP",  "entry")  # 0.005 * 0.80 = 0.004
    >>> effective_spread_cap("PREMARKET", "TREND", "entry")  # 0.010
    >>> effective_spread_cap("AFTERHOURS","TREND", "entry")  # 0.015
    """
    session_upper = session.upper()

    # ── Base cap by session ───────────────────────────────────────────────
    if session_upper == SESSION_PREMARKET:
        return CONFIG.exthours.max_pm_spread_pct          # PM: 1% (no regime mult)

    if session_upper == SESSION_AFTERHOURS:
        return CONFIG.exthours.max_ah_spread_pct          # AH: 1.5% (no regime mult)

    # RTH — phase-specific base
    if phase == "entry":
        base = CONFIG.decision.max_spread_pct_entry       # 0.5%
    else:  # "scan"
        base = CONFIG.scanner.max_spread_pct              # 0.5%

    # ── Regime tightening (RTH only) ─────────────────────────────────────
    if regime == REGIME_CHOP:
        if phase == "entry":
            base *= CONFIG.decision.chop_spread_multiplier    # 0.80
        else:
            base *= CONFIG.regime.chop_spread_multiplier      # 0.70

    return base


def rth_spread_cap_for_phase(phase: str = "entry", regime: str = "TREND") -> float:
    """Convenience shortcut for RTH-only callers."""
    return effective_spread_cap("RTH", regime=regime, phase=phase)
