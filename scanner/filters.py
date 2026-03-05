"""
filters.py — Hard filters for the dynamic universe scanner.

Each filter returns (passed: bool, rejection_reason: str).
All must pass for a ticker to enter DemandScore ranking.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Tuple

import pandas as pd
import pytz

from config.constants import (
    DEGRADE_IEX_PM_COVERAGE,
    EPSILON,
    REJECT_DOLLAR_VOLUME, REJECT_HALTED, REJECT_NO_CATALYST,
    REJECT_PRICE_RANGE, REJECT_RVOL, REJECT_SPREAD, REJECT_STALE_QUOTE,
    REJECT_UNSAFE_DATA,
)
from config.settings import CONFIG, ScannerConfig
from data.health import DH_BLOCK  # import constant directly to avoid circular dep

log = logging.getLogger(__name__)
ET  = pytz.timezone("America/New_York")


# ─────────────────────────────────────────────────────────────────────────────
# Individual filter functions
# ─────────────────────────────────────────────────────────────────────────────

def check_price(last: float, cfg: ScannerConfig = None) -> Tuple[bool, str]:
    cfg = cfg or CONFIG.scanner
    if cfg.min_price <= last <= cfg.max_price:
        return True, ""
    return False, f"{REJECT_PRICE_RANGE}(${last:.2f} not in ${cfg.min_price}-${cfg.max_price})"


def check_dollar_volume(dollar_vol: float, cfg: ScannerConfig = None) -> Tuple[bool, str]:
    cfg = cfg or CONFIG.scanner
    if dollar_vol >= cfg.min_dollar_volume:
        return True, ""
    return False, f"{REJECT_DOLLAR_VOLUME}(${dollar_vol/1e6:.1f}M < ${cfg.min_dollar_volume/1e6:.0f}M)"


def check_rvol(rvol: float, cfg: ScannerConfig = None, multiplier: float = 1.0) -> Tuple[bool, str]:
    """multiplier allows CHOP mode to raise threshold."""
    cfg = cfg or CONFIG.scanner
    threshold = cfg.min_rvol * multiplier
    if rvol >= threshold:
        return True, ""
    return False, f"{REJECT_RVOL}({rvol:.1f}x < {threshold:.1f}x required)"


def check_spread(
    bid: float,
    ask: float,
    cfg: ScannerConfig = None,
    multiplier: float = 1.0,
    session: str = None,
) -> Tuple[bool, str]:
    """
    Session-aware spread check.
    multiplier < 1 tightens spread in CHOP mode.
    session overrides threshold (PM and AH have wider allowed spreads).
    """
    cfg = cfg or CONFIG.scanner
    if bid <= EPSILON:
        return False, f"{REJECT_SPREAD}(zero bid)"
    spread_pct = (ask - bid) / ((ask + bid) / 2 + EPSILON)

    # Use extended-hours thresholds when in PM/AH session
    from config.constants import SESSION_PREMARKET, SESSION_AFTERHOURS
    eh_cfg = CONFIG.exthours
    if session == SESSION_PREMARKET:
        threshold = eh_cfg.max_pm_spread_pct
    elif session == SESSION_AFTERHOURS:
        threshold = eh_cfg.max_ah_spread_pct
    else:
        threshold = cfg.max_spread_pct * multiplier

    if spread_pct <= threshold:
        return True, ""
    return False, f"{REJECT_SPREAD}({spread_pct*100:.2f}% > {threshold*100:.2f}%)"


def check_stale_quote(quote_ts: datetime, cfg=None) -> Tuple[bool, str]:
    max_age = CONFIG.data.stale_quote_seconds
    if quote_ts is None:
        return False, REJECT_STALE_QUOTE
    age = (datetime.now(ET) - quote_ts).total_seconds()
    if age <= max_age:
        return True, ""
    return False, f"{REJECT_STALE_QUOTE}({age:.0f}s old)"


def check_halted(df_1m_today: pd.DataFrame) -> Tuple[bool, str]:
    """
    Detect halt: last 3+ consecutive 1m bars with identical OHLC or zero volume.
    """
    if df_1m_today is None or len(df_1m_today) < 3:
        return True, ""
    recent = df_1m_today.tail(3)
    if (recent["volume"] == 0).all():
        return False, REJECT_HALTED
    # All closes identical → likely halted
    if recent["close"].nunique() == 1 and recent["high"].nunique() == 1:
        return False, REJECT_HALTED
    return True, ""


def check_catalyst(
    gap_pct: float,
    intraday_range_pct: float,
    volume_spike_z: float,
    cfg: ScannerConfig = None,
) -> Tuple[bool, str]:
    """At least one expansion trigger must fire."""
    cfg = cfg or CONFIG.scanner
    has_gap    = abs(gap_pct)       >= cfg.min_gap_pct
    has_range  = intraday_range_pct >= cfg.min_intraday_range_pct
    has_volume = volume_spike_z     >= 1.5          # 1.5 sigma above average
    if has_gap or has_range or has_volume:
        return True, ""
    return (
        False,
        f"{REJECT_NO_CATALYST}(gap={gap_pct*100:.1f}%, "
        f"range={intraday_range_pct*100:.1f}%, z={volume_spike_z:.1f})",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Composite hard filter
# ─────────────────────────────────────────────────────────────────────────────

class HardFilter:
    """
    Runs all hard filters in order. Returns first failure reason or "".
    """

    def __init__(
        self,
        cfg: ScannerConfig = None,
        rvol_multiplier: float   = 1.0,
        spread_multiplier: float = 1.0,
    ):
        self._cfg         = cfg or CONFIG.scanner
        self._rvol_mult   = rvol_multiplier
        self._spread_mult = spread_multiplier

    def run(
        self,
        ticker:               str,
        last_price:           float,
        dollar_volume:        float,
        rvol:                 float,
        bid:                  float,
        ask:                  float,
        quote_ts:             datetime,
        gap_pct:              float,
        intraday_range_pct:   float,
        volume_spike_z:       float,
        df_1m_today:          pd.DataFrame = None,
        data_health_report=None,   # Optional[DataHealthReport]
        session:              str  = None,
    ) -> Tuple[bool, str]:
        """
        Returns (True, "") if all filters pass.
        Returns (False, reason) on first failure.

        data_health_report: if status==DH_BLOCK, fail immediately with REJECT_UNSAFE_DATA.
        """
        # ── DATA_HEALTH hard block (tier-1 safety gate, checked first) ────
        if data_health_report is not None:
            if data_health_report.status == DH_BLOCK:
                block_reason = str(getattr(data_health_report, "block_reason", "") or "")
                if DEGRADE_IEX_PM_COVERAGE in block_reason:
                    log.warning(
                        f"[{ticker}] DATA_HEALTH override: allowing IEX coverage-risk trade path: {block_reason}"
                    )
                else:
                    reason = f"{REJECT_UNSAFE_DATA}({block_reason})"
                    log.debug(f"[{ticker}] DATA_HEALTH BLOCK: {reason}")
                    return False, reason

        checks = [
            lambda: check_stale_quote(quote_ts),
            lambda: check_halted(df_1m_today),
            lambda: check_price(last_price, self._cfg),
            lambda: check_dollar_volume(dollar_volume, self._cfg),
            lambda: check_rvol(rvol, self._cfg, self._rvol_mult),
            lambda: check_spread(bid, ask, self._cfg, self._spread_mult, session=session),
            lambda: check_catalyst(gap_pct, intraday_range_pct, volume_spike_z, self._cfg),
        ]
        for check in checks:
            passed, reason = check()
            if not passed:
                log.debug(f"[{ticker}] HARD FILTER FAIL: {reason}")
                return False, reason
        return True, ""
