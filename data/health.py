"""
health.py — DATA_HEALTH validator.

Enforces a strict two-tier policy:

TIER 1 — HARD BLOCK (unsafe data, entry forbidden):
  • Stale quote (age > max_quote_age_hard_block_seconds)
  • 1m bar gap > max_bar_gap_minutes (feed outage)
  • Spread >= max_spread_hard_block_pct (locked/crossed/halted)
  • Bar count dropped vs previous cycle (data reset detected)
    • PM fake-volume detected (PM dollar vol < min / recent vol < min)

TIER 2 — DEGRADE (missing context, reduce size, continue):
  • 5m bars < min_bars_5m_context
  • 15m bars < min_bars_15m_context
  • 1h bars < min_bars_1h_context
    • IEX feed during PM/AH session (coverage risk)
    • Burst-profile on IEX (SIP recommended) — warning/degrade only

DataHealthReport is logged to scan CSV every cycle.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import pytz

from config.constants import (
    BLOCK_BAR_COUNT_DROP,
    BLOCK_BAR_GAP,
    BLOCK_HALT_DETECTED,
    BLOCK_PM_DOLLAR_VOLUME,
    BLOCK_PM_FAKE_VOLUME,
    BLOCK_PM_SPREAD,
    BLOCK_AH_SPREAD,
    BLOCK_CLOCK_DRIFT,
    BLOCK_SPREAD_LOCK,
    BLOCK_STALE_QUOTE,
    DEGRADE_IEX_PM_COVERAGE,
    DEGRADE_MISSING_15M,
    DEGRADE_MISSING_1H,
    DEGRADE_MISSING_5M,
    DH_BLOCK,
    DH_DEGRADE,
    DH_OK,
    EPSILON,
    SESSION_AFTERHOURS,
    SESSION_PREMARKET,
    SESSION_RTH,
    TF_15M,
    TF_1H,
    TF_1M,
    TF_5M,
)
from config.settings import CONFIG

log = logging.getLogger(__name__)
ET = pytz.timezone("America/New_York")


# ─────────────────────────────────────────────────────────────────────────────
# HEALTH REPORT
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class DataHealthReport:
    ticker: str
    status: str = DH_OK  # DH_OK | DH_DEGRADE | DH_BLOCK
    block_reason: str = ""  # populated if BLOCK
    degrade_reasons: List[str] = field(default_factory=list)  # populated if DEGRADE
    size_multiplier: float = 1.0  # 1.0 = full size; < 1.0 = degraded

    # Raw diagnostics (always populated for logging)
    quote_age_s: float = 0.0
    clock_drift_s: float = 0.0
    spread_pct: float = 0.0
    last_bar_age_s: float = 0.0
    bar_count_1m: int = 0
    bar_count_5m: int = 0
    bar_gap_minutes: float = 0.0
    feed_type: str = ""
    session: str = SESSION_RTH

    @property
    def is_tradeable(self) -> bool:
        return self.status != DH_BLOCK

    @property
    def can_trade_full_size(self) -> bool:
        return self.status == DH_OK


# ─────────────────────────────────────────────────────────────────────────────
# SESSION CLASSIFIER
# ─────────────────────────────────────────────────────────────────────────────


def current_session(now: datetime = None) -> str:
    """Classify current ET time into SESSION_* constant."""
    import pytz

    ET = pytz.timezone("America/New_York")
    now = now or datetime.now(ET)
    t = now.time()

    cfg = CONFIG.exthours
    pm_start = _t(cfg.pm_start)
    rth_open = _t(cfg.rth_open)
    ah_start = _t(cfg.ah_start)
    ah_end = _t(cfg.ah_end)

    if pm_start <= t < rth_open:
        return SESSION_PREMARKET
    if rth_open <= t < ah_start:
        return SESSION_RTH
    if ah_start <= t <= ah_end:
        return SESSION_AFTERHOURS
    return "OVERNIGHT"


# Public alias used by scanner/universe.py and tests
classify_session = current_session


def _t(s: str):
    """Parse 'HH:MM' to time."""
    from datetime import time as dtime

    h, m = s.split(":")
    return dtime(int(h), int(m))


# ─────────────────────────────────────────────────────────────────────────────
# MAIN VALIDATOR
# ─────────────────────────────────────────────────────────────────────────────


class DataHealthValidator:
    """
    Runs all health checks and returns a DataHealthReport.

    Usage:
        validator = DataHealthValidator()
        report = validator.check(ticker, mtf_bars, quote, prev_bar_count, feed_type)

        if not report.is_tradeable:
            log.warning(report.block_reason)
            continue
        size *= report.size_multiplier
    """

    def check(
        self,
        ticker: str,
        mtf_bars: Dict[str, pd.DataFrame],
        quote: dict,
        prev_bar_count_1m: int = 0,  # from previous cycle
        feed_type: str = FEED_ALPACA_IEX,
        now: datetime = None,
    ) -> DataHealthReport:
        now = now or datetime.now(ET)
        session = current_session(now)
        hcfg = CONFIG.health
        ehcfg = CONFIG.exthours

        report = DataHealthReport(
            ticker=ticker,
            feed_type=feed_type,
            session=session,
        )

        df_1m = mtf_bars.get(TF_1M) if mtf_bars else None
        df_5m = mtf_bars.get(TF_5M) if mtf_bars else None
        df_15m = mtf_bars.get(TF_15M) if mtf_bars else None
        df_1h = mtf_bars.get(TF_1H) if mtf_bars else None

        # ── Quote fields ────────────────────────────────────────────────────
        bid = quote.get("bid", 0.0)
        ask = quote.get("ask", 0.0)
        quote_ts = quote.get("timestamp", None)
        last = quote.get("last", 0.0)

        if quote_ts:
            if getattr(quote_ts, "tzinfo", None) is None:
                quote_ts = ET.localize(quote_ts)
            else:
                quote_ts = quote_ts.astimezone(ET)
            quote_age = (now - quote_ts).total_seconds()
            if quote_age < 0:
                report.clock_drift_s = round(abs(quote_age), 3)
            else:
                report.clock_drift_s = 0.0
        else:
            quote_age = 9999.0
            report.clock_drift_s = 0.0
        spread_pct = (
            ((ask - bid) / ((ask + bid) / 2 + EPSILON)) if bid > EPSILON else 1.0
        )

        report.quote_age_s = round(max(0.0, quote_age), 1)
        report.spread_pct = round(spread_pct, 5)

        # ── Last bar age ────────────────────────────────────────────────────
        last_bar_age = _last_bar_age(df_1m, now)
        report.last_bar_age_s = round(last_bar_age, 1)
        report.bar_count_1m = len(df_1m) if df_1m is not None else 0
        report.bar_count_5m = len(df_5m) if df_5m is not None else 0

        # ── Bar gap detection ───────────────────────────────────────────────
        bar_gap_min = _max_bar_gap_minutes(df_1m)
        report.bar_gap_minutes = round(bar_gap_min, 1)

        # TEST-ONLY override (explicit opt-in): bypass health enforcement.
        # Guarded to paper mode only to avoid accidental live misuse.
        if _is_data_health_test_override_enabled():
            log.warning(
                f"[{ticker}] DATA_HEALTH TEST OVERRIDE ACTIVE: bypassing TIER1/TIER2 enforcement."
            )
            report.status = DH_OK
            report.size_multiplier = 1.0
            return report

        # ════════════════════════════════════════════════════════════════════
        # TIER 1: HARD BLOCKS
        # ════════════════════════════════════════════════════════════════════

        # 0. Burst-profile safety on IEX:
        # Rule-override mode: degrade/warn instead of hard block.
        burst_iex_caution = (
            hcfg.block_burst_profile_on_iex
            and feed_type == FEED_ALPACA_IEX
            and bool(getattr(CONFIG.strategy, "is_burst_profile", False))
        )
        if burst_iex_caution:
            log.warning(
                f"[{ticker}] DATA_HEALTH CAUTION: "
                f"{DEGRADE_IEX_PM_COVERAGE}(BURST_PROFILE_REQUIRES_SIP)"
            )

        # 0b. Local clock drift vs feed timestamp (future quote) is unsafe.
        if quote_age < -hcfg.max_clock_drift_seconds:
            return self._block(
                report,
                f"{BLOCK_CLOCK_DRIFT}({abs(quote_age):.1f}s > {hcfg.max_clock_drift_seconds}s)",
            )

        # 1a. Stale quote
        if quote_age > hcfg.max_quote_age_hard_block_seconds:
            return self._block(
                report,
                f"{BLOCK_STALE_QUOTE}({quote_age:.0f}s > "
                f"{hcfg.max_quote_age_hard_block_seconds}s)",
            )

        # 1b. Spread lock (possible halt or crossed market)
        if spread_pct >= hcfg.max_spread_hard_block_pct:
            return self._block(
                report,
                f"{BLOCK_SPREAD_LOCK}({spread_pct*100:.2f}% >= "
                f"{hcfg.max_spread_hard_block_pct*100:.1f}%)",
            )

        # 1c. Bar gap (feed outage)
        if (
            bar_gap_min > hcfg.max_bar_gap_minutes
            and df_1m is not None
            and len(df_1m) > 0
        ):
            return self._block(
                report,
                f"{BLOCK_BAR_GAP}(gap={bar_gap_min:.1f}m > "
                f"{hcfg.max_bar_gap_minutes}m)",
            )

        # 1d. Bar count drop vs previous cycle (data reset)
        if (
            prev_bar_count_1m is not None
            and prev_bar_count_1m > 0
            and report.bar_count_1m > 0
            and prev_bar_count_1m - report.bar_count_1m >= hcfg.bar_count_drop_threshold
        ):
            return self._block(
                report,
                f"{BLOCK_BAR_COUNT_DROP}("
                f"prev={prev_bar_count_1m} cur={report.bar_count_1m})",
            )

        # 1e. Halt detection (zero-volume stall)
        if df_1m is not None and len(df_1m) >= 3:
            tail = df_1m.tail(3)
            if (tail["volume"] == 0).all() and tail["close"].nunique() == 1:
                return self._block(report, BLOCK_HALT_DETECTED)

        # 1f. PM/AH session-specific blocks
        iex_low_coverage_reason = ""
        if session == SESSION_PREMARKET:
            pm_block = self._pm_hard_checks(df_1m, bid, ask, spread_pct, now, ehcfg)
            if pm_block:
                if feed_type == FEED_ALPACA_IEX and (
                    pm_block.startswith(BLOCK_PM_DOLLAR_VOLUME)
                    or pm_block.startswith(BLOCK_PM_FAKE_VOLUME)
                ):
                    iex_low_coverage_reason = pm_block
                    log.warning(
                        f"[{ticker}] DATA_HEALTH IEX low-coverage downgrade: {pm_block}"
                    )
                else:
                    return self._block(report, pm_block)

        if session == SESSION_AFTERHOURS:
            if spread_pct > ehcfg.max_ah_spread_pct:
                return self._block(
                    report,
                    f"{BLOCK_AH_SPREAD}({spread_pct*100:.2f}% > "
                    f"{ehcfg.max_ah_spread_pct*100:.1f}%)",
                )

        # ════════════════════════════════════════════════════════════════════
        # TIER 2: CONTEXT DEGRADATION (size reduction, no block)
        # ════════════════════════════════════════════════════════════════════
        degrade_reasons = []
        size_mult = 1.0

        if df_5m is None or len(df_5m) < hcfg.min_bars_5m_context:
            degrade_reasons.append(DEGRADE_MISSING_5M)
            size_mult *= 0.75

        if df_15m is None or len(df_15m) < hcfg.min_bars_15m_context:
            degrade_reasons.append(DEGRADE_MISSING_15M)
            size_mult *= 0.85

        if df_1h is None or len(df_1h) < hcfg.min_bars_1h_context:
            degrade_reasons.append(DEGRADE_MISSING_1H)
            size_mult *= 0.90

        # IEX coverage risk during PM/AH
        if feed_type == FEED_ALPACA_IEX and session in (
            SESSION_PREMARKET,
            SESSION_AFTERHOURS,
        ):
            degrade_reasons.append(DEGRADE_IEX_PM_COVERAGE)
            size_mult *= 0.50
            log.warning(
                f"[{ticker}] IEX feed during {session} — coverage unreliable. "
                "Configure alpaca_feed='sip' for reliable PM/AH data."
            )

        if iex_low_coverage_reason:
            degrade_reasons.append(
                f"{DEGRADE_IEX_PM_COVERAGE}(LOW_COVERAGE:{iex_low_coverage_reason})"
            )
            size_mult *= 0.50

        # Burst strategy on IEX: allow trading but flag and reduce size.
        burst_iex_reason = f"{DEGRADE_IEX_PM_COVERAGE}(BURST_PROFILE_REQUIRES_SIP)"
        if burst_iex_caution and burst_iex_reason not in degrade_reasons:
            degrade_reasons.append(burst_iex_reason)
            size_mult *= 0.50

        if degrade_reasons:
            report.status = DH_DEGRADE
            report.degrade_reasons = degrade_reasons
            report.size_multiplier = round(max(size_mult, 0.10), 3)  # floor at 10%
            log.debug(
                f"[{ticker}] DATA_HEALTH DEGRADE: {degrade_reasons} "
                f"size_mult={report.size_multiplier:.2f}"
            )
        else:
            report.status = DH_OK

        return report

    # ── PM hard checks ───────────────────────────────────────────────────────

    def _pm_hard_checks(self, df_1m, bid, ask, spread_pct, now, ehcfg) -> str:
        """Return block reason string or empty string if all pass."""
        # PM spread cap (stricter than RTH)
        if spread_pct > ehcfg.max_pm_spread_pct:
            return (
                f"{BLOCK_PM_SPREAD}({spread_pct*100:.2f}% > "
                f"{ehcfg.max_pm_spread_pct*100:.1f}%)"
            )

        if df_1m is None or df_1m.empty:
            return BLOCK_PM_DOLLAR_VOLUME  # no data = can't verify

        ET = pytz.timezone("America/New_York")
        today = pd.Timestamp(now).normalize()
        df_today = df_1m[df_1m.index.normalize() == today]
        if df_today.empty:
            return ""

        # PM dollar volume
        pm_dvol = (df_today["close"] * df_today["volume"]).sum()
        if pm_dvol < ehcfg.min_pm_dollar_volume:
            return (
                f"{BLOCK_PM_DOLLAR_VOLUME}(${pm_dvol/1e6:.2f}M < "
                f"${ehcfg.min_pm_dollar_volume/1e6:.1f}M)"
            )

        # Recent activity: last N minutes
        window_start = now - pd.Timedelta(
            minutes=ehcfg.min_recent_activity_window_minutes
        )
        recent = df_today[df_today.index >= window_start]
        if recent.empty or recent["volume"].sum() < ehcfg.min_recent_activity_volume:
            recent_vol = recent["volume"].sum() if not recent.empty else 0
            return (
                f"{BLOCK_PM_FAKE_VOLUME}(recent_vol={recent_vol:,} < "
                f"{ehcfg.min_recent_activity_volume:,} in last "
                f"{ehcfg.min_recent_activity_window_minutes}m)"
            )

        return ""

    # ── Helpers ──────────────────────────────────────────────────────────────

    @staticmethod
    def _block(report: DataHealthReport, reason: str) -> DataHealthReport:
        report.status = DH_BLOCK
        report.block_reason = reason
        report.size_multiplier = 0.0
        log.debug(f"[{report.ticker}] DATA_HEALTH BLOCK: {reason}")
        return report


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _last_bar_age(df_1m: pd.DataFrame, now: datetime) -> float:
    """Seconds since the most recent 1m bar timestamp."""
    if df_1m is None or df_1m.empty:
        return 9999.0
    last_ts = df_1m.index[-1]
    if last_ts.tzinfo is None:
        last_ts = last_ts.tz_localize(ET)
    return (now - last_ts).total_seconds()


def _max_bar_gap_minutes(df_1m: pd.DataFrame) -> float:
    """
    Find the largest gap between consecutive 1m bar timestamps in minutes.
    Ignores overnight gaps (> 60 min) as those are expected.
    """
    if df_1m is None or len(df_1m) < 2:
        return 0.0
    diffs = df_1m.index.to_series().diff().dropna().dt.total_seconds() / 60
    # Ignore overnight/weekend gaps (> 60 min)
    intraday_gaps = diffs[diffs <= 60]
    if intraday_gaps.empty:
        return 0.0
    return float(intraday_gaps.max())


def _is_data_health_test_override_enabled() -> bool:
    """
    Enable with env var TEST_PROFILE=1 (or true/yes/on).
    Legacy alias: DATA_HEALTH_TEST_OVERRIDE.
    For safety, only honored in paper mode.
    """
    test_profile = str(os.environ.get("TEST_PROFILE", "")).strip().lower()
    legacy_flag = str(os.environ.get("DATA_HEALTH_TEST_OVERRIDE", "")).strip().lower()
    requested = test_profile in {"1", "true", "yes", "on"} or legacy_flag in {
        "1",
        "true",
        "yes",
        "on",
    }
    paper_only = bool(getattr(CONFIG.execution, "paper_mode", True))
    return requested and paper_only


# Module-level singleton
data_health_validator = DataHealthValidator()
