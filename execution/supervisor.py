"""
execution/supervisor.py — Market-State Supervisor.

Pure-function design: evaluate_market_state() takes a CycleMetrics snapshot and
returns a SupervisorOutput.  No global state, no I/O.

States (in escalation order):
    NORMAL        → full size, normal spread cap, standard SQS minimum
    CAUTION       → 70% size, tighter spread, standard SQS
    DEFENSIVE     → 50% size, very tight spread, raised SQS minimum
    HALT_ENTRIES  → no new entries; exits continue normally

Thresholds are drawn from CONFIG.supervisor.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum

from config.settings import CONFIG

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# State enum
# ─────────────────────────────────────────────────────────────────────────────


class MarketState(str, Enum):
    NORMAL = "NORMAL"
    CAUTION = "CAUTION"
    DEFENSIVE = "DEFENSIVE"
    HALT_ENTRIES = "HALT_ENTRIES"


# ─────────────────────────────────────────────────────────────────────────────
# Input / Output types
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class CycleMetrics:
    """
    Aggregated metrics for one engine cycle, passed to evaluate_market_state().

    All values should be computed BEFORE the candidate entry decision is made,
    so the supervisor can react to the current session environment.
    """

    # Data quality
    bar_latency_seconds: float = 0.0  # seconds since last bar arrived
    missing_bar_pct: float = 0.0  # fraction of expected 1m bars missing today

    # Quote quality (rolling window across all scanned tickers)
    median_spread_pct: float = 0.0  # median bid/ask spread
    p90_spread_pct: float = 0.0  # 90th-percentile spread

    # Execution quality
    rejection_rate_10c: float = 0.0  # order rejections / submissions (last 10 cycles)
    slippage_incidents: int = 0  # # of tickers with SLIPPAGE_BLOCK/SIZE_REDUCE

    # P&L trajectory
    rolling_pnl_slope: float = (
        0.0  # $/cycle linear slope over recent trades (negative = bad)
    )
    drawdown_velocity: float = (
        0.0  # rate of change of drawdown (negative = deepening fast)
    )


@dataclass(frozen=True)
class SupervisorOutput:
    """
    Recommended operating parameters returned by evaluate_market_state().
    The engine applies these each cycle.
    """

    state: MarketState = MarketState.NORMAL
    size_mult: float = 1.0  # multiply all position sizes
    spread_mult: float = 1.0  # multiply max_spread_pct cap
    min_sqs: float = 0.0  # override minimum Setup Quality Score (0 = don't override)
    trigger: str = ""  # human-readable reason for the current state


# ─────────────────────────────────────────────────────────────────────────────
# Core evaluation (pure function)
# ─────────────────────────────────────────────────────────────────────────────


def evaluate_market_state(metrics: CycleMetrics) -> SupervisorOutput:
    """
    Evaluate operating conditions and return a SupervisorOutput.

    Evaluation order: HALT_ENTRIES > DEFENSIVE > CAUTION > NORMAL
    The highest-severity condition that matches wins.
    """
    cfg = CONFIG.supervisor

    # ── HALT_ENTRIES ──────────────────────────────────────────────────────────
    if metrics.rejection_rate_10c >= cfg.halt_rejection_rate:
        trigger = (
            f"rejection_rate={metrics.rejection_rate_10c:.0%} ≥ "
            f"halt_threshold={cfg.halt_rejection_rate:.0%}"
        )
        log.warning("[Supervisor] HALT_ENTRIES: %s", trigger)
        return SupervisorOutput(
            state=MarketState.HALT_ENTRIES,
            size_mult=0.0,
            spread_mult=0.0,
            min_sqs=1.0,
            trigger=trigger,
        )

    if metrics.drawdown_velocity <= cfg.halt_drawdown_velocity:
        trigger = (
            f"drawdown_velocity={metrics.drawdown_velocity:.4f} ≤ "
            f"halt_threshold={cfg.halt_drawdown_velocity:.4f}"
        )
        log.warning("[Supervisor] HALT_ENTRIES: %s", trigger)
        return SupervisorOutput(
            state=MarketState.HALT_ENTRIES,
            size_mult=0.0,
            spread_mult=0.0,
            min_sqs=1.0,
            trigger=trigger,
        )

    # ── DEFENSIVE ─────────────────────────────────────────────────────────────
    if metrics.p90_spread_pct >= cfg.defensive_p90_spread:
        trigger = (
            f"p90_spread={metrics.p90_spread_pct:.3%} ≥ "
            f"defensive_threshold={cfg.defensive_p90_spread:.3%}"
        )
        log.info("[Supervisor] DEFENSIVE: %s", trigger)
        return SupervisorOutput(
            state=MarketState.DEFENSIVE,
            size_mult=cfg.defensive_size_mult,
            spread_mult=0.7,
            min_sqs=cfg.defensive_min_sqs,
            trigger=trigger,
        )

    if metrics.missing_bar_pct >= cfg.defensive_missing_bar_pct:
        trigger = (
            f"missing_bar_pct={metrics.missing_bar_pct:.0%} ≥ "
            f"defensive_threshold={cfg.defensive_missing_bar_pct:.0%}"
        )
        log.info("[Supervisor] DEFENSIVE: %s", trigger)
        return SupervisorOutput(
            state=MarketState.DEFENSIVE,
            size_mult=cfg.defensive_size_mult,
            spread_mult=0.7,
            min_sqs=cfg.defensive_min_sqs,
            trigger=trigger,
        )

    if metrics.rolling_pnl_slope <= cfg.defensive_pnl_slope:
        trigger = (
            f"pnl_slope={metrics.rolling_pnl_slope:.3f} ≤ "
            f"defensive_threshold={cfg.defensive_pnl_slope:.3f}"
        )
        log.info("[Supervisor] DEFENSIVE: %s", trigger)
        return SupervisorOutput(
            state=MarketState.DEFENSIVE,
            size_mult=cfg.defensive_size_mult,
            spread_mult=0.7,
            min_sqs=cfg.defensive_min_sqs,
            trigger=trigger,
        )

    # ── CAUTION ───────────────────────────────────────────────────────────────
    if metrics.median_spread_pct >= cfg.caution_median_spread:
        trigger = (
            f"median_spread={metrics.median_spread_pct:.3%} ≥ "
            f"caution_threshold={cfg.caution_median_spread:.3%}"
        )
        log.info("[Supervisor] CAUTION: %s", trigger)
        return SupervisorOutput(
            state=MarketState.CAUTION,
            size_mult=cfg.caution_size_mult,
            spread_mult=cfg.caution_spread_mult,
            trigger=trigger,
        )

    if metrics.bar_latency_seconds >= cfg.caution_bar_latency:
        trigger = (
            f"bar_latency={metrics.bar_latency_seconds:.0f}s ≥ "
            f"caution_threshold={cfg.caution_bar_latency:.0f}s"
        )
        log.info("[Supervisor] CAUTION: %s", trigger)
        return SupervisorOutput(
            state=MarketState.CAUTION,
            size_mult=cfg.caution_size_mult,
            spread_mult=cfg.caution_spread_mult,
            trigger=trigger,
        )

    if metrics.slippage_incidents >= cfg.caution_slip_incidents:
        trigger = (
            f"slippage_incidents={metrics.slippage_incidents} ≥ "
            f"caution_threshold={cfg.caution_slip_incidents}"
        )
        log.info("[Supervisor] CAUTION: %s", trigger)
        return SupervisorOutput(
            state=MarketState.CAUTION,
            size_mult=cfg.caution_size_mult,
            spread_mult=cfg.caution_spread_mult,
            trigger=trigger,
        )

    # ── NORMAL ────────────────────────────────────────────────────────────────
    return SupervisorOutput(
        state=MarketState.NORMAL,
        size_mult=1.0,
        spread_mult=1.0,
        trigger="all metrics within normal thresholds",
    )
