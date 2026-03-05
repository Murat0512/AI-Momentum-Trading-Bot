"""
decision/engine.py — Deterministic DecisionEngine.

Responsibilities:
  Layer B of the two-layer architecture.

  1. Receive Top-15 snapshot from UniverseManager.
  2. For each candidate, evaluate SetupQuality (VWAP/PMH/HOD/structure/pullback).
  3. Gate each candidate through:
       a) Microstructure gate (spread, quote age)
       b) DATA_HEALTH gate  (BLOCK = reject; DEGRADE = permitted, shrink size)
       c) Slippage gate     (BLOCK = reject; SIZE_REDUCE = shrink size)
       d) Risk gates        (max_trades, cooldown, daily_loss, ticker_cap)
       e) MinSQS gate       (SQS < min → reject, regardless of regime)
  4. Apply regime shaping to thresholds (CHOP → tighter gates).  Discovery
     is NEVER affected.  Only the decision-layer gates are tightened.
  5. Feed all valid candidates to DeterministicSelector.
  6. Return DecisionResult with full audit trail.

Non-negotiable:
  - Zero randomness — same inputs → same output.
  - Every rejection is logged with the gate that failed.
  - All multipliers (health, slippage, regime) compose multiplicatively.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
import pytz

from config.constants import (
    DH_BLOCK,
    EPSILON,
    GATE_DH_BLOCK,
    GATE_LOW_SQS,
    GATE_NOT_TOP15,
    GATE_NO_VALID_SETUP,
    GATE_PASS,
    GATE_QUOTE_STALE,
    GATE_RISK_COOLDOWN,
    GATE_RISK_DAILY_LOSS,
    GATE_RISK_MAX_TRADES,
    GATE_RISK_TICKER_CAP,
    GATE_SLIPPAGE_BLOCK,
    GATE_SPREAD_WIDE,
    REGIME_CHOP,
    SQ_PULLBACK,
    SQ_PRESSURE,
    SQ_RVOL_STRENGTH,
    SQ_STRUCTURE,
    SQ_VWAP_RECLAIM,
    SQ_VOLUME_EXP,
    TF_1M,
    TF_5M,
)
from config.settings import CONFIG
from data.health import DataHealthReport
from execution.slippage import SlippageMonitor, slippage_monitor
from risk.manager import RiskManager
from scanner.universe_manager import CandidateEntry
from selection.selector import DeterministicSelector, SelectionResult, TradeCandidate
from signals.setup import MomentumSetupV1, SetupResult
from signals.setup import calculate_pressure_score
from signals.structure import (
    calc_atr,
    has_valid_structure,
    premarket_high,
    intraday_high,
    structure_score,
)
from signals.vwap import current_vwap, vwap_distance_pct

log = logging.getLogger(__name__)
ET = pytz.timezone("America/New_York")


# ─────────────────────────────────────────────────────────────────────────────
# PULLBACK INTEGRITY
# ─────────────────────────────────────────────────────────────────────────────


def _pullback_integrity_score(df_1m: pd.DataFrame, lookback: int = 10) -> float:
    """
    Score the quality of a pullback on the 1m chart (0–1).

    A clean pullback shows:
      - Shallow retracement (< 61.8% of preceding impulse)
      - Volume contraction on down bars (no panic selling)
      - Price holding above a structural reference (VWAP / prior bar low)

    Returns 1.0 for an ideal tight pullback, 0.0 for random chop.
    """
    if df_1m is None or len(df_1m) < lookback + 2:
        return 0.5  # neutral / unknown

    tail = df_1m.tail(lookback + 2)
    closes = tail["close"]
    volumes = tail["volume"]
    highs = tail["high"]
    lows = tail["low"]

    # ── Component 1: Retracement depth ──────────────────────────────────
    # Find recent impulse: highest high vs most recent low
    recent_high = float(highs.iloc[-lookback:].max())
    recent_low = float(lows.iloc[-lookback:].min())
    impulse = recent_high - recent_low
    if impulse <= EPSILON:
        return 0.5

    # Current price vs high
    current = float(closes.iloc[-1])
    retrace_pct = (recent_high - current) / impulse  # 0 = at high, 1 = at low
    # Good pullback: retrace 0.1 – 0.5; bad: > 0.618 (deep) or < 0 (still running)
    if retrace_pct < 0:
        depth_score = 0.8  # still pushing higher — slightly less clean
    elif retrace_pct <= 0.382:
        depth_score = 1.0  # best zone: shallow pullback
    elif retrace_pct <= 0.618:
        depth_score = 0.6  # acceptable
    else:
        depth_score = 0.1  # deep — likely distribution, not accumulation

    # ── Component 2: Volume contraction on down bars ─────────────────────
    down_bars = tail[closes.diff() < 0]
    up_bars = tail[closes.diff() > 0]

    avg_down_vol = float(down_bars["volume"].mean()) if not down_bars.empty else 1.0
    avg_up_vol = float(up_bars["volume"].mean()) if not up_bars.empty else 1.0

    # Good: volume contracts on pulls, expands on pushes
    vol_ratio = avg_up_vol / (avg_down_vol + EPSILON)
    if vol_ratio >= 1.5:
        vol_score = 1.0
    elif vol_ratio >= 1.0:
        vol_score = 0.7
    elif vol_ratio >= 0.7:
        vol_score = 0.4
    else:
        vol_score = 0.1  # heavy selling = chop

    # ── Component 3: No large wicks (chop indicator) ─────────────────────
    body_sizes = (closes - tail["open"]).abs()
    range_sizes = highs - lows
    wick_ratio = float(
        (range_sizes - body_sizes).mean() / (range_sizes.mean() + EPSILON)
    )
    wick_score = max(0.0, 1.0 - wick_ratio * 2)  # 0 wick = 1.0, large wick = 0.0

    # ── Composite ─────────────────────────────────────────────────────────
    score = 0.40 * depth_score + 0.40 * vol_score + 0.20 * wick_score
    return round(min(max(score, 0.0), 1.0), 3)


def _enhanced_sqs(
    setup: SetupResult,
    df_1m: pd.DataFrame,
    cfg,
) -> Tuple[float, dict]:
    """
    SHARK MODE WEIGHTS: Prioritizes RVOL and Pullback integrity over VWAP proximity.
    """
    pullback = _pullback_integrity_score(df_1m)
    pressure = calculate_pressure_score(df_1m)
    rvol_strength = max(0.0, min(1.0, float(getattr(setup, "rvol_strength", 0.0))))

    components = {
        SQ_VWAP_RECLAIM: round(min(abs(setup.vwap_dist_pct) * 20, 1.0), 3),
        SQ_STRUCTURE: round(setup.structure_clarity, 3),
        SQ_VOLUME_EXP: round(min((setup.volume_expansion - 1.0) / 2.0, 1.0), 3),
        SQ_PULLBACK: round(pullback, 3),
        SQ_RVOL_STRENGTH: round(rvol_strength, 3),
        SQ_PRESSURE: round(pressure, 3),
    }

    score = (
        0.30 * components[SQ_RVOL_STRENGTH]
        + 0.25 * components[SQ_PULLBACK]
        + 0.20 * components[SQ_VOLUME_EXP]
        + 0.15 * components[SQ_VWAP_RECLAIM]
        + 0.10 * components[SQ_PRESSURE]
    )
    return round(min(score, 1.0), 4), components


# ─────────────────────────────────────────────────────────────────────────────
# GATE RESULT
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class GateResult:
    passed: bool = False
    gate_name: str = ""
    reason: str = ""
    size_multiplier: float = 1.0  # < 1 if degrade applies, 0 if blocked


# ─────────────────────────────────────────────────────────────────────────────
# DECISION RESULT
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class DecisionResult:
    """Full audit trail for one engine cycle."""

    # Selection outcome
    selected_ticker: Optional[str] = None
    setup: Optional[SetupResult] = None
    selection: Optional[SelectionResult] = None

    # Rejected candidates this cycle (ticker → gate that blocked it)
    rejected: Dict[str, str] = field(default_factory=dict)
    # Detailed rejection context (ticker → human-readable reason)
    rejected_detail: Dict[str, str] = field(default_factory=dict)

    # Aggregate reason
    reason: str = ""

    # Risk state at decision time
    regime: str = ""
    open_trades: int = 0
    daily_pnl: float = 0.0

    # All gate results per ticker (for replay / explainability)
    gate_log: Dict[str, List[dict]] = field(default_factory=dict)

    # Composite size multiplier for the selected trade
    health_size_multiplier: float = 1.0
    slippage_size_multiplier: float = 1.0

    # Metadata
    timestamp: Optional[datetime] = None
    top15_snapshot: List[dict] = field(default_factory=list)
    sqs_components: dict = field(default_factory=dict)


# ─────────────────────────────────────────────────────────────────────────────
# DECISION ENGINE
# ─────────────────────────────────────────────────────────────────────────────


class DecisionEngine:
    """
    Deterministic decision engine.

    Usage:
        engine = DecisionEngine(risk_manager, slippage_monitor)
        result = engine.run(
            top15      = universe_manager.top_n(),
            bar_store  = {ticker: df_1m, ...},
            mtf_store  = {ticker: mtf_dict, ...},
            quote_store= {ticker: quote, ...},
            dh_store   = {ticker: DataHealthReport, ...},
            regime     = current_regime,
            now        = datetime.now(ET),
        )
    """

    def __init__(
        self,
        risk_manager: RiskManager,
        slip_monitor: SlippageMonitor = None,
    ) -> None:
        self._risk = risk_manager
        self._slip = slip_monitor or slippage_monitor
        self._setup = MomentumSetupV1()
        self._selector = DeterministicSelector()
        self._dcfg = CONFIG.decision
        self._rcfg = CONFIG.risk

    def run(
        self,
        top15: List[CandidateEntry],
        bar_store: Dict[str, pd.DataFrame],  # ticker → df_1m
        mtf_store: Dict[str, Dict[str, pd.DataFrame]],  # ticker → mtf_dict
        quote_store: Dict[str, dict],  # ticker → quote
        dh_store: Dict[str, DataHealthReport],  # ticker → report
        regime: str = "",
        now: datetime = None,
    ) -> DecisionResult:
        """
        Run one full decision cycle.
        Returns DecisionResult (selected_ticker may be None if no valid setup found).
        """
        now = now or datetime.now(ET)
        dcfg = self._dcfg
        result = DecisionResult(
            timestamp=now,
            regime=regime,
            open_trades=len(self._risk.open_trades()),
            daily_pnl=self._risk.daily_pnl(),
            top15_snapshot=[e.to_dict() for e in top15],
        )

        # ── Regime shaping: adjust thresholds ────────────────────────────
        from utils.spread_policy import effective_spread_cap

        is_chop = regime == REGIME_CHOP
        spread_cap = effective_spread_cap(session="RTH", regime=regime, phase="entry")
        quote_limit = dcfg.max_quote_age_entry_seconds
        min_sqs = dcfg.min_setup_quality_score
        if is_chop:
            min_sqs = min_sqs * dcfg.chop_score_multiplier

        valid_candidates: List[TradeCandidate] = []

        for entry in top15:
            ticker = entry.ticker
            gates: List[dict] = []

            # ── Gate A: Microstructure ────────────────────────────────────
            quote = quote_store.get(ticker, {})
            bid = quote.get("bid", 0.0)
            ask = quote.get("ask", 0.0)
            quote_ts = quote.get("timestamp")
            spread_pct = (
                ((ask - bid) / ((ask + bid) / 2 + EPSILON)) if bid > EPSILON else 1.0
            )
            quote_age = (now - quote_ts).total_seconds() if quote_ts else 9999.0

            g = self._gate(
                GATE_SPREAD_WIDE,
                spread_pct <= spread_cap,
                f"spread={spread_pct*100:.2f}% > cap={spread_cap*100:.2f}%",
            )
            gates.append(g.__dict__)
            if not g.passed:
                self._log_rejection(result, ticker, g, gates)
                continue

            g = self._gate(
                GATE_QUOTE_STALE,
                quote_age <= quote_limit,
                f"quote_age={quote_age:.1f}s > limit={quote_limit}s",
            )
            gates.append(g.__dict__)
            if not g.passed:
                self._log_rejection(result, ticker, g, gates)
                continue

            # ── Gate B: DATA_HEALTH ───────────────────────────────────────
            dh = dh_store.get(ticker)
            health_mult = 1.0
            if dh is not None:
                if dh.status == DH_BLOCK:
                    g = self._gate(
                        GATE_DH_BLOCK,
                        False,
                        f"DH_BLOCK: {dh.block_reason}",
                    )
                    gates.append(g.__dict__)
                    self._log_rejection(result, ticker, g, gates)
                    continue
                health_mult = dh.size_multiplier  # 1.0 if OK, < 1.0 if DEGRADE
            gates.append(
                {"gate": "DATA_HEALTH", "passed": True, "size_mult": health_mult}
            )

            # ── Gate C: Slippage ──────────────────────────────────────────
            if self._slip.should_block(ticker, now):
                g = self._gate(
                    GATE_SLIPPAGE_BLOCK,
                    False,
                    "ticker temporarily blocked due to repeated high slippage",
                )
                gates.append(g.__dict__)
                self._log_rejection(result, ticker, g, gates)
                continue
            slip_mult = self._slip.size_multiplier(ticker)
            gates.append({"gate": "SLIPPAGE", "passed": True, "size_mult": slip_mult})

            # ── Gate D: Risk ──────────────────────────────────────────────
            risk_ok, risk_reason = self._risk_gate(ticker, now, regime=regime)
            if not risk_ok:
                g = self._gate(risk_reason, False, risk_reason)
                gates.append(g.__dict__)
                self._log_rejection(result, ticker, g, gates)
                continue
            gates.append({"gate": "RISK", "passed": True})

            # ── Gate E: Setup Quality ─────────────────────────────────────
            mtf_bars = mtf_store.get(ticker, {})
            setup = self._setup.check(ticker, mtf_bars, bid, ask, now=now)

            if not setup.valid:
                g = self._gate(
                    GATE_NO_VALID_SETUP,
                    False,
                    setup.rejection_reason or "setup invalid",
                )
                gates.append(g.__dict__)
                self._log_rejection(result, ticker, g, gates)
                continue

            # Enhanced SQS with pullback component
            df_1m = bar_store.get(ticker)
            enh_sqs, sqs_components = _enhanced_sqs(setup, df_1m, dcfg)
            setup.setup_quality_score = enh_sqs  # override with enhanced score

            if enh_sqs < min_sqs:
                g = self._gate(
                    GATE_LOW_SQS,
                    False,
                    f"SQS={enh_sqs:.3f} < min={min_sqs:.3f} (regime={regime})",
                )
                gates.append(g.__dict__)
                self._log_rejection(result, ticker, g, gates)
                continue

            gates.append(
                {
                    "gate": "SETUP",
                    "passed": True,
                    "sqs": enh_sqs,
                    "components": sqs_components,
                }
            )
            result.gate_log[ticker] = gates

            # ── Build TradeCandidate ──────────────────────────────────────
            from scanner.demand import DemandMetrics

            dm = DemandMetrics(
                ticker=ticker,
                last_price=entry.last_price,
                dollar_volume=entry.dollar_volume,
                rvol=entry.rvol,
                gap_pct=entry.gap_pct,
                demand_score=entry.demand_score,
                rank=entry.rank,
                bid=bid,
                ask=ask,
            )
            dm._dh_report = dh
            dm._feed_type = entry.feed_type

            candidate = TradeCandidate(metrics=dm, setup=setup)
            candidate._health_mult = health_mult  # stash for engine
            candidate._slip_mult = slip_mult
            candidate._sqs_comps = sqs_components
            valid_candidates.append(candidate)

        # ── Deterministic selection ───────────────────────────────────────
        if not valid_candidates:
            preview = []
            for ticker, gate in list(result.rejected.items())[:6]:
                detail = str(result.rejected_detail.get(ticker, "") or "")
                if gate == GATE_NO_VALID_SETUP and detail:
                    preview.append(f"{ticker}({gate}: {detail})")
                else:
                    preview.append(f"{ticker}({gate})")
            result.reason = (
                "No valid candidates after all gates. "
                "Rejected: " + ", ".join(preview)
            )
            log.info(f"[DecisionEngine] {result.reason}")
            return result

        sel = self._selector.select(valid_candidates)
        result.selection = sel

        if sel.selected is None:
            result.reason = sel.reason
            return result

        winner = sel.selected
        result.selected_ticker = winner.ticker
        result.setup = winner.setup
        result.health_size_multiplier = getattr(winner, "_health_mult", 1.0)
        result.slippage_size_multiplier = getattr(winner, "_slip_mult", 1.0)
        result.sqs_components = getattr(winner, "_sqs_comps", {})
        result.reason = GATE_PASS

        log.info(
            f"[DecisionEngine] SELECTED {winner.ticker} "
            f"DS={winner.demand_score:.3f} SQS={winner.setup_quality_score:.3f} "
            f"rank={winner.universe_rank} "
            f"health_mult={result.health_size_multiplier:.2f} "
            f"slip_mult={result.slippage_size_multiplier:.2f}"
        )
        return result

    # ── Internal helpers ─────────────────────────────────────────────────────

    def _gate(self, name: str, passed: bool, reason: str = "") -> GateResult:
        return GateResult(passed=passed, gate_name=name, reason=reason)

    def _log_rejection(
        self,
        result: DecisionResult,
        ticker: str,
        gate_res: GateResult,
        gates: List[dict],
    ) -> None:
        result.rejected[ticker] = gate_res.gate_name
        result.rejected_detail[ticker] = gate_res.reason or gate_res.gate_name
        result.gate_log[ticker] = gates

    def _risk_gate(
        self, ticker: str, now: datetime, regime: str = ""
    ) -> Tuple[bool, str]:
        """Check all risk limits using the public RiskManager API."""
        ok, reason = self._risk.can_trade(ticker, regime=regime, now=now)
        # Map REJECT_* reasons to GATE_* constants where possible
        if not ok:
            if "DAILY_LOSS" in reason:
                return False, GATE_RISK_DAILY_LOSS
            if "MAX_TRADES" in reason:
                return False, GATE_RISK_MAX_TRADES
            if "TICKER_TRADES" in reason:
                return False, GATE_RISK_TICKER_CAP
            if "COOLDOWN" in reason:
                return False, GATE_RISK_COOLDOWN
            return False, reason
        # Additionally enforce concurrent trades cap (not in RiskManager.can_trade)
        dcfg = CONFIG.decision
        if len(self._risk.open_trades()) >= dcfg.max_concurrent_trades:
            return False, GATE_RISK_MAX_TRADES
        return True, GATE_PASS
