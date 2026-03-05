"""
Acceptance Test 4 — Deterministic selection invariants.

Core invariant:
  Given identical DemandMetrics + SetupResult inputs, the selector ALWAYS
  returns the same ticker, with the same selection_reason, with zero
  randomness across N independent runs.

Sub-invariants:
  A. Highest DemandScore wins when scores differ.
  B. When DemandScore is tied, highest SetupQualityScore wins.
  C. When both are tied, tightest spread wins.
  D. selection_reason reveals which tiebreak field was decisive.
  E. universe_rank is correctly populated from DemandMetrics.rank.
  F. Running the selector 100× on identical inputs yields identical output.
"""

from __future__ import annotations

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dataclasses import dataclass, field
from typing import Optional
import pytest

from scanner.demand import DemandMetrics
from selection.selector import DeterministicSelector, TradeCandidate, SelectionResult


# ─────────────────────────────────────────────────────────────────────────────
# Minimal stub for SetupResult
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class StubSetup:
    valid:               bool  = True
    setup_quality_score: float = 0.5
    spread_pct:          float = 0.002
    entry_price:         float = 10.0
    stop_price:          float = 9.5
    rejection_reason:    str   = ""
    setup_name:          str   = "MomentumV1"
    break_level_name:    str   = "PMH"
    bars_1m:             int   = 60
    bars_5m:             int   = 12
    bars_15m:            int   = 4


def _dm(ticker: str, ds: float, rank: int = 1) -> DemandMetrics:
    m = DemandMetrics(
        ticker=ticker, last_price=10.0, dollar_volume=5e6,
        rvol=3.0, gap_pct=0.05, intraday_range_pct=0.04,
        volume_spike_z=2.0, demand_score=ds, rank=rank,
        bid=9.99, ask=10.01,
    )
    return m


def _candidate(ticker: str, ds: float, sqs: float, spread: float, rank: int = 1) -> TradeCandidate:
    dm    = _dm(ticker, ds, rank)
    setup = StubSetup(setup_quality_score=sqs, spread_pct=spread)
    return TradeCandidate(metrics=dm, setup=setup)  # type: ignore[arg-type]


selector = DeterministicSelector()


# ─────────────────────────────────────────────────────────────────────────────
# Test A: Highest DemandScore wins
# ─────────────────────────────────────────────────────────────────────────────

def test_highest_demand_score_wins():
    a = _candidate("AAAA", ds=4.5, sqs=0.6, spread=0.002, rank=1)
    b = _candidate("BBBB", ds=3.0, sqs=0.9, spread=0.001, rank=2)  # higher SQS/spread but lower DS

    result = selector.select([a, b])
    assert result.selected is not None
    assert result.selected.ticker == "AAAA", \
        f"Expected AAAA (DS=4.5 > DS=3.0), got {result.selected.ticker}"
    assert "demand_score" in result.selection_reason or "DS=" in result.selection_reason


# ─────────────────────────────────────────────────────────────────────────────
# Test B: Tied DS → highest SQS wins
# ─────────────────────────────────────────────────────────────────────────────

def test_sqs_tiebreak():
    ds_tie = 3.5
    a = _candidate("AAAA", ds=ds_tie, sqs=0.80, spread=0.003, rank=1)
    b = _candidate("BBBB", ds=ds_tie, sqs=0.55, spread=0.001, rank=2)  # tighter spread but lower SQS

    result = selector.select([a, b])
    assert result.selected is not None
    assert result.selected.ticker == "AAAA", \
        f"Expected AAAA (SQS=0.80 > SQS=0.55 with equal DS), got {result.selected.ticker}"


# ─────────────────────────────────────────────────────────────────────────────
# Test C: Tied DS and SQS → tightest spread wins
# ─────────────────────────────────────────────────────────────────────────────

def test_spread_tiebreak():
    ds_tie  = 3.5
    sqs_tie = 0.70
    a = _candidate("AAAA", ds=ds_tie, sqs=sqs_tie, spread=0.005, rank=1)  # wider
    b = _candidate("BBBB", ds=ds_tie, sqs=sqs_tie, spread=0.001, rank=2)  # tighter

    result = selector.select([a, b])
    assert result.selected is not None
    assert result.selected.ticker == "BBBB", \
        f"Expected BBBB (tightest spread wins when DS and SQS tied), got {result.selected.ticker}"


# ─────────────────────────────────────────────────────────────────────────────
# Test D: selection_reason documents the tiebreak field
# ─────────────────────────────────────────────────────────────────────────────

def test_selection_reason_is_populated():
    a = _candidate("AAAA", ds=5.0, sqs=0.7, spread=0.002, rank=1)
    b = _candidate("BBBB", ds=3.0, sqs=0.9, spread=0.001, rank=2)

    result = selector.select([a, b])
    assert result.selection_reason, "selection_reason must be non-empty"
    assert "AAAA" in result.selection_reason, \
        f"selection_reason must name the winner: {result.selection_reason}"


# ─────────────────────────────────────────────────────────────────────────────
# Test E: universe_rank populated correctly
# ─────────────────────────────────────────────────────────────────────────────

def test_universe_rank_populated():
    a = _candidate("AAAA", ds=5.0, sqs=0.7, spread=0.002, rank=1)
    b = _candidate("BBBB", ds=3.0, sqs=0.9, spread=0.001, rank=2)

    result = selector.select([a, b])
    assert result.universe_rank == 1, \
        f"Selected ticker had rank=1 in universe, got {result.universe_rank}"


# ─────────────────────────────────────────────────────────────────────────────
# Test F: 100 runs on identical inputs → always same winner
# ─────────────────────────────────────────────────────────────────────────────

def test_determinism_100_runs():
    """Same inputs must ALWAYS produce same result — zero randomness."""
    a = _candidate("AAAA", ds=4.5, sqs=0.6, spread=0.002, rank=1)
    b = _candidate("BBBB", ds=3.0, sqs=0.9, spread=0.001, rank=2)
    c = _candidate("CCCC", ds=4.5, sqs=0.7, spread=0.002, rank=3)  # DS tie with A but higher SQS

    winners = set()
    for _ in range(100):
        r = selector.select([a, b, c])
        if r.selected:
            winners.add(r.selected.ticker)

    assert len(winners) == 1, \
        f"Selector must be deterministic — got {len(winners)} different winners: {winners}"
    assert "CCCC" in winners, \
        f"CCCC should win (DS=4.5 tied with AAAA but SQS=0.7 > SQS=0.6), got: {winners}"


def test_ticker_alphabetical_tiebreak_when_scores_equal():
    a = _candidate("ZZZZ", ds=3.5, sqs=0.7, spread=0.002, rank=1)
    b = _candidate("AAAA", ds=3.5, sqs=0.7, spread=0.002, rank=2)

    result = selector.select([a, b])
    assert result.selected is not None
    assert result.selected.ticker == "AAAA"
    assert "tiebreak=ticker" in result.selection_reason


# ─────────────────────────────────────────────────────────────────────────────
# Test G: No valid setups → returns no selection
# ─────────────────────────────────────────────────────────────────────────────

def test_no_valid_setup_returns_empty():
    a = _candidate("AAAA", ds=5.0, sqs=0.7, spread=0.002, rank=1)
    a.setup.valid = False  # type: ignore[attr-defined]
    a.setup.rejection_reason = "no_catalyst"

    result = selector.select([a])
    assert result.selected is None
    assert "no valid" in result.reason.lower() or "setup" in result.reason.lower(), \
        f"reason should explain rejection: {result.reason}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
