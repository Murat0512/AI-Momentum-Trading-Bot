"""
selector.py — Deterministic trade candidate selection.

Given a list of (DemandMetrics, SetupResult) pairs,
select the single best trade using a fixed tie-breaking hierarchy:

  1. Valid setup only
  2. Highest DemandScore
  3. Then highest SetupQualityScore
  4. Then tightest spread

Same inputs ALWAYS produce the same selection — zero randomness.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Tuple

from scanner.demand import DemandMetrics
from signals.setup import SetupResult

log = logging.getLogger(__name__)


@dataclass
class TradeCandidate:
    metrics:  DemandMetrics
    setup:    SetupResult

    @property
    def ticker(self) -> str:
        return self.metrics.ticker

    @property
    def demand_score(self) -> float:
        return self.metrics.demand_score

    @property
    def setup_quality_score(self) -> float:
        return self.setup.setup_quality_score

    @property
    def spread_pct(self) -> float:
        return self.setup.spread_pct

    @property
    def universe_rank(self) -> int:
        """Rank in DemandScore universe (1 = highest demand score)."""
        return self.metrics.rank

    def selection_key(self) -> tuple:
        """
        Deterministic sort key.
        Sort order is ascending on this tuple so ticker is the final
        deterministic tie-break for fully-equal metrics.
        """
        return (
            -self.demand_score,
            -self.setup_quality_score,
            self.spread_pct,
            self.ticker,
        )


@dataclass
class SelectionResult:
    selected:         Optional[TradeCandidate] = None
    candidates:       List[TradeCandidate]     = field(default_factory=list)
    rejected:         List[TradeCandidate]     = field(default_factory=list)
    reason:           str                      = ""
    # Determinism audit fields
    selection_reason: str                      = ""   # full tie-break explanation
    universe_rank:    int                      = 0    # selected ticker's universe rank
    chosen_over:      List[str]                = field(default_factory=list)
    selected_at:      Optional[datetime]       = None


class DeterministicSelector:
    """
    Selects exactly one trade candidate per engine cycle.

    Rules:
      - Only consider candidates with setup.valid == True
      - Sort by (DemandScore DESC, SQS DESC, Spread ASC)
      - Return the top-ranked candidate
    """

    def select(
        self,
        candidates: List[TradeCandidate],
    ) -> SelectionResult:
        result = SelectionResult(selected_at=datetime.now())

        if not candidates:
            result.reason = "No candidates submitted"
            return result

        valid    = [c for c in candidates if c.setup.valid]
        rejected = [c for c in candidates if not c.setup.valid]
        result.rejected = rejected

        if not valid:
            result.reason = (
                "All candidates failed setup: "
                + "; ".join(
                    f"{c.ticker}({c.setup.rejection_reason})"
                    for c in rejected[:5]
                )
            )
            log.debug(f"Selector: no valid setups — {result.reason}")
            return result

        # Sort deterministically
        ranked = sorted(valid, key=lambda c: c.selection_key())
        winner = ranked[0]
        runner_ups = ranked[1:]

        result.candidates  = ranked
        result.selected    = winner
        result.universe_rank = winner.universe_rank
        result.chosen_over = [
            f"{c.ticker}(DS={c.demand_score:.2f},SQS={c.setup_quality_score:.2f})"
            for c in runner_ups
        ]

        # Build determinism audit string with tie-break explanation
        tie_break_field = "demand_score"
        if runner_ups:
            if winner.demand_score != runner_ups[0].demand_score:
                tie_break_field = "demand_score"
            elif winner.setup_quality_score != runner_ups[0].setup_quality_score:
                tie_break_field = "setup_quality_score"
            elif winner.spread_pct != runner_ups[0].spread_pct:
                tie_break_field = "spread"
            else:
                tie_break_field = "ticker"

        runner_summary = "; ".join(
            f"{c.ticker}(DS={c.demand_score:.2f},SQS={c.setup_quality_score:.2f}"
            f",spread={c.spread_pct*100:.2f}%,rank=#{c.universe_rank})"
            for c in runner_ups[:5]
        )
        result.selection_reason = (
            f"Chose {winner.ticker} "
            f"[DS={winner.demand_score:.3f} SQS={winner.setup_quality_score:.3f} "
            f"spread={winner.spread_pct*100:.2f}% rank=#{winner.universe_rank}] "
            f"via tiebreak={tie_break_field}"
            + (f" over: [{runner_summary}]" if runner_summary else "")
        )
        result.reason = result.selection_reason

        log.info(
            f"Selector: {result.selection_reason}"
        )
        return result
