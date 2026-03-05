"""
events/types.py — All domain event dataclasses.

Rules:
    * Every event is frozen=True (immutable after creation)
    * Every event inherits DomainEvent (carries cycle_id + ts)
    * Names are past-tense nouns describing what happened
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import pytz

ET = pytz.timezone("America/New_York")


def _now_et() -> datetime:
    return datetime.now(ET)


# ─────────────────────────────────────────────────────────────────────────────
# Base
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class DomainEvent:
    """Base class for all domain events."""

    cycle_id: int = 0
    ts: datetime = field(default_factory=_now_et)


# ─────────────────────────────────────────────────────────────────────────────
# Data / Universe layer
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class BarIngested(DomainEvent):
    ticker: str = ""
    tf: str = ""
    bar_ts: Optional[datetime] = None
    n_bars: int = 0


@dataclass(frozen=True)
class UniverseScanned(DomainEvent):
    n_passed: int = 0
    n_rejected: int = 0
    top_ticker: str = ""
    regime: str = ""


@dataclass(frozen=True)
class CandidateRanked(DomainEvent):
    ticker: str = ""
    demand_score: float = 0.0
    rank: int = 0
    rvol: float = 0.0
    gap_pct: float = 0.0


@dataclass(frozen=True)
class MomentumMetricsComputed(DomainEvent):
    ticker: str = ""
    dollar_flow_z: float = 0.0
    pressure_z: float = 0.0


# ─────────────────────────────────────────────────────────────────────────────
# Setup / Decision layer
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SetupQualified(DomainEvent):
    ticker: str = ""
    setup_type: str = ""
    sqs: float = 0.0
    entry_price: float = 0.0
    stop_price: float = 0.0
    r_distance: float = 0.0


@dataclass(frozen=True)
class DecisionRejected(DomainEvent):
    ticker: str = ""
    gate: str = ""
    reason: str = ""


# ─────────────────────────────────────────────────────────────────────────────
# Order layer
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class OrderSubmitted(DomainEvent):
    order_id: str = ""
    ticker: str = ""
    side: str = ""
    qty: int = 0
    limit_price: float = 0.0


@dataclass(frozen=True)
class OrderFilled(DomainEvent):
    order_id: str = ""
    ticker: str = ""
    side: str = ""
    filled_qty: int = 0
    filled_price: float = 0.0


@dataclass(frozen=True)
class OrderPartial(DomainEvent):
    order_id: str = ""
    ticker: str = ""
    filled_qty: int = 0
    remaining: int = 0
    fill_price: float = 0.0


@dataclass(frozen=True)
class OrderCancelled(DomainEvent):
    order_id: str = ""
    ticker: str = ""
    reason: str = ""


# ─────────────────────────────────────────────────────────────────────────────
# Lifecycle layer
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class LifecycleTransition(DomainEvent):
    trade_id: str = ""
    ticker: str = ""
    from_state: str = ""
    to_state: str = ""
    reason: str = ""
    shares_sold: int = 0
    pnl: float = 0.0


# ─────────────────────────────────────────────────────────────────────────────
# Risk / Gate layer
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class RiskGateBlocked(DomainEvent):
    ticker: str = ""
    gate: str = ""
    reason: str = ""


@dataclass(frozen=True)
class SlippageRecorded(DomainEvent):
    ticker: str = ""
    slippage_bps: float = 0.0
    slippage_r: float = 0.0
    action: str = (
        ""  # SLIPPAGE_OK | SLIPPAGE_WARN | SLIPPAGE_SIZE_REDUCE | SLIPPAGE_BLOCK
    )


@dataclass(frozen=True)
class HaltStateChange(DomainEvent):
    ticker: str = ""
    new_state: str = ""
    reason: str = ""


@dataclass(frozen=True)
class IntegrityGateTrip(DomainEvent):
    consecutive_rejects: int = 0
    threshold: int = 0
    codes: str = ""  # pipe-separated failure codes


# ─────────────────────────────────────────────────────────────────────────────
# Portfolio / Supervisor layer
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class PortfolioGateResult(DomainEvent):
    ticker: str = ""
    decision: str = ""  # ALLOW | ALLOW_WITH_MULTIPLIER | BLOCK
    reason: str = ""
    multiplier: float = 1.0


@dataclass(frozen=True)
class PositionSizeCapped(DomainEvent):
    ticker: str = ""
    qty_base: int = 0
    qty_final: int = 0
    cap_reason: str = ""
    cap_values: str = ""


@dataclass(frozen=True)
class SupervisorStateChange(DomainEvent):
    from_state: str = ""
    to_state: str = ""
    trigger: str = ""


@dataclass(frozen=True)
class LegacyAuditEvent(DomainEvent):
    event_id: str = ""
    event_type: str = ""
    run_id: str = ""
    ticker: str = ""
    payload: dict = field(default_factory=dict)


__all__ = [
    "DomainEvent",
    "BarIngested",
    "UniverseScanned",
    "CandidateRanked",
    "MomentumMetricsComputed",
    "SetupQualified",
    "DecisionRejected",
    "OrderSubmitted",
    "OrderFilled",
    "OrderPartial",
    "OrderCancelled",
    "LifecycleTransition",
    "RiskGateBlocked",
    "SlippageRecorded",
    "HaltStateChange",
    "IntegrityGateTrip",
    "PortfolioGateResult",
    "PositionSizeCapped",
    "SupervisorStateChange",
    "LegacyAuditEvent",
]
