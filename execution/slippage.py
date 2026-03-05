"""
execution/slippage.py — Per-ticker slippage monitoring.

Tracks the difference between expected entry price and actual fill
price for every trade. When slippage is persistently high:

  SLIPPAGE_WARN        → log only (> warn_bps)
  SLIPPAGE_SIZE_REDUCE → reduce position size by size_reduce_multiplier
  SLIPPAGE_BLOCK       → block ticker for block_duration_minutes

All thresholds are configurable via CONFIG.slippage.

Slippage is measured in two ways:
  1. Basis points (bps): (fill - expected) / expected × 10_000
  2. R units:            total_slippage_$ / risk_per_trade_$

The R-unit measure is the primary gate because it directly quantifies
how much the slippage erodes expected edge.

Thread-safe.
"""

from __future__ import annotations

import logging
import threading
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Deque, Dict, Optional, Tuple

import pytz

from config.constants import (
    SLIPPAGE_BLOCK, SLIPPAGE_OK, SLIPPAGE_SIZE_REDUCE, SLIPPAGE_WARN,
)
from config.settings import CONFIG

log = logging.getLogger(__name__)
ET  = pytz.timezone("America/New_York")


# ─────────────────────────────────────────────────────────────────────────────
# DATA TYPES
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class SlippageFill:
    """A single recorded fill entry."""
    ticker:          str
    expected_price:  float
    fill_price:      float
    spread_pct:      float       # spread at entry as fraction
    r_value:         float       # risk amount for this trade ($)
    recorded_at:     datetime    = field(default_factory=lambda: datetime.now(ET))

    @property
    def slippage_dollars(self) -> float:
        return self.fill_price - self.expected_price

    @property
    def slippage_bps(self) -> float:
        if self.expected_price <= 0:
            return 0.0
        return (self.fill_price - self.expected_price) / self.expected_price * 10_000.0

    @property
    def slippage_r(self) -> float:
        """Slippage expressed in R units."""
        if self.r_value <= 0:
            return 0.0
        return self.slippage_dollars / self.r_value


@dataclass
class SlippageState:
    """Running state for one ticker."""
    ticker:           str
    fills:            Deque[SlippageFill] = field(default_factory=deque)
    block_until:      Optional[datetime]  = None
    consecutive_high: int                 = 0   # consecutive fills > r_threshold

    def is_blocked(self, now: datetime = None) -> bool:
        now = now or datetime.now(ET)
        if self.block_until is None:
            return False
        return now < self.block_until

    def clear_block(self) -> None:
        self.block_until      = None
        self.consecutive_high = 0

    def avg_slippage_bps(self, lookback: int = None) -> float:
        fills = self._recent(lookback)
        if not fills:
            return 0.0
        return sum(f.slippage_bps for f in fills) / len(fills)

    def avg_slippage_r(self, lookback: int = None) -> float:
        fills = self._recent(lookback)
        if not fills:
            return 0.0
        return sum(f.slippage_r for f in fills) / len(fills)

    def _recent(self, lookback: int = None) -> list:
        n = lookback or CONFIG.slippage.lookback_trades
        return list(self.fills)[-n:]


# ─────────────────────────────────────────────────────────────────────────────
# SLIPPAGE MONITOR
# ─────────────────────────────────────────────────────────────────────────────

class SlippageMonitor:
    """
    Records and evaluates per-ticker slippage across all fills.

    Usage:
        monitor = SlippageMonitor()

        # After a fill is received:
        event = monitor.record_fill("TSLA", expected=150.00, fill=150.18,
                                    spread_pct=0.003, r_value=250.0)
        print(event)   # SLIPPAGE_WARN / SIZE_REDUCE / BLOCK

        # Before entry:
        if monitor.should_block("TSLA"):
            skip_entry()
        mult = monitor.size_multiplier("TSLA")
        shares = base_shares * mult
    """

    def __init__(self) -> None:
        self._states: Dict[str, SlippageState] = {}
        self._lock   = threading.RLock()   # RLock allows re-entrant calls within same thread
        self._cfg    = CONFIG.slippage

    # ── Record ───────────────────────────────────────────────────────────────

    def record_fill(
        self,
        ticker:         str,
        expected_price: float,
        fill_price:     float,
        spread_pct:     float,
        r_value:        float,
        now:            datetime = None,
    ) -> str:
        """
        Record a completed fill and return the slippage event tag.

        Returns one of:
            SLIPPAGE_OK, SLIPPAGE_WARN, SLIPPAGE_SIZE_REDUCE, SLIPPAGE_BLOCK
        """
        now  = now or datetime.now(ET)
        fill = SlippageFill(
            ticker         = ticker,
            expected_price = expected_price,
            fill_price     = fill_price,
            spread_pct     = spread_pct,
            r_value        = r_value,
            recorded_at    = now,
        )

        cfg = self._cfg
        with self._lock:
            state = self._get_or_create(ticker)

            state.fills.append(fill)
            # Trim to lookback window
            while len(state.fills) > cfg.lookback_trades:
                state.fills.popleft()

            bps        = fill.slippage_bps
            slip_r     = fill.slippage_r
            is_high_r  = slip_r > cfg.size_reduce_r_threshold

            if is_high_r:
                state.consecutive_high += 1
            else:
                state.consecutive_high = 0

            # Determine event level
            if state.consecutive_high >= cfg.block_consecutive_trades:
                state.block_until = now + timedelta(minutes=cfg.block_duration_minutes)
                state.consecutive_high = 0   # reset after blocking
                event = SLIPPAGE_BLOCK
                log.warning(
                    f"[SlippageMonitor] {ticker} BLOCKED for {cfg.block_duration_minutes}m "
                    f"— consecutive high slippage (last={slip_r:.3f}R / {bps:.1f}bps)"
                )
            elif is_high_r:
                event = SLIPPAGE_SIZE_REDUCE
                log.info(
                    f"[SlippageMonitor] {ticker} SIZE_REDUCE "
                    f"slip={slip_r:.3f}R / {bps:.1f}bps"
                )
            elif bps > cfg.warn_bps:
                event = SLIPPAGE_WARN
                log.debug(
                    f"[SlippageMonitor] {ticker} WARN slip={bps:.1f}bps"
                )
            else:
                event = SLIPPAGE_OK

        # Publish domain event (best-effort, outside lock)
        try:
            from events.bus import event_bus
            from events import current_cycle
            from events.types import SlippageRecorded
            event_bus.publish(SlippageRecorded(
                cycle_id     = current_cycle.id,
                ticker       = ticker,
                slippage_bps = fill.slippage_bps,
                slippage_r   = fill.slippage_r,
                action       = event,
            ))
        except Exception:  # noqa: BLE001
            pass

        return event

    # ── Query ─────────────────────────────────────────────────────────────────

    def should_block(self, ticker: str, now: datetime = None) -> bool:
        """Return True if the ticker is currently in a slippage-block window."""
        now = now or datetime.now(ET)
        with self._lock:
            state = self._states.get(ticker)
            if state is None:
                return False
            if state.is_blocked(now):
                return True
            # Auto-clear expired blocks
            if state.block_until and now >= state.block_until:
                state.clear_block()
            return False

    def size_multiplier(self, ticker: str) -> float:
        """
        Return the size multiplier to apply for this ticker.
        1.0 = full size; < 1.0 = reduce; 0.0 if blocked.
        """
        cfg = self._cfg
        if self.should_block(ticker):
            return 0.0
        with self._lock:
            state = self._states.get(ticker)
            if state is None:
                return 1.0
            avg_r = state.avg_slippage_r()
            if avg_r > cfg.size_reduce_r_threshold:
                return cfg.size_reduce_multiplier
        return 1.0

    def slippage_event_tag(self, ticker: str) -> str:
        """Current slippage status tag for a ticker."""
        if self.should_block(ticker):
            return SLIPPAGE_BLOCK
        mult = self.size_multiplier(ticker)
        if mult < 1.0:
            return SLIPPAGE_SIZE_REDUCE
        cfg = self._cfg
        with self._lock:
            state = self._states.get(ticker)
            if state is None:
                return SLIPPAGE_OK
            bps = state.avg_slippage_bps()
        return SLIPPAGE_WARN if bps > cfg.warn_bps else SLIPPAGE_OK

    def ticker_stats(self, ticker: str) -> dict:
        """
        Return diagnostic stats for one ticker.
        Safe to call from the explainability logger.
        """
        with self._lock:
            state = self._states.get(ticker)
            if state is None:
                return {"ticker": ticker, "fills": 0}
            return {
                "ticker":            ticker,
                "fills":             len(state.fills),
                "avg_slippage_bps":  round(state.avg_slippage_bps(), 2),
                "avg_slippage_r":    round(state.avg_slippage_r(), 4),
                "consecutive_high":  state.consecutive_high,
                "blocked":           state.is_blocked(),
                "block_until":       state.block_until.isoformat() if state.block_until else None,
                "size_multiplier":   self.size_multiplier(ticker),
            }

    def all_stats(self) -> Dict[str, dict]:
        """Diagnostic snapshot for all tracked tickers."""
        with self._lock:
            tickers = list(self._states.keys())
        return {t: self.ticker_stats(t) for t in tickers}

    def reset_ticker(self, ticker: str) -> None:
        """Clear slippage history for a ticker (e.g., at session start)."""
        with self._lock:
            if ticker in self._states:
                del self._states[ticker]

    def reset_all(self) -> None:
        """Clear all slippage history — call at daily session start."""
        with self._lock:
            self._states.clear()
        log.info("[SlippageMonitor] Reset all ticker slippage histories.")

    # ── Internal ─────────────────────────────────────────────────────────────

    def _get_or_create(self, ticker: str) -> SlippageState:
        """Caller must hold self._lock."""
        if ticker not in self._states:
            self._states[ticker] = SlippageState(ticker=ticker)
        return self._states[ticker]


# ─────────────────────────────────────────────────────────────────────────────
# Module-level singleton
# ─────────────────────────────────────────────────────────────────────────────
slippage_monitor = SlippageMonitor()
