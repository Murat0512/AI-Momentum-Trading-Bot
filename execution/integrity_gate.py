"""
execution/integrity_gate.py — MarketIntegrityGate

Hard-blocks new entries when market/data/broker integrity is compromised.
Completely separate from regime detection: regime changes *strategy*;
integrity controls *safety*.

Checks (evaluated every call to check()):
  1. Stream gap     — no fresh 1m bar from feed in > N seconds
  2. Crossed market — bid >= ask (quote integrity failure)
  3. Spread lock    — spread value identical for N consecutive ticks
  4. Broker rejects — N consecutive order rejects within window
  5. Data disconn   — feed never returned a bar since startup

Auto-clear (hysteresis):
  Gate stays closed until N consecutive clean ticks are seen.
  This prevents flip-flopping on transient glitches.

Thread-safe.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Tuple

import pytz

from config.constants import (
    INTEGRITY_BROKER_REJECT,
    INTEGRITY_CROSSED_MARKET,
    INTEGRITY_DATA_DISCONNECT,
    INTEGRITY_FORCED_HALT,
    INTEGRITY_OK,
    INTEGRITY_SPREAD_LOCK,
    INTEGRITY_STREAM_GAP,
)
from config.settings import CONFIG

log = logging.getLogger(__name__)
ET = pytz.timezone("America/New_York")


# ─────────────────────────────────────────────────────────────────────────────
# DATA TYPES
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class IntegrityFailure:
    code: str
    reason: str
    detected_at: datetime = field(default_factory=lambda: datetime.now(ET))


# ─────────────────────────────────────────────────────────────────────────────
# INTEGRITY GATE
# ─────────────────────────────────────────────────────────────────────────────


class IntegrityGate:
    """
    Evaluates market/data integrity every tick and blocks new entries when
    any check fails.

    Usage:
        ok, reason = integrity_gate.check(
            ticker        = "AAPL",
            bid           = 150.10,
            ask           = 150.12,
            last_bar_time = datetime(2024, 6, 10, 10, 30, tzinfo=ET),
        )
        if not ok:
            return   # skip this candidate

    Call record_reject() on each broker order rejection.
    Call record_fill()   on each successful fill (resets reject counter).
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._active_failures: List[IntegrityFailure] = []
        self._clean_ticks: int = 0
        self._is_halted: bool = False

        # Stream-gap tracking
        self._last_bar_time: Optional[datetime] = None
        self._received_any_bar: bool = False

        # Spread-lock tracking (rolling window)
        self._spread_history: List[float] = []

        # Broker reject-loop tracking
        self._reject_count: int = 0
        self._reject_window_start: Optional[datetime] = None

    # ── Public API ────────────────────────────────────────────────────────────

    def check(
        self,
        *,
        ticker: str = "",
        bid: float = 0.0,
        ask: float = 0.0,
        last_bar_time: Optional[datetime] = None,
        now: Optional[datetime] = None,
    ) -> Tuple[bool, str]:
        """
        Run all integrity checks. Returns (True, "") if clear,
        (False, reason) if any check fails.

        Should be called once per entry candidate per tick.
        """
        now = now or datetime.now(ET)
        cfg = CONFIG.integrity_gate
        failures: List[IntegrityFailure] = []

        with self._lock:
            # Update bar time observable
            if last_bar_time is not None:
                self._last_bar_time = last_bar_time
                self._received_any_bar = True

            # ── 1. Stream gap ─────────────────────────────────────────────────
            if self._received_any_bar and self._last_bar_time is not None:
                gap_s = (now - self._last_bar_time).total_seconds()
                if gap_s > cfg.stream_gap_seconds:
                    failures.append(
                        IntegrityFailure(
                            code=INTEGRITY_STREAM_GAP,
                            reason=(
                                f"last bar {gap_s:.0f}s ago "
                                f"(threshold={cfg.stream_gap_seconds}s)"
                            ),
                        )
                    )

            # ── 2. Crossed market ─────────────────────────────────────────────
            if bid > 0 and ask > 0 and bid >= ask:
                failures.append(
                    IntegrityFailure(
                        code=INTEGRITY_CROSSED_MARKET,
                        reason=f"bid={bid:.4f} >= ask={ask:.4f} [{ticker}]",
                    )
                )

            # ── 3. Spread lock ────────────────────────────────────────────────
            if bid > 0 and ask > 0:
                spread = round(ask - bid, 6)
                self._spread_history.append(spread)
                # Keep only the last N values
                if len(self._spread_history) > cfg.spread_lock_ticks:
                    self._spread_history = self._spread_history[
                        -cfg.spread_lock_ticks :
                    ]
                if (
                    len(self._spread_history) >= cfg.spread_lock_ticks
                    and len(set(self._spread_history)) == 1
                    and spread > 0
                ):
                    failures.append(
                        IntegrityFailure(
                            code=INTEGRITY_SPREAD_LOCK,
                            reason=(
                                f"spread locked at {spread:.6f} "
                                f"for {cfg.spread_lock_ticks} consecutive ticks [{ticker}]"
                            ),
                        )
                    )

            # ── 4. Broker reject loop ─────────────────────────────────────────
            if self._reject_count >= cfg.broker_reject_threshold:
                failures.append(
                    IntegrityFailure(
                        code=INTEGRITY_BROKER_REJECT,
                        reason=(
                            f"{self._reject_count} consecutive order rejects "
                            f"(threshold={cfg.broker_reject_threshold})"
                        ),
                    )
                )

            # ── 5. Data disconnect (no bars ever received) ────────────────────
            if not self._received_any_bar:
                failures.append(
                    IntegrityFailure(
                        code=INTEGRITY_DATA_DISCONNECT,
                        reason="no bar data received since startup",
                    )
                )

            # ── Hysteresis logic ──────────────────────────────────────────────
            if failures:
                self._active_failures = failures
                self._clean_ticks = 0
                self._is_halted = True
                reason_str = " | ".join(f"{f.code}: {f.reason}" for f in failures)
                if ticker:
                    log.warning(f"[INTEGRITY] Gate CLOSED [{ticker}]: {reason_str}")
                return False, reason_str
            else:
                self._clean_ticks += 1
                if self._clean_ticks >= cfg.hysteresis_ok_ticks and self._is_halted:
                    self._active_failures = []
                    self._is_halted = False
                    log.info(
                        f"[INTEGRITY] Gate cleared after "
                        f"{self._clean_ticks} consecutive clean ticks"
                    )
                if self._is_halted:
                    # Still halted (force_halt or waiting for hysteresis)
                    # Return the stored active-failure reason so callers know why
                    reason_str = " | ".join(
                        f"{f.code}: {f.reason}" for f in self._active_failures
                    )
                    return False, reason_str
                return True, ""

    def record_reject(self, now: Optional[datetime] = None) -> None:
        """
        Call whenever a broker order is rejected.
        Tracks the reject loop counter within a rolling time window.
        """
        now = now or datetime.now(ET)
        cfg = CONFIG.integrity_gate
        _trip = False
        with self._lock:
            if self._reject_window_start is None:
                self._reject_window_start = now

            window_age_s = (now - self._reject_window_start).total_seconds()
            if window_age_s > cfg.broker_reject_window_seconds:
                # Old window expired — start fresh
                self._reject_count = 0
                self._reject_window_start = now

            self._reject_count += 1
            log.debug(
                f"[INTEGRITY] reject recorded "
                f"(count={self._reject_count}/{cfg.broker_reject_threshold})"
            )
            if self._reject_count >= cfg.broker_reject_threshold:
                _trip = True
                _count = self._reject_count
                _threshold = cfg.broker_reject_threshold

        if _trip:
            try:
                from events.bus import event_bus
                from events import current_cycle
                from events.types import IntegrityGateTrip

                event_bus.publish(
                    IntegrityGateTrip(
                        cycle_id=current_cycle.id,
                        consecutive_rejects=_count,
                        threshold=_threshold,
                        codes="BROKER_REJECT",
                    )
                )
            except Exception:  # noqa: BLE001
                pass

    def record_fill(self) -> None:
        """
        Call whenever a broker order fills successfully.
        Resets the reject counter.
        """
        with self._lock:
            self._reject_count = 0
            self._reject_window_start = None

    def force_halt(self, reason: str) -> None:
        """
        Manually force the gate closed (e.g. called by reconciler on mismatch).
        Requires force_clear() or N clean ticks to re-open.
        """
        with self._lock:
            self._is_halted = True
            self._active_failures = [
                IntegrityFailure(
                    code=INTEGRITY_FORCED_HALT,
                    reason=reason,
                )
            ]
            self._clean_ticks = 0
        log.warning(f"[INTEGRITY] FORCED HALT: {reason}")

    def force_clear(self) -> None:
        """
        Manually clear the gate (e.g. after manual investigation of a mismatch).
        Skips the hysteresis countdown.
        """
        with self._lock:
            self._is_halted = False
            self._active_failures = []
            self._clean_ticks = CONFIG.integrity_gate.hysteresis_ok_ticks
        log.info("[INTEGRITY] Gate manually cleared")

    def is_halted(self) -> bool:
        """True if the gate is currently closed."""
        with self._lock:
            return self._is_halted

    def active_failures(self) -> List[IntegrityFailure]:
        """Returns a snapshot of the current active failures."""
        with self._lock:
            return list(self._active_failures)

    def reset(self) -> None:
        """Reset all state. Call at the start of each session."""
        with self._lock:
            self._active_failures = []
            self._clean_ticks = 0
            self._is_halted = False
            self._last_bar_time = None
            self._received_any_bar = False
            self._spread_history = []
            self._reject_count = 0
            self._reject_window_start = None
        log.info("[INTEGRITY] Gate reset for new session")


# Module-level singleton
integrity_gate = IntegrityGate()
