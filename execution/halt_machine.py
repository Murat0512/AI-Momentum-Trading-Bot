"""
execution/halt_machine.py — Market halt / circuit-breaker state machine.

Tracks the lifecycle of a detected market halt at the *ticker* level.
When the data health layer reports BLOCK_HALT_DETECTED (3 consecutive
zero-volume pinned bars), the halt machine enters HALTED state and blocks
new entries for that ticker.

After the halt clears, a mandatory *resume gate* applies:
  • N consecutive clean ticks must be observed before entries are re-enabled.
  • During the RESUMING phase a tighter spread requirement is enforced
    (resume_spread_multiplier × normal max spread).

The engine calls:
    halt_machine.on_health_block(ticker, reason)  # every blocked tick
    halt_machine.on_clean_tick(ticker)             # every clean tick
    halt_machine.is_blocked(ticker)                # before order submission
    halt_machine.resume_spread_multiplier(ticker)  # spread tightening factor
    halt_machine.reset(ticker)                     # on EOD / new session

Thread-safe. Module-level singleton `halt_machine` available.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional

import pytz

from config.settings import CONFIG

log = logging.getLogger(__name__)
ET = pytz.timezone("America/New_York")

# ── States ────────────────────────────────────────────────────────────────────
HALT_ACTIVE = "ACTIVE"  # normal — entries allowed
HALT_HALTED = "HALTED"  # halt detected — entries blocked
HALT_RESUMING = "RESUMING"  # cooldown clean ticks after halt clears


@dataclass
class _TickerHaltState:
    state: str = HALT_ACTIVE
    clean_ticks: int = 0
    detected_at: Optional[datetime] = None
    resuming_started_at: Optional[datetime] = None  # when RESUMING phase began
    resumed_at: Optional[datetime] = None
    total_halts: int = 0
    # First post-halt entry tracking
    # Set False when transitioning RESUMING→ACTIVE; set True after first entry
    post_halt_entry_complete: bool = True  # True = no pending size-down


class HaltStateMachine:
    """
    Per-ticker halt / resume gate.

    Config (from CONFIG.integrity_gate):
        hysteresis_ok_ticks — clean ticks required before resuming entries.

    Post-halt resume spread:
        During RESUMING the machine returns resume_spread_mult < 1.0
        so the engine can apply a tighter spread cap before accepting entries.
        Default: 0.60 (40% tighter than normal).
    """

    #: clean ticks required before transitioning RESUMING → ACTIVE
    CLEAN_TICKS_REQUIRED: int = 3
    #: spread multiplier applied while RESUMING (tighter = safer)
    RESUME_SPREAD_MULT: float = 0.60

    def __init__(self) -> None:
        self._lock: threading.Lock = threading.Lock()
        self._states: Dict[str, _TickerHaltState] = {}

    # ── Public API ────────────────────────────────────────────────────────────

    def on_health_block(self, ticker: str, reason: str) -> None:
        """
        Called by the engine each time the data-health layer returns
        BLOCK_HALT_DETECTED for *ticker*.
        """
        _state_changed = False
        with self._lock:
            s = self._get_or_create(ticker)
            if s.state == HALT_ACTIVE:
                s.state = HALT_HALTED
                s.clean_ticks = 0
                s.detected_at = datetime.now(ET)
                s.total_halts += 1
                _state_changed = True
                log.warning(
                    f"[HaltMachine:{ticker}] HALTED — {reason}  "
                    f"(total halts this session: {s.total_halts})"
                )
            elif s.state == HALT_RESUMING:
                # Halt re-detected during cooldown — restart the gate
                s.state = HALT_HALTED
                s.clean_ticks = 0
                s.resuming_started_at = None  # reset cooldown clock
                s.total_halts += 1
                _state_changed = True
                log.warning(
                    f"[HaltMachine:{ticker}] HALT RE-DETECTED during resume cooldown — "
                    f"gate reset ({reason})"
                )
            # If already HALTED: no state change, just continue counting
        if _state_changed:
            try:
                from events.bus import event_bus
                from events import current_cycle
                from events.types import HaltStateChange

                event_bus.publish(
                    HaltStateChange(
                        cycle_id=current_cycle.id,
                        ticker=ticker,
                        new_state=HALT_HALTED,
                        reason=reason,
                    )
                )
            except Exception:  # noqa: BLE001
                pass

    def on_clean_tick(self, ticker: str) -> None:
        """
        Called by the engine each tick when health check is clean for *ticker*.
        Advances the resume gate counter and promotes HALTED→RESUMING→ACTIVE.

        No-op for tickers that were never halted — does not create state.
        """
        _new_state = None
        with self._lock:
            if ticker not in self._states:
                return  # never halted — nothing to track, don't create state
            s = self._states[ticker]
            if s.state == HALT_ACTIVE:
                return  # already fully recovered

            now_et = datetime.now(ET)
            s.clean_ticks += 1

            if s.state == HALT_HALTED and s.clean_ticks >= 1:
                # First clean tick after halt → enter RESUMING cooldown
                s.state = HALT_RESUMING
                s.resuming_started_at = now_et  # start cooldown clock here
                _new_state = HALT_RESUMING
                log.info(
                    f"[HaltMachine:{ticker}] RESUMING — "
                    f"{s.clean_ticks}/{self.CLEAN_TICKS_REQUIRED} clean ticks"
                )

            if s.state == HALT_RESUMING and s.clean_ticks >= self.CLEAN_TICKS_REQUIRED:
                # Enforce optional time-based cooldown — measured from RESUMING
                # start (not halt detection), so a long halt doesn't skip cooldown.
                cooldown_s = CONFIG.integrity_gate.halt_resume_cooldown_seconds
                resuming_since = s.resuming_started_at or now_et
                elapsed = (now_et - resuming_since).total_seconds()
                if cooldown_s > 0 and elapsed < cooldown_s:
                    log.debug(
                        f"[HaltMachine:{ticker}] resume gate: ticks satisfied but "
                        f"cooldown not elapsed ({elapsed:.0f}s / {cooldown_s}s since RESUMING)"
                    )
                    _new_state = None  # not actually transitioning yet
                else:
                    s.state = HALT_ACTIVE
                    s.resumed_at = now_et
                    s.clean_ticks = 0
                    s.post_halt_entry_complete = False  # arm size-down for first entry
                    _new_state = HALT_ACTIVE
                    log.info(
                        f"[HaltMachine:{ticker}] ACTIVE — halt cleared after "
                        f"{self.CLEAN_TICKS_REQUIRED} clean ticks"
                    )
        if _new_state is not None:
            try:
                from events.bus import event_bus
                from events import current_cycle
                from events.types import HaltStateChange

                event_bus.publish(
                    HaltStateChange(
                        cycle_id=current_cycle.id,
                        ticker=ticker,
                        new_state=_new_state,
                        reason=f"clean_tick transition → {_new_state}",
                    )
                )
            except Exception:  # noqa: BLE001
                pass

    def is_blocked(self, ticker: str) -> bool:
        """True when entries should be blocked (HALTED or RESUMING)."""
        with self._lock:
            s = self._states.get(ticker)
            if s is None:
                return False
            return s.state in (HALT_HALTED, HALT_RESUMING)

    def resume_spread_multiplier(self, ticker: str) -> float:
        """
        Returns a spread-tightening multiplier for *ticker*.

        1.0  = no change (ACTIVE + first post-halt entry already done).
        0.60 = tightened (RESUMING, OR ACTIVE but first post-halt print pending).

        The engine multiplies configured max_spread_pct by this value.
        """
        with self._lock:
            s = self._states.get(ticker)
            if s is None:
                return 1.0
            if s.state == HALT_RESUMING:
                return self.RESUME_SPREAD_MULT
            if s.state == HALT_ACTIVE and not s.post_halt_entry_complete:
                return self.RESUME_SPREAD_MULT
            return 1.0

    def resume_size_multiplier(self, ticker: str) -> float:
        """
        Returns a position-size multiplier for *ticker*.

        1.0 = normal size (no pending post-halt adjustment).
        halt_post_entry_size_mult (default 0.5) = half-size for the first
        entry after a halt fully clears (ACTIVE but first print not yet made).
        """
        with self._lock:
            s = self._states.get(ticker)
            if s is None or s.state != HALT_ACTIVE:
                return 1.0
            if not s.post_halt_entry_complete:
                return CONFIG.integrity_gate.halt_post_entry_size_mult
            return 1.0

    def on_post_halt_entry(self, ticker: str) -> None:
        """
        Call this after the first order is submitted for *ticker* following a
        halt recovery.  Clears the post-halt size/spread adjustment so
        subsequent entries use normal parameters.
        """
        with self._lock:
            s = self._states.get(ticker)
            if s and s.state == HALT_ACTIVE and not s.post_halt_entry_complete:
                s.post_halt_entry_complete = True
                log.info(
                    f"[HaltMachine:{ticker}] post-halt first entry recorded — "
                    f"size/spread back to normal"
                )

    def current_state(self, ticker: str) -> str:
        """Returns the current halt state string for *ticker*."""
        with self._lock:
            s = self._states.get(ticker)
            return s.state if s else HALT_ACTIVE

    def reset(self, ticker: str) -> None:
        """Clear halt state for *ticker* (call at EOD or new session)."""
        with self._lock:
            self._states.pop(ticker, None)

    def reset_all(self) -> None:
        """Clear all ticker halt states (call at start of new session)."""
        with self._lock:
            self._states.clear()
        log.info("[HaltMachine] All ticker halt states cleared for new session")

    def status_summary(self) -> Dict[str, str]:
        """Returns {ticker: state} snapshot for diagnostics."""
        with self._lock:
            return {t: s.state for t, s in self._states.items()}

    # ── Internals ─────────────────────────────────────────────────────────────

    def _get_or_create(self, ticker: str) -> _TickerHaltState:
        if ticker not in self._states:
            self._states[ticker] = _TickerHaltState()
        return self._states[ticker]


# Module-level singleton
halt_machine = HaltStateMachine()
