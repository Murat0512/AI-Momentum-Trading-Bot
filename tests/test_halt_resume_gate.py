"""
tests/test_halt_resume_gate.py — Unit tests for the extended HaltStateMachine
                                  resume gate (halt_resume_cooldown_seconds,
                                  post-halt size/spread adjustments).

Covers:
  Baseline blocking
    1.  HALTED state blocks entries
    2.  RESUMING state blocks entries
    3.  ACTIVE state allows entries

  Tick gate
    4.  CLEAN_TICKS_REQUIRED ticks required before RESUMING → ACTIVE
    5.  Fewer than CLEAN_TICKS_REQUIRED ticks keeps state in RESUMING

  Time-based cooldown gate (halt_resume_cooldown_seconds)
    6.  When cooldown=0 the tick gate alone governs transition
    7.  Cooldown > 0 keeps state in RESUMING even after enough clean ticks
    8.  State transitions ACTIVE after cooldown + ticks both satisfied

  Spread gate
    9.  resume_spread_multiplier = RESUME_SPREAD_MULT during RESUMING
    10. resume_spread_multiplier = RESUME_SPREAD_MULT after resume
        until on_post_halt_entry() called
    11. resume_spread_multiplier = 1.0 after on_post_halt_entry()
    12. resume_spread_multiplier = 1.0 for never-halted ticker

  Size gate
    13. resume_size_multiplier = halt_post_entry_size_mult after resume
        until on_post_halt_entry() called
    14. resume_size_multiplier = 1.0 after on_post_halt_entry()
    15. resume_size_multiplier = 1.0 for never-halted ticker
    16. resume_size_multiplier = 1.0 while still in HALTED / RESUMING

  on_post_halt_entry
    17. Calling on_post_halt_entry normalises both spread and size
    18. Calling on_post_halt_entry when already normalised is a no-op

  Re-halt during resume
    19. Halt re-detected during RESUMING resets the gate to HALTED
    20. Re-halted ticker requires another N clean ticks before ACTIVE

  Reset
    21. reset() clears all per-ticker state
    22. After reset resuming is unblocked and multipliers are 1.0
"""

from __future__ import annotations

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta
from unittest.mock import patch
import pytest
import pytz

from config.constants import BLOCK_HALT_DETECTED
from config.settings import CONFIG
from execution.halt_machine import (
    HaltStateMachine,
    HALT_ACTIVE,
    HALT_HALTED,
    HALT_RESUMING,
)

ET = pytz.timezone("America/New_York")


def _now() -> datetime:
    return datetime.now(ET)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _halt(hm: HaltStateMachine, ticker: str = "TSLA") -> None:
    hm.on_health_block(ticker, BLOCK_HALT_DETECTED)


def _clean_ticks(hm: HaltStateMachine, n: int, ticker: str = "TSLA") -> None:
    for _ in range(n):
        hm.on_clean_tick(ticker)


def _full_resume(hm: HaltStateMachine, ticker: str = "TSLA") -> None:
    """Send the minimum required clean ticks to reach ACTIVE."""
    _clean_ticks(hm, HaltStateMachine.CLEAN_TICKS_REQUIRED, ticker)


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestHaltResumeGate:
    def setup_method(self):
        self.hm = HaltStateMachine()

    # ── Baseline blocking ────────────────────────────────────────────────────

    def test_halted_blocks_entry(self):
        _halt(self.hm)
        assert self.hm.is_blocked("TSLA") is True
        assert self.hm.current_state("TSLA") == HALT_HALTED

    def test_resuming_blocks_entry(self):
        _halt(self.hm)
        self.hm.on_clean_tick("TSLA")  # 1 tick → RESUMING
        assert self.hm.is_blocked("TSLA") is True
        assert self.hm.current_state("TSLA") == HALT_RESUMING

    def test_active_allows_entry(self):
        assert self.hm.is_blocked("TSLA") is False

    # ── Tick gate ─────────────────────────────────────────────────────────────

    def test_required_clean_ticks_transitions_to_active(self):
        _halt(self.hm)
        _full_resume(self.hm)
        assert self.hm.current_state("TSLA") == HALT_ACTIVE
        assert self.hm.is_blocked("TSLA") is False

    def test_fewer_than_required_ticks_stays_resuming(self):
        _halt(self.hm)
        # Only 2 ticks when 3 are required
        _clean_ticks(self.hm, HaltStateMachine.CLEAN_TICKS_REQUIRED - 1)
        assert self.hm.current_state("TSLA") == HALT_RESUMING

    # ── Time-based cooldown gate ─────────────────────────────────────────────

    def test_zero_cooldown_uses_tick_gate_only(self):
        """halt_resume_cooldown_seconds=0 means tick gate governs alone."""
        with patch.object(CONFIG.integrity_gate, "halt_resume_cooldown_seconds", 0):
            _halt(self.hm)
            _full_resume(self.hm)
        assert self.hm.current_state("TSLA") == HALT_ACTIVE

    def test_active_cooldown_keeps_resuming_state(self):
        """With cooldown=300s, state stays RESUMING after enough ticks if time not elapsed."""
        with patch.object(CONFIG.integrity_gate, "halt_resume_cooldown_seconds", 300):
            _halt(self.hm)
            _clean_ticks(self.hm, 1)      # HALTED → RESUMING (sets resuming_started_at=now)
            # resuming_started_at is "just now" so elapsed ≈ 0 < 300 — stays RESUMING
            _clean_ticks(self.hm, HaltStateMachine.CLEAN_TICKS_REQUIRED)
        assert self.hm.current_state("TSLA") == HALT_RESUMING

    def test_active_after_cooldown_and_ticks(self):
        """After cooldown elapsed AND required ticks, transitions to ACTIVE."""
        t_old = _now() - timedelta(seconds=600)  # 10 min ago
        with patch.object(CONFIG.integrity_gate, "halt_resume_cooldown_seconds", 300):
            _halt(self.hm)
            _clean_ticks(self.hm, 1)  # HALTED → RESUMING (sets resuming_started_at=now)
            # Backdate resuming_started_at so cooldown is satisfied
            self.hm._states["TSLA"].resuming_started_at = t_old
            _clean_ticks(self.hm, HaltStateMachine.CLEAN_TICKS_REQUIRED)
        assert self.hm.current_state("TSLA") == HALT_ACTIVE

    # ── Spread gate ───────────────────────────────────────────────────────────

    def test_spread_multiplier_tight_during_resuming(self):
        _halt(self.hm)
        self.hm.on_clean_tick("TSLA")  # → RESUMING
        mult = self.hm.resume_spread_multiplier("TSLA")
        assert mult == pytest.approx(HaltStateMachine.RESUME_SPREAD_MULT)
        assert mult < 1.0

    def test_spread_multiplier_tight_after_resume_before_first_entry(self):
        """Spread stays tight while ACTIVE + post_halt_entry_complete=False."""
        _halt(self.hm)
        _full_resume(self.hm)
        assert self.hm.current_state("TSLA") == HALT_ACTIVE
        mult = self.hm.resume_spread_multiplier("TSLA")
        assert mult == pytest.approx(HaltStateMachine.RESUME_SPREAD_MULT)

    def test_spread_normalises_after_post_halt_entry(self):
        _halt(self.hm)
        _full_resume(self.hm)
        self.hm.on_post_halt_entry("TSLA")
        assert self.hm.resume_spread_multiplier("TSLA") == pytest.approx(1.0)

    def test_spread_multiplier_one_for_never_halted(self):
        assert self.hm.resume_spread_multiplier("AMZN") == pytest.approx(1.0)

    # ── Size gate ─────────────────────────────────────────────────────────────

    def test_size_multiplier_reduced_after_resume(self):
        _halt(self.hm)
        _full_resume(self.hm)
        size_mult = self.hm.resume_size_multiplier("TSLA")
        expected  = CONFIG.integrity_gate.halt_post_entry_size_mult
        assert size_mult == pytest.approx(expected)
        assert size_mult < 1.0

    def test_size_multiplier_normalises_after_post_halt_entry(self):
        _halt(self.hm)
        _full_resume(self.hm)
        self.hm.on_post_halt_entry("TSLA")
        assert self.hm.resume_size_multiplier("TSLA") == pytest.approx(1.0)

    def test_size_multiplier_one_for_never_halted(self):
        assert self.hm.resume_size_multiplier("NVDA") == pytest.approx(1.0)

    def test_size_multiplier_one_while_halted(self):
        """Size-down only applies AFTER ticker is ACTIVE, not during HALTED."""
        _halt(self.hm)
        assert self.hm.resume_size_multiplier("TSLA") == pytest.approx(1.0)

    def test_size_multiplier_one_while_resuming(self):
        """Size-down only applies AFTER ticker is ACTIVE, not during RESUMING."""
        _halt(self.hm)
        self.hm.on_clean_tick("TSLA")  # → RESUMING
        assert self.hm.resume_size_multiplier("TSLA") == pytest.approx(1.0)

    # ── on_post_halt_entry ────────────────────────────────────────────────────

    def test_on_post_halt_entry_normalises_both_multipliers(self):
        _halt(self.hm)
        _full_resume(self.hm)
        self.hm.on_post_halt_entry("TSLA")
        assert self.hm.resume_spread_multiplier("TSLA") == pytest.approx(1.0)
        assert self.hm.resume_size_multiplier("TSLA")   == pytest.approx(1.0)

    def test_on_post_halt_entry_noop_when_already_complete(self):
        """Calling on_post_halt_entry twice should not raise or change state."""
        _halt(self.hm)
        _full_resume(self.hm)
        self.hm.on_post_halt_entry("TSLA")
        self.hm.on_post_halt_entry("TSLA")  # should be a no-op
        assert self.hm.resume_spread_multiplier("TSLA") == pytest.approx(1.0)

    def test_on_post_halt_entry_noop_for_never_halted(self):
        """Call on a ticker that was never halted — should be a no-op."""
        self.hm.on_post_halt_entry("AAPL")  # should not raise
        assert self.hm.resume_spread_multiplier("AAPL") == pytest.approx(1.0)

    # ── Re-halt during resume ─────────────────────────────────────────────────

    def test_rehalt_during_resuming_resets_to_halted(self):
        _halt(self.hm)
        self.hm.on_clean_tick("TSLA")  # → RESUMING
        _halt(self.hm)                  # re-detected
        assert self.hm.current_state("TSLA") == HALT_HALTED

    def test_rehalt_requires_fresh_tick_cycle(self):
        _halt(self.hm)
        self.hm.on_clean_tick("TSLA")  # → RESUMING
        _halt(self.hm)                  # back to HALTED
        # After re-halt, must still send required clean ticks from scratch
        _full_resume(self.hm)
        assert self.hm.current_state("TSLA") == HALT_ACTIVE

    # ── Reset ────────────────────────────────────────────────────────────────

    def test_reset_clears_ticker_state_completely(self):
        _halt(self.hm)
        self.hm.reset("TSLA")
        assert self.hm.current_state("TSLA") == HALT_ACTIVE
        assert self.hm.is_blocked("TSLA") is False
        assert self.hm.resume_spread_multiplier("TSLA") == pytest.approx(1.0)
        assert self.hm.resume_size_multiplier("TSLA")   == pytest.approx(1.0)

    def test_reset_all_clears_multiple_tickers(self):
        for t in ("TSLA", "AAPL", "NVDA"):
            self.hm.on_health_block(t, BLOCK_HALT_DETECTED)
        self.hm.reset_all()
        for t in ("TSLA", "AAPL", "NVDA"):
            assert self.hm.is_blocked(t) is False


# ─────────────────────────────────────────────────────────────────────────────
# Edge-case / config
# ─────────────────────────────────────────────────────────────────────────────

class TestHaltResumeConfig:
    def test_halt_post_entry_size_mult_default(self):
        assert CONFIG.integrity_gate.halt_post_entry_size_mult == pytest.approx(0.5)

    def test_halt_resume_cooldown_seconds_default(self):
        assert CONFIG.integrity_gate.halt_resume_cooldown_seconds == 0

    def test_clean_ticks_required_constant(self):
        assert HaltStateMachine.CLEAN_TICKS_REQUIRED == 3

    def test_resume_spread_mult_constant(self):
        assert HaltStateMachine.RESUME_SPREAD_MULT == pytest.approx(0.60)
