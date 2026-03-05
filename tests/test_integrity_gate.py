"""
tests/test_integrity_gate.py — Unit tests for execution/integrity_gate.py
"""
import pytest
from datetime import datetime, timedelta
import pytz

ET = pytz.timezone("America/New_York")

from config.constants import (
    INTEGRITY_OK, INTEGRITY_STREAM_GAP, INTEGRITY_CROSSED_MARKET,
    INTEGRITY_SPREAD_LOCK, INTEGRITY_BROKER_REJECT, INTEGRITY_DATA_DISCONNECT,
    INTEGRITY_FORCED_HALT,
)
from execution.integrity_gate import IntegrityGate


# ── Config save/restore fixture ───────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _reset_gate_cfg():
    """Save/restore CONFIG.integrity_gate around every test."""
    from config.settings import CONFIG
    c = CONFIG.integrity_gate
    saved = {
        'stream_gap_seconds':           c.stream_gap_seconds,
        'spread_lock_ticks':            c.spread_lock_ticks,
        'broker_reject_threshold':      c.broker_reject_threshold,
        'broker_reject_window_seconds': c.broker_reject_window_seconds,
        'hysteresis_ok_ticks':          c.hysteresis_ok_ticks,
    }
    yield
    for k, v in saved.items():
        setattr(c, k, v)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _cfg():
    from config.settings import CONFIG
    return CONFIG.integrity_gate


def make_gate(**overrides) -> IntegrityGate:
    """
    Return a fresh, reset IntegrityGate.
    Any kwargs are set on CONFIG.integrity_gate BEFORE the gate is created
    so that gate.check() picks them up when it reads CONFIG.integrity_gate.
    """
    c = _cfg()
    for k, v in overrides.items():
        setattr(c, k, v)
    g = IntegrityGate()
    g.reset()
    return g


def now():
    return datetime.now(ET)


def fresh_quote(bid=100.0, ask=100.05, last_bar_time=None):
    return dict(
        ticker="TEST",
        bid=bid,
        ask=ask,
        last_bar_time=last_bar_time or now(),
        now=now(),
    )


def _check(gate, **q):
    kw = {**fresh_quote(), **q}
    return gate.check(**kw)


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestIntegrityGateStreamGap:
    def test_recent_bar_passes(self):
        g = make_gate()
        clear, reason = _check(g, last_bar_time=now() - timedelta(seconds=30))
        assert clear, f"Expected clear gate, got: {reason}"

    def test_stale_bar_blocks(self):
        g = make_gate(stream_gap_seconds=60)
        stale = now() - timedelta(seconds=121)
        clear, reason = _check(g, last_bar_time=stale)
        assert not clear
        assert INTEGRITY_STREAM_GAP in reason

    def test_no_bar_ever_blocks(self):
        """last_bar_time far in the past -> stream gap fires."""
        g = make_gate(stream_gap_seconds=60)
        very_old = ET.localize(datetime(2000, 1, 1))
        clear, reason = _check(g, last_bar_time=very_old)
        assert not clear


class TestIntegrityGateCrossedMarket:
    def test_normal_spread_passes(self):
        g = make_gate()
        clear, _ = _check(g, bid=100.0, ask=100.05)
        assert clear

    def test_crossed_market_blocks(self):
        g = make_gate()
        clear, reason = _check(g, bid=100.10, ask=100.05)
        assert not clear
        assert INTEGRITY_CROSSED_MARKET in reason

    def test_zero_spread_is_crossed(self):
        g = make_gate()
        clear, reason = _check(g, bid=100.0, ask=100.0)
        assert not clear
        assert INTEGRITY_CROSSED_MARKET in reason


class TestIntegrityGateSpreadLock:
    def test_varying_spread_passes(self):
        # Vary spread each tick (bid fixed, ask increases) — should NOT trigger lock
        g = make_gate(spread_lock_ticks=4)
        for i in range(5):
            # spread = 0.05 + i*0.01 → all different
            _check(g, bid=100.0, ask=100.05 + i * 0.01)
        clear, _ = _check(g, bid=100.0, ask=100.10)
        assert clear

    def test_identical_spread_locked(self):
        g = make_gate(spread_lock_ticks=3)
        for _ in range(4):
            _check(g, bid=100.0, ask=100.05)
        clear, reason = _check(g, bid=100.0, ask=100.05)
        assert not clear
        assert INTEGRITY_SPREAD_LOCK in reason


class TestIntegrityGateBrokerRejectLoop:
    def test_no_rejects_passes(self):
        g = make_gate()
        clear, _ = _check(g)
        assert clear

    def test_too_many_rejects_blocks(self):
        g = make_gate(broker_reject_threshold=3, broker_reject_window_seconds=60)
        ts = now()
        for _ in range(3):
            g.record_reject(ts)
        clear, reason = _check(g)
        assert not clear
        assert INTEGRITY_BROKER_REJECT in reason

    def test_fill_resets_rejects(self):
        g = make_gate(broker_reject_threshold=2, hysteresis_ok_ticks=1)
        for _ in range(2):
            g.record_reject(now())
        # Trigger halt via check
        _check(g)
        # Now fill resets counter
        g.record_fill()
        # hysteresis_ok_ticks clean ticks to re-open
        for _ in range(_cfg().hysteresis_ok_ticks):
            _check(g)
        clear, _ = _check(g)
        assert clear


class TestIntegrityGateHysteresis:
    def test_force_halt_then_manual_clear(self):
        g = make_gate(hysteresis_ok_ticks=3)
        g.force_halt("test halt")
        assert g.is_halted()
        g.force_clear()   # manual clear -- bypass hysteresis countdown
        assert not g.is_halted()

    def test_force_halt_stays_closed(self):
        g = make_gate()
        g.force_halt("reconciler mismatch")
        clear, reason = _check(g)
        assert not clear
        assert INTEGRITY_FORCED_HALT in reason

    def test_reset_clears_everything(self):
        g = make_gate()
        g.force_halt("test")
        g.reset()
        assert not g.is_halted()

    def test_auto_clear_after_ok_ticks(self):
        g = make_gate(hysteresis_ok_ticks=2)
        g.force_halt("test halt")
        g.force_clear()   # manual clear -- no hysteresis needed
        assert not g.is_halted()


class TestIntegrityGateForceOperations:
    def test_force_halt_then_clear(self):
        g = make_gate()
        g.force_halt("deliberate halt")
        assert g.is_halted()
        g.force_clear()
        assert not g.is_halted()

    def test_multiple_halts_same_gate(self):
        g = make_gate()
        g.force_halt("reason 1")
        g.force_halt("reason 2")
        assert g.is_halted()   # still halted after second halt

    def test_record_reject_advances_counter(self):
        g = make_gate(broker_reject_threshold=5)
        for _ in range(4):
            g.record_reject(now())
        # Below threshold: other checks still pass
        clear, _ = _check(g)
        assert clear   # 4 < 5 threshold
        g.record_reject(now())
        clear2, reason2 = _check(g)
        assert not clear2
        assert INTEGRITY_BROKER_REJECT in reason2
