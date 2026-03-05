"""
tests/test_lifecycle.py — Acceptance tests for execution.lifecycle

LifecycleManager evaluates the deterministic state machine each tick
and emits LifecycleEvents for the engine to execute.

State machine tested:
    ENTRY → PARTIAL1 (+1R: partial sell intent + stop → breakeven)
    PARTIAL1 → PARTIAL2 (sell 50% at +1.5R)
    PARTIAL2 → TRAILING (trail stop at +2R)
    TRAILING → ratchet (new high raises trail stop)
    Any state → STOP_HIT
    TRAILING → VWAP_EXIT
    TRAILING → VOLUME_FADE_EXIT
    ENTRY/PARTIAL1 → TIME_STOP (no continuation)
    Any → TIME_STOP (session end)
    PARTIAL2/TRAILING → TARGET_HIT
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta

import pandas as pd
import pytest
import pytz

from config.constants import (
    LIFECYCLE_ENTRY,
    LIFECYCLE_PARTIAL1,
    LIFECYCLE_PARTIAL2,
    LIFECYCLE_SQUEEZE,
    LIFECYCLE_TRAILING,
    LIFECYCLE_CLOSED,
    LIFECYCLE_EVT_BREAKEVEN,
    LIFECYCLE_EVT_PARTIAL_SELL,
    LIFECYCLE_EVT_TRAIL_START,
    LIFECYCLE_EVT_STOP_HIT,
    LIFECYCLE_EVT_TARGET_HIT,
    LIFECYCLE_EVT_TIME_STOP,
    LIFECYCLE_EVT_VWAP_EXIT,
    LIFECYCLE_EVT_VOLUME_FADE,
)
from config.settings import CONFIG
from execution.lifecycle import LifecycleManager, LifecycleEvent

ET = pytz.timezone("America/New_York")


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────


def _now_mid_session() -> datetime:
    """Return a datetime safely in the middle of the RTH session."""
    return datetime.now(ET).replace(hour=11, minute=0, second=0, microsecond=0)


def _make_trade(
    entry: float = 100.0,
    stop: float = 99.0,  # 1R = $1.00
    shares: int = 100,
    lifecycle: str = LIFECYCLE_ENTRY,
    shares_remaining: int = 0,  # 0 → uses shares
    entry_time: datetime = None,
    regime: str = "TREND",
):
    """Create a minimal mock TradeRecord for lifecycle testing."""

    class FakeTrade:
        pass

    original_stop = stop  # preserve for initial_risk

    t = FakeTrade()
    t.trade_id = f"TEST_{entry:.0f}"
    t.ticker = "AAPL"
    t.entry_price = entry
    t.stop_price = stop
    t.initial_stop_price = original_stop  # never changes
    t.target_price = entry + (entry - stop) * CONFIG.lifecycle.hard_target_r
    t.shares = shares
    t.shares_remaining = shares_remaining if shares_remaining else shares
    t.lifecycle_state = lifecycle
    t.high_watermark = entry
    t.trail_active = False
    t.entry_time = entry_time or _now_mid_session()
    t.regime = regime

    @property
    def risk_per_share(self):
        return abs(self.entry_price - self.stop_price)

    FakeTrade.risk_per_share = risk_per_share
    return t


def _eval(
    trade,
    price: float,
    vwap: float = 0.0,
    atr: float = 0.50,
    volume: float = 0.0,
    avg_volume: float = 0.0,
    now: datetime = None,
):
    """Single-trade convenience wrapper around LifecycleManager."""
    mgr = LifecycleManager()
    events = mgr.evaluate_all(
        open_trades=[trade],
        price_map={trade.ticker: price},
        vwap_map={trade.ticker: vwap} if vwap else {},
        atr_map={trade.ticker: atr},
        volume_map={trade.ticker: volume} if volume else {},
        avg_vol_map={trade.ticker: avg_volume} if avg_volume else {},
        now=now or _now_mid_session(),
    )
    return events[0] if events else None


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY → PARTIAL1 (breakeven stop adjustment)
# ─────────────────────────────────────────────────────────────────────────────


class TestBreakevenTransition:
    def test_no_event_below_breakeven_r(self, monkeypatch):
        """At +0.5R (below +1R threshold) — no event emitted."""
        monkeypatch.setattr(CONFIG.lifecycle, "breakeven_r", 1.0)
        t = _make_trade(entry=100.0, stop=99.0)
        evt = _eval(t, price=100.50)  # +0.5R
        assert evt is None

    def test_breakeven_event_at_plus_1r(self, monkeypatch):
        """At +1R: PARTIAL_SELL intent emitted; stop moves to entry price."""
        monkeypatch.setattr(CONFIG.lifecycle, "breakeven_r", 1.0)
        monkeypatch.setattr(CONFIG.lifecycle, "partial1_exit_pct", 0.25)
        t = _make_trade(entry=100.0, stop=99.0)
        evt = _eval(t, price=101.10)  # past +1R (high_watermark updated)
        assert evt is not None
        assert evt.event == LIFECYCLE_EVT_PARTIAL_SELL
        assert evt.new_lifecycle_state == LIFECYCLE_PARTIAL1
        assert evt.shares_to_sell == 25
        assert evt.is_close == False
        assert t.stop_price == pytest.approx(100.0)  # breakeven

    def test_partial1_state_set_on_trade(self, monkeypatch):
        """TradeRecord.lifecycle_state updated to PARTIAL1 in-place."""
        monkeypatch.setattr(CONFIG.lifecycle, "breakeven_r", 1.0)
        t = _make_trade(entry=100.0, stop=99.0)
        _eval(t, price=101.20)
        assert t.lifecycle_state == LIFECYCLE_PARTIAL1

    def test_breakeven_not_retriggered(self, monkeypatch):
        """Already in PARTIAL1 — breakeven event NOT emitted again."""
        monkeypatch.setattr(CONFIG.lifecycle, "breakeven_r", 1.0)
        monkeypatch.setattr(CONFIG.lifecycle, "partial_exit_r", 1.5)
        t = _make_trade(entry=100.0, stop=99.0, lifecycle=LIFECYCLE_PARTIAL1)
        t.stop_price = 100.0  # already at breakeven
        t.high_watermark = 101.20
        evt = _eval(t, price=101.10)  # at +1R but below partial_exit_r
        # Should be None (no advance, no stop hit)
        assert evt is None or evt.event != LIFECYCLE_EVT_BREAKEVEN


# ─────────────────────────────────────────────────────────────────────────────
# PARTIAL1 → PARTIAL2 (50% sell at +1.5R)
# ─────────────────────────────────────────────────────────────────────────────


class TestPartialSell:
    def test_partial_sell_at_1_5r(self, monkeypatch):
        """At +1.5R: PARTIAL_SELL emitted; 50% of shares sold."""
        monkeypatch.setattr(CONFIG.lifecycle, "partial_exit_r", 1.5)
        monkeypatch.setattr(CONFIG.lifecycle, "partial_exit_pct", 0.50)
        monkeypatch.setattr(CONFIG.lifecycle, "trail_start_r", 2.0)
        t = _make_trade(
            entry=100.0, stop=99.0, lifecycle=LIFECYCLE_PARTIAL1, shares=100
        )
        t.stop_price = 100.0  # breakeven already set
        t.high_watermark = 100.0
        evt = _eval(t, price=101.60)  # +1.6R
        assert evt is not None
        assert evt.event == LIFECYCLE_EVT_PARTIAL_SELL
        assert evt.new_lifecycle_state == LIFECYCLE_PARTIAL2
        assert evt.shares_to_sell == 50  # 50% of 100
        assert evt.is_close == False

    def test_shares_remaining_not_mutated_on_partial_intent(self, monkeypatch):
        """Lifecycle intent event must not decrement shares before fill confirmation."""
        monkeypatch.setattr(CONFIG.lifecycle, "partial_exit_r", 1.5)
        monkeypatch.setattr(CONFIG.lifecycle, "partial_exit_pct", 0.50)
        monkeypatch.setattr(CONFIG.lifecycle, "trail_start_r", 2.0)
        t = _make_trade(
            entry=100.0, stop=99.0, lifecycle=LIFECYCLE_PARTIAL1, shares=100
        )
        t.stop_price = 100.0
        t.high_watermark = 100.0
        _eval(t, price=101.60)
        assert t.shares_remaining == 100
        assert t.lifecycle_state == LIFECYCLE_PARTIAL1

    def test_partial_sell_odd_shares(self, monkeypatch):
        """With 99 shares: intent quantity is deterministic floor(99 × 0.5) = 49."""
        monkeypatch.setattr(CONFIG.lifecycle, "partial_exit_r", 1.5)
        monkeypatch.setattr(CONFIG.lifecycle, "partial_exit_pct", 0.50)
        monkeypatch.setattr(CONFIG.lifecycle, "trail_start_r", 3.0)
        t = _make_trade(entry=100.0, stop=99.0, lifecycle=LIFECYCLE_PARTIAL1, shares=99)
        t.stop_price = 100.0
        t.high_watermark = 100.0
        evt = _eval(t, price=101.60)
        assert evt.shares_to_sell == 49
        assert t.shares_remaining == 99


# ─────────────────────────────────────────────────────────────────────────────
# PARTIAL2 → TRAILING (trail activation at +2R)
# ─────────────────────────────────────────────────────────────────────────────


class TestTrailActivation:
    def test_trail_starts_at_2r(self, monkeypatch):
        """At +2R in PARTIAL2: TRAIL_STARTED emitted with ATR-based stop."""
        monkeypatch.setattr(CONFIG.lifecycle, "trail_start_r", 2.0)
        monkeypatch.setattr(CONFIG.lifecycle, "trail_atr_multiplier", 1.0)
        t = _make_trade(entry=100.0, stop=99.0, lifecycle=LIFECYCLE_PARTIAL2)
        t.stop_price = 100.0  # already at breakeven
        t.shares_remaining = 50
        t.high_watermark = 100.0
        # price at +2R = 102.0; ATR=0.5; trail_stop = 102.0 - 0.5×1.0 = 101.5
        evt = _eval(t, price=102.10, atr=0.50)
        assert evt is not None
        assert evt.event == LIFECYCLE_EVT_TRAIL_START
        assert evt.new_lifecycle_state == LIFECYCLE_TRAILING
        assert evt.new_stop == pytest.approx(102.10 - 0.50, abs=0.02)
        assert t.trail_active == True

    def test_trail_stop_never_below_breakeven(self, monkeypatch):
        """Trail stop is clamped to at least breakeven."""
        monkeypatch.setattr(CONFIG.lifecycle, "trail_start_r", 2.0)
        monkeypatch.setattr(
            CONFIG.lifecycle, "trail_atr_multiplier", 5.0
        )  # huge ATR mult
        t = _make_trade(entry=100.0, stop=99.0, lifecycle=LIFECYCLE_PARTIAL2)
        t.stop_price = 100.0  # breakeven
        t.shares_remaining = 50
        t.high_watermark = 100.0
        evt = _eval(t, price=102.10, atr=10.0)  # would push trail below entry
        assert evt is not None
        assert evt.new_stop >= 100.0


# ─────────────────────────────────────────────────────────────────────────────
# TRAILING: stop ratchet
# ─────────────────────────────────────────────────────────────────────────────


class TestTrailingRatchet:
    def _make_trailing_trade(self, stop_at: float = 101.0):
        t = _make_trade(entry=100.0, stop=99.0, lifecycle=LIFECYCLE_TRAILING)
        t.stop_price = stop_at
        t.trail_active = True
        t.shares_remaining = 50
        t.high_watermark = 102.0
        return t

    def test_ratchet_on_new_high(self, monkeypatch):
        """New price high pushes trail stop up."""
        monkeypatch.setattr(CONFIG.lifecycle, "trail_atr_multiplier", 1.0)
        t = self._make_trailing_trade(stop_at=101.50)
        t.high_watermark = 102.0
        # price = 103.0 — new high; atr=0.50; new trail = 103.0 - 0.50 = 102.5 > 101.5 ✓
        evt = _eval(t, price=103.0, atr=0.50)
        assert evt is not None
        assert evt.event == LIFECYCLE_EVT_TRAIL_START  # ratchet reuses this tag
        assert evt.new_stop == pytest.approx(103.0 - 0.50, abs=0.02)

    def test_no_ratchet_when_price_below_current_trail(self, monkeypatch):
        """Price below current trail stop → STOP_HIT, not ratchet."""
        monkeypatch.setattr(CONFIG.lifecycle, "trail_atr_multiplier", 0.5)
        t = self._make_trailing_trade(stop_at=101.50)
        t.high_watermark = 102.0
        evt = _eval(t, price=101.40)  # below stop
        assert evt is not None
        assert evt.event == LIFECYCLE_EVT_STOP_HIT
        assert evt.is_close == True

    def test_volatility_crush_stop_clamping(self, monkeypatch):
        """Stop never decreases across ATR regime shifts (crush/spike)."""
        monkeypatch.setattr(CONFIG.lifecycle, "trail_atr_multiplier", 1.0)
        t = self._make_trailing_trade(stop_at=101.00)
        t.high_watermark = 103.00

        # First update with elevated ATR
        evt1 = _eval(t, price=103.00, atr=1.20)
        assert evt1 is not None
        assert evt1.event == LIFECYCLE_EVT_TRAIL_START
        stop_after_high_atr = t.stop_price

        # Volatility crush should only tighten/raise stop, never lower it.
        evt2 = _eval(t, price=103.00, atr=0.20)
        assert evt2 is not None
        assert evt2.event == LIFECYCLE_EVT_TRAIL_START
        stop_after_crush = t.stop_price
        assert stop_after_crush >= stop_after_high_atr

        # ATR spike afterwards must not reduce the already tightened stop.
        evt3 = _eval(t, price=103.00, atr=2.00)
        assert evt3 is None
        assert t.stop_price >= stop_after_crush


# ─────────────────────────────────────────────────────────────────────────────
# Stop hit (all states)
# ─────────────────────────────────────────────────────────────────────────────


class TestStopHit:
    @pytest.mark.parametrize(
        "state",
        [LIFECYCLE_ENTRY, LIFECYCLE_PARTIAL1, LIFECYCLE_PARTIAL2, LIFECYCLE_TRAILING],
    )
    def test_stop_hit_in_any_state(self, state, monkeypatch):
        """Stop hit triggers STOP_HIT close regardless of lifecycle state."""
        monkeypatch.setattr(CONFIG.lifecycle, "breakeven_r", 1.0)
        t = _make_trade(entry=100.0, stop=99.0, lifecycle=state)
        t.shares_remaining = (
            60 if state in (LIFECYCLE_PARTIAL2, LIFECYCLE_TRAILING) else 100
        )
        evt = _eval(t, price=98.90)  # below stop
        assert evt is not None
        assert evt.event == LIFECYCLE_EVT_STOP_HIT
        assert evt.is_close == True
        assert evt.shares_to_sell == t.shares_remaining

    def test_stop_hit_uses_shares_remaining(self):
        """STOP_HIT close uses shares_remaining, not original shares."""
        t = _make_trade(
            entry=100.0, stop=99.0, lifecycle=LIFECYCLE_PARTIAL2, shares=100
        )
        t.shares_remaining = 40
        evt = _eval(t, price=98.0)
        assert evt.shares_to_sell == 40


# ─────────────────────────────────────────────────────────────────────────────
# VWAP exit (TRAILING only)
# ─────────────────────────────────────────────────────────────────────────────


class TestVwapExit:
    def test_vwap_exit_when_price_below_vwap(self, monkeypatch):
        """In TRAILING: price falls below VWAP → VWAP_EXIT close."""
        monkeypatch.setattr(CONFIG.lifecycle, "exit_on_vwap_loss", True)
        t = _make_trade(entry=100.0, stop=99.0, lifecycle=LIFECYCLE_TRAILING)
        t.stop_price = 100.50
        t.trail_active = True
        t.shares_remaining = 50
        t.high_watermark = 102.0
        evt = _eval(t, price=101.80, vwap=102.00)  # price < vwap
        assert evt is not None
        assert evt.event == LIFECYCLE_EVT_VWAP_EXIT
        assert evt.is_close == True

    def test_no_vwap_exit_when_disabled(self, monkeypatch):
        """VWAP exit disabled → no VWAP_EXIT event even if price < vwap."""
        monkeypatch.setattr(CONFIG.lifecycle, "exit_on_vwap_loss", False)
        t = _make_trade(entry=100.0, stop=99.0, lifecycle=LIFECYCLE_TRAILING)
        t.stop_price = 100.50
        t.trail_active = True
        t.shares_remaining = 50
        t.high_watermark = 102.5
        evt = _eval(t, price=101.80, vwap=102.00)
        # No vwap exit; might be None or ratchet
        if evt is not None:
            assert evt.event != LIFECYCLE_EVT_VWAP_EXIT

    def test_vwap_exit_not_triggered_in_entry_state(self, monkeypatch):
        """VWAP exit only applies in TRAILING — not in ENTRY."""
        monkeypatch.setattr(CONFIG.lifecycle, "exit_on_vwap_loss", True)
        monkeypatch.setattr(CONFIG.lifecycle, "breakeven_r", 1.0)
        t = _make_trade(entry=100.0, stop=99.0, lifecycle=LIFECYCLE_ENTRY)
        # price above stop, above entry, but below vwap
        evt = _eval(t, price=100.40, vwap=100.50)
        if evt is not None:
            assert evt.event != LIFECYCLE_EVT_VWAP_EXIT


# ─────────────────────────────────────────────────────────────────────────────
# Volume fade exit (TRAILING only)
# ─────────────────────────────────────────────────────────────────────────────


class TestVolumeFadeExit:
    def test_volume_fade_exit_when_volume_collapses(self, monkeypatch):
        """In TRAILING: volume < 40% of avg → VOLUME_FADE_EXIT."""
        monkeypatch.setattr(CONFIG.lifecycle, "volume_fade_threshold", 0.40)
        t = _make_trade(entry=100.0, stop=99.0, lifecycle=LIFECYCLE_TRAILING)
        t.stop_price = 100.50
        t.trail_active = True
        t.shares_remaining = 50
        t.high_watermark = 102.0
        evt = _eval(t, price=101.80, volume=1000, avg_volume=5000)  # 20% < 40%
        assert evt is not None
        assert evt.event == LIFECYCLE_EVT_VOLUME_FADE
        assert evt.is_close == True

    def test_no_volume_fade_when_volume_healthy(self, monkeypatch):
        """Volume above threshold → no fade exit."""
        monkeypatch.setattr(CONFIG.lifecycle, "volume_fade_threshold", 0.40)
        t = _make_trade(entry=100.0, stop=99.0, lifecycle=LIFECYCLE_TRAILING)
        t.stop_price = 100.50
        t.trail_active = True
        t.shares_remaining = 50
        t.high_watermark = 102.5
        evt = _eval(t, price=101.80, volume=4000, avg_volume=5000)  # 80% > 40%
        if evt is not None:
            assert evt.event != LIFECYCLE_EVT_VOLUME_FADE


# ─────────────────────────────────────────────────────────────────────────────
# Hard target hit (PARTIAL2 / TRAILING)
# ─────────────────────────────────────────────────────────────────────────────


class TestTargetHit:
    def test_target_hit_in_partial2(self, monkeypatch):
        """At hard_target_r in PARTIAL2: TARGET_HIT close."""
        monkeypatch.setattr(CONFIG.lifecycle, "hard_target_r", 4.0)
        monkeypatch.setattr(CONFIG.lifecycle, "trail_start_r", 2.0)
        t = _make_trade(entry=100.0, stop=99.0, lifecycle=LIFECYCLE_PARTIAL2)
        t.stop_price = 100.0
        t.shares_remaining = 50
        t.high_watermark = 100.0
        evt = _eval(t, price=104.10)  # past +4R hard target
        assert evt is not None
        assert evt.event == LIFECYCLE_EVT_TARGET_HIT
        assert evt.is_close == True

    def test_target_hit_in_trailing(self, monkeypatch):
        """At hard_target_r in TRAILING: TARGET_HIT close."""
        monkeypatch.setattr(CONFIG.lifecycle, "hard_target_r", 4.0)
        t = _make_trade(entry=100.0, stop=99.0, lifecycle=LIFECYCLE_TRAILING)
        t.stop_price = 102.0
        t.trail_active = True
        t.shares_remaining = 30
        t.high_watermark = 103.5
        evt = _eval(t, price=104.50)
        assert evt is not None
        assert evt.event == LIFECYCLE_EVT_TARGET_HIT
        assert evt.is_close == True

    def test_target_hit_behavior_when_squeeze_disabled(self, monkeypatch):
        """Fallback guarantee: when squeeze is off, +4R still hard-closes."""
        monkeypatch.setattr(CONFIG.lifecycle, "parabolic_squeeze_enabled", False)
        monkeypatch.setattr(CONFIG.lifecycle, "parabolic_target_r", 4.0)
        monkeypatch.setattr(CONFIG.lifecycle, "hard_target_r", 4.0)

        t = _make_trade(entry=100.0, stop=99.0, lifecycle=LIFECYCLE_TRAILING)
        t.stop_price = 102.0
        t.trail_active = True
        t.shares_remaining = 30
        t.high_watermark = 103.5

        evt = _eval(t, price=104.50)
        assert evt is not None
        assert evt.event == LIFECYCLE_EVT_TARGET_HIT
        assert evt.is_close is True
        assert evt.shares_to_sell == 30

    def test_parabolic_squeeze_transition(self, monkeypatch):
        """With squeeze enabled, +4R in TRAILING emits partial sell + tight stop."""
        monkeypatch.setattr(CONFIG.lifecycle, "parabolic_squeeze_enabled", True)
        monkeypatch.setattr(CONFIG.lifecycle, "parabolic_target_r", 4.0)
        monkeypatch.setattr(CONFIG.lifecycle, "hard_target_r", 4.0)

        t = _make_trade(entry=100.0, stop=99.0, lifecycle=LIFECYCLE_TRAILING)
        t.stop_price = 102.0
        t.trail_active = True
        t.shares_remaining = 30
        t.high_watermark = 103.5

        evt = _eval(t, price=104.50, atr=0.50)
        assert evt is not None
        assert evt.event == LIFECYCLE_EVT_PARTIAL_SELL
        assert evt.is_close == False
        assert evt.shares_to_sell == 15
        assert evt.new_lifecycle_state == LIFECYCLE_SQUEEZE
        assert evt.new_stop == pytest.approx(104.50 - 0.25, abs=0.02)

    def test_squeeze_uses_completed_1m_low_not_live_wick(self, monkeypatch):
        """Squeeze stop uses previous completed 1m low, not current in-flight wick."""
        monkeypatch.setattr(CONFIG.lifecycle, "parabolic_squeeze_enabled", True)
        monkeypatch.setattr(CONFIG.lifecycle, "parabolic_target_r", 4.0)
        monkeypatch.setattr(CONFIG.lifecycle, "hard_target_r", 4.0)

        import data.cache as cache_mod

        idx = pd.date_range(
            start=pd.Timestamp("2026-03-04 10:00", tz=ET), periods=3, freq="1min"
        )
        df_1m = pd.DataFrame(
            {
                "open": [104.2, 104.0, 104.1],
                "high": [104.6, 104.5, 104.4],
                "low": [103.8, 103.6, 101.0],
                "close": [104.4, 104.2, 104.3],
                "volume": [2000, 2200, 500],
            },
            index=idx,
        )
        monkeypatch.setattr(
            cache_mod.bar_cache, "get_tf", lambda *_args, **_kwargs: df_1m
        )

        t = _make_trade(entry=100.0, stop=99.0, lifecycle=LIFECYCLE_TRAILING)
        t.stop_price = 102.0
        t.trail_active = True
        t.shares_remaining = 30
        t.high_watermark = 104.5

        evt = _eval(t, price=104.50, atr=3.0)
        assert evt is not None
        assert evt.event == LIFECYCLE_EVT_PARTIAL_SELL
        assert evt.new_lifecycle_state == LIFECYCLE_SQUEEZE
        assert evt.new_stop == pytest.approx(103.6, abs=1e-6)


# ─────────────────────────────────────────────────────────────────────────────
# Time continuation stop
# ─────────────────────────────────────────────────────────────────────────────


class TestTimeContinuationStop:
    def test_time_stop_fires_when_no_new_high(self, monkeypatch):
        """After time_continuation_minutes with no new high → TIME_STOP."""
        monkeypatch.setattr(CONFIG.lifecycle, "time_continuation_minutes", 5)
        entry_time = _now_mid_session() - timedelta(minutes=5)
        t = _make_trade(
            entry=100.0, stop=99.0, lifecycle=LIFECYCLE_ENTRY, entry_time=entry_time
        )
        t.high_watermark = 100.0  # no new high since entry
        evt = _eval(t, price=100.00)
        assert evt is not None
        assert evt.event == LIFECYCLE_EVT_TIME_STOP
        assert evt.is_close == True

    def test_time_stop_not_before_5m_boundary(self, monkeypatch):
        """At T+4m59s with no new high → no continuation time stop yet."""
        monkeypatch.setattr(CONFIG.lifecycle, "time_continuation_minutes", 5)
        now = _now_mid_session()
        entry_time = now - timedelta(minutes=4, seconds=59)
        t = _make_trade(
            entry=100.0, stop=99.0, lifecycle=LIFECYCLE_ENTRY, entry_time=entry_time
        )
        t.high_watermark = 100.0
        evt = _eval(t, price=100.00, now=now)
        assert evt is None

    def test_no_time_stop_if_new_high_made(self, monkeypatch):
        """If a new high was made, time continuation stop does NOT fire."""
        monkeypatch.setattr(CONFIG.lifecycle, "time_continuation_minutes", 5)
        monkeypatch.setattr(CONFIG.lifecycle, "breakeven_r", 1.0)
        entry_time = _now_mid_session() - timedelta(minutes=6)
        t = _make_trade(
            entry=100.0, stop=99.0, lifecycle=LIFECYCLE_ENTRY, entry_time=entry_time
        )
        t.high_watermark = 101.50  # high was made — continuation OK
        evt = _eval(t, price=100.80)
        # Shouldn't be a time stop
        if evt is not None:
            assert evt.event != LIFECYCLE_EVT_TIME_STOP

    def test_time_stop_not_in_trailing(self, monkeypatch):
        """Time continuation stop does NOT fire in TRAILING state."""
        monkeypatch.setattr(CONFIG.lifecycle, "time_continuation_minutes", 5)
        entry_time = _now_mid_session() - timedelta(minutes=6)
        t = _make_trade(
            entry=100.0, stop=99.0, lifecycle=LIFECYCLE_TRAILING, entry_time=entry_time
        )
        t.stop_price = 100.50
        t.trail_active = True
        t.shares_remaining = 50
        t.high_watermark = 102.0
        evt = _eval(t, price=101.80, atr=0.20)
        if evt is not None:
            assert evt.event != LIFECYCLE_EVT_TIME_STOP


# ─────────────────────────────────────────────────────────────────────────────
# Session time stop
# ─────────────────────────────────────────────────────────────────────────────


class TestSessionTimeStop:
    def test_session_end_fires_time_stop(self, monkeypatch):
        """2 minutes before session end → TIME_STOP closes all remaining."""
        monkeypatch.setattr(CONFIG.risk, "time_stop_minutes_before_close", 15)
        t = _make_trade(entry=100.0, stop=99.0, lifecycle=LIFECYCLE_TRAILING)
        t.stop_price = 100.50
        t.trail_active = True
        t.shares_remaining = 40
        t.high_watermark = 102.0
        close_h, close_m = map(int, str(CONFIG.session.session_close).split(":"))
        close_dt = datetime.now(ET).replace(
            hour=close_h, minute=close_m, second=0, microsecond=0
        )
        now_in_window = close_dt - timedelta(minutes=10)
        evt = _eval(t, price=101.0, now=now_in_window)
        assert evt is not None
        assert evt.event == LIFECYCLE_EVT_TIME_STOP
        assert evt.is_close == True


# ─────────────────────────────────────────────────────────────────────────────
# Full sequence integration: ENTRY → PARTIAL1 → PARTIAL2 → TRAILING → CLOSED
# ─────────────────────────────────────────────────────────────────────────────


class TestFullLifecycleSequence:
    def test_full_state_machine_progression(self, monkeypatch):
        """
        Walk a trade through all lifecycle states and verify each event
        in sequence. Configuration:
            entry=100, stop=99 (1R=$1), 100 shares
            breakeven_r=1.0, partial_exit_r=1.5, trail_start_r=2.0
        """
        monkeypatch.setattr(CONFIG.lifecycle, "breakeven_r", 1.0)
        monkeypatch.setattr(CONFIG.lifecycle, "partial_exit_r", 1.5)
        monkeypatch.setattr(CONFIG.lifecycle, "partial_exit_pct", 0.50)
        monkeypatch.setattr(CONFIG.lifecycle, "trail_start_r", 2.0)
        monkeypatch.setattr(CONFIG.lifecycle, "trail_atr_multiplier", 1.0)
        monkeypatch.setattr(CONFIG.lifecycle, "hard_target_r", 4.0)
        monkeypatch.setattr(CONFIG.lifecycle, "exit_on_vwap_loss", False)
        monkeypatch.setattr(CONFIG.lifecycle, "time_continuation_minutes", 30)

        t = _make_trade(entry=100.0, stop=99.0, shares=100)

        # Step 1: +1R → BREAKEVEN
        evt = _eval(t, price=101.10, atr=0.50)
        assert evt.event == LIFECYCLE_EVT_PARTIAL_SELL
        assert t.lifecycle_state == LIFECYCLE_PARTIAL1
        assert t.stop_price == 100.0
        assert evt.shares_to_sell == 25

        # Step 2: +1.5R → PARTIAL_SELL 50%
        t.high_watermark = 101.10
        evt = _eval(t, price=101.60, atr=0.50)
        assert evt.event == LIFECYCLE_EVT_PARTIAL_SELL
        assert evt.shares_to_sell == 50
        assert t.lifecycle_state == LIFECYCLE_PARTIAL1
        assert t.shares_remaining == 100

        # Engine reconciliation on fill moves state and remaining shares.
        t.lifecycle_state = LIFECYCLE_PARTIAL2
        t.shares_remaining = 50
        assert t.shares_remaining == 50

        # Step 3: +2R → TRAIL_START
        t.high_watermark = 101.60
        evt = _eval(t, price=102.10, atr=0.50)
        assert evt.event == LIFECYCLE_EVT_TRAIL_START
        assert t.lifecycle_state == LIFECYCLE_TRAILING
        assert t.trail_active == True
        assert t.stop_price >= 100.0  # at least breakeven

        # Step 4: New high → trail ratchet
        t.high_watermark = 102.10
        evt = _eval(t, price=103.00, atr=0.50)
        assert evt.event == LIFECYCLE_EVT_TRAIL_START  # ratchet
        expected_stop = 103.00 - 0.50
        assert t.stop_price == pytest.approx(expected_stop, abs=0.02)

        # Step 5: Price drops to trail stop → STOP_HIT
        t.high_watermark = 103.00
        evt = _eval(t, price=t.stop_price - 0.01, atr=0.50)
        assert evt.event == LIFECYCLE_EVT_STOP_HIT
        assert evt.is_close == True
        assert evt.shares_to_sell == 50
