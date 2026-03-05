"""
execution/lifecycle.py — Trade Lifecycle Manager

Implements the deterministic partial-scaling + trailing state machine.

State machine:
    ENTRY     → stop at structural level; full shares open
    PARTIAL1  → +1R achieved: sell 25% and raise stop to breakeven
    PARTIAL2  → +1.5R achieved: sell 50% of remaining; stop stays at breakeven
    TRAILING  → +2R achieved: ATR trail active on remaining shares
    SQUEEZE   → optional +4R mode: partial + ultra-tight trail
    CLOSED    → fully exited (stop hit, time stop, VWAP loss, etc.)

Events emitted by evaluate_all():
    BREAKEVEN_ADJUSTED  — stop raised; engine updates trade.stop_price only
    PARTIAL_SELL        — sell N shares; engine calls broker.sell + updates shares_remaining
    TRAIL_STARTED       — trail flag set; no broker action
    STOP_HIT            — sell all remaining; engine closes trade
    TARGET_HIT          — sell all remaining; engine closes trade
    TIME_STOP           — sell all remaining; engine closes trade
    VWAP_EXIT           — sell all remaining; engine closes trade
    VOLUME_FADE_EXIT    — sell all remaining; engine closes trade

Usage:
    mgr = LifecycleManager()
    events = mgr.evaluate_all(open_trades, price_map, vwap_map, atr_map, volume_map, now)
    for evt in events:
        engine._handle_lifecycle_event(evt, trade)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

import pytz

from config.constants import (
    EPSILON,
    LIFECYCLE_CLOSED,
    LIFECYCLE_ENTRY,
    LIFECYCLE_PARTIAL1,
    LIFECYCLE_PARTIAL2,
    LIFECYCLE_SQUEEZE,
    LIFECYCLE_TRAILING,
    LIFECYCLE_EVT_BREAKEVEN,
    LIFECYCLE_EVT_PARTIAL_SELL,
    LIFECYCLE_EVT_STOP_HIT,
    LIFECYCLE_EVT_TARGET_HIT,
    LIFECYCLE_EVT_TIME_STOP,
    LIFECYCLE_EVT_TRAIL_START,
    LIFECYCLE_EVT_VWAP_EXIT,
    LIFECYCLE_EVT_VOLUME_FADE,
)
from config.settings import CONFIG

log = logging.getLogger(__name__)
ET = pytz.timezone("America/New_York")


# ─────────────────────────────────────────────────────────────────────────────
# LIFECYCLE EVENT
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class LifecycleEvent:
    """
    A single action the engine must execute for a trade this tick.

    For stop adjustments (breakeven, trail start):
        shares_to_sell = 0; new_stop > 0; is_close = False
    For partial sell:
        shares_to_sell > 0; new_stop = 0; is_close = False
    For full close events (stop hit, time stop, vwap exit, etc.):
        shares_to_sell = shares_remaining; is_close = True
    """

    trade_id: str
    ticker: str
    event: str  # LIFECYCLE_EVT_* constant
    shares_to_sell: int = 0  # 0 = no order required this event
    new_stop: float = 0.0  # 0 = no stop change
    new_lifecycle_state: str = ""  # "" = no state change
    is_close: bool = False  # True = trade should be fully closed
    reason: str = ""  # human-readable detail


# ─────────────────────────────────────────────────────────────────────────────
# LIFECYCLE MANAGER
# ─────────────────────────────────────────────────────────────────────────────


class LifecycleManager:
    """
    Stateless evaluator — call evaluate_all() every tick.
    All mutable state lives on TradeRecord itself (lifecycle_state,
    shares_remaining, stop_price, trail_active, high_watermark).
    """

    # ── Main entry point ──────────────────────────────────────────────────────

    def evaluate_all(
        self,
        open_trades: list,  # List[TradeRecord]
        price_map: Dict[str, float],
        vwap_map: Dict[str, float] = None,
        atr_map: Dict[str, float] = None,
        volume_map: Dict[str, float] = None,  # current 1m bar volume
        avg_vol_map: Dict[str, float] = None,  # rolling avg 1m volume
        now: datetime = None,
    ) -> List[LifecycleEvent]:
        """
        Evaluate lifecycle for every open trade and return a flat list
        of events to be processed by the execution engine.
        At most one event is emitted per trade per tick.
        """
        now = now or datetime.now(ET)
        vwap_map = vwap_map or {}
        atr_map = atr_map or {}
        volume_map = volume_map or {}
        avg_vol_map = avg_vol_map or {}

        events: List[LifecycleEvent] = []

        for trade in open_trades:
            if trade.lifecycle_state == LIFECYCLE_CLOSED:
                continue

            price = price_map.get(trade.ticker, 0.0)
            if price <= EPSILON:
                continue

            prev_state = trade.lifecycle_state
            evt = self._evaluate_one(
                trade,
                price=price,
                vwap=vwap_map.get(trade.ticker, 0.0),
                atr=atr_map.get(trade.ticker, self._default_atr(trade)),
                volume=volume_map.get(trade.ticker, 0.0),
                avg_volume=avg_vol_map.get(trade.ticker, 0.0),
                now=now,
            )
            if evt is not None:
                events.append(evt)
                # Publish lifecycle domain event (best-effort)
                try:
                    from events.bus import event_bus
                    from events import current_cycle
                    from events.types import LifecycleTransition

                    event_bus.publish(
                        LifecycleTransition(
                            cycle_id=current_cycle.id,
                            trade_id=evt.trade_id,
                            ticker=evt.ticker,
                            from_state=prev_state,
                            to_state=evt.new_lifecycle_state or prev_state,
                            reason=evt.reason,
                            shares_sold=evt.shares_to_sell,
                            pnl=0.0,  # actual PnL not known until broker fill
                        )
                    )
                except Exception:  # noqa: BLE001
                    pass

        return events

    # ── Per-trade evaluation ──────────────────────────────────────────────────

    def _evaluate_one(
        self,
        trade,
        price: float,
        vwap: float,
        atr: float,
        volume: float,
        avg_volume: float,
        now: datetime,
    ) -> Optional[LifecycleEvent]:
        """
        Returns a single LifecycleEvent or None.
        Priority order (highest wins):
            1. Hard stop
            2. Hard target (PARTIAL2/TRAILING only — remainder)
            3. VWAP exit (TRAILING only)
            4. Volume fade (TRAILING only)
            5. Time continuation stop (ENTRY/PARTIAL1 only)
            6. Session time stop (any state)
            7. Advance state: ENTRY→PARTIAL1, PARTIAL1→PARTIAL2, PARTIAL2→TRAILING
        """
        cfg = CONFIG.lifecycle
        risk = CONFIG.risk
        state = trade.lifecycle_state

        # Update high watermark
        if price > trade.high_watermark:
            trade.high_watermark = price

        # Use a locked initial risk value so R targets never drift after entry.
        initial_risk = (
            float(getattr(trade, "initial_risk_px", 0.0))
            if float(getattr(trade, "initial_risk_px", 0.0)) > 0
            else (
                abs(trade.entry_price - trade.initial_stop_price)
                if hasattr(trade, "initial_stop_price") and trade.initial_stop_price > 0
                else trade.risk_per_share
            )
        )
        initial_risk = max(initial_risk, EPSILON)
        r_achieved = (trade.high_watermark - trade.entry_price) / initial_risk
        r_current = (price - trade.entry_price) / initial_risk

        # ── 1. Hard stop ──────────────────────────────────────────────────────
        if price <= trade.stop_price:
            shares = (
                trade.shares_remaining if trade.shares_remaining > 0 else trade.shares
            )
            return LifecycleEvent(
                trade_id=trade.trade_id,
                ticker=trade.ticker,
                event=LIFECYCLE_EVT_STOP_HIT,
                shares_to_sell=shares,
                new_lifecycle_state=LIFECYCLE_CLOSED,
                is_close=True,
                reason=f"stop_hit price={price:.4f} stop={trade.stop_price:.4f}",
            )

        # ── 2. Hard target / optional parabolic squeeze ───────────────────────
        if (
            state == LIFECYCLE_TRAILING
            and bool(getattr(cfg, "parabolic_squeeze_enabled", False))
            and float(getattr(cfg, "parabolic_target_r", 0.0)) > 0
            and r_current >= float(getattr(cfg, "parabolic_target_r", 0.0))
        ):
            shares_remaining = (
                trade.shares_remaining if trade.shares_remaining > 0 else trade.shares
            )
            shares_to_sell = max(1, int(shares_remaining * 0.50))
            squeeze_stop = self._tight_squeeze_stop(trade, atr)
            trade.stop_price = max(trade.stop_price, squeeze_stop)
            trade.trail_active = True
            log.info(
                f"[{trade.ticker}] LIFECYCLE SQUEEZE_INTENT: sell {shares_to_sell}sh "
                f"@ +{float(getattr(cfg, 'parabolic_target_r', 0.0)):.1f}R "
                f"stop→${trade.stop_price:.4f}"
            )
            return LifecycleEvent(
                trade_id=trade.trade_id,
                ticker=trade.ticker,
                event=LIFECYCLE_EVT_PARTIAL_SELL,
                shares_to_sell=shares_to_sell,
                new_stop=trade.stop_price,
                new_lifecycle_state=LIFECYCLE_SQUEEZE,
                reason=(
                    f"parabolic_squeeze 50% at +"
                    f"{float(getattr(cfg, 'parabolic_target_r', 0.0)):.1f}R"
                ),
            )

        if (
            state in (LIFECYCLE_PARTIAL2, LIFECYCLE_TRAILING)
            and cfg.hard_target_r > 0
            and r_current >= cfg.hard_target_r
        ):
            shares = (
                trade.shares_remaining if trade.shares_remaining > 0 else trade.shares
            )
            return LifecycleEvent(
                trade_id=trade.trade_id,
                ticker=trade.ticker,
                event=LIFECYCLE_EVT_TARGET_HIT,
                shares_to_sell=shares,
                new_lifecycle_state=LIFECYCLE_CLOSED,
                is_close=True,
                reason=f"hard_target r={r_current:.2f}R @ {cfg.hard_target_r:.1f}R",
            )

        # ── 3. VWAP exit (while trailing / squeeze) ───────────────────────────
        if (
            state in (LIFECYCLE_TRAILING, LIFECYCLE_SQUEEZE)
            and cfg.exit_on_vwap_loss
            and vwap > EPSILON
            and price < vwap
        ):
            shares = (
                trade.shares_remaining if trade.shares_remaining > 0 else trade.shares
            )
            return LifecycleEvent(
                trade_id=trade.trade_id,
                ticker=trade.ticker,
                event=LIFECYCLE_EVT_VWAP_EXIT,
                shares_to_sell=shares,
                new_lifecycle_state=LIFECYCLE_CLOSED,
                is_close=True,
                reason=f"vwap_loss price={price:.4f} vwap={vwap:.4f}",
            )

        # ── 4. Volume fade exit (while trailing / squeeze) ────────────────────
        if (
            state in (LIFECYCLE_TRAILING, LIFECYCLE_SQUEEZE)
            and volume > 0
            and avg_volume > 0
            and (volume / avg_volume) < cfg.volume_fade_threshold
        ):
            shares = (
                trade.shares_remaining if trade.shares_remaining > 0 else trade.shares
            )
            return LifecycleEvent(
                trade_id=trade.trade_id,
                ticker=trade.ticker,
                event=LIFECYCLE_EVT_VOLUME_FADE,
                shares_to_sell=shares,
                new_lifecycle_state=LIFECYCLE_CLOSED,
                is_close=True,
                reason=(
                    f"volume_fade vol={volume:.0f} avg={avg_volume:.0f} "
                    f"ratio={volume/avg_volume:.2f}<{cfg.volume_fade_threshold}"
                ),
            )

        # ── 5. Time continuation stop (ENTRY/PARTIAL1 only) ──────────────────
        #    If price has not made a new high within N minutes of entry → exit.
        if state in (LIFECYCLE_ENTRY, LIFECYCLE_PARTIAL1) and trade.entry_time:
            elapsed_m = (now - trade.entry_time).total_seconds() / 60
            if elapsed_m >= cfg.time_continuation_minutes:
                # Stale trade: no new high beyond entry since open.
                if trade.high_watermark <= trade.entry_price + EPSILON:
                    shares = (
                        trade.shares_remaining
                        if trade.shares_remaining > 0
                        else trade.shares
                    )
                    return LifecycleEvent(
                        trade_id=trade.trade_id,
                        ticker=trade.ticker,
                        event=LIFECYCLE_EVT_TIME_STOP,
                        shares_to_sell=shares,
                        new_lifecycle_state=LIFECYCLE_CLOSED,
                        is_close=True,
                        reason=(
                            f"time_continuation {elapsed_m:.0f}m, no new high since entry"
                        ),
                    )

        # ── 6. Session time stop ──────────────────────────────────────────────
        close_h, close_m = map(int, str(CONFIG.session.session_close).split(":"))
        session_end = now.replace(hour=close_h, minute=close_m, second=0, microsecond=0)
        regime_cfg = CONFIG.regime
        time_stop_m = (
            regime_cfg.chop_time_stop_minutes
            if trade.regime == "CHOP"
            else risk.time_stop_minutes_before_close
        )
        if (session_end - now).total_seconds() / 60 <= time_stop_m:
            shares = (
                trade.shares_remaining if trade.shares_remaining > 0 else trade.shares
            )
            return LifecycleEvent(
                trade_id=trade.trade_id,
                ticker=trade.ticker,
                event=LIFECYCLE_EVT_TIME_STOP,
                shares_to_sell=shares,
                new_lifecycle_state=LIFECYCLE_CLOSED,
                is_close=True,
                reason=f"session_end_stop {time_stop_m}m before close",
            )

        # ── 7. State advancement ──────────────────────────────────────────────

        # ENTRY → PARTIAL1: +1R reached → sell partial + move stop to breakeven
        if state == LIFECYCLE_ENTRY and r_achieved >= cfg.breakeven_r:
            new_stop = trade.entry_price  # exact breakeven
            trade.stop_price = new_stop
            trade.lifecycle_state = LIFECYCLE_PARTIAL1
            shares_remaining = (
                trade.shares_remaining if trade.shares_remaining > 0 else trade.shares
            )
            partial1_pct = max(
                0.0, min(1.0, float(getattr(cfg, "partial1_exit_pct", 0.25)))
            )
            shares_to_sell = max(1, int(shares_remaining * partial1_pct))
            log.info(
                f"[{trade.ticker}] LIFECYCLE PARTIAL1_INTENT: sell {shares_to_sell}sh "
                f"({partial1_pct*100:.0f}%) + stop→breakeven @ ${new_stop:.4f} "
                f"(r={r_achieved:.2f})"
            )
            return LifecycleEvent(
                trade_id=trade.trade_id,
                ticker=trade.ticker,
                event=LIFECYCLE_EVT_PARTIAL_SELL,
                shares_to_sell=shares_to_sell,
                new_stop=new_stop,
                new_lifecycle_state=LIFECYCLE_PARTIAL1,
                reason=(
                    f"partial1 {partial1_pct*100:.0f}% + breakeven "
                    f"at +{cfg.breakeven_r:.1f}R"
                ),
            )

        # PARTIAL1 → PARTIAL2: price reached +partial_exit_r → sell portion
        if state == LIFECYCLE_PARTIAL1 and r_achieved >= cfg.partial_exit_r:
            shares_remaining = (
                trade.shares_remaining if trade.shares_remaining > 0 else trade.shares
            )
            shares_to_sell = max(1, int(shares_remaining * cfg.partial_exit_pct))
            log.info(
                f"[{trade.ticker}] LIFECYCLE PARTIAL2_INTENT: sell {shares_to_sell}sh "
                f"({cfg.partial_exit_pct*100:.0f}%) @ ${price:.4f} (r={r_achieved:.2f})"
            )
            return LifecycleEvent(
                trade_id=trade.trade_id,
                ticker=trade.ticker,
                event=LIFECYCLE_EVT_PARTIAL_SELL,
                shares_to_sell=shares_to_sell,
                new_lifecycle_state=LIFECYCLE_PARTIAL2,
                reason=(
                    f"partial_exit {cfg.partial_exit_pct*100:.0f}% at "
                    f"+{cfg.partial_exit_r:.1f}R price={price:.4f}"
                ),
            )

        # PARTIAL2 → TRAILING: price reached +trail_start_r → activate trail
        if state == LIFECYCLE_PARTIAL2 and r_achieved >= cfg.trail_start_r:
            trail_stop = trade.high_watermark - (atr * cfg.trail_atr_multiplier)
            trail_stop = max(trail_stop, trade.stop_price)  # never lower existing stop
            trade.stop_price = trail_stop
            trade.trail_active = True
            trade.lifecycle_state = LIFECYCLE_TRAILING
            log.info(
                f"[{trade.ticker}] LIFECYCLE TRAILING: trail stop=${trail_stop:.4f} "
                f"(hwm={trade.high_watermark:.4f} atr={atr:.4f} "
                f"r={r_achieved:.2f})"
            )
            return LifecycleEvent(
                trade_id=trade.trade_id,
                ticker=trade.ticker,
                event=LIFECYCLE_EVT_TRAIL_START,
                new_stop=trail_stop,
                new_lifecycle_state=LIFECYCLE_TRAILING,
                reason=f"trail_start at +{cfg.trail_start_r:.1f}R",
            )

        # TRAILING: ratchet trail stop up as price makes new highs
        if state == LIFECYCLE_TRAILING and trade.trail_active:
            candidate_stop = trade.high_watermark - (atr * cfg.trail_atr_multiplier)
            if candidate_stop > trade.stop_price:
                trade.stop_price = candidate_stop
                return LifecycleEvent(
                    trade_id=trade.trade_id,
                    ticker=trade.ticker,
                    event=LIFECYCLE_EVT_TRAIL_START,  # reuse tag = ratchet
                    new_stop=candidate_stop,
                    new_lifecycle_state=LIFECYCLE_TRAILING,
                    reason=f"trail_ratchet stop→{candidate_stop:.4f}",
                )

        # SQUEEZE: ultra-tight ratchet (0.5 ATR or previous 1m low)
        if state == LIFECYCLE_SQUEEZE and trade.trail_active:
            candidate_stop = self._tight_squeeze_stop(trade, atr)
            if candidate_stop > trade.stop_price:
                trade.stop_price = candidate_stop
                return LifecycleEvent(
                    trade_id=trade.trade_id,
                    ticker=trade.ticker,
                    event=LIFECYCLE_EVT_TRAIL_START,
                    new_stop=candidate_stop,
                    new_lifecycle_state=LIFECYCLE_SQUEEZE,
                    reason=f"squeeze_ratchet stop→{candidate_stop:.4f}",
                )

        return None  # nothing to do this tick

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _default_atr(trade) -> float:
        """Fallback ATR from the data cache (prefer 1m), or a sensible default."""
        try:
            from data.cache import bar_cache
            from signals.structure import calc_atr
            from config.constants import TF_1M, TF_5M

            df_1m = bar_cache.get_tf(trade.ticker, TF_1M)
            if df_1m is not None and not df_1m.empty:
                atr_1m = calc_atr(df_1m)
                if atr_1m > EPSILON:
                    return atr_1m

            df_5m = bar_cache.get_tf(trade.ticker, TF_5M)
            if df_5m is not None and not df_5m.empty:
                atr_5m = calc_atr(df_5m)
                if atr_5m > EPSILON:
                    return atr_5m
        except Exception:
            pass
        # Default: 0.5% of entry price
        return trade.entry_price * 0.005 if trade.entry_price > EPSILON else 0.50

    @staticmethod
    def _tight_squeeze_stop(trade, atr: float) -> float:
        """Ultra-tight stop for parabolic squeeze mode (long-only)."""
        atr_tight = trade.high_watermark - (max(atr, EPSILON) * 0.5)
        prev_1m_low = 0.0
        try:
            from data.pipeline import get_last_closed_low
            from config.constants import TF_1M

            closed_low = get_last_closed_low(trade.ticker, TF_1M)
            if closed_low is not None and closed_low > EPSILON:
                prev_1m_low = float(closed_low)
        except Exception:
            pass

        # For long positions, tighter means higher protective stop.
        candidates = [atr_tight]
        if prev_1m_low > EPSILON:
            candidates.append(prev_1m_low)
        return max(max(candidates), trade.stop_price)
