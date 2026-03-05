"""
manager.py — Risk management engine.

Responsibilities:
  - Position sizing (fixed R)
  - Daily loss cap enforcement
  - Trade / ticker trade count limits
  - Re-entry cooldown enforcement
  - Time stop (close before session end)
  - Trailing stop evaluation on open positions
  - CHOP-mode size reduction

All state is session-scoped. Reset via reset_session() each day.
"""

from __future__ import annotations

import math
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pytz

from config.constants import (
    DIRECTION_LONG,
    EPSILON,
    LIFECYCLE_ENTRY,
    LIFECYCLE_SQUEEZE,
    REJECT_CONSEC_LOSS,
    REJECT_COOLDOWN,
    REJECT_DAILY_LOSS_CAP,
    REJECT_MAX_CONCURRENT,
    REJECT_MAX_TICKER_TRADES,
    REJECT_MAX_TRADES,
    REJECT_TICKER_LOSS_CAP,
    REGIME_CHOP,
    STATE_CLOSED,
    STATE_OPEN,
)
from config.settings import CONFIG
from signals.setup import SetupResult

log = logging.getLogger(__name__)
ET = pytz.timezone("America/New_York")


# ─────────────────────────────────────────────────────────────────────────────
# TRADE RECORD
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class TradeRecord:
    trade_id: str
    ticker: str
    direction: str = DIRECTION_LONG
    entry_price: float = 0.0
    stop_price: float = 0.0
    target_price: float = 0.0
    shares: int = 0
    entry_time: Optional[datetime] = None
    exit_time: Optional[datetime] = None
    exit_price: float = 0.0
    pnl: float = 0.0
    pnl_r: float = 0.0
    state: str = STATE_OPEN
    exit_reason: str = ""
    demand_score: float = 0.0
    setup_quality_score: float = 0.0
    regime: str = ""
    risk_multiplier: float = 1.0
    bars_1m: int = 0
    bars_5m: int = 0
    bars_15m: int = 0
    setup_name: str = ""
    chosen_over: str = ""
    # Determinism audit
    selection_reason: str = ""  # full tie-break explanation
    universe_rank: int = 0  # demand-score rank at entry
    # Data health audit
    size_degrade_reason: str = ""  # why size was reduced (empty = full size)
    feed_type: str = ""  # feed used for entry data
    notes: str = ""

    # Live tracking
    high_watermark: float = 0.0
    trail_active: bool = False

    # Lifecycle state machine (managed by execution.lifecycle)
    lifecycle_state: str = LIFECYCLE_ENTRY
    shares_remaining: int = 0  # updated as partials are sold
    initial_stop_price: float = 0.0  # original structural stop; never changes
    initial_risk_px: float = 0.0  # locked |entry - initial_stop| at entry

    @property
    def risk_per_share(self) -> float:
        """Current R (can be 0 when stop=breakeven — use initial_risk_per_share for R multiples)."""
        return abs(self.entry_price - self.stop_price)

    @property
    def initial_risk_per_share(self) -> float:
        """Original structural risk per share — use this for R-multiple calculations."""
        if self.initial_risk_px > 0:
            return self.initial_risk_px
        ref = (
            self.initial_stop_price if self.initial_stop_price > 0 else self.stop_price
        )
        return abs(self.entry_price - ref)

    @property
    def is_open(self) -> bool:
        return self.state == STATE_OPEN


# ─────────────────────────────────────────────────────────────────────────────
# RISK MANAGER
# ─────────────────────────────────────────────────────────────────────────────


class RiskManager:
    """
    Session-scoped risk manager.

    One instance lives for the duration of a trading day.
    Call reset_session() at the start of each new day.
    """

    
    
    def __init__(self, ib_connection=None, max_drawdown_pct=0.02, *args, **kwargs):
        self.__init_original__(*args, **kwargs)
        # Handle singleton pattern safely
        if getattr(self, "_initialized_phase4", False):
            return

        self._initialized_phase4 = True
        
        self.ib = ib_connection
        self.max_drawdown_pct = max_drawdown_pct
        self.starting_balance = 0.0
        self.is_halted = False
        
        if self.ib and self.ib.isConnected():
            self._init_balance()

    def _init_balance(self):
        try:
            summary = self.ib.accountSummary()
            for item in summary:
                if item.tag == 'NetLiquidation':
                    self.starting_balance = float(item.value)
                    log.info(f"??? RISK MANAGER ONLINE: Starting Balance => $ {self.starting_balance:.2f}")
                    break
        except Exception as e:
            log.error(f"Failed to fetch starting balance: {e}")

    def check_kill_switch(self) -> bool:
        if not self.ib or not self.ib.isConnected() or self.starting_balance == 0.0:
            return False

        if self.is_halted:
            return True

        try:
            current_balance = self.starting_balance
            summary = self.ib.accountSummary()
            for item in summary:
                if item.tag == 'NetLiquidation':
                    current_balance = float(item.value)
                    break
            
            drawdown = (self.starting_balance - current_balance) / self.starting_balance
            
            if drawdown >= self.max_drawdown_pct:
                log.critical(f"FATAL DRAWDOWN: {-drawdown*100:.2f}% (Limit: {-self.max_drawdown_pct*100:.2f}%)")
                self.flatten_all_positions()
                self.is_halted = True
                return True
                
        except Exception as e:
            log.error(f"Error checking kill switch: {e}")
            
        return False

    def flatten_all_positions(self):
        log.critical("FLATTENING ALL POSITIONS...")
        if not self.ib or not self.ib.isConnected():
            return
            
        from ib_insync import MarketOrder
        try:
            positions = self.ib.positions()
            for p in positions:
                if p.position == 0:
                    continue
                action = "SELL" if p.position > 0 else "BUY"
                qty = abs(p.position)
                log.critical(f"Flattening {qty} of {p.contract.symbol} ({action})")
                order = MarketOrder(action, qty)
                self.ib.placeOrder(p.contract, order)
        except Exception as e:
            log.error(f"Failed to flatten positions: {e}")

    def __init_original__(self):
        self.risk_cfg = CONFIG.risk
        self.port_cfg = CONFIG.portfolio
        self._trades: List[TradeRecord] = []
        self._ticker_counts: Dict[str, int] = defaultdict(int)
        self._last_entry: Dict[str, datetime] = {}
        self._chop_trade_count: int = 0
        self._daily_pnl: float = 0.0
        self._trade_counter: int = 0
        # New governors
        self._ticker_pnl_r: Dict[str, float] = defaultdict(float)
        self._consecutive_losses: int = 0
        self._ticker_blocked: set = set()  # blocked for day
        self._open_count: int = 0  # simultaneous positions

    # ── Session control ──────────────────────────────────────────────────────

    def reset_session(self) -> None:
        self._trades.clear()
        self._ticker_counts.clear()
        self._last_entry.clear()
        self._chop_trade_count = 0
        self._daily_pnl = 0.0
        self._trade_counter = 0
        self._ticker_pnl_r.clear()
        self._consecutive_losses = 0
        self._ticker_blocked.clear()
        self._open_count = 0
        log.info("RiskManager: session reset")

    # ── Gate checks (call BEFORE sizing) ────────────────────────────────────

    def can_trade(
        self,
        ticker: str,
        regime: str,
        now: datetime = None,
    ) -> tuple[bool, str]:
        """
        Returns (True, "") if trade is allowed, else (False, reason).
        Checks (in order): daily loss cap, daily trade count, consecutive loss
        circuit, concurrent position cap, ticker daily loss cap,
        per-ticker count, cooldown.
        """
        now = now or datetime.now(ET)
        cfg = CONFIG.risk
        sm = CONFIG.safe_mode

        # ── Safe-mode overrides ──────────────────────────────────────────────
        max_trades = cfg.max_trades_per_day
        if sm.enabled:
            max_trades = min(max_trades, sm.max_trades_per_day)
            if regime == REGIME_CHOP and sm.require_trend_only:
                return False, "SAFE_MODE_TREND_ONLY"

        # ── Daily loss cap ───────────────────────────────────────────────────
        cap_pct = sm.max_daily_loss_pct if sm.enabled else cfg.daily_loss_cap_pct
        cap = cfg.account_size * cap_pct
        if self._daily_pnl <= -cap:
            return (
                False,
                f"{REJECT_DAILY_LOSS_CAP}(PnL={self._daily_pnl:.0f}, cap={-cap:.0f})",
            )

        # ── Total daily trades ───────────────────────────────────────────────
        total_today = len(self._trades)
        if regime == REGIME_CHOP:
            max_trades = min(max_trades, CONFIG.regime.max_chop_trades_per_day)
        if total_today >= max_trades:
            return False, f"{REJECT_MAX_TRADES}({total_today}/{max_trades})"

        # ── Consecutive loss circuit breaker ─────────────────────────────────
        if self._consecutive_losses >= cfg.max_consecutive_losses:
            return False, (
                f"{REJECT_CONSEC_LOSS}"
                f"(streak={self._consecutive_losses}/{cfg.max_consecutive_losses})"
            )

        # ── Max simultaneous open positions ──────────────────────────────────
        max_concurrent = CONFIG.decision.max_concurrent_trades
        if sm.enabled:
            max_concurrent = min(max_concurrent, sm.max_concurrent_positions)
        if self._open_count >= max_concurrent:
            return False, (
                f"{REJECT_MAX_CONCURRENT}" f"(open={self._open_count}/{max_concurrent})"
            )

        # ── Per-ticker daily loss cap (in R) ─────────────────────────────────
        if ticker in self._ticker_blocked:
            ticker_r = self._ticker_pnl_r.get(ticker, 0.0)
            return False, (
                f"{REJECT_TICKER_LOSS_CAP}"
                f"({ticker}: {ticker_r:.2f}R ≤ -{cfg.max_ticker_daily_loss_r:.1f}R)"
            )

        # ── Per-ticker daily trade count ─────────────────────────────────────
        ticker_count = self._ticker_counts.get(ticker, 0)
        if ticker_count >= cfg.max_trades_per_ticker_per_day:
            return False, (
                f"{REJECT_MAX_TICKER_TRADES}"
                f"({ticker}: {ticker_count}/{cfg.max_trades_per_ticker_per_day})"
            )

        # ── Re-entry cooldown ────────────────────────────────────────────────
        last = self._last_entry.get(ticker)
        if last is not None:
            elapsed = (now - last).total_seconds() / 60
            cooldown = cfg.reentry_cooldown_minutes
            if elapsed < cooldown:
                return False, (
                    f"{REJECT_COOLDOWN}"
                    f"({ticker}: {elapsed:.0f}m < {cooldown}m cooldown)"
                )

        return True, ""

    # ── Position sizing ──────────────────────────────────────────────────────

    def size_position(
        self,
        entry_price: float,
        stop_price: float,
        regime: str,
        health_size_multiplier: float = 1.0,
    ) -> int:
        """
        Return share count using fixed-R sizing.

        shares = (account × risk_pct × regime_mult × health_mult) / risk_per_share

        health_size_multiplier: 1.0 = full size, <1.0 = degraded (missing context).
        Applied on top of the regime multiplier.
        """
        cfg = CONFIG.risk
        risk_amt = cfg.account_size * cfg.risk_per_trade_pct

        # CHOP mode reduction
        regime_mult = (
            CONFIG.regime.chop_size_multiplier if regime == REGIME_CHOP else 1.0
        )

        # DATA_HEALTH degrade (applied additively with regime mult)
        combined_mult = regime_mult * health_size_multiplier

        risk_per_share = abs(entry_price - stop_price)
        if risk_per_share <= EPSILON:
            log.warning("size_position: zero risk_per_share, defaulting to 1 share")
            return 1

        shares = int((risk_amt * combined_mult) / risk_per_share)
        shares = max(shares, 1)

        # Optional legacy cap (kept for backward compatibility only).
        if cfg.legacy_notional_cap_enabled:
            max_notional = cfg.account_size * 0.20
            max_shares = (
                int(max_notional / entry_price) if entry_price > EPSILON else shares
            )
            shares = min(shares, max_shares)

        return shares

    def get_position_size(
        self,
        ticker: str,
        entry_price: float,
        stop_loss: float,
        ticker_dvol: float,
        current_positions: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Rule-based sizing with portfolio overlays.

        Returns:
          {
            "can_trade": bool,
            "shares": int,
            "notional": float,
            "reason": str,
          }
        """
        equity = float(self.risk_cfg.account_size)
        dollar_risk_limit = equity * float(self.risk_cfg.risk_per_trade_pct)

        risk_per_share = abs(float(entry_price) - float(stop_loss))
        if risk_per_share <= EPSILON or entry_price <= EPSILON:
            return {"can_trade": False, "shares": 0, "reason": "Invalid Stop Loss"}

        shares = math.floor(dollar_risk_limit / risk_per_share)
        shares = max(shares, 0)
        notional = shares * float(entry_price)

        max_notional_allowed = equity * float(self.port_cfg.max_notional_per_trade_pct)
        if notional > max_notional_allowed:
            shares = math.floor(max_notional_allowed / float(entry_price))
            shares = max(shares, 0)
            notional = shares * float(entry_price)
            log.info(f"[{ticker}] Sizing capped by Portfolio Notional Limit")

        liquidity_limit = float(ticker_dvol) * float(
            self.port_cfg.liquidity_notional_cap_pct_of_dvol
        )
        if notional > liquidity_limit:
            shares = math.floor(liquidity_limit / float(entry_price))
            shares = max(shares, 0)
            notional = shares * float(entry_price)
            log.info(f"[{ticker}] Sizing capped by Liquidity (1% DVOL)")

        sector = self.port_cfg.sector_map.get(ticker, "UNKNOWN")
        active_sectors = [
            self.port_cfg.sector_map.get(t, "UNKNOWN") for t in current_positions.keys()
        ]

        if active_sectors.count(sector) >= int(self.port_cfg.max_per_sector):
            return {
                "can_trade": False,
                "shares": 0,
                "reason": f"Sector Limit Reached: {sector}",
            }

        if sector in active_sectors:
            shares = math.floor(
                shares * float(self.port_cfg.correlated_size_multiplier)
            )
            shares = max(shares, 0)
            notional = shares * float(entry_price)
            log.info(
                f"[{ticker}] Correlated sector detected. "
                f"Applying {self.port_cfg.correlated_size_multiplier}x multiplier."
            )

        if notional < float(self.port_cfg.min_trade_notional):
            return {
                "can_trade": False,
                "shares": 0,
                "reason": "Notional below minimum",
            }

        if len(current_positions) >= int(self.port_cfg.max_concurrent_positions):
            return {
                "can_trade": False,
                "shares": 0,
                "reason": "Max Concurrent Positions Reached",
            }

        return {
            "can_trade": True,
            "shares": int(shares),
            "notional": float(notional),
            "reason": "Passed all gates",
        }

    # ── Open a trade ─────────────────────────────────────────────────────────

    def open_trade(
        self,
        ticker: str,
        setup: SetupResult,
        regime: str,
        demand_score: float = 0.0,
        chosen_over: str = "",
        selection_reason: str = "",
        universe_rank: int = 0,
        size_degrade_reason: str = "",
        health_size_multiplier: float = 1.0,
        feed_type: str = "",
        shares_override: Optional[int] = None,
        now: datetime = None,
    ) -> TradeRecord:
        now = now or datetime.now(ET)
        cfg = CONFIG.risk

        if shares_override is not None and shares_override > 0:
            shares = int(shares_override)
        else:
            shares = self.size_position(
                setup.entry_price,
                setup.stop_price,
                regime,
                health_size_multiplier=health_size_multiplier,
            )
        risk_mult = (
            CONFIG.regime.chop_size_multiplier * health_size_multiplier
            if regime == REGIME_CHOP
            else 1.0 * health_size_multiplier
        )

        # Profit target
        risk_r = abs(setup.entry_price - setup.stop_price)
        target = (
            setup.entry_price + risk_r * cfg.profit_target_r
            if cfg.profit_target_r > 0
            else 0.0
        )

        self._trade_counter += 1
        trade_id = f"{ticker}_{now.strftime('%Y%m%d_%H%M%S')}_{self._trade_counter:03d}"

        trade = TradeRecord(
            trade_id=trade_id,
            ticker=ticker,
            direction=DIRECTION_LONG,
            entry_price=setup.entry_price,
            stop_price=setup.stop_price,
            target_price=round(target, 4),
            shares=shares,
            entry_time=now,
            state=STATE_OPEN,
            demand_score=demand_score,
            setup_quality_score=setup.setup_quality_score,
            regime=regime,
            risk_multiplier=risk_mult,
            bars_1m=setup.bars_1m,
            bars_5m=setup.bars_5m,
            bars_15m=setup.bars_15m,
            setup_name=setup.setup_name,
            chosen_over=chosen_over,
            selection_reason=selection_reason,
            universe_rank=universe_rank,
            size_degrade_reason=size_degrade_reason,
            feed_type=feed_type,
            high_watermark=setup.entry_price,
            initial_stop_price=setup.stop_price,
            initial_risk_px=abs(setup.entry_price - setup.stop_price),
        )

        trade.shares_remaining = shares  # initialise remaining shares

        self._trades.append(trade)
        self._ticker_counts[ticker] += 1
        self._last_entry[ticker] = now
        self._open_count += 1

        degrade_note = (
            f" [DEGRADE: {size_degrade_reason}]" if size_degrade_reason else ""
        )
        log.info(
            f"[{ticker}] OPENED {trade_id} | "
            f"{shares}sh @ ${setup.entry_price:.2f} "
            f"stop=${setup.stop_price:.2f} "
            f"target=${'N/A' if target == 0 else f'{target:.2f}'} "
            f"regime={regime} mult={risk_mult:.2f}"
            f"{degrade_note}"
        )
        return trade

    # ── Update open trades ────────────────────────────────────────────────────

    def update_open_trades(
        self,
        price_map: Dict[str, float],
        now: datetime = None,
    ) -> List[TradeRecord]:
        """
        Evaluate stop, target, trail stop, and time stop for all open trades.
        Returns list of trades that should be closed (with exit_price populated).
        """
        now = now or datetime.now(ET)
        to_close: List[TradeRecord] = []
        cfg = CONFIG.risk
        reg_cfg = CONFIG.regime

        for trade in self._trades:
            if not trade.is_open:
                continue

            ticker = trade.ticker
            price = price_map.get(ticker, 0.0)
            if price <= EPSILON:
                continue

            # Update watermark
            if price > trade.high_watermark:
                trade.high_watermark = price

            reason = ""

            # ── Hard stop ─────────────────────────────────────────────────
            if price <= trade.stop_price:
                reason = "STOP"

            # ── Profit target ─────────────────────────────────────────────
            elif trade.target_price > 0 and price >= trade.target_price:
                reason = "TARGET"

            # ── Trailing stop ─────────────────────────────────────────────
            else:
                r_achieved = (trade.high_watermark - trade.entry_price) / (
                    trade.initial_risk_per_share + EPSILON
                )
                if r_achieved >= cfg.trail_activate_r:
                    trade.trail_active = True
                if trade.trail_active:
                    trail_dist = trade.high_watermark - (
                        self._get_atr(ticker) * cfg.trail_atr_multiplier
                    )
                    if price <= trail_dist:
                        reason = "TRAIL_STOP"

            # ── Time stop ─────────────────────────────────────────────────
            close_h, close_m = map(int, str(CONFIG.session.session_close).split(":"))
            session_end = now.replace(
                hour=close_h, minute=close_m, second=0, microsecond=0
            )
            regime_time_stop = (
                reg_cfg.chop_time_stop_minutes
                if trade.regime == REGIME_CHOP
                else cfg.time_stop_minutes_before_close
            )
            if (session_end - now).total_seconds() / 60 <= regime_time_stop:
                reason = f"TIME_STOP({regime_time_stop}m_before_close)"

            if reason:
                trade.exit_price = price
                trade.exit_reason = reason
                to_close.append(trade)

        return to_close

    def close_trade(
        self,
        trade: TradeRecord,
        price: float,
        reason: str = "",
        now: datetime = None,
    ) -> None:
        now = now or datetime.now(ET)
        # Use shares_remaining for partial-exit scenarios
        closed_shares = (
            trade.shares_remaining if trade.shares_remaining > 0 else trade.shares
        )
        trade.exit_price = price
        trade.exit_time = now
        trade.exit_reason = reason or trade.exit_reason
        trade.state = STATE_CLOSED
        trade.pnl = (price - trade.entry_price) * closed_shares
        trade.pnl_r = trade.pnl / (
            trade.initial_risk_per_share * closed_shares + EPSILON
        )
        self._daily_pnl += trade.pnl
        self._open_count = max(0, self._open_count - 1)

        # Per-ticker R tracking
        self._ticker_pnl_r[trade.ticker] = (
            self._ticker_pnl_r.get(trade.ticker, 0.0) + trade.pnl_r
        )

        # Consecutive loss tracking & per-ticker block
        if trade.pnl_r < 0:
            self._consecutive_losses += 1
            ticker_total_r = self._ticker_pnl_r[trade.ticker]
            if ticker_total_r <= -CONFIG.risk.max_ticker_daily_loss_r:
                self._ticker_blocked.add(trade.ticker)
                log.warning(
                    f"[{trade.ticker}] TICKER BLOCKED for session: "
                    f"cumulative R = {ticker_total_r:.2f} breached "
                    f"-{CONFIG.risk.max_ticker_daily_loss_r:.1f}R cap"
                )
        else:
            self._consecutive_losses = 0  # reset on any win

        log.info(
            f"[{trade.ticker}] CLOSED {trade.trade_id} | "
            f"exit=${price:.2f} reason={trade.exit_reason} "
            f"PnL=${trade.pnl:.2f} ({trade.pnl_r:.2f}R) "
            f"consec_losses={self._consecutive_losses}"
        )

    # ── Getters ──────────────────────────────────────────────────────────────

    def open_trades(self) -> List[TradeRecord]:
        return [t for t in self._trades if t.is_open]

    def open_tickers(self) -> List[str]:
        return [t.ticker for t in self.open_trades()]

    def daily_pnl(self) -> float:
        return self._daily_pnl

    def all_trades(self) -> List[TradeRecord]:
        return list(self._trades)

    def consecutive_losses(self) -> int:
        return self._consecutive_losses

    def is_ticker_blocked(self, ticker: str) -> bool:
        return ticker in self._ticker_blocked

    def ticker_pnl_r(self, ticker: str) -> float:
        return self._ticker_pnl_r.get(ticker, 0.0)

    # ── Startup recovery ──────────────────────────────────────────────────────

    def recover_position(
        self,
        ticker: str,
        qty: int,
        avg_entry_price: float,
        stop_price: float = 0.0,
        regime: str = "",
        note: str = "RECOVERED_ON_RESTART",
        now: datetime = None,
    ) -> TradeRecord:
        """
        Re-register a broker position that was discovered at startup.

        Called by startup_recovery when broker holds a position that is
        not reflected in the (empty) internal state.  A best-effort
        TradeRecord is created so the lifecycle manager can track the
        position from the current tick onwards.

        If stop_price is 0, a conservative 2% structural stop is applied
        and flagged in the notes.  The lifecycle starts at LIFECYCLE_ENTRY
        so the stop-ratchet logic fires correctly on the next R target.
        """
        from config.constants import (
            DIRECTION_LONG,
            LIFECYCLE_ENTRY,
            LIFECYCLE_SQUEEZE,
            STATE_OPEN,
        )

        now = now or datetime.now(ET)

        if stop_price <= 0:
            stop_price = round(avg_entry_price * 0.98, 4)
            note = f"{note}|STOP_ESTIMATED_2PCT"

        self._trade_counter += 1
        trade_id = (
            f"{ticker}_RECOVERED_{now.strftime('%Y%m%d_%H%M%S')}_"
            f"{self._trade_counter:03d}"
        )

        risk_r = abs(avg_entry_price - stop_price)
        target = avg_entry_price + risk_r * CONFIG.risk.profit_target_r
        note_upper = (note or "").upper()
        recovered_state = (
            LIFECYCLE_SQUEEZE if "SQUEEZE" in note_upper else LIFECYCLE_ENTRY
        )

        trade = TradeRecord(
            trade_id=trade_id,
            ticker=ticker,
            direction=DIRECTION_LONG,
            entry_price=avg_entry_price,
            stop_price=stop_price,
            target_price=round(target, 4),
            shares=qty,
            entry_time=now,
            state=STATE_OPEN,
            regime=regime or "",
            high_watermark=avg_entry_price,
            initial_stop_price=stop_price,
            initial_risk_px=abs(avg_entry_price - stop_price),
            lifecycle_state=recovered_state,
            shares_remaining=qty,
            trail_active=recovered_state == LIFECYCLE_SQUEEZE,
            notes=note,
        )

        self._trades.append(trade)
        self._ticker_counts[ticker] += 1
        self._last_entry[ticker] = now
        self._open_count += 1

        log.warning(
            f"[RiskManager] RECOVERED position: {ticker} {qty}sh @ "
            f"${avg_entry_price:.4f} stop=${stop_price:.4f}  ({note})"
        )
        return trade

    def _get_atr(self, ticker: str) -> float:
        """Retrieve cached ATR or default."""
        try:
            from data.cache import bar_cache
            from signals.structure import calc_atr
            from config.constants import TF_1M, TF_5M

            df_1m = bar_cache.get_tf(ticker, TF_1M)
            if df_1m is not None and not df_1m.empty:
                atr_1m = calc_atr(df_1m)
                if atr_1m > EPSILON:
                    return atr_1m

            df_5m = bar_cache.get_tf(ticker, TF_5M)
            if df_5m is not None and not df_5m.empty:
                atr_5m = calc_atr(df_5m)
                if atr_5m > EPSILON:
                    return atr_5m
        except Exception:
            pass
        return 0.50  # default ATR fallback


# Module-level singleton
risk_manager = RiskManager()
