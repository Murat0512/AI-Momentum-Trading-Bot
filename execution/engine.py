"""
engine.py — Main trading engine loop.

Architecture:
  Every 60 seconds (1m bar close):
    1. Check session window
    2. Refresh SPY → detect regime
    3. Run universe scanner → Top 15
    4. For each Top 15: detect setup
    5. Deterministic selection → best candidate
    6. Gate checks (risk manager)
    7. Execute entry
    8. Monitor open positions → exits
    9. Log everything

This is the single entry point for live/paper operation.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime
import json
from typing import Dict, List, Optional

import pytz

from config.constants import (
    LIFECYCLE_EVT_PARTIAL_SELL,
    LIFECYCLE_EVT_STOP_HIT,
    REGIME_CHOP,
    REJECT_COOLDOWN,
    REJECT_DAILY_LOSS_CAP,
    REJECT_MAX_TICKER_TRADES,
    REJECT_MAX_TRADES,
    REJECT_NO_SETUP,
    REJECT_OUTSIDE_WINDOW,
    REJECT_ENTRY_RUNAWAY,
    REJECT_ENTRY_STALE_QUOTE,
    REJECT_WASH_GUARD,
    TF_1M,
    TF_5M,
    BLOCK_HALT_DETECTED,
    EPSILON,
    SESSION_PREMARKET,
    SESSION_RTH,
    SESSION_AFTERHOURS,
)
from config.settings import CONFIG
from data.cache import bar_cache
from data.fetcher import BatchFetcher
from data.pipeline import MTFPipeline
from execution.lifecycle import LifecycleEvent, LifecycleManager
from execution.order_manager import order_manager
from execution.orders import get_broker
from execution.halt_machine import halt_machine
from execution.monitor import execution_monitor
from execution.slippage import slippage_monitor
from trade_log.trade_logger import TradeLogger
from trade_log.event_log import event_log
from trade_log.session_recorder import session_recorder
from risk.manager import RiskManager, risk_manager
from risk.regime import RegimeDetector, regime_detector
from scanner.demand import DemandMetrics
from scanner.universe import UniverseScanner
from scanner.universe_manager import UniverseManager
from decision.engine import DecisionEngine
from selection.selector import DeterministicSelector, TradeCandidate
from signals.setup import MomentumSetupV1
from utils.time_utils import (
    is_session_active,
    minutes_since_open,
    minutes_to_close,
    now_et,
)
from data.health import classify_session

log = logging.getLogger(__name__)
ET = pytz.timezone("America/New_York")


class TradingEngine:
    """
    Momentum Day Trading Engine — main event loop.

    Usage:
        engine = TradingEngine()
        engine.run()   # blocks until session end
    """

    def __init__(
        self,
        risk_mgr: RiskManager = None,
        regime_det: RegimeDetector = None,
        logger: TradeLogger = None,
    ):
        self._risk = risk_mgr or risk_manager
        self._regime = regime_det or regime_detector
        self._logger = logger or TradeLogger()
        # Feed-authority guard (defense-in-depth): trading engine must use IBKR as authoritative market data in live/paper execution paths.
        self._broker = get_broker()
        self._fetcher = BatchFetcher(adapter_name="ibkr")
        self._pipeline = MTFPipeline()
        self._selector = DeterministicSelector()
        self._setup_det = MomentumSetupV1()
        self._lifecycle = LifecycleManager()
        self._universe_mgr = UniverseManager()
        self._decision_engine = DecisionEngine(self._risk)
        self._pending_exit_orders: Dict[str, dict] = {}
        self._ticker_states: Dict[str, dict] = {}
        self._current_candidates: List[str] = []

        # Session state
        self._session_date: Optional[str] = None
        self._cycle_count: int = 0

        # Phase 4: Risk Manager Kill Switch
        try:
            from risk.manager import RiskManager

            self.risk_manager = RiskManager(self._broker.ib, max_drawdown_pct=0.02)
        except Exception as e:
            from risk.manager import RiskManager
            from infrastructure.ib_connection import IBConnectionManager

            self.risk_manager = RiskManager(
                IBConnectionManager().ib, max_drawdown_pct=0.02
            )

    # ─── Main loop ────────────────────────────────────────────────────────────

    def run(self) -> None:
        """Block forever. Press Ctrl-C to stop."""
        log.info("=" * 70)
        log.info("MOMENTUM DAY TRADING ENGINE — STARTING")
        log.info(f"Mode: {'PAPER' if CONFIG.execution.paper_mode else 'LIVE'}")
        log.info(
            f"Account: ${CONFIG.risk.account_size:,.0f}  "
            f"Risk/trade: {CONFIG.risk.risk_per_trade_pct*100:.1f}%"
        )
        # Feed authority notice
        feed = (
            CONFIG.data.alpaca_feed
            if CONFIG.data.data_source_live == "alpaca"
            else CONFIG.data.data_source_live
        )
        log.info(f"Data feed (live): {feed}")

        log.info("=" * 70)

        try:
            while True:
                if (
                    getattr(self, "risk_manager", None)
                    and self.risk_manager.check_kill_switch()
                ):
                    log.critical("SYSTEM HALTED BY RISK MANAGER.")
                    break

                self._tick()
                self._sleep_until_next_cycle()
        except KeyboardInterrupt:
            log.info("Engine stopped by user")
            self._end_of_day_report()

    def _sleep_until_next_cycle(self) -> None:
        """Sleep until the next interval boundary +1s (HH:MM:01 cadence)."""
        interval = max(1, int(CONFIG.execution.loop_interval_seconds))
        now_ts = time.time()
        next_boundary = (int(now_ts // interval) + 1) * interval
        target = next_boundary + 1.0
        sleep_for = target - now_ts
        if sleep_for <= 0.0:
            sleep_for += interval
        log.debug(f"Sleeping {sleep_for:.2f}s until next evaluation boundary (+1s)")
        time.sleep(sleep_for)

    def run_once(self) -> None:
        """Single cycle — useful for testing."""
        self._tick()

    # ─── Single cycle ─────────────────────────────────────────────────────────

    def _tick(self) -> None:
        self._cycle_count += 1
        # Propagate cycle ID to domain event context
        try:
            from events import current_cycle

            current_cycle.id = self._cycle_count
        except Exception:
            pass
        now = now_et()
        log.info(f"\n{'─'*60}")
        log.info(
            f"CYCLE #{self._cycle_count} at {now.strftime('%Y-%m-%d %H:%M:%S %Z')}"
        )

        # ── Day boundary reset ───────────────────────────────────────────
        today = now.strftime("%Y-%m-%d")
        if self._session_date != today:
            self._on_new_day(today)

        # ── Session window check ─────────────────────────────────────────
        if not is_session_active(now):
            if not CONFIG.session.trade_extended_hours:
                log.info(
                    f"{REJECT_OUTSIDE_WINDOW}: {now.strftime('%H:%M')} ET — waiting"
                )
                return

            current = classify_session(now)
            if current not in {SESSION_PREMARKET, SESSION_RTH, SESSION_AFTERHOURS}:
                log.info(
                    f"{REJECT_OUTSIDE_WINDOW}: {now.strftime('%H:%M')} ET — waiting"
                )
                return

        # ── SPY fetch + regime ────────────────────────────────────────────
        regime = self._update_regime()

        # ── Close any positions that hit stops/targets/time ───────────────
        self._process_exits()

        # ── OrderManager tick: poll fills, TTL, stuck detection ───────────
        changed_orders = order_manager.tick(self._broker, now)
        self._reconcile_pending_exit_orders(changed_orders)
        # ── Supervisor evaluation ─────────────────────────────────────────────
        _halt_entries = False
        try:
            from execution.supervisor import (
                CycleMetrics,
                MarketState,
                evaluate_market_state,
            )
            from events.bus import event_bus
            from events import current_cycle as _cc
            from events.types import SupervisorStateChange

            _sv_metrics = CycleMetrics()  # defaults until telemetry is wired
            _sv = evaluate_market_state(_sv_metrics)
            _last_sv = getattr(self, "_sv_state", MarketState.NORMAL)
            if _sv.state != _last_sv:
                event_bus.publish(
                    SupervisorStateChange(
                        cycle_id=_cc.id,
                        from_state=(
                            _last_sv.value
                            if hasattr(_last_sv, "value")
                            else str(_last_sv)
                        ),
                        to_state=_sv.state.value,
                        trigger=_sv.trigger,
                    )
                )
            self._sv_state = _sv.state
            if _sv.state == MarketState.HALT_ENTRIES:
                log.info(
                    "[Supervisor] HALT_ENTRIES — new entries suppressed this cycle"
                )
                _halt_entries = True
        except Exception as _exc:
            log.debug("[Supervisor] evaluate skipped: %s", _exc)
        if _halt_entries:
            return
        # ── Universe scan ─────────────────────────────────────────────────
        scanner = UniverseScanner(
            fetcher=self._fetcher,
            pipeline=self._pipeline,
            chop_mode=(regime == REGIME_CHOP),
        )
        top_universe: List[DemandMetrics] = scanner.scan()
        self._current_candidates = scanner.trade_candidates
        log.info(
            "Execution handoff candidates: %s",
            (
                ", ".join(self._current_candidates)
                if self._current_candidates
                else "<none>"
            ),
        )

        if not top_universe:
            log.info("Universe scan: no candidates passed filters")
            return

        use_decision_stack = bool(getattr(CONFIG.runtime, "use_decision_stack", False))
        if use_decision_stack:
            selected = self._select_via_decision_stack(top_universe, regime, now)
            if selected is None:
                return
            best, sel = selected
            self._execute_entry(best, regime, sel, now)
            return

        # ── Legacy path: setup detection for each Top-N ticker ────────────
        candidates: List[TradeCandidate] = []

        for dm in top_universe:
            ok, reason = self._risk.can_trade(dm.ticker, regime, now)
            if not ok:
                log.debug(f"[{dm.ticker}] skipped pre-setup: {reason}")
                continue

            mtf_bars = bar_cache.get(dm.ticker)
            if mtf_bars is None:
                continue

            setup = self._setup_det.check(
                ticker=dm.ticker,
                mtf_bars=mtf_bars,
                bid=dm.bid,
                ask=dm.ask,
                now=now,
            )
            candidates.append(TradeCandidate(metrics=dm, setup=setup))

        sel = self._selector.select(candidates)

        if sel.selected is None:
            log.info(f"Selection: no valid trade — {sel.reason}")
            return

        best = sel.selected

        can_trade, gate_reason = self._risk.can_trade(best.ticker, regime, now)
        if not can_trade:
            log.info(f"[{best.ticker}] RISK GATE: {gate_reason}")
            return
        if slippage_monitor.should_block(best.ticker, now):
            log.info(f"[{best.ticker}] SLIPPAGE GATE: ticker temporarily blocked")
            return
        self._execute_entry(best, regime, sel, now)

    def _select_via_decision_stack(
        self,
        top_universe: List[DemandMetrics],
        regime: str,
        now: datetime,
    ) -> Optional[tuple[TradeCandidate, object]]:
        metrics_by_ticker = {dm.ticker: dm for dm in top_universe}

        for dm in top_universe:
            self._universe_mgr.update_from_metrics(dm)

        top15 = self._universe_mgr.top_n(CONFIG.scanner.top_n)
        if not top15:
            log.info("Decision stack: no candidates in UniverseManager")
            return None

        tickers = [entry.ticker for entry in top15]
        quotes = self._fetcher.fetch_quotes(tickers)

        bar_store = {}
        mtf_store = {}
        quote_store = {}
        dh_store = {}

        for entry in top15:
            ticker = entry.ticker
            mtf = bar_cache.get(ticker) or {}
            mtf_store[ticker] = mtf
            bar_store[ticker] = mtf.get(TF_1M)

            quote = quotes.get(ticker, {})
            quote_store[ticker] = {
                "bid": quote.get("bid", 0.0),
                "ask": quote.get("ask", 0.0),
                "timestamp": quote.get("timestamp"),
            }

            dm = metrics_by_ticker.get(ticker)
            if dm is not None:
                dh = getattr(dm, "_dh_report", None)
                if dh is not None:
                    dh_store[ticker] = dh

        decision = self._decision_engine.run(
            top15=top15,
            bar_store=bar_store,
            mtf_store=mtf_store,
            quote_store=quote_store,
            dh_store=dh_store,
            regime=regime,
            now=now,
        )

        try:
            from trade_log.explainability import explainability_logger

            explainability_logger.log_cycle(
                decision_result=decision,
                news_candidates=[],
                slippage_stats={},
                regime=regime,
                now=now,
            )
        except Exception as exc:
            log.debug("Explainability log_cycle skipped: %s", exc)

        if decision.selection is None or decision.selection.selected is None:
            log.info(f"Decision stack: no valid trade — {decision.reason}")
            return None

        return decision.selection.selected, decision.selection

    # ─── Entry execution ──────────────────────────────────────────────────────

    def _execute_entry(
        self,
        candidate: TradeCandidate,
        regime: str,
        sel: object,  # SelectionResult — carries audit trail
        now: datetime,
    ) -> None:
        setup = candidate.setup
        dm = candidate.metrics
        ticker = candidate.ticker

        def _block(reason: str, detail: str = "", gate: str = "ENTRY_BLOCK") -> None:
            payload_reason = detail or reason
            self._log_blackbox_decision(
                ticker=ticker,
                accepted=False,
                reason=reason,
                now=now,
                gate_results={
                    "gate": gate,
                    "detail": payload_reason,
                },
            )
            event_log.log_gate_event(
                gate=gate, passed=False, reason=payload_reason, ticker=ticker
            )

        quote_quality = str(getattr(dm, "_quote_quality", "ok") or "ok")
        if quote_quality != "ok":
            detail = f"quote_quality={quote_quality}"
            log.warning(f"[{ticker}] ENTRY BLOCK: invalid quote quality ({detail})")
            _block("QUOTE_QUALITY_INVALID", detail, gate="QUOTE_QUALITY")
            return

        if execution_monitor.is_excluded(ticker):
            log.warning(f"[{ticker}] EXECUTION EXCLUDE: sloppy fills exceeded 25% ATR")
            _block(
                "EXECUTION_EXCLUDED",
                "slippage exclusion active",
                gate="EXECUTION_MONITOR",
            )
            return

        cooloff_block, cooloff_reason = self._is_post_exit_cooldown_active(ticker, now)
        if cooloff_block:
            log.info(f"[{ticker}] {cooloff_reason}")
            _block(REJECT_COOLDOWN, cooloff_reason, gate="POST_EXIT_COOLDOWN")
            return

        # ── RTH hard block (defense-in-depth) ────────────────────────────
        # is_session_active is also checked in _tick(); this guard exists so
        # _execute_entry can NEVER submit an order outside RTH regardless of
        # caller.  CONFIG.session.trade_extended_hours=True bypasses this only
        # for paper/research — NEVER set True in a live config.
        if not CONFIG.session.trade_extended_hours and not is_session_active(now):
            msg = f"outside RTH ({now.strftime('%H:%M')} ET)"
            log.warning(f"[{ticker}] RTH BLOCK: {msg}")
            event_log.log_rth_block(ticker, msg, now.isoformat())
            _block(REJECT_OUTSIDE_WINDOW, msg, gate="RTH_WINDOW")
            return

        # ── Opening-volatility cooldown ─────────────────────────────────
        # Scan can run from session_open, but entries are blocked for a short
        # configurable window after the open to avoid first-15m whipsaws.
        if is_session_active(now) and getattr(self, "_cycle_count", 0) > 0:
            cooldown_mins = max(
                0,
                int(getattr(CONFIG.session, "entry_cooldown_minutes_after_open", 0)),
            )
            mins_from_open = minutes_since_open(now)
            if 0 <= mins_from_open < cooldown_mins:
                msg = (
                    "opening-volatility cooldown "
                    f"({mins_from_open:.1f}m < {cooldown_mins}m after open)"
                )
                log.info(f"[{ticker}] RTH BLOCK: {msg}")
                event_log.log_rth_block(ticker, msg, now.isoformat())
                _block(REJECT_COOLDOWN, msg, gate="OPENING_COOLDOWN")
                return

        # ── Halt machine gate ────────────────────────────────────────────
        # Blocks entries during and immediately after a detected halt event;
        # requires CLEAN_TICKS_REQUIRED clean ticks before entries resume.
        if halt_machine.is_blocked(ticker):
            state = halt_machine.current_state(ticker)
            log.info(f"[{ticker}] HALT GATE: blocked in state={state}")
            _block(BLOCK_HALT_DETECTED, f"halt state={state}", gate="HALT_MACHINE")
            return

        # ── Halt resume spread gate ──────────────────────────────────────
        # After a halt clears, effective spread cap is tightened until first
        # post-halt entry is recorded (post_halt_entry_complete=False).
        spread_mult = halt_machine.resume_spread_multiplier(ticker)
        effective_spread_cap = CONFIG.scanner.max_spread_pct * spread_mult
        if dm.ask > 0 and dm.bid > 0:
            mid = (dm.ask + dm.bid) / 2.0 + EPSILON
            spread_now = (dm.ask - dm.bid) / mid
            if spread_now > effective_spread_cap:
                log.info(
                    f"[{ticker}] SPREAD GATE (post-halt tightened): "
                    f"spread={spread_now:.4%} > cap={effective_spread_cap:.4%}"
                )
                _block(
                    "POST_HALT_SPREAD_BLOCK",
                    f"spread={spread_now:.4%} > cap={effective_spread_cap:.4%}",
                    gate="HALT_RESUME_SPREAD",
                )
                return

        # Retrieve DATA_HEALTH report + feed type attached by universe scanner
        dh_report = getattr(dm, "_dh_report", None)
        feed_type = getattr(dm, "_feed_type", "")
        health_mult = dh_report.size_multiplier if dh_report is not None else 1.0
        degrade_str = (
            "|".join(dh_report.degrade_reasons) if dh_report is not None else ""
        )

        # Compute share count (includes health degrade + halt resume multipliers)
        halt_size_mult = halt_machine.resume_size_multiplier(ticker)
        combined_mult = health_mult * halt_size_mult
        qty = self._risk.size_position(
            setup.entry_price,
            setup.stop_price,
            regime,
            health_size_multiplier=combined_mult,
        )
        if halt_size_mult < 1.0:
            log.info(
                f"[{ticker}] POST-HALT SIZE DOWN: "
                f"halt_mult={halt_size_mult:.2f} → {qty}sh"
            )

        # ── Portfolio gate ─────────────────────────────────────────────────────
        try:
            from risk.portfolio_controller import (
                BLOCK as _PF_BLOCK,
                OpenPosition as _OpenPos,
                evaluate as _pf_eval,
                _sector,
            )
            from events.bus import event_bus as _eb
            from events import current_cycle as _cc
            from events.types import PortfolioGateResult, PositionSizeCapped

            _open = [
                _OpenPos(
                    ticker=_t.ticker,
                    notional=_t.entry_price * _t.shares,
                    sector=_sector(_t.ticker),
                    dollar_volume=getattr(_t, "dollar_volume", 0.0),
                )
                for _t in self._risk.open_trades()
            ]
            _pd = _pf_eval(
                ticker=ticker,
                planned_qty=qty,
                planned_notional=qty * setup.entry_price,
                open_positions=_open,
                regime=regime,
                dollar_volume=getattr(dm, "dollar_volume", 0.0),
                entry_price=setup.entry_price,
                recent_volume=(
                    getattr(dm, "volume", 0.0)
                    if getattr(dm, "volume_1m", None) is None
                    else getattr(dm, "volume_1m", 0.0)
                ),
            )
            _eb.publish(
                PortfolioGateResult(
                    cycle_id=_cc.id,
                    ticker=ticker,
                    decision=_pd.action,
                    reason=_pd.reason,
                    multiplier=_pd.multiplier,
                )
            )
            if _pd.action == _PF_BLOCK:
                log.info(f"[{ticker}] PORTFOLIO GATE BLOCKED: {_pd.reason}")
                _block("PORTFOLIO_GATE_BLOCK", _pd.reason, gate="PORTFOLIO_GATE")
                return
            _qty_before_caps = qty
            if getattr(_pd, "qty_final", 0) > 0:
                qty = _pd.qty_final
            if qty < _qty_before_caps:
                _eb.publish(
                    PositionSizeCapped(
                        cycle_id=_cc.id,
                        ticker=ticker,
                        qty_base=_qty_before_caps,
                        qty_final=qty,
                        cap_reason=getattr(_pd, "cap_reason", ""),
                        cap_values=getattr(_pd, "cap_values", ""),
                    )
                )
                log.info(
                    f"[{ticker}] PORTFOLIO CAP: {_qty_before_caps}sh → {qty}sh "
                    f"({_pd.cap_reason or _pd.reason})"
                )
        except Exception as _pf_exc:
            log.debug("[PortfolioGate] skipped: %s", _pf_exc)

        wash_ok, wash_reason = self._run_wash_guard(ticker)
        if not wash_ok:
            log.warning(f"[{ticker}] {REJECT_WASH_GUARD}: {wash_reason}")
            _block(REJECT_WASH_GUARD, wash_reason, gate="WASH_GUARD")
            return

        # ── Entry quote freshness + runaway slippage guard ───────────────────
        # Final defense before submit: block entries when the quote snapshot is
        # stale or when ask drifted too far above setup.entry_price.
        def _as_float(value, default: float = 0.0) -> float:
            try:
                return float(value)
            except Exception:
                return default

        max_quote_age_s = float(
            max(0.0, getattr(CONFIG.execution, "entry_quote_max_age_seconds", 0.0))
        )
        quote_age_s = None
        if dh_report is not None:
            quote_age_s = _as_float(getattr(dh_report, "quote_age_s", 0.0), 0.0)
        if quote_age_s is not None and max_quote_age_s > 0.0:
            if quote_age_s > max_quote_age_s + EPSILON:
                detail = (
                    f"quote_age={quote_age_s:.1f}s > max={max_quote_age_s:.1f}s "
                    f"(entry={setup.entry_price:.4f} bid={dm.bid:.4f} ask={dm.ask:.4f})"
                )
                log.warning(f"[{ticker}] {REJECT_ENTRY_STALE_QUOTE}: {detail}")
                _block(
                    REJECT_ENTRY_STALE_QUOTE,
                    detail,
                    gate="ENTRY_QUOTE_FRESHNESS",
                )
                return

        ask_now = _as_float(getattr(dm, "ask", 0.0), 0.0)
        entry_ref = _as_float(getattr(setup, "entry_price", 0.0), 0.0)
        if ask_now > EPSILON and entry_ref > EPSILON:
            drift_pct = max(0.0, (ask_now - entry_ref) / entry_ref)
            spread_now = max(0.0, _as_float(getattr(dm, "spread_pct", 0.0), 0.0))
            atr = max(0.0, _as_float(getattr(setup, "atr", 0.0), 0.0))
            atr_pct = atr / max(entry_ref, EPSILON)

            base_pct = float(
                max(
                    0.0,
                    getattr(
                        CONFIG.execution,
                        "entry_runaway_base_pct",
                        CONFIG.execution.limit_slippage_pct,
                    ),
                )
            )
            spread_mult = float(
                max(0.0, getattr(CONFIG.execution, "entry_runaway_spread_mult", 1.5))
            )
            atr_mult = float(
                max(0.0, getattr(CONFIG.execution, "entry_runaway_atr_pct_mult", 0.10))
            )
            max_pct = float(
                max(0.0, getattr(CONFIG.execution, "entry_runaway_max_pct", 0.012))
            )

            allowed_pct = min(
                max_pct,
                max(base_pct, spread_now * spread_mult) + (atr_pct * atr_mult),
            )

            if drift_pct > allowed_pct + EPSILON:
                detail = (
                    f"ask_drift={drift_pct:.4%} > allowed={allowed_pct:.4%} "
                    f"(entry={entry_ref:.4f} ask={ask_now:.4f} "
                    f"spread={spread_now:.4%} atr_pct={atr_pct:.4%})"
                )
                log.warning(f"[{ticker}] {REJECT_ENTRY_RUNAWAY}: {detail}")
                _block(REJECT_ENTRY_RUNAWAY, detail, gate="ENTRY_SLIPPAGE_GUARD")
                return

        # Submit entry through OrderManager (TTL, duplicate protection, logging)
        from config.constants import ORDER_REJECTED

        mo = order_manager.submit(
            self._broker,
            ticker=ticker,
            side="buy",
            qty=qty,
            limit_price=setup.entry_price,
            signal_price=setup.entry_price,
            atr_1m=setup.atr,
            signal_timestamp=now,
            sqs_score=setup.setup_quality_score,
            rvol=dm.rvol,
            spread_at_fill=setup.spread_pct,
            bid_at_signal=dm.bid,
            ask_at_signal=dm.ask,
            now=now,
        )

        if mo is None or mo.status == ORDER_REJECTED:
            log.error(
                f"[{ticker}] ORDER FAILED: "
                f"{mo.reason if mo else 'pre-flight rejected by OrderManager'}"
            )
            _block(
                "ORDER_SUBMIT_FAILED",
                mo.reason if mo else "pre-flight rejected by OrderManager",
                gate="ORDER_MANAGER",
            )
            return

        # Register with risk manager (full determinism audit trail)
        chosen_over = getattr(sel, "chosen_over", [])
        trade = self._risk.open_trade(
            ticker=ticker,
            setup=setup,
            regime=regime,
            demand_score=dm.demand_score,
            chosen_over="; ".join(chosen_over[:5]),
            selection_reason=getattr(sel, "selection_reason", ""),
            universe_rank=candidate.universe_rank,
            size_degrade_reason=degrade_str,
            health_size_multiplier=combined_mult,
            feed_type=feed_type,
            shares_override=qty,
            now=now,
        )

        # Override entry price with actual fill (keep limit_price if still resting)
        if mo.filled_price and mo.filled_price > 0:
            trade.entry_price = mo.filled_price
            trade.high_watermark = mo.filled_price
        else:
            trade.high_watermark = setup.entry_price

        exec_metrics = getattr(mo, "execution_metrics", None) or {}
        if exec_metrics:
            existing_notes = str(getattr(trade, "notes", "") or "").strip()
            exec_blob = json.dumps({"execution": exec_metrics}, separators=(",", ":"))
            trade.notes = f"{existing_notes} {exec_blob}".strip()

        # Mark post-halt first entry — clears size/spread tightening
        halt_machine.on_post_halt_entry(ticker)

        # Log the trade
        self._logger.log_open(trade, dm)

        log.info(
            f"[{ticker}] ✓ ENTERED "
            f"{trade.shares}sh @ ${mo.filled_price or setup.entry_price:.4f} | "
            f"stop=${trade.stop_price:.2f} "
            f"target=${trade.target_price:.2f} | "
            f"DemandScore={dm.demand_score:.3f} "
            f"SQS={setup.setup_quality_score:.3f} | "
            f"regime={regime} | "
            f"break={setup.break_level_name} | "
            f"feed={feed_type}"
            + (f" [DEGRADE:{degrade_str}]" if degrade_str else "")
            + (f" [HALT_SIZE:{halt_size_mult:.2f}]" if halt_size_mult < 1.0 else "")
        )
        self._log_blackbox_decision(
            ticker=ticker,
            accepted=True,
            reason="ENTERED",
            now=now,
            gate_results={
                "qty": trade.shares,
                "entry_price": mo.filled_price or setup.entry_price,
                "sqs": setup.setup_quality_score,
                "demand_score": dm.demand_score,
                "regime": regime,
            },
        )

    # ─── Exit processing ──────────────────────────────────────────────────────

    def _process_exits(self) -> None:
        open_trades = self._risk.open_trades()
        if not open_trades:
            return

        # Fetch current quotes/prices for all open tickers
        tickers = list({t.ticker for t in open_trades})
        quotes = self._fetcher.fetch_quotes(tickers)
        price_map = {t: q.get("bid", 0.0) for t, q in quotes.items()}
        vwap_map = {t: self._get_vwap(t) for t in tickers}
        atr_map = {t: self._get_atr(t) for t in tickers}

        # Lifecycle manager produces one event per trade per tick
        events = self._lifecycle.evaluate_all(
            open_trades=open_trades,
            price_map=price_map,
            vwap_map=vwap_map,
            atr_map=atr_map,
            now=now_et(),
        )

        trade_index = {t.trade_id: t for t in open_trades}
        for evt in events:
            trade = trade_index.get(evt.trade_id)
            if trade is None:
                continue
            self._handle_lifecycle_event(evt, trade, price_map)

    def _handle_lifecycle_event(
        self,
        evt: LifecycleEvent,
        trade,
        price_map: Dict,
    ) -> None:
        """Execute broker action and update risk/log for a lifecycle event."""
        ticker = evt.ticker
        price = price_map.get(ticker, trade.entry_price)

        if evt.shares_to_sell > 0:
            # Route all exits through OrderManager (TTL, duplicate protection)
            from config.constants import ORDER_REJECTED

            if not evt.is_close and self._has_pending_exit_for_trade(trade.trade_id):
                return

            mo = order_manager.submit(
                self._broker,
                ticker=ticker,
                side="sell",
                qty=evt.shares_to_sell,
                limit_price=price,
                now=datetime.now(ET),
            )
            if mo is None or mo.status == ORDER_REJECTED:
                log.error(
                    f"[{ticker}] LIFECYCLE SELL FAILED: "
                    f"{mo.reason if mo else 'pre-flight rejected'}"
                )
                return
            fill = mo.filled_price if mo.filled_price and mo.filled_price > 0 else price
            if not evt.is_close:
                self._register_pending_exit_order(
                    trade=trade,
                    evt=evt,
                    order=mo,
                    expected_price=price,
                )
                self._reconcile_pending_exit_orders([mo])
        else:
            fill = price
            mo = None

        if evt.is_close:
            self._risk.close_trade(trade, fill, evt.reason)
            self._mark_ticker_exit(trade.ticker, now_et())
            self._logger.log_close(trade)
            log.info(
                f"[{ticker}] ✖ EXIT {evt.event} {evt.shares_to_sell}sh "
                f"@ ${fill:.4f} | {evt.reason}"
            )
            if mo is not None and trade.risk_per_share > 0:
                slippage_monitor.record_fill(
                    ticker=ticker,
                    expected_price=price,
                    fill_price=fill,
                    spread_pct=0.0,
                    r_value=trade.risk_per_share * trade.shares,
                )
        elif evt.shares_to_sell > 0:
            # Partial exits are reconciled on confirmed fills only.
            pass
        else:
            # Stop/trail adjustment only — no broker action needed
            if evt.new_stop > 0:
                log.debug(
                    f"[{ticker}] ▲ {evt.event} stop→${evt.new_stop:.4f} | {evt.reason}"
                )

    def _register_pending_exit_order(
        self, trade, evt: LifecycleEvent, order, expected_price: float
    ) -> None:
        self._pending_exit_orders[order.order_id] = {
            "trade": trade,
            "trade_id": trade.trade_id,
            "ticker": trade.ticker,
            "accounted_filled": 0,
            "is_close": bool(evt.is_close),
            "new_state": evt.new_lifecycle_state,
            "reason": evt.reason,
            "expected_price": expected_price,
        }

    def _has_pending_exit_for_trade(self, trade_id: str) -> bool:
        for pending in self._pending_exit_orders.values():
            if pending.get("trade_id") == trade_id:
                return True
        return False

    def _reconcile_pending_exit_orders(self, changed_orders: List[object]) -> None:
        if not changed_orders or not self._pending_exit_orders:
            return

        from config.constants import (
            ORDER_CANCELLED,
            ORDER_FILLED,
            ORDER_PARTIAL,
            ORDER_REJECTED,
            ORDER_REPLACED,
            ORDER_STUCK,
            ORDER_SUBMITTED,
        )

        terminal_without_fill = {
            ORDER_REJECTED,
            ORDER_CANCELLED,
            ORDER_STUCK,
            ORDER_REPLACED,
        }

        for order in changed_orders:
            pending = self._pending_exit_orders.get(order.order_id)
            if pending is None:
                continue

            trade = pending["trade"]
            prior_filled = int(pending.get("accounted_filled", 0) or 0)
            broker_filled = int(getattr(order, "filled_qty", 0) or 0)
            delta = max(0, broker_filled - prior_filled)

            if delta > 0 and not pending.get("is_close"):
                trade.shares_remaining = max(0, int(trade.shares_remaining) - delta)
                pending["accounted_filled"] = broker_filled

                if pending.get("new_state"):
                    trade.lifecycle_state = pending["new_state"]

                fill_price = getattr(order, "filled_price", 0.0) or pending.get(
                    "expected_price", trade.entry_price
                )
                self._logger.log_partial(
                    trade=trade,
                    shares_sold=delta,
                    fill_price=fill_price,
                    reason=str(pending.get("reason", "")),
                )
                log.info(
                    f"[{trade.ticker}] ◑ PARTIAL FILLED {delta}sh @ ${fill_price:.4f} | "
                    f"remaining={trade.shares_remaining}"
                )

            status = getattr(order, "status", ORDER_SUBMITTED)
            if status in terminal_without_fill:
                self._pending_exit_orders.pop(order.order_id, None)
                continue

            if status == ORDER_FILLED:
                self._pending_exit_orders.pop(order.order_id, None)
            elif status == ORDER_PARTIAL:
                self._pending_exit_orders[order.order_id] = pending

    # ─── Helpers ──────────────────────────────────────────────────────────────

    def _get_vwap(self, ticker: str) -> float:
        """Best-effort VWAP from bar cache."""
        try:
            from signals.indicators import calc_vwap
            from config.constants import TF_1M

            df = bar_cache.get_tf(ticker, TF_1M)
            if df is not None and not df.empty:
                return calc_vwap(df)
        except Exception:
            pass
        return 0.0

    def _state_for_ticker(self, ticker: str) -> dict:
        if not hasattr(self, "_ticker_states") or self._ticker_states is None:
            self._ticker_states = {}
        return self._ticker_states.setdefault(
            ticker,
            {
                "last_exit_time": None,
                "cool_off_expiry": None,
                "total_trades_today": 0,
            },
        )

    def _mark_ticker_exit(self, ticker: str, ts: datetime) -> None:
        state = self._state_for_ticker(ticker)
        cooldown_mins = max(
            0, int(getattr(CONFIG.execution, "post_exit_cooldown_minutes", 2))
        )
        expiry = ts
        if cooldown_mins > 0:
            from datetime import timedelta

            expiry = ts + timedelta(minutes=cooldown_mins)
        state["last_exit_time"] = ts
        state["cool_off_expiry"] = expiry
        state["total_trades_today"] = int(state.get("total_trades_today", 0) or 0) + 1

    def _is_post_exit_cooldown_active(
        self, ticker: str, now: datetime
    ) -> tuple[bool, str]:
        state = self._state_for_ticker(ticker)
        expiry = state.get("cool_off_expiry")
        if expiry is None:
            return False, ""
        remaining = (expiry - now).total_seconds()
        if remaining <= 0:
            return False, ""
        return (
            True,
            f"{REJECT_COOLDOWN}({ticker}: {remaining/60.0:.1f}m < {CONFIG.execution.post_exit_cooldown_minutes}m post-exit)",
        )

    def _run_wash_guard(self, ticker: str) -> tuple[bool, str]:
        if not bool(getattr(CONFIG.execution, "enable_wash_guard", True)):
            return True, "disabled"

        def _supports(method_name: str) -> bool:
            if not hasattr(self._broker, method_name):
                return False
            method = getattr(self._broker, method_name)
            if not callable(method):
                return False
            try:
                from unittest.mock import Mock

                if (
                    isinstance(self._broker, Mock)
                    and method_name not in self._broker.__dict__
                ):
                    return False
            except Exception:
                pass
            return True

        active_local = [o for o in order_manager.active_orders() if o.ticker == ticker]
        if active_local:
            return False, f"local_active_orders={len(active_local)}"

        cancelled = 0
        cancel_fn = getattr(self._broker, "cancel_open_orders", None)
        if _supports("cancel_open_orders"):
            try:
                cancelled = int(cancel_fn(ticker=ticker) or 0)
            except Exception as exc:
                return False, f"cancel_open_orders_failed={exc}"

        list_fn = getattr(self._broker, "list_open_orders", None)
        if _supports("list_open_orders"):
            try:
                broker_open = list_fn(ticker=ticker) or []
            except Exception as exc:
                return False, f"list_open_orders_failed={exc}"
            if not isinstance(broker_open, (list, tuple)):
                broker_open = []
            if broker_open:
                return False, f"broker_open_orders={len(broker_open)}"

        has_pos_fn = getattr(self._broker, "has_open_position", None)
        if _supports("has_open_position"):
            try:
                if bool(has_pos_fn(ticker)):
                    return False, "broker_position_not_flat"
            except Exception as exc:
                return False, f"has_open_position_failed={exc}"

        return True, f"cleared={cancelled}"

    def _log_blackbox_decision(
        self,
        ticker: str,
        accepted: bool,
        reason: str,
        now: datetime,
        gate_results: Optional[dict] = None,
    ) -> None:
        payload = {
            "accepted": accepted,
            "reason": reason,
            "timestamp": now.isoformat(),
            "ticker_state": self._state_for_ticker(ticker),
        }
        if gate_results:
            payload["gate_results"] = gate_results

        event_log.log_decision_evaluation(
            ticker=ticker,
            accepted=accepted,
            reason=reason,
            gate_results=payload,
        )
        session_recorder.record_decision(
            ticker=ticker,
            accepted=accepted,
            reason=reason,
            gate_results=payload,
            ts=now,
        )
        log.info(
            "[BLACKBOX] %s", json.dumps({"ticker": ticker, **payload}, default=str)
        )

    def _get_atr(self, ticker: str) -> float:
        """Best-effort ATR from bar cache."""
        try:
            from signals.structure import calc_atr
            from config.constants import TF_5M

            df = bar_cache.get_tf(ticker, TF_5M)
            if df is not None and not df.empty:
                return calc_atr(df)
        except Exception:
            pass
        return 0.50

    # ─── Regime ───────────────────────────────────────────────────────────────

    def _update_regime(self) -> str:
        spy_bars_dict = self._fetcher.fetch_all(["SPY"], CONFIG.data.lookback_days)
        if "SPY" in spy_bars_dict:
            mtf = self._pipeline.build("SPY", spy_bars_dict["SPY"])
            self._regime.update(mtf)
        return self._regime.detect()

    # ─── Session lifecycle ────────────────────────────────────────────────────

    def _on_new_day(self, today: str) -> None:
        log.info(f"\n{'='*60}")
        log.info(f"NEW TRADING DAY: {today}")
        log.info(f"{'='*60}")
        self._session_date = today
        self._risk.reset_session()
        self._logger.new_day(today)
        halt_machine.reset_all()
        order_manager.reset()
        self._pending_exit_orders.clear()
        self._ticker_states.clear()

        # Determine run_id from governance if available
        try:
            from execution.governance import governance

            run_id = governance.run_id
        except Exception:
            run_id = ""
        mode = "live" if not CONFIG.execution.paper_mode else "paper"
        event_log.new_day(today, run_id=run_id)
        session_recorder.new_day(today)
        event_log.log_session_start(today, run_id=run_id, mode=mode)

    def _end_of_day_report(self) -> None:
        trades = self._risk.all_trades()
        log.info("\n" + "=" * 60)
        log.info(f"END OF DAY REPORT — {self._session_date}")
        log.info(f"Total trades: {len(trades)}")
        log.info(f"Daily PnL: ${self._risk.daily_pnl():.2f}")

        wins = [t for t in trades if t.pnl > 0]
        losses = [t for t in trades if t.pnl < 0]
        log.info(f"Wins: {len(wins)}  Losses: {len(losses)}")

        daily_pnl = self._risk.daily_pnl()
        if trades:
            avg_r = sum(t.pnl_r for t in trades) / len(trades)
            daily_pnl_r = sum(t.pnl_r for t in trades)
            log.info(f"Avg R: {avg_r:.2f}")
        else:
            daily_pnl_r = 0.0

        try:
            from trade_log.event_log import guard_saves_snapshot

            guard_saves = guard_saves_snapshot(self._session_date)
        except Exception:
            guard_saves = {
                "ENTRY_QUOTE_STALE": 0,
                "ENTRY_PRICE_RUNAWAY": 0,
                "total": 0,
            }
        guard_saves_line = (
            f"guard_saves stale={guard_saves.get('ENTRY_QUOTE_STALE', 0)} "
            f"runaway={guard_saves.get('ENTRY_PRICE_RUNAWAY', 0)} "
            f"total={guard_saves.get('total', 0)}"
        )
        log.info(guard_saves_line)
        log.info("=" * 60)

        if self._session_date:
            event_log.log_session_end(
                date_str=self._session_date,
                total_trades=len(trades),
                daily_pnl=daily_pnl,
                daily_pnl_r=daily_pnl_r,
                guard_saves=guard_saves,
                guard_saves_line=guard_saves_line,
            )
            event_log.close()
