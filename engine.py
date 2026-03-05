"""
engine.py - Additive orchestrator for the milestone glue flow.

This file is intentionally separate from execution/engine.py to avoid
regressing the production engine path.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict

from config.settings import CONFIG
from execution.lifecycle_manager import LifecycleManager
from execution.order_processor import ExecutionEngine
from risk.manager import RiskManager
from scanner import MomentumScanner

logger = logging.getLogger(__name__)


class MomentumEngine:
    def __init__(self, broker_adapter: Any, data_provider: Any):
        self.broker = broker_adapter
        self.data = data_provider

        self.scanner = MomentumScanner()
        self.risk = RiskManager()
        self.execution = ExecutionEngine(self.broker)
        self.lifecycle = LifecycleManager()

        self.active_positions: Dict[str, Dict[str, Any]] = {}
        self.daily_pnl_pct = 0.0
        self.trades_today = 0

    def run_cycle(self) -> None:
        logger.info("--- Starting New Trading Cycle ---")
        self._sync_portfolio()
        self._manage_active_trades()
        if self._can_open_new_positions():
            self._process_new_opportunities()

    def _sync_portfolio(self) -> None:
        return

    def _manage_active_trades(self) -> None:
        for ticker, pos in list(self.active_positions.items()):
            snapshot = self.data.get_latest_snapshot(ticker)
            instruction = self.lifecycle.evaluate_active_trade(
                position=pos,
                current_price=float(snapshot["price"]),
                current_atr=float(snapshot["atr"]),
                current_volume_1m=float(snapshot["volume_1m"]),
                avg_volume_1m=float(snapshot["avg_vol_1m"]),
                minutes_since_entry=max(
                    0,
                    int((time.time() - float(pos.get("entry_time", time.time()))) / 60),
                ),
            )
            self._handle_lifecycle_instruction(ticker, instruction)

    def _process_new_opportunities(self) -> None:
        raw_data = self.data.get_universe_data(CONFIG.scanner.seed_tickers)
        candidates = self.scanner.filter_universe(raw_data)
        if candidates is None or candidates.empty:
            return

        for _, row in candidates.iterrows():
            ticker = str(row["ticker"])
            if ticker in self.active_positions:
                continue

            stop_loss = float(row["price"]) - float(row["atr"])
            risk_result = self.risk.get_position_size(
                ticker=ticker,
                entry_price=float(row["price"]),
                stop_loss=stop_loss,
                ticker_dvol=float(row["dollar_volume"]),
                current_positions=self.active_positions,
            )
            if not risk_result.get("can_trade", False):
                logger.info(
                    "[%s] Entry skipped: %s", ticker, risk_result.get("reason", "")
                )
                continue

            quote = {
                "ask": float(row["price"]),
                "bid": float(row["price"]) - 0.01,
                "timestamp": time.time(),
            }
            exec_prep = self.execution.prepare_entry(
                ticker=ticker,
                setup_entry_price=float(row["price"]),
                shares=int(risk_result["shares"]),
                current_quote=quote,
                atr=float(row["atr"]),
            )

            if exec_prep.get("action") != "EXECUTE":
                logger.info(
                    "[%s] Execution blocked: %s", ticker, exec_prep.get("reason", "")
                )
                continue

            self.broker.submit_order(
                ticker=ticker,
                qty=int(exec_prep["shares"]),
                limit_price=float(exec_prep["limit_price"]),
                side="buy",
            )

            self.active_positions[ticker] = {
                "ticker": ticker,
                "shares": int(exec_prep["shares"]),
                "entry_price": float(row["price"]),
                "stop_loss": stop_loss,
                "initial_stop_loss": stop_loss,
                "entry_time": time.time(),
                "high_watermark": float(row["price"]),
                "partial_1_taken": False,
                "partial_2_taken": False,
                "shares_already_sold": 0,
                "below_vwap": False,
            }
            self.trades_today += 1
            logger.info("[%s] Order Submitted: %s shares", ticker, exec_prep["shares"])

    def _can_open_new_positions(self) -> bool:
        if self.daily_pnl_pct <= -float(CONFIG.risk.daily_loss_cap_pct):
            return False
        if self.trades_today >= int(CONFIG.risk.max_trades_per_day):
            return False
        if len(self.active_positions) >= int(CONFIG.portfolio.max_concurrent_positions):
            return False
        return True

    def _handle_lifecycle_instruction(
        self, ticker: str, instruction: Dict[str, Any]
    ) -> None:
        action = str(instruction.get("action", "HOLD"))
        if action == "HOLD":
            return

        if action == "EXIT_ALL":
            self.broker.close_position(ticker)
            self.active_positions.pop(ticker, None)
            logger.warning("[%s] EXIT ALL: %s", ticker, instruction.get("reason", ""))
            return

        if action == "PARTIAL_EXIT":
            qty = int(instruction.get("shares_to_sell", 0) or 0)
            if qty <= 0:
                return
            pos = self.active_positions.get(ticker)
            limit_price = float(pos.get("entry_price", 0.0)) if pos is not None else 0.0
            self.broker.submit_order(
                ticker=ticker,
                qty=qty,
                side="sell",
                limit_price=limit_price,
            )
            if pos is not None:
                pos["shares_already_sold"] = (
                    int(pos.get("shares_already_sold", 0)) + qty
                )
                if instruction.get("new_stop") is not None:
                    pos["stop_loss"] = float(instruction["new_stop"])
            logger.info("[%s] %s", ticker, instruction.get("reason", ""))
            return

        if action == "ADJUST_STOP":
            pos = self.active_positions.get(ticker)
            if pos is not None and instruction.get("new_stop") is not None:
                pos["stop_loss"] = float(instruction["new_stop"])
            logger.info("[%s] %s", ticker, instruction.get("reason", ""))
