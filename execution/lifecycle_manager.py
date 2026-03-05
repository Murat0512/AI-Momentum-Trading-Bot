"""
execution/lifecycle_manager.py - Manages active trades from Entry to Close.
Implements scaling, trailing, and emergency exit logic.
"""

from __future__ import annotations

import logging
from typing import Any, Dict

from config.settings import CONFIG

logger = logging.getLogger(__name__)


class LifecycleManager:
    def __init__(self):
        self.lc_cfg = CONFIG.lifecycle
        self.risk_cfg = CONFIG.risk

    def evaluate_active_trade(
        self,
        position: Dict[str, Any],
        current_price: float,
        current_atr: float,
        current_volume_1m: float,
        avg_volume_1m: float,
        minutes_since_entry: int,
    ) -> Dict[str, Any]:
        """
        Detailed step-by-step evaluation of an open position.
        Matches LifecycleConfig state machine.
        """
        ticker = str(position["ticker"])
        entry_price = float(position["entry_price"])
        if "initial_stop_loss" not in position:
            position["initial_stop_loss"] = float(position["stop_loss"])
        initial_stop = float(position.get("initial_stop_loss", position["stop_loss"]))
        r_unit = abs(entry_price - initial_stop)

        if r_unit <= 0:
            return {
                "action": "EXIT_ALL",
                "shares_to_sell": int(position.get("shares", 0)),
                "new_stop": None,
                "reason": "Invalid R-unit: stop equals entry",
            }

        current_r = (float(current_price) - entry_price) / r_unit

        position["high_watermark"] = max(
            float(position.get("high_watermark", current_price)),
            float(current_price),
        )

        instructions = {
            "action": "HOLD",
            "shares_to_sell": 0,
            "new_stop": None,
            "reason": "",
        }

        if minutes_since_entry >= int(self.lc_cfg.time_continuation_minutes):
            if current_price <= entry_price:
                return {
                    "action": "EXIT_ALL",
                    "shares_to_sell": int(position.get("shares", 0)),
                    "new_stop": None,
                    "reason": "Time continuation expired (No progress)",
                }

        if avg_volume_1m > 0 and current_volume_1m < (
            avg_volume_1m * float(self.lc_cfg.volume_fade_threshold)
        ):
            return {
                "action": "EXIT_ALL",
                "shares_to_sell": int(position.get("shares", 0)),
                "new_stop": None,
                "reason": "Volume fade detected below threshold",
            }

        if self.lc_cfg.exit_on_vwap_loss and bool(position.get("below_vwap", False)):
            return {
                "action": "EXIT_ALL",
                "shares_to_sell": int(position.get("shares", 0)),
                "new_stop": None,
                "reason": "Price closed below VWAP",
            }

        if current_r >= float(self.lc_cfg.breakeven_r) and not bool(
            position.get("partial_1_taken", False)
        ):
            shares = int(position.get("shares", 0))
            shares_to_sell = int(shares * float(self.lc_cfg.partial1_exit_pct))
            shares_to_sell = max(1, shares_to_sell) if shares > 0 else 0
            instructions = {
                "action": "PARTIAL_EXIT",
                "shares_to_sell": shares_to_sell,
                "new_stop": entry_price,
                "reason": "Target 1R reached: Scaling 25% and moving to Breakeven",
            }
            position["partial_1_taken"] = True
            position["stop_loss"] = entry_price
            position["shares_already_sold"] = (
                int(position.get("shares_already_sold", 0)) + shares_to_sell
            )
            return instructions

        if current_r >= float(self.lc_cfg.partial_exit_r) and not bool(
            position.get("partial_2_taken", False)
        ):
            shares_total = int(position.get("shares", 0))
            remaining_shares = max(
                0,
                shares_total - int(position.get("shares_already_sold", 0)),
            )
            shares_to_sell = int(remaining_shares * float(self.lc_cfg.partial_exit_pct))
            shares_to_sell = max(1, shares_to_sell) if remaining_shares > 0 else 0
            new_stop = entry_price + (0.5 * r_unit)
            instructions = {
                "action": "PARTIAL_EXIT",
                "shares_to_sell": shares_to_sell,
                "new_stop": new_stop,
                "reason": "Target 1.5R reached: Scaling 50% of remainder",
            }
            position["partial_2_taken"] = True
            position["stop_loss"] = max(
                float(position.get("stop_loss", initial_stop)), new_stop
            )
            position["shares_already_sold"] = (
                int(position.get("shares_already_sold", 0)) + shares_to_sell
            )
            return instructions

        if current_r >= float(self.lc_cfg.trail_start_r):
            trail_stop = float(position["high_watermark"]) - (
                float(current_atr) * float(self.lc_cfg.trail_atr_multiplier)
            )
            current_stop = float(position.get("stop_loss", initial_stop))
            if trail_stop > current_stop:
                position["stop_loss"] = trail_stop
                return {
                    "action": "ADJUST_STOP",
                    "shares_to_sell": 0,
                    "new_stop": round(trail_stop, 2),
                    "reason": f"Trailing at {self.lc_cfg.trail_atr_multiplier}x ATR",
                }

        if float(self.lc_cfg.hard_target_r) > 0 and current_r >= float(
            self.lc_cfg.hard_target_r
        ):
            remaining = max(
                0,
                int(position.get("shares", 0))
                - int(position.get("shares_already_sold", 0)),
            )
            return {
                "action": "EXIT_ALL",
                "shares_to_sell": remaining,
                "new_stop": None,
                "reason": "Hard R-Target reached",
            }

        logger.debug("[%s] HOLD lifecycle state", ticker)
        return instructions
