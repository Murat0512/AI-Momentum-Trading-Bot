"""
execution/order_processor.py - Handles order submission, slippage protection,
and the limit order lifecycle.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any, Dict, Optional

from config.settings import CONFIG

logger = logging.getLogger(__name__)


class ExecutionEngine:
    def __init__(self, broker_adapter: Any):
        self.broker = broker_adapter
        self.exec_cfg = CONFIG.execution
        self.order_cfg = CONFIG.order_manager
        self.slip_cfg = CONFIG.slippage
        self._size_reduce_flags: Dict[str, bool] = {}

    @staticmethod
    def _extract_status(order_status: Any) -> str:
        if isinstance(order_status, dict):
            return str(order_status.get("status", "")).lower()
        return str(getattr(order_status, "status", "")).lower()

    @staticmethod
    def _quote_age_seconds(quote_ts: Any) -> float:
        if isinstance(quote_ts, datetime):
            return max(0.0, time.time() - quote_ts.timestamp())
        return max(0.0, time.time() - float(quote_ts))

    def _calculate_max_chase_price(
        self,
        setup_price: float,
        current_ask: float,
        spread_pct: float,
        atr: float,
    ) -> float:
        """
        Implements the dynamic runaway gate from ExecutionConfig.
        """
        if setup_price <= 0:
            return current_ask

        base_component = max(
            self.exec_cfg.entry_runaway_base_pct,
            spread_pct * self.exec_cfg.entry_runaway_spread_mult,
        )

        atr_component = (atr / setup_price) * self.exec_cfg.entry_runaway_atr_pct_mult

        total_allowed_drift_pct = min(
            base_component + atr_component,
            self.exec_cfg.entry_runaway_max_pct,
        )

        return setup_price * (1 + total_allowed_drift_pct)

    def prepare_entry(
        self,
        ticker: str,
        setup_entry_price: float,
        shares: int,
        current_quote: Dict[str, Any],
        atr: float,
    ) -> Dict[str, Any]:
        """
        Rule 5: Detailed pre-flight checks before order submission.
        """
        ask_price = float(current_quote["ask"])
        bid_price = float(current_quote["bid"])
        quote_age = self._quote_age_seconds(current_quote["timestamp"])
        spread_pct = (ask_price - bid_price) / max(ask_price, 1e-9)

        if quote_age > self.exec_cfg.entry_quote_max_age_seconds:
            return {
                "action": "REJECT",
                "reason": f"Stale quote: {quote_age:.2f}s old",
            }

        max_price = self._calculate_max_chase_price(
            setup_entry_price,
            ask_price,
            spread_pct,
            float(atr),
        )
        if ask_price > max_price:
            return {
                "action": "REJECT",
                "reason": f"Price ran away. Ask: {ask_price} > Max: {max_price:.4f}",
            }

        limit_price = ask_price * (1 + self.exec_cfg.limit_slippage_pct)

        return {
            "action": "EXECUTE",
            "ticker": ticker,
            "shares": int(shares),
            "limit_price": round(limit_price, 2),
            "order_type": self.exec_cfg.order_type,
        }

    def process_order_lifecycle(
        self,
        order_id: str,
        submission_time: float,
    ) -> Any:
        """
        Handles TTL and stuck-order logic from OrderManagerConfig.
        """
        order_status = self.broker.get_order_status(order_id)
        age = time.time() - submission_time
        status = self._extract_status(order_status)

        if status == "open" and age > self.order_cfg.limit_order_ttl_seconds:
            logger.warning(
                "Order %s expired TTL. Triggering Cancel/Replace.",
                order_id,
            )
            return self.broker.cancel_and_reprice(
                order_id,
                self.exec_cfg.cancel_replace_reprice_step,
            )

        if status == "open" and age > self.order_cfg.stuck_order_seconds:
            logger.error(
                "Order %s is STUCK. Requires intervention or emergency market sell.",
                order_id,
            )
            if self.exec_cfg.allow_market_sell_emergency:
                return self.broker.market_exit(order_id)

        return order_status

    def log_slippage(
        self,
        ticker: str,
        target_price: float,
        actual_fill_price: float,
        risk_per_share: Optional[float] = None,
    ) -> float:
        """
        Evaluates slippage against SlippageConfig.

        Returns slippage in bps.
        """
        if target_price <= 0:
            return 0.0

        slippage_bps = ((actual_fill_price - target_price) / target_price) * 10_000

        if slippage_bps > self.slip_cfg.warn_bps:
            logger.warning("[%s] High Slippage: %.2f bps", ticker, slippage_bps)

        if risk_per_share is not None and risk_per_share > 0:
            slippage_r = abs(actual_fill_price - target_price) / risk_per_share
            if slippage_r >= self.slip_cfg.size_reduce_r_threshold:
                self._size_reduce_flags[ticker] = True

        return float(slippage_bps)

    def should_reduce_size(self, ticker: str) -> bool:
        return bool(self._size_reduce_flags.get(ticker, False))
