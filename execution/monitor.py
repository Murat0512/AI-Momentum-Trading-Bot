"""
execution/monitor.py — Entry execution quality monitoring.

Tracks three entry-quality signals:
  1) Slippage-to-ATR (%): |fill - signal| / ATR(1m)
  2) Fill latency (ms): fill_timestamp - signal_timestamp
  3) Fill context snapshot: SQS, RVOL, spread

Writes metrics to logs/execution_YYYY-MM-DD.csv for milestone analysis.
"""

from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import pytz

from config.constants import EPSILON
from trade_log.session_recorder import session_recorder

log = logging.getLogger(__name__)
ET = pytz.timezone("America/New_York")

LOGS_DIR = Path(__file__).resolve().parent.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

EXECUTION_LOG_COLUMNS = [
    "ts",
    "order_id",
    "ticker",
    "signal_price",
    "fill_price",
    "slippage_abs",
    "atr_1m",
    "slippage_to_atr_pct",
    "latency_ms",
    "sqs_score",
    "rvol",
    "spread_at_fill",
    "bid_at_signal",
    "ask_at_signal",
    "excluded_after_fill",
]


@dataclass
class ExecutionMetrics:
    slippage_abs: float = 0.0
    slippage_to_atr_pct: float = 0.0
    latency_ms: int = 0
    sqs_score: float = 0.0
    rvol: float = 0.0
    spread_at_fill: float = 0.0
    excluded_after_fill: bool = False


class ExecutionMonitor:
    def __init__(self) -> None:
        self._logged_order_ids: set[str] = set()
        self._excluded_tickers: set[str] = set()

    def calculate_entry_efficiency(
        self,
        signal_price: float,
        fill_price: float,
        atr_1m: float,
    ) -> tuple[float, float]:
        slippage_abs = abs(float(fill_price) - float(signal_price))
        if atr_1m <= EPSILON:
            return round(slippage_abs, 4), 0.0
        slippage_to_atr_pct = (slippage_abs / float(atr_1m)) * 100.0
        if slippage_to_atr_pct > 25.0:
            log.critical(
                "SLOPPY_FILL: slippage %.4f is %.1f%% of ATR(1m)=%.4f",
                slippage_abs,
                slippage_to_atr_pct,
                atr_1m,
            )
        return round(slippage_abs, 4), round(slippage_to_atr_pct, 2)

    def log_execution_latency(
        self,
        signal_timestamp: Optional[datetime],
        fill_timestamp: Optional[datetime],
    ) -> int:
        if signal_timestamp is None or fill_timestamp is None:
            return 0
        return max(0, int((fill_timestamp - signal_timestamp).total_seconds() * 1000.0))

    def record_execution_context(
        self,
        ticker: str,
        sqs_score: float,
        rvol: float,
        spread_at_fill: float,
    ) -> dict:
        return {
            "ticker": ticker,
            "sqs_score": round(float(sqs_score or 0.0), 4),
            "rvol": round(float(rvol or 0.0), 4),
            "spread_at_fill": round(float(spread_at_fill or 0.0), 6),
        }

    def on_entry_filled(
        self,
        *,
        order_id: str,
        ticker: str,
        signal_price: float,
        fill_price: float,
        atr_1m: float,
        signal_timestamp: Optional[datetime],
        fill_timestamp: Optional[datetime],
        sqs_score: float,
        rvol: float,
        spread_at_fill: float,
        bid_at_signal: float = 0.0,
        ask_at_signal: float = 0.0,
    ) -> ExecutionMetrics:
        if order_id in self._logged_order_ids:
            return ExecutionMetrics()

        slippage_abs, slippage_to_atr_pct = self.calculate_entry_efficiency(
            signal_price=signal_price,
            fill_price=fill_price,
            atr_1m=atr_1m,
        )
        latency_ms = self.log_execution_latency(signal_timestamp, fill_timestamp)
        ctx = self.record_execution_context(
            ticker=ticker,
            sqs_score=sqs_score,
            rvol=rvol,
            spread_at_fill=spread_at_fill,
        )

        if spread_at_fill <= 0 and bid_at_signal > 0 and ask_at_signal > 0:
            mid = (bid_at_signal + ask_at_signal) / 2.0 + EPSILON
            spread_at_fill = (ask_at_signal - bid_at_signal) / mid

        metrics = ExecutionMetrics(
            slippage_abs=slippage_abs,
            slippage_to_atr_pct=slippage_to_atr_pct,
            latency_ms=latency_ms,
            sqs_score=float(ctx["sqs_score"]),
            rvol=float(ctx["rvol"]),
            spread_at_fill=float(ctx["spread_at_fill"]),
            excluded_after_fill=slippage_to_atr_pct > 25.0,
        )

        if metrics.excluded_after_fill:
            self._excluded_tickers.add(ticker)

        self._append_csv(
            {
                "ts": datetime.now(ET).isoformat(),
                "order_id": order_id,
                "ticker": ticker,
                "signal_price": round(float(signal_price or 0.0), 4),
                "fill_price": round(float(fill_price or 0.0), 4),
                "slippage_abs": metrics.slippage_abs,
                "atr_1m": round(float(atr_1m or 0.0), 4),
                "slippage_to_atr_pct": metrics.slippage_to_atr_pct,
                "latency_ms": metrics.latency_ms,
                "sqs_score": metrics.sqs_score,
                "rvol": metrics.rvol,
                "spread_at_fill": metrics.spread_at_fill,
                "bid_at_signal": round(float(bid_at_signal or 0.0), 4),
                "ask_at_signal": round(float(ask_at_signal or 0.0), 4),
                "excluded_after_fill": int(metrics.excluded_after_fill),
            }
        )

        session_recorder.record_fill_latency(
            order_id=order_id,
            ticker=ticker,
            latency_ms=metrics.latency_ms,
            slippage_to_atr_pct=metrics.slippage_to_atr_pct,
            spread_at_fill=metrics.spread_at_fill,
            ts=datetime.now(ET),
        )

        self._logged_order_ids.add(order_id)
        return metrics

    def is_excluded(self, ticker: str) -> bool:
        return ticker in self._excluded_tickers

    def clear_exclusions(self) -> None:
        self._excluded_tickers.clear()

    def _csv_path(self) -> Path:
        return LOGS_DIR / f"execution_{datetime.now(ET).strftime('%Y-%m-%d')}.csv"

    def _append_csv(self, row: dict) -> None:
        path = self._csv_path()
        first = not path.exists()
        with open(path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=EXECUTION_LOG_COLUMNS)
            if first:
                writer.writeheader()
            writer.writerow(row)


execution_monitor = ExecutionMonitor()
