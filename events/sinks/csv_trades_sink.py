from __future__ import annotations

import csv
from datetime import date
from pathlib import Path
from typing import IO, Optional

from events.types import LegacyAuditEvent, LifecycleTransition


class CsvTradesSink:
    """Append-only CSV sink for trade-close diagnostics."""

    def __init__(self, logs_dir: Path = Path("logs")) -> None:
        self._logs_dir = Path(logs_dir)
        self._logs_dir.mkdir(parents=True, exist_ok=True)
        self._current_date: Optional[str] = None
        self._fh: Optional[IO] = None
        self._writer: Optional[csv.DictWriter] = None

    def __call__(self, event: object) -> None:
        row = self._to_row(event)
        if row is None:
            return

        today = date.today().isoformat()
        if today != self._current_date:
            self._rotate(today)

        if self._fh is None or self._writer is None:
            return

        try:
            self._writer.writerow(row)
            self._fh.flush()
        except Exception:
            return

    def _to_row(self, event: object) -> Optional[dict]:
        if isinstance(event, LifecycleTransition) and event.to_state == "CLOSED":
            return {
                "date": event.ts.date().isoformat(),
                "ts": event.ts.isoformat(),
                "ticker": event.ticker,
                "trade_id": event.trade_id,
                "entry_price": "",
                "stop_price": "",
                "r_distance": "",
                "qty_entry": "",
                "qty_exit_total": event.shares_sold,
                "pnl_dollars": "",
                "pnl_r": event.pnl,
                "exit_reason": event.reason,
                "demand_score": "",
                "sqs": "",
            }

        if isinstance(event, LegacyAuditEvent):
            if event.event_type != "POSITION_CLOSED":
                return None
            payload = event.payload if isinstance(event.payload, dict) else {}
            return {
                "date": event.ts.date().isoformat(),
                "ts": event.ts.isoformat(),
                "ticker": event.ticker,
                "trade_id": payload.get("trade_id", ""),
                "entry_price": payload.get("entry_price", ""),
                "stop_price": payload.get("stop_price", ""),
                "r_distance": payload.get("r_distance", ""),
                "qty_entry": payload.get("shares", ""),
                "qty_exit_total": payload.get("shares_sold", payload.get("shares", "")),
                "pnl_dollars": payload.get("pnl", ""),
                "pnl_r": payload.get("pnl_r", ""),
                "exit_reason": payload.get("exit_reason", ""),
                "demand_score": payload.get("demand_score", ""),
                "sqs": payload.get("setup_quality_score", ""),
            }

        return None

    def _rotate(self, today: str) -> None:
        self.close()
        path = self._logs_dir / f"trades_{today}.csv"
        self._fh = open(path, "a", newline="", encoding="utf-8")
        self._current_date = today

        fieldnames = [
            "date",
            "ts",
            "ticker",
            "trade_id",
            "entry_price",
            "stop_price",
            "r_distance",
            "qty_entry",
            "qty_exit_total",
            "pnl_dollars",
            "pnl_r",
            "exit_reason",
            "demand_score",
            "sqs",
        ]
        self._writer = csv.DictWriter(self._fh, fieldnames=fieldnames)
        if path.stat().st_size == 0:
            self._writer.writeheader()

    def close(self) -> None:
        if self._fh is not None:
            try:
                self._fh.flush()
                self._fh.close()
            except OSError:
                pass
        self._fh = None
        self._writer = None
        self._current_date = None
