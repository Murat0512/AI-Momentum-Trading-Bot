from __future__ import annotations

import csv
from datetime import date
from pathlib import Path
from typing import IO, Optional

from events.types import (
    LegacyAuditEvent,
    OrderCancelled,
    OrderFilled,
    OrderPartial,
    OrderSubmitted,
)


class CsvOrdersSink:
    """Append-only CSV sink for order lifecycle diagnostics."""

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
        # Domain events
        if isinstance(event, OrderSubmitted):
            return {
                "ts": event.ts.isoformat(),
                "cycle_id": event.cycle_id,
                "ticker": event.ticker,
                "order_id": event.order_id,
                "side": event.side,
                "qty": event.qty,
                "limit_price": event.limit_price,
                "event_type": "submitted",
                "filled_qty": "",
                "filled_price": "",
                "reason": "",
            }
        if isinstance(event, OrderPartial):
            return {
                "ts": event.ts.isoformat(),
                "cycle_id": event.cycle_id,
                "ticker": event.ticker,
                "order_id": event.order_id,
                "side": "",
                "qty": "",
                "limit_price": "",
                "event_type": "partial",
                "filled_qty": event.filled_qty,
                "filled_price": event.fill_price,
                "reason": "",
            }
        if isinstance(event, OrderFilled):
            return {
                "ts": event.ts.isoformat(),
                "cycle_id": event.cycle_id,
                "ticker": event.ticker,
                "order_id": event.order_id,
                "side": event.side,
                "qty": event.filled_qty,
                "limit_price": "",
                "event_type": "filled",
                "filled_qty": event.filled_qty,
                "filled_price": event.filled_price,
                "reason": "",
            }
        if isinstance(event, OrderCancelled):
            return {
                "ts": event.ts.isoformat(),
                "cycle_id": event.cycle_id,
                "ticker": event.ticker,
                "order_id": event.order_id,
                "side": "",
                "qty": "",
                "limit_price": "",
                "event_type": "cancelled",
                "filled_qty": "",
                "filled_price": "",
                "reason": event.reason,
            }

        # Legacy adapter events from event_log
        if isinstance(event, LegacyAuditEvent):
            et = str(event.event_type)
            payload = event.payload if isinstance(event.payload, dict) else {}
            if et not in {
                "ORDER_SUBMITTED",
                "ORDER_PARTIAL_FILL",
                "ORDER_FILLED",
                "ORDER_CANCELLED",
                "ORDER_REJECTED",
                "ORDER_STUCK",
                "ORDER_CANCEL_REPLACE",
            }:
                return None
            return {
                "ts": event.ts.isoformat(),
                "cycle_id": event.cycle_id,
                "ticker": event.ticker,
                "order_id": payload.get("order_id", payload.get("new_order_id", "")),
                "side": payload.get("side", ""),
                "qty": payload.get("qty", ""),
                "limit_price": payload.get("limit_price", payload.get("new_limit", "")),
                "event_type": et.lower(),
                "filled_qty": payload.get("filled_qty", ""),
                "filled_price": payload.get("fill_price", ""),
                "reason": payload.get("reason", ""),
            }

        return None

    def _rotate(self, today: str) -> None:
        self.close()
        path = self._logs_dir / f"orders_{today}.csv"
        self._fh = open(path, "a", newline="", encoding="utf-8")
        self._current_date = today

        fieldnames = [
            "ts",
            "cycle_id",
            "ticker",
            "order_id",
            "side",
            "qty",
            "limit_price",
            "event_type",
            "filled_qty",
            "filled_price",
            "reason",
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
