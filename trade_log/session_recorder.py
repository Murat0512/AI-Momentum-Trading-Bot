"""
trade_log/session_recorder.py — Session black-box JSON recorder.

Writes newline-delimited JSON events to logs/session_recorder.json.
Used for pre-market/live audit trails:
  - candidate snapshots (including sentiment)
  - decision blocks/accepts
  - execution latency/fill metrics
"""

from __future__ import annotations

import json
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional

import pytz

ET = pytz.timezone("America/New_York")


class SessionRecorder:
    def __init__(self, logs_dir: str = "logs") -> None:
        self._lock = threading.RLock()
        self._logs_dir = Path(logs_dir)
        self._logs_dir.mkdir(parents=True, exist_ok=True)
        self._path = self._logs_dir / "session_recorder.json"
        self._session_date: Optional[str] = None

    def new_day(self, date_str: Optional[str] = None) -> None:
        today = date_str or datetime.now(ET).strftime("%Y-%m-%d")
        with self._lock:
            if self._session_date != today:
                self._session_date = today
                self._append(
                    {
                        "type": "session_start",
                        "date": today,
                        "started_at": datetime.now(ET).isoformat(),
                    }
                )

    def record_candidates(self, *, cycle_ts: datetime, records: list[dict]) -> None:
        if not records:
            return
        for rec in records:
            self._append(
                {
                    "type": "candidate",
                    "ts": cycle_ts.isoformat(),
                    "ticker": rec.get("ticker", ""),
                    "demand_score": float(rec.get("demand_score", 0.0) or 0.0),
                    "news_sentiment": float(rec.get("news_sentiment", 0.0) or 0.0),
                    "passed_filters": bool(rec.get("passed_filters", False)),
                    "rejection_reason": str(rec.get("rejection_reason", "") or ""),
                    "pct_change": float(rec.get("pct_change", 0.0) or 0.0),
                    "volume_rank": int(rec.get("volume_rank", 0) or 0),
                }
            )

    def record_decision(
        self,
        *,
        ticker: str,
        accepted: bool,
        reason: str,
        gate_results: Optional[dict] = None,
        ts: Optional[datetime] = None,
    ) -> None:
        stamp = ts or datetime.now(ET)
        self._append(
            {
                "type": "decision",
                "ts": stamp.isoformat(),
                "ticker": ticker,
                "accepted": bool(accepted),
                "reason": reason,
                "gate_results": gate_results or {},
            }
        )

    def record_fill_latency(
        self,
        *,
        order_id: str,
        ticker: str,
        latency_ms: int,
        slippage_to_atr_pct: float,
        spread_at_fill: float,
        ts: Optional[datetime] = None,
    ) -> None:
        stamp = ts or datetime.now(ET)
        self._append(
            {
                "type": "fill",
                "ts": stamp.isoformat(),
                "order_id": order_id,
                "ticker": ticker,
                "latency_ms": int(latency_ms),
                "slippage_to_atr_pct": float(slippage_to_atr_pct),
                "spread_at_fill": float(spread_at_fill),
            }
        )

    def _append(self, payload: dict) -> None:
        line = json.dumps(payload, separators=(",", ":"), default=str)
        with self._lock:
            with open(self._path, "a", encoding="utf-8") as fh:
                fh.write(line + "\n")


session_recorder = SessionRecorder()
