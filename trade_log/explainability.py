"""
trade_log/explainability.py — Full-cycle JSON-lines audit logger + replay harness.

Every engine cycle produces an event record containing:
  - Top-15 snapshot (all candidates with scores and DH status)
  - Decision result (selected ticker, SQS components, multipliers)
  - All gate results per rejected ticker
  - Regime state, data health events, slippage events
  - News candidates active at decision time
  - Session, feed type, universe size

File layout:
  logs/explain_{date}.jsonl     ← one JSON object per line per cycle

Replay harness (ExplainabilityReplayer):
  - Reads a log file and reconstructs the decision path cycle by cycle
  - Useful for post-session review and debugging

Log format per event:
  {
    "event_type":   "CYCLE_SNAPSHOT" | "DECISION" | "SKIP" | ...,
    "ts":           "2024-06-10T09:45:00-04:00",
    "session":      "RTH",
    "regime":       "TREND",
    "cycle_seq":    42,
    "top15":        [...],
    "decision":     { "selected_ticker", "reason", "sqs_components", ... },
    "gate_log":     { "TICKER": [{gate, passed, reason}, ...], ... },
    "news_active":  [...],
    "slippage":     { "TICKER": {...}, ... },
    "open_trades":  2,
    "daily_pnl":    -123.45,
  }
"""

from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import pytz

from config.constants import (
    LOG_CYCLE_SNAPSHOT,
    LOG_DECISION,
    LOG_HEALTH_EVENT,
    LOG_SKIP,
    LOG_SLIPPAGE_EVENT,
)

log = logging.getLogger(__name__)
ET = pytz.timezone("America/New_York")


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────


def _iso(dt: datetime) -> str:
    if dt is None:
        return ""
    if dt.tzinfo is None:
        dt = ET.localize(dt)
    return dt.isoformat()


def _safe_serialize(obj: Any) -> Any:
    """Recursively convert objects to JSON-serialisable types."""
    if isinstance(obj, datetime):
        return _iso(obj)
    if isinstance(obj, (list, tuple)):
        return [_safe_serialize(v) for v in obj]
    if isinstance(obj, dict):
        return {str(k): _safe_serialize(v) for k, v in obj.items()}
    if hasattr(obj, "__dict__"):
        return _safe_serialize(vars(obj))
    return obj


# ─────────────────────────────────────────────────────────────────────────────
# EXPLAINABILITY LOGGER
# ─────────────────────────────────────────────────────────────────────────────


class ExplainabilityLogger:
    """
    Writes one JSON-lines event per engine cycle to `logs/explain_{date}.jsonl`.

    Thread-safe.

    Usage:
        logger = ExplainabilityLogger()
        logger.log_cycle(
            decision_result = result,
            news_candidates = ingestor.get_candidates(),
            slippage_stats  = monitor.all_stats(),
            regime          = "TREND",
        )
    """

    def __init__(self, log_dir: str = "logs") -> None:
        self._log_dir = Path(log_dir)
        self._log_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._file = None  # current open file handle
        self._file_date = None  # YYYY-MM-DD string for current file
        self._cycle_seq = 0
        self._current_path: Optional[Path] = None

    # ── Public API ──────────────────────────────────────────────────────────

    def log_cycle(
        self,
        decision_result,  # DecisionResult from decision/engine.py
        news_candidates: List = None,
        slippage_stats: Dict = None,
        regime: str = "",
        now: datetime = None,
    ) -> None:
        """
        Log one full cycle snapshot.
        Called once per engine tick regardless of whether a trade was taken.
        """
        now = now or datetime.now(ET)
        sel = decision_result.selected_ticker

        if sel:
            event_type = LOG_DECISION
        elif decision_result.reason:
            event_type = LOG_SKIP
        else:
            event_type = LOG_CYCLE_SNAPSHOT

        self._write(
            {
                "event_type": event_type,
                "ts": _iso(now),
                "session": self._session_label(now),
                "regime": regime or decision_result.regime,
                "cycle_seq": self._next_seq(),
                "top15": decision_result.top15_snapshot,
                "decision": {
                    "selected_ticker": sel,
                    "reason": decision_result.reason,
                    "sqs_components": decision_result.sqs_components,
                    "selection_reason": (
                        decision_result.selection.selection_reason
                        if decision_result.selection
                        else ""
                    ),
                    "universe_rank": (
                        decision_result.selection.universe_rank
                        if decision_result.selection
                        else 0
                    ),
                    "health_size_mult": decision_result.health_size_multiplier,
                    "slippage_size_mult": decision_result.slippage_size_multiplier,
                },
                "gate_log": decision_result.gate_log,
                "rejected": decision_result.rejected,
                "rejected_detail": decision_result.rejected_detail,
                "news_active": self._serialise_news(news_candidates or []),
                "slippage": slippage_stats or {},
                "open_trades": decision_result.open_trades,
                "daily_pnl": decision_result.daily_pnl,
            }
        )

    def log_health_event(
        self,
        ticker: str,
        dh_report,  # DataHealthReport
        now: datetime = None,
    ) -> None:
        """Log a DATA_HEALTH status change."""
        now = now or datetime.now(ET)
        self._write(
            {
                "event_type": LOG_HEALTH_EVENT,
                "ts": _iso(now),
                "ticker": ticker,
                "dh_status": dh_report.status,
                "block_reason": dh_report.block_reason,
                "degrade_reasons": dh_report.degrade_reasons,
                "quote_age_s": dh_report.quote_age_s,
                "spread_pct": dh_report.spread_pct,
                "session": dh_report.session,
                "feed_type": dh_report.feed_type,
            }
        )

    def log_slippage_event(
        self,
        ticker: str,
        event_tag: str,
        fill,  # SlippageFill
        now: datetime = None,
    ) -> None:
        """Log a slippage record after a fill is received."""
        now = now or datetime.now(ET)
        self._write(
            {
                "event_type": LOG_SLIPPAGE_EVENT,
                "ts": _iso(now),
                "ticker": ticker,
                "slippage_tag": event_tag,
                "expected_price": fill.expected_price,
                "fill_price": fill.fill_price,
                "slippage_bps": round(fill.slippage_bps, 2),
                "slippage_r": round(fill.slippage_r, 4),
                "spread_pct": fill.spread_pct,
            }
        )

    def current_log_path(self) -> Optional[Path]:
        return self._current_path

    # ── Internal ────────────────────────────────────────────────────────────

    def _next_seq(self) -> int:
        self._cycle_seq += 1
        return self._cycle_seq

    def _write(self, record: dict) -> None:
        line = json.dumps(_safe_serialize(record), separators=(",", ":"))
        with self._lock:
            fh = self._get_file()
            fh.write(line + "\n")
            fh.flush()

    def close(self) -> None:
        """Close the open log file handle. Call before deleting the log directory."""
        with self._lock:
            if self._file:
                try:
                    self._file.close()
                except OSError:
                    pass
                self._file = None

    def _get_file(self):
        """Return the open file handle; rotate on date change. Caller holds lock."""
        today = datetime.now(ET).strftime("%Y-%m-%d")
        if self._file_date != today:
            if self._file:
                try:
                    self._file.close()
                except OSError:
                    pass
            path = self._log_dir / f"explain_{today}.jsonl"
            self._file = open(path, "a", encoding="utf-8")
            self._file_date = today
            self._current_path = path
            log.info(f"[ExplainabilityLogger] Opened {path}")
        return self._file

    def _session_label(self, now: datetime) -> str:
        try:
            from data.health import current_session

            return current_session(now)
        except Exception:
            return "UNKNOWN"

    @staticmethod
    def _serialise_news(candidates: List) -> List[dict]:
        out = []
        for c in candidates:
            out.append(
                {
                    "ticker": c.ticker,
                    "headline": c.headline[:100] if hasattr(c, "headline") else "",
                    "catalyst_type": getattr(c, "catalyst_type", ""),
                    "news_score": getattr(c, "news_score", 0.0),
                }
            )
        return out


# ─────────────────────────────────────────────────────────────────────────────
# REPLAY HARNESS
# ─────────────────────────────────────────────────────────────────────────────


class ExplainabilityReplayer:
    """
    Read an explain_*.jsonl log file and replay decision cycles.

    Usage:
        replayer = ExplainabilityReplayer("logs/explain_2024-06-10.jsonl")
        for event in replayer.iter_decisions():
            print(event["ts"], event["decision"]["selected_ticker"],
                  event["decision"]["reason"])

        # Summary of all selections:
        for summary in replayer.decision_summary():
            print(summary)
    """

    def __init__(self, log_path: str) -> None:
        self._path = Path(log_path)

    def iter_all(self) -> Iterator[dict]:
        """Iterate all events in chronological order."""
        if not self._path.exists():
            log.debug(f"[Replayer] Log file not found: {self._path}")
            return
        with open(self._path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError as exc:
                    log.warning(f"[Replayer] bad JSON line: {exc}")

    def iter_decisions(self) -> Iterator[dict]:
        """Iterate only trade-decision events."""
        for event in self.iter_all():
            if event.get("event_type") in (LOG_DECISION, LOG_SKIP, LOG_CYCLE_SNAPSHOT):
                yield event

    def decision_summary(self) -> List[dict]:
        """
        Return a list of summary dicts for every cycle, showing:
          - timestamp, regime, selected ticker, reason, SQS components
          - number of candidates considered
          - all rejected tickers + gates
        """
        summary = []
        for event in self.iter_decisions():
            dec = event.get("decision", {})
            summary.append(
                {
                    "ts": event.get("ts"),
                    "regime": event.get("regime"),
                    "session": event.get("session"),
                    "cycle_seq": event.get("cycle_seq"),
                    "selected": dec.get("selected_ticker"),
                    "reason": dec.get("reason"),
                    "universe_rank": dec.get("universe_rank"),
                    "health_mult": dec.get("health_size_mult", 1.0),
                    "slip_mult": dec.get("slippage_size_mult", 1.0),
                    "sqs_components": dec.get("sqs_components", {}),
                    "top15_count": len(event.get("top15", [])),
                    "rejected_count": len(event.get("rejected", {})),
                    "rejected": event.get("rejected", {}),
                    "open_trades": event.get("open_trades", 0),
                    "daily_pnl": event.get("daily_pnl", 0.0),
                }
            )
        return summary

    def find_cycle(self, cycle_seq: int) -> Optional[dict]:
        """Find a specific cycle by sequence number."""
        for event in self.iter_all():
            if event.get("cycle_seq") == cycle_seq:
                return event
        return None

    def gate_analysis(self) -> Dict[str, int]:
        """
        Count how many times each gate rejected a candidate across all cycles.
        Returns {gate_name: count}.
        """
        counts: Dict[str, int] = {}
        for event in self.iter_decisions():
            for ticker, gate in event.get("rejected", {}).items():
                counts[gate] = counts.get(gate, 0) + 1
        return dict(sorted(counts.items(), key=lambda x: -x[1]))

    def news_impact_analysis(self) -> Dict[str, dict]:
        """
        Show how often news-promoted tickers made it to selection.
        Returns {ticker: {"seen": N, "selected": M, "catalysts": [...]}}
        """
        ticker_stats: Dict[str, dict] = {}
        for event in self.iter_decisions():
            news = event.get("news_active", [])
            selected = event.get("decision", {}).get("selected_ticker")
            for n in news:
                t = n.get("ticker", "")
                if not t:
                    continue
                if t not in ticker_stats:
                    ticker_stats[t] = {"seen": 0, "selected": 0, "catalysts": set()}
                ticker_stats[t]["seen"] += 1
                if t == selected:
                    ticker_stats[t]["selected"] += 1
                ticker_stats[t]["catalysts"].add(n.get("catalyst_type", ""))
        # Sets aren't JSON-serialisable — convert to sorted lists
        for v in ticker_stats.values():
            v["catalysts"] = sorted(v["catalysts"])
        return ticker_stats


# ─────────────────────────────────────────────────────────────────────────────
# Module-level singleton
# ─────────────────────────────────────────────────────────────────────────────
explainability_logger = ExplainabilityLogger()
