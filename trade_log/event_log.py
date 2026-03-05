"""
trade_log/event_log.py — Deprecated legacy adapter over EventBus.

Every significant engine event is appended here as a JSON Lines record.
This enables full-day *replay* for debugging and post-session analysis.

Event types:
  UNIVERSE_SNAPSHOT    — top-N candidates each cycle
  PROMOTION            — ticker promoted into active universe
  DEMOTION             — ticker removed from universe
  DECISION_EVALUATED   — per-candidate accept/reject with full reason
  ORDER_SUBMITTED      — order sent to broker
  ORDER_ACKNOWLEDGED   — broker ACKed order (status: new/pending)
  ORDER_FILLED         — fill confirmed (full or partial)
  ORDER_PARTIAL_FILL   — partial fill recorded
  ORDER_CANCELLED      — cancel confirmed
  ORDER_CANCEL_REPLACE — cancel+replace sent (TTL or partial fill retry)
  ORDER_REJECTED       — broker rejected
  ORDER_STUCK          — order alive past stuck threshold
  POSITION_OPENED      — trade record created
  POSITION_CLOSED      — trade record closed
  POSITION_PARTIAL_EXIT — partial exit (partial R target)
  STOP_ADJUSTED        — stop price moved (breakeven / trail)
  LIFECYCLE_EVENT      — generic lifecycle state-machine transition
  GATE_EVENT           — any decision gate open/close
  GOVERNOR_EVENT       — risk governor block/allow
  INTEGRITY_EVENT      — integrity gate status change
  RECON_MISMATCH       — reconciliation found a mismatch
  RECON_HALT           — safety halt triggered by reconciler
  PREFLIGHT_PASS       — startup preflight checks all passed
  PREFLIGHT_FAIL       — one or more startup preflight checks failed
  ARMING_EVENT         — live arming attempt (pass or fail)
  REGIME_CHANGE        — TREND↔CHOP transition
  HALT_TRANSITION      — per-ticker halt state-machine transition
  CANDIDATE_EXPIRED    — TTL expiry from universe pool
  SESSION_START        — emitted when RTH trading window opens
  SESSION_END          — emitted when RTH trading window closes
  RESTART_RECOVERY     — startup state reconstruction record
  RTH_BLOCK            — order submission blocked outside trading window

File layout:
  logs/events_YYYY-MM-DD.jsonl  (one JSON object per line)

Replay:
  for event in EventLog.replay("2024-06-10"):
      print(event["event_type"], event["ts"])

This module is retained for backward compatibility. Runtime authority is the
EventBus stream consumed by `events/sinks/jsonl_sink.py`.

Thread-safe. Singleton `event_log` available at module level.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import pytz

from events import current_cycle
from events.bus import event_bus
from events.types import LegacyAuditEvent

log = logging.getLogger(__name__)
ET = pytz.timezone("America/New_York")


def guard_saves_snapshot(
    date_str: Optional[str] = None,
    log_dir: str = "logs",
) -> Dict[str, int]:
    """
    Count how many entries were blocked by entry safety guards for a given day.

    Sources:
      - DECISION_EVALUATED (accepted=false, reason in guard reasons)
      - GATE_EVENT fallback (ENTRY_QUOTE_FRESHNESS / ENTRY_SLIPPAGE_GUARD)
    """
    day = date_str or datetime.now(ET).strftime("%Y-%m-%d")
    path = Path(log_dir) / f"events_{day}.jsonl"
    counts: Dict[str, int] = {
        "ENTRY_QUOTE_STALE": 0,
        "ENTRY_PRICE_RUNAWAY": 0,
    }
    if not path.exists():
        counts["total"] = 0
        return counts

    try:
        with open(path, "r", encoding="utf-8") as handle:
            for line in handle:
                raw = (line or "").strip()
                if not raw:
                    continue
                try:
                    event = json.loads(raw)
                except Exception:
                    continue

                event_type = str(event.get("event_type", "") or "").strip()
                payload = event.get("payload", {}) or {}
                if not isinstance(payload, dict):
                    continue

                if event_type == "DECISION_EVALUATED":
                    accepted = bool(payload.get("accepted", False))
                    reason = str(payload.get("reason", "") or "").strip()
                    if not accepted and reason in counts:
                        counts[reason] = counts.get(reason, 0) + 1
                    continue

                if event_type == "GATE_EVENT":
                    if bool(payload.get("passed", True)):
                        continue
                    gate = str(payload.get("gate", "") or "").strip()
                    if gate == "ENTRY_QUOTE_FRESHNESS":
                        counts["ENTRY_QUOTE_STALE"] = (
                            counts.get("ENTRY_QUOTE_STALE", 0) + 1
                        )
                    elif gate == "ENTRY_SLIPPAGE_GUARD":
                        counts["ENTRY_PRICE_RUNAWAY"] = (
                            counts.get("ENTRY_PRICE_RUNAWAY", 0) + 1
                        )
    except Exception:
        counts["total"] = counts.get("ENTRY_QUOTE_STALE", 0) + counts.get(
            "ENTRY_PRICE_RUNAWAY", 0
        )
        return counts

    counts["total"] = counts.get("ENTRY_QUOTE_STALE", 0) + counts.get(
        "ENTRY_PRICE_RUNAWAY", 0
    )
    return counts


def _iso(dt: datetime) -> str:
    if dt is None:
        return ""
    if dt.tzinfo is None:
        dt = ET.localize(dt)
    return dt.isoformat()


def _safe_json(obj: Any) -> Any:
    """Recursively make an object JSON-serialisable."""
    if isinstance(obj, datetime):
        return _iso(obj)
    if isinstance(obj, (list, tuple)):
        return [_safe_json(v) for v in obj]
    if isinstance(obj, dict):
        return {str(k): _safe_json(v) for k, v in obj.items()}
    if hasattr(obj, "__dict__"):
        return _safe_json(vars(obj))
    return obj


# ─────────────────────────────────────────────────────────────────────────────
# EVENT LOG
# ─────────────────────────────────────────────────────────────────────────────


class EventLog:
    """
    Append-only structured event stream, written to JSONL.

    Every record contains at minimum:
        event_id    — unique UUID (short)
        event_type  — one of the constants in config.constants (EVT_*)
        ts          — ISO-8601 timestamp with timezone
        run_id      — config-hash-based run identifier (if governance enabled)
        payload     — event-specific data dict

    Usage:
        from trade_log.event_log import event_log                 # singleton
        event_log.new_day()                                       # call at session start
        event_log.log("ORDER_SUBMITTED", ticker="AAPL",
                       payload={"qty": 100, "limit_price": 150.0})
    """

    def __init__(self, log_dir: str = "logs") -> None:
        self._log_dir = Path(log_dir)
        self._lock = threading.Lock()
        self._path: Optional[Path] = None
        self._run_id: str = ""
        self._fh = None

    # ── Session control ───────────────────────────────────────────────────────

    def new_day(
        self,
        date_str: Optional[str] = None,
        run_id: str = "",
    ) -> None:
        """
        Open a new log file for today's session.
        Call once at session start.
        """
        with self._lock:
            if self._fh:
                try:
                    self._fh.close()
                except Exception:
                    pass
                self._fh = None

            today = date_str or datetime.now(ET).strftime("%Y-%m-%d")
            self._log_dir.mkdir(parents=True, exist_ok=True)
            self._path = self._log_dir / f"events_{today}.jsonl"
            self._run_id = run_id or ""

            # Open append handle for compatibility fallback mode.
            self._fh = open(self._path, "a", encoding="utf-8")
        log.info(
            "[EventLog] Legacy adapter active (EventBus authority) run_id=%r",
            self._run_id,
        )

    def close(self) -> None:
        """Close fallback sink (used only when no EventBus sinks are registered)."""
        with self._lock:
            if self._fh is not None:
                try:
                    self._fh.flush()
                    self._fh.close()
                except Exception:
                    pass
                self._fh = None

    # ── Write ─────────────────────────────────────────────────────────────────

    def log(
        self,
        event_type: str,
        *,
        ticker: str = "",
        payload: Dict[str, Any] = None,
        now: Optional[datetime] = None,
    ) -> None:
        """
        Append one event record to the JSONL file.

        Args:
            event_type : one of the EVT_* constants
            ticker     : optional primary ticker this event concerns
            payload    : arbitrary event-specific data
            now        : override timestamp (default = now ET)
        """
        now = now or datetime.now(ET)
        record: Dict[str, Any] = {
            "event_id": uuid.uuid4().hex[:12],
            "event_type": event_type,
            "ts": _iso(now),
            "run_id": self._run_id,
            "ticker": ticker,
            "payload": _safe_json(payload or {}),
        }
        legacy_event = LegacyAuditEvent(
            cycle_id=current_cycle.id,
            ts=now,
            event_id=record["event_id"],
            event_type=record["event_type"],
            run_id=record["run_id"],
            ticker=record["ticker"],
            payload=record["payload"],
        )

        event_bus.publish(legacy_event)

        # Backward-compatible fallback for isolated unit tests and adapters
        # that instantiate EventLog directly without configuring EventBus sinks.
        if len(event_bus) == 0:
            line = json.dumps(record, separators=(",", ":")) + "\n"
            with self._lock:
                if self._fh is None:
                    self.new_day()
                try:
                    self._fh.write(line)
                    self._fh.flush()
                except Exception as exc:
                    log.error(
                        "[EventLog] Fallback sink write failed (%s): %s",
                        event_type,
                        exc,
                    )

    # ── Convenience wrappers ──────────────────────────────────────────────────

    def log_order_submitted(
        self,
        ticker: str,
        side: str,
        qty: int,
        limit_price: float,
        order_id: str = "",
        run_id: str = "",
        **extras,
    ) -> None:
        from config.constants import EVT_ORDER_SUBMITTED

        self.log(
            EVT_ORDER_SUBMITTED,
            ticker=ticker,
            payload={
                "side": side,
                "qty": qty,
                "limit_price": limit_price,
                "order_id": order_id,
                **extras,
            },
        )

    def log_order_filled(
        self,
        ticker: str,
        side: str,
        qty: int,
        fill_price: float,
        order_id: str = "",
        **extras,
    ) -> None:
        from config.constants import EVT_ORDER_FILLED

        self.log(
            EVT_ORDER_FILLED,
            ticker=ticker,
            payload={
                "side": side,
                "qty": qty,
                "fill_price": fill_price,
                "order_id": order_id,
                **extras,
            },
        )

    def log_position_opened(self, trade) -> None:
        from config.constants import EVT_POSITION_OPENED

        self.log(
            EVT_POSITION_OPENED,
            ticker=trade.ticker,
            payload={
                "trade_id": trade.trade_id,
                "entry_price": trade.entry_price,
                "stop_price": trade.stop_price,
                "shares": trade.shares,
                "regime": trade.regime,
                "setup_name": trade.setup_name,
            },
        )

    def log_position_closed(self, trade) -> None:
        from config.constants import EVT_POSITION_CLOSED

        self.log(
            EVT_POSITION_CLOSED,
            ticker=trade.ticker,
            payload={
                "trade_id": trade.trade_id,
                "exit_price": trade.exit_price,
                "pnl": trade.pnl,
                "pnl_r": trade.pnl_r,
                "exit_reason": trade.exit_reason,
            },
        )

    def log_stop_adjusted(
        self,
        ticker: str,
        trade_id: str,
        old_stop: float,
        new_stop: float,
        reason: str,
        lifecycle_state: str = "",
    ) -> None:
        from config.constants import EVT_STOP_ADJUSTED

        self.log(
            EVT_STOP_ADJUSTED,
            ticker=ticker,
            payload={
                "trade_id": trade_id,
                "old_stop": old_stop,
                "new_stop": new_stop,
                "reason": reason,
                "lifecycle_state": lifecycle_state,
            },
        )

    def log_integrity_event(self, is_halted: bool, reason: str) -> None:
        from config.constants import EVT_INTEGRITY_EVENT

        self.log(
            EVT_INTEGRITY_EVENT,
            payload={
                "halted": is_halted,
                "reason": reason,
            },
        )

    def log_gate_event(
        self, gate: str, passed: bool, reason: str, ticker: str = ""
    ) -> None:
        from config.constants import EVT_GATE_EVENT

        self.log(
            EVT_GATE_EVENT,
            ticker=ticker,
            payload={
                "gate": gate,
                "passed": passed,
                "reason": reason,
            },
        )

    def log_universe_snapshot(self, candidates: list, cycle_seq: int = 0) -> None:
        from config.constants import EVT_UNIVERSE_SNAPSHOT

        self.log(
            EVT_UNIVERSE_SNAPSHOT,
            payload={
                "cycle_seq": cycle_seq,
                "count": len(candidates),
                "candidates": _safe_json(candidates),
            },
        )

    def log_promotion(
        self, ticker: str, source: str, reason: str, score: float = 0.0
    ) -> None:
        """Ticker newly entered the tracked universe."""
        from config.constants import EVT_PROMOTION

        self.log(
            EVT_PROMOTION,
            ticker=ticker,
            payload={
                "source": source,
                "reason": reason,
                "score": score,
            },
        )

    def log_demotion(self, ticker: str, reason: str, last_score: float = 0.0) -> None:
        """Ticker removed from the tracked universe (TTL expiry, etc.)."""
        from config.constants import EVT_DEMOTION

        self.log(
            EVT_DEMOTION,
            ticker=ticker,
            payload={
                "reason": reason,
                "last_score": last_score,
            },
        )

    def log_decision_evaluation(
        self,
        ticker: str,
        accepted: bool,
        reason: str,
        sqs: float = 0.0,
        demand_score: float = 0.0,
        regime: str = "",
        gate_results: dict = None,
    ) -> None:
        """
        Per-candidate decision record — emitted each tick a candidate is
        evaluated.  When accepted=False the reason explains which gate failed.
        Enables full replay of why every setup was taken or skipped.
        """
        from config.constants import EVT_DECISION

        self.log(
            EVT_DECISION,
            ticker=ticker,
            payload={
                "accepted": accepted,
                "reason": reason,
                "sqs": sqs,
                "demand_score": demand_score,
                "regime": regime,
                "gate_results": _safe_json(gate_results or {}),
            },
        )

    def log_governor_event(
        self,
        blocked: bool,
        reason: str,
        ticker: str = "",
        governor_name: str = "",
    ) -> None:
        """Risk governor allowed or blocked an action."""
        from config.constants import EVT_GOVERNOR_EVENT

        self.log(
            EVT_GOVERNOR_EVENT,
            ticker=ticker,
            payload={
                "blocked": blocked,
                "reason": reason,
                "governor_name": governor_name,
            },
        )

    def log_order_cancelled(self, ticker: str, order_id: str, reason: str) -> None:
        from config.constants import EVT_ORDER_CANCELLED

        self.log(
            EVT_ORDER_CANCELLED,
            ticker=ticker,
            payload={
                "order_id": order_id,
                "reason": reason,
            },
        )

    def log_order_stuck(self, ticker: str, order_id: str, age_seconds: float) -> None:
        from config.constants import EVT_ORDER_STUCK

        self.log(
            EVT_ORDER_STUCK,
            ticker=ticker,
            payload={
                "order_id": order_id,
                "age_seconds": age_seconds,
            },
        )

    def log_session_start(
        self, date_str: str, run_id: str = "", mode: str = "paper"
    ) -> None:
        """Emitted when the RTH trading window opens each day."""
        from config.constants import EVT_SESSION_START

        self.log(
            EVT_SESSION_START,
            payload={
                "date": date_str,
                "run_id": run_id,
                "mode": mode,
            },
        )

    def log_session_end(
        self,
        date_str: str,
        total_trades: int = 0,
        daily_pnl: float = 0.0,
        daily_pnl_r: float = 0.0,
        guard_saves: Optional[Dict[str, int]] = None,
        guard_saves_line: str = "",
    ) -> None:
        """Emitted when the RTH trading window closes."""
        from config.constants import EVT_SESSION_END

        payload: Dict[str, Any] = {
            "date": date_str,
            "total_trades": total_trades,
            "daily_pnl": daily_pnl,
            "daily_pnl_r": daily_pnl_r,
        }
        if guard_saves is not None:
            payload["guard_saves"] = _safe_json(guard_saves)
        if guard_saves_line:
            payload["guard_saves_line"] = guard_saves_line

        self.log(
            EVT_SESSION_END,
            payload=payload,
        )

    def log_restart(
        self,
        reconstructed_positions: dict,
        open_orders: list,
        drift_detected: bool,
        halt_triggered: bool = False,
        note: str = "",
    ) -> None:
        """
        Emitted during startup recovery after broker state is reconstructed.
        Records what was found so the day's event stream can be replayed
        from a clean baseline even after a mid-session restart.
        """
        from config.constants import EVT_RESTART

        self.log(
            EVT_RESTART,
            payload={
                "reconstructed_positions": reconstructed_positions,
                "open_orders": open_orders,
                "drift_detected": drift_detected,
                "halt_triggered": halt_triggered,
                "note": note,
            },
        )

    def log_rth_block(self, ticker: str, reason: str, now_str: str = "") -> None:
        """Order submission blocked because we are outside the RTH window."""
        from config.constants import EVT_RTH_BLOCK

        self.log(
            EVT_RTH_BLOCK,
            ticker=ticker,
            payload={
                "reason": reason,
                "now": now_str,
            },
        )

    def log_preflight(self, passed: bool, results: dict) -> None:
        from config.constants import EVT_PREFLIGHT_PASS, EVT_PREFLIGHT_FAIL

        evt = EVT_PREFLIGHT_PASS if passed else EVT_PREFLIGHT_FAIL
        self.log(evt, payload={"results": results, "all_passed": passed})

    def log_arming(self, mode: str, granted: bool, reason: str = "") -> None:
        from config.constants import EVT_ARMING

        self.log(
            EVT_ARMING,
            payload={
                "mode": mode,
                "granted": granted,
                "reason": reason,
            },
        )

    # ── New: order lifecycle completeness ─────────────────────────────────────

    def log_order_acknowledged(
        self, ticker: str, order_id: str, status: str, **extras
    ) -> None:
        """Broker acknowledged the order — emitted on status change to new/pending."""
        from config.constants import EVT_ORDER_ACKNOWLEDGED

        self.log(
            EVT_ORDER_ACKNOWLEDGED,
            ticker=ticker,
            payload={
                "order_id": order_id,
                "status": status,
                **extras,
            },
        )

    def log_order_partial_fill(
        self,
        ticker: str,
        order_id: str,
        filled_qty: int,
        fill_price: float,
        remaining_qty: int,
        **extras,
    ) -> None:
        """Partial fill received — position not yet fully opened."""
        from config.constants import EVT_ORDER_PARTIAL

        self.log(
            EVT_ORDER_PARTIAL,
            ticker=ticker,
            payload={
                "order_id": order_id,
                "filled_qty": filled_qty,
                "fill_price": fill_price,
                "remaining_qty": remaining_qty,
                **extras,
            },
        )

    def log_order_rejected(
        self, ticker: str, order_id: str, reason: str, **extras
    ) -> None:
        """Broker rejected the order."""
        from config.constants import EVT_ORDER_REJECTED

        self.log(
            EVT_ORDER_REJECTED,
            ticker=ticker,
            payload={
                "order_id": order_id,
                "reason": reason,
                **extras,
            },
        )

    def log_order_cancel_replace(
        self,
        ticker: str,
        old_order_id: str,
        new_order_id: str,
        new_limit: float,
        reason: str = "",
        **extras,
    ) -> None:
        """Cancel+replace sent (TTL expiry or partial fill re-attempt)."""
        from config.constants import EVT_ORDER_CANCEL_REPLACE

        self.log(
            EVT_ORDER_CANCEL_REPLACE,
            ticker=ticker,
            payload={
                "old_order_id": old_order_id,
                "new_order_id": new_order_id,
                "new_limit": new_limit,
                "reason": reason,
                **extras,
            },
        )

    # ── New: position lifecycle completeness ─────────────────────────────────

    def log_position_partial_exit(
        self,
        ticker: str,
        trade_id: str,
        shares_sold: int,
        price: float,
        pnl: float,
        remaining_shares: int,
        lifecycle_state: str = "",
        **extras,
    ) -> None:
        """Partial exit executed (stop at breakeven or partial R target)."""
        from config.constants import EVT_POSITION_PARTIAL

        self.log(
            EVT_POSITION_PARTIAL,
            ticker=ticker,
            payload={
                "trade_id": trade_id,
                "shares_sold": shares_sold,
                "price": price,
                "pnl": pnl,
                "remaining_shares": remaining_shares,
                "lifecycle_state": lifecycle_state,
                **extras,
            },
        )

    def log_lifecycle_event(
        self,
        ticker: str,
        trade_id: str,
        lifecycle_evt: str,
        payload: dict = None,
    ) -> None:
        """
        Generic lifecycle state-machine event (breakeven, trail start, etc.).
        Use the LIFECYCLE_EVT_* constants from config.constants for lifecycle_evt.
        """
        from config.constants import EVT_LIFECYCLE

        self.log(
            EVT_LIFECYCLE,
            ticker=ticker,
            payload={
                "trade_id": trade_id,
                "lifecycle_evt": lifecycle_evt,
                **(payload or {}),
            },
        )

    # ── New: regime + halt observability ─────────────────────────────────────

    def log_regime_change(
        self,
        old_regime: str,
        new_regime: str,
        adx: float = 0.0,
        spy_range_pct: float = 0.0,
    ) -> None:
        """Market regime transitioned (TREND ↔ CHOP)."""
        from config.constants import EVT_REGIME_CHANGE

        self.log(
            EVT_REGIME_CHANGE,
            payload={
                "old_regime": old_regime,
                "new_regime": new_regime,
                "adx": adx,
                "spy_range_pct": spy_range_pct,
            },
        )

    def log_halt_transition(
        self,
        ticker: str,
        from_state: str,
        to_state: str,
        reason: str = "",
    ) -> None:
        """Per-ticker halt state-machine transition recorded for replay."""
        from config.constants import EVT_HALT_TRANSITION

        self.log(
            EVT_HALT_TRANSITION,
            ticker=ticker,
            payload={
                "from_state": from_state,
                "to_state": to_state,
                "reason": reason,
            },
        )

    def log_candidate_expired(
        self,
        ticker: str,
        score: float,
        source: str = "",
        age_minutes: float = 0.0,
    ) -> None:
        """Ticker removed from universe pool due to TTL expiry."""
        from config.constants import EVT_CANDIDATE_EXPIRED

        self.log(
            EVT_CANDIDATE_EXPIRED,
            ticker=ticker,
            payload={
                "score": score,
                "source": source,
                "age_minutes": age_minutes,
            },
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Replay
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def replay(
        date_str: str,
        log_dir: str = "logs",
    ) -> Iterator[Dict[str, Any]]:
        """
        Read back a day's event log in chronological order.

        Usage:
            for event in EventLog.replay("2024-06-10"):
                if event["event_type"] == "POSITION_OPENED":
                    ...
        """
        path = Path(log_dir) / f"events_{date_str}.jsonl"
        if not path.exists():
            return
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError as exc:
                    log.warning(f"[EventLog] Corrupt line in {path}: {exc}")

    @staticmethod
    def list_dates(log_dir: str = "logs") -> List[str]:
        """Returns sorted list of all dates with event log files."""
        p = Path(log_dir)
        if not p.exists():
            return []
        return sorted(f.stem.replace("events_", "") for f in p.glob("events_*.jsonl"))


# Module-level singleton
event_log = EventLog()
