"""
execution/startup_recovery.py — Startup state reconstruction.

On a cold start where no positions exist this module is a no-op.
On a warm restart (mid-session crash/restart) it:

  1. Fetches broker positions and open orders.
  2. Compares broker state to internal state (both are initially empty).
  3. Re-registers any discovered positions in risk_manager.
  4. Re-registers any discovered orders in order_manager (idempotency guard).
  5. Flags drift and calls integrity_gate.force_halt() for unresolvable cases.
  6. Emits a RESTART_RECOVERY event to the event_log.

Design properties:
  • IDEMPOTENT — calling reconstruct_from_broker() twice is safe.
  • HALT ON UNRESOLVABLE DRIFT — unknown broker positions that cannot be
    mapped to a local trade record cause an integrity halt.
  • NO DUPLICATE STOPS — recovered orders registered in order_manager mean
    can_submit() blocks re-submission of the same stop/exit order.
  • PAPER BYPASS — PaperBroker (no ._client) returns immediately clean.

Usage (in main.py before starting the engine loop):

    from execution.startup_recovery import reconstruct_from_broker
    result = reconstruct_from_broker(broker, risk_manager, order_manager, event_log)
    if result.halt_triggered:
        log.critical("Startup reconstruction triggered a halt — review required")
        sys.exit(1)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

import pytz

log = logging.getLogger(__name__)
ET  = pytz.timezone("America/New_York")


# ─────────────────────────────────────────────────────────────────────────────
# RESULT TYPE
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class RecoveryResult:
    """Summary of what was found and reconstructed at startup."""
    reconstructed_positions: Dict[str, int]   = field(default_factory=dict)
    already_tracked:         Dict[str, int]   = field(default_factory=dict)
    open_orders:             List[dict]        = field(default_factory=list)
    drift_detected:          bool              = False
    halt_triggered:          bool              = False
    note:                    str               = ""

    @property
    def is_clean(self) -> bool:
        return not self.drift_detected and not self.halt_triggered

    def to_dict(self) -> dict:
        return {
            "reconstructed_positions": self.reconstructed_positions,
            "already_tracked":         self.already_tracked,
            "open_orders_count":       len(self.open_orders),
            "drift_detected":          self.drift_detected,
            "halt_triggered":          self.halt_triggered,
            "note":                    self.note,
        }


# ─────────────────────────────────────────────────────────────────────────────
# MAIN RECONSTRUCTION ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def reconstruct_from_broker(
    broker,
    risk_manager,
    order_manager,
    event_log  = None,
    now:         Optional[datetime] = None,
) -> RecoveryResult:
    """
    Reconstruct internal state from broker at startup.

    Args:
        broker        : AlpacaBroker or PaperBroker instance.
        risk_manager  : RiskManager singleton.
        order_manager : OrderManager singleton.
        event_log     : EventLog singleton (optional; skipped if None).
        now           : override timestamp (default = now ET).

    Returns:
        RecoveryResult describing what was found / reconstructed / halted.
    """
    now = now or datetime.now(ET)

    # ── PaperBroker: no real positions → always clean ─────────────────────
    if not hasattr(broker, "_client"):
        result = RecoveryResult(note="PaperBroker: no reconstruction needed")
        _emit_event(event_log, result)
        log.info("[StartupRecovery] PaperBroker detected — skipping reconstruction")
        return result

        log.info("[StartupRecovery] Live Broker detected — beginning reconstruction")

    # ── Fetch broker positions ──
    broker_positions: Dict[str, dict] = {}  # {ticker: {qty, avg_entry_price}}
    try:
        from infrastructure.ib_connection import IBConnectionManager
        conn_mgr = IBConnectionManager()
        if not conn_mgr.is_connected:
            conn_mgr.connect()
        ib = conn_mgr.ib

        ib_positions = ib.positions()
        for pos in ib_positions:
            sym = str(pos.contract.symbol)
            qty = int(pos.position)
            avg = float(pos.avgCost)
            if qty != 0:
                broker_positions[sym] = {"qty": qty, "avg_entry_price": avg}
        log.info(f"[StartupRecovery] Broker positions: {list(broker_positions.keys()) or 'none'}")
    except Exception as exc:
        msg = f"Failed to fetch IBKR broker positions: {exc}"
        log.error(f"[StartupRecovery] {msg}")
        result = RecoveryResult(drift_detected=True, halt_triggered=True, note=msg)
        _trigger_halt(msg)
        _emit_event(event_log, result)
        return result

    # ── Fetch open broker orders ──
    open_orders: List[dict] = []
    try:
        ib_orders = ib.openOrders()
        for o in ib_orders:
            open_orders.append({
                "order_id":    str(o.orderId),
                "ticker":      str(o.contract.symbol) if hasattr(o.contract, "symbol") else "",
                "side":        str(o.action).lower(),
                "qty":         int(float(getattr(o, "totalQuantity", 0))),
                "filled_qty":  0,
                "filled_price": 0.0,
                "limit_price": float(getattr(o, "lmtPrice", 0) or 0.0),
                "status":      "open",
            })
        log.info(f"[StartupRecovery] Open broker orders: {len(open_orders)}")
    except Exception as exc:
        log.warning(f"[StartupRecovery] Could not fetch open orders: {exc}")
        # Non-fatal: continue without order reconstruction

    # ── Build internal position map ───────────────────────────────────────
    #    On a cold start this will be empty.
    #    On a warm restart with a prior session, it may already have trades.
    internal_positions: Dict[str, int] = {}
    for trade in risk_manager.open_trades():
        qty = trade.shares_remaining if trade.shares_remaining > 0 else trade.shares
        internal_positions[trade.ticker] = (
            internal_positions.get(trade.ticker, 0) + qty
        )

    # ── Reconstruct positions ─────────────────────────────────────────────
    reconstructed: Dict[str, int] = {}
    already_tracked: Dict[str, int] = {}
    drift_detected = False

    for ticker, pos in broker_positions.items():
        broker_qty = pos["qty"]
        internal_qty = internal_positions.get(ticker, 0)

        if internal_qty == broker_qty:
            # Already correctly tracked — no action needed
            already_tracked[ticker] = broker_qty
            log.info(f"[StartupRecovery] {ticker}: {broker_qty}sh already tracked")

        elif internal_qty == 0:
            # Position exists at broker but not internally → recover it
            trade = risk_manager.recover_position(
                ticker          = ticker,
                qty             = broker_qty,
                avg_entry_price = pos["avg_entry_price"],
                stop_price      = 0.0,   # unknown; recover_position applies 2% stop
                note            = "RECOVERED_ON_RESTART",
                now             = now,
            )
            reconstructed[ticker] = broker_qty
            log.warning(
                f"[StartupRecovery] Reconstructed {ticker}: "
                f"{broker_qty}sh @ ${pos['avg_entry_price']:.4f}"
            )

        else:
            # Partial mismatch: could be a partial fill discrepancy
            delta = broker_qty - internal_qty
            drift_detected = True
            log.error(
                f"[StartupRecovery] DRIFT {ticker}: "
                f"broker={broker_qty} internal={internal_qty} delta={delta}"
            )

    # Check for internal positions that broker doesn't have (phantom positions)
    for ticker, internal_qty in internal_positions.items():
        if ticker not in broker_positions:
            drift_detected = True
            log.error(
                f"[StartupRecovery] PHANTOM POSITION: "
                f"internal has {internal_qty}sh {ticker} but broker shows 0"
            )

    # ── Register open broker orders in order_manager ──────────────────────
    for o in open_orders:
        order_manager.recover_order(
            broker_order_id = o["order_id"],
            ticker          = o["ticker"],
            side            = o["side"],
            qty             = o["qty"],
            status          = o["status"],
            limit_price     = o["limit_price"],
            filled_qty      = o["filled_qty"],
            filled_price    = o["filled_price"],
            now             = now,
        )

    # ── Build result ──────────────────────────────────────────────────────
    halt_triggered = False
    note = _build_note(reconstructed, already_tracked, open_orders, drift_detected)

    if drift_detected:
        halt_msg = (
            f"Startup reconstruction detected unresolvable drift. "
            f"Reconstructed: {reconstructed}  Phantom: "
            f"{[t for t in internal_positions if t not in broker_positions]}"
        )
        _trigger_halt(halt_msg)
        halt_triggered = True

    result = RecoveryResult(
        reconstructed_positions = reconstructed,
        already_tracked         = already_tracked,
        open_orders             = open_orders,
        drift_detected          = drift_detected,
        halt_triggered          = halt_triggered,
        note                    = note,
    )

    _emit_event(event_log, result)

    if result.is_clean:
        log.info(f"[StartupRecovery] ✓ Clean startup.  {note}")
    else:
        log.warning(f"[StartupRecovery] ⚠ Issues detected.  {note}")

    return result


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _trigger_halt(reason: str) -> None:
    try:
        from execution.integrity_gate import integrity_gate
        integrity_gate.force_halt(f"[StartupRecovery] {reason}")
    except Exception as exc:
        log.error(f"[StartupRecovery] Could not trigger integrity halt: {exc}")


def _emit_event(event_log, result: RecoveryResult) -> None:
    if event_log is None:
        return
    try:
        event_log.log_restart(
            reconstructed_positions = result.reconstructed_positions,
            open_orders             = result.open_orders,
            drift_detected          = result.drift_detected,
            halt_triggered          = result.halt_triggered,
            note                    = result.note,
        )
    except Exception as exc:
        log.debug(f"[StartupRecovery] event_log emit failed: {exc}")


def _build_note(
    reconstructed: dict,
    already_tracked: dict,
    open_orders: list,
    drift: bool,
) -> str:
    parts = []
    if reconstructed:
        parts.append(f"reconstructed={list(reconstructed.keys())}")
    if already_tracked:
        parts.append(f"already_tracked={list(already_tracked.keys())}")
    if open_orders:
        parts.append(f"open_orders={len(open_orders)}")
    if drift:
        parts.append("DRIFT_DETECTED")
    return "; ".join(parts) if parts else "no_positions_found"
