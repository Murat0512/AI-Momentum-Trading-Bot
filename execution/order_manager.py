"""
execution/order_manager.py — Order execution state machine.

Tracks every order through a well-defined lifecycle:

    PENDING     → submitted to broker
    SUBMITTED   → acknowledged, awaiting fill
    PARTIAL_FILLED → partially filled, remainder live
    FILLED      → fully filled (terminal)
    CANCELLED   → cancelled (terminal, includes TTL and manual)
    REJECTED    → broker rejected (terminal)
    STUCK       → alive beyond stuck_threshold_seconds (terminal)
    REPLACED    → cancel-replaced with a new order (terminal for old order)

Safety properties guaranteed by this module:
    • Duplicate prevention — Cannot submit a new buy/sell for a ticker
      if one is already PENDING/SUBMITTED/PARTIAL_FILLED.
    • TTL cancel/replace  — If a limit order is not filled within
      limit_order_ttl_seconds, cancel and resubmit once at the same price.
    • Stuck detection     — If an order is alive beyond stuck_order_seconds,
      cancel and flag as STUCK (triggers integrity gate reject counter).
    • Partial fill handling — PARTIAL state tracked; cancel-replace remainder
      if cancel_replace_on_partial is True.
    • All state changes are recorded in a structured event list.

Thread-safe.
"""

from __future__ import annotations

import logging
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pytz

from config.constants import (
    ORDER_CANCELLED,
    ORDER_FILLED,
    ORDER_PARTIAL,
    ORDER_PENDING,
    ORDER_REJECTED,
    ORDER_REPLACED,
    ORDER_STUCK,
    ORDER_SUBMITTED,
)
from config.settings import CONFIG

# Import integrity_gate at module level so it can be patched in tests
from execution.integrity_gate import integrity_gate as integrity_gate  # noqa: E402
from execution.monitor import execution_monitor

log = logging.getLogger(__name__)
ET = pytz.timezone("America/New_York")


# ─────────────────────────────────────────────────────────────────────────────
# DATA TYPES
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class ManagedOrder:
    """Represents a single order tracked through its full lifecycle."""

    order_id: str
    ticker: str
    side: str  # "buy" | "sell"
    qty: int  # total requested qty
    limit_price: float

    status: str = ORDER_PENDING
    submitted_at: Optional[datetime] = None
    filled_at: Optional[datetime] = None
    filled_price: float = 0.0
    filled_qty: int = 0  # cumulative fills
    cancel_replace_count: int = 0  # how many C/R cycles
    broker_order_id: str = ""  # broker-assigned ID
    reason: str = ""  # for rejected/stuck/cancelled

    # Entry execution context (for monitoring)
    signal_price: float = 0.0
    atr_1m: float = 0.0
    signal_timestamp: Optional[datetime] = None
    sqs_score: float = 0.0
    rvol: float = 0.0
    spread_at_fill: float = 0.0
    bid_at_signal: float = 0.0
    ask_at_signal: float = 0.0
    execution_metrics: dict = field(default_factory=dict)

    @property
    def is_active(self) -> bool:
        return self.status in (ORDER_PENDING, ORDER_SUBMITTED, ORDER_PARTIAL)

    @property
    def is_terminal(self) -> bool:
        return self.status in (
            ORDER_FILLED,
            ORDER_CANCELLED,
            ORDER_REJECTED,
            ORDER_STUCK,
            ORDER_REPLACED,
        )

    @property
    def remaining_qty(self) -> int:
        return max(0, self.qty - self.filled_qty)


@dataclass
class OrderEvent:
    """Emitted on every order state change."""

    order_id: str
    ticker: str
    side: str
    event: str  # e.g. "SUBMITTED", "FILLED", "STUCK"
    qty: int
    price: float = 0.0
    reason: str = ""
    ts: datetime = field(default_factory=lambda: datetime.now(ET))


# ─────────────────────────────────────────────────────────────────────────────
# ORDER MANAGER
# ─────────────────────────────────────────────────────────────────────────────


class OrderManager:
    """
    Session-scoped order state machine.
    Instantiate once and call tick() each engine cycle.

    Usage:
        mgr = OrderManager()

        # Before placing an order:
        ok, reason = mgr.can_submit(ticker="AAPL", side="buy")

        # Place the order:
        order = mgr.submit(broker, ticker="AAPL", side="buy",
                           qty=100, limit_price=150.0)

        # Each engine cycle:
        changed = mgr.tick(broker)
    """

    def __init__(self) -> None:
        self._lock: threading.Lock = threading.Lock()
        self._orders: Dict[str, ManagedOrder] = {}  # id → order
        self._pending_by_ticker: Dict[str, str] = {}  # ticker → id
        self._events: List[OrderEvent] = []  # session log

    # ── Pre-submission check ──────────────────────────────────────────────────

    def can_submit(self, ticker: str, side: str) -> Tuple[bool, str]:
        """
        Check whether a new order for ticker/side can be submitted.
        Returns (True, "") or (False, reason).
        """
        with self._lock:
            cfg = CONFIG.order_manager

            # Total active orders cap
            active = [o for o in self._orders.values() if o.is_active]
            if len(active) >= cfg.max_pending_orders:
                return False, (
                    f"max_pending_orders reached "
                    f"({len(active)}/{cfg.max_pending_orders})"
                )

            # Duplicate prevention per ticker
            existing_id = self._pending_by_ticker.get(ticker)
            if existing_id:
                existing = self._orders.get(existing_id)
                if existing and existing.is_active:
                    return False, (
                        f"duplicate: active {existing.side} order already pending "
                        f"for {ticker} (id={existing_id})"
                    )

            return True, ""

    # ── Submit ────────────────────────────────────────────────────────────────

    def submit(
        self,
        broker,
        ticker: str,
        side: str,
        qty: int,
        limit_price: float,
        now: Optional[datetime] = None,
        *,
        signal_price: float = 0.0,
        atr_1m: float = 0.0,
        signal_timestamp: Optional[datetime] = None,
        sqs_score: float = 0.0,
        rvol: float = 0.0,
        spread_at_fill: float = 0.0,
        bid_at_signal: float = 0.0,
        ask_at_signal: float = 0.0,
    ) -> Optional[ManagedOrder]:
        """
        Submit a new order via the broker and track it.
        Returns the ManagedOrder, or None if submission was rejected pre-flight.
        """
        now = now or datetime.now(ET)
        ok, reason = self.can_submit(ticker, side)
        if not ok:
            log.warning(f"[OrderManager] Cannot submit {side} {ticker}: {reason}")
            return None

        order_id = f"OM-{uuid.uuid4().hex[:10].upper()}"
        order = ManagedOrder(
            order_id=order_id,
            ticker=ticker,
            side=side,
            qty=qty,
            limit_price=limit_price,
            status=ORDER_PENDING,
            submitted_at=now,
            signal_price=signal_price,
            atr_1m=atr_1m,
            signal_timestamp=signal_timestamp or now,
            sqs_score=sqs_score,
            rvol=rvol,
            spread_at_fill=spread_at_fill,
            bid_at_signal=bid_at_signal,
            ask_at_signal=ask_at_signal,
        )

        try:
            if side == "buy":
                result = broker.buy(ticker=ticker, qty=qty, limit_price=limit_price)
            else:
                result = broker.sell(ticker=ticker, qty=qty, limit_price=limit_price)

            if result.success:
                order.broker_order_id = result.order_id
                order.status = ORDER_SUBMITTED
                # If broker returned an immediate fill, mark it
                if result.filled_price and result.filled_price > 0:
                    order.filled_qty = qty
                    order.filled_price = result.filled_price
                    order.filled_at = result.filled_at or now
                    order.status = ORDER_FILLED
                    if order.side == "buy":
                        m = execution_monitor.on_entry_filled(
                            order_id=order.order_id,
                            ticker=order.ticker,
                            signal_price=order.signal_price or order.limit_price,
                            fill_price=order.filled_price,
                            atr_1m=order.atr_1m,
                            signal_timestamp=order.signal_timestamp,
                            fill_timestamp=order.filled_at,
                            sqs_score=order.sqs_score,
                            rvol=order.rvol,
                            spread_at_fill=order.spread_at_fill,
                            bid_at_signal=order.bid_at_signal,
                            ask_at_signal=order.ask_at_signal,
                        )
                        order.execution_metrics = {
                            "slippage_abs": m.slippage_abs,
                            "slippage_to_atr_pct": m.slippage_to_atr_pct,
                            "latency_ms": m.latency_ms,
                            "sqs_score": m.sqs_score,
                            "rvol": m.rvol,
                            "spread_at_fill": m.spread_at_fill,
                        }
                    self._emit(order, ORDER_FILLED, price=result.filled_price)
                    # Notify integrity gate of successful fill
                    integrity_gate.record_fill()
                else:
                    self._emit(order, ORDER_SUBMITTED)
                log.info(
                    f"[OrderManager] {side.upper()} {qty}sh {ticker} "
                    f"@ ${limit_price:.4f} → {order.status}"
                )
            else:
                order.status = ORDER_REJECTED
                order.reason = result.message
                self._emit(order, ORDER_REJECTED, reason=result.message)
                log.warning(
                    f"[OrderManager] {side.upper()} {ticker} REJECTED: {result.message}"
                )
                integrity_gate.record_reject(now=now)

        except Exception as exc:
            order.status = ORDER_REJECTED
            order.reason = str(exc)
            self._emit(order, ORDER_REJECTED, reason=str(exc))
            log.error(f"[OrderManager] {side.upper()} {ticker} exception: {exc}")

        with self._lock:
            self._orders[order_id] = order
            if order.is_active:
                self._pending_by_ticker[ticker] = order_id
            elif self._pending_by_ticker.get(ticker) == order_id:
                del self._pending_by_ticker[ticker]

        return order

    def tick(
        self,
        broker,
        now: Optional[datetime] = None,
    ) -> List[ManagedOrder]:
        """
        Called each engine cycle. Polls broker for fill updates, checks for
        TTL expiry and stuck orders.  Returns list of orders whose status
        changed this tick.
        """
        now = now or datetime.now(ET)
        cfg = CONFIG.order_manager
        changed: List[ManagedOrder] = []

        # ── Phase 1: broker status polling ───────────────────────────────────
        # Resolve fills / cancellations before running TTL / stuck checks so
        # that an already-filled order is not erroneously cancel-replaced.
        if hasattr(broker, "get_order_status"):
            with self._lock:
                poll_targets = [
                    o
                    for o in self._orders.values()
                    if o.status in (ORDER_SUBMITTED, ORDER_PARTIAL)
                    and o.broker_order_id
                ]
            for order in poll_targets:
                try:
                    bs = broker.get_order_status(order.broker_order_id)
                except Exception as exc:
                    log.warning(f"[OrderManager] poll {order.order_id}: {exc}")
                    bs = None
                if bs is None:
                    continue

                with self._lock:
                    if order.is_terminal:
                        continue  # already resolved between lock acquires
                    if bs.status == "filled":
                        order.filled_qty = bs.filled_qty or order.qty
                        order.filled_price = bs.filled_price
                        order.filled_at = now
                        order.status = ORDER_FILLED
                        if order.side == "buy":
                            m = execution_monitor.on_entry_filled(
                                order_id=order.order_id,
                                ticker=order.ticker,
                                signal_price=order.signal_price or order.limit_price,
                                fill_price=order.filled_price,
                                atr_1m=order.atr_1m,
                                signal_timestamp=order.signal_timestamp,
                                fill_timestamp=order.filled_at,
                                sqs_score=order.sqs_score,
                                rvol=order.rvol,
                                spread_at_fill=order.spread_at_fill,
                                bid_at_signal=order.bid_at_signal,
                                ask_at_signal=order.ask_at_signal,
                            )
                            order.execution_metrics = {
                                "slippage_abs": m.slippage_abs,
                                "slippage_to_atr_pct": m.slippage_to_atr_pct,
                                "latency_ms": m.latency_ms,
                                "sqs_score": m.sqs_score,
                                "rvol": m.rvol,
                                "spread_at_fill": m.spread_at_fill,
                            }
                        if self._pending_by_ticker.get(order.ticker) == order.order_id:
                            del self._pending_by_ticker[order.ticker]
                        self._emit(order, ORDER_FILLED, price=bs.filled_price)
                        integrity_gate.record_fill()
                        changed.append(order)
                        log.info(
                            f"[OrderManager] POLLED FILL {order.ticker} "
                            f"{order.order_id} {order.filled_qty}sh "
                            f"@ ${order.filled_price:.4f}"
                        )
                    elif bs.status == "partial" and bs.filled_qty > order.filled_qty:
                        order.filled_qty = bs.filled_qty
                        order.filled_price = bs.filled_price
                        order.status = ORDER_PARTIAL
                        self._emit(order, ORDER_PARTIAL, price=bs.filled_price)
                        changed.append(order)
                    elif bs.status in ("cancelled", "rejected"):
                        order.status = (
                            ORDER_CANCELLED
                            if bs.status == "cancelled"
                            else ORDER_REJECTED
                        )
                        order.reason = bs.reason or bs.status
                        if self._pending_by_ticker.get(order.ticker) == order.order_id:
                            del self._pending_by_ticker[order.ticker]
                        self._emit(order, order.status, reason=order.reason)
                        changed.append(order)

        # ── Phase 2: TTL / stuck checks ───────────────────────────────────────
        ttl_to_replace: List[ManagedOrder] = []

        with self._lock:
            for order in list(self._orders.values()):
                if order.is_terminal:
                    continue
                if order.submitted_at is None:
                    continue

                age_s = (now - order.submitted_at).total_seconds()

                # ── Stuck: beyond absolute maximum ───────────────────────────
                if age_s > cfg.stuck_order_seconds:
                    self._do_cancel(broker, order, reason="stuck", now=now)
                    order.status = ORDER_STUCK
                    self._emit(order, ORDER_STUCK, reason=f"age={age_s:.0f}s")
                    if self._pending_by_ticker.get(order.ticker) == order.order_id:
                        del self._pending_by_ticker[order.ticker]
                    changed.append(order)
                    log.warning(
                        f"[OrderManager] STUCK order {order.order_id} "
                        f"({order.ticker} {order.side}) age={age_s:.0f}s"
                    )
                    integrity_gate.record_reject(now=now)

                # ── TTL cancel/replace ────────────────────────────────────────
                elif age_s > cfg.limit_order_ttl_seconds:
                    # PARTIAL orders: only cancel-replace when explicitly enabled.
                    # If disabled, let them age naturally until stuck_order_seconds.
                    if (
                        order.status == ORDER_PARTIAL
                        and not cfg.cancel_replace_on_partial
                    ):
                        pass
                    elif order.cancel_replace_count < 1:
                        # Increment counter BEFORE marking REPLACED so the new
                        # order inherits the correct count on creation.
                        order.cancel_replace_count += 1
                        self._do_cancel(broker, order, reason="ttl_cancel", now=now)
                        order.status = ORDER_REPLACED
                        self._emit(order, ORDER_REPLACED, reason="ttl_cancel_replace")
                        if self._pending_by_ticker.get(order.ticker) == order.order_id:
                            del self._pending_by_ticker[order.ticker]
                        changed.append(order)
                        ttl_to_replace.append(order)
                        log.info(
                            f"[OrderManager] TTL cancel-replace "
                            f"{order.ticker} {order.order_id} "
                            f"(C/R #{order.cancel_replace_count})"
                        )
                    else:
                        # Already replaced once — final cancel
                        self._do_cancel(broker, order, reason="ttl_final", now=now)
                        order.status = ORDER_CANCELLED
                        self._emit(order, ORDER_CANCELLED, reason="ttl_final")
                        if self._pending_by_ticker.get(order.ticker) == order.order_id:
                            del self._pending_by_ticker[order.ticker]
                        changed.append(order)
                        log.info(
                            f"[OrderManager] TTL final cancel "
                            f"{order.ticker} {order.order_id}"
                        )

        # ── Phase 3: resubmit C/R orders (outside lock) ───────────────────────
        for order in ttl_to_replace:
            new_limit = self._reprice(order)
            new = self.submit(
                broker,
                ticker=order.ticker,
                side=order.side,
                qty=order.remaining_qty or order.qty,
                limit_price=new_limit,
                now=now,
            )
            if new:
                # Inherit the incremented counter so a second TTL triggers final cancel
                new.cancel_replace_count = order.cancel_replace_count

        return changed

    # ── Fill recording ────────────────────────────────────────────────────────

    def record_fill(
        self,
        order_id: str,
        fill_price: float,
        fill_qty: int,
        now: Optional[datetime] = None,
    ) -> None:
        """
        Record a fill event (e.g. from broker callback or reconciliation).
        Handles partial and full fills.
        """
        now = now or datetime.now(ET)
        with self._lock:
            order = self._orders.get(order_id)
            if order is None:
                return

            order.filled_qty += fill_qty
            order.filled_price = fill_price

            if order.filled_qty >= order.qty:
                order.status = ORDER_FILLED
                order.filled_at = now
                if order.side == "buy":
                    m = execution_monitor.on_entry_filled(
                        order_id=order.order_id,
                        ticker=order.ticker,
                        signal_price=order.signal_price or order.limit_price,
                        fill_price=order.filled_price,
                        atr_1m=order.atr_1m,
                        signal_timestamp=order.signal_timestamp,
                        fill_timestamp=order.filled_at,
                        sqs_score=order.sqs_score,
                        rvol=order.rvol,
                        spread_at_fill=order.spread_at_fill,
                        bid_at_signal=order.bid_at_signal,
                        ask_at_signal=order.ask_at_signal,
                    )
                    order.execution_metrics = {
                        "slippage_abs": m.slippage_abs,
                        "slippage_to_atr_pct": m.slippage_to_atr_pct,
                        "latency_ms": m.latency_ms,
                        "sqs_score": m.sqs_score,
                        "rvol": m.rvol,
                        "spread_at_fill": m.spread_at_fill,
                    }
                if self._pending_by_ticker.get(order.ticker) == order_id:
                    del self._pending_by_ticker[order.ticker]
                self._emit(order, ORDER_FILLED, price=fill_price)
            else:
                order.status = ORDER_PARTIAL
                self._emit(order, ORDER_PARTIAL, price=fill_price)

    # ── Queries ───────────────────────────────────────────────────────────────

    def get_order(self, order_id: str) -> Optional[ManagedOrder]:
        with self._lock:
            return self._orders.get(order_id)

    def active_orders(self) -> List[ManagedOrder]:
        """Returns all currently active (non-terminal) orders."""
        with self._lock:
            return [o for o in self._orders.values() if o.is_active]

    def session_events(self) -> List[OrderEvent]:
        """Returns all order events recorded this session."""
        with self._lock:
            return list(self._events)

    def reset(self) -> None:
        """Clear all state. Call at session start."""
        with self._lock:
            self._orders.clear()
            self._pending_by_ticker.clear()
            self._events.clear()
        log.info("[OrderManager] Reset for new session")

    def recover_order(
        self,
        broker_order_id: str,
        ticker: str,
        side: str,
        qty: int,
        status: str,
        limit_price: float = 0.0,
        filled_qty: int = 0,
        filled_price: float = 0.0,
        now: Optional[datetime] = None,
    ) -> Optional[ManagedOrder]:
        """
        Re-register a broker order discovered at startup so that
        can_submit() correctly blocks duplicate orders for the same ticker.

        This is idempotent: if the order is already tracked, it is a no-op.
        Only active statuses (PENDING / SUBMITTED / PARTIAL_FILLED) are
        registered — terminal orders are ignored.

        Returns the ManagedOrder if registered, None if skipped.
        """
        # Map broker status strings to internal constants
        from config.constants import (
            ORDER_PARTIAL,
            ORDER_PENDING,
            ORDER_SUBMITTED,
        )

        active_statuses = {
            ORDER_PENDING,
            ORDER_SUBMITTED,
            ORDER_PARTIAL,
            "open",
            "partially_filled",
            "new",
            "accepted",
        }
        if status.lower() not in {s.lower() for s in active_statuses}:
            return None  # terminal — nothing to register

        now = now or datetime.now(ET)
        order_id = f"RECOVERED-{broker_order_id[:16]}"

        with self._lock:
            # Idempotency: skip if already tracked
            if order_id in self._orders:
                return self._orders[order_id]
            # Also skip if a different active order is already tracked for ticker
            if ticker in self._pending_by_ticker:
                return self._orders.get(self._pending_by_ticker[ticker])

            order = ManagedOrder(
                order_id=order_id,
                ticker=ticker,
                side=side,
                qty=qty,
                limit_price=limit_price,
                status=ORDER_SUBMITTED,
                submitted_at=now,
                filled_qty=filled_qty,
                filled_price=filled_price,
                broker_order_id=broker_order_id,
                reason="recovered_on_restart",
            )
            self._orders[order_id] = order
            self._pending_by_ticker[ticker] = order_id
            log.warning(
                f"[OrderManager] RECOVERED order: {broker_order_id} "
                f"{side} {ticker} {qty}sh (status={status})"
            )
            return order

    # ── Internals ─────────────────────────────────────────────────────────────

    def _reprice(self, order: ManagedOrder) -> float:
        """
        Compute a repriced limit for a cancel-replace attempt.

        Buys:  nudge up by cancel_replace_reprice_step (makes fill more likely).
        Sells: nudge down by the same step.

        The result is capped so total deviation from original stays within
        cancel_replace_slippage_cap.
        """
        cfg = CONFIG.execution
        step = cfg.cancel_replace_reprice_step
        cap = cfg.cancel_replace_slippage_cap
        base = order.limit_price

        if order.side == "buy":
            new_price = base * (1 + step)
            max_price = base * (1 + cap)
            return round(min(new_price, max_price), 4)
        else:
            new_price = base * (1 - step)
            min_price = base * (1 - cap)
            return round(max(new_price, min_price), 4)

    def _do_cancel(
        self,
        broker,
        order: ManagedOrder,
        reason: str,
        now: datetime,
    ) -> None:
        """Best-effort broker cancel (called within lock)."""
        try:
            if hasattr(broker, "cancel_order") and order.broker_order_id:
                broker.cancel_order(order.broker_order_id)
        except Exception as exc:
            log.warning(f"[OrderManager] cancel {order.order_id} failed: {exc}")
        order.reason = reason

    def _emit(
        self,
        order: ManagedOrder,
        event: str,
        price: float = 0.0,
        reason: str = "",
    ) -> None:
        """Append an order event to the session log and publish a domain event."""
        # For fill events, report how many shares were actually filled,
        # not the original requested quantity.
        event_qty = (
            order.filled_qty if event in (ORDER_FILLED, ORDER_PARTIAL) else order.qty
        )
        self._events.append(
            OrderEvent(
                order_id=order.order_id,
                ticker=order.ticker,
                side=order.side,
                event=event,
                qty=event_qty,
                price=price or order.filled_price,
                reason=reason or order.reason,
            )
        )
        # ── Publish domain event (lazy import to avoid circular deps) ─────────
        try:
            from events.bus import event_bus
            from events import current_cycle
            from events.types import (
                OrderCancelled,
                OrderFilled,
                OrderPartial,
                OrderSubmitted,
            )

            if event == ORDER_SUBMITTED:
                event_bus.publish(
                    OrderSubmitted(
                        cycle_id=current_cycle.id,
                        order_id=order.order_id,
                        ticker=order.ticker,
                        side=order.side,
                        qty=event_qty,
                        limit_price=order.limit_price,
                    )
                )
            elif event == ORDER_FILLED:
                event_bus.publish(
                    OrderFilled(
                        cycle_id=current_cycle.id,
                        order_id=order.order_id,
                        ticker=order.ticker,
                        side=order.side,
                        filled_qty=event_qty,
                        filled_price=price or order.filled_price,
                    )
                )
            elif event == ORDER_PARTIAL:
                event_bus.publish(
                    OrderPartial(
                        cycle_id=current_cycle.id,
                        order_id=order.order_id,
                        ticker=order.ticker,
                        filled_qty=event_qty,
                        remaining=max(0, order.qty - event_qty),
                        fill_price=price or order.filled_price,
                    )
                )
            elif event in (ORDER_CANCELLED, ORDER_STUCK, ORDER_REPLACED):
                event_bus.publish(
                    OrderCancelled(
                        cycle_id=current_cycle.id,
                        order_id=order.order_id,
                        ticker=order.ticker,
                        reason=reason or order.reason or event,
                    )
                )
        except Exception:  # noqa: BLE001
            pass  # domain events are best-effort; never block order flow


# Module-level singleton
order_manager = OrderManager()
