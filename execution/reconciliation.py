"""
execution/reconciliation.py — Broker truth reconciliation loop.

Alpaca (or any broker) is the source of truth.
This module periodically compares our internal position state
against what the broker actually reports.

Mismatch types detected:
  RECON_POSITION_MISSING_FROM_BROKER — we think we hold X shares; broker shows 0
  RECON_UNKNOWN_BROKER_POSITION      — broker has a position we don't know about
  RECON_QTY_MISMATCH                 — qty disagreement beyond tolerance

On any mismatch:
  • Logs the incident with full detail
  • If halt_on_mismatch=True → calls integrity_gate.force_halt()
  • Appends to incident_log for post-session review

After manual investigation:
  • Call reconciler.mark_resolved() to re-arm the gate

Thread-safe.
"""

# IBKR is the only supported broker for reconciliation.


# ─────────────────────────────────────────────────────────────────────────────
# DATA TYPES
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class ReconMismatch:
    mismatch_type: str
    ticker: str
    internal_qty: int
    broker_qty: int
    detail: str
    detected_at: datetime = field(default_factory=lambda: datetime.now(ET))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "mismatch_type": self.mismatch_type,
            "ticker": self.ticker,
            "internal_qty": self.internal_qty,
            "broker_qty": self.broker_qty,
            "detail": self.detail,
            "detected_at": self.detected_at.isoformat(),
        }


@dataclass
class ReconResult:
    run_at: datetime
    status: str  # RECON_OK | RECON_HALTED
    mismatches: List[ReconMismatch] = field(default_factory=list)
    broker_positions: Dict[str, int] = field(default_factory=dict)
    internal_positions: Dict[str, int] = field(default_factory=dict)
    buying_power: float = 0.0
    note: str = ""

    @property
    def is_clean(self) -> bool:
        return self.status == RECON_OK

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_at": self.run_at.isoformat(),
            "status": self.status,
            "buying_power": self.buying_power,
            "broker_positions": self.broker_positions,
            "internal_positions": self.internal_positions,
            "mismatches": [m.to_dict() for m in self.mismatches],
            "note": self.note,
        }


# ─────────────────────────────────────────────────────────────────────────────
# RECONCILER
# ─────────────────────────────────────────────────────────────────────────────


class BrokerReconciler:
    """
    Compares broker state vs risk_manager open trades.

    Usage:
        reconciler.reconcile(broker, risk_manager, event_log)
    """

    def __init__(self) -> None:
        self._lock: threading.Lock = threading.Lock()
        self._last_result: Optional[ReconResult] = None
        self._is_halted: bool = False
        self._incident_log: List[ReconResult] = []

    # ── Public API ────────────────────────────────────────────────────────────

    def reconcile(
        self,
        broker,
        risk_manager,
        event_log=None,
        now: Optional[datetime] = None,
    ) -> ReconResult:
        """
        Runs a full reconciliation pass.
        Returns a ReconResult. Halts integrity gate on mismatch if configured.
        """
        now = now or datetime.now(ET)
        cfg = CONFIG.reconciliation
        # ── Skip for PaperBroker (no _client) — always clean ─────────────────
        if not hasattr(broker, "_client"):
            result = ReconResult(
                run_at=now,
                status=RECON_OK,
                note="PaperBroker: reconciliation skipped (no live positions)",
            )
            return self._store(result, event_log)
        # ── Fetch broker positions ────────────────────────────────────────────
        broker_positions: Dict[str, int] = {}
        buying_power = 0.0

        try:
            broker_positions, buying_power = self._fetch_broker_positions(broker)
        except Exception as exc:
            log.error(f"[Reconciler] Failed to fetch broker positions: {exc}")
            result = ReconResult(
                run_at=now,
                status=RECON_HALTED,
                note=f"broker fetch error: {exc}",
            )
            return self._store(result, event_log)

        # ── Build internal position map ───────────────────────────────────────
        internal_positions: Dict[str, int] = {}
        for trade in risk_manager.open_trades():
            qty = trade.shares_remaining if trade.shares_remaining > 0 else trade.shares
            internal_positions[trade.ticker] = (
                internal_positions.get(trade.ticker, 0) + qty
            )

        # ── Detect mismatches ─────────────────────────────────────────────────
        mismatches: List[ReconMismatch] = []

        for ticker, internal_qty in internal_positions.items():
            broker_qty = broker_positions.get(ticker, 0)
            if broker_qty == 0:
                mismatches.append(
                    ReconMismatch(
                        mismatch_type=RECON_POS_MISSING,
                        ticker=ticker,
                        internal_qty=internal_qty,
                        broker_qty=0,
                        detail=(
                            f"internal holds {internal_qty}sh {ticker} "
                            f"but broker shows 0"
                        ),
                    )
                )
            elif abs(broker_qty - internal_qty) > cfg.qty_tolerance:
                mismatches.append(
                    ReconMismatch(
                        mismatch_type=RECON_QTY_MISMATCH,
                        ticker=ticker,
                        internal_qty=internal_qty,
                        broker_qty=broker_qty,
                        detail=(
                            f"{ticker}: internal={internal_qty} broker={broker_qty} "
                            f"(tolerance={cfg.qty_tolerance})"
                        ),
                    )
                )

        for ticker, broker_qty in broker_positions.items():
            if ticker not in internal_positions:
                mismatches.append(
                    ReconMismatch(
                        mismatch_type=RECON_POS_UNKNOWN,
                        ticker=ticker,
                        internal_qty=0,
                        broker_qty=broker_qty,
                        detail=(
                            f"broker holds {broker_qty}sh {ticker} "
                            f"unknown to internal state"
                        ),
                    )
                )

        status = RECON_HALTED if mismatches else RECON_OK
        result = ReconResult(
            run_at=now,
            status=status,
            mismatches=mismatches,
            broker_positions=broker_positions,
            internal_positions=internal_positions,
            buying_power=buying_power,
        )

        if mismatches:
            for m in mismatches:
                log.error(
                    f"[Reconciler] MISMATCH {m.mismatch_type} {m.ticker}: {m.detail}"
                )
            if cfg.halt_on_mismatch:
                integrity_gate.force_halt(
                    f"reconciliation found {len(mismatches)} mismatch(es) — "
                    "manual review required"
                )
                with self._lock:
                    self._is_halted = True

        return self._store(result, event_log)

    def mark_resolved(self) -> None:
        """
        Call after manual investigation of a mismatch to re-arm the gate.
        """
        with self._lock:
            self._is_halted = False
        integrity_gate.force_clear()
        log.info("[Reconciler] Mismatch marked resolved — integrity gate cleared")

    @property
    def last_result(self) -> Optional[ReconResult]:
        with self._lock:
            return self._last_result

    @property
    def is_halted(self) -> bool:
        with self._lock:
            return self._is_halted

    def incident_log(self) -> List[ReconResult]:
        """Returns all non-clean reconciliation results this session."""
        with self._lock:
            return list(self._incident_log)

    def reset(self) -> None:
        """Clear state for new session."""
        with self._lock:
            self._last_result = None
            self._is_halted = False
            self._incident_log = []
        log.info("[Reconciler] Reset for new session")

    # ── Internals ─────────────────────────────────────────────────────────────

    def _fetch_broker_positions(self, broker) -> Tuple[Dict[str, int], float]:
        """
        Extract current positions and buying power from the broker adapter.
        Uses IBConnectionManager natively rather than Alpaca REST calls.
        """
        positions: Dict[str, int] = {}
        buying_power = 0.0

        if (
            hasattr(broker, "_client")
            or hasattr(broker, "_ib")
            or str(broker.__class__.__name__).lower().find("ib") >= 0
            or getattr(broker, "adapter_name", "") in ("ibkr", "alpaca")
        ):
            try:
                from infrastructure.ib_connection import IBConnectionManager

                conn_mgr = IBConnectionManager()
                if not conn_mgr.is_connected:
                    conn_mgr.connect()
                ib = conn_mgr.ib

                ib_positions = ib.positions()
                for pos in ib_positions:
                    sym = pos.contract.symbol
                    qty = int(pos.position)
                    if qty != 0:
                        positions[sym] = qty

                acct_summary = ib.accountSummary()
                # Find BuyingPower in the summary
                for item in acct_summary:
                    if item.tag == "BuyingPower":
                        buying_power = float(item.value)
                        break

            except Exception as e:
                log.error(f"[Reconciler] Failed to fetch IBKR positions/account: {e}")
                pass
        # else: PaperBroker — no real positions; returns empty dict (always clean)

        return positions, buying_power

    def _store(
        self,
        result: ReconResult,
        event_log,
    ) -> ReconResult:
        """Persist result and emit to event log."""
        with self._lock:
            self._last_result = result
            if not result.is_clean:
                self._incident_log.append(result)

        if event_log is not None:
            try:
                from config.constants import EVT_RECON_HALT, EVT_RECON_MISMATCH

                evt_type = EVT_RECON_HALT if not result.is_clean else RECON_OK
                event_log.log(evt_type, payload=result.to_dict())
            except Exception as exc:
                log.debug(f"[Reconciler] event_log emit failed: {exc}")

        return result


# Module-level singleton
reconciler = BrokerReconciler()
