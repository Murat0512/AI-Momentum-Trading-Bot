import re

with open("execution/startup_recovery.py", "r", encoding="utf-8") as f:
    text = f.read()

replacement = """    log.info("[StartupRecovery] Live Broker detected — beginning reconstruction")

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
        # Non-fatal: continue without order reconstruction"""

old_pattern = r'log\.info\("\[StartupRecovery\] AlpacaBroker detected(.*?)# Non-fatal: continue without order reconstruction'

text = re.sub(old_pattern, replacement, text, flags=re.DOTALL)

with open("execution/startup_recovery.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Done updating startup_recovery.py")
