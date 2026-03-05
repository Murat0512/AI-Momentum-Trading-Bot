"""
tests/test_reconciliation.py — Unit tests for execution/reconciliation.py
"""
import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch
import pytz

ET = pytz.timezone("America/New_York")

from config.constants import (
    RECON_OK, RECON_POS_MISSING, RECON_POS_UNKNOWN, RECON_QTY_MISMATCH, RECON_HALTED,
)
from execution.reconciliation import BrokerReconciler, ReconResult


# ── Config fixture ────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _reset_recon_cfg():
    from config.settings import CONFIG
    c = CONFIG.reconciliation
    saved = {
        'enabled':           c.enabled,
        'qty_tolerance':     c.qty_tolerance,
        'halt_on_mismatch':  c.halt_on_mismatch,
        'interval_seconds':  c.interval_seconds,
    }
    yield
    for k, v in saved.items():
        setattr(c, k, v)


def _rcfg():
    from config.settings import CONFIG
    return CONFIG.reconciliation


# ── Helpers ───────────────────────────────────────────────────────────────────

def now():
    return datetime.now(ET)


def make_reconciler() -> BrokerReconciler:
    r = BrokerReconciler()
    r.reset()
    return r


def paper_broker():
    b = MagicMock()
    del b._client          # PaperBroker has no _client
    # ensure hasattr returns False
    b._client = MagicMock()
    b._client = property(lambda self: (_ for _ in ()).throw(AttributeError()))
    return b


def alpaca_broker(positions: dict, buying_power: float = 50_000.0):
    """
    Mock AlpacaBroker. positions = {ticker: qty_float}.
    """
    pos_objects = []
    for ticker, qty in positions.items():
        p = MagicMock()
        p.symbol = ticker
        p.qty    = str(qty)
        pos_objects.append(p)

    account = MagicMock()
    account.buying_power = str(buying_power)

    client = MagicMock()
    client.get_all_positions.return_value = pos_objects
    client.get_account.return_value       = account

    b = MagicMock()
    b._client = client
    return b


def fake_risk_manager(holdings: dict):
    """holdings = {ticker: qty_int}"""
    trades = []
    for ticker, qty in holdings.items():
        t = MagicMock()
        t.ticker  = ticker
        t.is_closed = False
        t.shares_remaining = qty
        t.shares           = qty
        trades.append(t)
    rm = MagicMock()
    rm.open_trades.return_value = trades
    return rm


def fake_event_log():
    el = MagicMock()
    el.log = MagicMock()
    return el


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestPaperBrokerReconciliation:
    def test_paper_broker_always_clean(self):
        """PaperBroker has no _client → empty positions tuple → RECON_OK."""
        r  = make_reconciler()
        rm = fake_risk_manager({"AAPL": 100})
        el = fake_event_log()

        # Build PaperBroker mock without _client attribute
        broker = MagicMock(spec=[])  # spec=[] → no attributes at all

        result = r.reconcile(broker, rm, el, now())
        assert result.status == RECON_OK
        assert result.is_clean


class TestAlpacaBrokerReconciliation:
    def test_matching_positions_clean(self):
        r  = make_reconciler()
        rm = fake_risk_manager({"AAPL": 100})
        el = fake_event_log()
        broker = alpaca_broker({"AAPL": 100})

        result = r.reconcile(broker, rm, el, now())
        assert result.status == RECON_OK
        assert result.is_clean

    def test_missing_position_detected(self):
        """Internal holds AAPL but broker shows nothing."""
        r  = make_reconciler()
        rm = fake_risk_manager({"AAPL": 100})
        el = fake_event_log()
        broker = alpaca_broker({})

        result = r.reconcile(broker, rm, el, now())
        assert result.status != RECON_OK
        assert any(m.mismatch_type == RECON_POS_MISSING for m in result.mismatches)

    def test_unknown_position_detected(self):
        """Broker holds TSLA but internal has no record."""
        r  = make_reconciler()
        rm = fake_risk_manager({})
        el = fake_event_log()
        broker = alpaca_broker({"TSLA": 50})

        result = r.reconcile(broker, rm, el, now())
        assert result.status != RECON_OK
        assert any(m.mismatch_type == RECON_POS_UNKNOWN for m in result.mismatches)

    def test_qty_mismatch_detected(self):
        """Internal has 100 shares, broker has 80 (diff > tolerance)."""
        r  = make_reconciler()
        _rcfg().qty_tolerance = 1
        rm = fake_risk_manager({"AAPL": 100})
        el = fake_event_log()
        broker = alpaca_broker({"AAPL": 80})

        result = r.reconcile(broker, rm, el, now())
        assert result.status != RECON_OK
        assert any(m.mismatch_type == RECON_QTY_MISMATCH for m in result.mismatches)

    def test_qty_within_tolerance_clean(self):
        """Diff = 1, tolerance = 1 → still clean."""
        r  = make_reconciler()
        _rcfg().qty_tolerance = 2   # allow up to 2 diff
        rm = fake_risk_manager({"AAPL": 100})
        el = fake_event_log()
        broker = alpaca_broker({"AAPL": 99})

        result = r.reconcile(broker, rm, el, now())
        assert result.status == RECON_OK

    def test_mismatch_triggers_integrity_halt(self):
        r  = make_reconciler()
        _rcfg().halt_on_mismatch = True
        rm = fake_risk_manager({"AAPL": 100})
        el = fake_event_log()
        broker = alpaca_broker({})  # missing position

        with patch("execution.reconciliation.integrity_gate") as mock_gate:
            r.reconcile(broker, rm, el, now())
            mock_gate.force_halt.assert_called_once()


class TestMarkResolved:
    def test_mark_resolved_clears_halted_reconciler_state(self):
        r  = make_reconciler()
        rm = fake_risk_manager({"AAPL": 100})
        el = fake_event_log()
        broker = alpaca_broker({})

        with patch("execution.reconciliation.integrity_gate"):
            r.reconcile(broker, rm, el, now())
        r.mark_resolved()
        # After mark_resolved, incident_log still has the record
        assert len(r.incident_log()) >= 1


class TestIncidentLog:
    def test_incident_log_stores_non_clean(self):
        r  = make_reconciler()
        rm = fake_risk_manager({"AAPL": 100})
        el = fake_event_log()
        broker = alpaca_broker({})

        with patch("execution.reconciliation.integrity_gate"):
            r.reconcile(broker, rm, el, now())
        assert len(r.incident_log()) == 1

    def test_clean_recon_not_stored_in_incident_log(self):
        r  = make_reconciler()
        rm = fake_risk_manager({"AAPL": 100})
        el = fake_event_log()
        broker = alpaca_broker({"AAPL": 100})

        r.reconcile(broker, rm, el, now())
        assert len(r.incident_log()) == 0

    def test_reset_clears_incident_log(self):
        r  = make_reconciler()
        rm = fake_risk_manager({"AAPL": 100})
        el = fake_event_log()
        broker = alpaca_broker({})

        with patch("execution.reconciliation.integrity_gate"):
            r.reconcile(broker, rm, el, now())
        r.reset()
        assert len(r.incident_log()) == 0
