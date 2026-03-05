"""
tests/test_order_manager.py -- Unit tests for execution/order_manager.py
"""
import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch
import pytz

ET = pytz.timezone("America/New_York")

from config.constants import (
    ORDER_PENDING, ORDER_SUBMITTED, ORDER_PARTIAL, ORDER_FILLED,
    ORDER_CANCELLED, ORDER_REJECTED, ORDER_STUCK,
)
from execution.order_manager import OrderManager, ManagedOrder


# -- Config save/restore fixture -----------------------------------------------

@pytest.fixture(autouse=True)
def _reset_om_cfg():
    """Save/restore CONFIG.order_manager around every test."""
    from config.settings import CONFIG
    c = CONFIG.order_manager
    saved = {
        'limit_order_ttl_seconds': c.limit_order_ttl_seconds,
        'stuck_order_seconds':     c.stuck_order_seconds,
        'max_pending_orders':      c.max_pending_orders,
        'cancel_replace_on_partial': c.cancel_replace_on_partial,
    }
    yield
    for k, v in saved.items():
        setattr(c, k, v)


def _cfg():
    from config.settings import CONFIG
    return CONFIG.order_manager


# -- Helpers -------------------------------------------------------------------

def fresh_manager() -> OrderManager:
    om = OrderManager()
    om.reset()
    return om


class _BrokerResult:
    """Minimal result object returned by broker.buy() / broker.sell()."""
    def __init__(self, success=True, order_id="BRK-001",
                 filled_price=0.0, filled_at=None, message=""):
        self.success      = success
        self.order_id     = order_id
        self.filled_price = filled_price
        self.filled_at    = filled_at
        self.message      = message


def mock_broker(accept=True, broker_order_id="BRK-001"):
    """Return a mock broker whose buy()/sell() match the real interface."""
    b = MagicMock()
    if accept:
        result = _BrokerResult(success=True, order_id=broker_order_id,
                               filled_price=0.0, filled_at=None, message="")
        b.buy.return_value  = result
        b.sell.return_value = result
        b.cancel_order.return_value = True
    else:
        fail = _BrokerResult(success=False, order_id="", message="broker reject")
        b.buy.return_value  = fail
        b.sell.return_value = fail
        b.cancel_order.return_value = True
    return b


def now():
    return datetime.now(ET)


# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------

class TestCanSubmit:
    def test_can_submit_fresh(self):
        om = fresh_manager()
        ok, reason = om.can_submit("AAPL", "buy")
        assert ok, reason

    def test_duplicate_rejected(self):
        om = fresh_manager()
        broker = mock_broker()
        om.submit(broker, "AAPL", "buy", 100, 150.0, now())
        ok, reason = om.can_submit("AAPL", "buy")
        assert not ok
        assert "duplicate" in reason.lower() or "active" in reason.lower()

    def test_max_pending_cap(self):
        om = fresh_manager()
        broker = mock_broker()
        _cfg().max_pending_orders = 2
        om.submit(broker, "AAPL", "buy",  100, 150.0, now())
        om.submit(broker, "TSLA", "buy",   50, 300.0, now())
        ok, reason = om.can_submit("NVDA", "buy")
        assert not ok
        assert "max_pending" in reason.lower() or "pending" in reason.lower()


class TestSubmit:
    def test_submit_creates_submitted_order(self):
        om = fresh_manager()
        broker = mock_broker()
        order = om.submit(broker, "AAPL", "buy", 100, 150.0, now())
        assert order is not None
        assert order.status == ORDER_SUBMITTED
        assert order.ticker == "AAPL"

    def test_submit_returns_rejected_on_broker_reject(self):
        om = fresh_manager()
        broker = mock_broker(accept=False)
        order = om.submit(broker, "AAPL", "buy", 100, 150.0, now())
        # May return the ManagedOrder with REJECTED status, or None
        if order is not None:
            assert order.status == ORDER_REJECTED
        else:
            terminal = [o for o in om._orders.values() if o.status == ORDER_REJECTED]
            assert len(terminal) == 1

    def test_submit_broker_reject_marks_rejected(self):
        om = fresh_manager()
        broker = mock_broker(accept=False)
        om.submit(broker, "AAPL", "buy", 100, 150.0, now())
        terminal = [o for o in om._orders.values() if o.status == ORDER_REJECTED]
        assert len(terminal) == 1

    def test_submit_broker_reject_records_in_integrity_gate(self):
        om = fresh_manager()
        broker = mock_broker(accept=False)
        with patch("execution.order_manager.integrity_gate") as mock_gate:
            om.submit(broker, "AAPL", "buy", 100, 150.0, now())
            mock_gate.record_reject.assert_called_once()


class TestTick:
    def test_tick_ttl_cancel_replace(self):
        om = fresh_manager()
        broker = mock_broker()
        _cfg().limit_order_ttl_seconds = 10
        _cfg().stuck_order_seconds     = 90
        ts = now()
        order = om.submit(broker, "AAPL", "buy", 100, 150.0, ts)
        assert order is not None
        # Advance past TTL but under stuck threshold
        om.tick(broker, ts + timedelta(seconds=35))
        assert broker.cancel_order.called

    def test_tick_stuck_detection(self):
        om = fresh_manager()
        broker = mock_broker()
        _cfg().limit_order_ttl_seconds = 5
        _cfg().stuck_order_seconds     = 20
        ts = now()
        order = om.submit(broker, "AAPL", "buy", 100, 150.0, ts)
        assert order is not None
        # First pass past TTL (age=6s > ttl=5s) → cancel+replace at t+6s
        om.tick(broker, ts + timedelta(seconds=6))
        # Second pass: replacement order was submitted at t+6s; at t+30s its age=24s > stuck=20s
        om.tick(broker, ts + timedelta(seconds=30))
        stuck = [o for o in om._orders.values() if o.status == ORDER_STUCK]
        assert len(stuck) == 1


class TestRecordFill:
    def test_partial_fill(self):
        om = fresh_manager()
        broker = mock_broker()
        order = om.submit(broker, "AAPL", "buy", 100, 150.0, now())
        assert order is not None
        om.record_fill(order.order_id, 150.0, 50, now())
        assert om._orders[order.order_id].status == ORDER_PARTIAL
        assert om._orders[order.order_id].filled_qty == 50

    def test_full_fill(self):
        om = fresh_manager()
        broker = mock_broker()
        order = om.submit(broker, "AAPL", "buy", 100, 150.0, now())
        assert order is not None
        om.record_fill(order.order_id, 150.0, 100, now())
        assert om._orders[order.order_id].status == ORDER_FILLED

    def test_full_fill_removes_from_active(self):
        om = fresh_manager()
        broker = mock_broker()
        order = om.submit(broker, "AAPL", "buy", 100, 150.0, now())
        assert order is not None
        om.record_fill(order.order_id, 150.0, 100, now())
        active = om.active_orders()
        assert all(o.order_id != order.order_id for o in active)


class TestActiveOrders:
    def test_active_orders_returns_non_terminal(self):
        om = fresh_manager()
        broker = mock_broker()
        order = om.submit(broker, "AAPL", "buy", 100, 150.0, now())
        assert order is not None
        assert any(o.order_id == order.order_id for o in om.active_orders())

    def test_reset_clears_all(self):
        om = fresh_manager()
        broker = mock_broker()
        om.submit(broker, "AAPL", "buy", 100, 150.0, now())
        om.reset()
        assert om.active_orders() == []
        assert om._orders == {}
