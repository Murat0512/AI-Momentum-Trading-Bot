# pyright: reportPrivateUsage=false, reportAttributeAccessIssue=false, reportUnknownLambdaType=false, reportUnknownParameterType=false, reportUnusedFunction=false

from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch

import pytest
import pytz

from config.settings import CONFIG
from execution.engine import TradingEngine

ET = pytz.timezone("America/New_York")


@pytest.fixture(autouse=True)
def _restore_execution_config():
    original_cooldown = CONFIG.execution.post_exit_cooldown_minutes
    original_wash = CONFIG.execution.enable_wash_guard
    yield
    CONFIG.execution.post_exit_cooldown_minutes = original_cooldown
    CONFIG.execution.enable_wash_guard = original_wash


def _engine_stub() -> TradingEngine:
    engine = TradingEngine.__new__(TradingEngine)
    engine._ticker_states = {}
    return engine


def test_post_exit_cooldown_blocks_until_expiry():
    engine = _engine_stub()
    now = datetime.now(ET)

    CONFIG.execution.post_exit_cooldown_minutes = 2
    engine._mark_ticker_exit("AAPL", now)

    blocked, reason = engine._is_post_exit_cooldown_active("AAPL", now)
    assert blocked is True
    assert "COOLDOWN_ACTIVE" in reason


def test_post_exit_cooldown_expires_after_window():
    engine = _engine_stub()
    now = datetime.now(ET)

    CONFIG.execution.post_exit_cooldown_minutes = 2
    engine._mark_ticker_exit("AAPL", now)

    blocked, reason = engine._is_post_exit_cooldown_active(
        "AAPL", now + timedelta(minutes=3)
    )
    assert blocked is False
    assert reason == ""


def test_wash_guard_blocks_when_local_active_order_exists():
    engine = _engine_stub()
    engine._broker = SimpleNamespace()

    with patch(
        "execution.engine.order_manager.active_orders",
        return_value=[SimpleNamespace(ticker="AAPL")],
    ):
        ok, reason = engine._run_wash_guard("AAPL")

    assert ok is False
    assert "local_active_orders" in reason


def test_wash_guard_passes_when_broker_state_is_clean():
    engine = _engine_stub()
    engine._broker = SimpleNamespace(
        cancel_open_orders=lambda ticker=None: 1,
        list_open_orders=lambda ticker=None: [],
        has_open_position=lambda ticker: False,
    )

    with patch("execution.engine.order_manager.active_orders", return_value=[]):
        ok, reason = engine._run_wash_guard("AAPL")

    assert ok is True
    assert "cleared=1" in reason
