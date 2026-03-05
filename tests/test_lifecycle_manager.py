from __future__ import annotations

from execution.lifecycle_manager import LifecycleManager


def _pos(**overrides):
    base = {
        "ticker": "TSLA",
        "entry_price": 100.0,
        "stop_loss": 99.0,
        "initial_stop_loss": 99.0,
        "shares": 100,
        "shares_already_sold": 0,
        "partial_1_taken": False,
        "partial_2_taken": False,
        "high_watermark": 100.0,
        "below_vwap": False,
    }
    base.update(overrides)
    return base


def test_partial1_and_breakeven_move():
    mgr = LifecycleManager()
    p = _pos()
    result = mgr.evaluate_active_trade(
        p,
        current_price=101.0,
        current_atr=0.5,
        current_volume_1m=10_000,
        avg_volume_1m=10_000,
        minutes_since_entry=1,
    )
    assert result["action"] == "PARTIAL_EXIT"
    assert result["shares_to_sell"] == 25
    assert result["new_stop"] == 100.0
    assert p["partial_1_taken"] is True
    assert p["stop_loss"] == 100.0


def test_partial2_sells_half_of_remaining():
    mgr = LifecycleManager()
    p = _pos(partial_1_taken=True, shares_already_sold=25, stop_loss=100.0)
    result = mgr.evaluate_active_trade(
        p,
        current_price=101.5,
        current_atr=0.5,
        current_volume_1m=10_000,
        avg_volume_1m=10_000,
        minutes_since_entry=2,
    )
    assert result["action"] == "PARTIAL_EXIT"
    assert result["shares_to_sell"] == 37
    assert p["partial_2_taken"] is True


def test_trailing_adjusts_stop_up_only():
    mgr = LifecycleManager()
    p = _pos(partial_1_taken=True, partial_2_taken=True, stop_loss=100.5)
    result = mgr.evaluate_active_trade(
        p,
        current_price=102.5,
        current_atr=0.5,
        current_volume_1m=10_000,
        avg_volume_1m=10_000,
        minutes_since_entry=3,
    )
    assert result["action"] in {"ADJUST_STOP", "HOLD"}


def test_time_continuation_exit_on_no_progress():
    mgr = LifecycleManager()
    p = _pos()
    result = mgr.evaluate_active_trade(
        p,
        current_price=99.9,
        current_atr=0.5,
        current_volume_1m=10_000,
        avg_volume_1m=10_000,
        minutes_since_entry=6,
    )
    assert result["action"] == "EXIT_ALL"
    assert "Time continuation" in result["reason"]


def test_volume_fade_exit():
    mgr = LifecycleManager()
    p = _pos()
    result = mgr.evaluate_active_trade(
        p,
        current_price=100.3,
        current_atr=0.5,
        current_volume_1m=100,
        avg_volume_1m=1000,
        minutes_since_entry=1,
    )
    assert result["action"] == "EXIT_ALL"
    assert "Volume fade" in result["reason"]
