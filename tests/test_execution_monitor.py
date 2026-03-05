from __future__ import annotations

from datetime import datetime, timedelta

import pytest
import pytz

from execution.monitor import ExecutionMonitor

ET = pytz.timezone("America/New_York")


def test_calculate_entry_efficiency_pct_of_atr():
    monitor = ExecutionMonitor()
    slippage_abs, slippage_to_atr_pct = monitor.calculate_entry_efficiency(
        signal_price=10.00,
        fill_price=10.05,
        atr_1m=0.50,
    )

    assert slippage_abs == pytest.approx(0.05)
    assert slippage_to_atr_pct == pytest.approx(10.0)


def test_sloppy_fill_logs_critical(caplog: pytest.LogCaptureFixture):
    monitor = ExecutionMonitor()

    with caplog.at_level("CRITICAL"):
        _, slippage_to_atr_pct = monitor.calculate_entry_efficiency(
            signal_price=10.00,
            fill_price=10.20,
            atr_1m=0.50,
        )

    assert slippage_to_atr_pct > 25.0
    assert any("SLOPPY_FILL" in rec.message for rec in caplog.records)


def test_log_execution_latency_ms():
    monitor = ExecutionMonitor()
    signal_ts = ET.localize(datetime(2026, 3, 4, 10, 1, 0, 0))
    fill_ts = signal_ts + timedelta(milliseconds=275)

    latency_ms = monitor.log_execution_latency(signal_ts, fill_ts)
    assert latency_ms == 275


def test_record_execution_context_snapshot():
    monitor = ExecutionMonitor()
    ctx = monitor.record_execution_context(
        ticker="ASNS",
        sqs_score=0.82,
        rvol=3.1,
        spread_at_fill=0.0032,
    )

    assert ctx["ticker"] == "ASNS"
    assert ctx["sqs_score"] == pytest.approx(0.82)
    assert ctx["rvol"] == pytest.approx(3.1)
    assert ctx["spread_at_fill"] == pytest.approx(0.0032)


def test_on_entry_filled_flags_ticker_exclusion():
    monitor = ExecutionMonitor()
    signal_ts = ET.localize(datetime(2026, 3, 4, 10, 1, 0, 0))
    fill_ts = signal_ts + timedelta(milliseconds=250)

    metrics = monitor.on_entry_filled(
        order_id="ORD-1",
        ticker="ASNS",
        signal_price=10.0,
        fill_price=10.2,
        atr_1m=0.5,
        signal_timestamp=signal_ts,
        fill_timestamp=fill_ts,
        sqs_score=0.8,
        rvol=3.0,
        spread_at_fill=0.0,
        bid_at_signal=9.99,
        ask_at_signal=10.01,
    )

    assert metrics.excluded_after_fill is True
    assert monitor.is_excluded("ASNS") is True
