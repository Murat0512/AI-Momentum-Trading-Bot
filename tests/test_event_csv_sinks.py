from __future__ import annotations

from events.sinks.csv_orders_sink import CsvOrdersSink
from events.sinks.csv_trades_sink import CsvTradesSink
from events.types import LegacyAuditEvent, LifecycleTransition, OrderFilled, OrderSubmitted


def test_csv_orders_sink_writes_header_and_appends(tmp_path):
    sink = CsvOrdersSink(logs_dir=tmp_path)
    sink(OrderSubmitted(cycle_id=1, order_id="O-1", ticker="AAPL", qty=10, side="buy"))
    sink.close()

    sink2 = CsvOrdersSink(logs_dir=tmp_path)
    sink2(OrderFilled(cycle_id=1, order_id="O-1", ticker="AAPL", filled_qty=10, filled_price=10.5, side="buy"))
    sink2.close()

    files = sorted(tmp_path.glob("orders_*.csv"))
    assert files
    lines = files[-1].read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 3  # header + 2 rows


def test_csv_orders_sink_handles_weird_legacy_payload(tmp_path):
    sink = CsvOrdersSink(logs_dir=tmp_path)
    sink(
        LegacyAuditEvent(
            event_type="ORDER_REJECTED",
            ticker="MSFT",
            payload="unexpected-string",  # not a dict
        )
    )
    sink.close()

    files = sorted(tmp_path.glob("orders_*.csv"))
    assert files
    lines = files[-1].read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2


def test_csv_trades_sink_writes_lifecycle_closed_and_legacy_close(tmp_path):
    sink = CsvTradesSink(logs_dir=tmp_path)
    sink(
        LifecycleTransition(
            cycle_id=2,
            trade_id="T-2",
            ticker="NVDA",
            from_state="OPEN",
            to_state="CLOSED",
            shares_sold=20,
            pnl=1.5,
            reason="target",
        )
    )
    sink(
        LegacyAuditEvent(
            cycle_id=2,
            event_type="POSITION_CLOSED",
            ticker="NVDA",
            payload={"trade_id": "T-3", "pnl_r": -1.0},
        )
    )
    sink.close()

    files = sorted(tmp_path.glob("trades_*.csv"))
    assert files
    lines = files[-1].read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 3


def test_csv_trades_sink_ignores_non_closed_lifecycle(tmp_path):
    sink = CsvTradesSink(logs_dir=tmp_path)
    sink(
        LifecycleTransition(
            cycle_id=4,
            trade_id="T-4",
            ticker="TSLA",
            from_state="OPEN",
            to_state="TRAILING",
            reason="trail-start",
        )
    )
    sink.close()

    files = sorted(tmp_path.glob("trades_*.csv"))
    assert not files
