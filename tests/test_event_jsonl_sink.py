"""
tests/test_event_jsonl_sink.py — Unit tests for events.sinks.jsonl_sink.
"""

import json
import tempfile
from datetime import datetime
from pathlib import Path

import pytest
import pytz

from events.sinks.jsonl_sink import JsonlSink
from events.types import OrderFilled, OrderSubmitted, SlippageRecorded

ET = pytz.timezone("America/New_York")


@pytest.fixture
def tmp_logs(tmp_path):
    return tmp_path


@pytest.fixture
def sink(tmp_logs):
    s = JsonlSink(logs_dir=tmp_logs)
    yield s
    s.close()


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────


class TestJsonlSink:
    def test_writes_valid_json_per_line(self, sink, tmp_logs):
        sink(OrderFilled(cycle_id=1, order_id="ORD1", ticker="NVDA", filled_qty=100))
        sink(OrderSubmitted(cycle_id=1, order_id="ORD1", ticker="NVDA"))
        sink.close()

        files = list(tmp_logs.glob("events_*.jsonl"))
        assert len(files) == 1

        lines = files[0].read_text().strip().splitlines()
        assert len(lines) == 2
        for line in lines:
            data = json.loads(line)  # must not raise
            assert isinstance(data, dict)

    def test_type_field_present(self, sink, tmp_logs):
        sink(OrderFilled(ticker="AMD"))
        sink.close()

        files = list(tmp_logs.glob("events_*.jsonl"))
        data = json.loads(files[0].read_text().strip())
        assert "_type" in data
        assert data["_type"] == "OrderFilled"

    def test_datetime_serialised_as_iso(self, sink, tmp_logs):
        evt = OrderFilled(
            cycle_id=5,
            ticker="TSLA",
            ts=datetime(2024, 1, 15, 10, 30, 0, tzinfo=ET),
        )
        sink(evt)
        sink.close()

        files = list(tmp_logs.glob("events_*.jsonl"))
        data = json.loads(files[0].read_text().strip())
        ts_str = data["ts"]
        assert isinstance(ts_str, str)
        # ISO format check — must contain date digits and separator
        assert "2024-01-15" in ts_str

    def test_multiple_events_each_on_own_line(self, sink, tmp_logs):
        for i in range(5):
            sink(OrderFilled(cycle_id=i, ticker="SPY"))
        sink.close()

        files = list(tmp_logs.glob("events_*.jsonl"))
        lines = files[0].read_text().strip().splitlines()
        assert len(lines) == 5

    def test_sink_does_not_raise_on_close_twice(self, sink):
        sink(OrderFilled(ticker="AAPL"))
        sink.close()
        sink.close()  # second close must be a no-op

    def test_file_created_in_logs_dir(self, sink, tmp_logs):
        sink(SlippageRecorded(ticker="MARA", action="SLIPPAGE_OK"))
        sink.close()
        assert any(tmp_logs.iterdir())
