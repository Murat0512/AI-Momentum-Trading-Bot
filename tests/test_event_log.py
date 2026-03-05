"""
tests/test_event_log.py — Unit tests for trade_log/event_log.py
"""
import json
import os
import tempfile
from datetime import datetime
import pytz
import pytest

ET = pytz.timezone("America/New_York")

from trade_log.event_log import EventLog
from config.constants import (
    EVT_ORDER_SUBMITTED, EVT_ORDER_FILLED, EVT_POSITION_OPENED,
    EVT_INTEGRITY_EVENT, EVT_PREFLIGHT_PASS, EVT_PREFLIGHT_FAIL, EVT_ARMING,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

@pytest.fixture
def tmplog(tmp_path):
    """EventLog writing to a temp directory."""
    el = EventLog(log_dir=str(tmp_path))
    el.new_day(date_str="2099-01-01", run_id="test-run")
    yield el, tmp_path
    el.close()


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestLogWrite:
    def test_log_creates_jsonl_file(self, tmplog):
        el, tmp = tmplog
        el.log("TEST_EVENT", ticker="AAPL", payload={"x": 1})
        el.close()
        files = list(tmp.glob("events_*.jsonl"))
        assert len(files) == 1

    def test_logged_event_is_valid_json(self, tmplog):
        el, tmp = tmplog
        el.log(EVT_ORDER_SUBMITTED, ticker="AAPL", payload={"qty": 100})
        el.close()
        lines = (tmp / "events_2099-01-01.jsonl").read_text().strip().splitlines()
        assert len(lines) == 1
        rec = json.loads(lines[0])
        assert rec["event_type"] == EVT_ORDER_SUBMITTED

    def test_run_id_attached(self, tmplog):
        el, tmp = tmplog
        el.log(EVT_ORDER_FILLED, ticker="TSLA")
        el.close()
        lines = (tmp / "events_2099-01-01.jsonl").read_text().strip().splitlines()
        rec = json.loads(lines[0])
        assert rec["run_id"] == "test-run"

    def test_event_id_is_unique(self, tmplog):
        el, tmp = tmplog
        for _ in range(5):
            el.log("REPEATED", ticker="AAPL")
        el.close()
        records = [
            json.loads(l)
            for l in (tmp / "events_2099-01-01.jsonl").read_text().splitlines()
            if l.strip()
        ]
        ids = [r["event_id"] for r in records]
        assert len(set(ids)) == 5

    def test_multiple_events_appended(self, tmplog):
        el, tmp = tmplog
        for evt in [EVT_ORDER_SUBMITTED, EVT_ORDER_FILLED, EVT_INTEGRITY_EVENT]:
            el.log(evt)
        el.close()
        records = (tmp / "events_2099-01-01.jsonl").read_text().strip().splitlines()
        assert len(records) == 3


class TestReplay:
    def test_replay_returns_events_in_order(self, tmplog):
        el, tmp = tmplog
        events = [EVT_ORDER_SUBMITTED, EVT_ORDER_FILLED, EVT_POSITION_OPENED]
        for evt in events:
            el.log(evt)
        el.close()
        replayed = list(EventLog.replay("2099-01-01", log_dir=str(tmp)))
        assert [r["event_type"] for r in replayed] == events

    def test_replay_nonexistent_date_returns_nothing(self, tmp_path):
        result = list(EventLog.replay("1900-01-01", log_dir=str(tmp_path)))
        assert result == []

    def test_replay_skips_corrupt_lines(self, tmp_path):
        p = tmp_path / "events_2099-02-02.jsonl"
        p.write_text('{"event_type":"OK"}\n{CORRUPT_JSON}\n{"event_type":"OK2"}\n')
        result = list(EventLog.replay("2099-02-02", log_dir=str(tmp_path)))
        assert len(result) == 2  # corrupt line skipped


class TestNewDay:
    def test_new_day_creates_new_file(self, tmp_path):
        el = EventLog(log_dir=str(tmp_path))
        el.new_day(date_str="2099-01-01")
        el.log("A")
        el.new_day(date_str="2099-01-02")
        el.log("B")
        el.close()
        dates = EventLog.list_dates(log_dir=str(tmp_path))
        assert "2099-01-01" in dates
        assert "2099-01-02" in dates

    def test_run_id_changes_on_new_day(self, tmp_path):
        el = EventLog(log_dir=str(tmp_path))
        el.new_day(date_str="2099-01-01", run_id="run-A")
        el.log("X")
        el.new_day(date_str="2099-01-02", run_id="run-B")
        el.log("Y")
        el.close()
        day2_lines = (tmp_path / "events_2099-01-02.jsonl").read_text().strip().splitlines()
        rec = json.loads(day2_lines[0])
        assert rec["run_id"] == "run-B"


class TestConvenienceWrappers:
    def test_log_order_submitted_wrapper(self, tmplog):
        el, tmp = tmplog
        el.log_order_submitted("AAPL", "buy", 100, 150.0, order_id="O1")
        el.close()
        rec = json.loads((tmp / "events_2099-01-01.jsonl").read_text().strip())
        assert rec["event_type"] == EVT_ORDER_SUBMITTED
        assert rec["payload"]["order_id"] == "O1"

    def test_log_preflight_pass(self, tmplog):
        el, tmp = tmplog
        el.log_preflight(True, {"broker_ping": True})
        el.close()
        rec = json.loads((tmp / "events_2099-01-01.jsonl").read_text().strip())
        assert rec["event_type"] == EVT_PREFLIGHT_PASS

    def test_log_preflight_fail(self, tmplog):
        el, tmp = tmplog
        el.log_preflight(False, {"api_keys_present": False})
        el.close()
        rec = json.loads((tmp / "events_2099-01-01.jsonl").read_text().strip())
        assert rec["event_type"] == EVT_PREFLIGHT_FAIL

    def test_log_arming(self, tmplog):
        el, tmp = tmplog
        el.log_arming("live", True, "preflight passed")
        el.close()
        rec = json.loads((tmp / "events_2099-01-01.jsonl").read_text().strip())
        assert rec["event_type"] == EVT_ARMING
        assert rec["payload"]["granted"] is True
