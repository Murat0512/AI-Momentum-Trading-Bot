"""
tests/test_event_replay_completeness.py — Verify every event wrapper in
    EventLog writes a well-formed record that can be read back via replay.

The strategy: write all events to a temp JSONL file, replay it, index
by event_type, then assert each category is present and its payload
contains the expected keys.

Layers covered
--------------
  Discovery
    1.  UNIVERSE_SNAPSHOT
    2.  PROMOTION
    3.  DEMOTION
    4.  CANDIDATE_EXPIRED  (new)

  Decision
    5.  DECISION_EVALUATED (accept)
    6.  DECISION_EVALUATED (reject)

  Order lifecycle
    7.  ORDER_SUBMITTED
    8.  ORDER_ACKNOWLEDGED  (new)
    9.  ORDER_FILLED
    10. ORDER_PARTIAL_FILL  (new)
    11. ORDER_CANCELLED
    12. ORDER_CANCEL_REPLACE (new)
    13. ORDER_REJECTED       (new)
    14. ORDER_STUCK

  Position lifecycle
    15. POSITION_OPENED
    16. POSITION_PARTIAL_EXIT (new)
    17. STOP_ADJUSTED
    18. LIFECYCLE_EVENT       (new)
    19. POSITION_CLOSED

  Safety
    20. GATE_EVENT
    21. GOVERNOR_EVENT
    22. INTEGRITY_EVENT
    23. REGIME_CHANGE   (new)
    24. HALT_TRANSITION (new)
    25. RTH_BLOCK

  Session
    26. SESSION_START
    27. SESSION_END
    28. RESTART_RECOVERY
    29. PREFLIGHT_PASS
    30. ARMING_EVENT

  Structural record invariants
    31. every record has: event_id, event_type, ts, run_id, ticker, payload
    32. event_id is 12-char hex
    33. ts is ISO-8601
    34. payload is always a dict
"""

from __future__ import annotations

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch
import pytest
import pytz

from trade_log.event_log import EventLog

ET = pytz.timezone("America/New_York")

# ─────────────────────────────────────────────────────────────────────────────
# Fixture: isolated EventLog writing to a temp dir
# ─────────────────────────────────────────────────────────────────────────────

DATE_STR = "2099-01-15"   # far future — won't collide with real log files


@pytest.fixture()
def tmp_log(tmp_path: Path):
    """Return a fresh EventLog pointing at a tmp directory."""
    el = EventLog(log_dir=str(tmp_path))
    el.new_day(date_str=DATE_STR, run_id="test-run")
    yield el
    el.close()


def _replay(el: EventLog) -> List[Dict[str, Any]]:
    """Flush and replay all records written so far."""
    el.close()
    records = list(EventLog.replay(DATE_STR, log_dir=str(el._log_dir)))
    # Re-open for further writes if needed
    el.new_day(date_str=DATE_STR, run_id="test-run")
    return records


def _index(records: List[Dict]) -> Dict[str, List[Dict]]:
    idx: Dict[str, List[Dict]] = {}
    for r in records:
        idx.setdefault(r["event_type"], []).append(r)
    return idx


# ─────────────────────────────────────────────────────────────────────────────
# Helpers: fake trade / candidate objects
# ─────────────────────────────────────────────────────────────────────────────

def _fake_trade(ticker: str = "TSLA", trade_id: str = "tid001"):
    t = MagicMock()
    t.ticker          = ticker
    t.trade_id        = trade_id
    t.entry_price     = 200.0
    t.shares          = 100
    t.stop_price      = 195.0
    t.target_price    = 210.0
    t.entry_time      = datetime.now(ET)
    t.exit_price      = 205.0
    t.exit_time       = datetime.now(ET)
    t.pnl             = 500.0
    t.pnl_r           = 1.0
    t.r_multiple      = 1.0
    t.lifecycle_state = "trail_active"
    t.regime          = "TREND"
    t.setup_name      = "gap_and_go"
    t.exit_reason     = "stop_hit"
    return t


def _fake_candidate(ticker: str = "TSLA", score: float = 72.5):
    c = MagicMock()
    c.ticker        = ticker
    c.demand_score  = score
    c.gap_pct       = 0.05
    c.rvol           = 2.1
    c.float_shares   = 5_000_000
    c.last_price    = 200.0
    c.source        = "seed"
    return c


# ─────────────────────────────────────────────────────────────────────────────
# Layer 1 — Discovery
# ─────────────────────────────────────────────────────────────────────────────

class TestDiscoveryEvents:
    def test_universe_snapshot_written_and_replayed(self, tmp_log: EventLog):
        candidates = [_fake_candidate("TSLA", 72.5), _fake_candidate("AAPL", 68.0)]
        tmp_log.log_universe_snapshot(candidates, cycle_seq=1)
        idx = _index(_replay(tmp_log))
        assert "UNIVERSE_SNAPSHOT" in idx
        payload = idx["UNIVERSE_SNAPSHOT"][0]["payload"]
        assert "cycle_seq" in payload

    def test_promotion_written_and_replayed(self, tmp_log: EventLog):
        tmp_log.log_promotion("TSLA", source="seed", reason="above_threshold", score=72.5)
        idx = _index(_replay(tmp_log))
        assert "PROMOTION" in idx
        p = idx["PROMOTION"][0]["payload"]
        assert p["score"] == pytest.approx(72.5)

    def test_demotion_written_and_replayed(self, tmp_log: EventLog):
        tmp_log.log_demotion("TSLA", reason="score_dropped", last_score=40.0)
        idx = _index(_replay(tmp_log))
        assert "DEMOTION" in idx

    def test_candidate_expired_written_and_replayed(self, tmp_log: EventLog):
        tmp_log.log_candidate_expired("TSLA", score=55.0, source="seed", age_minutes=31.0)
        idx = _index(_replay(tmp_log))
        assert "CANDIDATE_EXPIRED" in idx
        p = idx["CANDIDATE_EXPIRED"][0]["payload"]
        assert p["age_minutes"] == pytest.approx(31.0)
        assert p["source"] == "seed"


# ─────────────────────────────────────────────────────────────────────────────
# Layer 2 — Decision
# ─────────────────────────────────────────────────────────────────────────────

class TestDecisionEvents:
    def test_decision_evaluated_accept(self, tmp_log: EventLog):
        tmp_log.log_decision_evaluation(
            ticker="TSLA", accepted=True, reason="all_gates_passed",
            demand_score=72.5, gate_results={"spread": True, "integrity": True},
        )
        idx = _index(_replay(tmp_log))
        assert "DECISION_EVALUATED" in idx
        assert idx["DECISION_EVALUATED"][0]["payload"]["accepted"] is True

    def test_decision_evaluated_reject(self, tmp_log: EventLog):
        tmp_log.log_decision_evaluation(
            ticker="TSLA", accepted=False, reason="spread_too_wide",
            demand_score=65.0, gate_results={"spread": False},
        )
        idx = _index(_replay(tmp_log))
        assert "DECISION_EVALUATED" in idx
        assert idx["DECISION_EVALUATED"][0]["payload"]["accepted"] is False


# ─────────────────────────────────────────────────────────────────────────────
# Layer 3 — Order lifecycle
# ─────────────────────────────────────────────────────────────────────────────

class TestOrderLifecycleEvents:
    def _write_all(self, el: EventLog) -> None:
        el.log_order_submitted("TSLA", "BUY", 100, 200.0, order_id="oid001")
        el.log_order_acknowledged("TSLA", "oid001", "new")
        el.log_order_filled("TSLA", "BUY", 100, 200.5, order_id="oid001")
        el.log_order_partial_fill("TSLA", "oid001", 50, 200.5, 50)
        el.log_order_cancelled("TSLA", "oid001", reason="cancelled")
        el.log_order_cancel_replace("TSLA", "oid001", "oid002", 199.5, "ttl_expired")
        el.log_order_rejected("TSLA", "oid001", "insufficient_funds")
        el.log_order_stuck("TSLA", "oid001", age_seconds=30)

    def test_all_order_event_types_present(self, tmp_log: EventLog):
        self._write_all(tmp_log)
        idx = _index(_replay(tmp_log))
        expected = {
            "ORDER_SUBMITTED",
            "ORDER_ACKNOWLEDGED",
            "ORDER_FILLED",
            "ORDER_PARTIAL_FILL",
            "ORDER_CANCELLED",
            "ORDER_CANCEL_REPLACE",
            "ORDER_REJECTED",
            "ORDER_STUCK",
        }
        for evt in expected:
            assert evt in idx, f"Missing event type: {evt}"

    def test_order_acknowledged_payload(self, tmp_log: EventLog):
        tmp_log.log_order_acknowledged("TSLA", "oid001", status="new")
        idx = _index(_replay(tmp_log))
        p = idx["ORDER_ACKNOWLEDGED"][0]["payload"]
        assert p["order_id"] == "oid001"
        assert p["status"]   == "new"

    def test_order_partial_fill_payload(self, tmp_log: EventLog):
        tmp_log.log_order_partial_fill("TSLA", "oid001", filled_qty=50, fill_price=200.5, remaining_qty=50)
        idx = _index(_replay(tmp_log))
        p = idx["ORDER_PARTIAL_FILL"][0]["payload"]
        assert p["filled_qty"]    == 50
        assert p["remaining_qty"] == 50
        assert p["fill_price"]    == pytest.approx(200.5)

    def test_order_cancel_replace_payload(self, tmp_log: EventLog):
        tmp_log.log_order_cancel_replace("TSLA", "oid001", "oid002", 199.5, reason="ttl_expired")
        idx = _index(_replay(tmp_log))
        p = idx["ORDER_CANCEL_REPLACE"][0]["payload"]
        assert p["old_order_id"] == "oid001"
        assert p["new_order_id"] == "oid002"
        assert p["new_limit"]    == pytest.approx(199.5)
        assert p["reason"]       == "ttl_expired"

    def test_order_rejected_payload(self, tmp_log: EventLog):
        tmp_log.log_order_rejected("TSLA", "oid001", reason="margin_call")
        idx = _index(_replay(tmp_log))
        p = idx["ORDER_REJECTED"][0]["payload"]
        assert p["reason"] == "margin_call"


# ─────────────────────────────────────────────────────────────────────────────
# Layer 4 — Position lifecycle
# ─────────────────────────────────────────────────────────────────────────────

class TestPositionLifecycleEvents:
    def test_position_opened_written(self, tmp_log: EventLog):
        tmp_log.log_position_opened(_fake_trade())
        idx = _index(_replay(tmp_log))
        assert "POSITION_OPENED" in idx

    def test_position_partial_exit_payload(self, tmp_log: EventLog):
        tmp_log.log_position_partial_exit(
            "TSLA", "tid001",
            shares_sold=50, price=205.0, pnl=250.0,
            remaining_shares=50, lifecycle_state="breakeven_trail",
        )
        idx = _index(_replay(tmp_log))
        assert "POSITION_PARTIAL_EXIT" in idx
        p = idx["POSITION_PARTIAL_EXIT"][0]["payload"]
        assert p["shares_sold"]      == 50
        assert p["remaining_shares"] == 50
        assert p["pnl"]              == pytest.approx(250.0)
        assert p["lifecycle_state"]  == "breakeven_trail"

    def test_stop_adjusted_written(self, tmp_log: EventLog):
        tmp_log.log_stop_adjusted("TSLA", "tid001", 196.0, 198.0, "breakeven")
        idx = _index(_replay(tmp_log))
        assert "STOP_ADJUSTED" in idx

    def test_lifecycle_event_payload(self, tmp_log: EventLog):
        from config.constants import LIFECYCLE_EVT_BREAKEVEN
        tmp_log.log_lifecycle_event(
            "TSLA", "tid001",
            lifecycle_evt=LIFECYCLE_EVT_BREAKEVEN,
            payload={"new_stop": 200.0},
        )
        idx = _index(_replay(tmp_log))
        assert "LIFECYCLE_EVENT" in idx
        p = idx["LIFECYCLE_EVENT"][0]["payload"]
        assert p["lifecycle_evt"] == LIFECYCLE_EVT_BREAKEVEN
        assert p["new_stop"] == pytest.approx(200.0)

    def test_position_closed_written(self, tmp_log: EventLog):
        tmp_log.log_position_closed(_fake_trade())
        idx = _index(_replay(tmp_log))
        assert "POSITION_CLOSED" in idx


# ─────────────────────────────────────────────────────────────────────────────
# Layer 5 — Safety
# ─────────────────────────────────────────────────────────────────────────────

class TestSafetyEvents:
    def test_gate_event_written(self, tmp_log: EventLog):
        tmp_log.log_gate_event("spread_gate", passed=False, reason="spread_22bp")
        idx = _index(_replay(tmp_log))
        assert "GATE_EVENT" in idx

    def test_governor_event_written(self, tmp_log: EventLog):
        tmp_log.log_governor_event(blocked=True, reason="max_daily_loss_hit")
        idx = _index(_replay(tmp_log))
        assert "GOVERNOR_EVENT" in idx

    def test_integrity_event_written(self, tmp_log: EventLog):
        tmp_log.log_integrity_event(is_halted=True, reason="spread_too_wide")
        idx = _index(_replay(tmp_log))
        assert "INTEGRITY_EVENT" in idx

    def test_regime_change_payload(self, tmp_log: EventLog):
        tmp_log.log_regime_change("TREND", "CHOP", adx=18.5, spy_range_pct=0.004)
        idx = _index(_replay(tmp_log))
        assert "REGIME_CHANGE" in idx
        p = idx["REGIME_CHANGE"][0]["payload"]
        assert p["old_regime"] == "TREND"
        assert p["new_regime"] == "CHOP"
        assert p["adx"]        == pytest.approx(18.5)

    def test_halt_transition_payload(self, tmp_log: EventLog):
        tmp_log.log_halt_transition("TSLA", "ACTIVE", "HALTED", reason="zero_volume_pin")
        idx = _index(_replay(tmp_log))
        assert "HALT_TRANSITION" in idx
        p = idx["HALT_TRANSITION"][0]["payload"]
        assert p["from_state"] == "ACTIVE"
        assert p["to_state"]   == "HALTED"
        assert p["reason"]     == "zero_volume_pin"
        assert idx["HALT_TRANSITION"][0]["ticker"] == "TSLA"

    def test_rth_block_written(self, tmp_log: EventLog):
        tmp_log.log_rth_block("TSLA", "outside_rth")
        idx = _index(_replay(tmp_log))
        assert "RTH_BLOCK" in idx


# ─────────────────────────────────────────────────────────────────────────────
# Layer 6 — Session
# ─────────────────────────────────────────────────────────────────────────────

class TestSessionEvents:
    def test_session_start_written(self, tmp_log: EventLog):
        tmp_log.log_session_start("2099-01-15")
        idx = _index(_replay(tmp_log))
        assert "SESSION_START" in idx

    def test_session_end_written(self, tmp_log: EventLog):
        tmp_log.log_session_end("2099-01-15", daily_pnl=1200.0, total_trades=5)
        idx = _index(_replay(tmp_log))
        assert "SESSION_END" in idx

    def test_restart_recovery_written(self, tmp_log: EventLog):
        tmp_log.log_restart(
            reconstructed_positions={}, open_orders=[],
            drift_detected=False, note="after_crash",
        )
        idx = _index(_replay(tmp_log))
        assert "RESTART_RECOVERY" in idx

    def test_preflight_written(self, tmp_log: EventLog):
        tmp_log.log_preflight(passed=True, results={"broker": True, "data": True})
        idx = _index(_replay(tmp_log))
        # Can be PREFLIGHT_PASS or PREFLIGHT_FAIL — just ensure something is there
        matches = [k for k in idx if k.startswith("PREFLIGHT")]
        assert len(matches) >= 1

    def test_arming_event_written(self, tmp_log: EventLog):
        tmp_log.log_arming(mode="live", granted=True, reason="all_checks_passed")
        idx = _index(_replay(tmp_log))
        assert "ARMING_EVENT" in idx


# ─────────────────────────────────────────────────────────────────────────────
# Record structure invariants
# ─────────────────────────────────────────────────────────────────────────────

class TestRecordStructureInvariants:
    def test_every_record_has_required_fields(self, tmp_log: EventLog):
        tmp_log.log_order_submitted("TSLA", "BUY", 100, 200.0, order_id="oid001")
        tmp_log.log_regime_change("TREND", "CHOP")
        tmp_log.log_candidate_expired("TSLA", 55.0)
        records = _replay(tmp_log)
        required = {"event_id", "event_type", "ts", "run_id", "ticker", "payload"}
        for r in records:
            missing = required - r.keys()
            assert not missing, f"Record missing fields {missing}: {r['event_type']}"

    def test_event_id_is_12_char_hex(self, tmp_log: EventLog):
        tmp_log.log_order_submitted("TSLA", "BUY", 100, 200.0, order_id="oid001")
        records = _replay(tmp_log)
        for r in records:
            eid = r["event_id"]
            assert len(eid) == 12, f"event_id length {len(eid)} for {r['event_type']}"
            assert all(c in "0123456789abcdef" for c in eid)

    def test_ts_is_iso_format(self, tmp_log: EventLog):
        tmp_log.log_order_submitted("TSLA", "BUY", 100, 200.0, order_id="oid001")
        records = _replay(tmp_log)
        for r in records:
            ts = r["ts"]
            assert "T" in ts, f"ts not ISO-8601: {ts}"
            # Should parse without error
            datetime.fromisoformat(ts)

    def test_payload_is_always_dict(self, tmp_log: EventLog):
        tmp_log.log_order_submitted("TSLA", "BUY", 100, 200.0, order_id="oid001")
        tmp_log.log_session_start("2099-01-15")
        tmp_log.log_halt_transition("TSLA", "ACTIVE", "HALTED")
        records = _replay(tmp_log)
        for r in records:
            assert isinstance(r["payload"], dict), \
                f"payload is not dict for {r['event_type']}: {type(r['payload'])}"

    def test_run_id_persists_across_records(self, tmp_log: EventLog):
        tmp_log.log_order_submitted("TSLA", "BUY", 100, 200.0, order_id="oid001")
        tmp_log.log_session_start("2099-01-15")
        records = _replay(tmp_log)
        run_ids = {r["run_id"] for r in records}
        assert run_ids == {"test-run"}
