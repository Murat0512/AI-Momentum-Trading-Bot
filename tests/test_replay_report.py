from __future__ import annotations

import json

from tools.replay_report import build_report_rows, summarize_day


def test_replay_report_groups_and_percentiles(tmp_path):
    date_str = "2026-03-04"
    log_path = tmp_path / f"events_{date_str}.jsonl"

    events = [
        {
            "_type": "MomentumMetricsComputed",
            "ticker": "AAPL",
            "dollar_flow_z": 1.1,
            "pressure_z": 0.6,
            "cycle_id": 1,
        },
        {
            "_type": "LegacyAuditEvent",
            "event_type": "DECISION_EVALUATED",
            "ticker": "AAPL",
            "payload": {"demand_score": 0.8, "sqs": 0.7},
            "cycle_id": 1,
        },
        {
            "_type": "LegacyAuditEvent",
            "event_type": "ORDER_SUBMITTED",
            "ticker": "AAPL",
            "payload": {},
            "cycle_id": 1,
        },
        {
            "_type": "LegacyAuditEvent",
            "event_type": "ORDER_FILLED",
            "ticker": "AAPL",
            "payload": {},
            "cycle_id": 1,
        },
        {
            "_type": "LegacyAuditEvent",
            "event_type": "POSITION_CLOSED",
            "ticker": "AAPL",
            "payload": {"pnl_r": 1.5},
            "cycle_id": 5,
        },
        {
            "_type": "MomentumMetricsComputed",
            "ticker": "MSFT",
            "dollar_flow_z": -0.5,
            "pressure_z": -0.2,
            "cycle_id": 2,
        },
        {
            "_type": "LegacyAuditEvent",
            "event_type": "DECISION_EVALUATED",
            "ticker": "MSFT",
            "payload": {"demand_score": 0.2, "sqs": 0.3},
            "cycle_id": 2,
        },
        {
            "_type": "LegacyAuditEvent",
            "event_type": "ORDER_REJECTED",
            "ticker": "MSFT",
            "payload": {},
            "cycle_id": 2,
        },
        {
            "_type": "LegacyAuditEvent",
            "event_type": "POSITION_CLOSED",
            "ticker": "MSFT",
            "payload": {"pnl_r": -0.8},
            "cycle_id": 7,
        },
    ]

    with open(log_path, "w", encoding="utf-8") as fh:
        for e in events:
            fh.write(json.dumps(e) + "\n")

    summary = summarize_day(date_str, log_dir=str(tmp_path))
    rows = build_report_rows(summary)

    row_lookup = {(r["group"], r["metric"]): r for r in rows}

    assert row_lookup[("submitted", "demand_score")]["n"] == 1
    assert row_lookup[("submitted", "demand_score")]["mean"] == 0.8
    assert row_lookup[("filled", "sqs")]["mean"] == 0.7
    assert row_lookup[("rejected", "dollar_flow_z")]["mean"] == -0.5
    assert row_lookup[("winner", "pressure_z")]["mean"] == 0.6
    assert row_lookup[("loser", "pressure_z")]["mean"] == -0.2
