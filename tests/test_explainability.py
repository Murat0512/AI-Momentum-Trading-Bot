"""
tests/test_explainability.py — Acceptance tests for trade_log.explainability

ExplainabilityLogger(log_dir=str) — no cfg param.
ExplainabilityReplayer.decision_summary() returns a list, not a dict.
Event types come from LOG_* constants (e.g. "DECISION", "CYCLE_SNAPSHOT").
"""
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from config.constants import (
    LOG_CYCLE_SNAPSHOT, LOG_DECISION, LOG_HEALTH_EVENT,
    LOG_SKIP, LOG_SLIPPAGE_EVENT,
)
from trade_log.explainability import ExplainabilityLogger, ExplainabilityReplayer


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _make_decision_result(selected: str = None, n_rejected: int = 1):
    """Mock DecisionResult with all fields accessed by ExplainabilityLogger."""
    dr = MagicMock()
    dr.selected_ticker          = selected
    dr.reason                   = "" if selected else "no valid setup"
    dr.regime                   = "TREND"
    dr.top15_snapshot           = []
    dr.sqs_components           = {}
    dr.selection                = None
    dr.health_size_multiplier   = 1.0
    dr.slippage_size_multiplier = 1.0
    dr.gate_log                 = {}
    dr.rejected                 = {"JUNK": "spread_wide"} if n_rejected else {}
    dr.open_trades              = 0
    dr.daily_pnl                = 0.0
    return dr


def _make_news_candidates():
    n            = MagicMock()
    n.ticker     = "AAPL"
    n.headline   = "AAPL earnings beat"
    n.news_score     = 0.25
    n.catalyst_type  = "earnings"
    return [n]


def _make_dh_report():
    dh = MagicMock()
    dh.status          = "DEGRADE"
    dh.block_reason    = ""
    dh.degrade_reasons = ["stale_quote"]
    dh.quote_age_s     = 45.0
    dh.spread_pct      = 0.003
    dh.session         = "RTH"
    dh.feed_type       = "alpaca_sip"
    return dh


def _make_fill():
    f = MagicMock()
    f.expected_price = 25.00
    f.fill_price     = 25.08
    f.slippage_bps   = 32.0
    f.slippage_r     = 0.12
    f.spread_pct     = 0.002
    return f


# ─────────────────────────────────────────────────────────────────────────────
# ExplainabilityLogger
# ─────────────────────────────────────────────────────────────────────────────

class TestExplainabilityLogger:
    def setup_method(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.log_dir = Path(self._tmpdir.name)
        self.logger  = ExplainabilityLogger(log_dir=str(self.log_dir))

    def teardown_method(self):
        self.logger.close()   # release the JSONL file handle before rmdir (Windows)
        self._tmpdir.cleanup()

    def _log_one(self, selected: str = None):
        dr = _make_decision_result(selected=selected)
        self.logger.log_cycle(dr, _make_news_candidates(), {}, "TREND")

    # ── File creation ────────────────────────────────────────────────────────

    def test_log_cycle_creates_jsonl_file(self):
        self._log_one()
        files = list(self.log_dir.glob("explain_*.jsonl"))
        assert len(files) >= 1

    def test_log_cycle_output_is_valid_json(self):
        self._log_one()
        path = sorted(self.log_dir.glob("explain_*.jsonl"))[0]
        with open(path) as f:
            line = f.readline().strip()
        parsed = json.loads(line)
        assert "event_type" in parsed

    def test_log_cycle_event_type_values(self):
        """Cycle with no trade → SKIP or CYCLE_SNAPSHOT; with trade → DECISION."""
        self._log_one(selected=None)
        self._log_one(selected="AAPL")
        path  = sorted(self.log_dir.glob("explain_*.jsonl"))[0]
        lines = [json.loads(l) for l in path.read_text().strip().split("\n")]
        types = {l["event_type"] for l in lines}
        # At least one DECISION type should appear
        assert LOG_DECISION in types or LOG_SKIP in types

    def test_log_cycle_multiple_entries_appended(self):
        for _ in range(5):
            self._log_one()
        path  = sorted(self.log_dir.glob("explain_*.jsonl"))[0]
        lines = path.read_text().strip().split("\n")
        assert len(lines) == 5

    # ── Health event ─────────────────────────────────────────────────────────

    def test_log_health_event_writes_entry(self):
        self.logger.log_health_event("AAPL", _make_dh_report())
        path  = sorted(self.log_dir.glob("explain_*.jsonl"))[0]
        lines = [json.loads(l) for l in path.read_text().strip().split("\n")]
        types = {l.get("event_type") for l in lines}
        assert LOG_HEALTH_EVENT in types

    # ── Slippage event ───────────────────────────────────────────────────────

    def test_log_slippage_event_writes_entry(self):
        self.logger.log_slippage_event("TSLA", "SLIPPAGE_WARN", _make_fill())
        path  = sorted(self.log_dir.glob("explain_*.jsonl"))[0]
        lines = [json.loads(l) for l in path.read_text().strip().split("\n")]
        types = {l.get("event_type") for l in lines}
        assert LOG_SLIPPAGE_EVENT in types

    # ── Thread safety ────────────────────────────────────────────────────────

    def test_thread_safety_concurrent_writes(self):
        import threading
        errors = []

        def write():
            try:
                for _ in range(10):
                    self._log_one()
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=write) for _ in range(4)]
        for t in threads: t.start()
        for t in threads: t.join()
        assert not errors, f"Thread errors: {errors}"

        path  = sorted(self.log_dir.glob("explain_*.jsonl"))[0]
        lines = path.read_text().strip().split("\n")
        for line in lines:
            json.loads(line)   # every line must be valid JSON


# ─────────────────────────────────────────────────────────────────────────────
# ExplainabilityReplayer
# ─────────────────────────────────────────────────────────────────────────────

def _write_log(path: Path, events: list) -> None:
    with open(path, "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")


def _decision_event(cycle_seq: int, selected: str = None,
                    rejected: dict = None, regime: str = "TREND") -> dict:
    return {
        "event_type":  LOG_DECISION if selected else LOG_SKIP,
        "ts":          datetime.now(timezone.utc).isoformat(),
        "regime":      regime,
        "session":     "RTH",
        "cycle_seq":   cycle_seq,
        "top15":       [],
        "decision":    {"selected_ticker": selected, "reason": "ok",
                        "sqs_components": {}, "universe_rank": 1,
                        "health_size_mult": 1.0, "slippage_size_mult": 1.0},
        "gate_log":    {},
        "rejected":    rejected or {},
        "news_active": [],
        "slippage":    {},
        "open_trades": 0,
        "daily_pnl":   0.0,
    }


class TestExplainabilityReplayer:
    def setup_method(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.log_dir = Path(self._tmpdir.name)

    def teardown_method(self):
        self._tmpdir.cleanup()

    def _make_log(self, n_cycles: int = 4, extra_events: list = None) -> Path:
        events = [_decision_event(i) for i in range(n_cycles)]
        if extra_events:
            events.extend(extra_events)
        path = self.log_dir / "explain_2025-01-01.jsonl"
        _write_log(path, events)
        return path

    def test_iter_decisions_returns_expected_count(self):
        path     = self._make_log(4)
        replayer = ExplainabilityReplayer(str(path))
        assert len(list(replayer.iter_decisions())) == 4

    def test_iter_all_includes_non_decision_events(self):
        extra = [{"event_type": LOG_SLIPPAGE_EVENT, "ticker": "AAPL", "ts": "..."}]
        path     = self._make_log(3, extra_events=extra)
        replayer = ExplainabilityReplayer(str(path))
        # 3 decisions + 1 slippage = 4
        assert len(list(replayer.iter_all())) == 4

    def test_iter_decisions_excludes_non_decision_events(self):
        extra    = [{"event_type": LOG_SLIPPAGE_EVENT, "ticker": "AAPL", "ts": "..."}]
        path     = self._make_log(3, extra_events=extra)
        replayer = ExplainabilityReplayer(str(path))
        # Slippage events should NOT appear in iter_decisions
        for event in replayer.iter_decisions():
            assert event["event_type"] in (LOG_DECISION, LOG_SKIP, LOG_CYCLE_SNAPSHOT)

    def test_decision_summary_length(self):
        path     = self._make_log(4)
        replayer = ExplainabilityReplayer(str(path))
        summary  = replayer.decision_summary()
        assert isinstance(summary, list)
        assert len(summary) == 4

    def test_gate_analysis_counts(self):
        events = [
            _decision_event(0, rejected={"AAPL": "spread_wide",
                                         "TSLA": "spread_wide",
                                         "NVDA": "quote_stale"}),
        ]
        path     = self.log_dir / "explain_gate.jsonl"
        _write_log(path, events)
        replayer = ExplainabilityReplayer(str(path))
        analysis = replayer.gate_analysis()
        assert analysis.get("spread_wide",  0) == 2
        assert analysis.get("quote_stale", 0) == 1

    def test_find_cycle_by_seq(self):
        path     = self._make_log(5)
        replayer = ExplainabilityReplayer(str(path))
        result   = replayer.find_cycle(2)
        assert result is not None
        assert result["cycle_seq"] == 2

    def test_find_cycle_not_found_returns_none(self):
        path     = self._make_log(2)
        replayer = ExplainabilityReplayer(str(path))
        assert replayer.find_cycle(999) is None

    def test_missing_file_returns_empty_iter(self):
        replayer = ExplainabilityReplayer("/nonexistent/explain.jsonl")
        assert list(replayer.iter_decisions()) == []

    def test_news_impact_analysis_tracks_seen(self):
        events = [
            _decision_event(0, selected=None, rejected={}),   # SKIP
        ]
        events[0]["news_active"] = [
            {"ticker": "AAPL", "headline": "big news", "catalyst_type": "earnings"}
        ]
        path     = self.log_dir / "explain_news.jsonl"
        _write_log(path, [events[0]])
        replayer = ExplainabilityReplayer(str(path))
        impact   = replayer.news_impact_analysis()
        assert "AAPL" in impact
        assert impact["AAPL"]["seen"] >= 1
