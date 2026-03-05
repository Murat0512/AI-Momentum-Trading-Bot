"""
tests/test_governance.py — Unit tests for config/governance.py
"""
import pytest
from datetime import datetime
from unittest.mock import patch, MagicMock
import pytz

ET = pytz.timezone("America/New_York")

from config.governance import (
    compute_config_hash,
    create_run_manifest,
    generate_report,
    RunManifest,
    EvaluationReport,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_trade(ticker="AAPL", pnl_r=1.0, regime="trend", setup="breakout",
               entry_hour="10", is_closed=True):
    t = MagicMock()
    t.ticker     = ticker
    t.pnl_r      = pnl_r
    t.pnl        = pnl_r * 100
    t.risk_amount = 100.0
    t.regime     = regime
    t.setup_name = setup
    t.is_closed  = is_closed

    dt = ET.localize(datetime(2024, 6, 10, int(entry_hour), 30))
    t.entry_time = dt
    return t


def trades_batch(pnl_rs):
    return [make_trade(pnl_r=r) for r in pnl_rs]


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestComputeConfigHash:
    def test_returns_12_char_hex(self):
        h = compute_config_hash()
        assert len(h) == 12
        assert all(c in "0123456789abcdef" for c in h)

    def test_deterministic(self):
        h1 = compute_config_hash()
        h2 = compute_config_hash()
        assert h1 == h2

    def test_different_config_different_hash(self):
        from config.settings import CONFIG
        h1 = compute_config_hash()
        original_max = CONFIG.risk.max_trades_per_day
        CONFIG.risk.max_trades_per_day = original_max + 999
        h2 = compute_config_hash()
        CONFIG.risk.max_trades_per_day = original_max   # restore
        assert h1 != h2


class TestCreateRunManifest:
    def test_run_id_format(self):
        m = create_run_manifest(datetime(2024, 6, 10, tzinfo=ET))
        assert m.run_id.startswith("20240610-")
        parts = m.run_id.split("-")
        assert len(parts) == 2
        assert len(parts[1]) == 12

    def test_config_hash_matches(self):
        m = create_run_manifest()
        assert m.config_hash == compute_config_hash()

    def test_paper_mode_by_default(self):
        m = create_run_manifest()
        assert m.mode   == "paper"
        assert m.broker == "paper"

    def test_live_mode_passthrough(self):
        m = create_run_manifest(mode="live", broker="alpaca", account_size=25_000)
        assert m.mode         == "live"
        assert m.broker       == "alpaca"
        assert m.account_size == 25_000

    def test_started_at_is_iso_string(self):
        m = create_run_manifest()
        # Should not raise
        datetime.fromisoformat(m.started_at)


class TestGenerateReport:
    def test_empty_trades_warning(self):
        rpt = generate_report([], run_id="R1", config_hash="abc")
        assert rpt.trade_count == 0
        assert any("unreliable" in w.lower() or "trades" in w.lower() for w in rpt.warnings)

    def test_open_trades_excluded(self):
        trades = [make_trade(is_closed=False) for _ in range(3)]
        rpt = generate_report(trades)
        assert rpt.trade_count == 0

    def test_basic_win_loss_counts(self):
        trades = trades_batch([1.0, 2.0, -0.5, -1.0, 1.5])
        rpt = generate_report(trades)
        assert rpt.win_count  == 3
        assert rpt.loss_count == 2

    def test_expectancy_positive(self):
        trades = trades_batch([2.0, 2.0, -1.0, -1.0, 2.0])
        rpt = generate_report(trades)
        # 3 wins avg 2.0, 2 losses avg -1.0
        # E = 0.6*2 + 0.4*(-1) = 1.2 - 0.4 = 0.8
        assert rpt.expectancy_r == pytest.approx(0.8, abs=1e-3)

    def test_expectancy_negative_warning(self):
        trades = trades_batch([-1.0, -1.5, -2.0])
        rpt = generate_report(trades)
        assert rpt.expectancy_r < 0
        assert any("negative expectancy" in w.lower() for w in rpt.warnings)

    def test_max_drawdown_calculation(self):
        # Cumulative: 1, 2, 3, 1, 0  →  peak=3, max DD=3
        trades = trades_batch([1.0, 1.0, 1.0, -2.0, -1.0])
        rpt = generate_report(trades)
        assert rpt.max_drawdown_r == pytest.approx(3.0, abs=1e-3)

    def test_breakdown_by_regime(self):
        t1 = make_trade(pnl_r=1.0, regime="trend")
        t2 = make_trade(pnl_r=2.0, regime="trend")
        t3 = make_trade(pnl_r=-1.0, regime="range")
        rpt = generate_report([t1, t2, t3])
        assert "trend" in rpt.by_regime
        assert rpt.by_regime["trend"]["count"] == 2
        assert "range" in rpt.by_regime

    def test_breakdown_by_setup(self):
        t1 = make_trade(pnl_r=1.0, setup="breakout")
        t2 = make_trade(pnl_r=0.5, setup="vwap_reclaim")
        rpt = generate_report([t1, t2])
        assert "breakout"     in rpt.by_setup
        assert "vwap_reclaim" in rpt.by_setup

    def test_breakdown_by_hour(self):
        t1 = make_trade(pnl_r=1.0, entry_hour="10")
        t2 = make_trade(pnl_r=1.0, entry_hour="11")
        rpt = generate_report([t1, t2])
        assert "10" in rpt.by_hour
        assert "11" in rpt.by_hour

    def test_slippage_stats_propagated(self):
        trades = trades_batch([1.0] * 10)
        rpt = generate_report(trades, slippage_stats={"avg_r": 0.05})
        assert rpt.avg_slippage_r == pytest.approx(0.05, abs=1e-4)
