"""
tests/test_slippage_monitor.py — Acceptance tests for execution.slippage

SlippageMonitor() uses the global CONFIG singleton.
We monkeypatch CONFIG.slippage thresholds where specific values are needed.
"""
from datetime import datetime, timezone, timedelta

import pytest

from config.settings import CONFIG
from config.constants import SLIPPAGE_OK, SLIPPAGE_WARN, SLIPPAGE_SIZE_REDUCE, SLIPPAGE_BLOCK
from execution.slippage import SlippageFill, SlippageMonitor


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _now():
    return datetime.now(timezone.utc)


def _fill(ticker: str = "AAPL", expected: float = 100.0, fill_price: float = 100.0,
          spread: float = 0.001, r_value: float = 100.0, now=None) -> SlippageFill:
    return SlippageFill(
        ticker         = ticker,
        expected_price = expected,
        fill_price     = fill_price,
        spread_pct     = spread,
        r_value        = r_value,
        recorded_at    = now or _now(),
    )


# ─────────────────────────────────────────────────────────────────────────────
# SlippageFill properties
# ─────────────────────────────────────────────────────────────────────────────

class TestSlippageFill:
    def test_slippage_bps_positive_when_overpaid(self):
        f = _fill(fill_price=100.10)
        assert f.slippage_bps == pytest.approx(10.0, abs=0.5)

    def test_slippage_bps_zero_exact_fill(self):
        f = _fill(fill_price=100.0)
        assert f.slippage_bps == pytest.approx(0.0, abs=0.01)

    def test_slippage_r_ratio_computed(self):
        # slippage_dollars = 0.20; r_value = 10.0 dollars; ratio = 0.20/10.0 = 0.02
        f = _fill(expected=100.0, fill_price=100.20, r_value=10.0)
        assert f.slippage_r == pytest.approx(0.02, abs=0.001)

    def test_slippage_r_zero_if_no_r_value(self):
        f = _fill(r_value=0.0)
        assert f.slippage_r == 0.0


# ─────────────────────────────────────────────────────────────────────────────
# State transitions
# ─────────────────────────────────────────────────────────────────────────────

class TestSlippageTransitions:
    def setup_method(self):
        self.mon = SlippageMonitor()
        self.mon.reset_all()

    def test_clean_fills_return_ok(self, monkeypatch):
        """Fills well inside warn threshold → SLIPPAGE_OK."""
        monkeypatch.setattr(CONFIG.slippage, "warn_bps",               50.0)
        monkeypatch.setattr(CONFIG.slippage, "size_reduce_r_threshold", 0.80)
        for _ in range(3):
            tag = self.mon.record_fill("AAPL", 100.0, 100.001, 0.001, 100.0)
            assert tag == SLIPPAGE_OK

    def test_moderate_slippage_returns_warn(self, monkeypatch):
        """Fill > warn_bps but < size_reduce → SLIPPAGE_WARN."""
        monkeypatch.setattr(CONFIG.slippage, "warn_bps",               8.0)
        monkeypatch.setattr(CONFIG.slippage, "size_reduce_r_threshold", 0.50)
        # 10 bps fill, but r_value=10000 so r_ratio is tiny
        tag = self.mon.record_fill("AAPL", 100.0, 100.10, 0.001, 10_000.0)
        assert tag == SLIPPAGE_WARN

    def test_high_r_ratio_returns_size_reduce(self, monkeypatch):
        """Fill > size_reduce_r_threshold → SIZE_REDUCE (if < block_consecutive)."""
        monkeypatch.setattr(CONFIG.slippage, "size_reduce_r_threshold", 0.10)
        monkeypatch.setattr(CONFIG.slippage, "block_consecutive_trades", 5)
        # slippage = 5/100 = 0.05; r_value = 0.10 → ratio = 0.5 > 0.10
        tag = self.mon.record_fill("AAPL", 100.0, 105.0, 0.001, 0.10)
        assert tag == SLIPPAGE_SIZE_REDUCE

    def test_block_after_consecutive_bad_fills(self, monkeypatch):
        """3 consecutive fills above threshold → BLOCK on the 3rd."""
        monkeypatch.setattr(CONFIG.slippage, "size_reduce_r_threshold", 0.10)
        monkeypatch.setattr(CONFIG.slippage, "block_consecutive_trades", 3)
        monkeypatch.setattr(CONFIG.slippage, "block_duration_minutes",  30)
        for _ in range(3):
            tag = self.mon.record_fill("TSLA", 100.0, 105.0, 0.001, 0.10)
        assert tag == SLIPPAGE_BLOCK
        assert self.mon.should_block("TSLA", _now())

    def test_size_multiplier_one_for_clean_ticker(self):
        """Unknown ticker → size_multiplier == 1.0."""
        assert self.mon.size_multiplier("CLEAN") == 1.0

    def test_size_multiplier_zero_when_blocked(self, monkeypatch):
        monkeypatch.setattr(CONFIG.slippage, "size_reduce_r_threshold", 0.10)
        monkeypatch.setattr(CONFIG.slippage, "block_consecutive_trades", 2)
        monkeypatch.setattr(CONFIG.slippage, "block_duration_minutes",  30)
        for _ in range(2):
            self.mon.record_fill("GOOG", 100.0, 105.0, 0.001, 0.10)
        assert self.mon.size_multiplier("GOOG") == 0.0

    def test_size_multiplier_less_than_one_when_reducing(self, monkeypatch):
        monkeypatch.setattr(CONFIG.slippage, "size_reduce_r_threshold", 0.10)
        monkeypatch.setattr(CONFIG.slippage, "block_consecutive_trades", 5)
        self.mon.record_fill("AAPL", 100.0, 105.0, 0.001, 0.10)
        assert self.mon.size_multiplier("AAPL") < 1.0


# ─────────────────────────────────────────────────────────────────────────────
# Block window
# ─────────────────────────────────────────────────────────────────────────────

class TestBlockWindow:
    def setup_method(self):
        self.mon = SlippageMonitor()
        self.mon.reset_all()

    def test_block_auto_clears_after_duration(self, monkeypatch):
        monkeypatch.setattr(CONFIG.slippage, "size_reduce_r_threshold", 0.10)
        monkeypatch.setattr(CONFIG.slippage, "block_consecutive_trades", 2)
        monkeypatch.setattr(CONFIG.slippage, "block_duration_minutes",  1)  # 1 min block
        now = _now()
        for _ in range(2):
            self.mon.record_fill("TSLA", 100.0, 105.0, 0.001, 0.10, now)
        assert self.mon.should_block("TSLA", now)
        future = now + timedelta(minutes=2)
        assert not self.mon.should_block("TSLA", future)

    def test_unknown_ticker_never_blocked(self):
        assert not self.mon.should_block("UNKNOWN", _now())


# ─────────────────────────────────────────────────────────────────────────────
# Diagnostics
# ─────────────────────────────────────────────────────────────────────────────

class TestSlippageDiagnostics:
    def setup_method(self):
        self.mon = SlippageMonitor()
        self.mon.reset_all()

    def test_ticker_stats_returns_dict(self):
        self.mon.record_fill("AAPL", 100.0, 100.05, 0.001, 50.0)
        stats = self.mon.ticker_stats("AAPL")
        assert isinstance(stats, dict)
        assert stats.get("fills") == 1

    def test_all_stats_includes_all_tickers(self):
        for tkr in ("AAPL", "TSLA", "NVDA"):
            self.mon.record_fill(tkr, 100.0, 100.05, 0.001, 50.0)
        stats = self.mon.all_stats()
        assert set(stats.keys()) >= {"AAPL", "TSLA", "NVDA"}

    def test_reset_all_clears_state(self, monkeypatch):
        monkeypatch.setattr(CONFIG.slippage, "block_consecutive_trades", 2)
        monkeypatch.setattr(CONFIG.slippage, "size_reduce_r_threshold", 0.10)
        monkeypatch.setattr(CONFIG.slippage, "block_duration_minutes",  30)
        for _ in range(2):
            self.mon.record_fill("AAPL", 100.0, 105.0, 0.001, 0.10)
        self.mon.reset_all()
        assert not self.mon.should_block("AAPL", _now())
        assert self.mon.all_stats() == {}
