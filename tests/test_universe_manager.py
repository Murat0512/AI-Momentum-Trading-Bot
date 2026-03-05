"""
tests/test_universe_manager.py — Acceptance tests for scanner.universe_manager

UniverseManager() and CandidateEntry use the global CONFIG singleton.
Tests monkeypatch CONFIG.universe thresholds where specific values are needed.
"""
from datetime import datetime, timezone, timedelta

import pytest

from config.settings import CONFIG
from config.constants import SOURCE_DISCOVERY, SOURCE_NEWS, SOURCE_TRACKER
from scanner.universe_manager import CandidateEntry, UniverseManager


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _fake_metrics(ticker: str = "AAPL", demand: float = 0.65,
                  rvol: float = 3.0, price: float = 25.0):
    """Minimal DemandMetrics-like mock with all required attributes."""
    from unittest.mock import MagicMock
    m = MagicMock()
    m.ticker             = ticker
    m.demand_score       = demand
    m.last_price         = price
    m.dollar_volume      = price * 100_000
    m.rvol               = rvol
    m.gap_pct            = 0.05
    m.intraday_range_pct = 0.02
    m._dh_report         = None
    m._feed_type         = "alpaca_sip"
    return m


def _fake_tape(ticker: str = "AAPL", promoted: bool = True):
    from unittest.mock import MagicMock
    from config.constants import TAPE_PROMOTED, TAPE_REJECTED_VELOCITY
    t = MagicMock()
    t.ticker    = ticker
    t.tag       = TAPE_PROMOTED if promoted else TAPE_REJECTED_VELOCITY
    t.promoted  = promoted
    t.feed_type = "alpaca_sip"
    return t


# ─────────────────────────────────────────────────────────────────────────────
# CandidateEntry
# ─────────────────────────────────────────────────────────────────────────────

class TestCandidateEntry:
    def test_initial_composite_equals_demand(self, monkeypatch):
        """With zero news, composite_score == demand_score."""
        monkeypatch.setattr(CONFIG.universe, "news_weight",   1.0)
        monkeypatch.setattr(CONFIG.universe, "news_score_cap", 0.30)
        e = CandidateEntry(ticker="AAPL", demand_score=0.60)
        e.update_composite()
        assert e.composite_score == pytest.approx(0.60, abs=0.01)

    def test_news_score_adds_to_composite(self, monkeypatch):
        monkeypatch.setattr(CONFIG.universe, "news_weight",   1.0)
        monkeypatch.setattr(CONFIG.universe, "news_score_cap", 0.30)
        e = CandidateEntry(ticker="AAPL", demand_score=0.60, news_score=0.20)
        e.update_composite()
        assert e.composite_score == pytest.approx(0.80, abs=0.01)

    def test_news_score_cap_enforced(self, monkeypatch):
        monkeypatch.setattr(CONFIG.universe, "news_weight",   1.0)
        monkeypatch.setattr(CONFIG.universe, "news_score_cap", 0.30)
        e = CandidateEntry(ticker="AAPL", demand_score=0.50, news_score=0.99)
        e.update_composite()
        # capped: 0.50 + 0.30 = 0.80 max
        assert e.composite_score == pytest.approx(0.80, abs=0.01)

    def test_composite_never_exceeds_demand_plus_cap(self, monkeypatch):
        monkeypatch.setattr(CONFIG.universe, "news_weight",   2.0)
        monkeypatch.setattr(CONFIG.universe, "news_score_cap", 0.25)
        e = CandidateEntry(ticker="TSLA", demand_score=0.6, news_score=0.5)
        e.update_composite()
        assert e.composite_score <= 0.6 + 0.25 + 0.001


# ─────────────────────────────────────────────────────────────────────────────
# UniverseManager — update / score
# ─────────────────────────────────────────────────────────────────────────────

class TestUniverseManagerUpdates:
    def setup_method(self):
        self.mgr = UniverseManager()
        self.mgr.reset()

    def test_update_from_metrics_adds_ticker(self):
        self.mgr.update_from_metrics(_fake_metrics("AAPL"))
        assert self.mgr.pool_size() >= 1

    def test_update_from_metrics_upserts(self):
        """Re-inserting same ticker refreshes score, does NOT duplicate."""
        self.mgr.update_from_metrics(_fake_metrics("AAPL", demand=0.5))
        self.mgr.update_from_metrics(_fake_metrics("AAPL", demand=0.7))
        assert self.mgr.pool_size() == 1
        entry = self.mgr.get("AAPL")
        assert entry is not None
        assert entry.demand_score == pytest.approx(0.7)

    def test_news_score_raises_composite(self, monkeypatch):
        monkeypatch.setattr(CONFIG.universe, "news_weight",   1.0)
        monkeypatch.setattr(CONFIG.universe, "news_score_cap", 0.30)
        self.mgr.update_from_metrics(_fake_metrics("AAPL", demand=0.50))
        original_composite = self.mgr.get("AAPL").composite_score
        self.mgr.update_news_score("AAPL", 0.20, SOURCE_NEWS)
        new_composite = self.mgr.get("AAPL").composite_score
        assert new_composite > original_composite

    def test_news_score_cap_limits_composite(self, monkeypatch):
        monkeypatch.setattr(CONFIG.universe, "news_weight",   1.0)
        monkeypatch.setattr(CONFIG.universe, "news_score_cap", 0.30)
        self.mgr.update_from_metrics(_fake_metrics("AAPL", demand=0.50))
        self.mgr.update_news_score("AAPL", 0.99, SOURCE_NEWS)
        entry = self.mgr.get("AAPL")
        # demand 0.5 + cap 0.3 = 0.8 max
        assert entry.composite_score <= 0.81

    def test_update_tape_confirmed(self):
        self.mgr.update_from_metrics(_fake_metrics("TSLA", demand=0.60))
        self.mgr.update_tape_result("TSLA", _fake_tape("TSLA", promoted=True))
        entry = self.mgr.get("TSLA")
        assert entry is not None
        assert entry.tape_confirmed is True

    def test_tape_rejected_unconfirmed(self):
        self.mgr.update_from_metrics(_fake_metrics("TSLA", demand=0.60))
        self.mgr.update_tape_result("TSLA", _fake_tape("TSLA", promoted=False))
        assert self.mgr.pool_size() == 1
        entry = self.mgr.get("TSLA")
        assert not entry.tape_confirmed

    def test_news_score_creates_shell_entry(self):
        """update_news_score() for an unknown ticker creates a shell entry."""
        self.mgr.update_news_score("NEWCO", 0.20, SOURCE_NEWS)
        assert self.mgr.get("NEWCO") is not None


# ─────────────────────────────────────────────────────────────────────────────
# UniverseManager — expiry
# ─────────────────────────────────────────────────────────────────────────────

class TestUniverseManagerExpiry:
    def setup_method(self):
        self.mgr = UniverseManager()
        self.mgr.reset()

    def test_expire_stale_removes_old_entries(self, monkeypatch):
        """Passing a far-future `now` to expire_stale() removes all entries."""
        monkeypatch.setattr(CONFIG.universe, "expiry_minutes", 5)
        self.mgr.update_from_metrics(_fake_metrics("AAPL"))
        from datetime import timezone
        far_future = datetime.now(timezone.utc) + timedelta(hours=2)
        removed = self.mgr.expire_stale(far_future)
        assert "AAPL" in removed
        assert self.mgr.pool_size() == 0

    def test_expire_stale_keeps_fresh_entries(self):
        """expire_stale(now) must NOT remove a newly-added entry."""
        self.mgr.update_from_metrics(_fake_metrics("AAPL"))
        from datetime import timezone as _tz
        now     = datetime.now(_tz.utc)
        removed = self.mgr.expire_stale(now)
        assert len(removed) == 0
        assert self.mgr.pool_size() >= 1


# ─────────────────────────────────────────────────────────────────────────────
# UniverseManager — top_n
# ─────────────────────────────────────────────────────────────────────────────

class TestUniverseManagerTopN:
    def setup_method(self):
        self.mgr = UniverseManager()
        self.mgr.reset()

    def _load_tickers(self, n: int = 20):
        for i in range(n):
            self.mgr.update_from_metrics(
                _fake_metrics(f"TK{i:02d}", demand=round(i * 0.04, 3))
            )

    def test_top_n_returns_at_most_n(self, monkeypatch):
        monkeypatch.setattr(CONFIG.universe, "top_n", 5)
        self._load_tickers(20)
        top = self.mgr.top_n(5)
        assert len(top) <= 5

    def test_top_n_sorted_descending(self):
        self._load_tickers()
        top = self.mgr.top_n(5)
        scores = [e.composite_score for e in top]
        assert scores == sorted(scores, reverse=True)

    def test_top_n_rank_one_is_best(self):
        self._load_tickers()
        top = self.mgr.top_n(5)
        if top:
            assert top[0].rank == 1

    def test_top_n_empty_when_pool_empty(self):
        assert self.mgr.top_n(15) == []

    def test_is_top_n_high_score_ticker(self, monkeypatch):
        """Ticker with highest demand_score is in top_n regardless of n."""
        monkeypatch.setattr(CONFIG.universe, "min_composite_score", 0.0)
        self._load_tickers(10)
        self.mgr.top_n(3)
        # TK09 has demand 0.36, the highest
        assert self.mgr.is_top_n("TK09", n=3)

    def test_is_not_top_n_low_score_ticker(self, monkeypatch):
        """Ticker at the bottom of the ranking is NOT in top 3."""
        monkeypatch.setattr(CONFIG.universe, "min_composite_score", 0.0)
        self._load_tickers(10)
        self.mgr.top_n(3)
        assert not self.mgr.is_top_n("TK00", n=3)
