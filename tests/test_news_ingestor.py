"""
tests/test_news_ingestor.py — Acceptance tests for news.ingestor

All modules use a global CONFIG singleton; NewsIngestor() takes no constructor
arguments.  Where a test needs a custom threshold, we monkeypatch CONFIG.news.
"""
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

import pytest

from config.settings import CONFIG
from config.constants import (
    CATALYST_EARNINGS, CATALYST_FDA, CATALYST_MERGER,
    CATALYST_SQUEEZE, CATALYST_GENERAL,
)
from news.ingestor import (
    NewsIngestor,
    _extract_tickers,
    _score_catalyst,
)


# ─────────────────────────────────────────────────────────────────────────────
# _extract_tickers
# ─────────────────────────────────────────────────────────────────────────────

def test_extract_ticker_dollar_sign():
    """$AAPL in text → AAPL extracted."""
    result = _extract_tickers("$AAPL reports record earnings today", CONFIG.news)
    assert "AAPL" in result


def test_extract_ticker_exchange_prefix():
    """NYSE:TSLA → TSLA extracted."""
    result = _extract_tickers("NYSE:TSLA hits a new high", CONFIG.news)
    assert "TSLA" in result


def test_extract_ticker_noise_filtered():
    """Common noise words must NOT appear as tickers."""
    result = _extract_tickers(
        "The CEO of the company met SEC officials about an IPO", CONFIG.news
    )
    for noise in ("CEO", "SEC", "THE", "IPO"):
        assert noise not in result, f"Noise word {noise!r} leaked through"


def test_extract_ticker_max_cap(monkeypatch):
    """Respects cfg.max_news_tickers limit."""
    monkeypatch.setattr(CONFIG.news, "max_news_tickers", 3)
    text = " ".join(f"$TK{i:02d}" for i in range(10))     # 10 unique tickers
    result = _extract_tickers(text, CONFIG.news)
    assert len(result) <= 3


def test_extract_ticker_empty_string():
    assert _extract_tickers("", CONFIG.news) == []


# ─────────────────────────────────────────────────────────────────────────────
# _score_catalyst
# ─────────────────────────────────────────────────────────────────────────────

def test_score_catalyst_earnings_keyword():
    cat_type, score = _score_catalyst("Company delivers surprise earnings beat")
    assert cat_type == CATALYST_EARNINGS
    assert score > 0


def test_score_catalyst_fda_keyword():
    cat_type, score = _score_catalyst("FDA grants approval for new drug")
    assert cat_type == CATALYST_FDA


def test_score_catalyst_squeeze_keyword():
    cat_type, score = _score_catalyst("Traders spot potential short squeeze setup")
    assert cat_type == CATALYST_SQUEEZE


def test_score_catalyst_generic_headline():
    cat_type, score = _score_catalyst("Company updates its website")
    assert cat_type == CATALYST_GENERAL
    assert score <= CONFIG.news.catalyst_score_general + 0.01


def test_score_catalyst_capped():
    """Score never exceeds max_news_score_boost."""
    _, score = _score_catalyst("massive earnings explosion FDA approval merger squeeze")
    assert score <= CONFIG.news.max_news_score_boost + 1e-9


# ─────────────────────────────────────────────────────────────────────────────
# NewsIngestor class
# ─────────────────────────────────────────────────────────────────────────────

def _fake_article(headline: str = "AAPL earnings beat", ticker: str = "AAPL",
                  age_seconds: int = 5) -> dict:
    """Minimal Finnhub article dict."""
    ts = int((datetime.now(timezone.utc) - timedelta(seconds=age_seconds)).timestamp())
    return {"headline": headline, "datetime": ts, "related": ticker,
            "url": "", "summary": "", "source": "Reuters"}


class TestNewsIngestor:
    def setup_method(self):
        self.ingestor = NewsIngestor()

    def teardown_method(self):
        self.ingestor.stop()

    # ── get_candidates before any fetch ─────────────────────────────────────

    def test_get_candidates_empty_initially(self):
        result = self.ingestor.get_candidates()
        assert isinstance(result, list)
        assert len(result) == 0

    # ── fetch_once ───────────────────────────────────────────────────────────

    def test_fetch_once_populates_cache(self, monkeypatch):
        monkeypatch.setattr("news.ingestor._finnhub_key", lambda: "test_key")
        monkeypatch.setattr(
            "news.ingestor._fetch_general_news",
            lambda api_key, category="general": [
                _fake_article("AAPL earnings beat", "AAPL")
            ],
        )
        self.ingestor.fetch_once()
        candidates = self.ingestor.get_candidates()
        assert any(c.ticker == "AAPL" for c in candidates)

    def test_no_duplicate_candidates(self, monkeypatch):
        """Same (ticker, headline) fetched twice → deduplicated to 1."""
        monkeypatch.setattr("news.ingestor._finnhub_key", lambda: "test_key")
        monkeypatch.setattr(
            "news.ingestor._fetch_general_news",
            lambda api_key, category="general": [
                _fake_article("AAPL surges 15%", "AAPL")
            ],
        )
        self.ingestor.fetch_once()
        self.ingestor.fetch_once()
        candidates = self.ingestor.get_candidates()
        aapl = [c for c in candidates if c.ticker == "AAPL"]
        assert len(aapl) == 1

    # ── stale filtering ──────────────────────────────────────────────────────

    def test_stale_candidates_excluded(self, monkeypatch):
        """Articles older than max_headline_age_minutes are filtered out."""
        monkeypatch.setattr(CONFIG.news, "max_headline_age_minutes", 10)
        monkeypatch.setattr("news.ingestor._finnhub_key", lambda: "test_key")
        monkeypatch.setattr(
            "news.ingestor._fetch_general_news",
            lambda api_key, category="general": [
                _fake_article("Old news", "AAPL", age_seconds=3 * 3600)  # 3 h old
            ],
        )
        self.ingestor.fetch_once()
        candidates = self.ingestor.get_candidates()
        assert len(candidates) == 0

    # ── catalyst score ───────────────────────────────────────────────────────

    def test_catalyst_score_attached_to_candidate(self, monkeypatch):
        monkeypatch.setattr("news.ingestor._finnhub_key", lambda: "test_key")
        monkeypatch.setattr(
            "news.ingestor._fetch_general_news",
            lambda api_key, category="general": [
                _fake_article("NFLX beats earnings estimates by 30%", "NFLX")
            ],
        )
        self.ingestor.fetch_once()
        candidates = self.ingestor.get_candidates()
        nflx = [c for c in candidates if c.ticker == "NFLX"]
        assert nflx, "NFLX should appear in candidates"
        assert nflx[0].news_score > 0

    # ── lifecycle ────────────────────────────────────────────────────────────

    def test_start_stop_lifecycle(self, monkeypatch):
        monkeypatch.setattr("news.ingestor._finnhub_key", lambda: "test_key")
        monkeypatch.setattr(
            "news.ingestor._fetch_general_news",
            lambda api_key, category="general": [],
        )
        self.ingestor.start()
        assert not self.ingestor._stop_event.is_set()
        self.ingestor.stop()
        assert self.ingestor._stop_event.is_set()
