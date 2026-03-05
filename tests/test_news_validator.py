from __future__ import annotations

from dataclasses import dataclass

import pytest

from config.settings import CONFIG
from intelligence.news_validator import NewsValidationResult, NewsValidator


@dataclass
class _Candidate:
    ticker: str
    headline: str
    news_score: float = 0.4


@pytest.fixture(autouse=True)
def _sentiment_defaults(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(CONFIG.strategy, "sentiment_gate_enabled", True)
    monkeypatch.setattr(CONFIG.strategy, "sentiment_threshold", 0.2)


def test_validate_ticker_blocks_without_headlines(monkeypatch: pytest.MonkeyPatch):
    nv = NewsValidator()
    monkeypatch.setattr(
        "intelligence.news_validator.news_ingestor.get_candidates", lambda: []
    )
    monkeypatch.setattr(
        "intelligence.news_validator.news_ingestor.fetch_once", lambda: []
    )

    result = nv.validate_ticker("TSLA")
    assert isinstance(result, NewsValidationResult)
    assert result.allowed is False
    assert result.score == pytest.approx(0.0)


def test_validate_ticker_passes_positive(monkeypatch: pytest.MonkeyPatch):
    nv = NewsValidator()
    monkeypatch.setattr(
        "intelligence.news_validator.news_ingestor.get_candidates",
        lambda: [
            _Candidate(
                ticker="TSLA", headline="TSLA beats earnings and raises guidance"
            )
        ],
    )
    monkeypatch.setattr(nv, "_finbert_score", lambda _h: 0.55)

    result = nv.validate_ticker("TSLA")
    assert result.allowed is True
    assert result.score == pytest.approx(0.55)


def test_validate_ticker_blocks_below_threshold(monkeypatch: pytest.MonkeyPatch):
    nv = NewsValidator()
    monkeypatch.setattr(
        "intelligence.news_validator.news_ingestor.get_candidates",
        lambda: [_Candidate(ticker="TSLA", headline="mixed update")],
    )
    monkeypatch.setattr(nv, "_finbert_score", lambda _h: 0.1)

    result = nv.validate_ticker("TSLA")
    assert result.allowed is False
    assert result.score == pytest.approx(0.1)


def test_validate_ticker_throttles_fetch_once(monkeypatch: pytest.MonkeyPatch):
    nv = NewsValidator()
    monkeypatch.setattr(CONFIG.news, "cache_ttl_seconds", 9999)

    calls = {"n": 0}

    def _get_candidates():
        return []

    def _fetch_once():
        calls["n"] += 1
        return []

    monkeypatch.setattr(
        "intelligence.news_validator.news_ingestor.get_candidates", _get_candidates
    )
    monkeypatch.setattr(
        "intelligence.news_validator.news_ingestor.fetch_once", _fetch_once
    )

    nv.validate_ticker("TSLA")
    nv.validate_ticker("AAPL")
    nv.validate_ticker("NVDA")

    assert calls["n"] == 1
