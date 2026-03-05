from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

import pytz

from config.settings import CONFIG
from news.ingestor import NewsCandidate, news_ingestor

log = logging.getLogger(__name__)
ET = pytz.timezone("America/New_York")


@dataclass
class NewsValidationResult:
    ticker: str
    score: float
    allowed: bool
    reason: str = ""
    headline: str = ""
    source: str = ""
    validated_at: Optional[datetime] = None


class NewsValidator:
    """
    Finnhub + FinBERT sentiment validator for hard-gating entries.

    Score convention:
      +1.0 = strongly positive, 0 = neutral, -1.0 = strongly negative
    Gate:
      allowed iff score >= CONFIG.strategy.sentiment_threshold
    """

    def __init__(self) -> None:
        self._pipe = None
        self._load_failed = False
        self._last_refresh_at: Optional[datetime] = None

    def _refresh_interval_seconds(self) -> int:
        return int(getattr(CONFIG.news, "cache_ttl_seconds", 300) or 300)

    def _can_refresh_now(self) -> bool:
        if self._last_refresh_at is None:
            return True
        delta = (datetime.now(ET) - self._last_refresh_at).total_seconds()
        return delta >= self._refresh_interval_seconds()

    def validate_ticker(self, ticker: str) -> NewsValidationResult:
        now = datetime.now(ET)
        threshold = float(getattr(CONFIG.strategy, "sentiment_threshold", 0.20))

        if not bool(getattr(CONFIG.strategy, "sentiment_gate_enabled", True)):
            return NewsValidationResult(
                ticker=ticker,
                score=1.0,
                allowed=True,
                reason="sentiment gate disabled",
                validated_at=now,
            )

        candidates = self._ticker_candidates(ticker)
        if not candidates:
            # Mandatory hard gate: no positive news context means no trade.
            return NewsValidationResult(
                ticker=ticker,
                score=0.0,
                allowed=False,
                reason="no recent headlines",
                validated_at=now,
            )

        headline = candidates[0].headline
        score = self._finbert_score(headline)
        allowed = score >= threshold
        return NewsValidationResult(
            ticker=ticker,
            score=round(score, 4),
            allowed=allowed,
            reason=("pass" if allowed else f"score<{threshold:.2f}"),
            headline=headline,
            source="finnhub+finbert",
            validated_at=now,
        )

    def _ticker_candidates(self, ticker: str) -> List[NewsCandidate]:
        ticker = str(ticker or "").upper().strip()
        candidates = [c for c in news_ingestor.get_candidates() if c.ticker == ticker]
        if candidates:
            return sorted(candidates, key=lambda c: c.news_score, reverse=True)

        # Lazy one-shot refresh if cache is empty for ticker.
        # Throttled to avoid per-ticker Finnhub bursts in scanner loops.
        if not self._can_refresh_now():
            return []
        try:
            news_ingestor.fetch_once()
            self._last_refresh_at = datetime.now(ET)
        except Exception as exc:  # noqa: BLE001
            log.debug("[NewsValidator] fetch_once failed: %s", exc)
            return []

        candidates = [c for c in news_ingestor.get_candidates() if c.ticker == ticker]
        return sorted(candidates, key=lambda c: c.news_score, reverse=True)

    def _finbert_score(self, text: str) -> float:
        pipe = self._get_pipe()
        if pipe is None:
            return self._fallback_score(text)

        try:
            out = pipe(text[:512], truncation=True)
            if not out:
                return 0.0
            row = out[0]
            label = str(row.get("label", "")).lower()
            conf = float(row.get("score", 0.0))
            if "positive" in label:
                return conf
            if "negative" in label:
                return -conf
            return 0.0
        except Exception as exc:  # noqa: BLE001
            log.debug("[NewsValidator] FinBERT inference failed: %s", exc)
            return self._fallback_score(text)

    def _get_pipe(self):
        if self._pipe is not None:
            return self._pipe
        if self._load_failed:
            return None
        try:
            from transformers import pipeline

            self._pipe = pipeline(
                "text-classification",
                model="ProsusAI/finbert",
                tokenizer="ProsusAI/finbert",
            )
            return self._pipe
        except Exception as exc:  # noqa: BLE001
            self._load_failed = True
            log.warning(
                "[NewsValidator] FinBERT unavailable; using fallback sentiment (%s)",
                exc,
            )
            return None

    def _fallback_score(self, text: str) -> float:
        t = text.lower()
        pos = [
            "beats",
            "beat",
            "approval",
            "surge",
            "breakout",
            "raises guidance",
            "acquire",
            "partnership",
            "upgrade",
        ]
        neg = [
            "miss",
            "downgrade",
            "lawsuit",
            "offering",
            "dilution",
            "fraud",
            "probe",
            "delay",
            "halt",
        ]
        pos_hits = sum(1 for k in pos if k in t)
        neg_hits = sum(1 for k in neg if k in t)
        raw = pos_hits - neg_hits
        if raw == 0:
            return 0.0
        if raw > 0:
            return min(1.0, 0.2 + 0.15 * raw)
        return max(-1.0, -0.2 + 0.15 * raw)


news_validator = NewsValidator()
