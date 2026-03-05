"""
news/ingestor.py — Non-blocking Finnhub news ingestor.

Flow:
  1. Poll Finnhub every poll_interval_seconds (default 120s).
  2. Extract tickers from headline text via permissive regex + known-ticker filter.
  3. Tag each headline with a catalyst type using keyword rules.
  4. Assign a capped NewsCatalystScore per catalyst type.
  5. Return cached results immediately if the cache is fresh.
  6. Results are HINTS only — news never forces a trade.

Design:
  - All HTTP calls are isolated; failures degrade to empty cache gracefully.
  - Cache is thread-safe (lock-protected).
  - No external dependencies beyond `requests` (lazily checked at runtime).
  - Finnhub free tier: 60 API calls/minute; we stay well within limits.
"""

from __future__ import annotations

import logging
import os
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pytz

from config.constants import (
    ALL_CATALYST_TYPES,
    CATALYST_EARNINGS, CATALYST_FDA, CATALYST_GENERAL,
    CATALYST_MERGER, CATALYST_SQUEEZE,
    SOURCE_NEWS,
)
from config.settings import CONFIG

log = logging.getLogger(__name__)
ET  = pytz.timezone("America/New_York")


# ─────────────────────────────────────────────────────────────────────────────
# RESULT TYPE
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class NewsCandidate:
    """A single ticker mentioned in a news headline."""
    ticker:        str
    headline:      str
    catalyst_type: str          # one of ALL_CATALYST_TYPES
    news_score:    float        # capped at CONFIG.news.max_news_score_boost
    published_at:  datetime
    url:           str  = ""
    source:        str  = SOURCE_NEWS

    def age_minutes(self) -> float:
        now = datetime.now(ET)
        published = self.published_at
        if published.tzinfo is None:
            published = ET.localize(published)
        return (now - published).total_seconds() / 60.0

    def is_fresh(self, max_age_minutes: int = None) -> bool:
        limit = max_age_minutes or CONFIG.news.max_headline_age_minutes
        return self.age_minutes() <= limit


# ─────────────────────────────────────────────────────────────────────────────
# CATALYST KEYWORD RULES
# ─────────────────────────────────────────────────────────────────────────────

_CATALYST_KEYWORDS: Dict[str, List[str]] = {
    CATALYST_EARNINGS: [
        "earnings", "eps", "revenue", "quarterly results", "profit",
        "beat", "miss", "guidance", "forecast", "q1", "q2", "q3", "q4",
        "annual report", "fiscal year",
    ],
    CATALYST_FDA: [
        "fda", "approval", "drug", "clinical trial", "phase",
        "biologics", "nda", "bla", "510k", "clearance", "pdufa",
        "treatment", "therapy", "cancer", "disease",
    ],
    CATALYST_MERGER: [
        "merger", "acquisition", "buyout", "takeover", "offer",
        "deal", "combine", "acquire", "purchased by", "bought by",
        "tender offer", "shareholder vote", "going private",
    ],
    CATALYST_SQUEEZE: [
        "short squeeze", "short interest", "high short", "heavily shorted",
        "short covering", "gamma squeeze", "meme", "reddit", "wallstreetbets",
        "retail buying", "short sellers",
    ],
}

# Regex to extract potential ticker symbols from text
# Matches: $AAPL, AAPL:, "AAPL", (AAPL), NYSE:AAPL, NASDAQ:AAPL
_TICKER_PATTERN = re.compile(
    r"""
    (?:
        \$([A-Z]{1,6})               # $AAPL
        | (?:NYSE|NASDAQ|AMEX):([A-Z]{1,6})  # NYSE:AAPL
        | \b([A-Z]{2,6})\b           # bare AAPL (2-6 uppercase letters)
    )
    """,
    re.VERBOSE,
)

# Noise words that look like tickers but aren't
_TICKER_NOISE = frozenset([
    "THE", "AND", "FOR", "NOT", "BUT", "NEW", "ALL", "ARE", "BY", "IF",
    "OF", "TO", "IN", "IS", "IT", "BE", "AS", "AT", "OR", "AN", "UP",
    "DO", "SO", "NO", "WE", "MY", "HE", "SHE", "US", "CEO", "CFO",
    "COO", "CTO", "IPO", "ETF", "INC", "LLC", "LTD", "CO", "SEC",
    "GET", "GOT", "SET", "OUT", "HAS", "HAD", "CAN", "DID", "FED",
    "GDP", "CPI", "IMF", "WHO", "CDC", "COVID", "AI", "EV", "EPS",
    "Q1", "Q2", "Q3", "Q4", "YOY", "QOQ", "TTM", "TBD",
])


def _extract_tickers(text: str, cfg) -> List[str]:
    """Extract potential ticker symbols from a news headline/body."""
    found = set()
    for m in _TICKER_PATTERN.finditer(text):
        ticker = m.group(1) or m.group(2) or m.group(3)
        if not ticker:
            continue
        ticker = ticker.strip().upper()
        if (ticker in _TICKER_NOISE
                or len(ticker) < cfg.min_ticker_length
                or len(ticker) > cfg.max_ticker_length):
            continue
        found.add(ticker)
    return sorted(found)


def _score_catalyst(headline: str, url: str = "") -> Tuple[str, float]:
    """
    Classify a headline into a catalyst type and return its score.
    Falls through to GENERAL if no specific keywords match.
    """
    text = headline.lower()
    cfg  = CONFIG.news

    score_map = {
        CATALYST_EARNINGS: cfg.catalyst_score_earnings,
        CATALYST_FDA:      cfg.catalyst_score_fda,
        CATALYST_MERGER:   cfg.catalyst_score_merger,
        CATALYST_SQUEEZE:  cfg.catalyst_score_squeeze,
        CATALYST_GENERAL:  cfg.catalyst_score_general,
    }

    for catalyst_type, keywords in _CATALYST_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            score = min(score_map[catalyst_type], cfg.max_news_score_boost)
            return catalyst_type, score

    return CATALYST_GENERAL, min(score_map[CATALYST_GENERAL], cfg.max_news_score_boost)


# ─────────────────────────────────────────────────────────────────────────────
# FINNHUB CLIENT  (lazily imported — graceful if requests not installed)
# ─────────────────────────────────────────────────────────────────────────────

def _finnhub_key() -> str:
    """Resolve Finnhub API key from config or environment."""
    return CONFIG.news.finnhub_api_key or os.environ.get("FINNHUB_API_KEY", "")


def _fetch_general_news(api_key: str, category: str = "general") -> List[dict]:
    """GET /news from Finnhub.  Returns raw JSON list or []."""
    try:
        import requests
        url  = "https://finnhub.io/api/v1/news"
        resp = requests.get(url, params={"category": category, "token": api_key},
                            timeout=8)
        resp.raise_for_status()
        return resp.json() or []
    except Exception as exc:
        log.warning(f"[NewsIngestor] general news fetch failed: {exc}")
        return []


def _fetch_company_news(ticker: str, api_key: str, from_date: str, to_date: str) -> List[dict]:
    """GET /company-news from Finnhub for a specific ticker."""
    try:
        import requests
        url  = "https://finnhub.io/api/v1/company-news"
        resp = requests.get(url, params={
            "symbol": ticker, "from": from_date,
            "to": to_date, "token": api_key,
        }, timeout=8)
        resp.raise_for_status()
        return resp.json() or []
    except Exception as exc:
        log.debug(f"[NewsIngestor] company news fetch failed for {ticker}: {exc}")
        return []


def _parse_articles(articles: List[dict]) -> List[Tuple[str, str, datetime, str]]:
    """
    Parse raw Finnhub articles into (headline, url, published_at, summary).
    Handles both /news and /company-news response shapes.
    """
    parsed = []
    for a in articles:
        headline = a.get("headline", "") or a.get("title", "")
        url      = a.get("url", "")
        ts       = a.get("datetime", 0) or a.get("publishedAt", 0)
        summary  = a.get("summary", "")

        if not headline:
            continue
        try:
            published = datetime.fromtimestamp(int(ts), tz=ET)
        except (ValueError, TypeError, OSError):
            published = datetime.now(ET)

        parsed.append((headline, url, published, summary))
    return parsed


# ─────────────────────────────────────────────────────────────────────────────
# MAIN INGESTOR CLASS
# ─────────────────────────────────────────────────────────────────────────────

class NewsIngestor:
    """
    Non-blocking Finnhub news ingestor.

    Usage:
        ingestor = NewsIngestor()
        candidates = ingestor.get_candidates()   # always instant — uses cache

    The ingestor runs its own background polling thread.
    Failures are silently degraded; callers always receive the last
    successful fetch (empty list if no fetch has succeeded yet).
    """

    def __init__(self) -> None:
        self._cfg      = CONFIG.news
        self._lock     = threading.Lock()
        self._cache:   List[NewsCandidate] = []
        self._last_fetch: Optional[datetime] = None
        self._fetch_count: int = 0
        self._error_count: int = 0
        self._thread:  Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    # ── Public API ──────────────────────────────────────────────────────────

    def get_candidates(self, max_age_minutes: int = None) -> List[NewsCandidate]:
        """
        Return cached news candidates.  Always instant — never blocks.
        Filters to only fresh candidates (within max_headline_age_minutes).
        """
        with self._lock:
            limit = max_age_minutes or self._cfg.max_headline_age_minutes
            return [c for c in self._cache if c.is_fresh(limit)]

    def get_tickers(self, max_age_minutes: int = None) -> List[str]:
        """Convenience: unique tickers from fresh candidates."""
        seen = dict()  # ticker → highest score
        for c in self.get_candidates(max_age_minutes):
            if c.ticker not in seen or c.news_score > seen[c.ticker]:
                seen[c.ticker] = c.news_score
        return list(seen.keys())

    def get_ticker_score(self, ticker: str) -> float:
        """Return the highest news score for a ticker across all fresh headlines."""
        scores = [c.news_score for c in self.get_candidates() if c.ticker == ticker]
        return max(scores, default=0.0)

    def start(self) -> None:
        """Start background polling thread."""
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._poll_loop, daemon=True, name="NewsIngestor"
        )
        self._thread.start()
        log.info("[NewsIngestor] Background polling thread started.")

    def stop(self) -> None:
        """Stop background polling thread."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        log.info("[NewsIngestor] Polling thread stopped.")

    def fetch_once(self) -> List[NewsCandidate]:
        """
        Synchronous fetch — for explicit calls or tests.
        Returns candidates and updates the cache.
        """
        api_key = _finnhub_key()
        if not api_key:
            log.warning("[NewsIngestor] No FINNHUB_API_KEY configured — news disabled.")
            return []

        raw = _fetch_general_news(api_key, category="general")
        # Also pull from a second category
        raw += _fetch_general_news(api_key, category="forex")

        candidates = self._process_articles(raw)

        with self._lock:
            self._cache       = candidates
            self._last_fetch  = datetime.now(ET)
            self._fetch_count += 1

        log.info(
            f"[NewsIngestor] Fetched {len(raw)} articles → "
            f"{len(candidates)} candidates from {len(set(c.ticker for c in candidates))} tickers"
        )
        return candidates

    def status(self) -> dict:
        """Return diagnostic snapshot."""
        with self._lock:
            return {
                "cached_candidates": len(self._cache),
                "unique_tickers":    len({c.ticker for c in self._cache}),
                "last_fetch":        self._last_fetch.isoformat() if self._last_fetch else None,
                "fetch_count":       self._fetch_count,
                "error_count":       self._error_count,
            }

    # ── Internal ────────────────────────────────────────────────────────────

    def _poll_loop(self) -> None:
        """Background thread: polls Finnhub at configured interval."""
        while not self._stop_event.is_set():
            try:
                self.fetch_once()
            except Exception as exc:
                with self._lock:
                    self._error_count += 1
                log.warning(f"[NewsIngestor] poll_loop error: {exc}")
            self._stop_event.wait(timeout=self._cfg.poll_interval_seconds)

    def _process_articles(self, articles: List[dict]) -> List[NewsCandidate]:
        """Parse raw articles → deduplicated NewsCandidate list."""
        cfg        = self._cfg
        candidates = []
        seen_ticker_url: set = set()

        for headline, url, published_at, summary in _parse_articles(articles):
            age_min = (datetime.now(ET) - published_at).total_seconds() / 60
            if age_min > cfg.max_headline_age_minutes:
                continue

            # Build text corpus for ticker extraction + catalyst scoring
            corpus  = f"{headline} {summary}"
            tickers = _extract_tickers(corpus, cfg)

            catalyst_type, score = _score_catalyst(headline, url)

            for ticker in tickers[:cfg.max_news_tickers]:
                key = (ticker, url)
                if key in seen_ticker_url:
                    continue
                seen_ticker_url.add(key)

                candidates.append(NewsCandidate(
                    ticker        = ticker,
                    headline      = headline[:200],
                    catalyst_type = catalyst_type,
                    news_score    = round(score, 4),
                    published_at  = published_at,
                    url           = url,
                ))

        # deduplicate: keep highest-score per ticker
        deduped: Dict[str, NewsCandidate] = {}
        for c in candidates:
            if c.ticker not in deduped or c.news_score > deduped[c.ticker].news_score:
                deduped[c.ticker] = c

        return list(deduped.values())


# ─────────────────────────────────────────────────────────────────────────────
# Module-level singleton
# ─────────────────────────────────────────────────────────────────────────────
news_ingestor = NewsIngestor()
