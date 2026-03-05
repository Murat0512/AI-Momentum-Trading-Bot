"""
scanner/movers.py — Market movers candidate intake (Webull-style discovery feed).

Pulls Top Gainers / Most Active tickers from Alpaca's screener endpoint and
injects them as *candidate tickers only* into the Discovery pipeline.

Integration rules (non-negotiable)
------------------------------------
  • Movers candidates enter the same scan pool as seed tickers — they must pass
    all hard filters (price, dollar-volume, spread, health, halt gate).
  • Movers candidates NEVER bypass DemandScore ranking.
  • Movers candidates NEVER force a trade.
  • Candidates expire after MoversConfig.expiry_minutes (TTL).
  • Deduplication is automatic: movers tickers are merged via set-union with
    the existing seed list so no ticker is fetched twice.

Alpaca screener endpoint (free tier)
--------------------------------------
  GET https://data.alpaca.markets/v2/screener/stocks/movers
  Headers: APCA-API-KEY-ID, APCA-API-SECRET-KEY
  Returns: { "gainers": [...], "losers": [...], "most_actives": [...] }

Usage
------
  from scanner.movers import movers_ingestor

  # each engine cycle (in universe.scan()):
  movers_ingestor.fetch()                         # refresh store (throttled)
  extra_tickers = movers_ingestor.active_tickers() # merge with seed list
"""

from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pytz

from config.settings import CONFIG

log = logging.getLogger(__name__)
ET = pytz.timezone("America/New_York")

_ALPACA_GAINERS_URL = "https://data.alpaca.markets/v1beta1/screener/stocks/movers"
_ALPACA_MOST_ACTIVE_URL = (
    "https://data.alpaca.markets/v1beta1/screener/stocks/most-actives"
)


# ─────────────────────────────────────────────────────────────────────────────
# Data types
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class MoversCandidate:
    """A single ticker surfaced by the movers/most-active feed."""

    ticker: str
    rank: int  # position in its source list (1 = highest)
    pct_change: float  # % price move that put it on the list
    volume: int  # current session volume at fetch time
    last_price: float  # last traded price at fetch time
    source: str  # "top_gainers" | "most_active"
    pct_change_rank: int = 0
    volume_rank: int = 0
    in_both_lists: bool = False
    priority_rank: int = 10_000
    fetched_at: datetime = field(default_factory=lambda: datetime.now(ET))

    def age_seconds(self, now: Optional[datetime] = None) -> float:
        now = now or datetime.now(ET)
        fa = self.fetched_at
        if fa.tzinfo is None:
            fa = ET.localize(fa)
        return (now - fa).total_seconds()

    def is_expired(
        self,
        expiry_seconds: Optional[int] = None,
        now: Optional[datetime] = None,
    ) -> bool:
        limit = (
            expiry_seconds
            if expiry_seconds is not None
            else CONFIG.movers.expiry_minutes * 60
        )
        return self.age_seconds(now) > limit


# ─────────────────────────────────────────────────────────────────────────────
# TTL store
# ─────────────────────────────────────────────────────────────────────────────


class MoversStore:
    """Thread-safe TTL cache of MoversCandidate objects, keyed by ticker."""

    def __init__(self) -> None:
        self._lock: threading.Lock = threading.Lock()
        self._store: Dict[str, MoversCandidate] = {}

    def upsert(self, candidates: List[MoversCandidate]) -> None:
        """Add or refresh candidates (deduplicates by ticker — latest wins)."""
        with self._lock:
            for c in candidates:
                existing = self._store.get(c.ticker)
                if existing is None:
                    self._store[c.ticker] = c
                    continue
                merged = MoversCandidate(
                    ticker=c.ticker,
                    rank=min(existing.rank, c.rank),
                    pct_change=max(existing.pct_change, c.pct_change),
                    volume=max(existing.volume, c.volume),
                    last_price=(
                        c.last_price if c.last_price > 0 else existing.last_price
                    ),
                    source=c.source,
                    pct_change_rank=(
                        min(
                            r
                            for r in [existing.pct_change_rank, c.pct_change_rank]
                            if r > 0
                        )
                        if (existing.pct_change_rank > 0 or c.pct_change_rank > 0)
                        else 0
                    ),
                    volume_rank=(
                        min(r for r in [existing.volume_rank, c.volume_rank] if r > 0)
                        if (existing.volume_rank > 0 or c.volume_rank > 0)
                        else 0
                    ),
                    in_both_lists=existing.in_both_lists or c.in_both_lists,
                    priority_rank=min(existing.priority_rank, c.priority_rank),
                    fetched_at=max(existing.fetched_at, c.fetched_at),
                )
                if merged.pct_change_rank > 0 and merged.volume_rank > 0:
                    merged.in_both_lists = True
                if merged.in_both_lists:
                    merged.priority_rank = merged.pct_change_rank + merged.volume_rank
                self._store[c.ticker] = merged

    def expire_stale(self, now: Optional[datetime] = None) -> List[str]:
        """Remove expired entries; returns list of removed tickers."""
        with self._lock:
            now = now or datetime.now(ET)
            expiry_s = CONFIG.movers.expiry_minutes * 60
            stale = [t for t, c in self._store.items() if c.is_expired(expiry_s, now)]
            for t in stale:
                del self._store[t]
            return stale

    def active_tickers(self, now: Optional[datetime] = None) -> List[str]:
        """Ticker symbols for all non-expired candidates."""
        now = now or datetime.now(ET)
        expiry_s = CONFIG.movers.expiry_minutes * 60
        with self._lock:
            active = [
                c for c in self._store.values() if not c.is_expired(expiry_s, now)
            ]
            active.sort(
                key=lambda c: (
                    0 if c.in_both_lists else 1,
                    c.priority_rank,
                    c.pct_change_rank if c.pct_change_rank > 0 else 10_000,
                    c.volume_rank if c.volume_rank > 0 else 10_000,
                    c.ticker,
                )
            )
            return [c.ticker for c in active]

    def all_candidates(self, now: Optional[datetime] = None) -> List[MoversCandidate]:
        """All non-expired MoversCandidate objects."""
        now = now or datetime.now(ET)
        expiry_s = CONFIG.movers.expiry_minutes * 60
        with self._lock:
            return [c for c in self._store.values() if not c.is_expired(expiry_s, now)]

    def clear(self) -> None:
        with self._lock:
            self._store.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._store)


# ─────────────────────────────────────────────────────────────────────────────
# Ingestor
# ─────────────────────────────────────────────────────────────────────────────


class MoversIngestor:
    """
    Fetches top-gainers / most-active tickers from Alpaca screener on a
    throttled schedule and maintains a TTL store of live candidates.

    Call `fetch()` each engine cycle (it self-throttles to
    MoversConfig.poll_interval_seconds).

    Call `active_tickers()` to get the current non-expired movers list
    for inclusion in the universe scan pool.

    Network I/O is isolated in `_fetch_from_alpaca()` so tests can
    monkeypatch or subclass without mocking the network layer.
    """

    def __init__(self, store: Optional[MoversStore] = None) -> None:
        self._store: MoversStore = store or MoversStore()
        self._last_fetch: Optional[datetime] = None

    # ── Public API ────────────────────────────────────────────────────────────

    def fetch(self, now: Optional[datetime] = None) -> List[MoversCandidate]:
        """
        Refresh the movers store.  Throttled to
        MoversConfig.poll_interval_seconds.

        Returns the newly fetched candidates (empty list on skip or error).
        Never raises — errors are logged and an empty list returned so
        the discovery pipeline degrades gracefully.
        """
        if not CONFIG.movers.enabled:
            return []

        now = now or datetime.now(ET)
        if self._last_fetch:
            elapsed = (now - self._last_fetch).total_seconds()
            if elapsed < CONFIG.movers.poll_interval_seconds:
                return []

        candidates = self._fetch_from_alpaca(now)
        self._store.upsert(candidates)
        self._store.expire_stale(now)
        self._last_fetch = now

        if candidates:
            log.info(
                f"[Movers] Fetched {len(candidates)} candidates: "
                + ", ".join(c.ticker for c in candidates[:10])
                + ("..." if len(candidates) > 10 else "")
            )
        return candidates

    def active_tickers(self, now: Optional[datetime] = None) -> List[str]:
        """Non-expired movers ticker symbols for the next scan cycle."""
        now = now or datetime.now(ET)
        if tv_candidates:
            self._store.upsert(tv_candidates)
        self._store.expire_stale(now)
        return self._store.active_tickers(now)

    def all_candidates(self, now: Optional[datetime] = None) -> List[MoversCandidate]:
        """All non-expired MoversCandidate objects."""
        return self._store.all_candidates(now)

    def reset(self) -> None:
        """Clear store + fetch timestamp (call at session start)."""
        self._store.clear()
        self._last_fetch = None

    # ── Internal — override in tests ─────────────────────────────────────────

    def _fetch_from_alpaca(self, now: datetime) -> List[MoversCandidate]:
        """
        HTTP GET to Alpaca screener endpoint.
        Returns parsed candidates or [] if keys missing / API unavailable.
        """
        api_key = CONFIG.data.alpaca_api_key or ""
        secret_key = CONFIG.data.alpaca_secret_key or ""
        if not api_key or not secret_key:
            log.debug("[Movers] Alpaca keys not set — skipping movers fetch")
            return []

        try:
            import requests

            headers = {
                "APCA-API-KEY-ID": api_key,
                "APCA-API-SECRET-KEY": secret_key,
            }
            top = int(CONFIG.movers.max_tickers)

            # Activity-first source of truth
            resp_a = requests.get(
                _ALPACA_MOST_ACTIVE_URL,
                headers=headers,
                params={"top": top},
                timeout=10,
            )
            resp_a.raise_for_status()
            active_payload = resp_a.json() or {}

            # Momentum enrichment (optional)
            gainers_payload: dict = {"gainers": []}
            try:
                resp_g = requests.get(
                    _ALPACA_GAINERS_URL,
                    headers=headers,
                    params={"top": top},
                    timeout=10,
                )
                resp_g.raise_for_status()
                gainers_payload = resp_g.json() or {"gainers": []}
            except Exception as exc:
                log.warning(
                    "[Movers] top-gainers fetch failed; continuing with most-active only: %s",
                    exc,
                )

            return self._parse_response(gainers_payload, active_payload, now)
        except Exception as exc:
            log.warning(f"[Movers] most-active fetch error: {exc}")
            return []

    def _parse_response(
        self, gainers_data: dict, most_active_data: dict, now: datetime
    ) -> List[MoversCandidate]:
        """
        Parse Alpaca screener response into MoversCandidate objects.

        Expected Alpaca format::

            {
              "gainers":      [{"symbol": "TSLA", "percent_change": 5.2,
                                "volume": 1234567, "price": 251.0}, ...],
              "most_actives": [{"symbol": "NVDA", ...}, ...]
            }
        """
        cfg = CONFIG.movers
        candidates: List[MoversCandidate] = []

        def _parse_list(items: list, source: str, rank_field: str) -> None:
            for rank, item in enumerate(items[: cfg.max_tickers], start=1):
                ticker = item.get("symbol", "").upper().strip()
                if not ticker or len(ticker) > 6:
                    continue
                pct_change = float(item.get("percent_change", 0.0))
                volume = int(item.get("volume", 0))
                last_price = float(item.get("price", 0.0))
                # Apply min_pct_change filter
                if (
                        volume=volume,
                        last_price=last_price,
                        source=source,
            _parse_list(gainers_data.get("gainers", []), "top_gainers", "pct_change")
        if cfg.include_rvol_gainers:
            _parse_list(
                gainers_data.get("gainers", []),
                "top_gainers",
                "pct_change",
            )
        if cfg.include_most_active:
            _parse_list(
                most_active_data.get("most_actives", []),
                "most_active",
                "volume",
            )

        merged: Dict[str, MoversCandidate] = {}
        for c in candidates:
            prev = merged.get(c.ticker)
            if prev is None:
                merged[c.ticker] = c
                continue
            c.pct_change_rank = (
                min(prev.pct_change_rank, c.pct_change_rank)
                if prev.pct_change_rank > 0 and c.pct_change_rank > 0
                else (prev.pct_change_rank or c.pct_change_rank)
            )
            c.volume_rank = (
                min(prev.volume_rank, c.volume_rank)
                if prev.volume_rank > 0 and c.volume_rank > 0
                else (prev.volume_rank or c.volume_rank)
            )
            c.in_both_lists = c.pct_change_rank > 0 and c.volume_rank > 0
            c.priority_rank = (
                c.pct_change_rank + c.volume_rank if c.in_both_lists else 10_000
            )
            merged[c.ticker] = c

        out = list(merged.values())
        out.sort(
            key=lambda c: (
                0 if c.in_both_lists else 1,
                0 if c.source == "most_active" else 1,
                c.volume_rank if c.volume_rank > 0 else 10_000,
                c.pct_change_rank if c.pct_change_rank > 0 else 10_000,
                c.priority_rank,
                c.ticker,
            )
        )
        return out

    def _drain_tradingview_queue(self, now: datetime) -> List[MoversCandidate]:
        """
        Drain TradingView webhook queue written by tools/tradingview_webhook_receiver.py.

        Queue format: one JSON object per line. Required: ticker/symbol field.
        Each consumed payload becomes a MoversCandidate source="tradingview_webhook".
        """
        queue_path = _TV_QUEUE_PATH
        if not queue_path.exists():
            return []

        queue_path.parent.mkdir(parents=True, exist_ok=True)
        processing_path = queue_path.with_suffix(queue_path.suffix + ".processing")

        try:
            queue_path.replace(processing_path)
        except OSError:
            return []

        candidates: List[MoversCandidate] = []
        seen: set[str] = set()
        try:
            with open(processing_path, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        payload = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    ticker = (
                        str(
                            payload.get("ticker")
                            or payload.get("symbol")
                            or payload.get("tv_ticker")
                            or ""
                        )
                        .upper()
                        .strip()
                    )
                    if not ticker or len(ticker) > 6:
                        continue
                    if ticker in seen:
                        continue
                    seen.add(ticker)

                    candidates.append(
                        MoversCandidate(
                            ticker=ticker,
                            rank=1,
                            pct_change=float(payload.get("pct_change", 0.0) or 0.0),
                            volume=int(float(payload.get("volume", 0) or 0)),
                            last_price=float(payload.get("price", 0.0) or 0.0),
                            source="tradingview_webhook",
                            fetched_at=now,
                        )
                    )
        finally:
            try:
                processing_path.unlink(missing_ok=True)
            except OSError:
                pass

        if candidates:
            log.info(
                f"[Movers] Ingested {len(candidates)} TradingView webhook candidates: "
                + ", ".join(c.ticker for c in candidates[:10])
                + ("..." if len(candidates) > 10 else "")
            )
        return candidates


# Module-level singleton — import this everywhere
movers_ingestor = MoversIngestor()
