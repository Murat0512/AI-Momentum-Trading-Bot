"""
cache.py — In-memory bar cache for the trading session.

Stores MTF bar dicts keyed by ticker. Updated each engine cycle.
Provides staleness checking and incremental 1m bar appending.
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime
from typing import Dict, Optional

import pandas as pd
import pytz

from config.constants import TF_1M

log = logging.getLogger(__name__)
ET = pytz.timezone("America/New_York")


class BarCache:
    """
    Thread-safe in-memory cache.

    Structure:
        _store[ticker]["bars"]     = Dict[timeframe, pd.DataFrame]
        _store[ticker]["fetched_at"] = datetime (ET)
    """

    def __init__(self):
        self._store: Dict[str, dict] = {}
        self._lock = threading.Lock()

    # ── Write ────────────────────────────────────────────────────────────────

    def set(self, ticker: str, mtf_bars: Dict[str, pd.DataFrame]) -> None:
        """Store full MTF bar set for a ticker."""
        with self._lock:
            self._store[ticker] = {
                "bars": mtf_bars,
                "fetched_at": datetime.now(ET),
            }

    def append_1m_bar(self, ticker: str, bar: pd.Series) -> None:
        """
        Append a single 1m bar to the cache and invalidate higher-TF bars.
        Caller is responsible for re-running pipeline after appending.
        """
        with self._lock:
            entry = self._store.get(ticker)
            if entry is None or TF_1M not in entry["bars"]:
                log.debug(f"[{ticker}] append_1m_bar: no existing cache, skipping")
                return
            df = entry["bars"][TF_1M]

            # Fast path: in-place single-row insert/update avoids full-frame concat.
            # If timestamp exists, this replaces the existing row; otherwise appends.
            row = bar.reindex(df.columns)
            df.loc[bar.name, df.columns] = row.values
            if not df.index.is_monotonic_increasing:
                df.sort_index(inplace=True)

            entry["fetched_at"] = datetime.now(ET)

    # ── Read ─────────────────────────────────────────────────────────────────

    def get(self, ticker: str) -> Optional[Dict[str, pd.DataFrame]]:
        """Return MTF bar dict or None if not cached."""
        with self._lock:
            entry = self._store.get(ticker)
            return entry["bars"] if entry else None

    def get_tf(self, ticker: str, timeframe: str) -> Optional[pd.DataFrame]:
        """Return a single timeframe DataFrame or None."""
        bars = self.get(ticker)
        if bars is None:
            return None
        return bars.get(timeframe)

    def fetched_at(self, ticker: str) -> Optional[datetime]:
        with self._lock:
            entry = self._store.get(ticker)
            return entry["fetched_at"] if entry else None

    def is_stale(self, ticker: str, max_age_seconds: int = 60) -> bool:
        """True if cache entry is older than max_age_seconds."""
        ts = self.fetched_at(ticker)
        if ts is None:
            return True
        age = (datetime.now(ET) - ts).total_seconds()
        return age > max_age_seconds

    def all_tickers(self) -> list[str]:
        with self._lock:
            return list(self._store.keys())

    # ── Invalidate ───────────────────────────────────────────────────────────

    def evict(self, ticker: str) -> None:
        with self._lock:
            self._store.pop(ticker, None)

    def clear(self) -> None:
        with self._lock:
            self._store.clear()
        log.info("BarCache: cleared all entries")

    # ── Diagnostics ──────────────────────────────────────────────────────────

    def summary(self) -> dict:
        with self._lock:
            summary = {}
            for ticker, entry in self._store.items():
                summary[ticker] = {
                    tf: len(df) for tf, df in entry["bars"].items() if df is not None
                }
            return summary


# Module-level singleton
bar_cache = BarCache()
