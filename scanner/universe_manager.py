"""
scanner/universe_manager.py — Dynamic expiring candidate pool.

The UniverseManager maintains a live pool of tickers that have shown
meaningful price/volume activity. Every engine cycle:

  1. Discovery scanner pushes updates (DemandMetrics).
  2. NewsIngestor pushes tickers with news-catalyst scores.
  3. AlpacaTracker updates tape-confirmation status.
  4. Stale entries (inactive > expiry_minutes) are pruned.
  5. top_n() returns a ranked Top-15 snapshot.

Invariants:
  - Pool NEVER blocks discovery — any DemandMetrics update is accepted.
  - News score is a capped MODIFIER only; never replaces demand_score.
  - composite_score = demand_score + min(news_score, news_score_cap)
  - top_n() always returns a list (even if all scores are zero).
  - Thread-safe (lock-protected).
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pytz

from config.constants import (
    DH_OK,
    SOURCE_DISCOVERY, SOURCE_MOVERS, SOURCE_NEWS, SOURCE_TRACKER,
)
from config.settings import CONFIG
from scanner.demand import DemandMetrics

log = logging.getLogger(__name__)
ET  = pytz.timezone("America/New_York")


# ─────────────────────────────────────────────────────────────────────────────
# CANDIDATE ENTRY
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class CandidateEntry:
    """State of a single ticker in the live universe pool."""
    ticker:           str
    source:           str              = SOURCE_DISCOVERY   # origin
    first_seen:       datetime         = field(default_factory=lambda: datetime.now(ET))
    last_updated:     datetime         = field(default_factory=lambda: datetime.now(ET))

    # Scores
    demand_score:     float            = 0.0
    news_score:       float            = 0.0    # capped modifier
    composite_score:  float            = 0.0    # demand + capped_news
    rank:             int              = 0       # position in Top-N list

    # Rich metrics snapshot (from last DemandMetrics update)
    last_price:       float            = 0.0
    dollar_volume:    float            = 0.0
    rvol:             float            = 0.0
    gap_pct:          float            = 0.0
    intraday_range_pct: float          = 0.0
    pct_change:       float            = 0.0

    # DATA_HEALTH
    dh_status:        str              = DH_OK
    dh_block_reason:  str              = ""
    feed_type:        str              = ""
    session:          str              = ""

    # Tape confirmation
    tape_confirmed:   bool             = False

    # Expiry
    expiry:           Optional[datetime] = None

    def refresh_expiry(self, extra_minutes: int = None) -> None:
        """Extend expiry from now by the configured window."""
        cfg = CONFIG.universe
        minutes = extra_minutes or cfg.expiry_minutes
        self.expiry = datetime.now(ET) + timedelta(minutes=minutes)

    def is_expired(self, now: datetime = None) -> bool:
        now = now or datetime.now(ET)
        if self.expiry is None:
            return False
        return now > self.expiry

    def update_composite(self) -> None:
        """
        Recompute score with a small movers-origin boost.
        """
        cap  = CONFIG.universe.news_score_cap
        wt   = CONFIG.universe.news_weight
        mover_boost = 0.2 if self.source == SOURCE_MOVERS else 0.0
        self.composite_score = round(
            self.demand_score + min(self.news_score * wt, cap) + mover_boost,
            4,
        )

    def to_dict(self) -> dict:
        return {
            "ticker":          self.ticker,
            "source":          self.source,
            "composite_score": self.composite_score,
            "demand_score":    self.demand_score,
            "news_score":      self.news_score,
            "rank":            self.rank,
            "last_price":      self.last_price,
            "dollar_volume":   self.dollar_volume,
            "rvol":            self.rvol,
            "gap_pct":         self.gap_pct,
            "pct_change":      self.pct_change,
            "dh_status":       self.dh_status,
            "tape_confirmed":  self.tape_confirmed,
            "feed_type":       self.feed_type,
            "session":         self.session,
            "last_updated":    self.last_updated.isoformat(),
        }


# ─────────────────────────────────────────────────────────────────────────────
# UNIVERSE MANAGER
# ─────────────────────────────────────────────────────────────────────────────

class UniverseManager:
    """
    Manages the live, expiring candidate pool.

    Thread-safe.  The engine loop calls update_from_metrics() every cycle;
    the result is consumed by the DecisionEngine via top_n().
    """

    def __init__(self) -> None:
        self._pool:  Dict[str, CandidateEntry] = {}
        self._lock   = threading.Lock()
        self._cfg    = CONFIG.universe

    # ── Write path ──────────────────────────────────────────────────────────

    def update_from_metrics(
        self,
        metrics: DemandMetrics,
        movers_meta: Optional[Dict] = None,
    ) -> CandidateEntry:
        """
        Upsert a CandidateEntry from DemandMetrics.
        Called by the discovery scanner each cycle.
        Always succeeds — never rejects discovery data.
        """
        now  = datetime.now(ET)
        dh   = getattr(metrics, "_dh_report", None)
        feed = getattr(metrics, "_feed_type", "")
        movers_meta = movers_meta or {}

        with self._lock:
            entry = self._pool.get(metrics.ticker)
            if entry is None:
                entry = CandidateEntry(
                    ticker     = metrics.ticker,
                    source     = SOURCE_DISCOVERY,
                    first_seen = now,
                )
                self._pool[metrics.ticker] = entry

            entry.last_updated       = now
            entry.demand_score       = round(metrics.demand_score, 4)
            entry.last_price         = metrics.last_price
            entry.dollar_volume      = metrics.dollar_volume
            entry.rvol               = metrics.rvol
            entry.gap_pct            = metrics.gap_pct
            entry.intraday_range_pct = metrics.intraday_range_pct
            entry.feed_type          = feed
            entry.pct_change         = float(
                getattr(movers_meta.get(metrics.ticker), "pct_change", 0.0) or 0.0
            )

            if entry.pct_change > 0:
                entry.source = SOURCE_MOVERS

            if dh is not None:
                entry.dh_status      = dh.status
                entry.dh_block_reason = dh.block_reason
                entry.session        = dh.session

            entry.update_composite()
            entry.refresh_expiry()

        return entry

    def update_news_score(self, ticker: str, news_score: float,
                          source: str = SOURCE_NEWS) -> CandidateEntry:
        """
        Apply or refresh the news-catalyst modifier for a ticker.
        Creates a shell entry if the ticker wasn't already discovered.
        """
        now = datetime.now(ET)
        cfg = self._cfg

        with self._lock:
            entry = self._pool.get(ticker)
            if entry is None:
                # Placeholder — will be promoted to full entry once discovery
                # returns data; expiry is controlled by news_expiry_minutes.
                entry = CandidateEntry(
                    ticker     = ticker,
                    source     = source,
                    first_seen = now,
                )
                self._pool[ticker] = entry

            # Store raw score — capping is applied in update_composite() to
            # prevent double-capping (min(...cap) applied again there).
            entry.news_score  = round(news_score, 4)
            entry.last_updated = now
            entry.update_composite()
            entry.refresh_expiry(extra_minutes=cfg.news_expiry_minutes)

        return entry

    def update_tape_result(self, ticker: str, tape_result) -> Optional[CandidateEntry]:
        """
        Mark tape confirmation status.  `tape_result` is a TapeResult from
        news/tracker.py.  Returns the entry or None if ticker not in pool.
        """
        with self._lock:
            entry = self._pool.get(ticker)
            if entry is None:
                return None
            entry.tape_confirmed = tape_result.promoted
            entry.feed_type      = tape_result.feed_type
            if tape_result.promoted:
                entry.source = SOURCE_TRACKER
            entry.last_updated = datetime.now(ET)
            entry.update_composite()
        return entry

    def expire_stale(self, now: datetime = None) -> List[str]:
        """
        Remove entries whose expiry has passed.
        Returns list of removed tickers.
        """
        now     = now or datetime.now(ET)
        removed = []

        with self._lock:
            stale = [t for t, e in self._pool.items() if e.is_expired(now)]
            for ticker in stale:
                del self._pool[ticker]
                removed.append(ticker)

        if removed:
            log.debug(f"[UniverseManager] Expired {len(removed)} tickers: {removed}")
        return removed

    def reset(self) -> None:
        """Clear the pool — call at session start / session end."""
        with self._lock:
            self._pool.clear()
        log.info("[UniverseManager] Pool reset.")

    # ── Read path ────────────────────────────────────────────────────────────

    def top_n(self, n: int = None, now: datetime = None) -> List[CandidateEntry]:
        """
        Return the Top-N ranked candidates by composite_score.
        Always returns a list — never raises even if pool is empty.
        Assigns rank to each returned entry (1 = best).
        Calls expire_stale() first to prevent unbounded pool growth.
        """
        n   = n or self._cfg.top_n
        now = now or datetime.now(ET)
        min_score = self._cfg.min_composite_score

        self.expire_stale(now)  # prune stale entries before ranking

        with self._lock:
            eligible = [
                e for e in self._pool.values()
                if not e.is_expired(now) and e.composite_score >= min_score
            ]

        ranked = sorted(
            eligible,
            key=lambda e: (
                e.composite_score,
                e.pct_change,
                e.rvol,
                e.last_updated.timestamp(),
                e.ticker,
            ),
            reverse=True,
        )
        top    = ranked[:n]

        for i, entry in enumerate(top, start=1):
            entry.rank = i

        return top

    def promote_from_movers(self, ticker: str, pct_change: float = 0.0) -> CandidateEntry:
        """
        Add a ticker sourced from the market-movers feed.

        Creates a shell CandidateEntry with source=SOURCE_MOVERS and no
        score override; the entry will be enriched once discovery data or
        tape confirmation arrives. If the ticker is already in the pool
        (discovered via another path) its existing entry is returned
        unchanged so scores are never clobbered.
        """
        now = datetime.now(ET)
        with self._lock:
            entry = self._pool.get(ticker)
            if entry is not None:
                entry.pct_change = max(entry.pct_change, float(pct_change or 0.0))
                if entry.pct_change > 0:
                    entry.source = SOURCE_MOVERS
                entry.last_updated = now
                entry.refresh_expiry(extra_minutes=CONFIG.movers.expiry_minutes)
                entry.update_composite()
                return entry  # already tracked — do not override scores

            entry = CandidateEntry(
                ticker     = ticker,
                source     = SOURCE_MOVERS,
                first_seen = now,
                last_updated = now,
                pct_change = float(pct_change or 0.0),
            )
            entry.refresh_expiry(extra_minutes=CONFIG.movers.expiry_minutes)
            self._pool[ticker] = entry
            log.debug(f"[UniverseManager] Movers intake: {ticker}")

        return entry

    def get(self, ticker: str) -> Optional[CandidateEntry]:
        """Retrieve a specific entry or None."""
        with self._lock:
            return self._pool.get(ticker)

    def is_top_n(self, ticker: str, n: int = None) -> bool:
        """Return True if ticker is currently in the Top-N."""
        top = self.top_n(n)
        return any(e.ticker == ticker for e in top)

    def pool_size(self) -> int:
        """Number of non-expired entries in the pool."""
        now = datetime.now(ET)
        with self._lock:
            return sum(1 for e in self._pool.values() if not e.is_expired(now))

    def snapshot(self, n: int = None) -> List[dict]:
        """
        Return a JSON-serialisable snapshot of the Top-N.
        Used by the explainability logger.
        """
        return [e.to_dict() for e in self.top_n(n)]

    def full_pool_snapshot(self) -> List[dict]:
        """Snapshot of the entire pool (including below Top-N)."""
        with self._lock:
            return [e.to_dict() for e in self._pool.values()]


# ─────────────────────────────────────────────────────────────────────────────
# Module-level singleton
# ─────────────────────────────────────────────────────────────────────────────
universe_manager = UniverseManager()
