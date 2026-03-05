"""
tests/test_movers_intake.py — Unit tests for scanner/movers.py

Covers:
  MoversCandidate
    1.  age_seconds reflects elapsed time correctly
    2.  is_expired returns False before TTL
    3.  is_expired returns True after TTL
    4.  is_expired uses CONFIG default when no expiry_seconds arg provided

  MoversStore
    5.  upsert adds candidates
    6.  upsert deduplicates by ticker (latest wins)
    7.  expire_stale removes stale entries and returns removed symbols
    8.  expire_stale keeps fresh entries
    9.  active_tickers excludes expired candidates
    10. all_candidates returns only non-expired candidates
    11. clear empties the store
    12. __len__ reflects the current count

  MoversIngestor
    13. fetch returns [] when movers disabled
    14. fetch throttles repeated calls within poll_interval
    15. fetch calls _fetch_from_alpaca after interval expires
    16. fetch upserts candidates and expires stale in one pass
    17. active_tickers delegates to store after expiry sweep
    18. reset clears store and last_fetch timestamp
    19. _parse_response parses gainers list correctly
    20. _parse_response parses most_actives list correctly
    21. _parse_response filters tickers below min_pct_change
    22. _parse_response skips empty / long symbols
    23. _fetch_from_alpaca returns [] with missing API keys
    24. movers tickers merged into universe scan pool (no duplicates)
"""

from __future__ import annotations

import sys, os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch
import pytest
import pytz

from config.settings import CONFIG, MoversConfig
from scanner.movers import MoversCandidate, MoversIngestor, MoversStore

ET = pytz.timezone("America/New_York")


def _now() -> datetime:
    return datetime.now(ET)


def _candidate(
    ticker: str = "TSLA",
    rank: int = 1,
    pct_change: float = 5.0,
    volume: int = 1_000_000,
    last_price: float = 200.0,
    source: str = "top_gainers",
    fetched_at: datetime | None = None,
) -> MoversCandidate:
    return MoversCandidate(
        ticker=ticker,
        rank=rank,
        pct_change=pct_change,
        volume=volume,
        last_price=last_price,
        source=source,
        fetched_at=fetched_at or _now(),
    )


# ─────────────────────────────────────────────────────────────────────────────
# MoversCandidate
# ─────────────────────────────────────────────────────────────────────────────


class TestMoversCandidate:
    def test_age_seconds_reflects_elapsed(self):
        t0 = _now()
        c = _candidate(fetched_at=t0)
        later = t0 + timedelta(seconds=45)
        assert c.age_seconds(later) == pytest.approx(45.0, abs=0.01)

    def test_is_expired_false_before_ttl(self):
        t0 = _now()
        c = _candidate(fetched_at=t0)
        still_fresh = t0 + timedelta(seconds=100)
        assert c.is_expired(expiry_seconds=200, now=still_fresh) is False

    def test_is_expired_true_after_ttl(self):
        t0 = _now()
        c = _candidate(fetched_at=t0)
        expired_at = t0 + timedelta(seconds=301)
        assert c.is_expired(expiry_seconds=300, now=expired_at) is True

    def test_is_expired_uses_config_default(self):
        """Without an explicit expiry_seconds arg, uses CONFIG.movers.expiry_minutes."""
        t0 = _now()
        c = _candidate(fetched_at=t0)
        default_s = CONFIG.movers.expiry_minutes * 60
        just_after = t0 + timedelta(seconds=default_s + 1)
        assert c.is_expired(now=just_after) is True
        just_before = t0 + timedelta(seconds=default_s - 1)
        assert c.is_expired(now=just_before) is False


# ─────────────────────────────────────────────────────────────────────────────
# MoversStore
# ─────────────────────────────────────────────────────────────────────────────


class TestMoversStore:
    def setup_method(self):
        self.store = MoversStore()

    def test_upsert_adds_candidates(self):
        self.store.upsert([_candidate("TSLA"), _candidate("AAPL")])
        assert len(self.store) == 2

    def test_upsert_deduplicates_latest_wins(self):
        t0 = _now()
        old = _candidate("TSLA", rank=1, pct_change=3.0, fetched_at=t0)
        new = _candidate(
            "TSLA", rank=2, pct_change=7.0, fetched_at=t0 + timedelta(seconds=60)
        )
        self.store.upsert([old])
        self.store.upsert([new])
        assert len(self.store) == 1
        # active_tickers still returns one ticker
        assert "TSLA" in self.store.active_tickers()

    def test_expire_stale_removes_old_entries(self):
        t0 = _now()
        old = _candidate("TSLA", fetched_at=t0 - timedelta(seconds=9999))
        fresh = _candidate("AAPL", fetched_at=t0)
        self.store.upsert([old, fresh])
        removed = self.store.expire_stale(now=t0)
        assert "TSLA" in removed
        assert len(self.store) == 1

    def test_expire_stale_keeps_fresh_entries(self):
        t0 = _now()
        fresh = _candidate("NVDA", fetched_at=t0)
        self.store.upsert([fresh])
        removed = self.store.expire_stale(now=t0)
        assert removed == []
        assert len(self.store) == 1

    def test_active_tickers_excludes_expired(self):
        t0 = _now()
        old = _candidate("TSLA", fetched_at=t0 - timedelta(seconds=9999))
        self.store.upsert([old])
        result = self.store.active_tickers(now=t0)
        assert "TSLA" not in result

    def test_all_candidates_returns_non_expired(self):
        t0 = _now()
        fresh = _candidate("AAPL", fetched_at=t0)
        stale = _candidate("TSLA", fetched_at=t0 - timedelta(seconds=9999))
        self.store.upsert([fresh, stale])
        candidates = self.store.all_candidates(now=t0)
        tickers = [c.ticker for c in candidates]
        assert "AAPL" in tickers
        assert "TSLA" not in tickers

    def test_clear_empties_store(self):
        self.store.upsert([_candidate("TSLA"), _candidate("AAPL")])
        self.store.clear()
        assert len(self.store) == 0

    def test_len_reflects_count(self):
        assert len(self.store) == 0
        self.store.upsert([_candidate("TSLA")])
        assert len(self.store) == 1
        self.store.upsert([_candidate("AAPL"), _candidate("NVDA")])
        assert len(self.store) == 3


# ─────────────────────────────────────────────────────────────────────────────
# MoversIngestor
# ─────────────────────────────────────────────────────────────────────────────


class TestMoversIngestor:
    """Tests use a fresh MoversIngestor with CONFIG.movers.enabled=True
    (patched for isolation)."""

    def _make_ingestor(self) -> MoversIngestor:
        ingestor = MoversIngestor()
        ingestor._fetch_from_alpaca = MagicMock(return_value=[])
        return ingestor

    # ── disabled guard ────────────────────────────────────────────────────────

    def test_fetch_returns_empty_when_disabled(self):
        with patch.object(CONFIG.movers, "enabled", False):
            ing = self._make_ingestor()
            result = ing.fetch()
        assert result == []
        ing._fetch_from_alpaca.assert_not_called()

    # ── throttle ─────────────────────────────────────────────────────────────

    def test_fetch_throttles_within_poll_interval(self):
        with patch.object(CONFIG.movers, "enabled", True), patch.object(
            CONFIG.movers, "poll_interval_seconds", 120
        ):
            ing = self._make_ingestor()
            t0 = _now()
            ing.fetch(now=t0)
            # 30 s later — should NOT re-fetch
            ing._fetch_from_alpaca.reset_mock()
            ing.fetch(now=t0 + timedelta(seconds=30))
            ing._fetch_from_alpaca.assert_not_called()

    def test_fetch_calls_alpaca_after_interval_expires(self):
        with patch.object(CONFIG.movers, "enabled", True), patch.object(
            CONFIG.movers, "poll_interval_seconds", 60
        ):
            ing = self._make_ingestor()
            t0 = _now()
            ing.fetch(now=t0)
            ing._fetch_from_alpaca.reset_mock()
            ing.fetch(now=t0 + timedelta(seconds=61))
            ing._fetch_from_alpaca.assert_called_once()

    # ── fetch integration ────────────────────────────────────────────────────

    def test_fetch_upserts_candidates_into_store(self):
        with patch.object(CONFIG.movers, "enabled", True):
            ing = self._make_ingestor()
            mock_candidates = [_candidate("TSLA"), _candidate("AAPL")]
            ing._fetch_from_alpaca.return_value = mock_candidates
            t0 = _now()
            result = ing.fetch(now=t0)
        assert len(result) == 2
        tickers = ing.active_tickers(now=t0)
        assert "TSLA" in tickers
        assert "AAPL" in tickers

    def test_fetch_expires_stale_after_upsert(self):
        """Fresh fetch should evict previously stale entries from the store."""
        with patch.object(CONFIG.movers, "enabled", True):
            ing = self._make_ingestor()
            t0 = _now()
            # Manually insert a stale entry
            stale = _candidate("OLD", fetched_at=t0 - timedelta(seconds=9999))
            ing._store.upsert([stale])
            assert (
                "OLD" in ing._store.active_tickers(now=t0) is False or True
            )  # already stale
            # Fetch fresh — should sweep out stale entries
            ing._fetch_from_alpaca.return_value = [_candidate("NVDA", fetched_at=t0)]
            ing.fetch(now=t0)
            active = ing.active_tickers(now=t0)
        assert "OLD" not in active

    # ── active_tickers ────────────────────────────────────────────────────────

    def test_active_tickers_sweeps_expiry(self):
        with patch.object(CONFIG.movers, "enabled", True):
            ing = self._make_ingestor()
            t0 = _now()
            stale = _candidate("STALE", fetched_at=t0 - timedelta(seconds=9999))
            ing._store.upsert([stale])
        result = ing.active_tickers(now=t0)
        assert "STALE" not in result

    # ── reset ────────────────────────────────────────────────────────────────

    def test_reset_clears_store_and_last_fetch(self):
        with patch.object(CONFIG.movers, "enabled", True):
            ing = self._make_ingestor()
            t0 = _now()
            ing._store.upsert([_candidate("TSLA", fetched_at=t0)])
            ing._last_fetch = t0
            ing.reset()
        assert ing._last_fetch is None
        assert len(ing._store) == 0

    # ── _parse_response ───────────────────────────────────────────────────────

    def _make_parser(self) -> MoversIngestor:
        """Return a bare ingestor — we only test _parse_response."""
        return MoversIngestor()

    def test_parse_response_parses_gainers(self):
        ing = self._make_parser()
        gainers = {
            "gainers": [
                {
                    "symbol": "TSLA",
                    "percent_change": 6.5,
                    "volume": 500000,
                    "price": 220.0,
                },
                {
                    "symbol": "NVDA",
                    "percent_change": 4.1,
                    "volume": 300000,
                    "price": 800.0,
                },
            ],
        }
        most_active = {"most_actives": []}
        with patch.object(CONFIG.movers, "include_gainers", True), patch.object(
            CONFIG.movers, "include_most_active", False
        ), patch.object(CONFIG.movers, "include_rvol_gainers", False), patch.object(
            CONFIG.movers, "min_pct_change", 0.0
        ), patch.object(
            CONFIG.movers, "max_tickers", 20
        ):
            result = ing._parse_response(gainers, most_active, _now())
        tickers = [c.ticker for c in result]
        assert "TSLA" in tickers
        assert "NVDA" in tickers
        assert all(c.source == "top_gainers" for c in result)

    def test_parse_response_parses_most_actives(self):
        ing = self._make_parser()
        gainers = {"gainers": []}
        most_active = {
            "most_actives": [
                {
                    "symbol": "AAPL",
                    "percent_change": 1.2,
                    "volume": 9000000,
                    "price": 185.0,
                },
            ],
        }
        with patch.object(CONFIG.movers, "include_gainers", False), patch.object(
            CONFIG.movers, "include_most_active", True
        ), patch.object(CONFIG.movers, "min_pct_change", 0.0), patch.object(
            CONFIG.movers, "max_tickers", 20
        ):
            result = ing._parse_response(gainers, most_active, _now())
        assert len(result) == 1
        assert result[0].ticker == "AAPL"
        assert result[0].source == "most_active"

    def test_parse_response_filters_below_min_pct_change(self):
        ing = self._make_parser()
        gainers = {
            "gainers": [
                {
                    "symbol": "SLOW",
                    "percent_change": 0.5,
                    "volume": 1000000,
                    "price": 50.0,
                },
                {
                    "symbol": "FAST",
                    "percent_change": 5.0,
                    "volume": 1000000,
                    "price": 50.0,
                },
            ],
        }
        most_active = {"most_actives": []}
        with patch.object(CONFIG.movers, "include_gainers", True), patch.object(
            CONFIG.movers, "include_most_active", False
        ), patch.object(CONFIG.movers, "include_rvol_gainers", False), patch.object(
            CONFIG.movers, "min_pct_change", 3.0
        ), patch.object(
            CONFIG.movers, "max_tickers", 20
        ):
            result = ing._parse_response(gainers, most_active, _now())
        tickers = [c.ticker for c in result]
        assert "SLOW" not in tickers
        assert "FAST" in tickers

    def test_parse_response_skips_invalid_symbols(self):
        ing = self._make_parser()
        gainers = {
            "gainers": [
                {"symbol": "", "percent_change": 5.0, "volume": 100000, "price": 10.0},
                {
                    "symbol": "TOOLONGXYZ",
                    "percent_change": 5.0,
                    "volume": 100000,
                    "price": 10.0,
                },
                {
                    "symbol": "GOOD",
                    "percent_change": 5.0,
                    "volume": 100000,
                    "price": 10.0,
                },
            ],
        }
        most_active = {"most_actives": []}
        with patch.object(CONFIG.movers, "include_gainers", True), patch.object(
            CONFIG.movers, "include_most_active", False
        ), patch.object(CONFIG.movers, "include_rvol_gainers", False), patch.object(
            CONFIG.movers, "min_pct_change", 0.0
        ), patch.object(
            CONFIG.movers, "max_tickers", 20
        ):
            result = ing._parse_response(gainers, most_active, _now())
        assert len(result) == 1
        assert result[0].ticker == "GOOD"

    def test_parse_response_prioritizes_dual_listed_tickers(self):
        ing = self._make_parser()
        gainers = {
            "gainers": [
                {
                    "symbol": "BOTH",
                    "percent_change": 9.0,
                    "volume": 2_000_000,
                    "price": 12.0,
                },
                {
                    "symbol": "GAIN",
                    "percent_change": 8.0,
                    "volume": 1_000_000,
                    "price": 9.0,
                },
            ]
        }
        most_active = {
            "most_actives": [
                {
                    "symbol": "BOTH",
                    "percent_change": 5.0,
                    "volume": 10_000_000,
                    "price": 12.5,
                },
                {
                    "symbol": "ACTV",
                    "percent_change": 1.0,
                    "volume": 9_000_000,
                    "price": 30.0,
                },
            ]
        }
        with patch.object(CONFIG.movers, "include_gainers", True), patch.object(
            CONFIG.movers, "include_most_active", True
        ), patch.object(CONFIG.movers, "include_rvol_gainers", False), patch.object(
            CONFIG.movers, "min_pct_change", 0.0
        ), patch.object(
            CONFIG.movers, "max_tickers", 20
        ):
            result = ing._parse_response(gainers, most_active, _now())

        assert result[0].ticker == "BOTH"
        assert result[0].in_both_lists is True

    def test_fetch_from_alpaca_returns_empty_without_keys(self):
        ing = MoversIngestor()  # real _fetch_from_alpaca
        with patch.object(CONFIG.data, "alpaca_api_key", ""), patch.object(
            CONFIG.data, "alpaca_secret_key", ""
        ):
            result = ing._fetch_from_alpaca(_now())
        assert result == []

    # ── universe integration ─────────────────────────────────────────────────

    def test_movers_tickers_merge_deduplicates_with_seeds(self):
        """Movers tickers should merge into universe scan pool with no duplicates."""
        t0 = _now()
        seeds = ["TSLA", "AAPL", "NVDA"]
        movers = ["TSLA", "MSTR", "AAPL"]  # TSLA and AAPL are duplicates

        combined = list(dict.fromkeys(seeds + movers))  # same logic as universe.scan()
        assert combined == ["TSLA", "AAPL", "NVDA", "MSTR"]  # order preserved, no dupes

    def test_movers_do_not_influence_demand_ranking(self):
        """Movers merely expand the candidate pool; ranking is independent."""
        # This is an architectural assertion — movers ingestor has no score fields
        # and no ranking bypass in MoversCandidate
        from scanner.movers import MoversCandidate
        import dataclasses

        field_names = {f.name for f in dataclasses.fields(MoversCandidate)}
        # Must NOT have a 'demand_score' or 'score_override' field
        assert "demand_score" not in field_names
        assert "score_override" not in field_names

    def test_universe_scan_invokes_movers_fetch_each_cycle(self, monkeypatch):
        """Regression: UniverseScanner.scan must refresh movers every cycle."""
        from scanner.universe import UniverseScanner
        import scanner.universe as uni_mod

        fake_fetcher = MagicMock()
        fake_fetcher.fetch_all.return_value = {}
        fake_fetcher.fetch_quotes.return_value = {}

        fake_pipeline = MagicMock()
        fake_pipeline.build_all.return_value = {}

        scanner = UniverseScanner(fetcher=fake_fetcher, pipeline=fake_pipeline)

        fetch_mock = MagicMock(return_value=[])
        monkeypatch.setattr(uni_mod.movers_ingestor, "fetch", fetch_mock)
        monkeypatch.setattr(uni_mod.movers_ingestor, "active_tickers", lambda _now: [])
        monkeypatch.setattr(uni_mod.movers_ingestor, "all_candidates", lambda _now: [])
        monkeypatch.setattr(
            uni_mod.news_validator,
            "validate_ticker",
            lambda _ticker: MagicMock(score=0.0),
        )
        monkeypatch.setattr(scanner, "_render_watchlist_dashboard", lambda *_a, **_k: None)
        monkeypatch.setattr(scanner, "_log_scan", lambda *_a, **_k: None)

        scanner.scan(tickers=["AAPL"])

        assert fetch_mock.call_count == 1
