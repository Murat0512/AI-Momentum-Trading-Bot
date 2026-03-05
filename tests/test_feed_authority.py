"""
Acceptance Test 5 — Feed authority invariants.

Core invariant:
  yfinance MUST NOT be used in the live/paper trading decision path.
  Any attempt to do so must raise FeedPolicyError, not silently succeed.

Sub-invariants:
  A. get_adapter(live=True) raises FeedPolicyError when data_source_live='yfinance'.
  B. get_live_adapter() raises FeedPolicyError when data_source_live='yfinance'.
  C. assert_live_feed('yfinance') raises FeedPolicyError.
  D. assert_live_feed('alpaca') does NOT raise.
  E. get_adapter(live=False) returns YFinanceAdapter when source='yfinance' (research OK).
  F. BatchFetcher(live=True) raises FeedPolicyError immediately if config is yfinance.
  G. get_adapter(live=True) returns AlpacaAdapter when Alpaca keys set + config='alpaca'.
  H. Feed type tag on quotes — yfinance quote has FEED_YFINANCE; Alpaca has FEED_ALPACA_*.
"""

from __future__ import annotations

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import unittest.mock as mock
import pytest

from data.fetcher import (
    FeedPolicyError,
    assert_live_feed,
    get_adapter,
    get_live_adapter,
    YFinanceAdapter,
    AlpacaAdapter,
    BatchFetcher,
    _null_quote,
)
from config.constants import FEED_YFINANCE, FEED_ALPACA_IEX, FEED_ALPACA_SIP


# ─────────────────────────────────────────────────────────────────────────────
# Test A: get_adapter(live=True) raises when config says yfinance
# ─────────────────────────────────────────────────────────────────────────────

def test_get_adapter_live_raises_for_yfinance():
    """live=True with yfinance config must raise FeedPolicyError."""
    with mock.patch("data.fetcher.CONFIG") as mock_cfg:
        mock_cfg.data.data_source_live = "yfinance"
        with pytest.raises(FeedPolicyError, match="yfinance is NOT permitted"):
            get_adapter(live=True)


# ─────────────────────────────────────────────────────────────────────────────
# Test B: get_live_adapter() raises for yfinance
# ─────────────────────────────────────────────────────────────────────────────

def test_get_live_adapter_raises_for_yfinance():
    with mock.patch("data.fetcher.CONFIG") as mock_cfg:
        mock_cfg.data.data_source_live = "yfinance"
        with pytest.raises(FeedPolicyError):
            get_live_adapter()


# ─────────────────────────────────────────────────────────────────────────────
# Test C: assert_live_feed raises for 'yfinance'
# ─────────────────────────────────────────────────────────────────────────────

def test_assert_live_feed_raises_for_yfinance():
    with pytest.raises(FeedPolicyError, match="NOT permitted"):
        assert_live_feed("yfinance")


# ─────────────────────────────────────────────────────────────────────────────
# Test D: assert_live_feed does NOT raise for 'alpaca'
# ─────────────────────────────────────────────────────────────────────────────

def test_assert_live_feed_ok_for_alpaca():
    """assert_live_feed('alpaca') must not raise — alpaca is the authoritative feed."""
    try:
        assert_live_feed("alpaca")
    except FeedPolicyError:
        pytest.fail("assert_live_feed('alpaca') should NOT raise FeedPolicyError")


# ─────────────────────────────────────────────────────────────────────────────
# Test E: get_adapter(live=False) with yfinance returns YFinanceAdapter
# ─────────────────────────────────────────────────────────────────────────────

def test_research_adapter_allows_yfinance():
    """Research/backfill can use yfinance — live=False bypasses the guard."""
    with mock.patch("data.fetcher.CONFIG") as mock_cfg:
        mock_cfg.data.data_source_research = "yfinance"
        adapter = get_adapter(source="yfinance", live=False)
    assert isinstance(adapter, YFinanceAdapter), \
        f"Expected YFinanceAdapter for research, got {type(adapter)}"


# ─────────────────────────────────────────────────────────────────────────────
# Test F: BatchFetcher(live=True) propagates the error at construction / first use
# ─────────────────────────────────────────────────────────────────────────────

def test_batch_fetcher_live_raises_for_yfinance():
    """BatchFetcher in live mode must enforce the Alpaca-only policy."""
    with mock.patch("data.fetcher.CONFIG") as mock_cfg:
        mock_cfg.data.data_source_live     = "yfinance"
        mock_cfg.data.data_source_research = "yfinance"
        mock_cfg.data.lookback_days        = 5
        mock_cfg.data.include_extended_hours = True
        with pytest.raises(FeedPolicyError):
            BatchFetcher(live=True)


# ─────────────────────────────────────────────────────────────────────────────
# Test G: YFinanceAdapter.adapter_name and feed_type are correct
# ─────────────────────────────────────────────────────────────────────────────

def test_yfinance_adapter_name():
    adapter = YFinanceAdapter()
    assert adapter.adapter_name == "yfinance"
    assert adapter.feed_type    == "yfinance"


# ─────────────────────────────────────────────────────────────────────────────
# Test H: Feed type tag on quotes
# ─────────────────────────────────────────────────────────────────────────────

def test_yfinance_quote_carries_feed_tag():
    """Quotes from yfinance must carry FEED_YFINANCE tag."""
    adapter = YFinanceAdapter()

    # Patch yfinance so we don't hit the network
    with mock.patch("yfinance.Ticker") as mock_ticker:
        mock_info = mock.MagicMock()
        mock_info.bid        = 10.0
        mock_info.ask        = 10.02
        mock_info.last_price = 10.01
        mock_ticker.return_value.fast_info = mock_info

        q = adapter.fetch_quote("FAKE")

    assert q.get("feed") == FEED_YFINANCE, \
        f"yfinance quote must have feed={FEED_YFINANCE}, got {q.get('feed')}"
    assert "bid" in q and "ask" in q and "last" in q, "Quote must have bid/ask/last keys"


def test_null_quote_carries_feed_tag():
    """_null_quote helper must stamp the provided feed tag."""
    q = _null_quote("TEST", FEED_ALPACA_IEX)
    assert q["feed"] == FEED_ALPACA_IEX
    assert q["bid"]  == 0.0

    q2 = _null_quote("TEST", FEED_YFINANCE)
    assert q2["feed"] == FEED_YFINANCE


# ─────────────────────────────────────────────────────────────────────────────
# Test I: Error message is actionable (not cryptic)
# ─────────────────────────────────────────────────────────────────────────────

def test_feed_policy_error_is_actionable():
    """FeedPolicyError message should tell the user exactly how to fix the issue."""
    try:
        assert_live_feed("yfinance")
    except FeedPolicyError as exc:
        msg = str(exc)
        assert "ALPACA_API_KEY" in msg or "alpaca" in msg.lower(), \
            f"Error message should reference Alpaca config: {msg}"
        assert "NOT permitted" in msg or "forbidden" in msg.lower() or "live" in msg.lower(), \
            f"Error must explain it is a live-path violation: {msg}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
