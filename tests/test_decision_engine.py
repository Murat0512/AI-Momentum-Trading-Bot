"""
tests/test_decision_engine.py — Acceptance tests for decision.engine

DecisionEngine(risk_manager, slip_monitor=None) — risk injected in constructor.
quote_store values must be plain dicts: {"bid": float, "ask": float, "timestamp": datetime}.
top15 must be List[CandidateEntry].
"""
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from config.settings import CONFIG
from config.constants import (
    GATE_PASS, GATE_SPREAD_WIDE, GATE_QUOTE_STALE, GATE_DH_BLOCK,
    GATE_SLIPPAGE_BLOCK, GATE_RISK_MAX_TRADES, GATE_NO_VALID_SETUP,
    GATE_LOW_SQS, REGIME_TREND, REGIME_CHOP, DH_BLOCK as DH_BLOCK_STATUS,
)
from data.health import DataHealthReport
from decision.engine import (
    DecisionEngine,
    _pullback_integrity_score,
    _enhanced_sqs,
)
from scanner.universe_manager import CandidateEntry
from signals.setup import SetupResult


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _now():
    return datetime.now(timezone.utc)


def _flat_bars(n: int = 25, close: float = 25.0, vol: int = 100_000) -> pd.DataFrame:
    return pd.DataFrame({
        "open":   [close] * n,
        "high":   [close * 1.001] * n,
        "low":    [close * 0.999] * n,
        "close":  [close] * n,
        "volume": [vol] * n,
        "vwap":   [close * 0.998] * n,
    })


def _trending_bars(n: int = 25) -> pd.DataFrame:
    closes = [20.0 + i * 0.10 for i in range(n)]
    vols   = [100_000 + i * 1_000 for i in range(n)]
    return pd.DataFrame({
        "open":   closes,
        "high":   [c * 1.002 for c in closes],
        "low":    [c * 0.998 for c in closes],
        "close":  closes,
        "volume": vols,
        "vwap":   [c * 0.997 for c in closes],
    })


def _choppy_bars(n: int = 25) -> pd.DataFrame:
    import random
    random.seed(42)
    closes = [25.0 + random.uniform(-1, 1) for _ in range(n)]
    return pd.DataFrame({
        "open":   closes,
        "high":   [c + 1.5 for c in closes],
        "low":    [c - 1.5 for c in closes],
        "close":  closes,
        "volume": [50_000] * n,
        "vwap":   [25.0] * n,
    })


def _make_entry(ticker: str = "AAPL", demand: float = 0.70,
                price: float = 25.0) -> CandidateEntry:
    """Build a CandidateEntry suitable for top15."""
    e                    = CandidateEntry(ticker=ticker)
    e.demand_score       = demand
    e.composite_score    = demand
    e.last_price         = price
    e.dollar_volume      = price * 100_000
    e.rvol               = 3.0
    e.gap_pct            = 0.05
    e.intraday_range_pct = 0.02
    e.feed_type          = "alpaca_sip"
    return e


def _good_quote(price: float = 25.0, age_seconds: float = 1.0) -> dict:
    return {
        "bid":       price - 0.01,
        "ask":       price + 0.01,
        "timestamp": datetime.now(timezone.utc) - timedelta(seconds=age_seconds),
    }


def _wide_quote(price: float = 25.0) -> dict:
    return {
        "bid":       price - 1.0,
        "ask":       price + 1.0,
        "timestamp": datetime.now(timezone.utc),
    }


def _stale_quote(price: float = 25.0, age_minutes: float = 10.0) -> dict:
    return {
        "bid":       price - 0.01,
        "ask":       price + 0.01,
        "timestamp": datetime.now(timezone.utc) - timedelta(minutes=age_minutes),
    }


def _dh_ok(ticker: str = "AAPL") -> DataHealthReport:
    return DataHealthReport(ticker=ticker, status="OK", size_multiplier=1.0)


def _dh_block(ticker: str = "AAPL", reason: str = "stale") -> DataHealthReport:
    return DataHealthReport(ticker=ticker, status=DH_BLOCK_STATUS, block_reason=reason)


def _make_risk(can_trade: bool = True):
    r = MagicMock()
    r.can_trade.return_value   = (can_trade, "ok" if can_trade else "MAX_TRADES")
    r.open_trades.return_value = []   # empty list = 0 open trades
    r.daily_pnl.return_value   = 0.0
    return r


def _valid_setup_result(ticker: str = "AAPL") -> SetupResult:
    return SetupResult(
        ticker             = ticker,
        valid              = True,
        setup_name         = "vwap_reclaim",
        vwap_dist_pct      = 0.003,
        structure_clarity  = 0.80,
        volume_expansion   = 2.5,
        setup_quality_score= 0.70,
        entry_price        = 25.10,
        stop_price         = 24.50,
        atr                = 0.60,
    )


def _engine(monkeypatch=None, risk=None) -> DecisionEngine:
    risk = risk or _make_risk()
    return DecisionEngine(risk)


# ─────────────────────────────────────────────────────────────────────────────
# _pullback_integrity_score
# ─────────────────────────────────────────────────────────────────────────────

class TestPullbackIntegrityScore:
    def test_score_in_range(self):
        score = _pullback_integrity_score(_trending_bars())
        assert 0.0 <= score <= 1.0

    def test_trending_vs_choppy(self):
        """Trending bars score >= choppy bars on average."""
        t = _pullback_integrity_score(_trending_bars())
        c = _pullback_integrity_score(_choppy_bars())
        assert t >= c

    def test_few_bars_returns_neutral(self):
        """Fewer bars than lookback → neutral 0.5 (not an error)."""
        score = _pullback_integrity_score(_flat_bars(n=5), lookback=20)
        assert isinstance(score, float)
        assert score == pytest.approx(0.5, abs=0.1)

    def test_flat_bars_numeric(self):
        score = _pullback_integrity_score(_flat_bars())
        assert isinstance(score, float)


# ─────────────────────────────────────────────────────────────────────────────
# _enhanced_sqs
# ─────────────────────────────────────────────────────────────────────────────

class TestEnhancedSQS:
    def test_score_in_range(self):
        setup        = _valid_setup_result()
        score, comps = _enhanced_sqs(setup, _flat_bars(), CONFIG.decision)
        assert 0.0 <= score <= 1.0

    def test_components_dict_has_all_keys(self):
        setup        = _valid_setup_result()
        _, comps     = _enhanced_sqs(setup, _flat_bars(), CONFIG.decision)
        from config.constants import SQ_VWAP_RECLAIM, SQ_STRUCTURE, SQ_VOLUME_EXP, SQ_PULLBACK
        for key in (SQ_VWAP_RECLAIM, SQ_STRUCTURE, SQ_VOLUME_EXP, SQ_PULLBACK):
            assert key in comps, f"Missing SQS component key: {key}"

    def test_higher_volume_expansion_raises_score(self):
        low_vol  = _valid_setup_result()
        low_vol.volume_expansion = 1.1
        high_vol = _valid_setup_result()
        high_vol.volume_expansion = 5.0
        s_low,  _ = _enhanced_sqs(low_vol,  _flat_bars(), CONFIG.decision)
        s_high, _ = _enhanced_sqs(high_vol, _flat_bars(), CONFIG.decision)
        assert s_high >= s_low

    def test_none_df_graceful(self):
        """If bars are None, pullback defaults to 0.5 — no exception."""
        setup        = _valid_setup_result()
        score, comps = _enhanced_sqs(setup, None, CONFIG.decision)
        assert isinstance(score, float)


# ─────────────────────────────────────────────────────────────────────────────
# DecisionEngine — gate pipeline
# ─────────────────────────────────────────────────────────────────────────────

class TestDecisionEngineGates:
    def _run(self, entries, quote=None, dh=None, risk=None,
             regime=REGIME_TREND, now=None):
        risk   = risk   or _make_risk()
        engine = DecisionEngine(risk)
        now    = now    or _now()
        quote  = quote  or _good_quote()
        dh     = dh     or _dh_ok()

        bar_store   = {e.ticker: _flat_bars()  for e in entries}
        mtf_store   = {e.ticker: {}            for e in entries}
        quote_store = {e.ticker: quote         for e in entries}
        dh_store    = {e.ticker: dh            for e in entries}

        return engine.run(
            top15       = entries,
            bar_store   = bar_store,
            mtf_store   = mtf_store,
            quote_store = quote_store,
            dh_store    = dh_store,
            regime      = regime,
            now         = now,
        )

    # ── Spread gate ──────────────────────────────────────────────────────────

    def test_spread_gate_blocks_wide_spread(self, monkeypatch):
        monkeypatch.setattr(CONFIG.decision, "max_spread_pct_entry", 0.001)
        entry  = _make_entry()
        result = self._run([entry], quote=_wide_quote())
        assert entry.ticker in result.rejected
        assert result.rejected[entry.ticker] == GATE_SPREAD_WIDE

    def test_spread_gate_passes_narrow_spread(self, monkeypatch):
        monkeypatch.setattr(CONFIG.decision, "max_spread_pct_entry", 0.01)
        monkeypatch.setattr(CONFIG.decision, "max_quote_age_entry_seconds", 300)
        entry  = _make_entry()
        result = self._run([entry], quote=_good_quote())
        # Might fail at later gates but NOT at spread
        assert result.rejected.get(entry.ticker) != GATE_SPREAD_WIDE

    # ── Quote age gate ───────────────────────────────────────────────────────

    def test_quote_age_gate_blocks_stale_quote(self, monkeypatch):
        monkeypatch.setattr(CONFIG.decision, "max_spread_pct_entry",        0.10)
        monkeypatch.setattr(CONFIG.decision, "max_quote_age_entry_seconds", 10)
        entry  = _make_entry()
        result = self._run([entry], quote=_stale_quote(age_minutes=5))
        assert entry.ticker in result.rejected
        assert result.rejected[entry.ticker] == GATE_QUOTE_STALE

    # ── DH gate ──────────────────────────────────────────────────────────────

    def test_dh_block_gate_rejects(self, monkeypatch):
        monkeypatch.setattr(CONFIG.decision, "max_spread_pct_entry",        0.10)
        monkeypatch.setattr(CONFIG.decision, "max_quote_age_entry_seconds", 300)
        entry  = _make_entry()
        result = self._run([entry], dh=_dh_block("feed_down"))
        assert entry.ticker in result.rejected
        assert result.rejected[entry.ticker] == GATE_DH_BLOCK

    # ── Risk gate ─────────────────────────────────────────────────────────────

    def test_risk_gate_rejects_when_cant_trade(self, monkeypatch):
        monkeypatch.setattr(CONFIG.decision, "max_spread_pct_entry",        0.10)
        monkeypatch.setattr(CONFIG.decision, "max_quote_age_entry_seconds", 300)
        entry  = _make_entry()
        result = self._run([entry], risk=_make_risk(can_trade=False))
        assert entry.ticker in result.rejected
        # Should map to a risk-related gate constant
        assert "RISK" in result.rejected[entry.ticker].upper() or \
               result.rejected[entry.ticker] == GATE_RISK_MAX_TRADES

    # ── Determinism ──────────────────────────────────────────────────────────

    def test_determinism_same_inputs_same_rejected(self, monkeypatch):
        """
        When the same ticker is rejected (spread too wide), it is rejected
        consistently across multiple runs.
        """
        monkeypatch.setattr(CONFIG.decision, "max_spread_pct_entry", 0.001)
        entries = [_make_entry("AAPL"), _make_entry("TSLA")]
        now     = _now()
        results = []
        for _ in range(3):
            for e in entries:
                e.composite_score = e.demand_score   # reset
            results.append(
                self._run(entries, quote=_wide_quote(), now=now)
            )
        rejected_sets = [frozenset(r.rejected.keys()) for r in results]
        assert len(set(rejected_sets)) == 1, "Non-deterministic rejection"

    # ── Regime shaping ────────────────────────────────────────────────────────

    def test_chop_regime_tightens_spread_cap(self, monkeypatch):
        """
        With chop_spread_multiplier < 1, a spread that's OK in TREND is
        rejected in CHOP.
        """
        monkeypatch.setattr(CONFIG.decision, "max_spread_pct_entry",     0.01)
        monkeypatch.setattr(CONFIG.decision, "chop_spread_multiplier",   0.10)
        monkeypatch.setattr(CONFIG.decision, "max_quote_age_entry_seconds", 300)
        # 0.5% spread — fine in TREND (0.01 cap), blocked in CHOP (0.001 cap)
        quote = {"bid": 25.0, "ask": 25.125,   # 0.5% spread
                 "timestamp": datetime.now(timezone.utc)}
        entry = _make_entry()

        trend_result = self._run([entry], quote=quote, regime=REGIME_TREND)
        chop_result  = self._run([entry], quote=quote, regime=REGIME_CHOP)

        # In CHOP, the tighter cap (0.001) MUST block the 0.5% spread
        assert chop_result.rejected.get(entry.ticker) == GATE_SPREAD_WIDE, (
            f"CHOP should block at GATE_SPREAD_WIDE; got: {chop_result.rejected}"
        )
        # In TREND, the 1% cap should NOT fire at spread gate
        assert trend_result.rejected.get(entry.ticker) != GATE_SPREAD_WIDE, (
            "TREND should not fire GATE_SPREAD_WIDE with 1% cap and 0.5% spread"
        )
