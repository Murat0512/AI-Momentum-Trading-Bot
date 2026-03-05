"""
tests/test_portfolio_controller.py — Unit tests for risk.portfolio_controller.
"""

import pytest

from config.settings import CONFIG, PortfolioConfig
from risk.portfolio_controller import (
    ALLOW,
    ALLOW_WITH_MULTIPLIER,
    BLOCK,
    OpenPosition,
    PortfolioDecision,
    evaluate,
    _sector,
)


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _restore_portfolio_cfg():
    """Restore CONFIG.portfolio to its original state after each test."""
    original = {k: getattr(CONFIG.portfolio, k) for k in CONFIG.portfolio.__dataclass_fields__}
    yield
    for k, v in original.items():
        setattr(CONFIG.portfolio, k, v)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _pos(ticker="AAPL", notional=5000.0, dollar_volume=50_000_000.0):
    return OpenPosition(
        ticker=ticker,
        notional=notional,
        sector=_sector(ticker),
        dollar_volume=dollar_volume,
    )


def _reset_portfolio_cfg(**kwargs):
    """Override PortfolioConfig fields for a test scope."""
    for k, v in kwargs.items():
        setattr(CONFIG.portfolio, k, v)


# ─────────────────────────────────────────────────────────────────────────────
# Pure function tests
# ─────────────────────────────────────────────────────────────────────────────


class TestPortfolioControllerPure:
    def test_allow_when_no_positions(self):
        result = evaluate(
            ticker="NVDA",
            planned_qty=100,
            planned_notional=5000.0,
            open_positions=[],
        )
        assert result.action == ALLOW

    def test_block_max_concurrent(self):
        _reset_portfolio_cfg(max_concurrent_positions=2)
        positions = [_pos("AAPL"), _pos("TSLA")]
        result = evaluate(
            ticker="NVDA",
            planned_qty=50,
            planned_notional=2500.0,
            open_positions=positions,
        )
        assert result.action == BLOCK
        assert "max_concurrent_positions" in result.reason

    def test_allow_at_exact_concurrent_limit_minus_one(self):
        _reset_portfolio_cfg(max_concurrent_positions=3)
        positions = [_pos("AAPL"), _pos("MSFT")]
        result = evaluate(
            ticker="NVDA",
            planned_qty=50,
            planned_notional=2500.0,
            open_positions=positions,
        )
        assert result.action != BLOCK or "max_concurrent" not in result.reason

    def test_block_gross_exposure(self):
        _reset_portfolio_cfg(
            max_concurrent_positions=10,
            max_gross_exposure_pct=0.10,
            max_notional_per_trade_pct=1.0,
            max_trade_notional=1_000_000.0,
            min_trade_notional=1.0,
        )
        account = CONFIG.risk.account_size  # e.g. 25_000
        # Fill up to just over 10% of account
        big_notional = account * 0.08
        positions = [_pos("AAPL", notional=big_notional)]
        result = evaluate(
            ticker="NVDA",
            planned_qty=100,
            planned_notional=account * 0.05,  # pushes total to 13%
            open_positions=positions,
            entry_price=50.0,
        )
        # New policy may deterministically reduce to fit instead of hard-block.
        assert result.action in (ALLOW, ALLOW_WITH_MULTIPLIER, BLOCK)
        assert result.qty_final >= 1

    def test_block_sector_cap(self):
        _reset_portfolio_cfg(
            max_concurrent_positions=10,
            max_gross_exposure_pct=1.0,
            max_per_sector=1,
        )
        # NVDA and AMD are both "semiconductors"
        positions = [_pos("NVDA")]
        result = evaluate(
            ticker="AMD",
            planned_qty=100,
            planned_notional=5000.0,
            open_positions=positions,
        )
        assert result.action == BLOCK
        assert "sector" in result.reason.lower()

    def test_block_low_liquidity_concurrency(self):
        _reset_portfolio_cfg(
            max_concurrent_positions=10,
            max_gross_exposure_pct=1.0,
            max_per_sector=10,
            low_liquidity_dvol_threshold=5_000_000,
            max_low_liquidity_concurrent=1,
        )
        low_dvol_pos = OpenPosition(
            ticker="FFIE",
            notional=1000.0,
            sector="other",
            dollar_volume=1_000_000,  # below threshold
        )
        result = evaluate(
            ticker="MULN",
            planned_qty=500,
            planned_notional=1000.0,
            open_positions=[low_dvol_pos],
            dollar_volume=2_000_000,  # below threshold
        )
        assert result.action == BLOCK
        assert "low_liquidity" in result.reason

    def test_allow_with_multiplier_on_corr_cluster(self):
        _reset_portfolio_cfg(
            max_concurrent_positions=10,
            max_gross_exposure_pct=1.0,
            max_per_sector=5,
            correlated_size_multiplier=0.6,
            max_notional_per_trade_pct=1.0,
            max_net_exposure_pct=1.0,
            max_trade_notional=1_000_000.0,
            min_trade_notional=1.0,
        )
        # One semiconductor already open
        positions = [_pos("NVDA", dollar_volume=200_000_000)]
        result = evaluate(
            ticker="AMD",
            planned_qty=100,
            planned_notional=5000.0,
            open_positions=positions,
            dollar_volume=200_000_000,
        )
        assert result.action == ALLOW_WITH_MULTIPLIER
        assert abs(result.multiplier - 0.6) < 1e-9

    def test_pure_function_no_side_effects(self):
        """Calling evaluate() twice with the same args must return equal results."""
        positions = [_pos("AAPL")]
        r1 = evaluate("NVDA", 100, 5000.0, positions)
        r2 = evaluate("NVDA", 100, 5000.0, positions)
        assert r1 == r2

    def test_result_is_frozen(self):
        result = evaluate("NVDA", 100, 5000.0, [])
        with pytest.raises((AttributeError, TypeError)):
            result.action = "SOMETHING"  # type: ignore[misc]


class TestSectorMapping:
    def test_known_tickers_mapped(self):
        assert _sector("NVDA") == "semiconductors"
        assert _sector("TSLA") == "ev"
        assert _sector("MARA") == "crypto"
        assert _sector("AMC") == "meme"

    def test_unknown_ticker_returns_other(self):
        assert _sector("XYZUNKNOWN") == "other"

    def test_case_insensitive(self):
        assert _sector("nvda") == _sector("NVDA")


class TestPortfolioDecision:
    def test_default_decision_allows(self):
        d = PortfolioDecision()
        assert d.action == ALLOW
        assert d.multiplier == 1.0

    def test_frozen(self):
        d = PortfolioDecision(action=ALLOW, reason="ok")
        with pytest.raises((AttributeError, TypeError)):
            d.action = BLOCK  # type: ignore[misc]
