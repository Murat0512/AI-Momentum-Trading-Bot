from __future__ import annotations

import pytest

from config.settings import CONFIG
from risk.manager import RiskManager


@pytest.fixture(autouse=True)
def _restore_cfg():
    p = {k: getattr(CONFIG.portfolio, k) for k in CONFIG.portfolio.__dataclass_fields__}
    r = {k: getattr(CONFIG.risk, k) for k in CONFIG.risk.__dataclass_fields__}
    yield
    for k, v in p.items():
        setattr(CONFIG.portfolio, k, v)
    for k, v in r.items():
        setattr(CONFIG.risk, k, v)


def test_get_position_size_invalid_stop_loss_blocks():
    rm = RiskManager()
    result = rm.get_position_size(
        ticker="NVDA",
        entry_price=10.0,
        stop_loss=10.0,
        ticker_dvol=100_000_000.0,
        current_positions={},
    )
    assert result["can_trade"] is False
    assert result["shares"] == 0
    assert result["reason"] == "Invalid Stop Loss"


def test_get_position_size_applies_notional_and_liquidity_caps():
    rm = RiskManager()
    CONFIG.risk.account_size = 25_000.0
    CONFIG.risk.risk_per_trade_pct = 0.01
    CONFIG.portfolio.max_notional_per_trade_pct = 0.10
    CONFIG.portfolio.liquidity_notional_cap_pct_of_dvol = 0.01

    result = rm.get_position_size(
        ticker="TSLA",
        entry_price=10.0,
        stop_loss=9.0,
        ticker_dvol=100_000.0,
        current_positions={},
    )

    assert result["can_trade"] is True
    assert result["shares"] == 100
    assert result["notional"] == 1000.0


def test_get_position_size_blocks_on_sector_limit():
    rm = RiskManager()
    CONFIG.portfolio.max_per_sector = 1

    result = rm.get_position_size(
        ticker="NVDA",
        entry_price=10.0,
        stop_loss=9.0,
        ticker_dvol=100_000_000.0,
        current_positions={"AMD": {"shares": 100}},
    )

    assert result["can_trade"] is False
    assert result["shares"] == 0
    assert "Sector Limit Reached" in result["reason"]


def test_get_position_size_applies_correlated_multiplier():
    rm = RiskManager()
    CONFIG.risk.account_size = 25_000.0
    CONFIG.risk.risk_per_trade_pct = 0.01
    CONFIG.portfolio.max_per_sector = 2
    CONFIG.portfolio.correlated_size_multiplier = 0.6
    CONFIG.portfolio.max_notional_per_trade_pct = 1.0
    CONFIG.portfolio.liquidity_notional_cap_pct_of_dvol = 1.0
    CONFIG.portfolio.min_trade_notional = 1.0

    result = rm.get_position_size(
        ticker="NVDA",
        entry_price=10.0,
        stop_loss=9.0,
        ticker_dvol=1_000_000_000.0,
        current_positions={"AMD": {"shares": 100}},
    )

    # base fixed-R = floor(250/1)=250, correlated -> floor(250*0.6)=150
    assert result["can_trade"] is True
    assert result["shares"] == 150
    assert result["notional"] == 1500.0
