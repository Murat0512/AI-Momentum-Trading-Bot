from __future__ import annotations

import os
import sys

import pandas as pd
import pytest
import pytz
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.constants import REJECT_NO_STRUCTURE_BREAK, TF_1M, TF_5M
from config.settings import CONFIG
from scanner.rvol import best_rvol, calc_session_rvol
from signals.setup import MomentumSetupV1
import signals.setup as setup_mod

ET = pytz.timezone("America/New_York")


@pytest.fixture(autouse=True)
def _disable_sentiment_gate(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(CONFIG.strategy, "sentiment_gate_enabled", False)


def _df_1m(
    volumes: list[int], lows: list[float], closes: list[float], highs: list[float]
) -> pd.DataFrame:
    start = pd.Timestamp("2026-03-04 09:30", tz=ET)
    idx = pd.date_range(start=start, periods=len(volumes), freq="1min", tz=ET)
    opens = [c - 0.05 for c in closes]
    return pd.DataFrame(
        {
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": volumes,
        },
        index=idx,
    )


def _df_5m(closes: list[float]) -> pd.DataFrame:
    start = pd.Timestamp("2026-03-04 09:30", tz=ET)
    idx = pd.date_range(start=start, periods=len(closes), freq="5min", tz=ET)
    return pd.DataFrame(
        {
            "open": closes,
            "high": [c + 0.1 for c in closes],
            "low": [c - 0.1 for c in closes],
            "close": closes,
            "volume": [20_000] * len(closes),
        },
        index=idx,
    )


def test_low_volume_burst_handles_zero_baseline(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(CONFIG.setup, "require_above_vwap", False)

    volumes = [0] * 11
    lows = [100.0] * 11
    highs = [100.2] * 11
    closes = [100.1] * 11

    setup = MomentumSetupV1().check(
        ticker="TEST",
        mtf_bars={
            TF_1M: _df_1m(volumes=volumes, lows=lows, closes=closes, highs=highs),
            TF_5M: _df_5m(closes=[100.0, 100.0, 100.0]),
        },
        bid=100.05,
        ask=100.10,
    )

    assert setup.valid is False
    assert "Low Volume Burst" in setup.rejection_reason


def test_stop_uses_vwap_buffer_when_last3_lows_below_vwap(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(CONFIG.setup, "require_above_vwap", False)

    volumes = [1_000] * 10 + [2_500, 1_000]
    closes = [100.0] * 8 + [100.1, 100.2, 100.45, 100.40]
    highs = [100.2] * 8 + [100.2, 100.3, 100.5, 100.6]
    lows = [99.8] * 8 + [99.7, 99.6, 99.5, 99.7]  # confirmed last3 lows below VWAP

    setup = MomentumSetupV1().check(
        ticker="TEST",
        mtf_bars={
            TF_1M: _df_1m(volumes=volumes, lows=lows, closes=closes, highs=highs),
            TF_5M: _df_5m(closes=[100.0, 100.0, 100.0]),
        },
        bid=100.25,
        ask=100.30,
    )

    assert setup.valid is True
    assert setup.stop_price == pytest.approx(99.9, rel=0, abs=1e-6)


def test_stop_uses_last3_low_when_above_vwap(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(CONFIG.setup, "require_above_vwap", False)

    volumes = [1_000] * 10 + [2_500, 1_000]
    closes = [100.0] * 8 + [101.2, 101.4, 101.6, 101.5]
    highs = [100.2] * 8 + [101.3, 101.5, 101.8, 101.9]
    lows = [100.1] * 8 + [
        101.0,
        101.1,
        101.2,
        101.3,
    ]  # confirmed last 3 lows above VWAP

    setup = MomentumSetupV1().check(
        ticker="TEST",
        mtf_bars={
            TF_1M: _df_1m(volumes=volumes, lows=lows, closes=closes, highs=highs),
            TF_5M: _df_5m(closes=[100.0, 100.0, 100.0]),
        },
        bid=101.55,
        ask=101.60,
    )

    assert setup.valid is True
    assert setup.stop_price == pytest.approx(101.0, rel=0, abs=1e-6)


def test_burst_relaxation_allows_high_quality_setup(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(CONFIG.setup, "require_above_vwap", True)
    monkeypatch.setattr(CONFIG.setup, "min_1m_volume_burst_mult", 1.8)
    monkeypatch.setattr(CONFIG.setup, "min_1m_volume_burst_mult_floor", 1.45)
    monkeypatch.setattr(CONFIG.setup, "burst_relax_if_vwap_dist_below_pct", 0.02)
    monkeypatch.setattr(CONFIG.setup, "burst_relax_if_structure_min", 0.40)
    monkeypatch.setattr(CONFIG.setup, "burst_relax_if_5m_volume_expansion_min", 1.8)

    volumes = [1_000] * 10 + [1_500, 1_000]  # confirmed bar passes relaxed burst
    closes = [
        100.0,
        100.2,
        100.5,
        100.8,
        101.0,
        101.2,
        101.4,
        101.6,
        101.8,
        102.0,
        102.3,
        102.2,
    ]
    highs = [c + 0.15 for c in closes]
    lows = [c - 0.20 for c in closes]

    setup = MomentumSetupV1().check(
        ticker="TEST",
        mtf_bars={
            TF_1M: _df_1m(volumes=volumes, lows=lows, closes=closes, highs=highs),
            TF_5M: pd.DataFrame(
                {
                    "open": [100.0, 100.3, 100.6, 100.9, 101.1],
                    "high": [100.4, 100.7, 101.0, 101.3, 101.5],
                    "low": [99.8, 100.1, 100.4, 100.7, 100.9],
                    "close": [100.3, 100.6, 100.9, 101.1, 101.3],
                    "volume": [10_000, 10_000, 10_000, 10_000, 22_000],
                },
                index=pd.date_range(
                    start=pd.Timestamp("2026-03-04 09:30", tz=ET),
                    periods=5,
                    freq="5min",
                    tz=ET,
                ),
            ),
        },
        bid=102.00,
        ask=102.05,
    )

    assert setup.valid is True


def test_burst_rejects_when_positive_baseline_bars_insufficient(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(CONFIG.setup, "require_above_vwap", False)
    monkeypatch.setattr(CONFIG.setup, "min_1m_burst_baseline_bars", 5)

    volumes = [0, 0, 0, 0, 0, 0, 500, 0, 0, 0, 2_000]  # only one positive baseline bar
    lows = [100.0] * 11
    highs = [100.3] * 11
    closes = [100.1] * 11

    setup = MomentumSetupV1().check(
        ticker="TEST",
        mtf_bars={
            TF_1M: _df_1m(volumes=volumes, lows=lows, closes=closes, highs=highs),
            TF_5M: _df_5m(closes=[100.0, 100.1, 100.2]),
        },
        bid=100.15,
        ask=100.20,
    )

    assert setup.valid is False
    assert "Low Volume Burst" in setup.rejection_reason


def test_confirmation_gate_blocks_outside_window(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(CONFIG.setup, "require_above_vwap", False)
    monkeypatch.setattr(CONFIG.setup, "require_bar_close_confirmation", True)
    monkeypatch.setattr(CONFIG.setup, "confirmation_window_seconds", 10)

    volumes = [1_000] * 10 + [2_500]
    closes = [100.0] * 8 + [101.2, 101.4, 101.6]
    highs = [100.2] * 8 + [101.3, 101.5, 101.8]
    lows = [100.1] * 8 + [101.0, 101.1, 101.2]

    setup = MomentumSetupV1().check(
        ticker="TEST",
        mtf_bars={
            TF_1M: _df_1m(volumes=volumes, lows=lows, closes=closes, highs=highs),
            TF_5M: _df_5m(closes=[100.0, 100.0, 100.0]),
        },
        bid=101.55,
        ask=101.60,
        now=ET.localize(datetime(2026, 3, 4, 10, 1, 25)),
    )

    assert setup.valid is False
    assert "awaiting 1m close confirmation" in setup.rejection_reason


def test_breakout_uses_closed_bar_not_active_wick(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(CONFIG.setup, "require_above_vwap", False)
    monkeypatch.setattr(CONFIG.setup, "require_bar_close_confirmation", True)
    monkeypatch.setattr(CONFIG.setup, "confirmation_window_seconds", 10)

    volumes = [1_000] * 9 + [1_000, 2_500, 5_000]
    closes = [100.0] * 9 + [100.05, 100.15, 101.20]
    highs = [100.2] * 9 + [100.50, 100.60, 101.50]
    lows = [99.9] * 9 + [99.95, 100.00, 100.80]

    setup = MomentumSetupV1().check(
        ticker="TEST",
        mtf_bars={
            TF_1M: _df_1m(volumes=volumes, lows=lows, closes=closes, highs=highs),
            TF_5M: _df_5m(closes=[100.0, 100.1, 100.2]),
        },
        bid=101.00,
        ask=101.05,
        now=ET.localize(datetime(2026, 3, 4, 10, 1, 5)),
    )

    assert setup.valid is False
    assert REJECT_NO_STRUCTURE_BREAK in setup.rejection_reason


def test_setup_rvol_strength_matches_session_aware_provider():
    idx_prev = pd.date_range(
        start=pd.Timestamp("2026-03-03 09:30", tz=ET), periods=11, freq="1min", tz=ET
    )
    idx_today = pd.date_range(
        start=pd.Timestamp("2026-03-04 09:30", tz=ET), periods=11, freq="1min", tz=ET
    )
    idx = idx_prev.append(idx_today)
    vol = [100] * len(idx_prev) + [300] * len(idx_today)

    df = pd.DataFrame(
        {
            "open": [10.0] * len(idx),
            "high": [10.1] * len(idx),
            "low": [9.9] * len(idx),
            "close": [10.0] * len(idx),
            "volume": vol,
        },
        index=idx,
    )

    now = ET.localize(datetime(2026, 3, 4, 9, 40, 5))
    rvol = best_rvol(
        calc_session_rvol(df, now=now, lookback_days=CONFIG.exthours.rvol_lookback_days)
    )
    expected = round(max(0.0, min(1.0, (float(rvol) - 1.0) / 2.0)), 4)

    got = setup_mod._calculate_rvol_strength(df, now=now)
    assert got == pytest.approx(expected)


def test_sentiment_gate_blocks_setup(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(CONFIG.setup, "require_above_vwap", False)
    monkeypatch.setattr(CONFIG.strategy, "sentiment_gate_enabled", True)
    monkeypatch.setattr(CONFIG.strategy, "sentiment_threshold", 0.2)

    class _Blocked:
        score = 0.0
        allowed = False

    monkeypatch.setattr(
        setup_mod.news_validator, "validate_ticker", lambda _t: _Blocked()
    )

    volumes = [1_000] * 10 + [2_500, 1_000]
    closes = [100.0] * 8 + [101.2, 101.4, 101.6, 101.5]
    highs = [100.2] * 8 + [101.3, 101.5, 101.8, 101.9]
    lows = [100.1] * 8 + [101.0, 101.1, 101.2, 101.3]

    setup = MomentumSetupV1().check(
        ticker="TEST",
        mtf_bars={
            TF_1M: _df_1m(volumes=volumes, lows=lows, closes=closes, highs=highs),
            TF_5M: _df_5m(closes=[100.0, 100.0, 100.0]),
        },
        bid=101.55,
        ask=101.60,
        now=ET.localize(datetime(2026, 3, 4, 10, 1, 5)),
    )

    assert setup.valid is False
    assert "BLOCK_BY_SENTIMENT" in setup.rejection_reason
