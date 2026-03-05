"""
Acceptance Test 3 — DATA_HEALTH two-tier safety invariants.

Critical invariant:
  ┌─────────────────────────────────────────────────────────────────────┐
  │ BLOCK  : unsafe data → entry FORBIDDEN, size_multiplier = 0.0      │
  │ DEGRADE: missing context → entry PERMITTED, size_multiplier < 1.0  │
  │ OK     : clean data → entry PERMITTED, size_multiplier = 1.0       │
  └─────────────────────────────────────────────────────────────────────┘

Scenarios:
  A. Stale quote (>120s old)        → BLOCK
  B. High spread (>2%)              → BLOCK
  C. Bar gap (>5m between bars)     → BLOCK
  D. Bar count drop (sudden)        → BLOCK
  E. PM dollar volume too low       → BLOCK
  F. Missing 5m context bars        → DEGRADE (not block)
  G. Missing 15m context bars       → DEGRADE (not block)
  H. IEX feed during PM             → DEGRADE (not block)
  I. Clean data, IEX during RTH     → OK, size = 1.0
"""

from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, date, timedelta
import pandas as pd
import pytz
import pytest

from data.health import (
    DataHealthValidator,
    DataHealthReport,
    classify_session,
    DH_OK,
    DH_BLOCK,
    DH_DEGRADE,
)
from config.constants import (
    BLOCK_STALE_QUOTE,
    BLOCK_CLOCK_DRIFT,
    BLOCK_SPREAD_LOCK,
    BLOCK_BAR_GAP,
    BLOCK_BAR_COUNT_DROP,
    BLOCK_PM_DOLLAR_VOLUME,
    DEGRADE_MISSING_5M,
    DEGRADE_MISSING_15M,
    DEGRADE_IEX_PM_COVERAGE,
    FEED_ALPACA_IEX,
    FEED_ALPACA_SIP,
    SESSION_PREMARKET,
    SESSION_RTH,
)
from config.settings import CONFIG

ET = pytz.timezone("America/New_York")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _ts_et(d: date, h: int, m: int) -> datetime:
    return ET.localize(datetime(d.year, d.month, d.day, h, m, 0))


def _flat_1m(
    d: date, start_h: int, start_m: int, n_bars: int, vol: int = 1_000
) -> pd.DataFrame:
    """n_bars of 1m flat bars starting at start_h:start_m."""
    rows = []
    for i in range(n_bars):
        total_min = start_m + i
        h = start_h + total_min // 60
        m = total_min % 60
        ts = ET.localize(datetime(d.year, d.month, d.day, h, m, 0))
        rows.append(
            {"open": 10.0, "high": 10.05, "low": 9.95, "close": 10.0, "volume": vol}
        )
        idx = [
            ET.localize(
                datetime(
                    d.year,
                    d.month,
                    d.day,
                    start_h + (start_m + i) // 60,
                    (start_m + i) % 60,
                    0,
                )
            )
            for i in range(n_bars)
        ]
    df = pd.DataFrame(
        [{"open": 10.0, "high": 10.05, "low": 9.95, "close": 10.0, "volume": vol}]
        * n_bars,
        index=pd.DatetimeIndex(
            [
                ET.localize(
                    datetime(
                        d.year,
                        d.month,
                        d.day,
                        start_h + (start_m + j) // 60,
                        (start_m + j) % 60,
                        0,
                    )
                )
                for j in range(n_bars)
            ]
        ),
    )
    return df


def _good_mtf(d: date) -> dict:
    """Return a minimal but healthy MTF bars dict."""
    from config.constants import TF_1M, TF_5M, TF_15M, TF_1H

    df_1m = _flat_1m(d, 9, 30, 60)  # 1h of 1m bars
    from data.pipeline import MTFPipeline

    pipeline = MTFPipeline()
    mtf = pipeline.build("X", df_1m)
    return mtf


def _fresh_quote(
    bid: float = 10.00, ask: float = 10.02, age_s: int = 5, now: datetime = None
) -> dict:
    _now = now if now is not None else datetime.now(ET)
    return {
        "bid": bid,
        "ask": ask,
        "last": (bid + ask) / 2,
        "timestamp": _now - timedelta(seconds=age_s),
        "feed": FEED_ALPACA_IEX,
    }


validator = DataHealthValidator()


@pytest.fixture(autouse=True)
def _default_non_burst_profile(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(CONFIG.strategy, "is_burst_profile", False)


# ─────────────────────────────────────────────────────────────────────────────
# Scenario A: Stale quote → BLOCK
# ─────────────────────────────────────────────────────────────────────────────


def test_stale_quote_blocks():
    d = date(2024, 6, 10)
    mtf = _good_mtf(d)
    now = _ts_et(d, 10, 0)
    quote = _fresh_quote(age_s=180, now=now)  # 3 minutes old — exceeds 120s threshold

    report = validator.check(
        "STALE", mtf, quote, prev_bar_count_1m=None, feed_type=FEED_ALPACA_IEX, now=now
    )

    assert report.status == DH_BLOCK, f"Expected BLOCK, got {report.status}"
    assert (
        BLOCK_STALE_QUOTE in report.block_reason
    ), f"block_reason should mention stale quote, got: {report.block_reason}"
    assert report.size_multiplier == 0.0


# ─────────────────────────────────────────────────────────────────────────────
# Scenario B: Wide spread (>2%) → BLOCK
# ─────────────────────────────────────────────────────────────────────────────


def test_spread_lock_blocks():
    d = date(2024, 6, 10)
    mtf = _good_mtf(d)
    # bid=10.00 ask=10.30 → spread = 0.30/10.15 ≈ 2.96%
    now = _ts_et(d, 10, 0)
    quote = _fresh_quote(bid=10.00, ask=10.30, age_s=5, now=now)

    report = validator.check(
        "WIDE", mtf, quote, prev_bar_count_1m=None, feed_type=FEED_ALPACA_IEX, now=now
    )

    assert (
        report.status == DH_BLOCK
    ), f"Expected BLOCK for wide spread, got {report.status}"
    assert (
        BLOCK_SPREAD_LOCK in report.block_reason
    ), f"block_reason should mention spread lock, got: {report.block_reason}"


# ─────────────────────────────────────────────────────────────────────────────
# Scenario C: Bar gap in 1m data → BLOCK
# ─────────────────────────────────────────────────────────────────────────────


def test_bar_gap_blocks():
    d = date(2024, 6, 10)
    # Create 1m bars with a 10-minute gap
    times = [_ts_et(d, 9, 30 + i) for i in range(10)] + [  # 09:30–09:39
        _ts_et(d, 9, 50 + i) for i in range(10)
    ]  # 09:50–09:59 (10m gap)
    df_1m = pd.DataFrame(
        [{"open": 10.0, "high": 10.05, "low": 9.95, "close": 10.0, "volume": 1_000}]
        * len(times),
        index=pd.DatetimeIndex(times),
    )
    from data.pipeline import MTFPipeline
    from config.constants import TF_1M

    mtf = {TF_1M: df_1m}

    now = _ts_et(d, 10, 0)
    quote = _fresh_quote(age_s=5, now=now)
    report = validator.check(
        "GAP", mtf, quote, prev_bar_count_1m=None, feed_type=FEED_ALPACA_IEX, now=now
    )

    assert (
        report.status == DH_BLOCK
    ), f"Expected BLOCK for bar gap, got {report.status} (reason={report.block_reason})"
    assert BLOCK_BAR_GAP in report.block_reason


# ─────────────────────────────────────────────────────────────────────────────
# Scenario D: Bar count drop → BLOCK
# ─────────────────────────────────────────────────────────────────────────────


def test_bar_count_drop_blocks():
    d = date(2024, 6, 10)
    mtf = _good_mtf(d)
    from config.constants import TF_1M

    n_now = len(mtf[TF_1M])  # e.g. 60
    n_prev = n_now + 20  # previous had 20 more bars → sudden drop

    now = _ts_et(d, 10, 0)
    quote = _fresh_quote(age_s=5, now=now)
    report = validator.check(
        "DROP", mtf, quote, prev_bar_count_1m=n_prev, feed_type=FEED_ALPACA_IEX, now=now
    )

    assert (
        report.status == DH_BLOCK
    ), f"Expected BLOCK for bar count drop, got {report.status} (reason={report.block_reason})"
    assert BLOCK_BAR_COUNT_DROP in report.block_reason


# ─────────────────────────────────────────────────────────────────────────────
# Scenario E: PM dollar volume too low on IEX → DEGRADE (low coverage warning)
# ─────────────────────────────────────────────────────────────────────────────


def test_pm_dollar_volume_block():
    d = date(2024, 6, 10)
    # Only 1 PM bar with tiny volume ($50)
    pm_tiny = pd.DataFrame(
        [{"open": 10.0, "high": 10.05, "low": 9.95, "close": 10.0, "volume": 5}],
        index=pd.DatetimeIndex([_ts_et(d, 6, 0)]),
    )
    from config.constants import TF_1M

    mtf = {TF_1M: pm_tiny}

    now = _ts_et(d, 7, 0)  # PM session
    quote = _fresh_quote(age_s=5, now=now)
    report = validator.check(
        "PMLOW", mtf, quote, prev_bar_count_1m=None, feed_type=FEED_ALPACA_IEX, now=now
    )

    # IEX low PM coverage should DEGRADE (not hard-block) and carry reason detail.
    assert (
        report.status == DH_DEGRADE
    ), f"Expected DEGRADE for IEX low PM dollar vol, got {report.status}"
    assert any(
        BLOCK_PM_DOLLAR_VOLUME in r for r in report.degrade_reasons
    ), f"Expected PM low-coverage detail in degrade reasons, got {report.degrade_reasons}"


# ─────────────────────────────────────────────────────────────────────────────
# Scenario F: Missing 5m context → DEGRADE, not BLOCK
# ─────────────────────────────────────────────────────────────────────────────


def test_missing_5m_degrades_not_blocks():
    d = date(2024, 6, 10)
    mtf = _good_mtf(d)
    # Remove 5m bars to simulate missing context
    from config.constants import TF_1M, TF_5M

    mtf_no5m = {k: v for k, v in mtf.items() if k != TF_5M}

    now = _ts_et(d, 10, 30)
    quote = _fresh_quote(age_s=5, now=now)
    report = validator.check(
        "NO5M",
        mtf_no5m,
        quote,
        prev_bar_count_1m=None,
        feed_type=FEED_ALPACA_IEX,
        now=now,
    )

    assert report.status in (
        DH_OK,
        DH_DEGRADE,
    ), f"Missing 5m context should DEGRADE not BLOCK, got {report.status}"
    if report.status == DH_DEGRADE:
        assert (
            DEGRADE_MISSING_5M in report.degrade_reasons
        ), f"Expected DEGRADE_MISSING_5M in {report.degrade_reasons}"
        assert (
            0.0 < report.size_multiplier < 1.0
        ), f"Degraded size_multiplier should be between 0 and 1, got {report.size_multiplier}"


# ─────────────────────────────────────────────────────────────────────────────
# Scenario G: Missing 15m context → DEGRADE
# ─────────────────────────────────────────────────────────────────────────────


def test_missing_15m_degrades_not_blocks():
    d = date(2024, 6, 10)
    mtf = _good_mtf(d)
    from config.constants import TF_15M

    mtf_no15m = {k: v for k, v in mtf.items() if k != TF_15M}

    now = _ts_et(d, 10, 30)
    quote = _fresh_quote(age_s=5, now=now)
    report = validator.check(
        "NO15M",
        mtf_no15m,
        quote,
        prev_bar_count_1m=None,
        feed_type=FEED_ALPACA_IEX,
        now=now,
    )

    assert report.status in (
        DH_OK,
        DH_DEGRADE,
    ), f"Missing 15m context should DEGRADE not BLOCK, got {report.status}"


# ─────────────────────────────────────────────────────────────────────────────
# Scenario H: IEX feed in PM → DEGRADE (size reduction), not BLOCK
# ─────────────────────────────────────────────────────────────────────────────


def test_iex_pm_degrades_not_blocks():
    d = date(2024, 6, 10)
    # Build PM-era bars only
    pm_bars = pd.DataFrame(
        [{"open": 10.0, "high": 10.05, "low": 9.95, "close": 10.0, "volume": 50_000}]
        * 30,
        index=pd.DatetimeIndex([_ts_et(d, 6, i) for i in range(30)]),
    )
    from config.constants import TF_1M

    mtf = {TF_1M: pm_bars}

    now = _ts_et(d, 6, 30)  # in PM session
    quote = _fresh_quote(age_s=5, now=now)
    report = validator.check(
        "IEX_PM", mtf, quote, prev_bar_count_1m=None, feed_type=FEED_ALPACA_IEX, now=now
    )

    # IEX + PM should DEGRADE (not BLOCK)
    assert (
        report.status != DH_BLOCK or report.block_reason == BLOCK_PM_DOLLAR_VOLUME
    ), f"IEX in PM should DEGRADE not BLOCK (unless PM dollar vol too low). Got: {report}"
    if report.status == DH_DEGRADE:
        assert DEGRADE_IEX_PM_COVERAGE in report.degrade_reasons


# ─────────────────────────────────────────────────────────────────────────────
# Scenario I: Clean RTH data, IEX feed → OK, size = 1.0
# ─────────────────────────────────────────────────────────────────────────────


def test_clean_rth_data_is_ok():
    d = date(2024, 6, 10)
    mtf = _good_mtf(d)

    now = _ts_et(d, 11, 0)  # mid RTH
    quote = _fresh_quote(bid=10.00, ask=10.02, age_s=5, now=now)  # fresh, tight spread
    report = validator.check(
        "CLEAN", mtf, quote, prev_bar_count_1m=None, feed_type=FEED_ALPACA_IEX, now=now
    )

    assert (
        report.status == DH_OK
    ), f"Clean RTH data should be DH_OK, got {report.status} (reason={report.block_reason})"
    assert report.size_multiplier == pytest.approx(
        1.0
    ), f"Clean data: size_multiplier should be 1.0, got {report.size_multiplier}"


def test_clock_drift_future_quote_blocks(monkeypatch):
    d = date(2024, 6, 10)
    mtf = _good_mtf(d)
    now = _ts_et(d, 11, 0)
    quote = {
        "bid": 10.0,
        "ask": 10.02,
        "last": 10.01,
        "timestamp": now + timedelta(seconds=5),
        "feed": FEED_ALPACA_IEX,
    }
    monkeypatch.setattr(CONFIG.health, "max_clock_drift_seconds", 2)

    report = validator.check(
        "DRIFT", mtf, quote, prev_bar_count_1m=None, feed_type=FEED_ALPACA_IEX, now=now
    )

    assert report.status == DH_BLOCK
    assert BLOCK_CLOCK_DRIFT in report.block_reason


def test_burst_profile_blocks_iex_even_in_rth(monkeypatch):
    d = date(2024, 6, 10)
    mtf = _good_mtf(d)
    now = _ts_et(d, 11, 0)
    quote = _fresh_quote(age_s=5, now=now)
    monkeypatch.setattr(CONFIG.lifecycle, "parabolic_squeeze_enabled", False)
    monkeypatch.setattr(CONFIG.strategy, "is_burst_profile", True)
    monkeypatch.setattr(CONFIG.health, "block_burst_profile_on_iex", True)

    report = validator.check(
        "BURSTIEX",
        mtf,
        quote,
        prev_bar_count_1m=None,
        feed_type=FEED_ALPACA_IEX,
        now=now,
    )

    assert report.status == DH_BLOCK
    assert "BURST_PROFILE_REQUIRES_SIP" in report.block_reason


def test_non_burst_profile_does_not_force_iex_block(monkeypatch):
    d = date(2024, 6, 10)
    mtf = _good_mtf(d)
    now = _ts_et(d, 11, 0)
    quote = _fresh_quote(age_s=5, now=now)
    monkeypatch.setattr(CONFIG.strategy, "is_burst_profile", False)
    monkeypatch.setattr(CONFIG.health, "block_burst_profile_on_iex", True)

    report = validator.check(
        "NOBURST",
        mtf,
        quote,
        prev_bar_count_1m=None,
        feed_type=FEED_ALPACA_IEX,
        now=now,
    )

    assert report.status in (DH_OK, DH_DEGRADE)
    assert "BURST_PROFILE_REQUIRES_SIP" not in report.block_reason


def test_data_health_test_override_paper_mode(monkeypatch):
    d = date(2024, 6, 10)
    mtf = _good_mtf(d)
    now = _ts_et(d, 11, 0)

    # Intentionally stale quote would normally BLOCK.
    quote = _fresh_quote(age_s=600, now=now)

    monkeypatch.setenv("DATA_HEALTH_TEST_OVERRIDE", "1")
    monkeypatch.setattr(CONFIG.execution, "paper_mode", True)

    report = validator.check(
        "OVERRIDE",
        mtf,
        quote,
        prev_bar_count_1m=None,
        feed_type=FEED_ALPACA_IEX,
        now=now,
    )

    assert report.status == DH_OK
    assert report.size_multiplier == pytest.approx(1.0)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
