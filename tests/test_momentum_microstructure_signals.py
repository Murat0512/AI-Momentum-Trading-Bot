from __future__ import annotations

import pandas as pd

from scanner.demand import calculate_dollar_flow_momentum, compute_demand_score
from scanner.demand import compute_setup_quality_score
from signals.setup import calculate_pressure_score


def _df_from_rows(rows: list[tuple[float, float, float, float, float]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["open", "high", "low", "close", "volume"])


def test_dollar_flow_momentum_positive_on_inflow():
    # Last 5 bars have meaningfully higher close*volume than previous 5.
    rows = []
    rows += [(10.0, 10.2, 9.9, 10.0, 1000)] * 15
    rows += [(10.0, 10.5, 9.9, 10.4, 3000)] * 3
    rows += [(10.2, 10.9, 10.0, 10.8, 9000)] * 2
    df = _df_from_rows(rows)

    z = calculate_dollar_flow_momentum(df)
    assert z > 0.0


def test_dollar_flow_momentum_zero_on_flat_flow():
    # Flat close and flat volume -> no acceleration history.
    rows = [(10.0, 10.1, 9.9, 10.0, 1000)] * 30
    df = _df_from_rows(rows)

    z = calculate_dollar_flow_momentum(df)
    assert z == 0.0


def test_dollar_flow_deterministic_replay():
    rows = []
    rows += [(10.0, 10.2, 9.9, 10.0, 1200)] * 12
    rows += [(10.1, 10.6, 10.0, 10.5, 4500)] * 12
    df = _df_from_rows(rows)

    a = calculate_dollar_flow_momentum(df)
    b = calculate_dollar_flow_momentum(df.copy())
    c = calculate_dollar_flow_momentum(df)

    assert a == b == c


def test_pressure_high_when_close_near_high():
    # Close near high on all bars + reasonable volume => strong pressure.
    rows = [
        (10.0, 10.5, 9.8, 10.49, 3000),
        (10.2, 10.7, 10.0, 10.69, 3200),
        (10.4, 10.9, 10.2, 10.89, 3400),
        (10.5, 11.0, 10.3, 10.99, 3600),
        (10.7, 11.2, 10.5, 11.19, 3800),
        (10.8, 11.3, 10.6, 11.29, 4000),
        (10.9, 11.4, 10.7, 11.39, 4300),
    ]
    df = _df_from_rows(rows)

    score = calculate_pressure_score(df)
    assert score > 0.6


def test_pressure_low_when_close_near_low():
    rows = [
        (10.0, 10.5, 9.8, 9.81, 3000),
        (10.2, 10.7, 10.0, 10.01, 3200),
        (10.4, 10.9, 10.2, 10.21, 3400),
        (10.5, 11.0, 10.3, 10.31, 3600),
        (10.7, 11.2, 10.5, 10.51, 3800),
        (10.8, 11.3, 10.6, 10.61, 4000),
        (10.9, 11.4, 10.7, 10.71, 4300),
    ]
    df = _df_from_rows(rows)

    score = calculate_pressure_score(df)
    assert score < 0.4


def test_pressure_volume_weighting():
    # High-volume bars with stronger close-location should lift weighted pressure.
    low_vol_rows = [
        (10.0, 10.5, 9.8, 10.49, 1000),
        (10.2, 10.7, 10.0, 10.69, 1000),
        (10.4, 10.9, 10.2, 10.89, 1000),
        (10.5, 11.0, 10.3, 10.35, 1200),
        (10.7, 11.2, 10.5, 10.60, 1200),
        (10.8, 11.3, 10.6, 10.65, 1200),
        (10.9, 11.4, 10.7, 10.75, 1200),
    ]
    high_vol_rows = [
        (10.0, 10.5, 9.8, 10.49, 1000),
        (10.2, 10.7, 10.0, 10.69, 1000),
        (10.4, 10.9, 10.2, 10.89, 1000),
        (10.5, 11.0, 10.3, 10.35, 1200),
        (10.7, 11.2, 10.5, 10.60, 1200),
        (10.8, 11.3, 10.6, 11.29, 6000),
        (10.9, 11.4, 10.7, 11.39, 7000),
    ]

    low_score = calculate_pressure_score(_df_from_rows(low_vol_rows))
    high_score = calculate_pressure_score(_df_from_rows(high_vol_rows))
    assert high_score > low_score


def test_demand_score_includes_flow_metric():
    base = compute_demand_score(
        rvol=3.0,
        gap_pct=0.05,
        intraday_range_pct=0.06,
        volume_spike_z=2.0,
        dollar_flow_momentum_z=0.0,
    )
    with_flow = compute_demand_score(
        rvol=3.0,
        gap_pct=0.05,
        intraday_range_pct=0.06,
        volume_spike_z=2.0,
        dollar_flow_momentum_z=3.0,
    )
    assert with_flow > base


def test_sqs_includes_pressure_metric():
    low = compute_setup_quality_score(
        vwap_distance_pct=0.015,
        volume_expansion=2.0,
        structure_clarity=0.8,
        spread_pct=0.002,
        pressure_score=0.10,
        rvol_strength=0.70,
    )
    high = compute_setup_quality_score(
        vwap_distance_pct=0.015,
        volume_expansion=2.0,
        structure_clarity=0.8,
        spread_pct=0.002,
        pressure_score=0.90,
        rvol_strength=0.70,
    )
    assert high > low
