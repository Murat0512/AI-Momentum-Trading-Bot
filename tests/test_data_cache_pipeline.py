from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytz

from config.constants import TF_1M
from data.cache import BarCache
from data.pipeline import get_last_closed_low

ET = pytz.timezone("America/New_York")


def _bar(ts: datetime, o: float, h: float, l: float, c: float, v: float) -> pd.Series:
    return pd.Series(
        {"open": o, "high": h, "low": l, "close": c, "volume": v},
        name=ts,
    )


def test_append_1m_bar_appends_and_updates_in_place():
    cache = BarCache()
    i0 = ET.localize(datetime(2026, 3, 4, 9, 30))
    i1 = ET.localize(datetime(2026, 3, 4, 9, 31))
    i2 = ET.localize(datetime(2026, 3, 4, 9, 32))

    df = pd.DataFrame(
        {
            "open": [100.0, 100.2],
            "high": [100.3, 100.4],
            "low": [99.9, 100.1],
            "close": [100.2, 100.3],
            "volume": [1000, 1100],
        },
        index=[i0, i1],
    )

    cache.set("TEST", {TF_1M: df.copy()})
    cache.append_1m_bar("TEST", _bar(i2, 100.3, 100.5, 100.2, 100.4, 1200))
    cache.append_1m_bar("TEST", _bar(i1, 100.2, 100.45, 100.0, 100.35, 1300))

    out = cache.get_tf("TEST", TF_1M)
    assert out is not None
    assert len(out) == 3
    assert float(out.loc[i1, "high"]) == 100.45
    assert out.index.is_monotonic_increasing


def test_get_last_closed_low_returns_iloc_minus_2(monkeypatch):
    i0 = ET.localize(datetime(2026, 3, 4, 10, 0))
    i1 = ET.localize(datetime(2026, 3, 4, 10, 1))
    i2 = ET.localize(datetime(2026, 3, 4, 10, 2))

    df = pd.DataFrame(
        {
            "open": [10.0, 10.1, 10.2],
            "high": [10.2, 10.3, 10.4],
            "low": [9.9, 9.7, 9.5],
            "close": [10.1, 10.2, 10.3],
            "volume": [100, 200, 300],
        },
        index=[i0, i1, i2],
    )

    class _StubCache:
        def get_tf(self, ticker: str, timeframe: str):
            return df

    import data.cache as cache_mod

    monkeypatch.setattr(cache_mod, "bar_cache", _StubCache())

    val = get_last_closed_low("TEST", TF_1M)
    assert val == 9.7
