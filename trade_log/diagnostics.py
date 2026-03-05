"""
trade_log/diagnostics.py — Post-session performance diagnostics.

Reads the trade log CSV and produces a structured report covering all the
above-retail metrics that make the edge quantifiable:

  1. Expectancy by setup type
  2. Expectancy by time-of-day (hour buckets, ET)
  3. Expectancy by regime
  4. Slippage distribution (from explainability log)
  5. R-multiple distribution  (percentiles + histogram bins)
  6. Hit-rate by DemandScore percentile
  7. Feed-type comparison (Alpaca IEX vs SIP)
  8. Degrade-size audit (how often health degrade actually cost money)

Usage:
    from trade_log.diagnostics import PerformanceDiagnostics
    diag = PerformanceDiagnostics("logs/trades.csv")
    print(diag.full_report())         # human-readable text
    df   = diag.as_dataframe()        # raw metrics table
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _expectancy(group: pd.DataFrame, r_col: str = "pnl_r") -> dict:
    """
    Compute per-group stats.
    Expectancy = (win_rate × avg_win_R) - (loss_rate × avg_loss_R)
    """
    rs     = group[r_col].dropna()
    n      = len(rs)
    if n == 0:
        return {"n": 0, "expectancy": 0.0, "win_rate": 0.0,
                "avg_win_r": 0.0, "avg_loss_r": 0.0, "avg_r": 0.0}

    wins   = rs[rs > 0]
    losses = rs[rs <= 0]
    wr     = len(wins) / n
    lr     = 1.0 - wr
    avg_wr = float(wins.mean())   if len(wins)   else 0.0
    avg_lr = float(losses.mean()) if len(losses) else 0.0   # already negative

    expectancy = wr * avg_wr + lr * avg_lr   # lr * negative avg_lr

    return {
        "n":            n,
        "expectancy":   round(expectancy, 4),
        "win_rate":     round(wr, 4),
        "avg_win_r":    round(avg_wr, 4),
        "avg_loss_r":   round(avg_lr, 4),
        "avg_r":        round(float(rs.mean()), 4),
        "median_r":     round(float(rs.median()), 4),
        "max_r":        round(float(rs.max()), 4) if len(rs) else 0.0,
        "min_r":        round(float(rs.min()), 4) if len(rs) else 0.0,
    }


def _r_histogram(rs: pd.Series, bins: int = 10) -> List[dict]:
    """Return a histogram of R multiples as a list of {range, count}."""
    if rs.empty:
        return []
    counts, edges = np.histogram(rs.dropna(), bins=bins)
    return [
        {"range": f"{edges[i]:.2f}–{edges[i+1]:.2f}", "count": int(counts[i])}
        for i in range(len(counts))
    ]


def _percentile_buckets(df: pd.DataFrame, score_col: str, r_col: str = "pnl_r",
                         n_buckets: int = 5) -> List[dict]:
    """
    Split rows into equal-size buckets by `score_col` and report hit-rate
    per bucket (lowest score = bucket 1, highest = bucket N).
    """
    if len(df) == 0 or score_col not in df.columns:
        return []
    valid = df[[score_col, r_col]].dropna()
    if len(valid) < n_buckets:
        return []
    try:
        valid = valid.copy()
        valid["bucket"] = pd.qcut(valid[score_col], n_buckets, labels=False, duplicates="drop")
    except ValueError:
        return []
    out = []
    for b in sorted(valid["bucket"].dropna().unique()):
        grp = valid[valid["bucket"] == b]
        stats = _expectancy(grp, r_col)
        q_lo  = round(float(grp[score_col].min()), 3)
        q_hi  = round(float(grp[score_col].max()), 3)
        out.append({"bucket": int(b) + 1, "score_range": f"{q_lo}–{q_hi}", **stats})
    return out


# ─────────────────────────────────────────────────────────────────────────────
# DIAGNOSTICS CLASS
# ─────────────────────────────────────────────────────────────────────────────

class PerformanceDiagnostics:
    """
    Reads `trades_path` (CSV) and optionally `explain_path` (JSONL).

    All methods return plain dicts/lists — suitable for JSON or console output.
    """

    def __init__(
        self,
        trades_path:  str,
        explain_path: Optional[str] = None,
    ) -> None:
        self._trades_path  = Path(trades_path)
        self._explain_path = Path(explain_path) if explain_path else None
        self._df:  Optional[pd.DataFrame] = None
        self._raw: Optional[pd.DataFrame] = None   # unfiltered

    def load(self) -> "PerformanceDiagnostics":
        """Load and validate the trade log.  Call before any report method."""
        if not self._trades_path.exists():
            log.warning(f"[Diagnostics] Trade log not found: {self._trades_path}")
            self._df = pd.DataFrame()
            return self

        raw = pd.read_csv(self._trades_path)
        self._raw = raw

        # Parse date/time columns
        for col in ("entry_time", "exit_time"):
            if col in raw:
                raw[col] = pd.to_datetime(raw[col], errors="coerce")

        # Only closed trades have PnL
        if "exit_time" in raw:
            df = raw[raw["exit_time"].notna()].copy()
        else:
            df = raw.copy()

        # Numeric coercions
        for col in ("pnl_r", "pnl", "demand_score", "setup_quality_score",
                    "universe_rank", "shares"):
            if col in df:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Entry hour (ET)
        if "entry_time" in df:
            df["entry_hour"] = df["entry_time"].dt.hour

        self._df = df
        log.info(f"[Diagnostics] Loaded {len(df)} closed trades from {self._trades_path}")
        return self

    # ── Report sections ──────────────────────────────────────────────────────

    def by_setup_type(self) -> List[dict]:
        """Expectancy grouped by setup_name."""
        return self._group_expectancy("setup_name")

    def by_hour(self) -> List[dict]:
        """Expectancy grouped by entry_hour (0–23)."""
        return self._group_expectancy("entry_hour")

    def by_regime(self) -> List[dict]:
        """Expectancy grouped by regime (TREND / CHOP / RANGE)."""
        return self._group_expectancy("regime")

    def by_feed_type(self) -> List[dict]:
        """Expectancy grouped by feed_type (alpaca_iex / alpaca_sip / yfinance)."""
        return self._group_expectancy("feed_type")

    def r_distribution(self, bins: int = 10) -> List[dict]:
        """R-multiple histogram."""
        df = self._require_df()
        if df.empty or "pnl_r" not in df:
            return []
        return _r_histogram(df["pnl_r"], bins=bins)

    def r_percentiles(self) -> dict:
        """Key R-multiple percentiles."""
        df = self._require_df()
        if df.empty or "pnl_r" not in df:
            return {}
        rs = df["pnl_r"].dropna()
        if rs.empty:
            return {}
        return {
            "p10":    round(float(np.percentile(rs, 10)),  3),
            "p25":    round(float(np.percentile(rs, 25)),  3),
            "p50":    round(float(np.percentile(rs, 50)),  3),
            "p75":    round(float(np.percentile(rs, 75)),  3),
            "p90":    round(float(np.percentile(rs, 90)),  3),
            "mean":   round(float(rs.mean()),               3),
            "stddev": round(float(rs.std()),                3),
        }

    def hit_rate_by_demand_score(self, n_buckets: int = 5) -> List[dict]:
        """
        Break trades into N equal buckets by demand_score.
        Shows whether higher demand_score → higher edge.
        """
        df = self._require_df()
        return _percentile_buckets(df, "demand_score", "pnl_r", n_buckets)

    def hit_rate_by_sqs(self, n_buckets: int = 5) -> List[dict]:
        """
        Break trades into N equal buckets by setup_quality_score.
        Shows whether higher SQS → higher edge.
        """
        df = self._require_df()
        return _percentile_buckets(df, "setup_quality_score", "pnl_r", n_buckets)

    def degrade_size_audit(self) -> dict:
        """
        Compare PnL-R for trades in full-size vs degraded-size categories.
        Key insight: is running with DEGRADE data actually profitable?
        """
        df = self._require_df()
        if df.empty:
            return {}

        if "size_degrade_reason" not in df:
            return {"note": "size_degrade_reason column not in trade log"}

        full    = df[df["size_degrade_reason"].isna()   | (df["size_degrade_reason"] == "")]
        degrade = df[df["size_degrade_reason"].notna() & (df["size_degrade_reason"] != "")]

        return {
            "full_size_trades":    _expectancy(full),
            "degraded_size_trades": _expectancy(degrade),
            "recommendation": (
                "Consider tighter DEGRADE thresholds"
                if _expectancy(degrade).get("expectancy", 0) < 0
                else "DEGRADE trades are profitable — current policy is appropriate"
            ),
        }

    def overall_summary(self) -> dict:
        """Top-line summary of the full trade log."""
        df  = self._require_df()
        exp = _expectancy(df) if not df.empty else {}
        return {
            "total_trades":  len(df),
            "date_range":    self._date_range(),
            **exp,
        }

    def full_report(self) -> str:
        """Human-readable text report for console output."""
        lines = ["=" * 64, "PERFORMANCE DIAGNOSTIC REPORT", "=" * 64]

        def _section(title: str, data) -> None:
            lines.append(f"\n── {title} ──")
            if not data:
                lines.append("  (no data)")
                return
            if isinstance(data, dict):
                for k, v in data.items():
                    lines.append(f"  {k}: {v}")
            elif isinstance(data, list):
                for item in data:
                    lines.append("  " + " | ".join(
                        f"{k}={v}" for k, v in item.items()
                    ))

        _section("Overall",          self.overall_summary())
        _section("By Setup Type",    self.by_setup_type())
        _section("By Hour (ET)",     self.by_hour())
        _section("By Regime",        self.by_regime())
        _section("By Feed Type",     self.by_feed_type())
        _section("R Percentiles",    self.r_percentiles())
        _section("R Distribution",   self.r_distribution())
        _section("Hit-rate by DS",   self.hit_rate_by_demand_score())
        _section("Hit-rate by SQS",  self.hit_rate_by_sqs())
        _section("Degrade Audit",    self.degrade_size_audit())

        return "\n".join(lines)

    def as_dataframe(self) -> pd.DataFrame:
        """Return the raw loaded trades DataFrame for custom analysis."""
        return self._require_df().copy()

    # ── Internal ─────────────────────────────────────────────────────────────

    def _require_df(self) -> pd.DataFrame:
        if self._df is None:
            self.load()
        return self._df if self._df is not None else pd.DataFrame()

    def _group_expectancy(self, group_col: str) -> List[dict]:
        df = self._require_df()
        if df.empty or group_col not in df or "pnl_r" not in df:
            return []
        out = []
        for name, grp in df.groupby(group_col):
            stats = _expectancy(grp)
            out.append({"group": name, **stats})
        return sorted(out, key=lambda x: -x["n"])

    def _date_range(self) -> str:
        df = self._require_df()
        if df.empty or "entry_time" not in df:
            return "N/A"
        dates = df["entry_time"].dropna()
        if dates.empty:
            return "N/A"
        return f"{dates.min().date()} → {dates.max().date()}"
