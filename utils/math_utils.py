"""
math_utils.py — Shared math / statistics helpers.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from config.constants import EPSILON


def z_score(series: pd.Series, window: int = 20) -> pd.Series:
    """Rolling z-score of a series."""
    mu  = series.rolling(window).mean()
    std = series.rolling(window).std().replace(0, EPSILON)
    return (series - mu) / std


def pct_change(a: float, b: float) -> float:
    """Signed percent change from b to a: (a-b)/b"""
    if abs(b) < EPSILON:
        return 0.0
    return (a - b) / b


def clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def safe_div(numerator: float, denominator: float, default: float = 0.0) -> float:
    if abs(denominator) < EPSILON:
        return default
    return numerator / denominator


def round_to_tick(price: float, tick: float = 0.01) -> float:
    """Round price to nearest tick size."""
    return round(round(price / tick) * tick, len(str(tick).split(".")[-1]))


def annualized_sharpe(returns: pd.Series, risk_free: float = 0.0, periods_per_year: int = 252) -> float:
    """Annualized Sharpe ratio from daily returns."""
    excess = returns - risk_free / periods_per_year
    if excess.std() < EPSILON:
        return 0.0
    return float((excess.mean() / excess.std()) * np.sqrt(periods_per_year))


def max_drawdown(equity_curve: pd.Series) -> float:
    """Maximum drawdown as a fraction of peak equity."""
    roll_max = equity_curve.cummax()
    dd = (equity_curve - roll_max) / (roll_max + EPSILON)
    return float(dd.min())


def expectancy(wins: list[float], losses: list[float]) -> float:
    """
    Trading expectancy = (win_rate × avg_win) - (loss_rate × avg_loss).
    Returns value in R multiples.
    """
    total = len(wins) + len(losses)
    if total == 0:
        return 0.0
    win_rate  = len(wins)   / total
    loss_rate = len(losses) / total
    avg_win   = np.mean(wins)   if wins   else 0.0
    avg_loss  = abs(np.mean(losses)) if losses else 0.0
    return win_rate * avg_win - loss_rate * avg_loss
