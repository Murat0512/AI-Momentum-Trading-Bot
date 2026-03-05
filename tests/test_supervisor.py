"""
tests/test_supervisor.py — Unit tests for execution.supervisor.
"""

import pytest

from config.settings import CONFIG
from execution.supervisor import (
    CycleMetrics,
    MarketState,
    SupervisorOutput,
    evaluate_market_state,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _metrics(**kwargs):
    """Build a CycleMetrics with healthy defaults, overriding specific fields."""
    defaults = dict(
        bar_latency_seconds=10.0,
        missing_bar_pct=0.02,
        median_spread_pct=0.002,
        p90_spread_pct=0.004,
        rejection_rate_10c=0.05,
        slippage_incidents=0,
        rolling_pnl_slope=0.10,
        drawdown_velocity=0.0,
    )
    defaults.update(kwargs)
    return CycleMetrics(**defaults)


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────


class TestSupervisorNormal:
    def test_normal_state_all_healthy(self):
        result = evaluate_market_state(_metrics())
        assert result.state == MarketState.NORMAL
        assert result.size_mult == 1.0
        assert result.spread_mult == 1.0

    def test_pure_function_no_side_effects(self):
        m = _metrics()
        r1 = evaluate_market_state(m)
        r2 = evaluate_market_state(m)
        assert r1 == r2


class TestSupervisorCaution:
    def test_caution_on_high_median_spread(self):
        cfg = CONFIG.supervisor
        result = evaluate_market_state(
            _metrics(median_spread_pct=cfg.caution_median_spread + 0.001)
        )
        assert result.state == MarketState.CAUTION
        assert result.size_mult == cfg.caution_size_mult

    def test_caution_on_bar_latency(self):
        cfg = CONFIG.supervisor
        result = evaluate_market_state(
            _metrics(bar_latency_seconds=cfg.caution_bar_latency + 1.0)
        )
        assert result.state == MarketState.CAUTION

    def test_caution_on_slippage_incidents(self):
        cfg = CONFIG.supervisor
        result = evaluate_market_state(
            _metrics(slippage_incidents=cfg.caution_slip_incidents)
        )
        assert result.state == MarketState.CAUTION


class TestSupervisorDefensive:
    def test_defensive_on_p90_spread(self):
        cfg = CONFIG.supervisor
        result = evaluate_market_state(
            _metrics(p90_spread_pct=cfg.defensive_p90_spread + 0.001)
        )
        assert result.state == MarketState.DEFENSIVE
        assert result.size_mult == cfg.defensive_size_mult

    def test_defensive_on_missing_bars(self):
        cfg = CONFIG.supervisor
        result = evaluate_market_state(
            _metrics(missing_bar_pct=cfg.defensive_missing_bar_pct + 0.01)
        )
        assert result.state == MarketState.DEFENSIVE

    def test_defensive_on_pnl_slope(self):
        cfg = CONFIG.supervisor
        result = evaluate_market_state(
            _metrics(
                rolling_pnl_slope=cfg.defensive_pnl_slope - 0.1  # more negative = worse
            )
        )
        assert result.state == MarketState.DEFENSIVE
        assert result.min_sqs == cfg.defensive_min_sqs


class TestSupervisorHaltEntries:
    def test_halt_on_rejection_rate(self):
        cfg = CONFIG.supervisor
        result = evaluate_market_state(
            _metrics(rejection_rate_10c=cfg.halt_rejection_rate + 0.01)
        )
        assert result.state == MarketState.HALT_ENTRIES
        assert result.size_mult == 0.0

    def test_halt_on_drawdown_velocity(self):
        cfg = CONFIG.supervisor
        result = evaluate_market_state(
            _metrics(
                drawdown_velocity=cfg.halt_drawdown_velocity - 0.001  # more negative
            )
        )
        assert result.state == MarketState.HALT_ENTRIES
        assert result.size_mult == 0.0

    def test_halt_priority_over_defensive(self):
        """HALT_ENTRIES conditions must win over DEFENSIVE conditions."""
        cfg = CONFIG.supervisor
        result = evaluate_market_state(
            _metrics(
                rejection_rate_10c=cfg.halt_rejection_rate + 0.05,
                p90_spread_pct=cfg.defensive_p90_spread + 0.01,
            )
        )
        assert result.state == MarketState.HALT_ENTRIES

    def test_defensive_priority_over_caution(self):
        """DEFENSIVE must win over CAUTION when both triggered."""
        cfg = CONFIG.supervisor
        result = evaluate_market_state(
            _metrics(
                p90_spread_pct=cfg.defensive_p90_spread + 0.01,
                median_spread_pct=cfg.caution_median_spread + 0.001,
            )
        )
        assert result.state == MarketState.DEFENSIVE


class TestSupervisorOutputImmutability:
    def test_output_is_frozen(self):
        result = evaluate_market_state(_metrics())
        with pytest.raises((AttributeError, TypeError)):
            result.state = MarketState.HALT_ENTRIES  # type: ignore[misc]

    def test_trigger_is_set(self):
        result = evaluate_market_state(_metrics())
        assert isinstance(result.trigger, str)
        assert len(result.trigger) > 0
