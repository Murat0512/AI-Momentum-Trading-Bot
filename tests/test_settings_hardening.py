"""
tests/test_settings_hardening.py — Verifies all settings.py hardening improvements.

Covers:
  ArmingConfig:
    1.  live_trading defaults to False (paper-safe)
    2.  require_preflight defaults to True
    3.  confirm_live_trading_phrase matches ARMING_CONFIRM_STRING in constants
    4.  ArmingConfig is accessible via CONFIG.arming

    Risk defaults:
        5.  baseline risk_per_trade_pct = 1.0%
    6.  safe_mode risk_per_trade_pct is tighter than baseline
    7.  safe_mode risk is exactly 0.25%

  Session boundary unification:
    8.  ExtendedHoursConfig.pm_end == SessionConfig.premarket_end (09:30)
    9.  Both values are "09:30" (not the old "09:29")

  Session-aware dollar volume:
    10. min_rth_dollar_volume exists and is > 0
    11. min_ah_dollar_volume exists and is > 0
    12. min_rth_dollar_volume > min_ah_dollar_volume (RTH stricter than AH)
    13. min_pm_dollar_volume still exists

  min_bars_today semantics:
    14. DataConfig.min_bars_today == 5 (preserved value)

  News naming:
    15. max_news_score_boost attribute exists on NewsConfig
    16. max_news_catalyst_score attribute does NOT exist (clean rename)
    17. news_score_cap matches max_news_score_boost in value

  effective_spread_cap:
    18. RTH TREND scan == scanner.max_spread_pct
    19. RTH TREND entry == decision.max_spread_pct_entry
    20. RTH CHOP entry < RTH TREND entry
    21. PM entry == exthours.max_pm_spread_pct (no regime mult)
    22. AH entry == exthours.max_ah_spread_pct (no regime mult)
    23. PM chop == PM trend (regime irrelevant in PM)
    24. RTH CHOP scan applies regime.chop_spread_multiplier
    25. rth_spread_cap_for_phase shortcut returns correct value
"""

from __future__ import annotations

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from config.settings import CONFIG, ArmingConfig, SessionConfig, ExtendedHoursConfig
from config.constants import (
    ARMING_CONFIRM_STRING, SESSION_PREMARKET, SESSION_RTH, SESSION_AFTERHOURS,
    REGIME_CHOP,
)
from utils.spread_policy import effective_spread_cap, rth_spread_cap_for_phase


# ─────────────────────────────────────────────────────────────────────────────
# ArmingConfig
# ─────────────────────────────────────────────────────────────────────────────

class TestArmingConfig:
    def test_live_trading_default_false(self):
        """live_trading must default False — system starts in paper-safe mode."""
        assert ArmingConfig().live_trading is False

    def test_require_preflight_default_true(self):
        """Preflight checks are mandatory by default."""
        assert ArmingConfig().require_preflight is True

    def test_confirm_phrase_matches_constant(self):
        """ArmingConfig phrase must match the ARMING_CONFIRM_STRING constant."""
        assert ArmingConfig().confirm_live_trading_phrase == ARMING_CONFIRM_STRING

    def test_arming_accessible_via_config(self):
        """CONFIG.arming is wired into MasterConfig."""
        assert hasattr(CONFIG, "arming")
        assert isinstance(CONFIG.arming, ArmingConfig)

    def test_config_arming_live_trading_default(self):
        """The singleton CONFIG also has live_trading=False."""
        assert CONFIG.arming.live_trading is False


# ─────────────────────────────────────────────────────────────────────────────
# Risk defaults
# ─────────────────────────────────────────────────────────────────────────────

class TestRiskDefaults:
    def test_baseline_risk_is_one_percent(self):
        """Baseline risk is 1.0% in fixed-R mode."""
        assert CONFIG.risk.risk_per_trade_pct == pytest.approx(0.01)

    def test_safe_mode_risk_tighter_than_baseline(self):
        """SafeMode risk_per_trade_pct must be strictly less than baseline."""
        assert CONFIG.safe_mode.risk_per_trade_pct < CONFIG.risk.risk_per_trade_pct

    def test_safe_mode_risk_is_quarter_percent(self):
        """SafeMode risk is 0.25% — half of the 0.5% baseline."""
        assert CONFIG.safe_mode.risk_per_trade_pct == pytest.approx(0.0025)


# ─────────────────────────────────────────────────────────────────────────────
# Session boundary unification
# ─────────────────────────────────────────────────────────────────────────────

class TestSessionBoundaryUnification:
    def test_pm_end_matches_premarket_end(self):
        """ExtendedHoursConfig.pm_end and SessionConfig.premarket_end must agree."""
        assert CONFIG.exthours.pm_end == CONFIG.session.premarket_end

    def test_pm_end_is_09_30(self):
        """Both must be 09:30 (old value was 09:29 — off-by-one risk)."""
        assert CONFIG.exthours.pm_end == "09:30"

    def test_premarket_end_is_09_30(self):
        assert CONFIG.session.premarket_end == "09:30"


# ─────────────────────────────────────────────────────────────────────────────
# Session-aware dollar volume
# ─────────────────────────────────────────────────────────────────────────────

class TestSessionAwareDollarVolume:
    def test_min_rth_dollar_volume_exists(self):
        assert hasattr(CONFIG.exthours, "min_rth_dollar_volume")
        assert CONFIG.exthours.min_rth_dollar_volume > 0

    def test_min_ah_dollar_volume_exists(self):
        assert hasattr(CONFIG.exthours, "min_ah_dollar_volume")
        assert CONFIG.exthours.min_ah_dollar_volume > 0

    def test_rth_stricter_than_ah(self):
        """RTH min dollar volume should exceed AH threshold."""
        assert CONFIG.exthours.min_rth_dollar_volume > CONFIG.exthours.min_ah_dollar_volume

    def test_pm_dollar_volume_preserved(self):
        """Existing min_pm_dollar_volume must still exist."""
        assert hasattr(CONFIG.exthours, "min_pm_dollar_volume")
        assert CONFIG.exthours.min_pm_dollar_volume > 0


# ─────────────────────────────────────────────────────────────────────────────
# min_bars_today (soft gate preserved)
# ─────────────────────────────────────────────────────────────────────────────

class TestMinBarsToday:
    def test_value_preserved(self):
        assert CONFIG.data.min_bars_today == 5


# ─────────────────────────────────────────────────────────────────────────────
# News naming consistency
# ─────────────────────────────────────────────────────────────────────────────

class TestNewsNaming:
    def test_max_news_score_boost_exists(self):
        assert hasattr(CONFIG.news, "max_news_score_boost")

    def test_old_max_news_catalyst_score_gone(self):
        """Renamed field must no longer exist under the old name."""
        assert not hasattr(CONFIG.news, "max_news_catalyst_score")

    def test_universe_news_score_cap_matches_boost(self):
        """UniverseConfig.news_score_cap must equal NewsConfig.max_news_score_boost."""
        assert CONFIG.universe.news_score_cap == pytest.approx(
            CONFIG.news.max_news_score_boost
        )


# ─────────────────────────────────────────────────────────────────────────────
# effective_spread_cap
# ─────────────────────────────────────────────────────────────────────────────

class TestEffectiveSpreadCap:
    def test_rth_trend_scan_equals_scanner_cap(self):
        cap = effective_spread_cap("RTH", "TREND", "scan")
        assert cap == pytest.approx(CONFIG.scanner.max_spread_pct)

    def test_rth_trend_entry_equals_decision_cap(self):
        cap = effective_spread_cap("RTH", "TREND", "entry")
        assert cap == pytest.approx(CONFIG.decision.max_spread_pct_entry)

    def test_rth_chop_entry_is_tighter_than_trend(self):
        trend = effective_spread_cap("RTH", "TREND", "entry")
        chop  = effective_spread_cap("RTH", "CHOP",  "entry")
        assert chop < trend

    def test_rth_chop_entry_applies_decision_multiplier(self):
        cap = effective_spread_cap("RTH", "CHOP", "entry")
        expected = CONFIG.decision.max_spread_pct_entry * CONFIG.decision.chop_spread_multiplier
        assert cap == pytest.approx(expected)

    def test_rth_chop_scan_applies_regime_multiplier(self):
        cap = effective_spread_cap("RTH", "CHOP", "scan")
        expected = CONFIG.scanner.max_spread_pct * CONFIG.regime.chop_spread_multiplier
        assert cap == pytest.approx(expected)

    def test_pm_returns_pm_spread_cap(self):
        cap = effective_spread_cap("PREMARKET", "TREND", "entry")
        assert cap == pytest.approx(CONFIG.exthours.max_pm_spread_pct)

    def test_ah_returns_ah_spread_cap(self):
        cap = effective_spread_cap("AFTERHOURS", "TREND", "entry")
        assert cap == pytest.approx(CONFIG.exthours.max_ah_spread_pct)

    def test_pm_regime_irrelevant(self):
        """PM spread cap must be identical regardless of regime."""
        assert effective_spread_cap("PREMARKET", "TREND", "entry") == pytest.approx(
            effective_spread_cap("PREMARKET", "CHOP", "entry")
        )

    def test_ah_regime_irrelevant(self):
        """AH spread cap must be identical regardless of regime."""
        assert effective_spread_cap("AFTERHOURS", "TREND", "entry") == pytest.approx(
            effective_spread_cap("AFTERHOURS", "CHOP", "entry")
        )

    def test_case_insensitive_session(self):
        """Session string is case-insensitive."""
        assert effective_spread_cap("rth",       "TREND", "entry") == pytest.approx(
               effective_spread_cap("RTH",       "TREND", "entry"))
        assert effective_spread_cap("premarket", "TREND", "entry") == pytest.approx(
               effective_spread_cap("PREMARKET", "TREND", "entry"))

    def test_shortcut_rth_spread_cap_for_phase(self):
        """rth_spread_cap_for_phase shortcut matches effective_spread_cap directly."""
        assert rth_spread_cap_for_phase("entry", "TREND") == pytest.approx(
            effective_spread_cap("RTH", "TREND", "entry"))
        assert rth_spread_cap_for_phase("entry", "CHOP") == pytest.approx(
            effective_spread_cap("RTH", "CHOP", "entry"))
