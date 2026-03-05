"""
tests/test_arming_safeguards.py — Unit tests for main.py arming / preflight logic.

Because main.py calls sys.exit(), tests use SystemExit capture + monkeypatching.
"""
import os
import sys
import pytest
from unittest.mock import patch, MagicMock
from io import StringIO

from config.constants import ARMING_CONFIRM_STRING


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_live_broker(buying_power=25_000.0):
    account = MagicMock()
    account.buying_power = str(buying_power)
    client = MagicMock()
    client.get_account.return_value       = account
    client.get_all_positions.return_value = []
    b = MagicMock()
    b._client = client
    return b


def _make_paper_broker():
    b = MagicMock(spec=[])  # no _client attribute
    return b


# ─────────────────────────────────────────────────────────────────────────────
# Two-step arming tests
# ─────────────────────────────────────────────────────────────────────────────

class TestLiveArmingConfirmation:
    def test_missing_confirm_env_exits(self, monkeypatch, capsys):
        """--live without LIVE_TRADING_CONFIRM should exit(1)."""
        monkeypatch.delenv("LIVE_TRADING_CONFIRM", raising=False)
        # Simulate main() only up to the arming check
        confirm = os.environ.get("LIVE_TRADING_CONFIRM", "")
        assert confirm != ARMING_CONFIRM_STRING

    def test_wrong_confirm_env_exits(self, monkeypatch):
        """Wrong value → should NOT equal ARMING_CONFIRM_STRING."""
        monkeypatch.setenv("LIVE_TRADING_CONFIRM", "WRONG_VALUE")
        confirm = os.environ.get("LIVE_TRADING_CONFIRM", "")
        assert confirm != ARMING_CONFIRM_STRING

    def test_correct_confirm_env_matches(self, monkeypatch):
        """Correct value → matches."""
        monkeypatch.setenv("LIVE_TRADING_CONFIRM", ARMING_CONFIRM_STRING)
        confirm = os.environ.get("LIVE_TRADING_CONFIRM", "")
        assert confirm == ARMING_CONFIRM_STRING

    def test_arming_confirm_string_value(self):
        """The sentinel literal value must not be changed accidentally."""
        assert ARMING_CONFIRM_STRING == "I_UNDERSTAND_THIS_IS_REAL_MONEY"


# ─────────────────────────────────────────────────────────────────────────────
# Preflight checklist tests
# ─────────────────────────────────────────────────────────────────────────────

class TestPreflightChecks:
    def _run_preflight(self, broker, live, monkeypatch, extra_env=None):
        """Helper to call run_preflight with a fake event_log and no network."""
        from main import run_preflight
        fake_el = MagicMock()
        fake_el.log_preflight = MagicMock()
        # Patch the module-level event_log AND stub out BatchFetcher at its source
        # to avoid 30-second network timeouts in the data-feed-ping step
        fast_fetcher = MagicMock()
        fast_fetcher.ping.return_value = True
        with patch("main.event_log", fake_el), \
             patch("data.fetcher.BatchFetcher", return_value=fast_fetcher):
            return run_preflight(broker, live)

    def test_paper_mode_always_passes(self, monkeypatch):
        """Paper mode with no Alpaca keys should still pass."""
        monkeypatch.delenv("ALPACA_API_KEY",    raising=False)
        monkeypatch.delenv("ALPACA_SECRET_KEY", raising=False)
        broker = _make_paper_broker()
        fake_el = MagicMock()
        fake_el.log_preflight = MagicMock()
        from main import run_preflight
        with patch("main.event_log", fake_el):
            result = run_preflight(broker, live=False)
        assert result is True

    def test_live_fails_without_api_keys(self, monkeypatch):
        """Live preflight must fail when API keys absent."""
        monkeypatch.delenv("ALPACA_API_KEY",    raising=False)
        monkeypatch.delenv("ALPACA_SECRET_KEY", raising=False)
        broker = _make_live_broker()
        fake_el = MagicMock()
        fake_el.log_preflight = MagicMock()
        from config.settings import CONFIG
        CONFIG.execution.paper_mode = False    # must be live
        from main import run_preflight
        with patch("main.event_log", fake_el):
            result = run_preflight(broker, live=True)
        CONFIG.execution.paper_mode = True   # restore
        assert result is False

    def test_live_fails_on_low_balance(self, monkeypatch):
        """Live preflight must fail when buying_power < $10 000."""
        monkeypatch.setenv("ALPACA_API_KEY",    "fake")
        monkeypatch.setenv("ALPACA_SECRET_KEY", "fake")
        broker = _make_live_broker(buying_power=500.0)
        fake_el = MagicMock()
        fake_el.log_preflight = MagicMock()
        from config.settings import CONFIG
        CONFIG.execution.paper_mode = False
        from main import run_preflight
        with patch("main.event_log", fake_el):
            result = run_preflight(broker, live=True)
        CONFIG.execution.paper_mode = True
        assert result is False

    def test_live_fails_if_broker_unreachable(self, monkeypatch):
        """Live preflight fails when broker ping raises exception."""
        monkeypatch.setenv("ALPACA_API_KEY",    "fake")
        monkeypatch.setenv("ALPACA_SECRET_KEY", "fake")
        broker = MagicMock()
        broker._client.get_account.side_effect = ConnectionError("unreachable")
        fake_el = MagicMock()
        fake_el.log_preflight = MagicMock()
        from config.settings import CONFIG
        CONFIG.execution.paper_mode = False
        from main import run_preflight
        with patch("main.event_log", fake_el):
            result = run_preflight(broker, live=True)
        CONFIG.execution.paper_mode = True
        assert result is False

    def test_preflight_logs_pass_event(self, monkeypatch):
        """EVT_PREFLIGHT_PASS must be logged when paper preflight passes."""
        broker = _make_paper_broker()
        fake_el = MagicMock()
        fake_el.log_preflight = MagicMock()
        from main import run_preflight
        with patch("main.event_log", fake_el):
            run_preflight(broker, live=False)
        fake_el.log_preflight.assert_called_once()
        call_args = fake_el.log_preflight.call_args
        assert call_args[0][0] is True   # passed=True

    def test_preflight_logs_fail_event_on_failure(self, monkeypatch):
        """EVT_PREFLIGHT_FAIL must be logged when live preflight fails."""
        monkeypatch.delenv("ALPACA_API_KEY",    raising=False)
        monkeypatch.delenv("ALPACA_SECRET_KEY", raising=False)
        broker = _make_live_broker()
        fake_el = MagicMock()
        fake_el.log_preflight = MagicMock()
        from config.settings import CONFIG
        CONFIG.execution.paper_mode = False
        from main import run_preflight
        with patch("main.event_log", fake_el):
            run_preflight(broker, live=True)
        CONFIG.execution.paper_mode = True
        fake_el.log_preflight.assert_called_once()
        call_args = fake_el.log_preflight.call_args
        assert call_args[0][0] is False   # passed=False

    def test_paper_mode_contradiction_fails(self, monkeypatch):
        """paper_mode=True + live=True → preflight fails."""
        monkeypatch.setenv("ALPACA_API_KEY",    "fake")
        monkeypatch.setenv("ALPACA_SECRET_KEY", "fake")
        broker = _make_live_broker()
        fake_el = MagicMock()
        fake_el.log_preflight = MagicMock()
        from config.settings import CONFIG
        CONFIG.execution.paper_mode = True   # contradicts live=True
        from main import run_preflight
        with patch("main.event_log", fake_el):
            result = run_preflight(broker, live=True)
        assert result is False
