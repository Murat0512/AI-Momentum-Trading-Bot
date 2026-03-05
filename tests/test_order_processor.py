from __future__ import annotations

import time
from unittest.mock import MagicMock

from config.settings import CONFIG
from execution.order_processor import ExecutionEngine


class TestOrderProcessor:
    def test_prepare_entry_rejects_stale_quote(self):
        engine = ExecutionEngine(MagicMock())
        quote = {
            "bid": 10.0,
            "ask": 10.01,
            "timestamp": time.time()
            - (CONFIG.execution.entry_quote_max_age_seconds + 1),
        }
        result = engine.prepare_entry("TSLA", 10.0, 100, quote, atr=0.2)
        assert result["action"] == "REJECT"
        assert "Stale quote" in result["reason"]

    def test_prepare_entry_rejects_runaway(self):
        engine = ExecutionEngine(MagicMock())
        quote = {
            "bid": 10.0,
            "ask": 10.5,
            "timestamp": time.time(),
        }
        result = engine.prepare_entry("TSLA", 10.0, 100, quote, atr=0.1)
        assert result["action"] == "REJECT"
        assert "Price ran away" in result["reason"]

    def test_prepare_entry_returns_execute_payload(self):
        engine = ExecutionEngine(MagicMock())
        quote = {
            "bid": 10.0,
            "ask": 10.01,
            "timestamp": time.time(),
        }
        result = engine.prepare_entry("TSLA", 10.0, 100, quote, atr=0.1)
        assert result["action"] == "EXECUTE"
        assert result["ticker"] == "TSLA"
        assert result["shares"] == 100
        assert "limit_price" in result

    def test_process_order_lifecycle_triggers_ttl_reprice(self):
        broker = MagicMock()
        broker.get_order_status.return_value = {"status": "open"}
        broker.cancel_and_reprice.return_value = {"status": "replaced"}

        engine = ExecutionEngine(broker)
        submission_time = time.time() - (
            CONFIG.order_manager.limit_order_ttl_seconds + 1
        )
        result = engine.process_order_lifecycle("OID-1", submission_time)

        broker.cancel_and_reprice.assert_called_once()
        assert result["status"] == "replaced"

    def test_log_slippage_sets_reduce_flag(self):
        engine = ExecutionEngine(MagicMock())
        bps = engine.log_slippage(
            "NVDA",
            target_price=10.0,
            actual_fill_price=10.05,
            risk_per_share=0.1,
        )
        assert bps > 0
        assert engine.should_reduce_size("NVDA") is True
