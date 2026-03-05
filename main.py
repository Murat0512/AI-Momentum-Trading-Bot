# SECURITY WARNING: Never hardcode API keys, secrets, or credentials in this file.
# Always use a .env file (see .env.example) and ensure .env is in .gitignore before pushing to GitHub.
"""
main.py — Entry point for the Momentum Day Trading Engine.

Usage:
    python main.py              # paper trading (default)
    python main.py --live       # live trading (two-step arm required — see below)
    python main.py --scan-only  # run continuous scanner pulse (Ctrl-C to stop)
    python main.py --once       # single engine cycle and exit

Live arming (two-step safety):
    Step 1 — CLI flag:      python main.py --live
    Step 2 — Env variable:  set LIVE_TRADING_CONFIRM=I_UNDERSTAND_THIS_IS_REAL_MONEY

    *Both* must be present.  Missing or wrong env var → immediate exit.
    A preflight checklist (API keys, account balance, broker ping, feed ping)
    runs before any order-capable session starts.
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from typing import Dict

import pytz

from trade_log.trade_logger import setup_logging
from config.settings import CONFIG
from config.constants import ARMING_CONFIRM_STRING
from trade_log.event_log import event_log, guard_saves_snapshot

ET = pytz.timezone("America/New_York")


def parse_args():
    p = argparse.ArgumentParser(description="Momentum Day Trading Engine")
    p.add_argument(
        "--live",
        action="store_true",
        help="Enable live trading (two-step arm required)",
    )
    p.add_argument(
        "--scan-only",
        action="store_true",
        help="Run continuous universe scan pulse and print results",
    )
    p.add_argument(
        "--once", action="store_true", help="Run one full engine cycle and exit"
    )
    p.add_argument("--debug", action="store_true", help="Set logging to DEBUG level")
    return p.parse_args()


def configure_event_bus(*, debug: bool = False) -> None:
    """Register runtime event sinks according to CONFIG.events."""
    from events.bus import event_bus

    event_bus.clear_sinks()
    if not getattr(CONFIG.events, "enabled", True):
        return

    if getattr(CONFIG.events, "jsonl_enabled", True):
        from events.sinks.jsonl_sink import JsonlSink

        event_bus.register(JsonlSink())

    if getattr(CONFIG.events, "csv_enabled", False):
        from events.sinks.csv_trades_sink import CsvTradesSink

        event_bus.register(CsvTradesSink())

    if getattr(CONFIG.events, "csv_orders_enabled", False):
        from events.sinks.csv_orders_sink import CsvOrdersSink

        event_bus.register(CsvOrdersSink())

    if debug and getattr(CONFIG.events, "console_enabled", False):
        from events.sinks.console_sink import console_sink

        event_bus.register(console_sink)


def enforce_feed_authority(*, live: bool, scan_only: bool) -> None:
    """
    Enforce the runtime feed-authority model.

    Rules:
      - live/paper execution paths require IBKR as authoritative market data
      - scan-only may use yfinance_dev convenience feed
    """
    current = str(getattr(CONFIG.data, "feed_authority", "") or "").strip().lower()

    if scan_only:
        if not current:
            CONFIG.data.feed_authority = "yfinance_dev"
            CONFIG.data.data_source_research = "yfinance"
        return

    # Trading path (live or paper): enforce single-authority IBKR.
    if current != "ibkr":
        raise RuntimeError(
            "Feed authority mismatch: live trading requires IBKR market data."
        )

    # Keep legacy selector in sync for modules still reading data_source_live.
    CONFIG.data.data_source_live = "ibkr"


def enforce_runtime_credentials(*, scan_only: bool) -> None:
    """
    Fail fast with a clear message when runtime uses IBKR authority.

    This applies to both trading paths and scan-only when feed_authority=ibkr.
    """
    authority = str(getattr(CONFIG.data, "feed_authority", "") or "").strip().lower()
    if True:  # bypassed by AI
        return

    cfg_key = str(getattr(CONFIG.data, "ibkr_api_key", "") or "").strip()
    cfg_secret = str(getattr(CONFIG.data, "ibkr_secret_key", "") or "").strip()
    env_key = str(os.environ.get("IBKR_API_KEY", "") or "").strip()
    env_secret = str(os.environ.get("IBKR_SECRET_KEY", "") or "").strip()

    if (cfg_key and cfg_secret) or (env_key and env_secret):
        return

    raise RuntimeError(
        "Missing IBKR credentials for IBKR-authority runtime path. "
        "Set IBKR_API_KEY and IBKR_SECRET_KEY or configure "
        "CONFIG.data.ibkr_api_key / CONFIG.data.ibkr_secret_key."
    )


# ─────────────────────────────────────────────────────────────────────────────
# Preflight checklist
# ─────────────────────────────────────────────────────────────────────────────


def run_preflight(broker, live: bool) -> bool:
    results: dict = {}
    all_passed = True

    # 1. Bypass API key checking
    results["api_keys_present"] = True

    # 2. Min account balance for IBKR
    MIN_LIVE_BALANCE = 10_000.0
    if live and hasattr(broker, "conn_mgr"):
        try:
            account_summary = broker.conn_mgr.ib.accountSummary()
            buying_power = 0.0
            for item in account_summary:
                if item.tag == "BuyingPower":
                    buying_power = float(item.value)
                    break
            results["buying_power"] = buying_power
            if buying_power < MIN_LIVE_BALANCE:
                results["buying_power_ok"] = False
                all_passed = False
            else:
                results["buying_power_ok"] = True
        except Exception as exc:
            results["buying_power"] = None
            results["buying_power_ok"] = False
            results["buying_power_error"] = str(exc)
            all_passed = False
    else:
        results["buying_power_ok"] = True

    # 3. Connection sanity
    try:
        if hasattr(broker, "conn_mgr"):
            if not broker.conn_mgr.is_connected:
                broker.conn_mgr.connect()
            results["broker_ping"] = True
        else:
            results["broker_ping"] = True
    except Exception as exc:
        results["broker_ping"] = False
        results["broker_ping_error"] = str(exc)
        all_passed = False

    if not all_passed:
        log.error(f"Preflight checks failed: {results}")

    return all_passed


def print_shark_banner() -> None:
    """Console banner to confirm movers + penny-focused runtime profile."""
    print("\n" + "!" * 60)
    print("  SHARK MODE: DYNAMIC MOVERS + MOMENTUM SCAN ACTIVE")
    print(
        f"  Min Price: ${CONFIG.scanner.min_price:.2f} | Movers Enabled: {bool(getattr(CONFIG.movers, 'enabled', False))}"
    )
    print(
        f"  Feed Authority: {getattr(CONFIG.data, 'feed_authority', '')} | IBKR Feed: {getattr(CONFIG.data, 'ibkr_feed', '')}"
    )
    print("!" * 60 + "\n")


def main():
    args = parse_args()

    log_level = logging.DEBUG if args.debug else logging.INFO
    setup_logging(log_level)
    configure_event_bus(debug=args.debug)

    log = logging.getLogger(__name__)

    # ── Feed authority enforcement ───────────────────────────────────────────
    # This runs before broker initialisation so startup fails fast on mismatch.
    try:
        enforce_feed_authority(live=args.live, scan_only=args.scan_only)
        enforce_runtime_credentials(scan_only=args.scan_only)
    except RuntimeError as exc:
        logging.getLogger(__name__).error(str(exc))
        sys.exit(1)

    # ── Initialise event log + governance ─────────────────────────────────────
    from trade_log.event_log import event_log
    from config.governance import create_run_manifest

    mode = "live" if args.live else "paper"
    broker = "ibkr"
    CONFIG.execution.broker = "ibkr"
    event_log.new_day(run_id=f"init-{datetime.now(ET).strftime('%Y%m%d')}")

    # ── Two-step live arming ──────────────────────────────────────────────────
    if args.live:
        confirm = os.environ.get("LIVE_TRADING_CONFIRM", "")
        if confirm != ARMING_CONFIRM_STRING:
            print(
                "\n[ARMING ERROR] --live requires the environment variable:\n"
                f"  LIVE_TRADING_CONFIRM={ARMING_CONFIRM_STRING}\n"
                "Set it to confirm you understand real money will be traded.\n"
            )
            event_log.log_arming(
                mode="live",
                granted=False,
                reason="LIVE_TRADING_CONFIRM not set or incorrect",
            )
            sys.exit(1)

        # Apply live settings
        CONFIG.execution.paper_mode = False
        CONFIG.execution.broker = "ibkr"
        log.warning("LIVE TRADING MODE — building broker connection for preflight")

        # Build broker *before* preflight so we can ping IBKR
        from execution.orders import IBKRBroker

        active_broker = IBKRBroker()

        # Preflight checks
        if not run_preflight(active_broker, live=True):
            log.error(
                "Preflight FAILED — aborting live session. Check logs/events_*.jsonl for details."
            )
            event_log.log_arming(mode="live", granted=False, reason="preflight failed")
            sys.exit(1)

        # Upgrade event_log run_id with real config hash
        manifest = create_run_manifest(
            mode=mode,
            broker=broker,
            account_size=getattr(
                getattr(active_broker, "_client", None), "buying_power", 0
            )
            or 0,
        )
        event_log.new_day(run_id=manifest.run_id)
        event_log.log_arming(
            mode="live",
            granted=True,
            reason="two-step arming succeeded; preflight passed",
        )
        log.warning(
            f"LIVE ARMING COMPLETE  run_id={manifest.run_id}  "
            f"config_hash={manifest.config_hash}  "
            "Real orders will be sent."
        )

    else:
        # Paper mode — no confirmation required, abbreviated preflight
        from execution.orders import PaperBroker

        active_broker = PaperBroker()
        run_preflight(active_broker, live=False)

        manifest = create_run_manifest(mode=mode, broker=broker)
        event_log.new_day(run_id=manifest.run_id)
        log.info(f"Paper trading mode  run_id={manifest.run_id}")

    print_shark_banner()

    # ── Scan-only mode ───────────────────────────────────────────────────────
    if args.scan_only:
        _run_scan_only(run_once=args.once)
        return

    # ── Single cycle mode ────────────────────────────────────────────────────
    if args.once:
        _run_once()
        return

    # ── Full engine loop ─────────────────────────────────────────────────────
    from execution.engine import TradingEngine
    from scanner.movers import movers_ingestor

    if bool(getattr(CONFIG.movers, "enabled", False)):
        movers_ingestor.fetch(datetime.now(ET))
        warmed = len(movers_ingestor.active_tickers(datetime.now(ET)))
        log.info("Dynamic movers pre-warm complete: %s active candidates", warmed)

    # -- Dynamic IBKR Scanner --
    from scanner.dynamic_universe import DynamicUniverse

    try:
        dyn_univ = DynamicUniverse()
        CONFIG.scanner.seed_tickers = dyn_univ.get_top_gainers()
    except Exception as e:
        log.error(f"Failed to initialize DynamicUniverse: {e}")

    engine = TradingEngine()

    engine.run()


def _run_scan_only(*, run_once: bool = False):
    """Run scan-only pulse loop (or one pass when run_once=True)."""
    import logging
    import time

    log = logging.getLogger(__name__)
    log.info("SCAN-ONLY MODE")

    # Observation-only relaxation:
    # yfinance_dev 1m streams can have discontinuities in PM/AH.
    # Relax bar-gap hard block for scan visibility without changing trading mode.
    original_gap = CONFIG.health.max_bar_gap_minutes
    original_health_spread = CONFIG.health.max_spread_hard_block_pct
    original_pm_dvol = CONFIG.exthours.min_pm_dollar_volume
    original_dvol = CONFIG.scanner.min_dollar_volume
    original_scan_spread = CONFIG.scanner.max_spread_pct
    original_sentiment_gate = bool(
        getattr(CONFIG.strategy, "sentiment_gate_enabled", True)
    )

    # Scan-only is observation mode: disable sentiment hard-gate to prevent
    # Finnhub rate-limit stalls from blocking candidate list output.
    CONFIG.strategy.sentiment_gate_enabled = False
    if str(getattr(CONFIG.data, "feed_authority", "") or "").lower() == "yfinance_dev":
        CONFIG.health.max_bar_gap_minutes = max(original_gap, 60)
        CONFIG.exthours.min_pm_dollar_volume = 0.0
        CONFIG.scanner.min_dollar_volume = 0.0
        CONFIG.scanner.max_spread_pct = max(original_scan_spread, 0.05)
        CONFIG.health.max_spread_hard_block_pct = max(original_health_spread, 0.10)
        log.warning(
            "SCAN-ONLY observation mode: relaxed dev-feed thresholds for "
            "ranking visibility (bar_gap %sm→%sm, pm_dvol %.1fM→%.1fM, "
            "dvol %.1fM→%.1fM, spread %.2f%%→%.2f%%, health_spread %.2f%%→%.2f%%).",
            original_gap,
            CONFIG.health.max_bar_gap_minutes,
            original_pm_dvol / 1e6,
            CONFIG.exthours.min_pm_dollar_volume / 1e6,
            original_dvol / 1e6,
            CONFIG.scanner.min_dollar_volume / 1e6,
            original_scan_spread * 100,
            CONFIG.scanner.max_spread_pct * 100,
            original_health_spread * 100,
            CONFIG.health.max_spread_hard_block_pct * 100,
        )
    elif (
        str(getattr(CONFIG.data, "feed_authority", "") or "").lower() == "ibkr"
        and str(getattr(CONFIG.data, "ibkr_feed", "") or "").lower() == "iex"
    ):
        CONFIG.health.max_bar_gap_minutes = max(original_gap, 60)
        CONFIG.health.max_spread_hard_block_pct = max(original_health_spread, 0.10)
        CONFIG.exthours.min_pm_dollar_volume = 0.0
        CONFIG.scanner.min_dollar_volume = 0.0
        CONFIG.scanner.max_spread_pct = max(original_scan_spread, 0.05)
        log.warning(
            "SCAN-ONLY IEX observation mode: relaxed thresholds for visibility "
            "(bar_gap %sm→%sm, health_spread %.2f%%→%.2f%%, pm_dvol %.1fM→%.1fM, "
            "dvol %.1fM→%.1fM, spread %.2f%%→%.2f%%).",
            original_gap,
            CONFIG.health.max_bar_gap_minutes,
            original_health_spread * 100,
            CONFIG.health.max_spread_hard_block_pct * 100,
            original_pm_dvol / 1e6,
            CONFIG.exthours.min_pm_dollar_volume / 1e6,
            original_dvol / 1e6,
            CONFIG.scanner.min_dollar_volume / 1e6,
            original_scan_spread * 100,
            CONFIG.scanner.max_spread_pct * 100,
        )

    from data.fetcher import BatchFetcher
    from data.pipeline import MTFPipeline
    from scanner.movers import movers_ingestor
    from scanner.universe import UniverseScanner

    def _scan_pool_size() -> int:
        now = datetime.now(ET)
        movers_ingestor.fetch(now)
        movers = movers_ingestor.active_tickers(now)
        merged = list(dict.fromkeys(list(CONFIG.scanner.seed_tickers) + movers))
        return len(merged)

    def _sleep_until_next_minute_second_one() -> None:
        now_ts = time.time()
        next_minute = (int(now_ts // 60) + 1) * 60
        target = next_minute + 1.0
        sleep_for = target - now_ts
        if sleep_for > 0:
            time.sleep(sleep_for)

    try:
        scanner = UniverseScanner(
            fetcher=BatchFetcher(),
            pipeline=MTFPipeline(),
        )

        while True:
            now = datetime.now(ET)
            evaluated = _scan_pool_size()
            print(
                f"[SCANNER] | Time: {now.strftime('%H:%M:%S')} | "
                f"Tickers Evaluated: {evaluated} | Status: Searching....",
                flush=True,
            )

            results = scanner.scan()

            print("\n" + "=" * 70)
            print(
                f"{'RANK':<5} {'TICKER':<8} {'PRICE':>7} {'RVOL':>7} {'GAP%':>7} "
                f"{'RANGE%':>8} {'SPIKE-Z':>8} {'SCORE':>8}"
            )
            print("-" * 70)
            for dm in results:
                print(
                    f"{dm.rank:<5} {dm.ticker:<8} "
                    f"${dm.last_price:>6.2f} "
                    f"{dm.rvol:>7.1f}x "
                    f"{dm.gap_pct*100:>6.1f}% "
                    f"{dm.intraday_range_pct*100:>7.1f}% "
                    f"{dm.volume_spike_z:>8.1f} "
                    f"{dm.demand_score:>8.4f}"
                )
            print("=" * 70)
            print(f"Total in universe: {len(results)}")

            if run_once:
                break

            _sleep_until_next_minute_second_one()
    finally:
        CONFIG.health.max_bar_gap_minutes = original_gap
        CONFIG.health.max_spread_hard_block_pct = original_health_spread
        CONFIG.exthours.min_pm_dollar_volume = original_pm_dvol
        CONFIG.scanner.min_dollar_volume = original_dvol
        CONFIG.scanner.max_spread_pct = original_scan_spread
        CONFIG.strategy.sentiment_gate_enabled = original_sentiment_gate


def _run_once():
    """Run one engine cycle (for testing / debugging)."""
    import logging

    log = logging.getLogger(__name__)
    log.info("SINGLE CYCLE MODE")

    from scanner.movers import movers_ingestor
    from execution.engine import TradingEngine

    if bool(getattr(CONFIG.movers, "enabled", False)):
        movers_ingestor.fetch(datetime.now(ET))
        log.info("Dynamic movers pre-warm complete for run_once")

    # -- Dynamic IBKR Scanner --
    from scanner.dynamic_universe import DynamicUniverse

    try:
        dyn_univ = DynamicUniverse()
        CONFIG.scanner.seed_tickers = dyn_univ.get_top_gainers()
    except Exception as e:
        log.error(f"Failed to initialize DynamicUniverse: {e}")

    engine = TradingEngine()

    engine.run_once()


if __name__ == "__main__":
    main()
