"""
universe.py — Dynamic Universe Scanner orchestrator.

Cycle:
  1. Fetch 1m bars for seed tickers (incl. extended hours)
  2. Apply DATA_HEALTH validation per ticker (BLOCK / DEGRADE / OK)
  3. Compute demand metrics using session-aware RVOL
  4. Apply hard filters (spread uses session-appropriate threshold)
  5. Score with DemandScore
  6. Rank → return Top 15

Output: List[DemandMetrics] sorted by DemandScore descending
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import pytz

from config.constants import (
    BLOCK_HALT_DETECTED,
    BLOCK_PM_DOLLAR_VOLUME,
    BLOCK_PM_FAKE_VOLUME,
    DEGRADE_IEX_PM_COVERAGE,
    DH_BLOCK,
    FEED_ALPACA_IEX,
    REJECT_NOT_TOP_N,
    SCAN_LOG_COLUMNS,
    TF_1M,
    WATCH_IN_POSITION,
    WATCH_READY_BREAKOUT,
    WATCH_SCANNING,
    WATCH_VERIFYING_NEWS,
)
from config.settings import CONFIG
from data.cache import bar_cache
from data.fetcher import BatchFetcher
from data.health import data_health_validator, classify_session
from data.pipeline import MTFPipeline, bars_today, premarket_bars
from execution.halt_machine import halt_machine
from intelligence.news_validator import news_validator
from scanner.demand import (
    DemandMetrics,
    calculate_dollar_flow_momentum,
    calc_gap_pct,
    calc_intraday_range_pct,
    calc_volume_spike_z,
    compute_demand_score,
    rank_universe,
)
from signals.setup import calculate_pressure_score
from scanner.filters import HardFilter, check_dollar_volume, check_price
from scanner.movers import movers_ingestor
from scanner.rvol import RVOLResult, best_rvol, calc_session_rvol
from risk.manager import risk_manager
from trade_log.session_recorder import session_recorder

log = logging.getLogger(__name__)
ET = pytz.timezone("America/New_York")


class UniverseScanner:
    """
    Real-time dynamic universe scanner.

    Call `scan()` once per engine cycle (every 1m bar close).
    Returns the current Top-N tickers as DemandMetrics objects.
    """

    def __init__(
        self,
        fetcher: BatchFetcher = None,
        pipeline: MTFPipeline = None,
        chop_mode: bool = False,
    ):
        self._fetcher = fetcher or BatchFetcher()
        self._pipeline = pipeline or MTFPipeline()
        self._chop_mode = chop_mode

        # CHOP-mode risk multipliers
        regime_cfg = CONFIG.regime
        self._rvol_mult = regime_cfg.chop_rvol_multiplier if chop_mode else 1.0
        self._spread_mult = regime_cfg.chop_spread_multiplier if chop_mode else 1.0

        # Per-ticker 1m bar count from previous cycle (for bar-count drop check)
        self._prev_bar_counts: Dict[str, int] = {}
        self._current_candidates: List[str] = []
        self._trade_candidates: List[str] = []

    @property
    def current_candidates(self) -> List[str]:
        return list(self._current_candidates)

    @property
    def trade_candidates(self) -> List[str]:
        return list(self._trade_candidates)

    # ─── Public API ──────────────────────────────────────────────────────────

    def scan(self, tickers: List[str] = None) -> List[DemandMetrics]:
        """
        Run full scan cycle.

        Args:
            tickers: override seed list (useful for testing / external feed)

        Returns:
            Top-N DemandMetrics sorted by DemandScore descending
        """
        tickers = tickers or CONFIG.scanner.seed_tickers
        now = datetime.now(ET)
        session = classify_session(now)

        # Refresh movers from Alpaca (throttled), then merge active movers into
        # the scan pool (set-union — no duplicates).
        movers_ingestor.fetch(now)
        movers_tickers = movers_ingestor.active_tickers(now)
        movers_meta = {c.ticker: c for c in movers_ingestor.all_candidates(now)}
        if movers_tickers:
            log.info(f"UniverseScanner: merging {len(movers_tickers)} movers tickers")
        tickers = list(dict.fromkeys(list(tickers) + movers_tickers))  # ordered dedup

        # Sentiment snapshot for every candidate in the scan pool.
        sentiment_scores: Dict[str, float] = {}
        for ticker in tickers:
            sentiment_scores[ticker] = news_validator.validate_ticker(ticker).score

        log.info(
            f"UniverseScanner: scanning {len(tickers)} candidates "
            f"(chop={self._chop_mode}, session={session})"
        )

        # 1. Fetch + build MTF bars (include extended hours for PM/AH context)
        bars_dict = self._fetcher.fetch_all(
            tickers,
            CONFIG.data.lookback_days,
            include_extended_hours=CONFIG.data.include_extended_hours,
        )
        mtf_all = self._pipeline.build_all(bars_dict)

        # Cache results
        for ticker, mtf_bars in mtf_all.items():
            bar_cache.set(ticker, mtf_bars)

        # 2. Fetch live quotes
        quotes = self._fetcher.fetch_quotes(list(mtf_all.keys()))

        # 3. Compute demand metrics + DATA_HEALTH + hard filters
        current_candidates = []
        candidates: List[DemandMetrics] = []
        scan_records: List[dict] = []

        for ticker, mtf_bars in mtf_all.items():
            df_1m = mtf_bars.get(TF_1M)
            if df_1m is None or df_1m.empty:
                continue

            symbol_now = datetime.now(ET)

            quote = quotes.get(ticker, {})
            bid = quote.get("bid", 0.0)
            ask = quote.get("ask", 0.0)
            last = quote.get("last", 0.0) or (
                df_1m["close"].iloc[-1] if not df_1m.empty else 0.0
            )
            quote_ts = quote.get("timestamp", datetime.now(ET))
            feed_type = quote.get("feed", getattr(self._fetcher, "feed_type", "ibkr"))
            quote_quality = str(quote.get("_quote_quality", "ok") or "ok")
            quote_ok = quote_quality == "ok"

            df_today = bars_today(df_1m)
            df_premkt = premarket_bars(df_1m)

            # ── DATA_HEALTH validation ────────────────────────────────────
            prev_count = self._prev_bar_counts.get(ticker, None)
            dh_report = data_health_validator.check(
                ticker=ticker,
                mtf_bars=mtf_bars,
                quote=quote,
                prev_bar_count_1m=prev_count,
                feed_type=feed_type,
                now=symbol_now,
            )
            # Update monotonicity baseline
            self._prev_bar_counts[ticker] = len(df_1m)

            # ── Halt machine wiring (per-ticker halt detection) ──────────
            # Signal halt_machine so it can manage the resume gate independently
            # of the broader health-block logic.
            if BLOCK_HALT_DETECTED in dh_report.block_reason:
                halt_machine.on_health_block(ticker, dh_report.block_reason)
            else:
                halt_machine.on_clean_tick(ticker)

            # ── Session-aware RVOL ────────────────────────────────────────
            rvol_result: RVOLResult = calc_session_rvol(
                df_1m, symbol_now, CONFIG.exthours.rvol_lookback_days
            )
            rvol = best_rvol(rvol_result)

            # ── Other demand components ───────────────────────────────────
            gap_pct = calc_gap_pct(df_1m)
            prev_close = _prev_close(df_1m)
            intraday_range = calc_intraday_range_pct(df_today, prev_close)
            volume_spike_z = calc_volume_spike_z(df_1m)
            dollar_flow_z = calculate_dollar_flow_momentum(df_1m)
            dollar_volume = _dollar_volume(df_today, last)
            pm_dollar_vol = (
                _dollar_volume(df_premkt, last) if df_premkt is not None else 0.0
            )

            # Base candidate accumulation: explicit price + dollar-volume threshold pass.
            price_ok, _ = check_price(last)
            dvol_ok, _ = check_dollar_volume(dollar_volume)
            if price_ok and dvol_ok and quote_ok:
                current_candidates.append(ticker)

            demand_score = compute_demand_score(
                rvol_result=rvol_result,
                gap_pct=gap_pct,
                intraday_range_pct=intraday_range,
                volume_spike_z=volume_spike_z,
                dollar_flow_momentum_z=dollar_flow_z,
            )

            # ── Hard filter (includes DATA_HEALTH block check) ────────────
            hf = HardFilter(
                rvol_multiplier=self._rvol_mult,
                spread_multiplier=self._spread_mult,
            )
            passed, rejection = hf.run(
                ticker=ticker,
                last_price=last,
                dollar_volume=dollar_volume,
                rvol=rvol,
                bid=bid,
                ask=ask,
                quote_ts=quote_ts,
                gap_pct=gap_pct,
                intraday_range_pct=intraday_range,
                volume_spike_z=volume_spike_z,
                df_1m_today=df_today,
                data_health_report=dh_report,
                session=session,
            )

            # Mandatory thin-IEX bypass for visibility: do not suppress from candidate list.
            low_coverage_on_iex = feed_type == FEED_ALPACA_IEX and (
                any(
                    DEGRADE_IEX_PM_COVERAGE in reason
                    for reason in getattr(dh_report, "degrade_reasons", [])
                )
                or BLOCK_PM_DOLLAR_VOLUME in str(dh_report.block_reason or "")
                or BLOCK_PM_FAKE_VOLUME in str(dh_report.block_reason or "")
            )
            if low_coverage_on_iex and ticker not in current_candidates:
                current_candidates.append(ticker)
            if low_coverage_on_iex and not passed:
                rejection = (
                    f"{rejection}|DEGRADED_THIN_IEX"
                    if rejection
                    else "DEGRADED_THIN_IEX"
                )
            if not quote_ok:
                rejection = (
                    f"{rejection}|QUOTE_{quote_quality.upper()}"
                    if rejection
                    else f"QUOTE_{quote_quality.upper()}"
                )

            scan_records.append(
                {
                    "scan_time": now.isoformat(),
                    "ticker": ticker,
                    "session": session,
                    "price": last,
                    "dollar_volume": dollar_volume,
                    "pm_dollar_volume": pm_dollar_vol,
                    "rvol": rvol,
                    "rvol_rth": rvol_result.rvol_rth,
                    "rvol_pm": rvol_result.rvol_pm,
                    "rvol_ah": rvol_result.rvol_ah,
                    "gap_pct": gap_pct,
                    "intraday_range_pct": intraday_range,
                    "volume_spike_z": volume_spike_z,
                    "dollar_flow_momentum_z": dollar_flow_z,
                    "demand_score": demand_score,
                    "rank": 0,
                    "passed_filters": passed,
                    "rejection_reason": rejection,
                    "feed_type": feed_type,
                    "quote_quality": quote_quality,
                    "pct_change": float(
                        getattr(movers_meta.get(ticker), "pct_change", 0.0)
                    ),
                    "volume_rank": int(
                        getattr(movers_meta.get(ticker), "volume_rank", 0) or 0
                    ),
                    "news_sentiment": float(sentiment_scores.get(ticker, 0.0)),
                    # DATA_HEALTH fields
                    "dh_status": dh_report.status,
                    "dh_block_reason": dh_report.block_reason,
                    "dh_degrade_reasons": "|".join(dh_report.degrade_reasons),
                    "dh_quote_age_s": dh_report.quote_age_s,
                    "dh_spread_pct": dh_report.spread_pct,
                    "dh_last_bar_age_s": dh_report.last_bar_age_s,
                    "dh_bar_count_1m": dh_report.bar_count_1m,
                    "dh_bar_count_5m": dh_report.bar_count_5m,
                    "dh_bar_gap": dh_report.bar_gap_minutes,
                }
            )

            if not passed:
                continue

            m = DemandMetrics(
                ticker=ticker,
                last_price=last,
                dollar_volume=dollar_volume,
                rvol=rvol,
                gap_pct=gap_pct,
                intraday_range_pct=intraday_range,
                volume_spike_z=volume_spike_z,
                dollar_flow_momentum_z=dollar_flow_z,
                demand_score=demand_score,
                bid=bid,
                ask=ask,
            )
            # Attach health report so engine can propagate size_multiplier
            m._dh_report = dh_report  # type: ignore[attr-defined]
            m._feed_type = feed_type  # type: ignore[attr-defined]
            m._quote_quality = quote_quality  # type: ignore[attr-defined]
            m._pct_change = float(getattr(movers_meta.get(ticker), "pct_change", 0.0))  # type: ignore[attr-defined]
            m._volume_rank = int(getattr(movers_meta.get(ticker), "volume_rank", 0) or 0)  # type: ignore[attr-defined]
            m._news_sentiment = float(sentiment_scores.get(ticker, 0.0))  # type: ignore[attr-defined]
            candidates.append(m)

        # 4. Rank → Top N
        top_n = rank_universe(candidates)
        self._current_candidates = list(dict.fromkeys(current_candidates))
        self._trade_candidates = [m.ticker for m in top_n]
        top_set = {m.ticker for m in top_n}

        # Publish deterministic momentum diagnostics for replay/observability.
        try:
            from events import current_cycle
            from events.bus import event_bus
            from events.types import MomentumMetricsComputed

            for m in top_n:
                mtf = mtf_all.get(m.ticker, {})
                df_m = mtf.get(TF_1M)
                pressure_z = calculate_pressure_score(df_m)
                event_bus.publish(
                    MomentumMetricsComputed(
                        cycle_id=current_cycle.id,
                        ticker=m.ticker,
                        dollar_flow_z=float(getattr(m, "dollar_flow_momentum_z", 0.0)),
                        pressure_z=float(pressure_z),
                    )
                )
        except Exception as _exc:
            log.debug("[UniverseScanner] momentum metric publish skipped: %s", _exc)

        # Update scan records with final ranks
        rank_map = {m.ticker: m.rank for m in top_n}
        for rec in scan_records:
            if rec["ticker"] in rank_map:
                rec["rank"] = rank_map[rec["ticker"]]
            elif rec["passed_filters"] and rec["ticker"] not in top_set:
                rec["rejection_reason"] = REJECT_NOT_TOP_N

        action = "Executing" if top_n else "Waiting"
        print(
            f"[SCAN COMPLETE] | Candidates Found: {self._current_candidates} | Action: {action}.",
            flush=True,
        )
        print(f"trade_candidates = {self._trade_candidates}", flush=True)

        try:
            session_recorder.new_day(now.strftime("%Y-%m-%d"))
            session_recorder.record_candidates(cycle_ts=now, records=scan_records)
        except Exception as exc:
            log.warning("Session recorder write skipped: %s", exc)

        try:
            self._log_scan(scan_records)
        except Exception as exc:
            log.warning("Scan log write skipped: %s", exc)

        self._render_watchlist_dashboard(top_n, sentiment_scores)

        log.info(
            f"UniverseScanner: {len(top_n)} tickers in Top-{CONFIG.scanner.top_n}: "
            + ", ".join(f"{m.ticker}({m.demand_score:.2f})" for m in top_n)
        )
        return top_n

    def _render_watchlist_dashboard(
        self,
        ranked: List[DemandMetrics],
        sentiment_scores: Dict[str, float],
    ) -> None:
        if not ranked:
            return

        in_position = {t.ticker for t in risk_manager.open_trades()}

        header = (
            "\n"
            + "-" * 86
            + "\n"
            + "[TICKER] | [% CHANGE] | [VOLUME RANK] | [NEWS SENTIMENT] | [BOT ACTION]\n"
            + "-" * 86
        )
        rows = []
        for dm in ranked[:15]:
            ticker = dm.ticker
            pct_change = float(getattr(dm, "_pct_change", 0.0))
            v_rank = int(getattr(dm, "_volume_rank", 0) or 0)
            sentiment = float(
                sentiment_scores.get(ticker, getattr(dm, "_news_sentiment", 0.0))
            )

            status = WATCH_SCANNING
            if ticker in in_position:
                status = WATCH_IN_POSITION
            elif ticker in sentiment_scores:
                status = (
                    WATCH_READY_BREAKOUT
                    if sentiment
                    >= float(getattr(CONFIG.strategy, "sentiment_threshold", 0.2))
                    else WATCH_VERIFYING_NEWS
                )

            rows.append(
                f"{ticker:<8} | {pct_change:>8.2f}% | {v_rank:>11} | {sentiment:>14.2f} | {status}"
            )

        log.info(header + "\n" + "\n".join(rows) + "\n" + "-" * 86)

    # ─── Internal helpers ────────────────────────────────────────────────────

    def _log_scan(self, records: List[dict]) -> None:
        """Append scan results to today's scan log (CSV)."""
        try:
            from trade_log.trade_logger import get_scan_log_path

            path = get_scan_log_path()
            # Fill missing columns with empty string for backward compat
            df = pd.DataFrame(records)
            for col in SCAN_LOG_COLUMNS:
                if col not in df.columns:
                    df[col] = ""
            df = df.reindex(columns=SCAN_LOG_COLUMNS)
            df.to_csv(
                path, mode="a", header=not pd.io.common.file_exists(path), index=False
            )
        except Exception as exc:
            log.debug(f"Scan log write failed: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# Module-level helpers
# ─────────────────────────────────────────────────────────────────────────────


def _prev_close(df_1m: pd.DataFrame) -> float:
    """Return the most recent prior-day closing price."""
    ET = pytz.timezone("America/New_York")
    today = pd.Timestamp.now(tz=ET).normalize()
    prior = df_1m[df_1m.index.normalize() < today]
    if prior.empty:
        return df_1m["close"].iloc[0] if not df_1m.empty else 0.0
    return float(prior.iloc[-1]["close"])


def _dollar_volume(df_today: pd.DataFrame, last_price: float) -> float:
    """Today's approximate dollar volume = sum(volume) * last_price."""
    if df_today is None or df_today.empty:
        return 0.0
    # Use actual (close * volume) for each bar for accuracy
    dv = (df_today["close"] * df_today["volume"]).sum()
    return float(dv) if dv > 0 else float(df_today["volume"].sum() * last_price)
