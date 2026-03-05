"""
trade_logger.py — Full-transparency trade and scan logging.

Every trade is logged with:
  - DemandScore
  - SetupQualityScore
  - Why chosen over others
  - All TF bar counts
  - Risk multiplier (if CHOP)
  - Entry / exit details

Output files (in logs/ directory):
  trades_YYYY-MM-DD.csv   — trade records
  scan_YYYY-MM-DD.csv     — per-cycle universe scan results
  engine_YYYY-MM-DD.log   — full engine log (via Python logging)
"""

from __future__ import annotations

import csv
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

import pytz

from config.constants import TRADE_LOG_COLUMNS, SCAN_LOG_COLUMNS, PARTIAL_EXIT_LOG_COLUMNS

log = logging.getLogger(__name__)
ET  = pytz.timezone("America/New_York")

LOGS_DIR = Path(__file__).resolve().parent.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# PATH HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _today_str() -> str:
    return datetime.now(ET).strftime("%Y-%m-%d")


def get_trade_log_path(date_str: str = None) -> Path:
    return LOGS_DIR / f"trades_{date_str or _today_str()}.csv"


def get_scan_log_path(date_str: str = None) -> Path:
    return LOGS_DIR / f"scan_{date_str or _today_str()}.csv"


def get_engine_log_path(date_str: str = None) -> Path:
    return LOGS_DIR / f"engine_{date_str or _today_str()}.log"


# ─────────────────────────────────────────────────────────────────────────────
# TRADE LOGGER
# ─────────────────────────────────────────────────────────────────────────────

class TradeLogger:
    """
    Writes trade open/close events to a CSV file.
    Appends incrementally — no full rewrites.
    """

    def __init__(self):
        self._today: Optional[str] = None
        self._trade_path:   Optional[Path] = None
        self._partial_path: Optional[Path] = None

    def new_day(self, date_str: str = None) -> None:
        self._today        = date_str or _today_str()
        self._trade_path   = get_trade_log_path(self._today)
        self._partial_path = LOGS_DIR / f"partials_{self._today}.csv"
        self._init_csv(self._trade_path,   TRADE_LOG_COLUMNS)
        self._init_csv(self._partial_path, PARTIAL_EXIT_LOG_COLUMNS)
        log.info(f"TradeLogger: logging to {self._trade_path}")

    def _init_csv(self, path: Path, columns: list) -> None:
        if not path.exists():
            with open(path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(columns)

    def log_open(self, trade, demand_metrics=None) -> None:
        """Log trade entry (partial record — exit fields blank)."""
        try:
            row = self._trade_to_row(trade, demand_metrics)
            self._append_row(row)
        except Exception as exc:
            log.warning(f"TradeLogger.log_open failed: {exc}")

    def log_close(self, trade) -> None:
        """
        Update the trade record row with exit data.
        Appends a new 'CLOSE' row — simple audit trail.
        """
        try:
            row = self._trade_to_row(trade)
            self._append_row(row)
        except Exception as exc:
            log.warning(f"TradeLogger.log_close failed: {exc}")

    def log_partial(
        self,
        trade,
        shares_sold:  int,
        fill_price:   float,
        reason:       str = "",
    ) -> None:
        """
        Log a partial exit row to partials_YYYY-MM-DD.csv.
        Called by the engine each time a lifecycle PARTIAL_SELL is executed.
        """
        try:
            now            = datetime.now(ET)
            pnl_partial    = (fill_price - trade.entry_price) * shares_sold
            row = {
                "trade_id":          trade.trade_id,
                "ticker":            trade.ticker,
                "date":              self._today or _today_str(),
                "partial_time":      now.strftime("%H:%M:%S"),
                "shares_sold":       shares_sold,
                "shares_remaining":  getattr(trade, "shares_remaining", 0),
                "fill_price":        fill_price,
                "lifecycle_state":   getattr(trade, "lifecycle_state", ""),
                "reason":            reason,
                "pnl_partial":       round(pnl_partial, 2),
            }
            path = self._partial_path or LOGS_DIR / f"partials_{_today_str()}.csv"
            self._init_csv(path, PARTIAL_EXIT_LOG_COLUMNS)
            with open(path, "a", newline="") as f:
                writer = csv.DictWriter(
                    f, fieldnames=PARTIAL_EXIT_LOG_COLUMNS, extrasaction="ignore"
                )
                writer.writerow(row)
        except Exception as exc:
            log.warning(f"TradeLogger.log_partial failed: {exc}")

    def _append_row(self, row: dict) -> None:
        path = self._trade_path or get_trade_log_path()
        self._init_csv(path, TRADE_LOG_COLUMNS)
        with open(path, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=TRADE_LOG_COLUMNS, extrasaction="ignore")
            writer.writerow(row)

    def _trade_to_row(self, trade, dm=None) -> dict:
        return {
            "trade_id":            trade.trade_id,
            "ticker":              trade.ticker,
            "date":                self._today or _today_str(),
            "entry_time":          _fmt_dt(trade.entry_time),
            "exit_time":           _fmt_dt(trade.exit_time),
            "direction":           trade.direction,
            "entry_price":         trade.entry_price,
            "exit_price":          trade.exit_price,
            "shares":              trade.shares,
            "pnl":                 round(trade.pnl, 2),
            "pnl_r":               round(trade.pnl_r, 3),
            "stop_price":          trade.stop_price,
            "target_price":        trade.target_price,
            "demand_score":        trade.demand_score,
            "setup_quality_score": trade.setup_quality_score,
            "regime":              trade.regime,
            "risk_multiplier":     trade.risk_multiplier,
            "bars_1m":             trade.bars_1m,
            "bars_5m":             trade.bars_5m,
            "bars_15m":            trade.bars_15m,
            "setup_name":          trade.setup_name,
            "exit_reason":         trade.exit_reason,
            "chosen_over":         trade.chosen_over,
            # Phase 2 determinism + data health audit fields
            "selection_reason":    getattr(trade, "selection_reason", ""),
            "universe_rank":       getattr(trade, "universe_rank",    0),
            "size_degrade_reason": getattr(trade, "size_degrade_reason", ""),
            "feed_type":           getattr(trade, "feed_type",        ""),
            "notes":               trade.notes,
        }


# ─────────────────────────────────────────────────────────────────────────────
# LOGGING SETUP (Python logging → rotating file + console)
# ─────────────────────────────────────────────────────────────────────────────

def setup_logging(level: int = logging.INFO) -> None:
    """
    Configure root logger to write to:
      - Console (INFO+)
      - Daily rotating file (DEBUG+)
    Call once at startup (main.py).
    """
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)-8s] %(name)-30s %(message)s",
        datefmt="%H:%M:%S",
    )

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(fmt)

    # File handler
    log_path = get_engine_log_path()
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    root.addHandler(ch)
    root.addHandler(fh)

    log.info(f"Logging initialized → {log_path}")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _fmt_dt(dt: Optional[datetime]) -> str:
    if dt is None:
        return ""
    return dt.strftime("%H:%M:%S")
