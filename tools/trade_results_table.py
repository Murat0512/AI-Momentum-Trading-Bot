from __future__ import annotations

import argparse
import csv
import glob
import json
import os
from datetime import datetime
from dataclasses import dataclass
from typing import Any, List, Optional, cast


@dataclass
class TradeRow:
    trade_id: str
    ticker: str
    entry_time: str
    exit_time: str
    shares: int
    entry_price: float
    exit_price: float
    pnl: float
    pnl_r: float
    stop_price: float
    regime: str
    exit_reason: str
    slippage_abs: float
    slippage_to_atr_pct: float

    @property
    def is_closed(self) -> bool:
        return bool(self.exit_time.strip())


def _latest_trade_file(logs_dir: str) -> str:
    matches = glob.glob(os.path.join(logs_dir, "trades_*.csv"))
    if not matches:
        raise FileNotFoundError(f"No trade log files found in: {logs_dir}")
    return max(matches, key=os.path.getmtime)


def _to_float(value: str, default: float = 0.0) -> float:
    try:
        return float((value or "").strip())
    except Exception:
        return default


def _to_int(value: str, default: int = 0) -> int:
    try:
        return int(float((value or "").strip()))
    except Exception:
        return default


def _looks_like_trade_id(value: str) -> bool:
    value = (value or "").strip()
    return bool(value) and "_" in value and not value.startswith("202")


def load_trades(csv_path: str) -> List[TradeRow]:
    rows: List[TradeRow] = []
    latest_by_id: dict[str, TradeRow] = {}
    with open(csv_path, "r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            trade_id = (row.get("trade_id") or "").strip()
            if not _looks_like_trade_id(trade_id):
                continue

            latest_by_id[trade_id] = TradeRow(
                trade_id=trade_id,
                ticker=(row.get("ticker") or "").strip(),
                entry_time=(row.get("entry_time") or "").strip(),
                exit_time=(row.get("exit_time") or "").strip(),
                shares=_to_int(row.get("shares", "0"), 0),
                entry_price=_to_float(row.get("entry_price", "0"), 0.0),
                exit_price=_to_float(row.get("exit_price", "0"), 0.0),
                pnl=_to_float(row.get("pnl", "0"), 0.0),
                pnl_r=_to_float(row.get("pnl_r", "0"), 0.0),
                stop_price=_to_float(row.get("stop_price", "0"), 0.0),
                regime=(row.get("regime") or "").strip(),
                exit_reason=(row.get("exit_reason") or "").strip(),
                slippage_abs=_extract_execution_metric(
                    row.get("notes", ""), "slippage_abs"
                ),
                slippage_to_atr_pct=_extract_execution_metric(
                    row.get("notes", ""),
                    "slippage_to_atr_pct",
                ),
            )

    rows = list(latest_by_id.values())
    rows.sort(key=lambda trade: trade.entry_time)
    return rows


def _fmt_money(value: float) -> str:
    return f"${value:,.2f}"


def _fmt_num(value: float) -> str:
    return f"{value:.3f}"


def _fmt_pct(value: float) -> str:
    return f"{value:.2f}%"


def _extract_execution_metric(notes: str, key: str, default: float = 0.0) -> float:
    raw = (notes or "").strip()
    if not raw:
        return default
    try:
        payload: Any = json.loads(raw)
        execution: dict[str, Any] = {}
        if isinstance(payload, dict):
            payload_dict = cast(dict[str, Any], payload)
            raw_execution = payload_dict.get("execution", {})
            if isinstance(raw_execution, dict):
                execution = cast(dict[str, Any], raw_execution)
        value: Any = execution.get(key, default)
        return _to_float(str(value), default)
    except Exception:
        return default


def _print_table(title: str, rows: List[List[str]]) -> None:
    print(f"\n{title}")
    if not rows:
        print("(none)")
        return
    widths = [max(len(cell) for cell in col) for col in zip(*rows)]
    for index, row in enumerate(rows):
        line = " | ".join(cell.ljust(widths[i]) for i, cell in enumerate(row))
        print(line)
        if index == 0:
            print("-+-".join("-" * width for width in widths))


def _session_date_from_csv_path(csv_path: str) -> str:
    base = os.path.basename(csv_path)
    # trades_YYYY-MM-DD.csv
    if base.startswith("trades_") and base.endswith(".csv"):
        candidate = base[len("trades_") : -len(".csv")]
        try:
            datetime.strptime(candidate, "%Y-%m-%d")
            return candidate
        except Exception:
            return ""
    return ""


def _entry_guard_rejections(logs_dir: str, date: str) -> dict[str, int]:
    counts = {
        "ENTRY_QUOTE_STALE": 0,
        "ENTRY_PRICE_RUNAWAY": 0,
    }
    if not date:
        return counts
    path = os.path.join(logs_dir, f"events_{date}.jsonl")
    if not os.path.exists(path):
        return counts

    try:
        with open(path, "r", encoding="utf-8") as handle:
            for line in handle:
                raw = (line or "").strip()
                if not raw:
                    continue
                try:
                    event = json.loads(raw)
                except Exception:
                    continue
                if str(event.get("event_type", "") or "") != "DECISION_EVALUATED":
                    continue
                payload = event.get("payload", {}) or {}
                if not isinstance(payload, dict):
                    continue
                accepted = bool(payload.get("accepted", False))
                reason = str(payload.get("reason", "") or "").strip()
                if not accepted and reason in counts:
                    counts[reason] = counts.get(reason, 0) + 1
    except Exception:
        return counts

    return counts


def summarize(trades: List[TradeRow]) -> None:
    closed = [trade for trade in trades if trade.is_closed]
    open_trades = [trade for trade in trades if not trade.is_closed]

    total_closed = len(closed)
    wins = sum(1 for trade in closed if trade.pnl > 0)
    losses = sum(1 for trade in closed if trade.pnl < 0)
    win_rate = (wins / total_closed * 100.0) if total_closed else 0.0
    total_pnl = sum(trade.pnl for trade in closed)
    total_r = sum(trade.pnl_r for trade in closed)
    avg_win = (
        sum(trade.pnl for trade in closed if trade.pnl > 0) / wins if wins else 0.0
    )
    avg_loss = (
        sum(trade.pnl for trade in closed if trade.pnl < 0) / losses if losses else 0.0
    )
    expectancy = (win_rate / 100.0) * avg_win + (1 - win_rate / 100.0) * avg_loss
    avg_r = (total_r / total_closed) if total_closed else 0.0

    closed_with_slippage = [
        trade
        for trade in closed
        if trade.slippage_abs > 0 or trade.slippage_to_atr_pct > 0
    ]
    avg_slippage_abs = (
        sum(trade.slippage_abs for trade in closed_with_slippage)
        / len(closed_with_slippage)
        if closed_with_slippage
        else 0.0
    )
    avg_slippage_atr_pct = (
        sum(trade.slippage_to_atr_pct for trade in closed_with_slippage)
        / len(closed_with_slippage)
        if closed_with_slippage
        else 0.0
    )
    max_slippage_abs = max(
        (trade.slippage_abs for trade in closed_with_slippage), default=0.0
    )

    print("\n=== Trade Session Summary ===")
    print(f"Closed trades : {total_closed}")
    print(f"Open trades   : {len(open_trades)}")
    print(f"Wins / Losses : {wins} / {losses}")
    print(f"Win rate      : {win_rate:.1f}%")
    print(f"Total PnL     : {_fmt_money(total_pnl)}")
    print(f"Total R       : {_fmt_num(total_r)}")
    print(f"Avg PnL/trade : {_fmt_money(expectancy)}")
    print(f"Avg R/trade   : {_fmt_num(avg_r)}")
    print(f"Avg Win       : {_fmt_money(avg_win)}")
    print(f"Avg Loss      : {_fmt_money(avg_loss)}")
    print(
        f"Avg Slippage  : {avg_slippage_abs:.4f} ({_fmt_pct(avg_slippage_atr_pct)} ATR)"
    )
    print(f"Max Slippage  : {max_slippage_abs:.4f}")

    exit_breakdown: dict[str, int] = {}
    for trade in closed:
        reason = (trade.exit_reason or "UNKNOWN").strip() or "UNKNOWN"
        exit_breakdown[reason] = exit_breakdown.get(reason, 0) + 1

    regime_breakdown: dict[str, int] = {}
    for trade in closed:
        regime = (trade.regime or "UNKNOWN").strip() or "UNKNOWN"
        regime_breakdown[regime] = regime_breakdown.get(regime, 0) + 1

    breakdown_rows: List[List[str]] = [["bucket", "value", "count"]]
    for reason, count in sorted(
        exit_breakdown.items(), key=lambda item: (-item[1], item[0])
    ):
        breakdown_rows.append(["exit_reason", reason, str(count)])
    for regime, count in sorted(
        regime_breakdown.items(), key=lambda item: (-item[1], item[0])
    ):
        breakdown_rows.append(["regime", regime, str(count)])

    closed_rows: List[List[str]] = [
        [
            "trade_id",
            "ticker",
            "entry",
            "exit",
            "shares",
            "pnl",
            "pnl_r",
            "slippage",
            "reason",
        ]
    ]
    for trade in closed[-15:]:
        closed_rows.append(
            [
                trade.trade_id,
                trade.ticker,
                trade.entry_time,
                trade.exit_time,
                str(trade.shares),
                _fmt_money(trade.pnl),
                _fmt_num(trade.pnl_r),
                f"{trade.slippage_abs:.4f}",
                trade.exit_reason or "-",
            ]
        )

    open_rows: List[List[str]] = [
        [
            "trade_id",
            "ticker",
            "entry",
            "shares",
            "entry_px",
            "regime",
        ]
    ]
    for trade in open_trades:
        open_rows.append(
            [
                trade.trade_id,
                trade.ticker,
                trade.entry_time,
                str(trade.shares),
                f"{trade.entry_price:.4f}",
                trade.regime or "-",
            ]
        )

    _print_table("Recent Closed Trades (last 15)", closed_rows)
    _print_table("Breakdown", breakdown_rows)
    _print_table("Open Trades", open_rows)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Print clean summary table from trade logs."
    )
    parser.add_argument(
        "--file", dest="csv_file", default="", help="Path to trades CSV (optional)."
    )
    parser.add_argument(
        "--logs-dir",
        dest="logs_dir",
        default="logs",
        help="Logs directory (default: logs).",
    )
    args = parser.parse_args()

    csv_file: Optional[str] = args.csv_file.strip() if args.csv_file else None
    if not csv_file:
        csv_file = _latest_trade_file(args.logs_dir)

    print(f"Using trade file: {csv_file}")
    trades = load_trades(csv_file)
    summarize(trades)

    session_date = _session_date_from_csv_path(csv_file)
    guard_counts = _entry_guard_rejections(args.logs_dir, session_date)
    guard_rows = [
        ["bucket", "value", "count"],
        ["entry_reject", "ENTRY_QUOTE_STALE", str(guard_counts["ENTRY_QUOTE_STALE"])],
        [
            "entry_reject",
            "ENTRY_PRICE_RUNAWAY",
            str(guard_counts["ENTRY_PRICE_RUNAWAY"]),
        ],
    ]
    _print_table("Entry Guard Rejections", guard_rows)


if __name__ == "__main__":
    main()
