from __future__ import annotations

import argparse
import csv
import glob
import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, cast


@dataclass
class TradeRow:
    trade_id: str
    date: str
    ticker: str
    entry_time: str
    exit_time: str
    pnl: float
    pnl_r: float
    slippage_abs: float
    slippage_to_atr_pct: float

    @property
    def is_closed(self) -> bool:
        return bool((self.exit_time or "").strip())


@dataclass
class DayCard:
    date: str
    total_trades: int
    closed_trades: int
    open_trades: int
    wins: int
    losses: int
    breakeven: int
    win_rate_pct: float
    total_pnl: float
    total_r: float
    avg_pnl: float
    avg_r: float
    avg_win: float
    avg_loss: float
    expectancy: float
    profit_factor: float
    avg_slippage_abs: float
    avg_slippage_atr_pct: float
    reject_entry_stale_quote: int
    reject_entry_runaway: int


def _to_float(value: str, default: float = 0.0) -> float:
    try:
        return float((value or "").strip())
    except Exception:
        return default


def _looks_like_trade_id(value: str) -> bool:
    value = (value or "").strip()
    return bool(value) and "_" in value and not value.startswith("202")


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


def _trade_files(logs_dir: str, date: str | None, all_days: bool) -> list[str]:
    if date:
        file_path = os.path.join(logs_dir, f"trades_{date}.csv")
        if not os.path.exists(file_path):
            raise FileNotFoundError(
                f"No trade log found for date={date} at {file_path}"
            )
        return [file_path]

    matches = glob.glob(os.path.join(logs_dir, "trades_*.csv"))
    if not matches:
        raise FileNotFoundError(f"No trade log files found in: {logs_dir}")
    matches = sorted(matches)
    if all_days:
        return matches
    return [matches[-1]]


def _load_trades(files: Iterable[str]) -> list[TradeRow]:
    latest_by_id: dict[str, TradeRow] = {}

    for csv_path in files:
        with open(csv_path, "r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                trade_id = (row.get("trade_id") or "").strip()
                if not _looks_like_trade_id(trade_id):
                    continue
                latest_by_id[trade_id] = TradeRow(
                    trade_id=trade_id,
                    date=(row.get("date") or "").strip(),
                    ticker=(row.get("ticker") or "").strip(),
                    entry_time=(row.get("entry_time") or "").strip(),
                    exit_time=(row.get("exit_time") or "").strip(),
                    pnl=_to_float(row.get("pnl", "0"), 0.0),
                    pnl_r=_to_float(row.get("pnl_r", "0"), 0.0),
                    slippage_abs=_extract_execution_metric(
                        row.get("notes", ""), "slippage_abs"
                    ),
                    slippage_to_atr_pct=_extract_execution_metric(
                        row.get("notes", ""), "slippage_to_atr_pct"
                    ),
                )

    return list(latest_by_id.values())


def _compute_day_card(date: str, trades: list[TradeRow], logs_dir: str) -> DayCard:
    closed = [trade for trade in trades if trade.is_closed]
    open_trades = [trade for trade in trades if not trade.is_closed]

    wins = sum(1 for trade in closed if trade.pnl > 0)
    losses = sum(1 for trade in closed if trade.pnl < 0)
    breakeven = sum(1 for trade in closed if trade.pnl == 0)

    closed_count = len(closed)
    win_rate_pct = (wins / closed_count * 100.0) if closed_count else 0.0

    total_pnl = sum(trade.pnl for trade in closed)
    total_r = sum(trade.pnl_r for trade in closed)

    avg_pnl = (total_pnl / closed_count) if closed_count else 0.0
    avg_r = (total_r / closed_count) if closed_count else 0.0

    avg_win = (
        sum(trade.pnl for trade in closed if trade.pnl > 0) / wins if wins else 0.0
    )
    avg_loss = (
        sum(trade.pnl for trade in closed if trade.pnl < 0) / losses if losses else 0.0
    )

    expectancy = (win_rate_pct / 100.0) * avg_win + (
        1 - win_rate_pct / 100.0
    ) * avg_loss

    gross_profit = sum(trade.pnl for trade in closed if trade.pnl > 0)
    gross_loss_abs = abs(sum(trade.pnl for trade in closed if trade.pnl < 0))
    if gross_loss_abs > 0:
        profit_factor = gross_profit / gross_loss_abs
    elif gross_profit > 0:
        profit_factor = 999.0
    else:
        profit_factor = 0.0

    with_slippage = [
        trade
        for trade in closed
        if trade.slippage_abs > 0 or trade.slippage_to_atr_pct > 0
    ]
    avg_slippage_abs = (
        sum(trade.slippage_abs for trade in with_slippage) / len(with_slippage)
        if with_slippage
        else 0.0
    )
    avg_slippage_atr_pct = (
        sum(trade.slippage_to_atr_pct for trade in with_slippage) / len(with_slippage)
        if with_slippage
        else 0.0
    )

    reject_counts = _load_entry_reject_counts(logs_dir=logs_dir, date=date)

    return DayCard(
        date=date,
        total_trades=len(trades),
        closed_trades=closed_count,
        open_trades=len(open_trades),
        wins=wins,
        losses=losses,
        breakeven=breakeven,
        win_rate_pct=win_rate_pct,
        total_pnl=total_pnl,
        total_r=total_r,
        avg_pnl=avg_pnl,
        avg_r=avg_r,
        avg_win=avg_win,
        avg_loss=avg_loss,
        expectancy=expectancy,
        profit_factor=profit_factor,
        avg_slippage_abs=avg_slippage_abs,
        avg_slippage_atr_pct=avg_slippage_atr_pct,
        reject_entry_stale_quote=reject_counts.get("ENTRY_QUOTE_STALE", 0),
        reject_entry_runaway=reject_counts.get("ENTRY_PRICE_RUNAWAY", 0),
    )


def _load_entry_reject_counts(logs_dir: str, date: str) -> dict[str, int]:
    """
    Load explicit entry-guard rejection counts from events_YYYY-MM-DD.jsonl.

    Primary source: DECISION_EVALUATED events where accepted=false.
    Fallback source: GATE_EVENT gate names from entry guards.
    """
    target = os.path.join(logs_dir, f"events_{date}.jsonl")
    counts = {
        "ENTRY_QUOTE_STALE": 0,
        "ENTRY_PRICE_RUNAWAY": 0,
    }
    if not os.path.exists(target):
        return counts

    try:
        with open(target, "r", encoding="utf-8") as handle:
            for line in handle:
                raw = (line or "").strip()
                if not raw:
                    continue
                try:
                    event = json.loads(raw)
                except Exception:
                    continue

                event_type = str(event.get("event_type", "") or "").strip()
                payload = event.get("payload", {}) or {}
                if not isinstance(payload, dict):
                    continue

                if event_type == "DECISION_EVALUATED":
                    accepted = bool(payload.get("accepted", False))
                    reason = str(payload.get("reason", "") or "").strip()
                    if not accepted and reason in counts:
                        counts[reason] = counts.get(reason, 0) + 1
                    continue

                if event_type == "GATE_EVENT":
                    gate = str(payload.get("gate", "") or "").strip()
                    passed = bool(payload.get("passed", True))
                    if passed:
                        continue
                    if gate == "ENTRY_QUOTE_FRESHNESS":
                        counts["ENTRY_QUOTE_STALE"] = (
                            counts.get("ENTRY_QUOTE_STALE", 0) + 1
                        )
                    elif gate == "ENTRY_SLIPPAGE_GUARD":
                        counts["ENTRY_PRICE_RUNAWAY"] = (
                            counts.get("ENTRY_PRICE_RUNAWAY", 0) + 1
                        )
    except Exception:
        return counts

    return counts


def _fmt_money(value: float) -> str:
    return f"${value:,.2f}"


def _print_cards(cards: list[DayCard]) -> None:
    headers = [
        "date",
        "closed",
        "open",
        "wins",
        "losses",
        "win_rate",
        "pnl",
        "total_r",
        "avg_r",
        "expectancy",
        "pf",
        "avg_slip",
        "avg_slip_atr",
        "reject_stale",
        "reject_runaway",
    ]

    rows: list[list[str]] = [headers]
    for card in cards:
        rows.append(
            [
                card.date,
                str(card.closed_trades),
                str(card.open_trades),
                str(card.wins),
                str(card.losses),
                f"{card.win_rate_pct:.1f}%",
                _fmt_money(card.total_pnl),
                f"{card.total_r:.3f}",
                f"{card.avg_r:.3f}",
                _fmt_money(card.expectancy),
                f"{card.profit_factor:.2f}",
                f"{card.avg_slippage_abs:.4f}",
                f"{card.avg_slippage_atr_pct:.2f}%",
                str(card.reject_entry_stale_quote),
                str(card.reject_entry_runaway),
            ]
        )

    widths = [max(len(cell) for cell in col) for col in zip(*rows)]
    print("\n=== Strategy Report Card ===")
    for idx, row in enumerate(rows):
        line = " | ".join(cell.ljust(widths[i]) for i, cell in enumerate(row))
        print(line)
        if idx == 0:
            print("-+-".join("-" * w for w in widths))


def _to_dict(card: DayCard) -> dict[str, Any]:
    return {
        "date": card.date,
        "total_trades": card.total_trades,
        "closed_trades": card.closed_trades,
        "open_trades": card.open_trades,
        "wins": card.wins,
        "losses": card.losses,
        "breakeven": card.breakeven,
        "win_rate_pct": round(card.win_rate_pct, 3),
        "total_pnl": round(card.total_pnl, 2),
        "total_r": round(card.total_r, 3),
        "avg_pnl": round(card.avg_pnl, 2),
        "avg_r": round(card.avg_r, 3),
        "avg_win": round(card.avg_win, 2),
        "avg_loss": round(card.avg_loss, 2),
        "expectancy": round(card.expectancy, 2),
        "profit_factor": round(card.profit_factor, 3),
        "avg_slippage_abs": round(card.avg_slippage_abs, 5),
        "avg_slippage_atr_pct": round(card.avg_slippage_atr_pct, 3),
        "reject_entry_stale_quote": int(card.reject_entry_stale_quote),
        "reject_entry_runaway": int(card.reject_entry_runaway),
    }


def _write_csv(cards: list[DayCard], output_path: str) -> None:
    fieldnames = [
        "date",
        "total_trades",
        "closed_trades",
        "open_trades",
        "wins",
        "losses",
        "breakeven",
        "win_rate_pct",
        "total_pnl",
        "total_r",
        "avg_pnl",
        "avg_r",
        "avg_win",
        "avg_loss",
        "expectancy",
        "profit_factor",
        "avg_slippage_abs",
        "avg_slippage_atr_pct",
        "reject_entry_stale_quote",
        "reject_entry_runaway",
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for card in cards:
            writer.writerow(_to_dict(card))


def _write_json(cards: list[DayCard], output_path: str) -> None:
    payload = [_to_dict(card) for card in cards]
    with open(output_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def _normalize_date(value: str | None) -> str | None:
    if not value:
        return None
    value = value.strip()
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return value
    except ValueError as exc:
        raise ValueError("--date must be in YYYY-MM-DD format") from exc


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate daily strategy report card from trade logs."
    )
    parser.add_argument(
        "--logs-dir", default="logs", help="Logs directory (default: logs)."
    )
    parser.add_argument("--date", default="", help="Specific date (YYYY-MM-DD).")
    parser.add_argument(
        "--all-days",
        action="store_true",
        help="Aggregate all available trade files into one row per date.",
    )
    parser.add_argument(
        "--export-csv",
        default="",
        help="Optional output CSV path for report card rows.",
    )
    parser.add_argument(
        "--export-json",
        default="",
        help="Optional output JSON path for report card rows.",
    )
    args = parser.parse_args()

    date = _normalize_date(args.date)
    files = _trade_files(args.logs_dir, date=date, all_days=args.all_days)
    trades = _load_trades(files)

    by_date: dict[str, list[TradeRow]] = {}
    for trade in trades:
        d = (trade.date or "").strip()
        if not d:
            continue
        by_date.setdefault(d, []).append(trade)

    if date:
        dates = [date]
    elif args.all_days:
        dates = sorted(by_date.keys())
    else:
        dates = [max(by_date.keys())]

    cards = [_compute_day_card(d, by_date.get(d, []), args.logs_dir) for d in dates]
    _print_cards(cards)

    if args.export_csv:
        _write_csv(cards, args.export_csv)
        print(f"\nCSV exported: {args.export_csv}")

    if args.export_json:
        _write_json(cards, args.export_json)
        print(f"JSON exported: {args.export_json}")


if __name__ == "__main__":
    main()
