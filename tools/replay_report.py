from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from statistics import mean
from typing import Dict, Iterable, List, Optional

METRICS = ["demand_score", "sqs", "dollar_flow_z", "pressure_z"]
GROUPS = ["submitted", "filled", "rejected", "winner", "loser"]


def _safe_float(value) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _percentile(values: List[float], q: float) -> float:
    if not values:
        return 0.0
    vals = sorted(values)
    if len(vals) == 1:
        return vals[0]
    pos = (len(vals) - 1) * q
    lower = int(pos)
    upper = min(lower + 1, len(vals) - 1)
    frac = pos - lower
    return vals[lower] * (1.0 - frac) + vals[upper] * frac


def _extract_event_type(rec: dict) -> str:
    if rec.get("_type") == "LegacyAuditEvent":
        return str(rec.get("event_type", ""))
    return str(rec.get("_type", ""))


def _extract_ticker(rec: dict) -> str:
    return str(rec.get("ticker", "") or "")


def _update_snapshot(snapshot: Dict[str, float], rec: dict) -> None:
    rec_type = rec.get("_type")
    if rec_type == "MomentumMetricsComputed":
        v = _safe_float(rec.get("dollar_flow_z"))
        if v is not None:
            snapshot["dollar_flow_z"] = v
        v = _safe_float(rec.get("pressure_z"))
        if v is not None:
            snapshot["pressure_z"] = v
        return

    if rec_type == "CandidateRanked":
        v = _safe_float(rec.get("demand_score"))
        if v is not None:
            snapshot["demand_score"] = v
        return

    if rec_type == "SetupQualified":
        v = _safe_float(rec.get("sqs"))
        if v is not None:
            snapshot["sqs"] = v
        return

    if rec_type == "LegacyAuditEvent":
        if rec.get("event_type") != "DECISION_EVALUATED":
            return
        payload = rec.get("payload") if isinstance(rec.get("payload"), dict) else {}
        v = _safe_float(payload.get("demand_score"))
        if v is not None:
            snapshot["demand_score"] = v
        v = _safe_float(payload.get("sqs"))
        if v is not None:
            snapshot["sqs"] = v


def summarize_day(date_str: str, log_dir: str = "logs") -> dict:
    path = Path(log_dir) / f"events_{date_str}.jsonl"
    results: Dict[str, Dict[str, List[float]]] = {
        group: {metric: [] for metric in METRICS} for group in GROUPS
    }
    if not path.exists():
        return results

    latest_by_ticker: Dict[str, Dict[str, float]] = {}

    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            ticker = _extract_ticker(rec)
            if ticker:
                snapshot = latest_by_ticker.setdefault(ticker, {})
                _update_snapshot(snapshot, rec)

            event_type = _extract_event_type(rec)
            payload = rec.get("payload") if isinstance(rec.get("payload"), dict) else {}

            groups: List[str] = []
            if event_type in {"ORDER_SUBMITTED", "OrderSubmitted"}:
                groups.append("submitted")
            if event_type in {"ORDER_FILLED", "OrderFilled"}:
                groups.append("filled")
            if event_type in {"ORDER_REJECTED"}:
                groups.append("rejected")
            if event_type in {"POSITION_CLOSED", "LifecycleTransition"}:
                pnl_r = _safe_float(payload.get("pnl_r"))
                if pnl_r is None:
                    pnl_r = _safe_float(rec.get("pnl"))
                if pnl_r is not None:
                    groups.append("winner" if pnl_r > 0 else "loser")

            if not groups or not ticker:
                continue

            snapshot = latest_by_ticker.get(ticker, {})
            for g in groups:
                for metric in METRICS:
                    value = snapshot.get(metric)
                    if value is not None:
                        results[g][metric].append(value)

    return results


def build_report_rows(summary: dict) -> List[dict]:
    rows: List[dict] = []
    for group in GROUPS:
        for metric in METRICS:
            values = summary[group][metric]
            if not values:
                rows.append(
                    {
                        "group": group,
                        "metric": metric,
                        "n": 0,
                        "mean": "",
                        "p50": "",
                        "p75": "",
                        "p90": "",
                    }
                )
                continue
            rows.append(
                {
                    "group": group,
                    "metric": metric,
                    "n": len(values),
                    "mean": round(mean(values), 4),
                    "p50": round(_percentile(values, 0.50), 4),
                    "p75": round(_percentile(values, 0.75), 4),
                    "p90": round(_percentile(values, 0.90), 4),
                }
            )
    return rows


def print_report(rows: Iterable[dict], date_str: str) -> None:
    print(f"Replay report for {date_str}")
    print("group      metric           n   mean      p50       p75       p90")
    print("---------- ---------------- --- --------- --------- --------- ---------")
    for row in rows:
        n = row["n"]
        mean_v = row["mean"] if row["mean"] != "" else ""
        p50_v = row["p50"] if row["p50"] != "" else ""
        p75_v = row["p75"] if row["p75"] != "" else ""
        p90_v = row["p90"] if row["p90"] != "" else ""
        print(
            f"{row['group']:<10} {row['metric']:<16} {n:>3} "
            f"{str(mean_v):>9} {str(p50_v):>9} {str(p75_v):>9} {str(p90_v):>9}"
        )


def export_csv(rows: Iterable[dict], output_path: str) -> None:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["group", "metric", "n", "mean", "p50", "p75", "p90"],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay signal report by outcome group"
    )
    parser.add_argument("--date", required=True, help="Session date, e.g. 2026-03-04")
    parser.add_argument(
        "--log-dir", default="logs", help="Directory containing events_YYYY-MM-DD.jsonl"
    )
    parser.add_argument(
        "--csv-out", default="", help="Optional path to write report CSV"
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = summarize_day(args.date, log_dir=args.log_dir)
    rows = build_report_rows(summary)
    print_report(rows, args.date)
    if args.csv_out:
        export_csv(rows, args.csv_out)


if __name__ == "__main__":
    main()
