from __future__ import annotations

import json
from datetime import datetime

import pytz

from scanner.movers import MoversIngestor

ET = pytz.timezone("America/New_York")


def test_movers_ingestor_drains_tradingview_queue(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    logs = tmp_path / "logs"
    logs.mkdir(parents=True, exist_ok=True)

    queue = logs / "tradingview_signal_queue.jsonl"
    queue.write_text(
        "\n".join(
            [
                json.dumps({"ticker": "TSLA", "price": 250.1, "volume": 10000}),
                json.dumps({"symbol": "NVDA", "price": 179.9}),
                json.dumps({"ticker": "TSLA", "price": 251.0}),  # duplicate
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    ing = MoversIngestor()
    tickers = ing.active_tickers(datetime.now(ET))

    assert "TSLA" in tickers
    assert "NVDA" in tickers
    assert not queue.exists()


def test_movers_ingestor_ignores_invalid_queue_rows(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    logs = tmp_path / "logs"
    logs.mkdir(parents=True, exist_ok=True)

    queue = logs / "tradingview_signal_queue.jsonl"
    queue.write_text(
        "\n".join(
            [
                "not-json",
                json.dumps({"foo": "bar"}),
                json.dumps({"ticker": ""}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    ing = MoversIngestor()
    tickers = ing.active_tickers(datetime.now(ET))

    assert tickers == []
