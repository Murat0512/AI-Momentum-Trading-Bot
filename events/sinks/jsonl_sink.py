"""
events/sinks/jsonl_sink.py — Append-only JSONL event log.

One JSON object per line, flushed after every write.
File rotates daily: logs/events_YYYY-MM-DD.jsonl

Usage::

    from events.sinks.jsonl_sink import JsonlSink
    from events.bus import event_bus

    sink = JsonlSink()
    event_bus.register(sink)
"""

from __future__ import annotations

import dataclasses
import json
import logging
import os
from datetime import date, datetime
from pathlib import Path
from typing import IO, Optional

log = logging.getLogger(__name__)

_LOGS_DIR = Path("logs")


def _default_serialiser(obj):
    """JSON serialiser hook: converts datetime → ISO string, others → str."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, date):
        return obj.isoformat()
    return str(obj)


class JsonlSink:
    """
    Callable sink that writes each domain event to a daily JSONL file.

    Thread-safety: each call to __call__ opens and closes (or flushes) in a
    way that is safe for single-writer use.  If multi-thread writes are needed,
    wrap calls with a lock at a higher level.
    """

    def __init__(self, logs_dir: Path = _LOGS_DIR) -> None:
        self._logs_dir = Path(logs_dir)
        self._logs_dir.mkdir(parents=True, exist_ok=True)
        self._current_date: Optional[str] = None
        self._fh: Optional[IO] = None

    # ── Callable interface ────────────────────────────────────────────────────

    def __call__(self, event: object) -> None:
        today = date.today().isoformat()
        if today != self._current_date:
            self._rotate(today)

        if self._fh is None:
            return

        try:
            d = dataclasses.asdict(event)
        except TypeError:
            # Non-dataclass event (shouldn't happen, but be defensive)
            d = {"_raw": repr(event)}

        d["_type"] = type(event).__name__
        line = json.dumps(d, default=_default_serialiser)
        self._fh.write(line + "\n")
        self._fh.flush()

    # ── File management ───────────────────────────────────────────────────────

    def _rotate(self, today: str) -> None:
        """Open a new file for today, closing the previous one if open."""
        self.close()
        path = self._logs_dir / f"events_{today}.jsonl"
        try:
            self._fh = open(path, "a", encoding="utf-8", buffering=1)  # line-buffered
            self._current_date = today
            log.debug("[JsonlSink] opened %s", path)
        except OSError as exc:
            log.error("[JsonlSink] cannot open %s: %s", path, exc)
            self._fh = None

    def close(self) -> None:
        """Flush and close the current log file."""
        if self._fh is not None:
            try:
                self._fh.flush()
                self._fh.close()
            except OSError:
                pass
            self._fh = None
            self._current_date = None
