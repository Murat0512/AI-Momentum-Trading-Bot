"""
events/sinks/console_sink.py — DEBUG-level log-line for every event.

Useful during development to see the event stream without parsing files.

Usage::

    from events.sinks.console_sink import console_sink
    from events.bus import event_bus

    event_bus.register(console_sink)
"""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)


def console_sink(event: object) -> None:
    """
    Log every domain event at DEBUG level.
    Format: [Event] <ClassName> cycle=<N> ticker=<T> ...
    """
    cls = type(event).__name__
    cycle = getattr(event, "cycle_id", "?")
    ticker = getattr(event, "ticker", "")
    extra = f" ticker={ticker}" if ticker else ""
    log.debug("[Event] %s cycle=%s%s", cls, cycle, extra)
