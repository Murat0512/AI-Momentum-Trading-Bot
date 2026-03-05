"""
events/bus.py — Lightweight in-process event bus.

Design contract:
    * publish() NEVER raises — sink exceptions are swallowed and logged.
    * Sinks are callables: sink(event) → None
    * Thread-safe: parallel publish() calls are safe (GIL sufficient for list reads)
    * Zero external dependencies (no asyncio, no queues)
"""

from __future__ import annotations

import logging
from typing import Callable, List

log = logging.getLogger(__name__)


class EventBus:
    """
    In-process synchronous event bus.

    Usage::

        from events.bus import event_bus
        from events.types import OrderFilled

        event_bus.register(my_sink)
        event_bus.publish(OrderFilled(order_id="X", ticker="NVDA", ...))
    """

    def __init__(self) -> None:
        self._sinks: List[Callable] = []

    def register(self, sink: Callable) -> None:
        """Add a sink callable. Sinks are called in registration order."""
        self._sinks.append(sink)

    def unregister(self, sink: Callable) -> None:
        """Remove a sink (e.g. in tests for cleanup)."""
        try:
            self._sinks.remove(sink)
        except ValueError:
            pass

    def clear_sinks(self) -> None:
        """Remove all sinks. Useful between test cases."""
        self._sinks.clear()

    def publish(self, event: object) -> None:
        """
        Publish an event to all registered sinks.
        Guarantees: this method never raises, regardless of sink behaviour.
        """
        for sink in self._sinks:
            try:
                sink(event)
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "[EventBus] sink %s raised on %s: %s",
                    getattr(sink, "__name__", repr(sink)),
                    type(event).__name__,
                    exc,
                )

    def __len__(self) -> int:
        return len(self._sinks)


# Module-level singleton — import and use everywhere
event_bus = EventBus()
