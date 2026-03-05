"""
events/sinks/test_sink.py — In-memory sink for unit testing.

Usage::

    from events.sinks.test_sink import TestSink
    from events.bus import event_bus
    from events.types import OrderFilled

    sink = TestSink()
    event_bus.register(sink)

    # ... run code that publishes events ...

    fills = sink.of_type(OrderFilled)
    assert len(fills) == 1
"""

from __future__ import annotations

from typing import List, Type, TypeVar

T = TypeVar("T")


class TestSink:
    """
    Collects all published events into an in-memory list for inspection.

    This class is intentionally kept simple and framework-agnostic so it
    can be used with pytest, unittest, or any other test runner.
    """

    def __init__(self) -> None:
        self.events: List[object] = []

    # ── Callable sink interface ────────────────────────────────────────────────

    def __call__(self, event: object) -> None:
        self.events.append(event)

    # ── Query helpers ─────────────────────────────────────────────────────────

    def of_type(self, event_type: Type[T]) -> List[T]:
        """Return all recorded events that are instances of *event_type*."""
        return [e for e in self.events if isinstance(e, event_type)]

    def latest(self, event_type: Type[T]) -> T:
        """Return the most recent event of *event_type*, or raise IndexError."""
        matches = self.of_type(event_type)
        if not matches:
            raise IndexError(f"No events of type {event_type.__name__} recorded")
        return matches[-1]

    def count(self, event_type: Type[T] = None) -> int:
        """Count events, optionally filtered by type."""
        if event_type is None:
            return len(self.events)
        return len(self.of_type(event_type))

    def clear(self) -> None:
        """Reset the event list."""
        self.events.clear()

    def __len__(self) -> int:
        return len(self.events)

    def __repr__(self) -> str:  # pragma: no cover
        return f"TestSink({len(self.events)} events)"
