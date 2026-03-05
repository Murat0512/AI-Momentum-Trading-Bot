"""
events/__init__.py — Domain event bus package.

Provides:
    current_cycle  — mutable context object; engine sets .id each tick
    event_bus      — re-exported singleton from events.bus
"""

from __future__ import annotations


class _CycleCtx:
    """Mutable cycle-ID carrier; thread-safe for single-writer / many-reader."""

    __slots__ = ("id",)

    def __init__(self) -> None:
        self.id: int = 0


current_cycle = _CycleCtx()

# Convenience re-export so callers can do: from events import event_bus
from events.bus import event_bus  # noqa: E402

__all__ = ["current_cycle", "event_bus"]
