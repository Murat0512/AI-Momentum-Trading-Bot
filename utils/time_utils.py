"""
time_utils.py — Market session time helpers (US/Eastern).
"""

from __future__ import annotations

from datetime import datetime, time

import pytz

from config.settings import CONFIG

ET = pytz.timezone("America/New_York")


def now_et() -> datetime:
    return datetime.now(ET)


def _parse_time(t: str) -> time:
    """Parse 'HH:MM' string to time object."""
    h, m = t.split(":")
    return time(int(h), int(m))


def is_session_active(now: datetime = None) -> bool:
    """True if current ET time is within the configured trading window."""
    now  = now or now_et()
    t    = now.timetz() if now.tzinfo else now.replace(tzinfo=ET).timetz()
    # Strip tzinfo from time for comparison
    t    = now.time()
    open_t  = _parse_time(CONFIG.session.session_open)
    close_t = _parse_time(CONFIG.session.session_close)
    return open_t <= t <= close_t


def minutes_to_close(now: datetime = None) -> float:
    """Minutes remaining until session close."""
    now = now or now_et()
    close = now.replace(
        hour   = int(CONFIG.session.session_close.split(":")[0]),
        minute = int(CONFIG.session.session_close.split(":")[1]),
        second = 0,
        microsecond = 0,
    )
    delta = (close - now).total_seconds() / 60
    return max(delta, 0.0)


def minutes_since_open(now: datetime = None) -> float:
    """Minutes elapsed since session open."""
    now = now or now_et()
    open_dt = now.replace(
        hour   = int(CONFIG.session.session_open.split(":")[0]),
        minute = int(CONFIG.session.session_open.split(":")[1]),
        second = 0,
        microsecond = 0,
    )
    delta = (now - open_dt).total_seconds() / 60
    return max(delta, 0.0)


def is_premarket(now: datetime = None) -> bool:
    now = now or now_et()
    t   = now.time()
    pm_start = _parse_time(CONFIG.session.premarket_start)
    pm_end   = _parse_time(CONFIG.session.premarket_end)
    return pm_start <= t < pm_end


def next_bar_close_seconds(interval_minutes: int = 1) -> float:
    """
    Seconds until the next bar close (aligned to clock minutes).
    Useful for sleeping precisely to bar boundaries.
    """
    now     = now_et()
    elapsed = now.second + now.microsecond / 1e6
    bar_sec = interval_minutes * 60
    return bar_sec - (elapsed % bar_sec)
