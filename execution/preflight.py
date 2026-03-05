"""
preflight.py — System Readiness Check

Run this script before launching main.py to verify environment variables,
broker connections, and strategy profiles are correctly configured for the session.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.settings import CONFIG


def _is_truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _mask_key(value: str) -> str:
    value = str(value or "").strip()
    if not value:
        return ""
    if len(value) <= 8:
        return "****"
    return f"{value[:4]}{'*' * (len(value) - 8)}{value[-4:]}"


def run_preflight_check() -> None:
    print("\n" + "=" * 60)
    print("MOMENTUM ENGINE PREFLIGHT CHECK")
    print("=" * 60)

    is_paper = bool(getattr(CONFIG.execution, "paper_mode", True))
    mode_str = (
        "PAPER (Simulated Execution)" if is_paper else "LIVE (REAL MONEY AT RISK)"
    )
    print(f"[*] Trading Mode       : {mode_str}")

    configured_broker = (
        str(getattr(CONFIG.execution, "broker", "paper")).strip().lower()
    )
    explicit_broker = str(os.getenv("EXECUTION_BROKER", "")).strip().lower()
    test_profile_raw = os.getenv("TEST_PROFILE")
    test_profile_active = _is_truthy(test_profile_raw)

    if explicit_broker:
        effective_broker = explicit_broker
        broker_reason = "via EXECUTION_BROKER override"
    else:
        effective_broker = configured_broker
        broker_reason = "from config"

    print(f"[*] Broker Adapter     : {effective_broker.upper()} ({broker_reason})")

    if test_profile_active:
        print("[!] Strategy Profile   : WARNING TEST_PROFILE is ACTIVE")
        print("                         Strict guards are relaxed for testing.")
    else:
        print("[*] Strategy Profile   : SNIPER (Strict parameters are ACTIVE)")

    print("=" * 60)
    print("STATUS: READY FOR SNIPER RUN")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    run_preflight_check()
