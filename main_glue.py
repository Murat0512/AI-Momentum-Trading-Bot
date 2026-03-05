"""
main_glue.py - Optional additive entry point for the milestone glue flow.

This does not replace the repository's production main.py.
"""

from __future__ import annotations

import logging
import time

## Removed AlpacaBridge import; IBKR-only cleanup
import os
from config.settings import CONFIG
from engine import MomentumEngine

# If available, load .env file manually, though PowerShell already set it in env
try:
import os
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass



logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("MainGlue")


def start_engine() -> None:
    logger.info("Initializing Momentum Glue Engine...")
    # TODO: Insert IBKR broker/data provider initialization here if needed
## Removed AlpacaBridge import; IBKR-only cleanup

    logger.info("Account Size: $%s", CONFIG.risk.account_size)
    # Placeholder for main loop
    logger.info("[CLEANUP] AlpacaBridge and Alpaca references removed. Please configure IBKR provider.")
    # TODO: Insert IBKR broker/data provider initialization here if needed
    # engine = MomentumEngine(broker_adapter=ibkr_adapter, data_provider=ibkr_data)
if __name__ == "__main__":
    start_engine()
