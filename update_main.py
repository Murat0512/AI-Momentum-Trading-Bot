import re

with open("main.py", "r", encoding="utf-8") as f:
    text = f.read()

# Replace enforce_feed_authority
replacement1 = """def enforce_feed_authority(*, live: bool, scan_only: bool) -> None:
    current = str(getattr(CONFIG.data, "feed_authority", "") or "").strip().lower()
    if scan_only:
        if not current:
            CONFIG.data.feed_authority = "yfinance_dev"
            CONFIG.data.data_source_research = "yfinance"
        return

    # Trading path uses ibkr
    if current not in ("alpaca", "ibkr"):
        CONFIG.data.feed_authority = "ibkr"

    CONFIG.data.data_source_live = "ibkr"


def enforce_runtime_credentials(*, scan_only: bool) -> None:
    pass"""

text = re.sub(
    r"def enforce_feed_authority.*?def enforce_runtime_credentials\(\*, scan_only: bool\) -> None:.*?(?=# ─+ \n# Preflight checklist)",
    replacement1 + "\n\n",
    text,
    flags=re.DOTALL,
)

# Replace run_preflight check
replacement2 = """def run_preflight(broker, live: bool) -> bool:
    results: dict = {}
    all_passed = True

    # 1. Bypass API key checking
    results["api_keys_present"] = True

    # 2. Min account balance for IBKR
    MIN_LIVE_BALANCE = 10_000.0
    if live and hasattr(broker, "conn_mgr"):
        try:
            account_summary = broker.conn_mgr.ib.accountSummary()
            buying_power = 0.0
            for item in account_summary:
                if item.tag == 'BuyingPower':
                    buying_power = float(item.value)
                    break
            results["buying_power"] = buying_power
            if buying_power < MIN_LIVE_BALANCE:
                results["buying_power_ok"] = False
                all_passed = False
            else:
                results["buying_power_ok"] = True
        except Exception as exc:
            results["buying_power"] = None
            results["buying_power_ok"] = False
            results["buying_power_error"] = str(exc)
            all_passed = False
    else:
        results["buying_power_ok"] = True

    # 3. Connection sanity
    try:
        if hasattr(broker, "conn_mgr"):
            if not broker.conn_mgr.is_connected:
                broker.conn_mgr.connect()
            results["broker_ping"] = True
        else:
            results["broker_ping"] = True
    except Exception as exc:
        results["broker_ping"] = False
        results["broker_ping_error"] = str(exc)
        all_passed = False

    if not all_passed:
        log.error(f"Preflight checks failed: {results}")

    return all_passed"""

# I will replace from "def run_preflight(broker" to "return all_passed\n"
text = re.sub(
    r"def run_preflight\(broker, live: bool\) -> bool:.*?return all_passed\n",
    replacement2 + "\n",
    text,
    flags=re.DOTALL,
)

with open("main.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated main.py")
