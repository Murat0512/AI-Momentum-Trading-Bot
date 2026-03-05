import logging
import asyncio

try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

from ib_insync import Stock, MarketOrder
from infrastructure.ib_connection import IBConnectionManager

# Setup basic logging to catch any IBKR system messages
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


def test_live_order():
    print("=" * 60)
    print("🚀 INITIATING TWS ORDER EXECUTION TEST (IBKR)")
    print("=" * 60)

    # 1. Connect to TWS
    conn_mgr = IBConnectionManager()
    conn_mgr.connect()
    ib = conn_mgr.ib

    ticker = "AAPL"
    quantity = 1

    # 2. Qualify the Contract
    print(f"📡 Qualifying contract for {ticker}...")
    contract = Stock(ticker, "SMART", "USD")
    try:
        ib.qualifyContracts(contract)
        print("✅ Contract qualified successfully.")
    except Exception as e:
        print(f"❌ Failed to qualify {ticker}. Error: {e}")
        return

    # 3. Define the Order
    print(f"⚡ Placing MARKET BUY order for {quantity} share(s) of {ticker}...")
    order = MarketOrder("BUY", quantity)

    # 4. Fire the Order
    trade = ib.placeOrder(contract, order)

    # 5. Wait for the Fill
    print("⏳ Waiting for IBKR fill response (2 seconds)...")
    ib.sleep(2.0)

    # 6. Report the Results
    print("\n" + "=" * 60)
    print("🏁 EXECUTION RESULTS")
    print(f"✅ Order Status: {trade.orderStatus.status}")
    print(
        f"💰 Shares Filled: {trade.orderStatus.filled} @ Average Price: ${trade.orderStatus.avgFillPrice}"
    )
    print("=" * 60)


if __name__ == "__main__":
    test_live_order()
