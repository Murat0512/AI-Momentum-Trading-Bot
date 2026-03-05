import sys
import logging
from data.fetcher import BatchFetcher

# Set up basic logging so we can see any inner warnings from ib_insync
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


def run_tws_ping():
    print("=" * 60)
    print("🚀 INITIATING TWS DATA PING TEST (IBKR)")
    print("=" * 60)

    # 1. Initialize the new IBKR fetcher
    # We explicitly call 'ibkr' to trigger your new IBAdapter
    try:
        fetcher = BatchFetcher(adapter_name="ibkr")
        print("✅ Fetcher initialized successfully. Connecting to TWS...")
    except Exception as e:
        print(f"❌ Failed to initialize fetcher: {e}")
        return

    tickers_to_test = ["SPY", "QQQ", "AAPL"]
    print(f"\n📡 Testing Historical Data (1m Bars) for: {tickers_to_test}")
    print("⏳ Waiting for paced API responses (Expect a 1-second delay per ticker)...")

    # 2. Fetch Historical Bars
    # We request 2 days of data to ensure we catch the most recent market hours
    historical_results = fetcher.fetch_all(tickers_to_test, days=2)

    for ticker in tickers_to_test:
        df = historical_results.get(ticker)
        if df is not None and not df.empty:
            # Print the shape (rows, columns) and the timestamp of the very last bar
            print(
                f"  🟢 {ticker}: Success! Retrieved {df.shape[0]} rows. Last Bar: {df.index[-1]}"
            )
        else:
            print(
                f"  🔴 {ticker}: FAILED. No data returned. Check TWS permissions or contract validity."
            )

    # 3. Fetch Live Snapshot Quotes
    print("\n⚡ Testing Live Snapshot Quotes...")
    quote_results = fetcher.fetch_quotes(tickers_to_test)

    for ticker in tickers_to_test:
        q = quote_results.get(ticker)
        if q and q.get("_quote_quality") != "fetch_error":
            bid = q.get("bid", 0.0)
            ask = q.get("ask", 0.0)
            last = q.get("last", 0.0)
            quality = q.get("_quote_quality", "unknown")

            # Diagnostic: If Bid/Ask are 0.0 or -1.0, you lack real-time data subscriptions in TWS
            if bid <= 0 or ask <= 0:
                status = "⚠️ WARNING: Delayed Data or No Live Subscription"
            else:
                status = "✅ Live Data Flowing"

            print(
                f"  💰 {ticker}: Bid=${bid:.2f} | Ask=${ask:.2f} | Last=${last:.2f} | {status}"
            )
        else:
            print(f"  🔴 {ticker}: FAILED to fetch quote.")

    print("\n" + "=" * 60)
    print("🏁 TWS DATA PING TEST COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    run_tws_ping()
