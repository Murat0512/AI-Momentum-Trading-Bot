import asyncio
import sys

if sys.version_info >= (3, 10):
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())
from ib_insync import *
import pandas as pd
import matplotlib.pyplot as plt
import os
from datetime import datetime


# --- 1. THE REACTIVE LOGIC ---
class ReactiveLogic:
    def __init__(self, vol_mult=3.0):
        self.vol_mult = vol_mult

    def analyze(self, df):
        if len(df) < 20:
            return None
        df_calc = df.copy()
        df_calc["sma_10"] = df_calc["close"].rolling(10).mean()
        df_calc["vol_sma_10"] = df_calc["volume"].rolling(10).mean()

        last = df_calc.iloc[-1]
        prev = df_calc.iloc[-2]

        vol_burst = last["volume"] > (last["vol_sma_10"] * self.vol_mult)
        slope_up = last["sma_10"] > prev["sma_10"]

        if vol_burst and slope_up:
            return {
                "Time": last["date"],
                "Price": last["close"],
                "Volume": last["volume"],
            }
        return None


# --- 2. THE VISUALIZER ---
def save_signal_chart(symbol, df, signals):
    """Generates and saves a PNG chart of the price action, signals, and volume."""
    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(14, 8), sharex=True, gridspec_kw={"height_ratios": [2, 1]}
    )

    # Price and SMA
    ax1.plot(df["date"], df["close"], label="Price", color="black", alpha=0.7)
    ax1.plot(
        df["date"],
        df["close"].rolling(10).mean(),
        label="SMA-10",
        color="blue",
        alpha=0.5,
    )

    # Mark signals
    if signals:
        sig_times = [s["Time"] for s in signals]
        sig_prices = [s["Price"] for s in signals]
        ax1.scatter(
            sig_times,
            sig_prices,
            color="green",
            marker="^",
            s=100,
            label="Burst Signal",
            zorder=5,
        )

    ax1.set_title(f"Reactive Momentum Test: {symbol}")
    ax1.set_ylabel("Price")
    ax1.legend()
    ax1.grid(True, which="both", linestyle="--", alpha=0.5)

    # Volume and Volume SMA
    ax2.bar(df["date"], df["volume"], label="Volume", color="gray", alpha=0.6)
    ax2.plot(
        df["date"],
        df["volume"].rolling(10).mean(),
        label="Vol SMA-10",
        color="orange",
        linewidth=2,
    )
    ax2.set_ylabel("Volume")
    ax2.legend()
    ax2.grid(True, which="both", linestyle="--", alpha=0.5)

    plt.xlabel("Time")
    plt.tight_layout()

    # Save logic
    if not os.path.exists("backtest_plots"):
        os.makedirs("backtest_plots")

    filename = f"backtest_plots/{symbol}_{datetime.now().strftime('%Y%m%d')}.png"
    plt.savefig(filename, dpi=200)
    plt.close()
    print(f"Chart saved: {filename}")


# --- 3. MAIN RUNNER ---
def run_visual_backtest(tickers):
    ib = IB()
    all_signals = []
    try:
        ib.connect("127.0.0.1", 7497, clientId=20)
        ib.reqMarketDataType(3)  # Use delayed data if live is unavailable

        logic = ReactiveLogic(vol_mult=3.0)

        for symbol in tickers:
            contract = Stock(symbol, "SMART", "USD")
            ib.qualifyContracts(contract)

            bars = ib.reqHistoricalData(contract, "", "1 D", "1 min", "TRADES", True)
            if not bars:
                print(f"No data for {symbol}.")
                continue

            df = util.df(bars)
            ticker_signals = []

            for i in range(20, len(df)):
                sig = logic.analyze(df.iloc[: i + 1])
                if sig:
                    sig["Symbol"] = symbol
                    ticker_signals.append(sig)
                    all_signals.append(sig)

            save_signal_chart(symbol, df, ticker_signals)

        # Export all signals to CSV for automated review
        if all_signals:
            signals_df = pd.DataFrame(all_signals)
            signals_df = signals_df[["Symbol", "Time", "Price", "Volume"]]
            csv_name = f"backtest_plots/buy_signals_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
            signals_df.to_csv(csv_name, index=False)
            print(f"All buy signals exported to: {csv_name}")
        else:
            print("No buy signals detected for any ticker.")
    finally:
        ib.disconnect()


if __name__ == "__main__":
    run_visual_backtest(["NVDA", "TSLA", "AAPL", "AMD", "MSFT", "TTD"])
