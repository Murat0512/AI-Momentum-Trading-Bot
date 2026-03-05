code = '''import pandas as pd
import logging

class MomentumBurstLogic:
    def __init__(self, volume_multiplier: float = 3.0, hard_stop_pct: float = 0.02, trail_activation_pct: float = 0.015, trail_distance_pct: float = 0.01):
        """
        Initializes the quantitative momentum parameters.
        volume_multiplier: Requires 300% of average volume to trigger.
        hard_stop_pct: Strict -2.0% floor on all entries.
        trail_activation_pct: Stock must reach +1.5% profit to start trailing.
        trail_distance_pct: Trails the peak by exactly 1.0%.
        """
        self.logger = logging.getLogger(__name__)
        self.volume_multiplier = volume_multiplier
        self.hard_stop_pct = hard_stop_pct
        self.trail_activation_pct = trail_activation_pct
        self.trail_distance_pct = trail_distance_pct

    def evaluate_setup(self, ticker: str, df: pd.DataFrame) -> bool:
        """
        Analyzes the 1-minute DataFrame to find explosive volume and price breakouts.
        Returns True if the setup meets strict institutional criteria.
        """
        if df is None or len(df) < 15:
            return False

        try:
            # Calculate the 10-period SMA for Volume
            df['vol_sma_10'] = df['volume'].rolling(window=10).mean()
            # Calculate the 10-period recent high for Price Breakout
            df['recent_high_10'] = df['high'].rolling(window=10).max().shift(1)

            # Isolate the most recently completed 1-minute bar
            current_bar = df.iloc[-1]
            
            # Prevent division by zero errors on flat data
            if pd.isna(current_bar['vol_sma_10']) or current_bar['vol_sma_10'] == 0:
                return False

            # Math Check 1: Is the volume explosive? (> 300% of SMA)
            volume_spike = current_bar['volume'] > (current_bar['vol_sma_10'] * self.volume_multiplier)
            
            # Math Check 2: Did the closing price break the recent 10-minute high?
            price_breakout = current_bar['close'] > current_bar['recent_high_10']

            if volume_spike and price_breakout:
                self.logger.info(f"?? MOMENTUM BURST DETECTED: {ticker} | Vol: {current_bar['volume']} (SMA: {current_bar['vol_sma_10']:.0f}) | Breakout Price: \")
                return True
                
            return False

        except Exception as e:
            self.logger.error(f"? Math calculation failed on {ticker}: {e}")
            return False

    def manage_open_position(self, ticker: str, entry_price: float, current_price: float, highest_price: float) -> str:
        """
        The Dynamic Exit Router. 
        Returns 'HOLD', 'SELL_STOP_LOSS', or 'SELL_TRAIL_PROFIT' based on exact percentages.
        """
        if entry_price <= 0 or current_price <= 0:
            return "HOLD"

        # 1. Check Hard Stop Loss
        drawdown_pct = (current_price - entry_price) / entry_price
        if drawdown_pct <= -self.hard_stop_pct:
            self.logger.warning(f"?? STOP LOSS HIT: {ticker} dropped {(drawdown_pct*100):.2f}%. Executing defensive exit.")
            return "SELL_STOP_LOSS"

        # 2. Check Trailing Take-Profit
        profit_pct = (current_price - entry_price) / entry_price
        
        # Has the stock climbed high enough to activate the trailing stop?
        if profit_pct >= self.trail_activation_pct:
            # Calculate how far the current price has dropped from the highest observed price
            pullback_from_peak_pct = (highest_price - current_price) / highest_price
            
            # If it drops more than 1.0% from the peak, lock in the win
            if pullback_from_peak_pct >= self.trail_distance_pct:
                self.logger.info(f"?? TRAIL PROFIT HIT: {ticker} pulled back {(pullback_from_peak_pct*100):.2f}% from peak of \. Securing gains.")
                return "SELL_TRAIL_PROFIT"

        # 3. If no conditions are met, hold the line
        return "HOLD"
'''

with open('strategy/burst_logic.py', 'w', encoding='utf-8') as f:
    f.write(code)
