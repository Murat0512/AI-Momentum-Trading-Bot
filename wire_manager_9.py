code = '''import logging
import time
from infrastructure.ib_connection import IBConnectionManager
from execution.orders import IBBroker
from scanner.dynamic_universe import DynamicUniverse
from risk.manager import RiskManager
from strategy.burst_logic import MomentumBurstLogic
from data.fetcher import BatchFetcher

class TradingEngine:
    def __init__(self):
        self.logger = logging.getLogger('TradingEngine')
        
        # 1. Initialize Singleton Infrastructure
        self.conn_mgr = IBConnectionManager()
        self.conn_mgr.connect()
        self.ib = self.conn_mgr.ib
        
        # 2. Initialize Modules
        self.broker = IBBroker()
        self.universe = DynamicUniverse(self.ib)
        self.risk_manager = RiskManager(self.ib, max_drawdown_pct=0.02)
        self.strategy = MomentumBurstLogic()
        self.fetcher = BatchFetcher(adapter_name='ibkr')
        
        # 3. Position Memory Bank
        # Format: { 'AAPL': {'entry_price': 150.00, 'highest_price': 150.00, 'qty': 10} }
        self.active_positions = {}
        self.tickers = []

    def sync_existing_positions(self):
        """
        Reads existing TWS positions on boot and populates the memory bank so it doesn't lose track of active trades if restarted.
        """
        self.logger.info("?? Syncing existing portfolio positions...")
        positions = self.ib.positions()
        for pos in positions:
            ticker = pos.contract.symbol
            qty = int(pos.position)
            if qty > 0:
                # Get a live quote to establish a baseline if we restarted the bot mid-trade
                quote = self.broker._get_live_quote(pos.contract)
                current_price = quote['ask'] if quote['ask'] > 0 else pos.avgCost
                
                self.active_positions[ticker] = {
                    'entry_price': pos.avgCost,
                    'highest_price': current_price,
                    'qty': qty
                }
                self.logger.info(f"?? Synced: {qty} shares of {ticker} @ avg cost \")

    def run(self):
        self.logger.info("="*60)
        self.logger.info("?? MOMENTUM DAY TRADING ENGINE — MILESTONE 4 ONLINE")
        self.logger.info("="*60)
        
        self.sync_existing_positions()
        self.tickers = self.universe.get_top_gainers()
        
        # The Infinite Heartbeat Loop
        while True:
            try:
                # 1. THE HARD KILL SWITCH
                if self.risk_manager.check_kill_switch():
                    self.logger.critical("?? SYSTEM HALTED BY RISK MANAGER. Terminating heartbeat.")
                    break

                # 2. DEFENSE PHASE: Manage Open Positions
                positions_to_remove = []
                for ticker, state in self.active_positions.items():
                    # Create contract and get instantaneous quote
                    from ib_insync import Stock
                    contract = Stock(ticker, 'SMART', 'USD')
                    quote = self.broker._get_live_quote(contract)
                    current_price = quote['bid'] # Use Bid price since we are evaluating a SELL
                    
                    if current_price <= 0:
                        continue
                        
                    # Update Highest Price Memory
                    if current_price > state['highest_price']:
                        state['highest_price'] = current_price
                        self.logger.debug(f"?? {ticker} made a new high: \")

                    # Ask the Strategy Engine what to do
                    action = self.strategy.manage_open_position(
                        ticker, 
                        state['entry_price'], 
                        current_price, 
                        state['highest_price']
                    )

                    if action in ["SELL_STOP_LOSS", "SELL_TRAIL_PROFIT"]:
                        self.logger.info(f"? EXECUTING EXIT: {action} on {ticker}")
                        trade = self.broker.sell(ticker, state['qty'])
                        if trade:
                            positions_to_remove.append(ticker)

                # Clean up memory bank after selling
                for ticker in positions_to_remove:
                    del self.active_positions[ticker]

                # 3. OFFENSE PHASE: Scan for New Setups
                # Only scan if we have cash/capacity to take new trades (e.g., max 3 concurrent positions)
                if len(self.active_positions) < 3:
                    historical_data = self.fetcher.fetch_all(self.tickers, days=1)
                    
                    for ticker in self.tickers:
                        if ticker in self.active_positions:
                            continue # Don't buy a stock we already own
                            
                        df = historical_data.get(ticker)
                        if self.strategy.evaluate_setup(ticker, df):
                            self.logger.info(f"?? SETUP CONFIRMED: Triggering BUY on {ticker}")
                            
                            # Fixed 10 share test size for now, can be dynamically calculated later based on Buying Power
                            qty = 10 
                            trade = self.broker.buy(ticker, qty)
                            
                            if trade:
                                # Add to memory bank to begin trailing immediately
                                fill_price = float(trade.orderStatus.avgFillPrice) if trade.orderStatus.avgFillPrice > 0 else df.iloc[-1]['close']
                                self.active_positions[ticker] = {
                                    'entry_price': fill_price,
                                    'highest_price': fill_price,
                                    'qty': qty
                                }

                # Pace the loop to avoid API limits
                time.sleep(60)

            except Exception as e:
                self.logger.error(f"? FATAL LOOP ERROR: {e}")
                time.sleep(10) # Pause briefly before attempting to recover loop

if __name__ == "__main__":
    import coloredlogs
    coloredlogs.install(level='DEBUG')
    engine = TradingEngine()
    engine.run()
'''

with open('execution/engine.py', 'w', encoding='utf-8') as f:
    f.write(code)
