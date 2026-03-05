import re

with open('risk/manager.py', 'r', encoding='utf-8') as f:
    text = f.read()

injection = '''
    def __init__(self, ib_connection=None, max_drawdown_pct=0.02, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Handle singleton pattern safely
        if getattr(self, "_initialized_phase4", False):
            return
        self._initialized_phase4 = True
        
        self.ib = ib_connection
        self.max_drawdown_pct = max_drawdown_pct
        self.starting_balance = 0.0
        self.is_halted = False
        
        if self.ib and self.ib.isConnected():
            self._init_balance()

    def _init_balance(self):
        try:
            summary = self.ib.accountSummary()
            for item in summary:
                if item.tag == 'NetLiquidation':
                    self.starting_balance = float(item.value)
                    log.info(f"??? RISK MANAGER ONLINE: Starting Balance => \$ {self.starting_balance:.2f}")
                    break
        except Exception as e:
            log.error(f"Failed to fetch starting balance: {e}")

    def check_kill_switch(self) -> bool:
        if not self.ib or not self.ib.isConnected() or self.starting_balance == 0.0:
            return False

        if self.is_halted:
            return True

        try:
            current_balance = self.starting_balance
            summary = self.ib.accountSummary()
            for item in summary:
                if item.tag == 'NetLiquidation':
                    current_balance = float(item.value)
                    break
            
            drawdown = (self.starting_balance - current_balance) / self.starting_balance
            
            if drawdown >= self.max_drawdown_pct:
                log.critical(f"FATAL DRAWDOWN: {-drawdown*100:.2f}% (Limit: {-self.max_drawdown_pct*100:.2f}%)")
                self.flatten_all_positions()
                self.is_halted = True
                return True
                
        except Exception as e:
            log.error(f"Error checking kill switch: {e}")
            
        return False

    def flatten_all_positions(self):
        log.critical("FLATTENING ALL POSITIONS...")
        if not self.ib or not self.ib.isConnected():
            return
            
        from ib_insync import MarketOrder
        try:
            positions = self.ib.positions()
            for p in positions:
                if p.position == 0:
                    continue
                action = "SELL" if p.position > 0 else "BUY"
                qty = abs(p.position)
                log.critical(f"Flattening {qty} of {p.contract.symbol} ({action})")
                order = MarketOrder(action, qty)
                self.ib.placeOrder(p.contract, order)
        except Exception as e:
            log.error(f"Failed to flatten positions: {e}")
'''
# Inject these methods into the RiskManager class
# We'll just replace the original __init__ def and append the rest
text = re.sub(r'def __init__\(self\):', injection + '\n    def __init_original__(self):', text)

with open('risk/manager.py', 'w', encoding='utf-8') as f:
    f.write(text)
