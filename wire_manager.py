import re

with open('execution/engine.py', 'r', encoding='utf-8') as f:
    text = f.read()

# Replace the __init__ area around self._cycle_count to add the RiskManager init
init_injection = '''self._cycle_count: int = 0

        # Phase 4: Risk Manager Kill Switch
        try:
            from risk.manager import RiskManager
            self.risk_manager = RiskManager(self._broker.ib, max_drawdown_pct=0.02)
        except Exception as e:
            from risk.manager import RiskManager
            from infrastructure.ib_connection import IBConnectionManager
            self.risk_manager = RiskManager(IBConnectionManager().ib, max_drawdown_pct=0.02)

'''
text = re.sub(r'self._cycle_count: int = 0\n', init_injection, text)

# Inject the check into the while loop
loop_injection = '''while True:
                if getattr(self, "risk_manager", None) and self.risk_manager.check_kill_switch():
                    log.critical("SYSTEM HALTED BY RISK MANAGER.")
                    break
'''
text = re.sub(r'while True:', loop_injection, text)

with open('execution/engine.py', 'w', encoding='utf-8') as f:
    f.write(text)
