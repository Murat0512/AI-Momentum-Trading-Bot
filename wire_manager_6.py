import re

with open('risk/manager.py', 'r', encoding='utf-8') as f:
    text = f.read()

replacement = '''
    def __init__(self, ib_connection=None, max_drawdown_pct=0.02, *args, **kwargs):
        self.__init_original__(*args, **kwargs)
        # Handle singleton pattern safely
        if getattr(self, "_initialized_phase4", False):
            return
'''
text = re.sub(r'def __init__\(self, ib_connection=None, max_drawdown_pct=0\.02, \*args, \*\*kwargs\):\n        pass\n        # Handle singleton pattern safely\n        if getattr\(self, "_initialized_phase4", False\):\n            return', replacement, text)

# Correcting the pass we added previously
with open('risk/manager.py', 'w', encoding='utf-8') as f:
    f.write(text.replace('log.info(f"??? ', 'log.info(f"??? ').replace('\$ ', '$ '))
