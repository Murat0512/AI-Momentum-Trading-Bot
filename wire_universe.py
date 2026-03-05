import re

with open('main.py', 'r', encoding='utf-8') as f:
    text = f.read()

injection = '''
    # -- Dynamic Webull-Style IBKR Scanner --
    from scanner.dynamic_universe import DynamicUniverse
    try:
        dyn_univ = DynamicUniverse()
        CONFIG.scanner.seed_tickers = dyn_univ.get_top_gainers()
    except Exception as e:
        log.error(f"Failed to initialize DynamicUniverse: {e}")

    engine = TradingEngine()
'''

text = re.sub(r'    engine = TradingEngine\(\)', injection, text)

with open('main.py', 'w', encoding='utf-8') as f:
    f.write(text)
