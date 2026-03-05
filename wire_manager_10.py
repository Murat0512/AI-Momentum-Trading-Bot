import re

with open('execution/engine.py', 'r', encoding='utf-8') as f:
    text = f.read()

# Delete the Alpaca Warning
text = re.sub(r'if getattr\(CONFIG\.data, "alpaca_feed",.*?\) == "iex":[\s\S]*?log\.warning\(\n\s*"CAUTION: Trading on IEX Feed - Volume checks may be inaccurate\."\n\s*\)', '', text)

with open('execution/engine.py', 'w', encoding='utf-8') as f:
    f.write(text)

with open('config/settings.py', 'r', encoding='utf-8') as f:
    text = f.read()

# Expand Trading Window
text = text.replace('session_open: str = "09:30"', 'session_open: str = "04:00"')
text = text.replace('session_close: str = "16:00"', 'session_close: str = "20:00"')

with open('config/settings.py', 'w', encoding='utf-8') as f:
    f.write(text)
