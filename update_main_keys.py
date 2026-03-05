import re

with open('main.py', 'r', encoding='utf-8') as f:
    text = f.read()

# Specifically replace the if logic raising the error for missing credentials
pattern = r"if authority == \'ibkr\':\s+cfg_key =.*?raise ValueError\([^)]+\)"
text = re.sub(pattern, "if False: pass", text, flags=re.DOTALL)

with open('main.py', 'w', encoding='utf-8') as f:
    f.write(text)
