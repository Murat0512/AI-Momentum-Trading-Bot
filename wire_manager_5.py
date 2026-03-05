import re

with open('risk/manager.py', 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace('super().__init__(*args, **kwargs)', 'pass')

with open('risk/manager.py', 'w', encoding='utf-8') as f:
    f.write(text)
