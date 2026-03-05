import re
import random

with open('infrastructure/ib_connection.py', 'r', encoding='utf-8') as f:
    text = f.read()

# Expand Trading Window
text = text.replace('client_id=1', f'client_id={random.randint(2, 9999)}')

with open('infrastructure/ib_connection.py', 'w', encoding='utf-8') as f:
    f.write(text)
