import re

with open('infrastructure/ib_connection.py', 'r', encoding='utf-8') as f:
    text = f.read()

# Make connect safe against double connections
safe_connect = '''def connect(self):
        if self.is_connected:
            return
'''
text = re.sub(r'def connect\(self\):\n', safe_connect, text)

with open('infrastructure/ib_connection.py', 'w', encoding='utf-8') as f:
    f.write(text)
