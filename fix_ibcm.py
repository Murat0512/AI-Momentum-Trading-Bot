import re

with open('infrastructure/ib_connection.py', 'r') as f:
    text = f.read()

# Fix the module error for Python 3.14 + ib_insync compatibility we saw in logging
text = text.replace('if util.isAsyncio():', 'if hasattr(util, "isAsyncio") and util.isAsyncio():')

with open('infrastructure/ib_connection.py', 'w') as f:
    f.write(text)
