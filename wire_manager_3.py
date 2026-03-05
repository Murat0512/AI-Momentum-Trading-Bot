import re

with open('infrastructure/ib_connection.py', 'r', encoding='utf-8') as f:
    text = f.read()

# Make IBConnectionManager a singleton
singleton_injection = '''class IBConnectionManager:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(IBConnectionManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, host="127.0.0.1", port=7497, client_id=1):
        if getattr(self, "_initialized", False):
            return
        self._initialized = True
'''
text = re.sub(r'class IBConnectionManager:\s+"""[^"]+"""\s+def __init__\(self, host="127\.0\.0\.1", port=7497, client_id=1\):', singleton_injection, text, flags=re.MULTILINE)

with open('infrastructure/ib_connection.py', 'w', encoding='utf-8') as f:
    f.write(text)
