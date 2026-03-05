import re

with open('data/fetcher.py', 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace('def fetch_all(self, tickers: list, days: int = 2) -> dict:', 'def fetch_all(self, tickers: list, days: int = 2, **kwargs) -> dict:')

with open('data/fetcher.py', 'w', encoding='utf-8') as f:
    f.write(text)
