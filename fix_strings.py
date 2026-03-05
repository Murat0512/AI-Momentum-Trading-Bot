import re

with open('execution/orders.py', 'r', encoding='utf-8') as f:
    text = f.read()

# I will just manually inject the whole _passes_spread_defense function text to be absolutely sure.
def_replace = '''
    def _passes_spread_defense(self, ticker: str, quote: dict, max_spread_pct: float = 0.01) -> bool:
        """
        PHASE 3: The Spread & Commission Defense.
        Aborts trades if the invisible fee of the Bid/Ask spread is too high.
        """
        bid = quote['bid']
        ask = quote['ask']

        if bid <= 0 or ask <= 0:
            self.logger.warning(f"??? SPREAD DEFENSE: {ticker} rejected. Invalid Quote (Bid: \, Ask: \).")
            return False

        spread = ask - bid
        spread_pct = spread / ask

        # Max acceptable spread is set to 1.0% (0.01)
        if spread_pct > max_spread_pct:
            self.logger.warning(f"??? SPREAD DEFENSE: {ticker} rejected. Spread too wide ({(spread_pct*100):.2f}% > {(max_spread_pct*100):.2f}%).")
            return False

        self.logger.info(f"? SPREAD DEFENSE: {ticker} passed. Spread: \ ({(spread_pct*100):.2f}%).")
        return True
'''

pattern = r"    def _passes_spread_defense\(self, ticker: str, quote: dict, max_spread_pct: float = 0\.01\) -> bool:.*?return True"
text = re.sub(pattern, def_replace.strip(), text, flags=re.DOTALL)

with open('execution/orders.py', 'w', encoding='utf-8') as f:
    f.write(text)
