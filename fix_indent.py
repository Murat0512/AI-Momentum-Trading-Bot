with open('execution/orders.py', 'r') as f:
    text = f.read()

text = text.replace('def _passes_spread_defense(self', '    def _passes_spread_defense(self')
text = text.replace('Bid: \\, Ask: \\', 'Bid: {bid:.2f}, Ask: {ask:.2f}')
text = text.replace('Spread: \\ ', 'Spread:  ')

with open('execution/orders.py', 'w') as f:
    f.write(text)
