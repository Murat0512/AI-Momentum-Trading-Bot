with open('execution/orders.py', 'r') as f:
    text = f.read()

text = text.replace('LIMIT BUY for {qty} {ticker} @ \\ (Ask: \\)', 'LIMIT BUY for {qty} {ticker} @  (Ask: )')
text = text.replace('LIMIT SELL for {qty} {ticker} @ \\ (Bid: \\)', 'LIMIT SELL for {qty} {ticker} @  (Bid: )')
text = text.replace('Invalid Bid Price (\\)', 'Invalid Bid Price ()')

with open('execution/orders.py', 'w') as f:
    f.write(text)
