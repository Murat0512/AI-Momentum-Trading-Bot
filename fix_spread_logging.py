import re

with open('execution/orders.py', 'r', encoding='utf-8') as f:
    text = f.read()

# Fix the specific lines messed up by PowerShell escaping
text = re.sub(
    r'self.logger.warning\(f"\?\?\? SPREAD DEFENSE: \{ticker\} rejected. Invalid Quote \(Bid: \$ Ask: \$\)."\)',
    'self.logger.warning(f"??? SPREAD DEFENSE: {ticker} rejected. Invalid Quote (Bid: \, Ask: \).")',
    text
)

text = re.sub(
    r'self.logger.warning\(f"\?\?\? SPREAD DEFENSE: \{ticker\} rejected. Spread too wide',
    'self.logger.warning(f"??? SPREAD DEFENSE: {ticker} rejected. Spread too wide',
    text
)

text = re.sub(
    r'self.logger.info\(f"\? SPREAD DEFENSE: \{ticker\} passed. Spread:\n\$\(\{\(spread_pct\*100\):.2f\}\%\)."\)',
    'self.logger.info(f"? SPREAD DEFENSE: {ticker} passed. Spread: \ ({(spread_pct*100):.2f}%).")',
    text
)

text = re.sub(
    r'self.logger.error\(f"\? TRADE ABORTED: \{ticker\} failed spread\ndefense. Too illiquid."\)',
    'self.logger.error(f"? TRADE ABORTED: {ticker} failed spread defense. Too illiquid.")',
    text
)

# And fix the Extended Hours ones
text = re.sub(
    r'self.logger.info\(f"\?\? EXTENDED HOURS: Routing LIMIT BUY for \{qty\} \{ticker\} @ \$\(Ask: \$\)"\)',
    'self.logger.info(f"?? EXTENDED HOURS: Routing LIMIT BUY for {qty} {ticker} @ \ (Ask: \)")',
    text
)

text = re.sub(
    r'self.logger.info\(f"\?\? EXTENDED HOURS: Routing LIMIT SELL for \{qty\} \{ticker\} @ \$\(Bid: \$\)"\)',
    'self.logger.info(f"?? EXTENDED HOURS: Routing LIMIT SELL for {qty} {ticker} @ \ (Bid: \)")',
    text
)

with open('execution/orders.py', 'w', encoding='utf-8') as f:
    f.write(text)
