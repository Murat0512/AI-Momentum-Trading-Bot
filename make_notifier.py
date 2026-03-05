# SECURITY WARNING: Never hardcode Discord webhook URLs or credentials in this file.
# Always use a .env file (see .env.example) and ensure .env is in .gitignore before pushing to GitHub.
import os

code = '''import logging
import requests
from datetime import datetime
import pytz

class DiscordNotifier:
    def __init__(self, webhook_url: str = None):
        """
        Initializes the Discord Webhook dispatch system.
        """
        self.logger = logging.getLogger(__name__)
        if webhook_url is None:
            webhook_url = os.environ.get("DISCORD_WEBHOOK_URL", "")
        self.webhook_url = webhook_url
        self.eastern = pytz.timezone('US/Eastern')

    def send_eod_report(self, starting_balance: float, ending_balance: float):
        """
        Calculates the day's P&L and dispatches a rich embedded report to Discord.
        """
        if not self.webhook_url:
            self.logger.warning("?? Webhook URL not provided. EOD Report printed to terminal only.")
            return

        if starting_balance <= 0:
            self.logger.error("? Cannot generate EOD report: Invalid starting balance.")
            return

        # 1. Calculate the exact math
        pnl_dollars = ending_balance - starting_balance
        pnl_pct = (pnl_dollars / starting_balance) * 100
        
        # 2. Determine Sentiment and Embed Color
        # Discord uses decimal color codes: Green (5763719), Red (15548997), Gray (9807270)
        if pnl_dollars > 0:
            day_type = "?? PROFITABLE DAY"
            embed_color = 5763719 
        elif pnl_dollars < 0:
            day_type = "?? DRAWDOWN DAY"
            embed_color = 15548997
        else:
            day_type = "? FLAT DAY"
            embed_color = 9807270

        date_str = datetime.now(self.eastern).strftime("%Y-%m-%d")

        # 3. Format the Discord Rich Embed JSON Payload
        payload = {
            "username": "Momentum Sniper Engine",
            "avatar_url": "https://cdn-icons-png.flaticon.com/512/2622/2622081.png", # Optional bot icon
            "embeds": [
                {
                    "title": f"?? End of Day Report: {date_str}",
                    "description": f"**Status:** {day_type}",
                    "color": embed_color,
                    "fields": [
                        {
                            "name": "Starting Balance",
                            "value": f"\",
                            "inline": True
                        },
                        {
                            "name": "Ending Balance",
                            "value": f"\",
                            "inline": True
                        },
                        {
                            "name": "Net P&L ($)",
                            "value": f"**\**",
                            "inline": False
                        },
                        {
                            "name": "Net P&L (%)",
                            "value": f"**{pnl_pct:,.2f}%**",
                            "inline": False
                        }
                    ],
                    "footer": {
                        "text": "Interactive Brokers | Phase 5 Architecture"
                    },
                    "timestamp": datetime.utcnow().isoformat()
                }
            ]
        }

        # 4. Dispatch the payload
        try:
            self.logger.info("?? Dispatching Webhook EOD Report to Discord...")
            response = requests.post(self.webhook_url, json=payload)
            
            if response.status_code == 204:
                self.logger.info("? Discord EOD Report successfully delivered!")
            else:
                self.logger.error(f"? Failed to deliver Discord report. Status Code: {response.status_code}")
                
        except Exception as e:
            self.logger.error(f"? Critical failure dispatching webhook: {e}")
'''

# Create __init__.py so reporting acts as a module
with open("reporting/__init__.py", "w", encoding="utf-8") as f:
    f.write("")

with open("reporting/notifier.py", "w", encoding="utf-8") as f:
    f.write(code)

print("reporting/notifier.py created successfully with the provided webhook URL.")
