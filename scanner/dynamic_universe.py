import logging
from ib_insync import ScannerSubscription
from infrastructure.ib_connection import IBConnectionManager

class DynamicUniverse:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        # Connect to our established IBKR local socket
        self.conn_mgr = IBConnectionManager()
        if not self.conn_mgr.is_connected:
            self.conn_mgr.connect()
        self.ib = self.conn_mgr.ib

    def get_top_gainers(self, min_price=2.0, max_price=50.0, min_volume=500000, max_results=30) -> list:
        """
        Webull-Style Premarket Scanner.
        Pings IBKR for the top percentage gainers matching retail momentum criteria.
        """
        self.logger.info("📡 Pinging IBKR Scanner for Top % Gainers...")

        # Define the exact parameters of the scan
        sub = ScannerSubscription(
            instrument='STK',
            locationCode='STK.US.MAJOR',
            scanCode='TOP_PERC_GAIN',
            abovePrice=min_price,
            belowPrice=max_price,
            aboveVolume=min_volume
        )

        try:
            # Request the data from TWS
            scan_data = self.ib.reqScannerData(
                sub, 
                scannerSubscriptionOptions=[], 
                scannerSubscriptionFilterOptions=[]
            )
            
            tickers = []
            for item in scan_data:
                symbol = item.contractDetails.contract.symbol
                # Prevent duplicates and respect the max_results limit
                if symbol not in tickers:
                    tickers.append(symbol)
                    if len(tickers) >= max_results:
                        break
                        
            if not tickers:
                self.logger.warning("⚠️ Scanner returned 0 results. Check TWS API permissions or time of day.")
                return self._fallback_list()

            self.logger.info(f"✅ Dynamic Universe built. {len(tickers)} targets found: {tickers}")
            return tickers
        
        except Exception as e:
            self.logger.error(f"? Scanner API failed: {e}. Falling back to default list.")
            return self._fallback_list()

    def _fallback_list(self) -> list:
        """
        Safety net: If the API fails, trade highly liquid mega-caps so the bot doesn't crash.
        """
        return ["SPY", "QQQ", "AAPL", "NVDA", "TSLA", "AMD"]
