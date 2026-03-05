import asyncio

try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())
import asyncio
import logging
from ib_insync import IB, util

logger = logging.getLogger("IBConnection")


class IBConnectionManager:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(IBConnectionManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, host="127.0.0.1", port=7497, client_id=706):
        if getattr(self, "_initialized", False):
            return
        self._initialized = True

        self.host = host
        self.port = port
        self.client_id = client_id

        self.ib = IB()

        # Bind the disconnected event for our reconnect loop
        self.ib.disconnectedEvent += self._on_disconnected

    @property
    def is_connected(self):
        return self.ib.isConnected()

    def connect(self):
        if self.is_connected:
            return
        logger.info(
            f"Connecting to IBKR at {self.host}:{self.port} with clientId={self.client_id}..."
        )
        try:
            self.ib.connect(self.host, self.port, clientId=self.client_id)
            logger.info("Successfully connected to IBKR.")
        except Exception as e:
            logger.error(f"Failed to connect on initial attempt: {e}")
            self._schedule_reconnect()

    def disconnect(self):
        """Disconnect gracefully."""
        self.ib.disconnect()

    def _on_disconnected(self, *args):
        logger.warning("Disconnected from IBKR. The reconnect loop is taking over...")
        self._schedule_reconnect()

    def _schedule_reconnect(self):
        if hasattr(util, "isAsyncio") and util.isAsyncio():
            loop = asyncio.get_event_loop()
            loop.create_task(self._reconnect_loop())
        else:
            # Fallback for sync contexts
            while not self.ib.isConnected():
                util.sleep(5)
                try:
                    logger.info("Attempting synchronous reconnect...")
                    self.ib.connect(self.host, self.port, clientId=self.client_id)
                except Exception:
                    pass

    async def _reconnect_loop(self):
        """Asynchronous reconnect loop designed to backoff and retry until TWS is back."""
        while not self.ib.isConnected():
            logger.info("Attempting to reconnect to IBKR in 5 seconds...")
            await asyncio.sleep(5)
            try:
                self.ib.connect(self.host, self.port, clientId=self.client_id)
                if self.ib.isConnected():
                    logger.info("Successfully RESTORED connection to IBKR!")
                    break
            except Exception as e:
                logger.debug(f"Reconnect attempt failed: {e}")
