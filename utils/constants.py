import os
from typing import Dict

from utils.style import PrettyText

INTERVAL_TO_MS: Dict[str, int] = {
    "1m": 60_000,
    "3m": 3 * 60_000,
    "5m": 5 * 60_000,
    "15m": 15 * 60_000,
    "30m": 30 * 60_000,
    "1h": 60 * 60_000,
    "2h": 2 * 60 * 60_000,
    "4h": 4 * 60 * 60_000,
    "8h": 8 * 60 * 60_000,
    "12h": 12 * 60 * 60_000,
    "1d": 24 * 60 * 60_000,
    "3d": 3 * 24 * 60 * 60_000,
    "1w": 7 * 24 * 60_000 * 60,
    "1M": 30 * 24 * 60_000 * 60,
}

PRICE_EPS = 1e-4
VALID_LIMIT_TIFS = {"Alo", "Gtc", "Ioc"}

LOG_DIR = "logs"
DEFAULT_LOG_FILE = os.path.join(LOG_DIR, "hypertrader.log")
AUTO_TRADES_LOG_FILE = os.path.join(LOG_DIR, "auto_trades.log")
WATCH_RETRY_SLEEP_SECONDS = 3.0

cp = PrettyText()
