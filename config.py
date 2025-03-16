import os
import sys

class Config:
    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
    CIELO_API_KEY = os.environ.get("CIELO_API_KEY", "")
    HELIUS_API_KEY = os.environ.get("HELIUS_API_KEY", "")
    DATABASE_PATH = os.environ.get("DATABASE_PATH", "tradingbot.db")
    
    MIN_TRANSACTION_USD = 200
    MIN_TRADERS_FOR_SIGNAL = 2
    SIGNAL_WINDOW_SECONDS = 540
    MIN_CONFIDENCE_THRESHOLD = 0.3
    HIGH_VOLUME_THRESHOLD = 5000
    MAX_SCORE = 10.0
    
    HELIUS_CACHE_DURATION = 300

    @classmethod
    def check_required_config(cls):
        required_vars = ["TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "CIELO_API_KEY", "HELIUS_API_KEY"]
        missing = [var for var in required_vars if not getattr(cls, var)]
        if missing:
            print(f"🚨 ERROR: Missing environment variables: {', '.join(missing)}")
            sys.exit(1)
        print("✅ Required configuration verified")
