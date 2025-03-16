import os
import sys

class Config:
    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
    CIELO_API_KEY = os.environ.get("CIELO_API_KEY", "bb4dbdac-9ac7-4c42-97d3-f6435d0674da")
    HELIUS_API_KEY = os.environ.get("HELIUS_API_KEY", "4314df2b-76aa-406a-8635-b9b0e6a0a51e")
    DATABASE_PATH = os.environ.get("DATABASE_PATH", "tradingbot.db")
    
    MIN_TRANSACTION_USD = 200
    MIN_TRADERS_FOR_SIGNAL = 2
    SIGNAL_WINDOW_SECONDS = 540
    MIN_CONFIDENCE_THRESHOLD = 0.3
    MIN_VOLUME_USD = 2000
    MIN_MARKETCAP = 100000

    DEFAULT_SCORE = 5.0
    MAX_SCORE = 10.0

    HELIUS_CACHE_DURATION = 300

    @classmethod
    def check_required_config(cls):
        required_vars = ["TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "CIELO_API_KEY", "HELIUS_API_KEY"]
        missing = [var for var in required_vars if not getattr(cls, var)]
        if missing:
            print(f"🚨 ERROR: Faltan variables de entorno: {', '.join(missing)}")
            sys.exit(1)
        print("✅ Configuración requerida verificada")
