import os
import sys
import json

class Config:
    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
    CIELO_API_KEY = os.environ.get("CIELO_API_KEY", "bb4dbdac-9ac7-4c42-97d3-f6435d0674da")
    DATABASE_PATH = os.environ.get("DATABASE_PATH", "/data/tradingbot.db" if os.path.exists("/data") else "tradingbot.db")

    RUGCHECK_PRIVATE_KEY = os.environ.get("RUGCHECK_PRIVATE_KEY", "")
    RUGCHECK_WALLET_PUBKEY = os.environ.get("RUGCHECK_WALLET_PUBKEY", "")

    MIN_TRANSACTION_USD = 200
    MIN_TRADERS_FOR_SIGNAL = 2
    SIGNAL_WINDOW_SECONDS = 540
    MIN_CONFIDENCE_THRESHOLD = 0.3
    MIN_VOLUME_USD = 2000

    DEFAULT_SCORE = 5.0
    MAX_SCORE = 10.0
    MIN_SCORE = 0.0
    BUY_SCORE_INCREASE = 0.1
    SELL_SCORE_INCREASE = 0.2

    MIN_MARKETCAP = 100000
    MAX_MARKETCAP = 500_000_000
    VOL_NORMALIZATION_FACTOR = 10000.0

    # Configuración para memecoins
    MEMECOIN_CONFIG = {
        "MIN_VOLUME_USD": 1000,
        "MIN_CONFIDENCE": 0.4,
        "VOLUME_GROWTH_THRESHOLD": 0.3,
        "TX_RATE_THRESHOLD": 10
    }

    SIGNAL_THROTTLING = 10

    # Configuraciones para GMGN (sin API key)
    GMGN_BASE_URL = "https://api.gmgn.ai/public/v1/"

    _dynamic_config = {}

    @classmethod
    def load_dynamic_config(cls, db_connection=None):
        # Implementación para cargar configuración dinámica desde BD
        pass

    @classmethod
    def get(cls, key, default=None):
        if key in cls._dynamic_config:
            return cls._dynamic_config[key]
        if hasattr(cls, key.upper()):
            return getattr(cls, key.upper())
        return default

    @classmethod
    def check_required_config(cls):
        required_vars = ["DATABASE_PATH", "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "CIELO_API_KEY"]
        missing = [var for var in required_vars if not getattr(cls, var)]
        if missing:
            print(f"🚨 ERROR: Faltan variables de entorno: {', '.join(missing)}")
            sys.exit(1)
        print("✅ Configuración requerida verificada")
