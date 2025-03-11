import os
import sys
import json

class Config:
    """
    Clase centralizada para manejar la configuración del bot.
    Carga los valores desde variables de entorno, base de datos o valores por defecto.
    """
    # API Keys y URLs
    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
    CIELO_API_KEY = os.environ.get("CIELO_API_KEY", "")
    RUGCHECK_PRIVATE_KEY = os.environ.get("RUGCHECK_PRIVATE_KEY", "")
    RUGCHECK_WALLET_PUBKEY = os.environ.get("RUGCHECK_WALLET_PUBKEY", "")
    HELIUS_API_KEY = os.environ.get("HELIUS_API_KEY", "")  # Nueva variable

    # Configuración de la base de datos
    DATABASE_PATH = os.environ.get("DATABASE_PATH", "/data/tradingbot.db" if os.path.exists("/data") else "tradingbot.db")

    # Configuración de señales (valores por defecto)
    MIN_TRANSACTION_USD = 200
    MIN_TRADERS_FOR_SIGNAL = 2
    SIGNAL_WINDOW_SECONDS = 540  # 9 minutos
    MIN_CONFIDENCE_THRESHOLD = 0.3
    MIN_VOLUME_USD = 2000

    # Configuración de scoring
    DEFAULT_SCORE = 5.0
    MAX_SCORE = 10.0
    MIN_SCORE = 0.0
    BUY_SCORE_INCREASE = 0.1
    SELL_SCORE_INCREASE = 0.2

    # Configuración para filtros de volumen/market cap
    MIN_MARKETCAP = 100000
    MAX_MARKETCAP = 500_000_000
    VOL_NORMALIZATION_FACTOR = 10000.0

    # Nuevas configuraciones para señales
    SIGNAL_THROTTLING = 10  # Máximo de señales por hora
    ADAPT_CONFIDENCE_THRESHOLD = True
    HIGH_QUALITY_TRADER_SCORE = 7.0

    # Configuración para memecoins (nuevos parámetros)
    MEMECOIN_CONFIG = {
        "MIN_VOLUME_USD": 1000,
        "MIN_CONFIDENCE": 0.4,
        "VOLUME_GROWTH_THRESHOLD": 0.3,
        "TX_RATE_THRESHOLD": 10
    }

    # Nuevo flag para Helius API requerida (reemplaza a DexScreener)
    HELIUS_API_REQUIRED = True

    # **Desactivar filtrado por RugCheck**
    ENABLE_RUGCHECK_FILTERING = False

    # Lista de tokens especiales a ignorar
    IGNORE_TOKENS = [
        "native",
        "So11111111111111111111111111111111111111112",
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
    ]

    # Tokens conocidos (para evitar errores y añadir información)
    KNOWN_TOKENS = {
        "So11111111111111111111111111111111111111112": {
            "name": "SOL",
            "price": 0,
            "market_cap": 15000000000,
            "vol_1h": 1000000
        },
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": {
            "name": "USDC",
            "price": 1,
            "market_cap": 35000000000,
            "vol_1h": 2000000
        },
        "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": {
            "name": "USDT",
            "price": 1,
            "market_cap": 40000000000,
            "vol_1h": 3000000
        }
    }

    _dynamic_config = {}
    
    @classmethod
    def load_dynamic_config(cls, db_connection=None):
        if not db_connection:
            return
        try:
            from db import get_all_settings
            settings = get_all_settings(db_connection)
            for key, value in settings.items():
                cls._dynamic_config[key] = value
                if hasattr(cls, key.upper()):
                    attr_value = getattr(cls, key.upper())
                    if isinstance(attr_value, int):
                        setattr(cls, key.upper(), int(value))
                    elif isinstance(attr_value, float):
                        setattr(cls, key.upper(), float(value))
                    else:
                        setattr(cls, key.upper(), value)
        except Exception as e:
            print(f"⚠️ Error cargando configuración dinámica: {e}")
    
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
            print(f"🚨 ERROR: Faltan variables de entorno requeridas: {', '.join(missing)}")
            sys.exit(1)
        print("✅ Configuración requerida verificada correctamente")
        
    @classmethod
    def update_setting(cls, key, value):
        cls._dynamic_config[key] = value
        if hasattr(cls, key.upper()):
            attr_value = getattr(cls, key.upper())
            if isinstance(attr_value, int):
                setattr(cls, key.upper(), int(value))
            elif isinstance(attr_value, float):
                setattr(cls, key.upper(), float(value))
            else:
                setattr(cls, key.upper(), value)
