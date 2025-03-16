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
    MIN_VOLUME_USD = 2000
    MAX_SCORE = 10.0
    DEFAULT_SCORE = 5.0
    
    HELIUS_CACHE_DURATION = 300
    SIGNAL_THROTTLING = 10

    # Configuración dinámica cargada desde la base de datos
    _dynamic_config = {}
    _initialized = False

    @classmethod
    def initialize(cls):
        """Inicializa la configuración. Debe llamarse antes de utilizar Config.get()"""
        if cls._initialized:
            return
        try:
            # Intentamos cargar la configuración desde la base de datos,
            # pero no falla si no podemos
            cls.load_dynamic_config()
        except Exception as e:
            print(f"Advertencia: No se pudo cargar la configuración dinámica: {e}")
        cls._initialized = True

    @classmethod
    def load_dynamic_config(cls, db_connection=None):
        try:
            import db
            settings = db.execute_cached_query("SELECT key, value FROM bot_settings")
            for setting in settings:
                key = setting['key']
                value = setting['value']
                cls._dynamic_config[key] = value
            print(f"Dynamic config loaded: {len(cls._dynamic_config)} parameters")
        except Exception as e:
            print(f"Error loading dynamic config: {e}")

    @classmethod
    def get(cls, key, default=None):
        """
        Obtiene un valor de configuración, con el siguiente orden de prioridad:
        1. Configuración dinámica (de DB)
        2. Atributos de clase Config
        3. Valor por defecto proporcionado
        """
        if not cls._initialized:
            cls.initialize()
            
        if key in cls._dynamic_config:
            return cls._dynamic_config[key]
        if hasattr(cls, key.upper()):
            return getattr(cls, key.upper())
        return default

    @classmethod
    def check_required_config(cls):
        required_vars = ["TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "CIELO_API_KEY", "HELIUS_API_KEY"]
        missing = [var for var in required_vars if not getattr(cls, var)]
        if missing:
            print(f"🚨 ERROR: Missing environment variables: {', '.join(missing)}")
            sys.exit(1)
        print("✅ Required configuration verified")

# Inicializar Config al importar el módulo
Config.initialize()
