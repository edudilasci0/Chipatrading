# config.py
import os
import sys
import logging

class Config:
    # Configuraciones de API y credenciales
    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
    CIELO_API_KEY = os.environ.get("CIELO_API_KEY", "")
    HELIUS_API_KEY = os.environ.get("HELIUS_API_KEY", "")
    DATABASE_PATH = os.environ.get("DATABASE_PATH", "tradingbot.db")
    
    # Parámetros para validación de transacciones y señales
    MIN_TRANSACTION_USD = float(os.environ.get("MIN_TRANSACTION_USD", "200"))
    MIN_TRADERS_FOR_SIGNAL = int(os.environ.get("MIN_TRADERS_FOR_SIGNAL", "2"))
    SIGNAL_WINDOW_SECONDS = int(os.environ.get("SIGNAL_WINDOW_SECONDS", "540"))
    MIN_CONFIDENCE_THRESHOLD = float(os.environ.get("MIN_CONFIDENCE_THRESHOLD", "0.3"))
    HIGH_VOLUME_THRESHOLD = float(os.environ.get("HIGH_VOLUME_THRESHOLD", "5000"))
    MIN_VOLUME_USD = float(os.environ.get("MIN_VOLUME_USD", "2000"))
    MAX_SCORE = 10.0
    DEFAULT_SCORE = 5.0
    
    # Nuevos parámetros para análisis avanzado
    WHALE_TRANSACTION_THRESHOLD = float(os.environ.get("WHALE_TRANSACTION_THRESHOLD", "10000"))
    LIQUIDITY_HEALTHY_THRESHOLD = float(os.environ.get("LIQUIDITY_HEALTHY_THRESHOLD", "20000"))
    SLIPPAGE_WARNING_THRESHOLD = float(os.environ.get("SLIPPAGE_WARNING_THRESHOLD", "10"))
    HOLDER_GROWTH_SIGNIFICANT = float(os.environ.get("HOLDER_GROWTH_SIGNIFICANT", "5"))
    
    # Coeficientes para cálculo de confianza
    TRADER_QUALITY_WEIGHT = float(os.environ.get("TRADER_QUALITY_WEIGHT", "0.35"))
    WHALE_ACTIVITY_WEIGHT = float(os.environ.get("WHALE_ACTIVITY_WEIGHT", "0.20"))
    HOLDER_GROWTH_WEIGHT = float(os.environ.get("HOLDER_GROWTH_WEIGHT", "0.15"))
    LIQUIDITY_HEALTH_WEIGHT = float(os.environ.get("LIQUIDITY_HEALTH_WEIGHT", "0.15"))
    TECHNICAL_FACTORS_WEIGHT = float(os.environ.get("TECHNICAL_FACTORS_WEIGHT", "0.15"))
    
    # Parámetros para DEX y APIs
    DEX_CACHE_TTL = int(os.environ.get("DEX_CACHE_TTL", "60"))  # 60 segundos
    MARKET_METRICS_CACHE_TTL = int(os.environ.get("MARKET_METRICS_CACHE_TTL", "300"))  # 5 minutos
    HOLDER_GROWTH_TTL = int(os.environ.get("HOLDER_GROWTH_TTL", "3600"))  # 1 hora
    TRENDING_DATA_TTL = int(os.environ.get("TRENDING_DATA_TTL", "1800"))  # 30 minutos
    TOKEN_ANALYSIS_CACHE_TTL = int(os.environ.get("TOKEN_ANALYSIS_CACHE_TTL", "300"))  # 5 minutos
    
    # Configuración de monitoreo
    PERFORMANCE_UPDATE_INTERVAL = int(os.environ.get("PERFORMANCE_UPDATE_INTERVAL", "180"))  # 3 minutos
    EARLY_MONITORING_INTERVALS = [3, 8, 15, 25]  # Minutos para monitoreo intensivo
    
    # Configuración para throttling y limitaciones
    HELIUS_CACHE_DURATION = int(os.environ.get("HELIUS_CACHE_DURATION", "300"))
    SIGNAL_THROTTLING = int(os.environ.get("SIGNAL_THROTTLING", "10"))
    MAX_API_RETRIES = int(os.environ.get("MAX_API_RETRIES", "3"))
    API_RETRY_DELAY = int(os.environ.get("API_RETRY_DELAY", "2"))
    
    # Configuración de señales
    SIGNAL_LEVELS = {
        "S": 0.9,  # Confianza >= 0.9
        "A": 0.8,  # Confianza >= 0.8
        "B": 0.6,  # Confianza >= 0.6
        "C": 0.3   # Confianza >= 0.3
    }
    
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
        
        # Configurar logging
        cls.setup_logging()
        
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
    
    @classmethod
    def setup_logging(cls):
        """Configura el sistema de logging para todo el proyecto"""
        log_level = os.environ.get("LOG_LEVEL", "INFO")
        log_levels = {
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "ERROR": logging.ERROR,
            "CRITICAL": logging.CRITICAL
        }
        
        level = log_levels.get(log_level.upper(), logging.INFO)
        
        # Configuración básica para todos los loggers
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler()
            ]
        )
        
        # Configurar loggers específicos para módulos
        loggers = [
            "signal_logic", "performance_tracker", "whale_detector",
            "market_metrics", "token_analyzer", "trader_profiler",
            "dex_monitor", "telegram_utils", "database"
        ]
        
        for logger_name in loggers:
            logger = logging.getLogger(logger_name)
            logger.setLevel(level)
            
            # Evitar propagar eventos a los handlers del padre
            # si ya tiene handlers propios
            if logger.handlers:
                logger.propagate = False
        
        # Configurar bibliotecas externas para que no sean tan verbosas
        logging.getLogger("aiohttp").setLevel(logging.WARNING)
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("websockets").setLevel(logging.WARNING)
    
    @classmethod
    def update_setting(cls, key, value):
        """
        Actualiza un valor de configuración en tiempo de ejecución
        y lo guarda en la base de datos si está disponible.
        """
        cls._dynamic_config[key] = value
        
        try:
            import db
            db.update_setting(key, str(value))
            print(f"Updated setting {key} = {value} (saved to DB)")
        except Exception as e:
            print(f"Setting {key} = {value} updated in memory only: {e}")
    
    @classmethod
    def get_signal_level(cls, confidence):
        """
        Determina el nivel de señal (S, A, B, C) basado en la confianza.
        
        Args:
            confidence (float): Valor de confianza entre 0 y 1
            
        Returns:
            str: Nivel de señal (S, A, B, C)
        """
        if confidence >= cls.SIGNAL_LEVELS["S"]:
            return "S"
        elif confidence >= cls.SIGNAL_LEVELS["A"]:
            return "A"
        elif confidence >= cls.SIGNAL_LEVELS["B"]:
            return "B"
        elif confidence >= cls.SIGNAL_LEVELS["C"]:
            return "C"
        return "D"  # Por debajo del umbral mínimo

# Inicializar Config al importar el módulo
Config.initialize()
