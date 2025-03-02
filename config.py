import os
import sys
import json

class Config:
    """
    Clase centralizada para manejar la configuración del bot.
    Carga los valores desde variables de entorno, base de datos o
    valores por defecto.
    """
    # API Keys y URLs
    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
    CIELO_API_KEY = os.environ.get("CIELO_API_KEY", "bb4dbdac-9ac7-4c42-97d3-f6435d0674da")
    DATABASE_PATH = os.environ.get("DATABASE_PATH", "/data/tradingbot.db" 
                                   if os.path.exists("/data") else "tradingbot.db")

    # Configuración de Rugcheck
    RUGCHECK_PRIVATE_KEY = os.environ.get("RUGCHECK_PRIVATE_KEY", "")
    RUGCHECK_WALLET_PUBKEY = os.environ.get("RUGCHECK_WALLET_PUBKEY", "")

    # Configuración de las señales (valores por defecto)
    MIN_TRANSACTION_USD = 300
    MIN_TRADERS_FOR_SIGNAL = 3
    SIGNAL_WINDOW_SECONDS = 540  # 9 minutos como solicitaste
    MIN_CONFIDENCE_THRESHOLD = 0.4
    MIN_VOLUME_USD = 5000

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
    
    # Nuevas configuraciones
    SIGNAL_THROTTLING = 10  # Máximo de señales por hora
    ADAPT_CONFIDENCE_THRESHOLD = True  # Ajustar umbrales según rendimiento
    HIGH_QUALITY_TRADER_SCORE = 7.0  # Umbral para traders de alta calidad
    
    # Valores dinámicos desde base de datos
    _dynamic_config = {}
    
    @classmethod
    def load_dynamic_config(cls, db_connection=None):
        """
        Carga configuración dinámica desde la base de datos
        """
        # Si no tenemos conexión, usamos los valores por defecto
        if not db_connection:
            return
            
        try:
            # Importar aquí para evitar dependencia circular
            from db import get_all_settings
            
            # Obtener todas las configuraciones
            settings = get_all_settings(db_connection)
            
            # Actualizar valores dinámicos
            for key, value in settings.items():
                cls._dynamic_config[key] = value
                
                # Actualizar atributos de clase para acceso más fácil
                if hasattr(cls, key.upper()):
                    # Convertir al tipo correcto
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
        """
        Obtiene un valor de configuración, priorizando:
        1. Valores dinámicos cargados desde BD
        2. Atributos de clase (variables de entorno o valores por defecto)
        3. Valor por defecto proporcionado
        """
        # Primero buscar en config dinámica
        if key in cls._dynamic_config:
            return cls._dynamic_config[key]
            
        # Luego buscar como atributo de clase
        if hasattr(cls, key.upper()):
            return getattr(cls, key.upper())
            
        # Finalmente retornar valor por defecto
        return default
        
    @classmethod
    def check_required_config(cls):
        """
        Verifica que todas las configuraciones requeridas estén presentes.
        Sale del programa si faltan configuraciones críticas.
        """
        required_vars = ["DATABASE_PATH", "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "CIELO_API_KEY"]
        missing = [var for var in required_vars if not getattr(cls, var)]
        
        if missing:
            print(f"🚨 ERROR: Faltan variables de entorno requeridas: {', '.join(missing)}")
            print("Configura estas variables en tu entorno o en un archivo .env")
            sys.exit(1)
        
        print("✅ Configuración requerida verificada correctamente")
        
    @classmethod
    def update_setting(cls, key, value):
        """
        Actualiza un valor de configuración en memoria.
        No persiste el cambio en la base de datos.
        
        Args:
            key: Clave de configuración
            value: Nuevo valor
        """
        cls._dynamic_config[key] = value
        
        # También actualizar atributo de clase si existe
        if hasattr(cls, key.upper()):
            attr_value = getattr(cls, key.upper())
            
            # Convertir al tipo correcto
            if isinstance(attr_value, int):
                setattr(cls, key.upper(), int(value))
            elif isinstance(attr_value, float):
                setattr(cls, key.upper(), float(value))
            else:
                setattr(cls, key.upper(), value)
