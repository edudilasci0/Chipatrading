#!/usr/bin/env python3
"""
config.py – Configuración centralizada para el bot de trading en Solana
"""

import os
import logging
import time

class Config:
    # Variables de API y credenciales
    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
    CIELO_API_KEY = os.environ.get("CIELO_API_KEY", "")
    
    # Configuración para DexScreener
    DEXSCREENER_BASE_URL = os.environ.get("DEXSCREENER_BASE_URL", "https://api.dexscreener.com")
    DEXSCREENER_API_KEY = os.environ.get("DEXSCREENER_API_KEY", "")
    
    # Parámetros de transacción y umbrales
    MIN_TRANSACTION_USD = os.environ.get("MIN_TRANSACTION_USD", "200")
    MIN_TRADERS_FOR_SIGNAL = os.environ.get("MIN_TRADERS_FOR_SIGNAL", "2")
    SIGNAL_WINDOW_SECONDS = os.environ.get("SIGNAL_WINDOW_SECONDS", "540")
    MIN_CONFIDENCE_THRESHOLD = os.environ.get("MIN_CONFIDENCE_THRESHOLD", "0.3")
    MCAP_THRESHOLD = os.environ.get("MCAP_THRESHOLD", "100000")
    VOLUME_THRESHOLD = os.environ.get("VOLUME_THRESHOLD", "200000")
    
    # Configuración de caché
    HELIUS_CACHE_DURATION = os.environ.get("HELIUS_CACHE_DURATION", "300")
    DEXSCREENER_CACHE_DURATION = os.environ.get("DEXSCREENER_CACHE_DURATION", "300")
    
    # Configuración de salud de fuentes
    SOURCE_HEALTH_CHECK_INTERVAL = os.environ.get("SOURCE_HEALTH_CHECK_INTERVAL", "60")
    MAX_SOURCE_FAILURES = os.environ.get("MAX_SOURCE_FAILURES", "3")
    SOURCE_TIMEOUT = os.environ.get("SOURCE_TIMEOUT", "300")
    
    # Configuración de base de datos
    DATABASE_PATH = os.environ.get("DATABASE_PATH", "")
    
    # Configuración de logging
    LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
    DEFAULT_SCORE = os.environ.get("DEFAULT_SCORE", "5.0")
    
    # Configuración para trading y análisis
    HIGH_QUALITY_TRADER_SCORE = os.environ.get("HIGH_QUALITY_TRADER_SCORE", "7.0")
    WHALE_TRANSACTION_THRESHOLD = os.environ.get("WHALE_TRANSACTION_THRESHOLD", "10000")
    LIQUIDITY_HEALTHY_THRESHOLD = os.environ.get("LIQUIDITY_HEALTHY_THRESHOLD", "20000")
    SLIPPAGE_WARNING_THRESHOLD = os.environ.get("SLIPPAGE_WARNING_THRESHOLD", "10")
    TRADER_QUALITY_WEIGHT = os.environ.get("TRADER_QUALITY_WEIGHT", "0.35")
    WHALE_ACTIVITY_WEIGHT = os.environ.get("WHALE_ACTIVITY_WEIGHT", "0.20")
    HOLDER_GROWTH_WEIGHT = os.environ.get("HOLDER_GROWTH_WEIGHT", "0.15")
    LIQUIDITY_HEALTH_WEIGHT = os.environ.get("LIQUIDITY_HEALTH_WEIGHT", "0.15")
    TECHNICAL_FACTORS_WEIGHT = os.environ.get("TECHNICAL_FACTORS_WEIGHT", "0.15")
    
    # Cache para valores obtenidos de la base de datos
    _db_cache = {}
    _db_cache_timestamp = {}
    _db_cache_ttl = 300  # 5 minutos

    @staticmethod
    def get(key, default=None):
        """
        Obtiene un valor de configuración o devuelve el valor por defecto si no existe.
        
        Args:
            key: Clave de configuración
            default: Valor por defecto si la clave no existe
            
        Returns:
            El valor de la configuración o el valor por defecto
        """
        # Primero intentar obtener el valor desde la clase
        try:
            value = getattr(Config, key.upper(), None)
            if value is not None:
                return value
        except:
            pass
            
        # Luego intentar obtener desde caché
        now = time.time()
        if key in Config._db_cache and now - Config._db_cache_timestamp.get(key, 0) < Config._db_cache_ttl:
            return Config._db_cache[key]
            
        # Finalmente intentar obtener desde la base de datos
        try:
            import db
            if hasattr(db, 'execute_cached_query'):
                result = db.execute_cached_query("SELECT value FROM bot_settings WHERE key = %s", (key,), max_age=300)
                if result and len(result) > 0 and 'value' in result[0]:
                    Config._db_cache[key] = result[0]['value']
                    Config._db_cache_timestamp[key] = now
                    return result[0]['value']
        except Exception as e:
            logger = logging.getLogger("config")
            logger.debug(f"Error obteniendo valor desde BD para '{key}': {e}")
            
        return default

    @staticmethod
    def load_dynamic_config():
        """Recarga la configuración dinámica desde la base de datos"""
        Config._db_cache = {}
        Config._db_cache_timestamp = {}
        logger = logging.getLogger("config")
        logger.info("Configuración dinámica recargada")
        
        try:
            import db
            if hasattr(db, 'execute_cached_query'):
                settings = db.execute_cached_query("SELECT key, value FROM bot_settings", max_age=1)
                for setting in settings:
                    Config._db_cache[setting['key']] = setting['value']
                    Config._db_cache_timestamp[setting['key']] = time.time()
                logger.info(f"Cargados {len(settings)} ajustes desde la base de datos")
        except Exception as e:
            logger.error(f"Error cargando configuración dinámica: {e}")

    @staticmethod
    def update_setting(key: str, value: str) -> bool:
        """
        Actualiza un ajuste en memoria y en la base de datos si está disponible
        
        Args:
            key: Clave del ajuste
            value: Valor a establecer
            
        Returns:
            bool: True si la actualización fue exitosa
        """
        try:
            # Actualizar en memoria
            setattr(Config, key.upper(), value)
            Config._db_cache[key] = value
            Config._db_cache_timestamp[key] = time.time()
            
            # Actualizar en la base de datos si está disponible
            try:
                import db
                if hasattr(db, 'update_setting'):
                    db.update_setting(key, value)
            except Exception as e:
                logger = logging.getLogger("config")
                logger.warning(f"Error actualizando ajuste en BD: {e}")
                
            logger = logging.getLogger("config")
            logger.info(f"Actualizado ajuste {key} = {value}")
            return True
        except Exception as e:
            logger = logging.getLogger("config")
            logger.error(f"Error actualizando ajuste {key}: {e}")
            return False

    @staticmethod
    def check_required_config():
        missing = []
        if not Config.TELEGRAM_BOT_TOKEN:
            missing.append("TELEGRAM_BOT_TOKEN")
        if not Config.TELEGRAM_CHAT_ID:
            missing.append("TELEGRAM_CHAT_ID")
        if not Config.CIELO_API_KEY:
            missing.append("CIELO_API_KEY")
        if not Config.DATABASE_PATH:
            missing.append("DATABASE_PATH")
        if missing:
            raise ValueError(f"Faltan las siguientes variables de entorno requeridas: {', '.join(missing)}")

    @staticmethod
    def setup_logging():
        logging.basicConfig(
            level=getattr(logging, Config.LOG_LEVEL),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        logging.getLogger("telegram_utils").setLevel(getattr(logging, Config.LOG_LEVEL))
        logging.getLogger("transaction_manager").setLevel(getattr(logging, Config.LOG_LEVEL))
        logging.getLogger("signal_logic").setLevel(getattr(logging, Config.LOG_LEVEL))

# Ejecutar configuración inicial
Config.setup_logging()

logger = logging.getLogger("config")
logger.debug("Configuración inicial cargada:")
logger.debug(f"TELEGRAM_BOT_TOKEN: {Config.TELEGRAM_BOT_TOKEN}")
logger.debug(f"TELEGRAM_CHAT_ID: {Config.TELEGRAM_CHAT_ID}")
logger.debug(f"CIELO_API_KEY: {Config.CIELO_API_KEY}")
logger.debug(f"DEXSCREENER_BASE_URL: {Config.DEXSCREENER_BASE_URL}")
logger.debug(f"MIN_TRANSACTION_USD: {Config.MIN_TRANSACTION_USD}")
logger.debug(f"MCAP_THRESHOLD: {Config.MCAP_THRESHOLD}")
logger.debug(f"VOLUME_THRESHOLD: {Config.VOLUME_THRESHOLD}")
