#!/usr/bin/env python3
"""
config.py – Configuración centralizada para el bot de trading en Solana

Esta versión define una clase Config que encapsula todas las variables de configuración
y funciones necesarias para el funcionamiento del bot. De esta forma, el resto del
código puede importar Config mediante "from config import Config".
"""

import os
import logging

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
    
    # Configuración de salud de fuentes (para Cielo y DexScreener)
    SOURCE_HEALTH_CHECK_INTERVAL = os.environ.get("SOURCE_HEALTH_CHECK_INTERVAL", "60")
    MAX_SOURCE_FAILURES = os.environ.get("MAX_SOURCE_FAILURES", "3")
    SOURCE_TIMEOUT = os.environ.get("SOURCE_TIMEOUT", "300")
    
    # Configuración de base de datos
    DATABASE_PATH = os.environ.get("DATABASE_PATH", "")
    
    # Configuración de logging
    LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
    DEFAULT_SCORE = os.environ.get("DEFAULT_SCORE", "5.0")

    @staticmethod
    def load_dynamic_config():
        """
        Carga configuración dinámica desde la base de datos o archivo de configuración.
        Este método se puede extender para recargar parámetros sin reiniciar el servicio.
        """
        logging.getLogger("config").info("Dynamic configuration reloaded.")

    @staticmethod
    def update_setting(key: str, value: str) -> bool:
        """
        Actualiza la configuración en memoria. Esta función se puede integrar con
        la base de datos para persistir cambios.
        
        Args:
            key: Nombre del setting
            value: Nuevo valor a asignar
        
        Returns:
            bool: True si se actualizó correctamente, False en caso contrario.
        """
        setattr(Config, key, value)
        logging.getLogger("config").info(f"Updated setting {key} = {value} (in memory)")
        return True

    @staticmethod
    def check_required_config():
        """
        Verifica que las variables de entorno críticas estén definidas.
        """
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
        """
        Configura el logging para el sistema.
        """
        logging.basicConfig(
            level=Config.LOG_LEVEL,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        # Se pueden configurar niveles específicos para módulos
        logging.getLogger("telegram_utils").setLevel(Config.LOG_LEVEL)
        logging.getLogger("transaction_manager").setLevel(Config.LOG_LEVEL)
        logging.getLogger("signal_logic").setLevel(Config.LOG_LEVEL)

# Ejecutar configuración inicial
Config.setup_logging()
Config.check_required_config()

logger = logging.getLogger("config")
logger.debug("Configuración inicial cargada:")
logger.debug(f"TELEGRAM_BOT_TOKEN: {Config.TELEGRAM_BOT_TOKEN}")
logger.debug(f"TELEGRAM_CHAT_ID: {Config.TELEGRAM_CHAT_ID}")
logger.debug(f"CIELO_API_KEY: {Config.CIELO_API_KEY}")
logger.debug(f"DEXSCREENER_BASE_URL: {Config.DEXSCREENER_BASE_URL}")
logger.debug(f"MIN_TRANSACTION_USD: {Config.MIN_TRANSACTION_USD}")
logger.debug(f"MCAP_THRESHOLD: {Config.MCAP_THRESHOLD}")
logger.debug(f"VOLUME_THRESHOLD: {Config.VOLUME_THRESHOLD}")
