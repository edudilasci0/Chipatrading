#!/usr/bin/env python3
"""
config.py – Configuración centralizada para el bot de trading en Solana

Esta versión actualizada elimina las referencias a Helius y agrega
parámetros específicos para DexScreener, facilitando la migración hacia
una arquitectura basada en Cielo (transacciones) y DexScreener (datos de mercado).
"""

import os
import logging

# Variables de API y credenciales
# Clave para el bot de Telegram
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# Clave de API para Cielo Finance
CIELO_API_KEY = os.environ.get("CIELO_API_KEY", "")

# Configuración para DexScreener (si se requiere autenticación o URL base personalizada)
DEXSCREENER_BASE_URL = os.environ.get("DEXSCREENER_BASE_URL", "https://api.dexscreener.com")
# En caso de necesitar clave de API para DexScreener, se puede agregar aquí (por ahora se deja vacío)
DEXSCREENER_API_KEY = os.environ.get("DEXSCREENER_API_KEY", "")

# Parámetros de transacción y umbrales
MIN_TRANSACTION_USD = os.environ.get("MIN_TRANSACTION_USD", "200")
MIN_TRADERS_FOR_SIGNAL = os.environ.get("MIN_TRADERS_FOR_SIGNAL", "2")
SIGNAL_WINDOW_SECONDS = os.environ.get("SIGNAL_WINDOW_SECONDS", "540")
MIN_CONFIDENCE_THRESHOLD = os.environ.get("MIN_CONFIDENCE_THRESHOLD", "0.3")
# Umbrales de mercado
MCAP_THRESHOLD = os.environ.get("MCAP_THRESHOLD", "100000")
VOLUME_THRESHOLD = os.environ.get("VOLUME_THRESHOLD", "200000")

# Configuración de caché
HELIUS_CACHE_DURATION = os.environ.get("HELIUS_CACHE_DURATION", "300")  # Si ya no se usa Helius, puede usarse para DexScreener
# Parámetro para DexScreener (usar el mismo valor si no se requiere diferencia)
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

def load_dynamic_config():
    """
    Carga configuración dinámica desde la base de datos o archivo de configuración.
    Este método se puede extender para recargar parámetros sin reiniciar el servicio.
    """
    # Aquí se podría implementar la recarga dinámica; por ahora solo se imprime un log.
    logging.getLogger("config").info("Dynamic configuration reloaded.")

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
    # En esta implementación se actualiza en memoria; en producción se debería
    # persistir el valor en la base de datos mediante el módulo db.
    globals()[key] = value
    logging.getLogger("config").info(f"Updated setting {key} = {value} (in memory)")
    return True

def check_required_config():
    """
    Verifica que las variables de entorno críticas estén definidas.
    Se eliminan las variables referentes a Helius y se verifica solo lo necesario.
    """
    missing = []
    if not TELEGRAM_BOT_TOKEN:
        missing.append("TELEGRAM_BOT_TOKEN")
    if not TELEGRAM_CHAT_ID:
        missing.append("TELEGRAM_CHAT_ID")
    if not CIELO_API_KEY:
        missing.append("CIELO_API_KEY")
    if not DATABASE_PATH:
        missing.append("DATABASE_PATH")
    # Opcional: Si DexScreener requiere autenticación, se puede verificar DEXSCREENER_API_KEY
    if missing:
        raise ValueError(f"Faltan las siguientes variables de entorno requeridas: {', '.join(missing)}")

def setup_logging():
    """
    Configura el logging para el sistema.
    """
    logging.basicConfig(
        level=LOG_LEVEL,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    # Se puede configurar loggers específicos para ciertos módulos si es necesario.
    logging.getLogger("telegram_utils").setLevel(LOG_LEVEL)
    logging.getLogger("transaction_manager").setLevel(LOG_LEVEL)
    logging.getLogger("signal_logic").setLevel(LOG_LEVEL)

# Ejecutar configuración inicial
setup_logging()
check_required_config()

# Opcional: imprimir configuración cargada (para depuración)
logging.getLogger("config").debug("Configuración inicial cargada:")
logging.getLogger("config").debug(f"TELEGRAM_BOT_TOKEN: {TELEGRAM_BOT_TOKEN}")
logging.getLogger("config").debug(f"TELEGRAM_CHAT_ID: {TELEGRAM_CHAT_ID}")
logging.getLogger("config").debug(f"CIELO_API_KEY: {CIELO_API_KEY}")
logging.getLogger("config").debug(f"DEXSCREENER_BASE_URL: {DEXSCREENER_BASE_URL}")
logging.getLogger("config").debug(f"MIN_TRANSACTION_USD: {MIN_TRANSACTION_USD}")
logging.getLogger("config").debug(f"MCAP_THRESHOLD: {MCAP_THRESHOLD}")
logging.getLogger("config").debug(f"VOLUME_THRESHOLD: {VOLUME_THRESHOLD}")

# Fin del archivo de configuración
