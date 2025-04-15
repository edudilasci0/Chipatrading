#!/usr/bin/env python3
# main.py - Punto de entrada principal para el bot de trading en Solana

import asyncio
import signal
import sys
import logging
import time
import traceback
from datetime import datetime, timedelta
from config import Config
import db

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Servicios y APIs
from cielo_api import CieloAPI
from dexscreener_client import DexScreenerClient

# Componentes principales
from wallet_tracker import WalletTracker
from wallet_manager import WalletManager
from scoring import ScoringSystem
from signal_logic import SignalLogic
from performance_tracker import PerformanceTracker
from transaction_manager import TransactionManager

# Componentes avanzados
from market_metrics import MarketMetricsAnalyzer
from token_analyzer import TokenAnalyzer
from trader_profiler import TraderProfiler

# Utilidades
from telegram_utils import fix_telegram_commands, send_telegram_message

logger = logging.getLogger(__name__)
shutdown_flag = False

def setup_signal_handlers():
    def handle_shutdown(signum, frame):
        global shutdown_flag
        logger.info(f"Señal de terminación recibida ({signum}). Deteniendo procesos...")
        shutdown_flag = True
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)
    logger.info("✅ Manejadores de señales configurados")

async def init_database():
    """Inicializa la base de datos y verifica su estado"""
    try:
        if not db.init_db():
            logger.critical("No se pudo inicializar la base de datos. Abortando.")
            return False
        return True
    except Exception as e:
        logger.critical(f"Error inicializando base de datos: {e}")
        return False

async def cleanup_resources(components):
    """Limpia los recursos de los componentes"""
    logger.info("🧹 Limpiando recursos...")
    for name, component in components.items():
        if hasattr(component, 'close'):
            try:
                await component.close()
                logger.info(f"✅ {name} cerrado correctamente")
            except Exception as e:
                logger.error(f"Error cerrando {name}: {e}")

async def main():
    components = {}  # Inicializamos components al principio
    try:
        # Configurar logging
        setup_signal_handlers()
        logger.info("✅ Manejadores de señales configurados")
        
        # Inicializar base de datos
        if not await init_database():
            return 1
        
        logger.info("🔄 Inicializando componentes...")
        
        # Inicializar componentes
        dexscreener_client = DexScreenerClient()
        signal_logic = SignalLogic(dexscreener_client=dexscreener_client)
        
        components = {
            'dexscreener_client': dexscreener_client,
            'signal_logic': signal_logic
        }
        
        # Iniciar el bucle principal
        while not shutdown_flag:
            try:
                await signal_logic.process_signals()
                await asyncio.sleep(60)  # Esperar 1 minuto entre iteraciones
            except Exception as e:
                logger.error(f"Error en el bucle principal: {str(e)}")
                await asyncio.sleep(60)  # Esperar antes de reintentar
                
    except Exception as e:
        logger.critical(f"Error crítico en main: {str(e)}")
        logger.critical(f"Traceback:\n{traceback.format_exc()}")
        return 1
    finally:
        if components:
            await cleanup_resources(components)
    return 0

if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("Bot detenido por usuario")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Error no manejado: {e}")
        logger.critical(traceback.format_exc())
        sys.exit(1)
