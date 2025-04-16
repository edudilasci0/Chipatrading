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
    """Configura manejadores para señales de sistema"""
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

async def init_components():
    """Inicializa todos los componentes necesarios para el bot"""
    components = {}
    
    # Inicializar componentes de mercado y análisis
    logger.info("🏗️ Inicializando componentes de análisis de mercado...")
    dexscreener_client = DexScreenerClient()
    market_metrics = MarketMetricsAnalyzer(dexscreener_client=dexscreener_client)
    token_analyzer = TokenAnalyzer(dexscreener_client=dexscreener_client)
    
    # Inicializar gestores de wallets
    logger.info("👛 Inicializando gestores de wallets...")
    wallet_tracker = WalletTracker()  # Carga básica desde traders_data.json
    wallet_manager = WalletManager()  # Gestor avanzado con soporte para BD
    
    # Inicializar API de datos en tiempo real y procesamiento
    logger.info("🔄 Inicializando API de datos en tiempo real...")
    cielo_api = CieloAPI()
    trader_profiler = TraderProfiler()
    
    # Inicializar lógica de señales
    logger.info("🚨 Inicializando lógica de señales...")
    signal_logic = SignalLogic(dexscreener_client=dexscreener_client)
    
    # Inicializar el gestor de transacciones con los componentes apropiados
    transaction_manager = TransactionManager(
        signal_logic=signal_logic,
        wallet_tracker=wallet_tracker,
        wallet_manager=wallet_manager
    )
    transaction_manager.cielo_adapter = cielo_api  # Asignar el adaptador después de la inicialización
    
    components = {
        'dexscreener_client': dexscreener_client,
        'wallet_tracker': wallet_tracker,
        'wallet_manager': wallet_manager,
        'cielo_api': cielo_api,
        'transaction_manager': transaction_manager,
        'signal_logic': signal_logic,
        'market_metrics': market_metrics,
        'token_analyzer': token_analyzer,
        'trader_profiler': trader_profiler
    }
    
    return components

async def init_telegram_bot(signal_logic, wallet_manager):
    """Inicializa el bot de Telegram para comandos"""
    telegram_bot_token = Config.TELEGRAM_BOT_TOKEN
    telegram_chat_id = Config.TELEGRAM_CHAT_ID
    
    if not telegram_bot_token or not telegram_chat_id:
        logger.warning("⚠️ No se encontraron credenciales para Telegram, los comandos no estarán disponibles")
        return None
        
    logger.info("🤖 Iniciando bot de Telegram para comandos...")
    telegram_task = asyncio.create_task(
        fix_telegram_commands()(
            telegram_bot_token, 
            telegram_chat_id, 
            signal_logic, 
            wallet_manager
        )
    )
    return telegram_task

async def load_wallets(wallet_tracker, wallet_manager):
    """Carga wallets de todas las fuentes disponibles"""
    wallets_from_json = wallet_tracker.get_wallets()
    wallets_from_db = wallet_manager.get_all_wallet_addresses()
    
    # Combinar wallets de diferentes fuentes (eliminando duplicados)
    all_wallets = list(set(wallets_from_json + wallets_from_db))
    
    if not all_wallets:
        logger.warning("⚠️ No se han cargado wallets. Puedes añadirlas mediante comandos de Telegram o en traders_data.json")
    else:
        logger.info(f"📋 Cargadas {len(all_wallets)} wallets para monitoreo ({len(wallets_from_json)} desde JSON, {len(wallets_from_db)} desde BD)")
    
    return all_wallets

async def cleanup_resources(components):
    """Limpia todos los recursos y conexiones de los componentes"""
    logger.info("🧹 Limpiando recursos...")
    for name, component in components.items():
        if hasattr(component, 'close'):
            try:
                await component.close()
                logger.info(f"✅ {name} cerrado correctamente")
            except Exception as e:
                logger.error(f"Error cerrando {name}: {e}")

async def main_loop(components, all_wallets):
    """Bucle principal de funcionamiento del bot"""
    cielo_api = components['cielo_api']
    signal_logic = components['signal_logic']
    transaction_manager = components['transaction_manager']
    wallet_tracker = components['wallet_tracker']
    wallet_manager = components['wallet_manager']
    
    # Conectar a la API de datos y configurar callbacks
    await cielo_api.connect(all_wallets)
    transaction_manager.set_message_callback(signal_logic.process_transaction)
    
    # Iniciar bucle principal
    while not shutdown_flag:
        try:
            # Procesar señales basadas en datos existentes
            await signal_logic.process_signals()
            
            # Verificar estado de la conexión con Cielo
            if not cielo_api.source_health["healthy"]:
                logger.warning("⚠️ La conexión con Cielo API no está saludable, intentando reconectar...")
                # Recargar wallets por si se añadieron nuevas
                updated_wallets = await load_wallets(wallet_tracker, wallet_manager)
                await cielo_api.connect(updated_wallets)
            
            await asyncio.sleep(60)  # Esperar 1 minuto entre iteraciones
        except Exception as e:
            logger.error(f"Error en el bucle principal: {str(e)}")
            await asyncio.sleep(60)  # Esperar antes de reintentar

async def main():
    """Función principal del bot de trading"""
    components = {}
    try:
        # Configurar manejadores de señales
        setup_signal_handlers()
        
        # Inicializar base de datos
        if not await init_database():
            return 1
        
        # Inicializar componentes
        logger.info("🔄 Inicializando componentes...")
        components = await init_components()
        
        # Inicializar bot de Telegram
        telegram_task = await init_telegram_bot(
            components['signal_logic'], 
            components['wallet_manager']
        )
        
        # Cargar wallets de todas las fuentes
        all_wallets = await load_wallets(
            components['wallet_tracker'], 
            components['wallet_manager']
        )
        
        # Iniciar bucle principal
        await main_loop(components, all_wallets)
                
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
