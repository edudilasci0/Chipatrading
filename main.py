# main.py - Punto de entrada principal para el bot de trading en Solana
import asyncio
import signal
import sys
import logging
import time
from datetime import datetime
import traceback

# Configuración y base de datos
from config import Config
import db

# Servicios y APIs
from cielo_api import CieloAPI
from helius_client import HeliusClient
# Se elimina: from scalper_monitor import ScalperActivityMonitor
from dexscreener_client import DexScreenerClient
# Se elimina la importación de RugCheckAPI:
# from rugcheck import RugCheckAPI

# Componentes principales
from wallet_tracker import WalletTracker
from scoring import ScoringSystem
from signal_logic import SignalLogic
from performance_tracker import PerformanceTracker
from signal_predictor import SignalPredictor

# Componentes avanzados
from dex_monitor import DexMonitor
from market_metrics import MarketMetricsAnalyzer
from token_analyzer import TokenAnalyzer
from trader_profiler import TraderProfiler
from whale_detector import WhaleDetector

# Utilidades
from telegram_utils import fix_telegram_commands, fix_on_cielo_message
import telegram_utils

# Nuevos componentes de gestión
from wallet_manager import WalletManager
from transaction_manager import TransactionManager

# Configuración de logging
logger = logging.getLogger(__name__)

# Flag para manejar la terminación del bot
shutdown_flag = False

def setup_signal_handlers():
    """Configura manejadores para señales de terminación"""
    def handle_shutdown(signum, frame):
        global shutdown_flag
        logger.info(f"Señal de terminación recibida ({signum}). Deteniendo procesos...")
        shutdown_flag = True
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)
    logger.info("✅ Manejadores de señales configurados")

async def cleanup_resources(components):
    """Limpia recursos antes de cerrar el bot"""
    logger.info("🧹 Limpiando recursos...")
    for name, component in components.items():
        if hasattr(component, 'close_session'):
            try:
                await component.close_session()
                logger.info(f"Conexión cerrada para {name}")
            except Exception as e:
                logger.error(f"Error cerrando {name}: {e}")
        if hasattr(component, 'stop_cleanup') and callable(component.stop_cleanup):
            try:
                component.stop_cleanup()
                logger.info(f"Tareas de limpieza detenidas para {name}")
            except Exception as e:
                logger.error(f"Error deteniendo tareas de {name}: {e}")
    try:
        if hasattr(db, 'pool') and db.pool:
            db.pool.closeall()
            logger.info("Conexiones de BD cerradas")
    except Exception as e:
        logger.error(f"Error cerrando pool de BD: {e}")

async def periodic_maintenance(components):
    """Realiza mantenimiento periódico de caché y limpieza de datos"""
    try:
        cleanup_interval = int(Config.get("CACHE_CLEANUP_INTERVAL", 3600))
        while not shutdown_flag:
            await asyncio.sleep(cleanup_interval)
            if shutdown_flag:
                break
            logger.info("🧹 Iniciando mantenimiento periódico...")
            start_time = time.time()
            for name, component in components.items():
                if hasattr(component, 'cleanup_old_data') and callable(component.cleanup_old_data):
                    try:
                        component.cleanup_old_data()
                    except Exception as e:
                        logger.error(f"Error en limpieza de {name}: {e}")
            db.clear_query_cache()
            Config.load_dynamic_config()
            duration = time.time() - start_time
            logger.info(f"✅ Mantenimiento completado en {duration:.2f}s")
    except Exception as e:
        logger.error(f"Error en tarea de mantenimiento: {e}")

async def process_pending_signals():
    """Procesa señales pendientes al iniciar"""
    try:
        recent_signals = db.get_recent_untracked_signals(hours=24)
        if not recent_signals:
            logger.info("No hay señales pendientes para procesar")
            return []
        logger.info(f"Procesando {len(recent_signals)} señales pendientes...")
        return recent_signals
    except Exception as e:
        logger.error(f"Error procesando señales pendientes: {e}")
        return []

async def run_heartbeat(interval=300):
    """Envía señales de heartbeat periódicas"""
    while not shutdown_flag:
        try:
            stats = {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "uptime": int(time.time() - start_time),
                "db_cache": db.get_cache_stats(),
                "signal_count": db.count_signals_today(),
                "transaction_count": db.count_transactions_today()
            }
            logger.info(f"❤️ Heartbeat | Señales hoy: {stats['signal_count']} | Transacciones: {stats['transaction_count']}")
            await asyncio.sleep(interval)
        except Exception as e:
            logger.error(f"Error en heartbeat: {e}")
            await asyncio.sleep(60)

async def main():
    global shutdown_flag, start_time
    try:
        start_time = time.time()
        setup_signal_handlers()
        db_ready = db.init_db()
        if not db_ready:
            logger.critical("No se pudo inicializar la base de datos. Abortando.")
            return 1
        Config.check_required_config()
        logger.info("🔄 Inicializando componentes...")
        
        # Servicios externos
        helius_client = HeliusClient(Config.HELIUS_API_KEY)
        # Se elimina la inicialización de GMGN:
        # gmgn_client = GMGNClient()
        dexscreener_client = DexScreenerClient()
        # Se elimina RugCheckAPI:
        # rugcheck_api = RugCheckAPI()
        helius_client.dexscreener_client = dexscreener_client
        
        # Componentes principales
        wallet_tracker = WalletTracker()
        scoring_system = ScoringSystem()
        signal_predictor = SignalPredictor()
        
        # Componentes avanzados
        dex_monitor = DexMonitor()
        whale_detector = WhaleDetector(helius_client=helius_client)
        market_metrics = MarketMetricsAnalyzer(helius_client=helius_client, dexscreener_client=dexscreener_client)
        token_analyzer = TokenAnalyzer(token_data_service=helius_client)
        trader_profiler = TraderProfiler(scoring_system=scoring_system)
        # Se elimina scalper_monitor:
        # scalper_monitor = ScalperActivityMonitor()
        performance_tracker = PerformanceTracker(
            token_data_service=helius_client,
            dex_monitor=dex_monitor,
            market_metrics=market_metrics,
            whale_detector=whale_detector
        )
        performance_tracker.token_analyzer = token_analyzer
        
        # Inicialización de SignalLogic sin gmgn_client y RugCheckAPI
        signal_logic = SignalLogic(
            scoring_system=scoring_system,
            helius_client=helius_client,
            rugcheck_api=None,
            ml_predictor=signal_predictor,
            wallet_tracker=wallet_tracker
        )
        signal_logic.whale_detector = whale_detector
        signal_logic.market_metrics = market_metrics
        signal_logic.token_analyzer = token_analyzer
        signal_logic.trader_profiler = trader_profiler
        signal_logic.dex_monitor = dex_monitor
        signal_logic.dexscreener_client = dexscreener_client
        signal_logic.performance_tracker = performance_tracker
        
        Config.update_setting("mcap_threshold", "100000")
        Config.update_setting("volume_threshold", "200000")
        
        logger.info("✅ Todos los componentes inicializados correctamente")
        logger.info("📊 Umbrales establecidos: Market Cap mín. $100K, Volumen mín. $200K")
        
        components = {
            "helius_client": helius_client,
            "dexscreener_client": dexscreener_client,
            # Se elimina "gmgn_client"
            "dex_monitor": dex_monitor,
            "whale_detector": whale_detector,
            "market_metrics": market_metrics,
            "token_analyzer": token_analyzer,
            "trader_profiler": trader_profiler,
            "scoring_system": scoring_system,
            "signal_logic": signal_logic,
            "performance_tracker": performance_tracker
            # Se elimina "scalper_monitor"
        }
        
        maintenance_task = asyncio.create_task(periodic_maintenance(components))
        heartbeat_task = asyncio.create_task(run_heartbeat())
        
        pending_signals = await process_pending_signals()
        for signal in pending_signals:
            token = signal.get("token")
            if token:
                performance_tracker.add_signal(token, signal)
        
        # Iniciar bot de Telegram, pasando WalletManager
        telegram_process_commands = fix_telegram_commands()
        wallet_manager = WalletManager()
        if Config.TELEGRAM_BOT_TOKEN and Config.TELEGRAM_CHAT_ID:
            telegram_task = asyncio.create_task(
                telegram_process_commands(
                    Config.TELEGRAM_BOT_TOKEN,
                    Config.TELEGRAM_CHAT_ID,
                    signal_logic,
                    wallet_manager
                )
            )
            logger.info("✅ Bot de Telegram inicializado")
        else:
            logger.warning("⚠️ Bot de Telegram no configurado")
            telegram_task = None
        
        on_cielo_message = fix_on_cielo_message()
        
        # Configuración y gestión de TransactionManager (se elimina scalper_monitor)
        transaction_manager = TransactionManager(
            signal_logic=signal_logic,
            wallet_tracker=wallet_tracker,
            scoring_system=scoring_system,
            wallet_manager=wallet_manager
        )
        cielo_api = CieloAPI(api_key=Config.CIELO_API_KEY)
        transaction_manager.cielo_adapter = cielo_api
        transaction_manager.helius_adapter = helius_client
        
        await transaction_manager.start()
        
        async def monitor_transaction_manager():
            while not shutdown_flag:
                await asyncio.sleep(30)
                stats = transaction_manager.get_stats()
                if not stats["active_source"] or stats["active_source"] == "none":
                    logger.warning("Fuente de datos inactiva, reiniciando transaction_manager")
                    await transaction_manager.stop()
                    await asyncio.sleep(5)
                    await transaction_manager.start()
        monitor_task = asyncio.create_task(monitor_transaction_manager())
        # Fin TransactionManager
        
        startup_message = (
            "🚀 *Bot Iniciado Correctamente*\n\n"
            f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            "Versión: 1.0.0\n"
            f"Señales hoy: {db.count_signals_today()}\n"
            f"Transacciones hoy: {db.count_transactions_today()}\n\n"
            "Umbrales: Market Cap mín. $100K, Volumen mín. $200K\n\n"
            "Monitoreo de wallets activado."
        )
        telegram_utils.send_telegram_message(startup_message)
        
        wallets_to_track = wallet_tracker.get_wallets()
        if not wallets_to_track:
            logger.error("❌ No hay wallets para seguir. Revisa traders_data.json")
            return 1
        logger.info(f"📋 Siguiendo {len(wallets_to_track)} wallets")
        
        while not shutdown_flag:
            await asyncio.sleep(5)
        
        if telegram_task:
            telegram_task.cancel()
        monitor_task.cancel()
        maintenance_task.cancel()
        heartbeat_task.cancel()
        await transaction_manager.stop()
        await cleanup_resources(components)
        
        logger.info("✅ Bot detenido correctamente")
        return 0
    
    except Exception as e:
        logger.critical(f"🚨 Error crítico: {e}")
        logger.critical(traceback.format_exc())
        return 1

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
