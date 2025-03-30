import asyncio
import signal
import sys
import logging
import time
import traceback
from datetime import datetime
from config import Config
import db

# Servicios y APIs
from cielo_api import CieloAPI
from helius_client import HeliusClient
from dexscreener_client import DexScreenerClient

# Componentes principales
from wallet_tracker import WalletTracker
from scoring import ScoringSystem
from signal_logic import SignalLogic
from performance_tracker import PerformanceTracker

# Componentes avanzados
from dex_monitor import DexMonitor
from market_metrics import MarketMetricsAnalyzer
from token_analyzer import TokenAnalyzer
from trader_profiler import TraderProfiler
from whale_detector import WhaleDetector
from signal_predictor import SignalPredictor

# Utilidades
from telegram_utils import fix_telegram_commands, fix_on_cielo_message, send_telegram_message

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

async def cleanup_resources(components):
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
    try:
        pending_signals = db.get_recent_untracked_signals(24)
        if not pending_signals:
            logger.info("No hay señales pendientes para procesar")
            return []
        logger.info(f"Procesando {len(pending_signals)} señales pendientes...")
        return pending_signals
    except Exception as e:
        logger.error(f"Error procesando señales pendientes: {e}")
        return []

async def run_heartbeat(interval=300):
    start_time = time.time()
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
    global shutdown_flag
    try:
        start_time = time.time()
        setup_signal_handlers()
        if not db.init_db():
            logger.critical("No se pudo inicializar la base de datos. Abortando.")
            return 1
        Config.check_required_config()
        logger.info("🔄 Inicializando componentes...")
        
        helius_client = HeliusClient(Config.HELIUS_API_KEY)
        dexscreener_client = DexScreenerClient()
        cielo_api = CieloAPI(api_key=Config.CIELO_API_KEY)
        helius_client.dexscreener_client = dexscreener_client
        
        wallet_tracker = WalletTracker()
        scoring_system = ScoringSystem()
        signal_predictor = SignalPredictor()
        
        dex_monitor = DexMonitor()
        whale_detector = WhaleDetector(helius_client=helius_client)
        market_metrics = MarketMetricsAnalyzer(helius_client=helius_client, dexscreener_client=dexscreener_client)
        token_analyzer = TokenAnalyzer(token_data_service=helius_client)
        trader_profiler = TraderProfiler(scoring_system=scoring_system)
        
        performance_tracker = PerformanceTracker(
            token_data_service=helius_client,
            dex_monitor=dex_monitor,
            market_metrics=market_metrics,
            whale_detector=whale_detector
        )
        performance_tracker.token_analyzer = token_analyzer
        
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
        signal_logic.performance_tracker = performance_tracker
        
        Config.update_setting("mcap_threshold", "100000")
        Config.update_setting("volume_threshold", "200000")
        
        logger.info("✅ Todos los componentes inicializados correctamente")
        logger.info("📊 Umbrales establecidos: Market Cap mín. $100K, Volumen mín. $200K")
        
        components = {
            "helius_client": helius_client,
            "dexscreener_client": dexscreener_client,
            "dex_monitor": dex_monitor,
            "whale_detector": whale_detector,
            "market_metrics": market_metrics,
            "token_analyzer": token_analyzer,
            "trader_profiler": trader_profiler,
            "scoring_system": scoring_system,
            "signal_logic": signal_logic,
            "performance_tracker": performance_tracker
        }
        
        maintenance_task = asyncio.create_task(periodic_maintenance(components))
        heartbeat_task = asyncio.create_task(run_heartbeat())
        
        pending_signals = await process_pending_signals()
        for signal in pending_signals:
            token = signal.get("token")
            if token:
                performance_tracker.add_signal(token, signal)
        
        telegram_process_commands = fix_telegram_commands()
        if Config.TELEGRAM_BOT_TOKEN and Config.TELEGRAM_CHAT_ID:
            telegram_task = asyncio.create_task(
                telegram_process_commands(
                    Config.TELEGRAM_BOT_TOKEN,
                    Config.TELEGRAM_CHAT_ID,
                    signal_logic,
                    wallet_tracker
                )
            )
            logger.info("✅ Bot de Telegram inicializado")
        else:
            logger.warning("⚠️ Bot de Telegram no configurado")
            telegram_task = None
        
        on_cielo_message = fix_on_cielo_message()
        wallets_to_track = wallet_tracker.get_wallets()
        if not wallets_to_track:
            logger.error("❌ No hay wallets para seguir. Revisa traders_data.json")
            return 1
        
        logger.info(f"📋 Siguiendo {len(wallets_to_track)} wallets")
        while not shutdown_flag:
            try:
                await cielo_api.run_forever_wallets(
                    wallets=wallets_to_track,
                    on_message_callback=lambda message: on_cielo_message(
                        message,
                        wallet_tracker,
                        scoring_system,
                        signal_logic
                    )
                )
            except Exception as e:
                logger.error(f"Error en conexión WebSocket: {e}")
                if not shutdown_flag:
                    logger.info("🔄 Esperando para reconectar...")
                    await asyncio.sleep(15)
        if telegram_task:
            telegram_task.cancel()
        maintenance_task.cancel()
        heartbeat_task.cancel()
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
