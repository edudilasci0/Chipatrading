# main.py
import os
import sys
import time
import signal
import asyncio
import logging
from datetime import datetime

# Configurar logging básico
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("chipatrading")

# Importar configuración
from config import Config
# Asegurarse de inicializar la configuración
Config.initialize()

# Importar módulos básicos
from wallet_tracker import WalletTracker
from cielo_api import CieloAPI
from scoring import ScoringSystem
from signal_logic import SignalLogic
from telegram_utils import send_telegram_message, fix_telegram_commands, fix_on_cielo_message
import db

# Importar nuevos módulos de análisis
from performance_tracker import PerformanceTracker
from whale_detector import WhaleDetector
from market_metrics import MarketMetricsAnalyzer
from token_analyzer import TokenAnalyzer
from trader_profiler import TraderProfiler
from dex_monitor import DexMonitor
from scalper_monitor import ScalperActivityMonitor

# Variable global para controlar la ejecución del bot
bot_running = True

# Función para limpiar recursos y salir adecuadamente
def cleanup_and_exit(signum=None, frame=None):
    global bot_running
    logger.info("Iniciando apagado gracioso del bot...")
    bot_running = False
    # Dar tiempo a las tareas asíncronas para finalizar
    time.sleep(2)
    logger.info("Bot apagado completamente")
    sys.exit(0)

# Función para limpiar datos antiguos de manera periódica
async def cleanup_data_periodically(modules, interval=3600):
    """
    Limpia datos antiguos de todos los módulos de análisis periódicamente.
    
    Args:
        modules: Lista de módulos con método cleanup_old_data
        interval: Intervalo en segundos entre limpiezas
    """
    while True:
        try:
            logger.info("Ejecutando limpieza periódica de datos...")
            for module in modules:
                if hasattr(module, 'cleanup_old_data'):
                    module.cleanup_old_data()
            await asyncio.sleep(interval)
        except Exception as e:
            logger.error(f"Error en cleanup_data_periodically: {e}")
            await asyncio.sleep(60)

# Función para monitorear rendimiento del sistema
async def monitor_system_health(interval=300):
    """
    Monitorea la salud del sistema (memoria, CPU, conexiones) y registra métricas.
    """
    try:
        import psutil
    except ImportError:
        logger.warning("psutil no está instalado. Monitoreo de sistema deshabilitado.")
        return

    while True:
        try:
            # Obtener métricas del sistema
            memory = psutil.virtual_memory()
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # Si el uso de memoria es alto, intentar liberar caché
            if memory.percent > 85:
                logger.warning(f"⚠️ Uso de memoria alto: {memory.percent}%. Limpiando cachés...")
                db.clear_query_cache()
                # Intentar forzar recolección de basura
                import gc
                gc.collect()
            
            # Registrar métricas
            logger.info(f"Métricas del sistema: Memoria: {memory.percent}%, CPU: {cpu_percent}%")
            
            await asyncio.sleep(interval)
        except Exception as e:
            logger.error(f"Error en monitor_system_health: {e}")
            await asyncio.sleep(60)

# Función principal del bot
async def main():
    global bot_running
    try:
        print("\n==== STARTING TRADING BOT ====")
        print(f"Date/Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        Config.check_required_config()
        
        # Inicializar la base de datos
        db.init_db()
        
        # Inicializar componentes básicos
        wallet_tracker = WalletTracker()
        wallets = wallet_tracker.get_wallets()
        logger.info(f"Wallets loaded: {len(wallets)}")
        
        scoring_system = ScoringSystem()
        
        # Inicializar APIs y servicios de datos
        helius_client = None
        try:
            from helius_client import HeliusClient
            helius_client = HeliusClient(Config.HELIUS_API_KEY)
            logger.info("✅ Helius Client initialized")
        except Exception as e:
            logger.error(f"Error initializing Helius client: {e}")
            sys.exit(1)  # Salir si no se puede inicializar Helius
        
        gmgn_client = None
        try:
            from gmgn_client import GMGNClient
            gmgn_client = GMGNClient()
            logger.info("✅ GMGN Client initialized")
        except Exception as e:
            logger.warning(f"GMGN Client initialization failed: {e}")
        
        dexscreener_client = None
        try:
            from dexscreener_client import DexScreenerClient
            dexscreener_client = DexScreenerClient()
            logger.info("✅ DexScreener Client initialized")
        except Exception as e:
            logger.warning(f"DexScreener Client initialization failed: {e}")
        
        # Inicializar nuevos módulos de análisis
        dex_monitor = DexMonitor()
        logger.info("✅ DEX Monitor initialized")
        
        whale_detector = WhaleDetector(helius_client=helius_client)
        logger.info("✅ Whale Detector initialized")
        
        market_metrics = MarketMetricsAnalyzer(
            helius_client=helius_client, 
            dexscreener_client=dexscreener_client
        )
        logger.info("✅ Market Metrics Analyzer initialized")
        
        token_analyzer = TokenAnalyzer(token_data_service=helius_client)
        logger.info("✅ Token Analyzer initialized")
        
        trader_profiler = TraderProfiler(scoring_system=scoring_system)
        logger.info("✅ Trader Profiler initialized")
        
        # Inicializar lógica de señales con todos los nuevos módulos
        signal_logic = SignalLogic(
            scoring_system=scoring_system, 
            helius_client=helius_client, 
            gmgn_client=gmgn_client
        )
        signal_logic.wallet_tracker = wallet_tracker
        signal_logic.whale_detector = whale_detector
        signal_logic.market_metrics = market_metrics
        signal_logic.token_analyzer = token_analyzer
        signal_logic.trader_profiler = trader_profiler
        signal_logic.dex_monitor = dex_monitor
        if dexscreener_client:
            signal_logic.dexscreener_client = dexscreener_client
        
        # Inicializar Performance Tracker con todos los servicios disponibles
        performance_tracker = PerformanceTracker(
            token_data_service=helius_client,
            dex_monitor=dex_monitor,
            market_metrics=market_metrics,
            whale_detector=whale_detector
        )
        signal_logic.performance_tracker = performance_tracker
        
        # Inicializar monitor de scalpers
        scalper_monitor = ScalperActivityMonitor()
        
        # Iniciar el bot de Telegram
        telegram_commands_function = fix_telegram_commands()
        telegram_commands_function(
            Config.TELEGRAM_BOT_TOKEN, 
            Config.TELEGRAM_CHAT_ID, 
            signal_logic
        )
        
        # Enviar mensaje de inicio
        send_telegram_message("🚀 *Trading Bot Started*\nMonitoring Solana transactions with enhanced analysis...")
        
        # Lista de tareas asíncronas a ejecutar
        tasks = [
            # Tarea de procesamiento de candidatos
            asyncio.create_task(signal_logic._process_candidates()),
            
            # Tarea de limpieza periódica de datos
            asyncio.create_task(cleanup_data_periodically([
                whale_detector, 
                market_metrics, 
                token_analyzer, 
                trader_profiler, 
                dex_monitor, 
                scalper_monitor
            ])),
            
            # Monitoreo de salud del sistema
            asyncio.create_task(monitor_system_health())
        ]
        
        # Inicializar cliente WebSocket de Cielo
        cielo_client = CieloAPI(Config.CIELO_API_KEY)
        cielo_message_handler = fix_on_cielo_message()
        
        # Tarea para conexión WebSocket a Cielo
        tasks.append(asyncio.create_task(
            cielo_client.run_forever_wallets(
                wallets,
                lambda message: cielo_message_handler(
                    message, wallet_tracker, scoring_system, signal_logic, scalper_monitor,
                    # Pasar nuevos módulos al handler
                    trader_profiler=trader_profiler,
                    whale_detector=whale_detector
                ),
                {"chains": ["solana"], "tx_types": ["swap", "transfer"]}
            )
        ))
        
        logger.info(f"✅ Bot started with {len(tasks)} tasks")
        
        # Bucle principal de supervisión
        while bot_running:
            # Comprobar el estado de las tareas
            for i, task in enumerate(tasks):
                if task.done():
                    try:
                        err = task.exception()
                        if err:
                            logger.error(f"Task #{i} failed: {err}")
                            # Reiniciar la tarea fallida según su tipo
                            if i == 0:  # Procesamiento de candidatos
                                tasks[i] = asyncio.create_task(signal_logic._process_candidates())
                                logger.info("Signal processing task restarted")
                            elif i == 1:  # Limpieza de datos
                                tasks[i] = asyncio.create_task(cleanup_data_periodically([
                                    whale_detector, market_metrics, token_analyzer, 
                                    trader_profiler, dex_monitor, scalper_monitor
                                ]))
                                logger.info("Cleanup task restarted")
                            elif i == 2:  # Monitoreo del sistema
                                tasks[i] = asyncio.create_task(monitor_system_health())
                                logger.info("System monitoring task restarted")
                            elif i == 3:  # WebSocket Cielo
                                tasks[i] = asyncio.create_task(
                                    cielo_client.run_forever_wallets(
                                        wallets,
                                        lambda message: cielo_message_handler(
                                            message, wallet_tracker, scoring_system, signal_logic, scalper_monitor,
                                            trader_profiler=trader_profiler,
                                            whale_detector=whale_detector
                                        ),
                                        {"chains": ["solana"], "tx_types": ["swap", "transfer"]}
                                    )
                                )
                                logger.info("Cielo WebSocket task restarted")
                    except Exception as e:
                        logger.error(f"Error checking task #{i}: {e}", exc_info=True)
            
            # Mostrar estado del bot
            candidates_count = signal_logic.get_active_candidates_count()
            signals_today = db.count_signals_today()
            cache_stats = db.get_cache_stats()
            
            logger.info(
                f"Bot status: {candidates_count} tokens monitored, "
                f"{signals_today} signals today, "
                f"DB cache: {cache_stats.get('hit_ratio', 0):.2f} hit ratio"
            )
            
            # Esperar antes de la siguiente comprobación
            await asyncio.sleep(30)
            
    except Exception as e:
        logger.critical(f"Critical error in main: {e}", exc_info=True)
        send_telegram_message(f"⚠️ *Critical error*: Bot stopped: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Registrar manejadores de señales para salida adecuada
    signal.signal(signal.SIGTERM, cleanup_and_exit)
    signal.signal(signal.SIGINT, cleanup_and_exit)
    
    # Ejecutar el bucle de eventos asíncrono
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
        cleanup_and_exit()
    finally:
        loop.close()
