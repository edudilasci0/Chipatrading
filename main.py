#!/usr/bin/env python3
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
from gmgn_client import GMGNClient
from dexscreener_client import DexScreenerClient

# Componentes principales
from wallet_tracker import WalletTracker
from scoring import ScoringSystem
from signal_logic import SignalLogic
from performance_tracker import PerformanceTracker
from scalper_monitor import ScalperActivityMonitor
from rugcheck import RugCheckAPI

# Componentes avanzados
from dex_monitor import DexMonitor
from market_metrics import MarketMetricsAnalyzer
from token_analyzer import TokenAnalyzer
from trader_profiler import TraderProfiler
from whale_detector import WhaleDetector
from signal_predictor import SignalPredictor

# Utilidades
from telegram_utils import fix_telegram_commands, fix_on_cielo_message
import telegram_utils

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
    
    # Cerrar conexiones HTTP/WebSocket
    for name, component in components.items():
        if hasattr(component, 'close_session'):
            try:
                await component.close_session()
                logger.info(f"Conexión cerrada para {name}")
            except Exception as e:
                logger.error(f"Error cerrando {name}: {e}")
        
        # Detener tareas en segundo plano
        if hasattr(component, 'stop_cleanup') and callable(component.stop_cleanup):
            try:
                component.stop_cleanup()
                logger.info(f"Tareas de limpieza detenidas para {name}")
            except Exception as e:
                logger.error(f"Error deteniendo tareas de {name}: {e}")
    
    # Cerrar pool de base de datos
    try:
        if hasattr(db, 'pool') and db.pool:
            db.pool.closeall()
            logger.info("Conexiones de BD cerradas")
    except Exception as e:
        logger.error(f"Error cerrando pool de BD: {e}")

async def periodic_maintenance(components):
    """Realiza mantenimiento periódico de caché y limpieza de datos"""
    try:
        cleanup_interval = int(Config.get("CACHE_CLEANUP_INTERVAL", 3600))  # 1 hora por defecto
        
        while not shutdown_flag:
            # Esperar antes de la primera ejecución
            await asyncio.sleep(cleanup_interval)
            
            if shutdown_flag:
                break
                
            logger.info("🧹 Iniciando mantenimiento periódico...")
            
            start_time = time.time()
            
            # Limpiar datos antiguos en cada componente
            for name, component in components.items():
                if hasattr(component, 'cleanup_old_data') and callable(component.cleanup_old_data):
                    try:
                        component.cleanup_old_data()
                    except Exception as e:
                        logger.error(f"Error en limpieza de {name}: {e}")
            
            # Limpiar caché de consultas
            db.clear_query_cache()
            
            # Recargar configuración dinámica
            Config.load_dynamic_config()
            
            duration = time.time() - start_time
            logger.info(f"✅ Mantenimiento completado en {duration:.2f}s")
            
    except Exception as e:
        logger.error(f"Error en tarea de mantenimiento: {e}")

async def process_pending_signals():
    """Procesa señales pendientes al iniciar"""
    try:
        # Obtener señales recientes sin seguimiento completo
        recent_signals = db.get_recent_untracked_signals(hours=24)
        
        if not recent_signals:
            logger.info("No hay señales pendientes para procesar")
            return
        
        logger.info(f"Procesando {len(recent_signals)} señales pendientes...")
        
        # Sería ideal tener la instancia de PerformanceTracker aquí, pero
        # como esto es una función separada, se manejará desde el main
        
        return recent_signals
    except Exception as e:
        logger.error(f"Error procesando señales pendientes: {e}")
        return []

async def run_heartbeat(interval=300):
    """Envía señales de heartbeat periódicas"""
    while not shutdown_flag:
        try:
            # Recopilar estadísticas
            stats = {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "uptime": int(time.time() - start_time),
                "db_cache": db.get_cache_stats(),
                "signal_count": db.count_signals_today(),
                "transaction_count": db.count_transactions_today()
            }
            
            logger.info(f"❤️ Heartbeat | Señales hoy: {stats['signal_count']} | Transacciones: {stats['transaction_count']}")
            
            # Opcional: guardar estadísticas en BD
            
            await asyncio.sleep(interval)
        except Exception as e:
            logger.error(f"Error en heartbeat: {e}")
            await asyncio.sleep(60)  # Intervalo más corto en caso de error

async def main():
    global shutdown_flag, start_time
    
    try:
        # Registrar tiempo de inicio
        start_time = time.time()
        
        # Configurar manejadores de señales
        setup_signal_handlers()
        
        # Inicializar base de datos
        db_ready = db.init_db()
        if not db_ready:
            logger.critical("No se pudo inicializar la base de datos. Abortando.")
            return 1
        
        # Verificar configuración requerida
        Config.check_required_config()
        
        # Inicializar componentes
        logger.info("🔄 Inicializando componentes...")
        
        # Servicios externos
        helius_client = HeliusClient(Config.HELIUS_API_KEY)
        gmgn_client = GMGNClient()
        dexscreener_client = DexScreenerClient()
        rugcheck_api = RugCheckAPI()
        
        # Conectar cliente DexScreener a Helius como fallback
        helius_client.dexscreener_client = dexscreener_client
        
        # Inicializar componentes principales
        wallet_tracker = WalletTracker()
        scoring_system = ScoringSystem()
        signal_predictor = SignalPredictor()
        
        # Componentes avanzados
        dex_monitor = DexMonitor()
        whale_detector = WhaleDetector(helius_client=helius_client)
        market_metrics = MarketMetricsAnalyzer(helius_client=helius_client, dexscreener_client=dexscreener_client)
        token_analyzer = TokenAnalyzer(token_data_service=helius_client)
        trader_profiler = TraderProfiler(scoring_system=scoring_system)
        
        # Monitores y detectores
        scalper_monitor = ScalperActivityMonitor()
        performance_tracker = PerformanceTracker(
            token_data_service=helius_client,
            dex_monitor=dex_monitor,
            market_metrics=market_metrics,
            whale_detector=whale_detector
        )
        
        # Añadir token_analyzer al performance_tracker para análisis técnico
        performance_tracker.token_analyzer = token_analyzer
        
        # Inicializar la lógica central de señales
        signal_logic = SignalLogic(
            scoring_system=scoring_system,
            helius_client=helius_client,
            gmgn_client=gmgn_client,
            rugcheck_api=rugcheck_api,
            ml_predictor=signal_predictor,
            wallet_tracker=wallet_tracker
        )
        
        # Conectar componentes avanzados a la lógica de señales
        signal_logic.whale_detector = whale_detector
        signal_logic.market_metrics = market_metrics
        signal_logic.token_analyzer = token_analyzer
        signal_logic.trader_profiler = trader_profiler
        signal_logic.dex_monitor = dex_monitor
        
        # Añadir referencia cruzada al performance_tracker
        signal_logic.performance_tracker = performance_tracker
        
        # Registro y estado
        logger.info("✅ Todos los componentes inicializados correctamente")
        
        # Recopilar componentes para mantenimiento y limpieza
        components = {
            "helius_client": helius_client,
            "dexscreener_client": dexscreener_client,
            "gmgn_client": gmgn_client,
            "dex_monitor": dex_monitor,
            "whale_detector": whale_detector,
            "market_metrics": market_metrics,
            "token_analyzer": token_analyzer,
            "trader_profiler": trader_profiler,
            "scoring_system": scoring_system,
            "signal_logic": signal_logic,
            "performance_tracker": performance_tracker,
            "scalper_monitor": scalper_monitor
        }
        
        # Iniciar tarea de mantenimiento periódico
        maintenance_task = asyncio.create_task(periodic_maintenance(components))
        
        # Iniciar heartbeat
        heartbeat_task = asyncio.create_task(run_heartbeat())
        
        # Procesar señales pendientes
        pending_signals = await process_pending_signals()
        for signal in pending_signals:
            token = signal.get("token")
            if token:
                performance_tracker.add_signal(token, signal)
        
        # Iniciar bot de Telegram
        telegram_process_commands = fix_telegram_commands()
        
        if Config.TELEGRAM_BOT_TOKEN and Config.TELEGRAM_CHAT_ID:
            telegram_task = asyncio.create_task(
                telegram_process_commands(
                    Config.TELEGRAM_BOT_TOKEN,
                    Config.TELEGRAM_CHAT_ID,
                    signal_logic
                )
            )
            logger.info("✅ Bot de Telegram inicializado")
        else:
            logger.warning("⚠️ Bot de Telegram no configurado")
            telegram_task = None
        
        # Preparar manejador de mensajes de Cielo
        on_cielo_message = fix_on_cielo_message()
        
        # Enviar mensaje de inicio
        logger.info("🚀 Bot iniciado correctamente, conectando a Cielo...")
        startup_message = (
            "🚀 *Bot Iniciado Correctamente*\n\n"
            f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"Versión: 1.0.0\n"
            f"Señales hoy: {db.count_signals_today()}\n"
            f"Transacciones hoy: {db.count_transactions_today()}\n\n"
            "Monitoreo de wallets activado."
        )
        telegram_utils.send_telegram_message(startup_message)
        
        # Obtener lista de wallets a seguir
        wallets_to_track = wallet_tracker.get_wallets()
        if not wallets_to_track:
            logger.error("❌ No hay wallets para seguir. Revisa traders_data.json")
            return 1
        
        logger.info(f"📋 Siguiendo {len(wallets_to_track)} wallets")
        
        # Iniciar cliente WebSocket y mantener conexión
        cielo_api = CieloAPI(api_key=Config.CIELO_API_KEY)
        
        # Conectar a Cielo en un bucle para manejar reconexiones
        while not shutdown_flag:
            try:
                await cielo_api.run_forever_wallets(
                    wallets=wallets_to_track,
                    on_message_callback=lambda message: on_cielo_message(
                        message,
                        wallet_tracker,
                        scoring_system,
                        signal_logic,
                        scalper_monitor
                    )
                )
            except Exception as e:
                logger.error(f"Error en conexión WebSocket: {e}")
                if not shutdown_flag:
                    # Esperar antes de reconectar para evitar spam de reconexiones
                    logger.info("🔄 Esperando para reconectar...")
                    await asyncio.sleep(15)
        
        # Esperar por tareas si es necesario
        if telegram_task:
            telegram_task.cancel()
        
        maintenance_task.cancel()
        heartbeat_task.cancel()
        
        # Limpiar recursos
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
