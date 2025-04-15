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
from dexscreener_client import DexScreenerClient
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

async def run_heartbeat(transaction_manager, interval=300):
    start_time = time.time()
    while not shutdown_flag:
        try:
            stats = {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "uptime": int(time.time() - start_time),
                "db_cache": db.get_cache_stats(),
                "signal_count": db.count_signals_today(),
                "transaction_count": db.count_transactions_today(),
                "tx_rate": transaction_manager.tx_counts["last_minute"] if hasattr(transaction_manager, "tx_counts") else 0,
                "tx_processed": transaction_manager.tx_counts["processed"] if hasattr(transaction_manager, "tx_counts") else 0
            }
            tx_rate_str = f"Rate: {stats['tx_rate']}/min" if "tx_rate" in stats and stats["tx_rate"] else ""
            logger.info(f"❤️ Heartbeat | Señales hoy: {stats['signal_count']} | Transacciones: {stats['transaction_count']} | {tx_rate_str}")
            if (hasattr(transaction_manager, "tx_counts") and 
                transaction_manager.tx_counts["total"] > 0 and 
                transaction_manager.tx_counts["last_minute"] == 0):
                logger.warning("⚠️ Sin transacciones recientes. Podría haber un problema de conexión con Cielo.")
            await asyncio.sleep(interval)
        except Exception as e:
            logger.error(f"Error en heartbeat: {e}")
            await asyncio.sleep(60)

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

async def run_delayed_diagnostics(transaction_manager):
    """Ejecuta diagnósticos después de un tiempo de inicialización"""
    try:
        # Esperar a que el sistema se inicialice completamente
        await asyncio.sleep(30)
        
        logger.info("🔍 Ejecutando diagnósticos de transacciones...")
        
        # Verificar si tenemos un adaptador de Cielo configurado
        if transaction_manager and transaction_manager.cielo_adapter:
            # Ejecutar diagnósticos completos
            logger.info("Ejecutando diagnósticos completos de Cielo...")
            diagnostics = await transaction_manager.cielo_adapter.run_diagnostics()
            
            # Registrar estadísticas de transacciones
            tx_stats = diagnostics['transactions']
            logger.info(f"Estadísticas de transacciones: Total={tx_stats['total']}, Procesadas={tx_stats['processed']}, Errores={tx_stats['errors']}")
            
            # Verificar últimas transacciones si existen
            if 'last_transactions' in tx_stats and tx_stats['last_transactions']:
                logger.info(f"Últimas transacciones recibidas: {len(tx_stats['last_transactions'])}")
                for idx, tx in enumerate(tx_stats['last_transactions']):
                    logger.info(f"  Transacción #{idx+1}: {tx['type']} {tx['token']} por ${tx['amount']:.2f} desde {tx['wallet']}")
            else:
                logger.info("No se han detectado transacciones recientes")
            
            # Intentar simular una transacción para verificar procesamiento
            logger.info("Simulando transacción para verificar procesamiento...")
            simulation_result = await transaction_manager.cielo_adapter.simulate_transaction()
            logger.info(f"Simulación de transacción: {'✅ Exitosa' if simulation_result else '❌ Fallida'}")
            
            # Verificar estados de conexión
            conn_status = "✅ Conectado" if transaction_manager.cielo_adapter.is_connected() else "❌ Desconectado"
            logger.info(f"Estado de conexión: {conn_status}")
            
            # Verificar estado de suscripciones
            sub_status = transaction_manager.cielo_adapter.check_subscription_status()
            logger.info(f"Estado de suscripciones: Solicitadas={sub_status['total_requested']}, Confirmadas={sub_status['confirmed']}, Pendientes={sub_status['pending']}")
            
            if sub_status['pending'] > 0:
                logger.warning(f"⚠️ Hay {sub_status['pending']} wallets sin confirmar la suscripción")
                if sub_status['missing_wallets']:
                    logger.warning(f"Ejemplos de wallets sin confirmar: {', '.join(sub_status['missing_wallets'][:5])}")
            
            logger.info("🔍 Diagnósticos completados")
        else:
            logger.warning("⚠️ No se puede ejecutar diagnóstico - Adaptador Cielo no disponible")
            
    except Exception as e:
        logger.error(f"❌ Error ejecutando diagnósticos: {e}", exc_info=True)

async def main():
    global shutdown_flag
    try:
        start_time = time.time()
        setup_signal_handlers()
        
        # Inicializar base de datos
        if not db.init_db():
            logger.critical("No se pudo inicializar la base de datos. Abortando.")
            return 1
            
        # Verificar configuración
        Config.check_required_config()
        logger.info("🔄 Inicializando componentes...")
        
        # Inicializar cliente DexScreener
        dexscreener_client = DexScreenerClient()
        
        # Inicializar componentes principales
        signal_logic = SignalLogic(dexscreener_client=dexscreener_client)
        wallet_manager = WalletManager()
        wallet_tracker = WalletTracker()
        transaction_manager = TransactionManager(
            signal_logic=signal_logic,
            wallet_tracker=wallet_tracker,
            wallet_manager=wallet_manager
        )
        
        # Configurar componentes
        components = {
            "dexscreener_client": dexscreener_client,
            "signal_logic": signal_logic,
            "wallet_manager": wallet_manager,
            "wallet_tracker": wallet_tracker,
            "transaction_manager": transaction_manager,
            "risk_manager": signal_logic.risk_manager  # Añadir Risk Manager a componentes
        }
        
        logger.info("✅ Todos los componentes inicializados correctamente")
        
        # Iniciar tareas principales
        tasks = [
            asyncio.create_task(transaction_manager.start()),
            asyncio.create_task(periodic_maintenance(components)),
            asyncio.create_task(run_heartbeat(transaction_manager))
        ]
        
        # Esperar a que todas las tareas terminen o se reciba señal de shutdown
        await asyncio.gather(*tasks)
        
        return 0
        
    except Exception as e:
        logger.critical(f"Error crítico en main: {e}", exc_info=True)
        return 1
    finally:
        await cleanup_resources(components)

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
