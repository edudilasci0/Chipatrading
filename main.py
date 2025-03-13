import os
import sys
import time
import signal
import asyncio
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("chipatrading")

from config import Config
from wallet_tracker import WalletTracker
from cielo_api import CieloAPI
from scoring import ScoringSystem
# Eliminamos la importación de RugCheckAPI
from signal_logic import SignalLogic
from performance_tracker import PerformanceTracker
from telegram_utils import send_telegram_message, process_telegram_commands
from scalper_monitor import ScalperActivityMonitor  # Módulo nuevo
import db

bot_running = True

async def on_cielo_message(message, wallet_tracker, scoring_system, signal_logic, scalper_monitor):
    try:
        # Log informativo del mensaje recibido
        logger.debug(f"Mensaje recibido: {message[:100]}...")
        # Importación local para usar json sin modificar los imports globales
        import json
        data = json.loads(message)
        msg_type = data.get("type", "desconocido")
        logger.info(f"Procesando mensaje de tipo: {msg_type}")
        if msg_type == "tx" and "data" in data:
            tx_data = data["data"]
            logger.debug(f"Datos de transacción: {tx_data}")
            # Enviar la transacción a SignalLogic para procesar la señal
            signal_logic.process_transaction(tx_data)
            # Actualizar scalper monitor si corresponde
            scalper_monitor.process_transaction(tx_data)
            logger.info("Transacción procesada en on_cielo_message")
        else:
            logger.debug("Mensaje no procesable (no es 'tx') o sin datos")
    except Exception as e:
        logger.error(f"Error en on_cielo_message: {e}", exc_info=True)

def adaptive_signal_check():
    """
    Función para realizar una verificación adaptativa de señales según la actividad.
    (Implementa aquí la lógica adaptativa basada en métricas del mercado)
    """
    pass

def send_early_alpha_alert(signal_info):
    """
    Envía alerta específica para tokens en fase Early Alpha.
    """
    message = f"🚨 *Early Alpha Alert*\nToken: `{signal_info['token']}`\nConfianza: `{signal_info['confidence']:.2f}`\n..."
    send_telegram_message(message)

def send_daily_runner_alert(signal_info):
    """
    Envía alerta para Daily Runners de alta calidad.
    """
    message = f"🔥 *Daily Runner Alert*\nToken: `{signal_info['token']}`\nConfianza: `{signal_info['confidence']:.2f}`\n..."
    send_telegram_message(message)

async def cleanup_discoveries_periodically(scalper_monitor):
    """
    Tarea periódica para limpiar descubrimientos antiguos en el ScalperActivityMonitor.
    """
    while bot_running:
        try:
            emerging = scalper_monitor.get_emerging_tokens()
            logger.info(f"Tokens emergentes detectados: {len(emerging)}")
        except Exception as e:
            logger.error(f"Error en cleanup_discoveries_periodically: {e}", exc_info=True)
        await asyncio.sleep(1800)

async def main():
    global bot_running
    try:
        # Mensajes de inicio en consola
        print("\n==== INICIANDO TRADING BOT ====")
        print(f"Fecha/hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        Config.check_required_config()
        db.init_db()
        
        # Cargar wallets para monitoreo
        wallet_tracker = WalletTracker()
        wallets = wallet_tracker.get_wallets()
        print(f"✅ Cargadas {len(wallets)} wallets para monitoreo")
        logger.info(f"Wallets cargadas: {wallets}")
        
        # Inicializar servicios
        scoring_system = ScoringSystem()
        # Eliminamos la inicialización de RugCheckAPI
        # rugcheck_api = RugCheckAPI()
        # rugcheck_api.authenticate()
        
        helius_client = None
        if Config.HELIUS_API_KEY:
            from helius_client import HeliusClient
            helius_client = HeliusClient(Config.HELIUS_API_KEY)
            logger.info("✅ Cliente Helius inicializado")
        
        # Opcional: Inicializar GMGN si está configurado
        gmgn_client = None
        try:
            from gmgn_client import GMGNClient
            gmgn_client = GMGNClient()
            logger.info("✅ Cliente GMGN inicializado")
        except Exception as e:
            logger.warning(f"No se pudo inicializar cliente GMGN: {e}")
        
        # Inicializar módulos principales
        signal_logic = SignalLogic(
            scoring_system=scoring_system, 
            helius_client=helius_client, 
            gmgn_client=gmgn_client
            # No se pasa rugcheck_api
        )
        performance_tracker = PerformanceTracker(token_data_service=helius_client)
        signal_logic.performance_tracker = performance_tracker
        scalper_monitor = ScalperActivityMonitor()
        
        # Iniciar bot de Telegram
        is_bot_active = await process_telegram_commands(Config.TELEGRAM_BOT_TOKEN, Config.TELEGRAM_CHAT_ID, signal_logic)
        
        # Notificar inicio vía Telegram
        send_telegram_message("🚀 *Trading Bot Iniciado*\nMonitoreando transacciones en Solana...")
        
        # Crear tareas asíncronas
        tasks = [
            asyncio.create_task(signal_logic.check_signals_periodically()),
            asyncio.create_task(cleanup_discoveries_periodically(scalper_monitor))
        ]
        
        # Iniciar cliente Cielo para recibir mensajes
        cielo_client = CieloAPI(Config.CIELO_API_KEY)
        
        async def process_cielo(message):
            try:
                await on_cielo_message(message, wallet_tracker, scoring_system, signal_logic, scalper_monitor)
            except Exception as e:
                logger.error(f"Error en process_cielo: {e}", exc_info=True)
        
        cielo_task = asyncio.create_task(
            cielo_client.run_forever_wallets(
                wallets, 
                process_cielo, 
                {"chains": ["solana"], "tx_types": ["swap", "transfer"]}
            )
        )
        tasks.append(cielo_task)
        
        logger.info(f"✅ Bot iniciado y funcionando con {len(tasks)} tareas")
        
        # Bucle principal para vigilar tareas y generar logs informativos
        while bot_running:
            for i, task in enumerate(tasks):
                if task.done():
                    try:
                        err = task.exception()
                        if err:
                            logger.error(f"Tarea #{i} falló: {err}")
                            # Reiniciar tareas fallidas según su índice
                            if i == 0:
                                tasks[i] = asyncio.create_task(signal_logic.check_signals_periodically())
                                logger.info("Tarea de verificación de señales reiniciada")
                            elif i == 1:
                                tasks[i] = asyncio.create_task(cleanup_discoveries_periodically(scalper_monitor))
                                logger.info("Tarea de limpieza reiniciada")
                            elif i == 2:
                                tasks[i] = asyncio.create_task(cielo_client.run_forever_wallets(
                                    wallets, 
                                    process_cielo, 
                                    {"chains": ["solana"], "tx_types": ["swap", "transfer"]}
                                ))
                                logger.info("Tarea de WebSocket Cielo reiniciada")
                    except Exception as e:
                        logger.error(f"Error verificando tarea #{i}: {e}", exc_info=True)
            
            logger.info(f"Estado del bot: {len(signal_logic.token_candidates)} tokens monitoreados, {db.count_signals_today()} señales hoy")
            await asyncio.sleep(30)
            
    except Exception as e:
        logger.error(f"Error crítico en main: {e}", exc_info=True)
        send_telegram_message(f"⚠️ *Error Crítico*: El bot se ha detenido: {e}")
        sys.exit(1)

if __name__ == "__main__":
    signal.signal(signal.SIGTERM, lambda s, f: sys.exit(0))
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("Bot detenido por el usuario")
    finally:
        loop.close()
