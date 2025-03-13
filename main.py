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
from rugcheck import RugCheckAPI
from signal_logic import SignalLogic
from performance_tracker import PerformanceTracker
from telegram_utils import send_telegram_message, process_telegram_commands
from scalper_monitor import ScalperActivityMonitor  # Importación del nuevo módulo
import db

bot_running = True

async def on_cielo_message(message, wallet_tracker, scoring_system, signal_logic, scalper_monitor):
    try:
        data = json.loads(message)
        if data.get("type") == "tx" and "data" in data:
            tx_data = data["data"]
            # Procesamiento de transacciones de Cielo (swap, transfer, etc.)
            # [Código de procesamiento actual...]
            # Enviar transacción a signal_logic:
            signal_logic.process_transaction(tx_data)
            # Además, si la transacción proviene de un scalper conocido, actualizar scalper_monitor
            scalper_monitor.process_transaction(tx_data)
            # Guardar en BD, actualizar scores, etc.
        # Manejar otros tipos de mensajes...
    except Exception as e:
        logger.error(f"Error en on_cielo_message: {e}", exc_info=True)

def adaptive_signal_check():
    """
    Función para realizar una verificación adaptativa de señales según la actividad.
    """
    # [Implementar lógica adaptativa basada en la actividad del mercado y señales]
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
    Tarea periódica para limpiar descubrimientos antiguos en scalper_monitor.
    """
    while bot_running:
        try:
            # La limpieza interna ya se maneja en scalper_monitor; aquí se puede generar reportes o logs
            emerging = scalper_monitor.get_emerging_tokens()
            logger.info(f"Tokens emergentes detectados: {len(emerging)}")
        except Exception as e:
            logger.error(f"Error en cleanup_discoveries_periodically: {e}", exc_info=True)
        await asyncio.sleep(1800)

async def main():
    global bot_running
    try:
        Config.check_required_config()
        db.init_db()
        
        wallet_tracker = WalletTracker()
        scoring_system = ScoringSystem()
        rugcheck_api = RugCheckAPI()
        rugcheck_api.authenticate()
        helius_client = None
        if Config.HELIUS_API_KEY:
            from helius_client import HeliusClient
            helius_client = HeliusClient(Config.HELIUS_API_KEY)
            logger.info("✅ Cliente Helius inicializado")
        
        # Inicializar nuevos módulos
        signal_logic = SignalLogic(scoring_system=scoring_system, helius_client=helius_client, rugcheck_api=rugcheck_api)
        performance_tracker = PerformanceTracker(token_data_service=helius_client)
        signal_logic.performance_tracker = performance_tracker
        scalper_monitor = ScalperActivityMonitor()  # Inicialización del monitor de scalpers
        
        # Iniciar bot de Telegram
        is_bot_active = await process_telegram_commands(Config.TELEGRAM_BOT_TOKEN, Config.TELEGRAM_CHAT_ID, signal_logic)
        
        tasks = [
            asyncio.create_task(signal_logic.check_signals_periodically()),
            asyncio.create_task(cleanup_discoveries_periodically(scalper_monitor))
        ]
        
        wallets = wallet_tracker.get_wallets()
        cielo_client = CieloAPI(Config.CIELO_API_KEY)
        
        async def process_cielo(message):
            try:
                await on_cielo_message(message, wallet_tracker, scoring_system, signal_logic, scalper_monitor)
            except Exception as e:
                logger.error(f"Error en process_cielo: {e}", exc_info=True)
        
        cielo_task = asyncio.create_task(cielo_client.run_forever_wallets(wallets, process_cielo, {"chains": ["solana"], "tx_types": ["swap", "transfer"]}))
        tasks.append(cielo_task)
        
        while bot_running:
            # Verificar y reiniciar tareas fallidas
            for i, task in enumerate(tasks):
                if task.done():
                    try:
                        err = task.exception()
                        if err:
                            logger.error(f"Tarea #{i} falló: {err}")
                    except Exception as e:
                        logger.error(f"Error verificando tarea #{i}: {e}", exc_info=True)
            await asyncio.sleep(30)
    except Exception as e:
        logger.error(f"Error en main: {e}", exc_info=True)
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
