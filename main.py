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
# Se elimina la dependencia de RugCheckAPI
from signal_logic import SignalLogic
from performance_tracker import PerformanceTracker
from telegram_utils import send_telegram_message, process_telegram_commands
from scalper_monitor import ScalperActivityMonitor  # Módulo nuevo
import db

bot_running = True

async def on_cielo_message(message, wallet_tracker, scoring_system, signal_logic, scalper_monitor):
    try:
        # Importación local para usar json sin modificar los imports globales
        import json
        data = json.loads(message)
        msg_type = data.get("type", "desconocido")
        logger.info(f"Procesando mensaje de tipo: {msg_type}")
        if msg_type == "tx" and "data" in data:
            tx_data = data["data"]
            # Normalizar datos para compatibilidad
            normalized_tx = {}
            normalized_tx["wallet"] = tx_data.get("wallet")
            if "token1_address" in tx_data and tx_data.get("token1_address") not in ["native", "So11111111111111111111111111111111111111112"]:
                normalized_tx["token"] = tx_data.get("token1_address")
                normalized_tx["type"] = "BUY"
                normalized_tx["token_name"] = tx_data.get("token1_name", "Unknown")
                normalized_tx["token_symbol"] = tx_data.get("token1_symbol", "???")
                normalized_tx["amount_usd"] = float(tx_data.get("token1_amount_usd", 0))
            elif "token0_address" in tx_data and tx_data.get("token0_address") not in ["native", "So11111111111111111111111111111111111111112"]:
                normalized_tx["token"] = tx_data.get("token0_address")
                normalized_tx["type"] = "SELL"
                normalized_tx["token_name"] = tx_data.get("token0_name", "Unknown")
                normalized_tx["token_symbol"] = tx_data.get("token0_symbol", "???")
                normalized_tx["amount_usd"] = float(tx_data.get("token0_amount_usd", 0))
            elif "contract_address" in tx_data:
                normalized_tx["token"] = tx_data.get("contract_address")
                normalized_tx["type"] = "TRANSFER"
                normalized_tx["token_name"] = tx_data.get("name", "Unknown")
                normalized_tx["token_symbol"] = tx_data.get("symbol", "???")
                normalized_tx["amount_usd"] = float(tx_data.get("amount_usd", 0))
            else:
                logger.warning("No se pudo determinar el token para la transacción")
                return

            normalized_tx["timestamp"] = tx_data.get("timestamp", int(time.time()))
            logger.info(f"Transacción normalizada: {normalized_tx['token']} | {normalized_tx['type']} | ${normalized_tx['amount_usd']:.2f}")

            # Procesar la transacción en SignalLogic y ScalperMonitor
            signal_logic.process_transaction(normalized_tx)
            scalper_monitor.process_transaction(normalized_tx)
        elif msg_type not in ["wallet_subscribed", "pong"]:
            logger.debug(f"Mensaje de tipo {msg_type} no procesado")
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
        print("\n==== INICIANDO TRADING BOT ====")
        print(f"Fecha/hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        Config.check_required_config()
        db.init_db()
        
        wallet_tracker = WalletTracker()
        wallets = wallet_tracker.get_wallets()
        print(f"✅ Cargadas {len(wallets)} wallets para monitoreo")
        logger.info(f"Wallets cargadas: {wallets}")
        
        scoring_system = ScoringSystem()
        # Se elimina RugCheckAPI
        helius_client = None
        if Config.HELIUS_API_KEY:
            from helius_client import HeliusClient
            helius_client = HeliusClient(Config.HELIUS_API_KEY)
            logger.info("✅ Cliente Helius inicializado")
        
        gmgn_client = None
        try:
            from gmgn_client import GMGNClient
            gmgn_client = GMGNClient()
            logger.info("✅ Cliente GMGN inicializado")
        except Exception as e:
            logger.warning(f"No se pudo inicializar cliente GMGN: {e}")
        
        signal_logic = SignalLogic(
            scoring_system=scoring_system, 
            helius_client=helius_client, 
            gmgn_client=gmgn_client
        )
        performance_tracker = PerformanceTracker(token_data_service=helius_client)
        signal_logic.performance_tracker = performance_tracker
        scalper_monitor = ScalperActivityMonitor()
        
        is_bot_active = await process_telegram_commands(Config.TELEGRAM_BOT_TOKEN, Config.TELEGRAM_CHAT_ID, signal_logic)
        send_telegram_message("🚀 *Trading Bot Iniciado*\nMonitoreando transacciones en Solana...")
        
        tasks = [
            asyncio.create_task(signal_logic.check_signals_periodically()),
            asyncio.create_task(cleanup_discoveries_periodically(scalper_monitor))
        ]
        
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
        
        while bot_running:
            for i, task in enumerate(tasks):
                if task.done():
                    try:
                        err = task.exception()
                        if err:
                            logger.error(f"Tarea #{i} falló: {err}")
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
