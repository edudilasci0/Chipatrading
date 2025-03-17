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
# Asegurarse de inicializar la configuración
Config.initialize()

from wallet_tracker import WalletTracker
from cielo_api import CieloAPI   # Este módulo se encarga de la conexión WebSocket a Cielo
from helius_client import HeliusClient  # Cliente para consultas a Helius
from scoring import ScoringSystem
from signal_logic import SignalLogic, optimize_signal_confidence, enhance_alpha_detection
from performance_tracker import PerformanceTracker
from telegram_utils import send_telegram_message, fix_telegram_commands, fix_on_cielo_message
from scalper_monitor import ScalperActivityMonitor
import db

bot_running = True

async def cleanup_discoveries_periodically(scalper_monitor, interval=3600):
    while True:
        try:
            # Aquí se podría implementar la lógica para limpiar datos antiguos
            await asyncio.sleep(interval)
        except Exception as e:
            logger.error(f"Error in cleanup_discoveries: {e}")
            await asyncio.sleep(60)

async def main():
    global bot_running
    try:
        print("\n==== STARTING TRADING BOT ====")
        print(f"Date/Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        Config.check_required_config()
        db.init_db()
        
        wallet_tracker = WalletTracker()
        wallets = wallet_tracker.get_wallets()
        logger.info(f"Wallets loaded: {len(wallets)}")
        
        scoring_system = ScoringSystem()
        helius_client = HeliusClient(Config.HELIUS_API_KEY)
        logger.info("✅ Helius Client initialized")
        
        gmgn_client = None
        try:
            from gmgn_client import GMGNClient
            gmgn_client = GMGNClient()
            logger.info("✅ GMGN Client initialized")
        except Exception as e:
            logger.warning(f"GMGN Client initialization failed: {e}")
        
        # Inicializar DexScreenerClient como respaldo (si está implementado)
        dexscreener_client = None
        try:
            from dexscreener_client import DexScreenerClient
            dexscreener_client = DexScreenerClient()
            logger.info("✅ DexScreener Client initialized")
        except Exception as e:
            logger.warning(f"DexScreener Client initialization failed: {e}")
        
        signal_logic = SignalLogic(scoring_system=scoring_system, helius_client=helius_client, gmgn_client=gmgn_client)
        signal_logic.wallet_tracker = wallet_tracker  # Asignar referencia a WalletTracker
        if dexscreener_client:
            signal_logic.dexscreener_client = dexscreener_client
        
        # Aplicar optimizaciones al objeto SignalLogic
        signal_logic.compute_confidence = optimize_signal_confidence().__get__(signal_logic, SignalLogic)
        signal_logic.detect_emerging_alpha_tokens = enhance_alpha_detection().__get__(signal_logic, SignalLogic)
        if not hasattr(signal_logic, 'get_active_candidates_count'):
            signal_logic.get_active_candidates_count = lambda: len(signal_logic.token_candidates)
        
        performance_tracker = PerformanceTracker(token_data_service=helius_client)
        signal_logic.performance_tracker = performance_tracker
        
        scalper_monitor = ScalperActivityMonitor()
        
        # Iniciar el bot de Telegram usando fix_telegram_commands (esta función inicia internamente el polling)
        telegram_commands_function = fix_telegram_commands()
        # Nota: No usamos 'await' aquí, ya que telegram_commands_function no es una coroutine.
        telegram_commands_function(Config.TELEGRAM_BOT_TOKEN, Config.TELEGRAM_CHAT_ID, signal_logic)
        
        send_telegram_message("🚀 *Trading Bot Started*\nMonitoring Solana transactions...")
        
        tasks = [
            asyncio.create_task(signal_logic._process_candidates()),
            asyncio.create_task(cleanup_discoveries_periodically(scalper_monitor))
        ]
        
        cielo_client = CieloAPI(Config.CIELO_API_KEY)
        cielo_message_handler = fix_on_cielo_message()
        # CORRECCIÓN PRINCIPAL: usar lambda para pasar correctamente todos los parámetros a on_cielo_message
        tasks.append(asyncio.create_task(
            cielo_client.run_forever_wallets(
                wallets,
                lambda message: cielo_message_handler(message, wallet_tracker, scoring_system, signal_logic, scalper_monitor),
                {"chains": ["solana"], "tx_types": ["swap", "transfer"]}
            )
        ))
        
        logger.info(f"✅ Bot started with {len(tasks)} tasks")
        
        while bot_running:
            for i, task in enumerate(tasks):
                if task.done():
                    try:
                        err = task.exception()
                        if err:
                            logger.error(f"Task #{i} failed: {err}")
                            if i == 0:
                                tasks[i] = asyncio.create_task(signal_logic._process_candidates())
                                logger.info("Signal processing task restarted")
                            elif i == 1:
                                tasks[i] = asyncio.create_task(cleanup_discoveries_periodically(scalper_monitor))
                                logger.info("Cleanup task restarted")
                            elif i == 2:
                                tasks[i] = asyncio.create_task(
                                    cielo_client.run_forever_wallets(
                                        wallets,
                                        lambda message: cielo_message_handler(message, wallet_tracker, scoring_system, signal_logic, scalper_monitor),
                                        {"chains": ["solana"], "tx_types": ["swap", "transfer"]}
                                    )
                                )
                                logger.info("Cielo WebSocket task restarted")
                    except Exception as e:
                        logger.error(f"Error checking task #{i}: {e}", exc_info=True)
            logger.info(f"Bot status: {len(signal_logic.token_candidates)} tokens monitored, {db.count_signals_today()} signals today")
            await asyncio.sleep(30)
            
    except Exception as e:
        logger.error(f"Critical error in main: {e}", exc_info=True)
        send_telegram_message(f"⚠️ *Critical error*: Bot stopped: {e}")
        sys.exit(1)

if __name__ == "__main__":
    signal.signal(signal.SIGTERM, lambda s, f: sys.exit(0))
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    finally:
        loop.close()
