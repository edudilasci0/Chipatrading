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
from cielo_api import HeliusClient, CieloAPI
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
            # Aquí podrías agregar lógica de limpieza específica (si la deseas)
            await asyncio.sleep(interval)
        except Exception as e:
            logger.error(f"Error in cleanup_discoveries: {e}")
            await asyncio.sleep(60)

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
        logger.info(f"Wallets loaded: {wallets}")
        
        scoring_system = ScoringSystem()
        helius_client = None
        if Config.HELIUS_API_KEY:
            helius_client = HeliusClient(Config.HELIUS_API_KEY)
            logger.info("✅ Cliente Helius inicializado")
        
        gmgn_client = None
        try:
            from gmgn_client import GMGNClient
            gmgn_client = GMGNClient()
            logger.info("✅ Cliente GMGN inicializado")
        except Exception as e:
            logger.warning(f"GMGN client not initialized: {e}")
        
        signal_logic = SignalLogic(
            scoring_system=scoring_system,
            helius_client=helius_client,
            gmgn_client=gmgn_client
        )
        signal_logic.wallet_tracker = wallet_tracker
        signal_logic.compute_confidence = optimize_signal_confidence().__get__(signal_logic, SignalLogic)
        signal_logic.detect_emerging_alpha_tokens = enhance_alpha_detection().__get__(signal_logic, SignalLogic)
        
        performance_tracker = PerformanceTracker(token_data_service=helius_client)
        signal_logic.performance_tracker = performance_tracker
        
        scalper_monitor = ScalperActivityMonitor()
        
        telegram_commands = fix_telegram_commands()
        is_bot_active = await telegram_commands(Config.TELEGRAM_BOT_TOKEN, Config.TELEGRAM_CHAT_ID, signal_logic)
        
        send_telegram_message("🚀 *Trading Bot Iniciado*\nMonitoreando transacciones en Solana...")
        
        tasks = [
            asyncio.create_task(signal_logic.check_signals_periodically()),
            asyncio.create_task(cleanup_discoveries_periodically(scalper_monitor)),
        ]
        
        cielo_api_instance = CieloAPI(Config.CIELO_API_KEY)
        cielo_message_handler = fix_on_cielo_message(wallet_tracker, scoring_system, signal_logic, scalper_monitor)
        cielo_task = asyncio.create_task(
            cielo_api_instance.run_forever_wallets(
                wallets,
                cielo_message_handler,
                {"chains": ["solana"], "tx_types": ["swap", "transfer"]}
            )
        )
        tasks.append(cielo_task)
        
        logger.info(f"✅ Bot iniciado and running with {len(tasks)} tasks")
        
        while bot_running:
            for i, task in enumerate(tasks):
                if task.done():
                    try:
                        err = task.exception()
                        if err:
                            logger.error(f"Task #{i} failed: {err}")
                            if i == 0:
                                tasks[i] = asyncio.create_task(signal_logic.check_signals_periodically())
                                logger.info("Signal check task restarted")
                            elif i == 1:
                                tasks[i] = asyncio.create_task(cleanup_discoveries_periodically(scalper_monitor))
                                logger.info("Cleanup task restarted")
                            elif i == 2:
                                tasks[i] = asyncio.create_task(
                                    cielo_api_instance.run_forever_wallets(
                                        wallets,
                                        cielo_message_handler,
                                        {"chains": ["solana"], "tx_types": ["swap", "transfer"]}
                                    )
                                )
                                logger.info("Cielo WebSocket task restarted")
                    except Exception as e:
                        logger.error(f"Error checking task #{i}: {e}", exc_info=True)
            logger.info(f"Bot state: {len(signal_logic.token_candidates)} tokens monitored, {db.count_signals_today()} signals today")
            await asyncio.sleep(30)
            
    except Exception as e:
        logger.error(f"Critical error in main: {e}", exc_info=True)
        send_telegram_message(f"⚠️ *Critical Error*: The bot has stopped: {e}")
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
