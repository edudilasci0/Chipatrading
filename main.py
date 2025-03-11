import asyncio
import json
import os
import sys
import time
import signal
import logging
import threading
from datetime import datetime, timedelta

import db
from config import Config
from cielo_api import CieloAPI
from scoring import ScoringSystem
from signal_logic import SignalLogic
from telegram_utils import send_telegram_message, process_telegram_commands
from performance_tracker import PerformanceTracker
from ml_preparation import MLDataPreparation
from signal_predictor import SignalPredictor
from rugcheck import RugCheckAPI
from gmgn_client import GMGNClient  # Nuevo cliente GMGN

def setup_logging():
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "chipatrading.log")
    log_level = getattr(logging, Config.get("LOG_LEVEL", "INFO"))
    logger = logging.getLogger("chipatrading")
    logger.setLevel(log_level)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_format)
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(log_level)
    file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_format)
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger

logger = setup_logging()

wallets_list = []
running = True
signal_predictor = None
ml_data_preparation = None
is_bot_active = None
cielo_ws_connection = None
message_counter = 0
transaction_counter = 0
last_counter_log = time.time()
last_heartbeat = time.time()
performance_stats = {
    "start_time": time.time(),
    "signals_emitted": 0,
    "transactions_processed": 0,
    "errors": 0,
    "last_error": None,
    "memory_usage": 0
}
stats_lock = threading.Lock()
wallet_cache = {"last_update": 0, "wallets": []}

def load_wallets():
    file_path = "traders_data.json"
    try:
        with open(file_path, "r") as f:
            data = json.load(f)
            wallets = [entry["Wallet"] for entry in data]
            return wallets
    except Exception as e:
        logger.error(f"Error cargando wallets: {e}")
        return []

async def send_boot_sequence():
    boot_messages = [
        "**🚀 Iniciando ChipaTrading Bot**\nPreparando servicios y verificaciones...",
        "**📡 Módulos de Monitoreo Activados**\nEscaneando wallets definidas para transacciones relevantes...",
        "**📊 Cargando Parámetros de Mercado**\nConectando con Helius y GMGN para datos de mercado...",
        "**🔒 Verificando Seguridad**\nConectando con RugCheck para validar tokens...",
        "**⚙️ Inicializando Lógica de Señales**\nConfigurando reglas de scoring y agrupación de traders...",
        "**✅ Sistema Operativo**\nListo para monitorear transacciones y generar alertas."
    ]
    for msg in boot_messages:
        send_telegram_message(msg)
        await asyncio.sleep(2)

async def monitoring_task():
    global message_counter, transaction_counter, last_counter_log, last_heartbeat, running
    while running:
        try:
            await asyncio.sleep(1800)  # Cada 30 minutos
            # Aquí se pueden agregar métricas adicionales (uso de psutil, etc.)
        except Exception as e:
            logger.error(f"⚠️ Error en monitoring_task: {e}", exc_info=True)
            await asyncio.sleep(60)

async def main():
    global wallets_list, signal_predictor, ml_data_preparation, last_heartbeat, is_bot_active, cielo_ws_connection
    try:
        last_heartbeat = time.time()
        signal.signal(signal.SIGINT, lambda s, f: sys.exit(0))
        signal.signal(signal.SIGTERM, lambda s, f: sys.exit(0))
        Config.check_required_config()
        logger.info("🗃️ Inicializando base de datos...")
        db.init_db()
        Config.load_dynamic_config()
        await send_boot_sequence()

        rugcheck_api = None
        if Config.RUGCHECK_PRIVATE_KEY and Config.RUGCHECK_WALLET_PUBKEY:
            try:
                logger.info("🔐 Inicializando RugCheck API...")
                rugcheck_api = RugCheckAPI()
                jwt_token = rugcheck_api.authenticate()
                if not jwt_token:
                    logger.warning("⚠️ No se pudo obtener token JWT de RugCheck")
                    send_telegram_message("⚠️ *Advertencia*: No se pudo conectar con RugCheck")
                else:
                    logger.info("✅ RugCheck API inicializada")
            except Exception as e:
                logger.error(f"⚠️ Error al configurar RugCheck: {e}")
                rugcheck_api = None

        logger.info("⚙️ Inicializando componentes...")
        cielo_client = CieloAPI(Config.CIELO_API_KEY)
        scoring_system = ScoringSystem()
        ml_data_preparation = MLDataPreparation()
        signal_predictor = SignalPredictor()
        model_loaded = signal_predictor.load_model()
        if model_loaded:
            logger.info("✅ Modelo ML cargado")
        else:
            logger.info("ℹ️ Modelo ML pendiente de entrenamiento")
        wallets_list = load_wallets()
        logger.info(f"📋 Se cargaron {len(wallets_list)} wallets")

        gmgn_client = None
        try:
            gmgn_client = GMGNClient()
            logger.info("✅ GMGN API inicializada")
        except Exception as e:
            logger.error(f"⚠️ Error en GMGN: {e}")
            gmgn_client = None

        signal_logic = SignalLogic(
            scoring_system=scoring_system,
            helius_client=cielo_client,  # Usamos Cielo para WebSocket o consultas push
            gmgn_client=gmgn_client,
            rugcheck_api=rugcheck_api,
            ml_predictor=signal_predictor
        )

        performance_tracker = PerformanceTracker(cielo_client)
        signal_logic.performance_tracker = performance_tracker

        logger.info("🤖 Iniciando bot de comandos de Telegram...")
        is_bot_active = await process_telegram_commands(Config.TELEGRAM_BOT_TOKEN, Config.TELEGRAM_CHAT_ID, signal_logic)
        logger.info("🔄 Iniciando tareas periódicas...")

        # Asegúrate de llamar al método correcto: check_signals_periodically()
        asyncio.create_task(signal_logic.check_signals_periodically())
        asyncio.create_task(monitoring_task())
        # Otras tareas asíncronas pueden agregarse aquí

        logger.info("📡 Iniciando conexión con Cielo API...")
        await cielo_client.run_forever(wallets_list, lambda msg: print(msg), {"chains": ["solana"], "tx_types": ["swap", "transfer"]})
    except Exception as e:
        logger.critical(f"🚨 Error crítico: {e}", exc_info=True)
        send_telegram_message(f"🚨 *Error Crítico*: {e}")
        sys.exit(1)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Bot terminado por el usuario")
