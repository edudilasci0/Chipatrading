import os
import sys
import time
import signal
import asyncio
import logging
import json
from datetime import datetime, timedelta

# Configuración básica de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("chipatrading")

# Importar componentes del sistema
from config import Config
from wallet_tracker import WalletTracker
from cielo_api import CieloAPI
from scoring import ScoringSystem
from rugcheck import RugCheckAPI
from signal_logic import SignalLogic
from performance_tracker import PerformanceTracker
from telegram_utils import send_telegram_message, process_telegram_commands
import db

# Componentes opcionales
try:
    from signal_predictor import SignalPredictor
    ml_available = True
except ImportError:
    ml_available = False
    logger.warning("⚠️ Módulo de ML no disponible")

try:
    from gmgn_client import GMGNClient
    gmgn_available = True
except ImportError:
    gmgn_available = False
    logger.warning("⚠️ Cliente GMGN no disponible")

try:
    from helius_client import HeliusClient
    helius_available = True
except ImportError:
    helius_available = False
    logger.warning("⚠️ Cliente Helius no disponible")

# Variable global para control del bot
bot_running = True

# Función para manejar mensajes entrantes de Cielo
async def on_cielo_message(message, wallet_tracker, scoring_system, signal_logic):
    try:
        data = json.loads(message)
        if data.get("type") == "tx" and "data" in data:
            tx_data = data["data"]
            # Verifica que el mensaje tenga la información necesaria
            if all(key in tx_data for key in ["wallet", "token", "amount_usd", "tx_type"]):
                # Se agrega la transacción a la lógica de señales
                signal_logic.add_transaction(
                    tx_data["wallet"],
                    tx_data["token"],
                    float(tx_data["amount_usd"]),
                    tx_data["tx_type"]
                )
    except Exception as e:
        logger.error(f"Error procesando mensaje de Cielo: {e}", exc_info=True)

# Tarea de monitoreo adicional (por ejemplo, para revisar estado del bot)
async def monitoring_task():
    global bot_running, message_counter, transaction_counter, last_counter_log, last_heartbeat
    while bot_running:
        try:
            # Espera 30 minutos entre reportes
            await asyncio.sleep(1800)
            # Aquí se pueden agregar más métricas (p.ej., usando psutil)
            logger.info("Enviando reporte de estado (monitoring_task)")
            send_telegram_message("📊 Reporte de estado: Todo en orden.")
        except Exception as e:
            logger.error(f"⚠️ Error en monitoring_task: {e}", exc_info=True)
            await asyncio.sleep(60)

async def main():
    global bot_running
    try:
        # Verificar configuración y conectar con la base de datos
        Config.check_required_config()
        db.init_db()
        Config.load_dynamic_config()

        # Mensaje de inicio a Telegram
        send_telegram_message("🚀 Iniciando ChipaTrading Bot...")

        # Inicializar componentes principales
        wallet_tracker = WalletTracker()
        scoring_system = ScoringSystem()
        rugcheck_api = None
        if Config.RUGCHECK_PRIVATE_KEY and Config.RUGCHECK_WALLET_PUBKEY:
            try:
                logger.info("🔐 Inicializando RugCheck API...")
                rugcheck_api = RugCheckAPI()
                jwt_token = rugcheck_api.authenticate()
                if not jwt_token:
                    logger.warning("⚠️ No se obtuvo token JWT de RugCheck")
                    send_telegram_message("⚠️ *Advertencia*: No se pudo conectar con RugCheck")
                else:
                    logger.info("✅ RugCheck API inicializada")
            except Exception as e:
                logger.error(f"⚠️ Error en RugCheck: {e}", exc_info=True)
                rugcheck_api = None

        # Inicializar clientes de datos de mercado
        helius_client = None
        if helius_available and Config.HELIUS_API_KEY:
            helius_client = HeliusClient(Config.HELIUS_API_KEY)
            logger.info("✅ Cliente Helius inicializado")
        else:
            logger.warning("⚠️ Cliente Helius no disponible o falta API key")

        gmgn_client = None
        if gmgn_available:
            try:
                gmgn_client = GMGNClient()
                logger.info("✅ Cliente GMGN inicializado")
            except Exception as e:
                logger.error(f"⚠️ Error en GMGN: {e}", exc_info=True)
                gmgn_client = None

        # Inicializar el módulo de ML si está disponible
        ml_predictor = None
        if ml_available:
            ml_predictor = SignalPredictor()
            if os.path.exists("ml_data/training_data.csv"):
                ml_predictor.train_model()

        # Inicializar la lógica de señales con los clientes de datos
        signal_logic = SignalLogic(
            scoring_system=scoring_system,
            helius_client=helius_client,
            gmgn_client=gmgn_client,
            rugcheck_api=rugcheck_api,
            ml_predictor=ml_predictor
        )

        # Inicializar tracker de rendimiento
        performance_tracker = PerformanceTracker(token_data_service=helius_client)
        signal_logic.performance_tracker = performance_tracker

        # Iniciar bot de comandos de Telegram (no bloqueante)
        is_bot_active = await process_telegram_commands(
            Config.TELEGRAM_BOT_TOKEN,
            Config.TELEGRAM_CHAT_ID,
            signal_logic
        )

        # Mensaje de resumen a Telegram
        active_apis = []
        if helius_client:
            active_apis.append("Helius")
        if gmgn_client:
            active_apis.append("GMGN")
        if rugcheck_api:
            active_apis.append("RugCheck")
        send_telegram_message(
            "🚀 *Bot Iniciado*\n"
            f"• Monitoreando {len(wallet_tracker.get_wallets())} wallets\n"
            f"• Configuración: Min {Config.MIN_TRADERS_FOR_SIGNAL} traders, {Config.MIN_TRANSACTION_USD}$ min por TX\n"
            f"• Confianza mínima: {Config.MIN_CONFIDENCE_THRESHOLD}\n"
            f"• APIs activas: {', '.join(active_apis)}\n"
            "Usa /status para ver el estado actual del bot"
        )

        # Crear y lanzar tareas asíncronas
        asyncio.create_task(signal_logic.check_signals_periodically())
        asyncio.create_task(monitoring_task())

        # Inicializar cliente Cielo para WebSocket y monitoreo de wallets
        wallets = wallet_tracker.get_wallets()
        cielo_client = CieloAPI(Config.CIELO_API_KEY)
        asyncio.create_task(cielo_client.run_forever_wallets(
            wallets,
            lambda msg: asyncio.create_task(on_cielo_message(msg, wallet_tracker, scoring_system, signal_logic)),
            {"chains": ["solana"], "tx_types": ["swap", "transfer"]}
        ))

        # Mantener el main corriendo
        while True:
            await asyncio.sleep(3600)

    except KeyboardInterrupt:
        logger.info("Bot interrumpido por el usuario")
        bot_running = False
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error en main: {e}", exc_info=True)
        send_telegram_message(f"⚠️ *Error Crítico*: El bot se ha detenido: {e}")
        sys.exit(1)

if __name__ == "__main__":
    signal.signal(signal.SIGTERM, lambda s, f: sys.exit(0))
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Bot detenido por el usuario")
