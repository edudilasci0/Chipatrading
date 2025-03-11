import os
import sys
import time
import signal
import asyncio
import logging
from datetime import datetime

# Configurar logging
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
from signal_logic import SignalLogic  # Importar desde signal_logic.py
from performance_tracker import PerformanceTracker
from telegram_utils import send_telegram_message, process_telegram_commands
import db

# Opcional: importar componentes adicionales si están disponibles
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

async def process_transaction(tx_data, scoring_system, signal_logic):
    """
    Procesa una transacción recibida.
    
    Args:
        tx_data: Datos de la transacción
        scoring_system: Sistema de scoring
        signal_logic: Lógica de señales
    """
    try:
        wallet = tx_data.get("wallet")
        if not wallet:
            return
            
        # Actualizar score del trader basado en la transacción
        scoring_system.update_score_on_trade(wallet, tx_data)
        
        # Guardar transacción en BD
        db.save_transaction(tx_data)
        
        # Actualizar estado interno del token para detección de señales
        token = tx_data.get("token")
        if token:
            # Aquí procesamos el token para posibles señales
            # (implementación detallada en signal_logic)
            pass
    except Exception as e:
        logger.error(f"Error procesando transacción: {e}")

async def on_cielo_message(message, wallet_tracker, scoring_system, signal_logic):
    """
    Callback para mensajes recibidos de Cielo.
    
    Args:
        message: Mensaje recibido del API de Cielo
        wallet_tracker: Tracker de wallets
        scoring_system: Sistema de scoring
        signal_logic: Lógica de señales
    """
    try:
        import json
        data = json.loads(message)
        
        if data.get("type") == "pong":
            logger.debug("📥 Pong recibido de Cielo WebSocket")
            return
            
        if "transactions" not in data:
            return
            
        tx_count = len(data["transactions"])
        if tx_count > 0:
            logger.info(f"📦 Recibidas {tx_count} transacciones")
            
            for tx in data["transactions"]:
                # Procesar solo si la wallet está en nuestra lista de seguimiento
                wallet = tx.get("wallet")
                if wallet and wallet in wallet_tracker.get_wallets():
                    # Procesar transacción
                    await process_transaction(tx, scoring_system, signal_logic)
    except Exception as e:
        logger.error(f"Error en on_cielo_message: {e}")

async def main():
    """
    Función principal del bot
    """
    try:
        # Verificar configuración
        Config.check_required_config()
        
        # Inicializar base de datos
        db.init_db()
        
        # Inicializar componentes
        wallet_tracker = WalletTracker()
        scoring_system = ScoringSystem()
        rugcheck_api = RugCheckAPI()
        rugcheck_api.authenticate()
        
        # Inicializar clientes opcionales
        gmgn_client = GMGNClient() if gmgn_available else None
        
        # Inicializar predictor ML si está disponible
        ml_predictor = None
        if ml_available:
            ml_predictor = SignalPredictor()
            if os.path.exists("ml_data/training_data.csv"):
                ml_predictor.train_model()
        
        # Inicializar lógica de señales
        signal_logic = SignalLogic(
            scoring_system=scoring_system,
            helius_client=None,  # No tenemos un cliente Helius inicializado
            gmgn_client=gmgn_client,
            rugcheck_api=rugcheck_api,
            ml_predictor=ml_predictor
        )
        
        # Inicializar tracker de rendimiento
        performance_tracker = PerformanceTracker()
        signal_logic.performance_tracker = performance_tracker
        
        # Iniciar comando de Telegram (no bloqueante)
        is_bot_active = await process_telegram_commands(
            Config.TELEGRAM_BOT_TOKEN,
            Config.TELEGRAM_CHAT_ID,
            signal_logic
        )
        
        # Crear y ejecutar tareas asíncronas
        
        # Tarea 1: Verificar señales periódicamente
        asyncio.create_task(signal_logic.check_signals_periodically())
        
        # Tarea 2: Iniciar WebSocket de Cielo para monitorear wallets
        wallets = wallet_tracker.get_wallets()
        cielo_client = CieloAPI()
        
        # Mensaje inicial a Telegram
        send_telegram_message(
            "🚀 *Bot Iniciado*\n"
            f"• Monitoreando {len(wallets)} wallets\n"
            f"• Configuración: Min {Config.MIN_TRADERS_FOR_SIGNAL} traders, "
            f"{Config.MIN_TRANSACTION_USD}$ min por TX\n"
            f"• Confianza mínima: {Config.MIN_CONFIDENCE_THRESHOLD}\n"
            "Usa /status para ver el estado actual del bot"
        )
        
        # Iniciar WebSocket y mantener la conexión
        await cielo_client.run_forever_wallets(
            wallets,
            lambda msg: on_cielo_message(msg, wallet_tracker, scoring_system, signal_logic)
        )
        
    except KeyboardInterrupt:
        logger.info("Bot interrumpido por usuario")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error en main: {e}")
        send_telegram_message(f"⚠️ *Error Crítico*\nEl bot se ha detenido: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Manejar señales de terminación
    signal.signal(signal.SIGTERM, lambda s, f: sys.exit(0))
    
    # Iniciar loop de eventos
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("Bot detenido por usuario")
    finally:
        loop.close()
