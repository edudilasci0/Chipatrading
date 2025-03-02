import asyncio
import json
import os
import sys
import time
import signal
import logging
from datetime import datetime, timedelta

# Importar componentes del sistema
import db
from config import Config
from cielo_api import CieloAPI
from dexscreener_api import DexScreenerClient
from scoring import ScoringSystem
from signal_logic import SignalLogic
from telegram_utils import send_telegram_message
from performance_tracker import PerformanceTracker
from ml_preparation import MLDataPreparation
from signal_predictor import SignalPredictor
from rugcheck import login_rugcheck_solana

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('chipatrading.log')
    ]
)
logger = logging.getLogger("chipatrading")

# Variables globales
wallets_list = []
running = True
signal_predictor = None
ml_data_preparation = None

# Variables para monitoreo
message_counter = 0
transaction_counter = 0
last_counter_log = time.time()
last_heartbeat = time.time()

def load_wallets():
    """
    Carga las wallets desde traders_data.json
    
    Returns:
        list: Lista de direcciones de wallets
    """
    try:
        with open("traders_data.json", "r") as f:
            data = json.load(f)
            wallets = [entry["Wallet"] for entry in data]
            logger.info(f"📋 Se cargaron {len(wallets)} wallets desde traders_data.json")
            return wallets
    except FileNotFoundError:
        logger.warning("⚠️ No se encontró el archivo traders_data.json")
        return []
    except json.JSONDecodeError:
        logger.error("🚨 Error decodificando JSON en traders_data.json")
        return []
    except Exception as e:
        logger.error(f"🚨 Error al cargar traders_data.json: {e}")
        return []

async def send_boot_sequence():
    """
    Envía una secuencia de mensajes de inicio a Telegram.
    """
    boot_messages = [
        "**🚀 Iniciando ChipaTrading Bot**\nPreparando servicios y verificaciones...",
        "**📡 Módulos de Monitoreo Activados**\nEscaneando wallets definidas para transacciones relevantes...",
        "**📊 Cargando Parámetros de Mercado**\nConectando con DexScreener para datos de volumen...",
        "**🔒 Verificando Seguridad**\nConectando con RugCheck para verificar tokens...",
        "**⚙️ Inicializando Lógica de Señales**\nConfigurando reglas de scoring y agrupación de traders...",
        "**✅ Sistema Operativo**\nListo para monitorear transacciones y generar alertas."
    ]
    
    for msg in boot_messages:
        send_telegram_message(msg)
        await asyncio.sleep(2)

async def daily_summary_task(signal_logic, signal_predictor=None):
    """
    Envía un resumen diario de actividad.
    
    Args:
        signal_logic: Instancia de SignalLogic
        signal_predictor: Instancia de SignalPredictor o None
    """
    while running:
        try:
            # Calcular tiempo hasta la próxima medianoche
            now = datetime.now()
            next_midnight = (now + timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            seconds_until_midnight = (next_midnight - now).total_seconds()
