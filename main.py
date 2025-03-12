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

# Variable global para el control del bot
bot_running = True

# Función para procesar mensajes entrantes de Cielo (con logs adicionales)
async def on_cielo_message(message, wallet_tracker, scoring_system, signal_logic):
    try:
        # Log inicial: tipo de mensaje
        logger.info(f"Procesando mensaje de Cielo tipo: {json.loads(message).get('type', 'desconocido')}")
        data = json.loads(message)
        if data.get("type") == "tx" and "data" in data:
            tx_data = data["data"]
            logger.info(f"Datos de transacción: wallet={tx_data.get('wallet')[:8]}..., tx_type={tx_data.get('tx_type')}")
            
            # Procesar transacciones tipo swap
            if tx_data.get("tx_type") == "swap":
                processed_tx = {}
                # Si token0_address es "native" o SOL envuelto, se asume COMPRA y se usa token1_address
                if tx_data.get("token0_address") in ["native", "So11111111111111111111111111111111111111112"]:
                    processed_tx["wallet"] = tx_data.get("wallet")
                    processed_tx["token"] = tx_data.get("token1_address")
                    processed_tx["type"] = "BUY"
                    processed_tx["amount_usd"] = float(tx_data.get("token1_amount_usd", 0))
                    logger.info(f"Detectada COMPRA: {tx_data.get('wallet')[:8]}... compra {tx_data.get('token1_address')[:8]}...")
                # Si token1_address es "native" o SOL envuelto, se asume VENTA y se usa token0_address
                elif tx_data.get("token1_address") in ["native", "So11111111111111111111111111111111111111112"]:
                    processed_tx["wallet"] = tx_data.get("wallet")
                    processed_tx["token"] = tx_data.get("token0_address")
                    processed_tx["type"] = "SELL"
                    processed_tx["amount_usd"] = float(tx_data.get("token0_amount_usd", 0))
                    logger.info(f"Detectada VENTA: {tx_data.get('wallet')[:8]}... vende {tx_data.get('token0_address')[:8]}...")
                else:
                    # Caso por defecto: se asume COMPRA usando token1_address
                    processed_tx["wallet"] = tx_data.get("wallet")
                    processed_tx["token"] = tx_data.get("token1_address")
                    processed_tx["type"] = "BUY"
                    processed_tx["amount_usd"] = float(tx_data.get("token1_amount_usd", 0))
                    logger.info(f"Detectado swap sin SOL, asumiendo COMPRA: {tx_data.get('wallet')[:8]}... -> {tx_data.get('token1_address')[:8]}...")
                
                # Asignar timestamp (si no viene, usar el actual)
                processed_tx["timestamp"] = int(tx_data.get("timestamp", time.time()))
                logger.info(f"Transacción procesada: {processed_tx['wallet'][:8]}... {processed_tx['type']} {processed_tx['token'][:8]}... ${processed_tx['amount_usd']:.2f}")
                
                # Verificar monto mínimo
                min_tx_usd = float(Config.get("MIN_TRANSACTION_USD", 200))
                if processed_tx["amount_usd"] < min_tx_usd:
                    logger.info(f"Transacción descartada por monto bajo: ${processed_tx['amount_usd']:.2f} < ${min_tx_usd}")
                    return
                
                # Actualizar score si la wallet está en la lista de seguimiento
                if processed_tx["wallet"] in wallet_tracker.get_wallets():
                    logger.info(f"Actualizando score para wallet {processed_tx['wallet'][:8]}...")
                    try:
                        scoring_system.update_score_on_trade(processed_tx["wallet"], processed_tx)
                        logger.info(f"Score actualizado para {processed_tx['wallet'][:8]}...")
                    except Exception as e:
                        logger.error(f"Error actualizando score: {e}")
                
                # Procesar transacción en la lógica de señales
                logger.info("Enviando transacción a signal_logic.process_transaction...")
                try:
                    signal_logic.process_transaction(processed_tx)
                    logger.info("Transacción procesada por signal_logic")
                except Exception as e:
                    logger.error(f"Error en signal_logic.process_transaction: {e}", exc_info=True)
                
                # Guardar la transacción en la base de datos
                try:
                    logger.info("Guardando transacción en base de datos...")
                    db.save_transaction(processed_tx)
                    logger.info("Transacción guardada en base de datos")
                except Exception as e:
                    logger.error(f"Error guardando transacción en BD: {e}")
                    
            elif tx_data.get("tx_type") == "transfer":
                logger.info(f"Transacción tipo transfer detectada (no procesada): {tx_data.get('wallet')[:8]}...")
                # Aquí se puede implementar lógica adicional para transferencias
                pass
    except json.JSONDecodeError:
        logger.error("Error decodificando mensaje JSON de Cielo")
    except Exception as e:
        logger.error(f"Error procesando mensaje de Cielo: {e}", exc_info=True)

# Función de monitoreo mejorada
async def enhanced_monitoring_task(signal_logic):
    global bot_running
    while bot_running:
        try:
            await asyncio.sleep(1800)  # 30 minutos
            active_tokens = signal_logic.get_active_candidates_count()
            recent_signals = len(signal_logic.recent_signals)
            total_transactions = db.count_transactions_today()
            
            logger.info("=== REPORTE DE ESTADO ===")
            logger.info(f"Tokens monitoreados: {active_tokens}")
            logger.info(f"Señales recientes: {recent_signals}")
            logger.info(f"Transacciones hoy: {total_transactions}")
            
            status_message = (
                "📊 *Reporte de Estado*\n\n"
                f"• Tokens monitoreados: `{active_tokens}`\n"
                f"• Señales recientes: `{recent_signals}`\n"
                f"• Transacciones hoy: `{total_transactions}`\n\n"
                "✅ Sistema operando normalmente."
            )
            send_telegram_message(status_message)
        except Exception as e:
            logger.error(f"⚠️ Error en enhanced_monitoring_task: {e}", exc_info=True)
            await asyncio.sleep(60)

# Función principal
async def main():
    global bot_running
    try:
        Config.check_required_config()
        db.init_db()
        Config.load_dynamic_config()
        send_telegram_message("🚀 Iniciando ChipaTrading Bot...")

        wallet_tracker = WalletTracker()
        scoring_system = ScoringSystem()
        
        # Inicializar RugCheck
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

        # Inicializar cliente Helius
        helius_client = None
        if helius_available and Config.HELIUS_API_KEY:
            helius_client = HeliusClient(Config.HELIUS_API_KEY)
            logger.info("✅ Cliente Helius inicializado")
        else:
            logger.warning("⚠️ Cliente Helius no disponible o falta API key")

        # Inicializar cliente GMGN (sin API key)
        gmgn_client = None
        if gmgn_available:
            try:
                gmgn_client = GMGNClient()
                logger.info("✅ Cliente GMGN inicializado")
            except Exception as e:
                logger.error(f"⚠️ Error en GMGN: {e}", exc_info=True)
                gmgn_client = None

        # Inicializar módulo ML si está disponible
        ml_predictor = None
        if ml_available:
            ml_predictor = SignalPredictor()
            if os.path.exists("ml_data/training_data.csv"):
                ml_predictor.train_model()

        # Inicializar lógica de señales
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

        # Iniciar bot de comandos de Telegram
        is_bot_active = await process_telegram_commands(
            Config.TELEGRAM_BOT_TOKEN,
            Config.TELEGRAM_CHAT_ID,
            signal_logic
        )

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

        tasks = []
        tasks.append(asyncio.create_task(signal_logic.check_signals_periodically()))
        tasks.append(asyncio.create_task(enhanced_monitoring_task(signal_logic)))

        async def process_cielo_message(message):
            try:
                await on_cielo_message(message, wallet_tracker, scoring_system, signal_logic)
            except Exception as e:
                logger.error(f"Error en process_cielo_message: {e}", exc_info=True)

        wallets = wallet_tracker.get_wallets()
        cielo_client = CieloAPI(Config.CIELO_API_KEY)
        cielo_task = asyncio.create_task(cielo_client.run_forever_wallets(
            wallets,
            process_cielo_message,
            {"chains": ["solana"], "tx_types": ["swap", "transfer"]}
        ))
        tasks.append(cielo_task)

        while bot_running:
            for idx, task in enumerate(tasks):
                if task.done():
                    try:
                        err = task.exception()
                        if err:
                            logger.error(f"Tarea #{idx} finalizó con error: {err}")
                            if idx == 2:  # Si es la tarea de Cielo, reiniciar
                                logger.info("Reiniciando conexión Cielo...")
                                tasks[idx] = asyncio.create_task(cielo_client.run_forever_wallets(
                                    wallets,
                                    process_cielo_message,
                                    {"chains": ["solana"], "tx_types": ["swap", "transfer"]}
                                ))
                    except Exception as e:
                        logger.error(f"Error verificando tarea #{idx}: {e}", exc_info=True)
            await asyncio.sleep(30)

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
