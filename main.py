import os
import sys
import time
import signal
import asyncio
import logging
import json
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
from signal_logic import SignalLogic
from performance_tracker import PerformanceTracker
from telegram_utils import send_telegram_message, process_telegram_commands
import db
import time

# Importar módulos opcionales
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

# Función para procesar mensajes entrantes desde Cielo
async def on_cielo_message(message, wallet_tracker, scoring_system, signal_logic):
    try:
        data = json.loads(message)
        msg_type = data.get("type", "desconocido")
        logger.info(f"Procesando mensaje de Cielo tipo: {msg_type}")
        
        if data.get("type") == "tx" and "data" in data:
            tx_data = data["data"]
            logger.info(f"Datos de transacción: wallet={tx_data.get('wallet')[:8]}..., tx_type={tx_data.get('tx_type')}")
            
            if tx_data.get("tx_type") == "swap":
                processed_tx = {}
                # Detección precisa de operación: se capturan metadatos adicionales (p.ej., nombre/símbolo si están presentes)
                if tx_data.get("token0_address") in ["native", "So11111111111111111111111111111111111111112"]:
                    processed_tx["wallet"] = tx_data.get("wallet")
                    processed_tx["token"] = tx_data.get("token1_address")
                    processed_tx["type"] = "BUY"
                    processed_tx["amount_usd"] = float(tx_data.get("token1_amount_usd", 0))
                    processed_tx["token_name"] = tx_data.get("token1_name", "desconocido")
                    processed_tx["token_symbol"] = tx_data.get("token1_symbol", "desconocido")
                    logger.info(f"Detectada COMPRA: {tx_data.get('wallet')[:8]}... compra {tx_data.get('token1_address')[:8]}...")
                elif tx_data.get("token1_address") in ["native", "So11111111111111111111111111111111111111112"]:
                    processed_tx["wallet"] = tx_data.get("wallet")
                    processed_tx["token"] = tx_data.get("token0_address")
                    processed_tx["type"] = "SELL"
                    processed_tx["amount_usd"] = float(tx_data.get("token0_amount_usd", 0))
                    processed_tx["token_name"] = tx_data.get("token0_name", "desconocido")
                    processed_tx["token_symbol"] = tx_data.get("token0_symbol", "desconocido")
                    logger.info(f"Detectada VENTA: {tx_data.get('wallet')[:8]}... vende {tx_data.get('token0_address')[:8]}...")
                else:
                    processed_tx["wallet"] = tx_data.get("wallet")
                    processed_tx["token"] = tx_data.get("token1_address")
                    processed_tx["type"] = "BUY"
                    processed_tx["amount_usd"] = float(tx_data.get("token1_amount_usd", 0))
                    processed_tx["token_name"] = tx_data.get("token1_name", "desconocido")
                    processed_tx["token_symbol"] = tx_data.get("token1_symbol", "desconocido")
                    logger.info(f"Detectado swap sin SOL, asumiendo COMPRA: {tx_data.get('wallet')[:8]}... -> {tx_data.get('token1_address')[:8]}...")
                
                processed_tx["timestamp"] = int(tx_data.get("timestamp", time.time()))
                logger.info(f"Transacción procesada: {processed_tx['wallet'][:8]}... {processed_tx['type']} {processed_tx['token'][:8]}... ${processed_tx['amount_usd']:.2f}")
                
                # Mantener umbral de transacción actual
                min_tx_usd = float(Config.get("MIN_TRANSACTION_USD", 200))
                if processed_tx["amount_usd"] < min_tx_usd:
                    logger.info(f"Transacción descartada por monto bajo: ${processed_tx['amount_usd']:.2f} < ${min_tx_usd}")
                    return
                
                # Actualizar score si la wallet es seguida
                if processed_tx["wallet"] in wallet_tracker.get_wallets():
                    logger.info(f"Actualizando score para wallet {processed_tx['wallet'][:8]}...")
                    try:
                        scoring_system.update_score_on_trade(processed_tx["wallet"], processed_tx)
                        logger.info(f"Score actualizado para {processed_tx['wallet'][:8]}...")
                    except Exception as e:
                        logger.error(f"Error actualizando score: {e}")
                
                # Enviar transacción a signal_logic
                logger.info("Enviando transacción a signal_logic.process_transaction...")
                try:
                    signal_logic.process_transaction(processed_tx)
                    logger.info("Transacción procesada por signal_logic")
                except Exception as e:
                    logger.error(f"Error en signal_logic.process_transaction: {e}", exc_info=True)
                
                # Guardar transacción en la BD
                try:
                    logger.info("Guardando transacción en BD...")
                    db.save_transaction(processed_tx)
                    logger.info("Transacción guardada en BD")
                except Exception as e:
                    logger.error(f"Error guardando transacción en BD: {e}")
                    
            elif tx_data.get("tx_type") == "transfer":
                # Procesamiento básico de transferencias
                processed_tx = {}
                processed_tx["wallet"] = tx_data.get("wallet")
                processed_tx["token"] = tx_data.get("token_address", "desconocido")
                processed_tx["type"] = "TRANSFER"
                processed_tx["amount_usd"] = float(tx_data.get("amount_usd", 0))
                processed_tx["timestamp"] = int(tx_data.get("timestamp", time.time()))
                logger.info(f"Detectada TRANSFERENCIA: wallet {processed_tx['wallet'][:8]}... token {processed_tx['token'][:8]}... ${processed_tx['amount_usd']:.2f}")
                # Verificar monto mínimo (igual que swap)
                min_tx_usd = float(Config.get("MIN_TRANSACTION_USD", 200))
                if processed_tx["amount_usd"] < min_tx_usd:
                    logger.info(f"Transferencia descartada por monto bajo: ${processed_tx['amount_usd']:.2f} < ${min_tx_usd}")
                    return
                # Notificar transferencias muy significativas (p.ej., 10x el mínimo)
                if processed_tx["amount_usd"] >= 10 * min_tx_usd:
                    alert_msg = f"🚨 Transferencia muy significativa detectada: {processed_tx['wallet']} -> {processed_tx['token']} por ${processed_tx['amount_usd']:.2f}"
                    send_telegram_message(alert_msg)
                    logger.info("Notificación de transferencia significativa enviada")
                # Guardar transferencia en la BD
                try:
                    logger.info("Guardando transferencia en BD...")
                    db.save_transaction(processed_tx)
                    logger.info("Transferencia guardada en BD")
                except Exception as e:
                    logger.error(f"Error guardando transferencia en BD: {e}")
    except json.JSONDecodeError:
        logger.error("Error decodificando mensaje JSON de Cielo")
    except Exception as e:
        logger.error(f"Error procesando mensaje de Cielo: {e}", exc_info=True)

# Función de monitoreo mejorado
async def enhanced_monitoring_task(signal_logic):
    global bot_running
    failure_counter = 0
    while bot_running:
        try:
            await asyncio.sleep(1800)  # Cada 30 minutos
            active_tokens = signal_logic.get_active_candidates_count()
            recent_signals = len(signal_logic.recent_signals)
            total_transactions = db.count_transactions_today()
            # Estadísticas adicionales: tasa de éxito, ganancia promedio y top traders
            success_rate = signal_logic.performance_tracker.get_success_rate() if hasattr(signal_logic.performance_tracker, "get_success_rate") else 0
            avg_profit = signal_logic.performance_tracker.get_avg_profit() if hasattr(signal_logic.performance_tracker, "get_avg_profit") else 0
            top_traders = signal_logic.performance_tracker.get_top_traders() if hasattr(signal_logic.performance_tracker, "get_top_traders") else []
            
            logger.info("=== REPORTE DE ESTADO ===")
            logger.info(f"Tokens monitoreados: {active_tokens}")
            logger.info(f"Señales recientes: {recent_signals}")
            logger.info(f"Transacciones hoy: {total_transactions}")
            logger.info(f"Tasa de éxito: {success_rate:.2f}")
            logger.info(f"Ganancia promedio: ${avg_profit:.2f}")
            logger.info(f"Top traders: {top_traders}")
            
            status_message = (
                "📊 *Reporte de Estado*\n\n"
                f"• Tokens monitoreados: `{active_tokens}`\n"
                f"• Señales recientes: `{recent_signals}`\n"
                f"• Transacciones hoy: `{total_transactions}`\n"
                f"• Tasa de éxito: `{success_rate:.2f}`\n"
                f"• Ganancia promedio: `${avg_profit:.2f}`\n"
                f"• Top traders: `{', '.join(top_traders)}`\n\n"
                "✅ Sistema operando normalmente."
            )
            send_telegram_message(status_message)
        except Exception as e:
            failure_counter += 1
            logger.error(f"⚠️ Error en enhanced_monitoring_task: {e}", exc_info=True)
            if failure_counter >= 3:
                alert_msg = f"⚠️ La tarea de monitoreo ha fallado {failure_counter} veces consecutivas."
                send_telegram_message(alert_msg)
            await asyncio.sleep(60)

# Función principal
async def main():
    global bot_running
    try:
        Config.check_required_config()
        db.init_db()
        
        wallet_tracker = WalletTracker()
        scoring_system = ScoringSystem()
        rugcheck_api = RugCheckAPI()
        rugcheck_api.authenticate()
        
        gmgn_client = GMGNClient() if gmgn_available else None
        
        helius_client = None
        if helius_available and Config.HELIUS_API_KEY:
            helius_client = HeliusClient(Config.HELIUS_API_KEY)
            logger.info("✅ Cliente Helius inicializado")
        else:
            logger.warning("⚠️ Cliente Helius no disponible o falta API key")
        
        ml_predictor = None
        if ml_available:
            from signal_predictor import SignalPredictor
            ml_predictor = SignalPredictor()
            if os.path.exists("ml_data/training_data.csv"):
                ml_predictor.train_model()
        
        signal_logic = SignalLogic(
            scoring_system=scoring_system,
            helius_client=helius_client,
            gmgn_client=gmgn_client,
            rugcheck_api=rugcheck_api,
            ml_predictor=ml_predictor
        )
        
        performance_tracker = PerformanceTracker(token_data_service=helius_client)
        signal_logic.performance_tracker = performance_tracker
        
        # Notificar inicio del sistema con resumen de capacidades y componentes disponibles
        capabilities = [
            "Procesamiento de mensajes de Cielo",
            "Monitoreo del sistema",
            "Gestión de errores",
            "Procesamiento de transferencias"
        ]
        init_message = (
            "🚀 *Inicio del Bot de Trading*\n\n"
            f"• Capacidades: {', '.join(capabilities)}\n"
            f"• ML: {'Disponible' if ml_available else 'No disponible'}\n"
            f"• GMGN: {'Disponible' if gmgn_available else 'No disponible'}\n"
            f"• Helius: {'Disponible' if helius_available and Config.HELIUS_API_KEY else 'No disponible'}"
        )
        send_telegram_message(init_message)
        
        is_bot_active = await process_telegram_commands(
            Config.TELEGRAM_BOT_TOKEN,
            Config.TELEGRAM_CHAT_ID,
            signal_logic
        )
        
        tasks = []
        tasks.append(asyncio.create_task(signal_logic.check_signals_periodically()))
        tasks.append(asyncio.create_task(enhanced_monitoring_task(signal_logic)))
        
        wallets = wallet_tracker.get_wallets()
        cielo_client = CieloAPI(Config.CIELO_API_KEY)
        
        async def process_cielo_message(message):
            try:
                await on_cielo_message(message, wallet_tracker, scoring_system, signal_logic)
            except Exception as e:
                logger.error(f"Error en process_cielo_message: {e}", exc_info=True)
        
        cielo_task = asyncio.create_task(cielo_client.run_forever_wallets(
            wallets,
            process_cielo_message,
            {"chains": ["solana"], "tx_types": ["swap", "transfer"]}
        ))
        tasks.append(cielo_task)
        
        # Entrenamiento periódico automático del modelo ML
        async def periodic_ml_training():
            global bot_running
            while bot_running:
                try:
                    if ml_predictor:
                        logger.info("Entrenamiento periódico del modelo ML iniciado...")
                        ml_predictor.train_model()
                        logger.info("Entrenamiento del modelo ML completado")
                    await asyncio.sleep(3600)  # Cada hora
                except Exception as e:
                    logger.error(f"Error en entrenamiento ML periódico: {e}", exc_info=True)
                    await asyncio.sleep(60)
        tasks.append(asyncio.create_task(periodic_ml_training()))
        
        # Reinicio automático de tareas fallidas
        while bot_running:
            for i, task in enumerate(tasks):
                if task.done():
                    try:
                        error = task.exception()
                        if error:
                            logger.error(f"Tarea #{i} falló: {error}")
                            # Reiniciar tarea según su índice
                            if i == 2:  # Tarea de Cielo
                                logger.info("Reiniciando conexión Cielo...")
                                tasks[i] = asyncio.create_task(cielo_client.run_forever_wallets(
                                    wallets,
                                    process_cielo_message,
                                    {"chains": ["solana"], "tx_types": ["swap", "transfer"]}
                                ))
                            elif i == 3:  # Tarea de entrenamiento ML
                                logger.info("Reiniciando entrenamiento ML periódico...")
                                tasks[i] = asyncio.create_task(periodic_ml_training())
                    except Exception as e:
                        logger.error(f"Error verificando tarea #{i}: {e}", exc_info=True)
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
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("Bot detenido por el usuario")
    finally:
        loop.close()
