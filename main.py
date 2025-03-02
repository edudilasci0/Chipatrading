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
            
            # Esperar hasta la medianoche
            logger.info(f"⏰ Programando resumen diario para dentro de {seconds_until_midnight/3600:.1f} horas")
            await asyncio.sleep(seconds_until_midnight)
            
            # Obtener datos para el resumen
            signals_today = db.count_signals_today()
            active_tokens = signal_logic.get_active_candidates_count()
            tx_today = db.count_transactions_today()
            recent_signals = signal_logic.get_recent_signals(hours=24)
            
            # Preparar mensaje base
            summary_msg = (
                "*📈 Resumen Diario ChipaTrading*\n\n"
                f"• Señales emitidas hoy: `{signals_today}`\n"
                f"• Tokens actualmente monitoreados: `{active_tokens}`\n"
                f"• Total de transacciones procesadas: `{tx_today}`\n"
            )
            
            # Añadir información del modelo ML si está disponible
            if signal_predictor and signal_predictor.model:
                model_info = signal_predictor.get_model_info()
                summary_msg += (
                    f"\n*🧠 Modelo de Predicción*\n"
                    f"• Exactitud: `{model_info['accuracy']:.2f}`\n"
                    f"• Ejemplos entrenados: `{model_info['sample_count']}`\n"
                    f"• Última actualización: `{model_info['last_training'][:10]}`\n"
                )
            
            # Añadir resumen de señales recientes si hay alguna
            if recent_signals:
                summary_msg += "\n*🔍 Últimas Señales (24h)*\n"
                for token, ts, conf in recent_signals[:5]:  # Mostrar máximo 5
                    ts_str = datetime.fromtimestamp(ts).strftime('%H:%M')
                    summary_msg += f"• `{token[:10]}...` - Confianza: {conf:.2f} ({ts_str})\n"
                
                if len(recent_signals) > 5:
                    summary_msg += f"...y {len(recent_signals) - 5} más\n"
            
            summary_msg += "\nEl sistema continúa monitoreando wallets. ¡Buenas operaciones! 📊"
            
            # Enviar mensaje
            send_telegram_message(summary_msg)
            logger.info("📊 Resumen diario enviado")
            
        except Exception as e:
            logger.error(f"⚠️ Error en resumen diario: {e}")
            # Si falla, esperar 1 hora y reintentar
            await asyncio.sleep(3600)

async def refresh_wallets_task(interval=3600):
    """
    Actualiza la lista de wallets periódicamente.
    
    Args:
        interval: Intervalo en segundos entre actualizaciones
    """
    global wallets_list
    
    while running:
        try:
            # Cargar wallets desde el archivo
            new_wallets = load_wallets()
            if not new_wallets:
                logger.warning("⚠️ No se pudieron cargar wallets, manteniendo lista actual")
                await asyncio.sleep(interval)
                continue
                
            # Contar wallets nuevas
            added_count = 0
            for wallet in new_wallets:
                if wallet not in wallets_list:
                    wallets_list.append(wallet)
                    added_count += 1
            
            if added_count > 0:
                logger.info(f"📋 Se añadieron {added_count} nuevas wallets, total: {len(wallets_list)}")
                # Notificar por Telegram si se añadieron muchas
                if added_count >= 5:
                    send_telegram_message(
                        f"*📋 Actualización de Wallets*\nSe han añadido {added_count} nuevas wallets para monitorear.\nTotal: {len(wallets_list)} wallets"
                    )
            
            # Esperar hasta la próxima actualización
            await asyncio.sleep(interval)
            
        except Exception as e:
            logger.error(f"⚠️ Error actualizando wallets: {e}")
            await asyncio.sleep(interval)

async def monitoring_task():
    """
    Tarea periódica para monitorizar el estado del bot y enviar alertas.
    """
    global message_counter, transaction_counter, last_counter_log, last_heartbeat, running
    
    while running:
        try:
            # Esperar 10 minutos
            await asyncio.sleep(600)
            
            now = time.time()
            elapsed_since_last_log = now - last_counter_log
            
            # Enviar estadísticas cada 10 minutos
            msg = (
                "*📊 Estado del Bot*\n\n"
                f"• Mensajes recibidos (últimos {elapsed_since_last_log/60:.1f} min): `{message_counter}`\n"
                f"• Transacciones procesadas: `{transaction_counter}`\n"
                f"• Wallets monitoreadas: `{len(wallets_list)}`\n"
                f"• Tiempo desde inicio: `{(now - last_heartbeat)/3600:.1f}h`\n"
            )
            
            send_telegram_message(msg)
            logger.info(f"📊 Mensajes: {message_counter}, Transacciones: {transaction_counter} en los últimos {elapsed_since_last_log/60:.1f} minutos")
            
            # Resetear contadores
            message_counter = 0
            transaction_counter = 0
            last_counter_log = now
            
        except Exception as e:
            logger.error(f"⚠️ Error en monitoring_task: {e}")
            await asyncio.sleep(60)  # Esperar 1 minuto en caso de error

async def ml_periodic_tasks(ml_data_preparation, dex_client, signal_predictor, interval=86400):
    """
    Ejecuta tareas periódicas relacionadas con Machine Learning:
    - Recolección de outcomes
    - Entrenamiento del modelo
    
    Args:
        ml_data_preparation: Instancia de MLDataPreparation
        dex_client: Instancia de DexScreenerClient
        signal_predictor: Instancia de SignalPredictor
        interval: Intervalo en segundos entre ejecuciones
    """
    while running:
        try:
            logger.info("🧠 Ejecutando tareas periódicas de ML...")
            
            # 1. Recolectar outcomes para señales pasadas
            outcomes_count = ml_data_preparation.collect_signal_outcomes(dex_client)
            
            # 2. Si se recolectaron suficientes outcomes nuevos, entrenar modelo
            if outcomes_count >= 3:
                logger.info(f"🧠 Intentando entrenar modelo con {outcomes_count} nuevos outcomes...")
                
                # Preparar dataset de entrenamiento
                training_df = ml_data_preparation.prepare_training_data()
                
                if training_df is not None and len(training_df) >= 20:
                    # Entrenar modelo
                    trained = signal_predictor.train_model(force=(outcomes_count >= 5))
                    
                    if trained:
                        logger.info("🧠 Modelo entrenado exitosamente")
                        # Notificar por Telegram
                        model_info = signal_predictor.get_model_info()
                        send_telegram_message(
                            f"*🧠 Modelo ML Actualizado*\n\n"
                            f"• Exactitud: `{model_info['accuracy']:.2f}`\n"
                            f"• Ejemplos de entrenamiento: `{model_info['sample_count']}`\n"
                            f"• Feature más importante: `{signal_predictor.features[0]}`\n\n"
                            f"El modelo se usará para evaluar la calidad de nuevas señales."
                        )
            
            # Esperar hasta la próxima ejecución
            logger.info(f"🧠 Próxima ejecución de tareas ML en {interval/3600:.1f} horas")
            await asyncio.sleep(interval)
            
        except Exception as e:
            logger.error(f"⚠️ Error en tareas ML: {e}")
            await asyncio.sleep(interval)

async def on_cielo_message(message, signal_logic, ml_data_preparation, dex_client, scoring_system, signal_predictor=None):
    """
    Procesa los mensajes recibidos de Cielo.
    """
    global message_counter, transaction_counter
    
    # Incrementar contador de mensajes
    message_counter += 1
    
    try:
        data = json.loads(message)
        
        # Loguear tipo de mensaje
        if "type" in data:
            logger.info(f"📡 Mensaje tipo: {data['type']}")
        
        if "transactions" in data:
            tx_count = len(data["transactions"])
            logger.info(f"📦 Mensaje con {tx_count} transacciones recibido")
            
            for tx in data["transactions"]:
                tx_type = tx.get("type", "")
                usd_value = float(tx.get("amount_usd", 0))
                wallet = tx.get("wallet", "")
                token = tx.get("token", "")
                
                # Log básico de cada transacción para depuración
                logger.info(f"📝 Tx: {tx_type}, {usd_value}$, Token: {token}, Wallet: {wallet[:8]}...")
                
                # Filtrar transacciones relevantes
                min_tx_usd = float(Config.get("min_transaction_usd", Config.MIN_TRANSACTION_USD))
                
                # Para nuestro propósito, consideramos "swap" y "transfer" como transacciones relevantes
                if tx_type in ["swap", "transfer"] and usd_value >= min_tx_usd:
                    # Determinar si es compra o venta
                    # Por defecto consideramos "swap" como compra y necesitamos 
                    # analizar más datos para determinar la dirección
                    
                    actual_tx_type = "BUY"  # Valor predeterminado
                    
                    # Si es swap, necesitamos ver el detalle de la transacción
                    if tx_type == "swap":
                        # Determinar si es compra o venta según los datos disponibles
                        if "direction" in tx:
                            if tx["direction"] == "out":
                                actual_tx_type = "SELL"
                    
                    # Incrementar contador
                    transaction_counter += 1
                    
                    logger.info(f"💵 Transacción relevante: {actual_tx_type} {usd_value}$ en {token} por {wallet}")
                    
                    # Guardar en la BD
                    tx_data = {
                        "wallet": wallet,
                        "token": token,
                        "type": actual_tx_type,
                        "amount_usd": usd_value
                    }
                    db.save_transaction(tx_data)
                    
                    # Añadir a la lógica de señales
                    signal_logic.add_transaction(wallet, token, usd_value, actual_tx_type)
                    
                    # Si es una transacción grande, extraer features para ML
                    if usd_value >= min_tx_usd * 2:  # Doble del mínimo para enfocarnos en transacciones significativas
                        # Extraer características para ML
                        features = ml_data_preparation.extract_signal_features(token, dex_client, scoring_system)
                        
                        # Si hay modelo ML y features extraídas, predecir éxito
                        if signal_predictor and signal_predictor.model and features:
                            success_prob = signal_predictor.predict_success(features)
                            
                            # Log de predicción
                            if success_prob > 0.7:
                                logger.info(f"🧠 Alta probabilidad de éxito para {token}: {success_prob:.2f}")
    
    except json.JSONDecodeError as e:
        logger.warning(f"⚠️ Error al decodificar JSON: {e}")
    except Exception as e:
        logger.error(f"🚨 Error en on_cielo_message: {e}")

def handle_shutdown(sig, frame):
    """
    Maneja el cierre del programa.
    """
    global running
    print("\n🛑 Señal de cierre recibida, terminando tareas...")
    running = False
    
    # Intentar guardar estados antes de salir
    try:
        if ml_data_preparation:
            ml_data_preparation.save_features_to_csv()
            ml_data_preparation.save_outcomes_to_csv()
        print("✅ Estados guardados correctamente")
    except Exception as e:
        print(f"⚠️ Error guardando estados: {e}")
    
    print("👋 ChipaTrading Bot finalizado")
    sys.exit(0)

async def main():
    """
    Función principal que inicializa y ejecuta el bot.
    """
    global wallets_list, signal_predictor, ml_data_preparation, last_heartbeat
    
    try:
        # Registrar inicio para heartbeat
        last_heartbeat = time.time()
        
        # Registrar manejador de señales para cierre ordenado
        signal.signal(signal.SIGINT, handle_shutdown)
        signal.signal(signal.SIGTERM, handle_shutdown)
        
        # 1. Verificar configuración requerida
        Config.check_required_config()
        
        # 2. Inicializar la base de datos
        logger.info("🗃️ Inicializando base de datos...")
        db.init_db()
        
        # 3. Cargar configuración dinámica desde BD
        Config.load_dynamic_config()
        
        # 4. Enviar secuencia de inicio a Telegram
        await send_boot_sequence()
        
        # 5. Configurar RugCheck si hay credenciales
        rugcheck_jwt = None
        if Config.RUGCHECK_PRIVATE_KEY and Config.RUGCHECK_WALLET_PUBKEY:
            try:
                logger.info("🔐 Conectando con RugCheck...")
                rugcheck_jwt = login_rugcheck_solana()
                
                if not rugcheck_jwt:
                    logger.warning("⚠️ No se pudo obtener token JWT de RugCheck")
                    send_telegram_message("⚠️ *Advertencia*: No se pudo conectar con RugCheck para validar tokens")
            except Exception as e:
                logger.error(f"⚠️ Error al configurar RugCheck: {e}")
        
        # 6. Inicializar componentes principales
        logger.info("⚙️ Inicializando componentes...")
        
        dex_client = DexScreenerClient()
        scoring_system = ScoringSystem()
        signal_logic = SignalLogic(scoring_system, dex_client, rugcheck_jwt)
        
        # 7. Inicializar componentes de ML
        logger.info("🧠 Inicializando componentes de Machine Learning...")
        
        ml_data_preparation = MLDataPreparation()
        signal_predictor = SignalPredictor()
        model_loaded = signal_predictor.load_model()
        
        if model_loaded:
            logger.info("✅ Modelo ML cargado correctamente")
        else:
            logger.info("ℹ️ No se encontró modelo ML previo, se entrenará automáticamente cuando haya suficientes datos")
        
        # 8. Cargar wallets iniciales
        wallets_list = load_wallets()
        logger.info(f"📋 Se cargaron {len(wallets_list)} wallets para monitorear")
        
        if not wallets_list:
            logger.warning("⚠️ No se cargaron wallets, verificar traders_data.json")
            send_telegram_message("⚠️ *Advertencia*: No se cargaron wallets para monitorear. Verificar `traders_data.json`")
        
        # 9. Iniciar tareas periódicas en segundo plano
        logger.info("🔄 Iniciando tareas periódicas...")
        
        # Tarea de verificación de señales
        asyncio.create_task(signal_logic.check_signals_periodically())
        
        # Tarea de resumen diario
        asyncio.create_task(daily_summary_task(signal_logic, signal_predictor))
        
        # Tarea de actualización de wallets
        asyncio.create_task(refresh_wallets_task())
        
        # Tarea de Machine Learning
        asyncio.create_task(ml_periodic_tasks(ml_data_preparation, dex_client, signal_predictor))
        
        # Tarea de monitoreo
        asyncio.create_task(monitoring_task())
        
        # 10. Configurar callback para mensajes de Cielo
        async def handle_message(msg):
            await on_cielo_message(msg, signal_logic, ml_data_preparation, dex_client, scoring_system, signal_predictor)
        
        # 11. Conectar a Cielo API y suscribir wallets
        logger.info("🔄 Conectando a Cielo API...")
        cielo = CieloAPI(Config.CIELO_API_KEY)
        
        filter_params = {
            "chains": ["solana"],
            "tx_types": ["swap", "transfer"],  # Incluir swaps y transferencias
            "min_usd_value": float(Config.get("min_transaction_usd", Config.MIN_TRANSACTION_USD))
        }
        
        # 12. Enviar mensaje de inicio exitoso
        send_telegram_message(
            "🚀 *ChipaTrading Bot Iniciado*\n\n"
            f"• Wallets monitoreadas: `{len(wallets_list)}`\n"
            f"• Monitoreo de transacciones: `>{Config.get('min_transaction_usd')}$`\n"
            f"• Validación RugCheck: `{'Activa' if rugcheck_jwt else 'Inactiva'}`\n"
            f"• Modelo ML: `{'Cargado' if model_loaded else 'Pendiente'}`\n\n"
            "Sistema listo para detectar señales Daily Runner. ¡Buenas operaciones! 📊"
        )
        
        # 13. Ejecutar el WebSocket (esto bloquea indefinidamente)
        logger.info("📡 Iniciando suscripción a wallets...")
        await cielo.run_forever_wallets(wallets_list, handle_message, filter_params)
        
    except Exception as e:
        logger.critical(f"🚨 Error crítico en la función principal: {e}", exc_info=True)
        send_telegram_message(f"🚨 *Error Crítico*\nEl bot ha encontrado un error grave y necesita reiniciarse:\n`{str(e)}`")
        # Esperar antes de salir para permitir que se envíe el mensaje
        await asyncio.sleep(5)
        sys.exit(1)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\n👋 Terminando ChipaTrading Bot por interrupción del usuario...")
    except Exception as e:
        logger.critical(f"🚨 Error crítico no capturado: {e}", exc_info=True)
        print(f"🚨 Error crítico: {e}")
