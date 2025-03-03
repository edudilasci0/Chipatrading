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
from telegram_utils import send_telegram_message, process_telegram_commands
from performance_tracker import PerformanceTracker
from ml_preparation import MLDataPreparation
from signal_predictor import SignalPredictor
from rugcheck import RugCheckAPI

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('chipatrading.log')  # Simple path in the current directory
    ]
)
logger = logging.getLogger("chipatrading")

# Variables globales
wallets_list = []
running = True
signal_predictor = None
ml_data_preparation = None
is_bot_active = None
cielo_ws_connection = None  # Variable para mantener la conexión WebSocket

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
            
            # Obtener estadísticas de rendimiento
            try:
                stats = db.get_signals_performance_stats()
                if stats:
                    summary_msg += "\n*📊 Rendimiento de Señales*\n"
                    for stat in stats:
                        timeframe = stat["timeframe"]
                        success_rate = stat["success_rate"]
                        avg_percent = stat["avg_percent_change"]
                        
                        emoji = "🟢" if success_rate >= 60 else "🟡" if success_rate >= 50 else "🔴"
                        summary_msg += f"{emoji} *{timeframe}*: {success_rate}% éxito, {avg_percent}% promedio\n"
            except Exception as e:
                logger.error(f"Error obteniendo estadísticas para resumen: {e}")
            
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
                for token, ts, conf, sig_id in recent_signals[:5]:  # Mostrar máximo 5
                    ts_str = datetime.fromtimestamp(ts).strftime('%H:%M')
                    summary_msg += f"• {sig_id} `{token[:10]}...` - Confianza: {conf:.2f} ({ts_str})\n"
                
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
            
            # Verificar si el bot está activo, si no, omitir el mensaje
            if is_bot_active and not is_bot_active():
                logger.info("⏸️ Bot en modo inactivo, omitiendo mensaje de estado")
                continue
            
            now = time.time()
            elapsed_since_last_log = now - last_counter_log
            
            # Enviar estadísticas cada 10 minutos
            msg = (
                "*📊 Estado del Bot*\n\n"
                f"• Mensajes recibidos (últimos {elapsed_since_last_log/60:.1f} min): `{message_counter}`\n"
                f"• Transacciones procesadas: `{transaction_counter}`\n"
                f"• Wallets monitoreadas: `{len(wallets_list)}`\n"
                f"• Tiempo desde inicio: `{(now - last_heartbeat)/3600:.1f}h`\n"
                f"• Estado: `{'Activo' if is_bot_active and is_bot_active() else 'Inactivo'}`\n"
            )
            
            send_telegram_message(msg)
            logger.info(f"📊 Mensajes: {message_counter}, Transacciones: {transaction_counter} en los últimos {elapsed_since_last_log/60:.1f} minutos")
            
            # Verificar si hay suficientes mensajes - posible problema de conexión
            if message_counter < 5 and elapsed_since_last_log > 300:
                send_telegram_message(
                    "⚠️ *Advertencia*: Pocos mensajes recibidos en los últimos minutos. "
                    "Posible problema de conexión con Cielo API."
                )
            
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

async def maintenance_check_task():
    """
    Monitoriza el estado del sistema y envía alertas si hay problemas.
    """
    global message_counter, last_counter_log
    
    last_processed_msg_time = time.time()
    
    while running:
        try:
            await asyncio.sleep(900)  # Verificar cada 15 minutos
            
            # Verificar si el bot está activo, si no, omitir verificación
            if is_bot_active and not is_bot_active():
                logger.info("⏸️ Bot en modo inactivo, omitiendo verificación de mantenimiento")
                continue
            
            now = time.time()
            # Si no se han procesado mensajes en 30 minutos, puede haber un problema
            if now - last_processed_msg_time > 1800 and message_counter < 5:
                send_telegram_message(
                    "⚠️ *Alerta de Mantenimiento*\n"
                    "No se han recibido suficientes mensajes en los últimos 30 minutos.\n"
                    "Posible problema de conexión con Cielo API."
                )
            
            # Verificar conexión a BD
            try:
                db.count_signals_today()
                # Actualizar tiempo si todo funciona
                if message_counter > 0:
                    last_processed_msg_time = now
            except Exception as e:
                send_telegram_message(
                    f"⚠️ *Alerta de Base de Datos*\n"
                    f"Error al conectar con la base de datos: {e}"
                )
        except Exception as e:
            logger.error(f"Error en tarea de mantenimiento: {e}")
            # No hacer nada más, simplemente continuar en la próxima iteración

async def on_cielo_message(message, signal_logic, ml_data_preparation, dex_client, scoring_system, signal_predictor=None):
    """
    Procesa los mensajes recibidos de Cielo.
    """
    global message_counter, transaction_counter, wallets_list, is_bot_active
    
    # Verificar si el bot está activo
    if is_bot_active and not is_bot_active():
        logger.info("⏸️ Bot en modo inactivo, ignorando mensaje")
        return
    
    # Incrementar contador de mensajes
    message_counter += 1
    
    try:
        # Log para depuración inicial del mensaje completo
        logger.debug(f"Mensaje recibido (primeros 200 caracteres): {message[:200]}...")
        
        data = json.loads(message)
        
        # Loguear tipo de mensaje
        if "type" in data:
            logger.info(f"📡 Mensaje tipo: {data['type']}")
        
        # Procesar transacción si es un mensaje de tipo 'tx'
        if data.get("type") == "tx" and "data" in data:
            tx_data = data["data"]
            logger.info(f"📦 Mensaje de transacción recibido: {json.dumps(tx_data)[:200]}...")
            
            try:
                # Verificar que tenemos wallet
                if "wallet" not in tx_data:
                    logger.warning("⚠️ Transacción sin wallet, ignorando")
                    return
                
                # Extraer wallet
                wallet = tx_data.get("wallet", "unknown_wallet")
                
                # Verificar si la wallet está en nuestra lista - Solo procesar si está
                if wallet not in wallets_list:
                    logger.info(f"⚠️ Wallet {wallet[:8]}... no está en la lista de seguimiento, ignorando")
                    return
                
                # Determinar tipo de transacción
                tx_type = tx_data.get("tx_type", "unknown_type")
                token = None
                actual_tx_type = None
                usd_value = 0.0
                
                # Procesar según tipo de transacción
                if tx_type == "transfer":
                    # Para transferencias, usar contract_address
                    token = tx_data.get("contract_address")
                    
                    # En transferencias, si la wallet está en 'to', es BUY; si está en 'from', es SELL
                    if wallet == tx_data.get("to"):
                        actual_tx_type = "BUY"
                    elif wallet == tx_data.get("from"):
                        actual_tx_type = "SELL"
                    
                    # Obtener valor USD
                    usd_value = float(tx_data.get("amount_usd", 0))
                    
                elif tx_type == "swap":
                    # Para swaps, necesitamos determinar qué token y dirección
                    token0 = tx_data.get("token0_address")
                    token1 = tx_data.get("token1_address")
                    
                    # Si ambos están presentes, determinar dirección y token
                    if token0 and token1:
                        # SOL y tokens comunes generalmente son token1 en swaps
                        sol_token = "So11111111111111111111111111111111111111112"
                        usdc_token = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
                        usdt_token = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
                        common_tokens = [sol_token, usdc_token, usdt_token]
                        
                        if token1 in common_tokens:
                            # Si token1 es SOL/USDC/USDT, entonces token0 es el token de interés
                            token = token0
                            actual_tx_type = "SELL"  # Vendiendo token por SOL/stablecoin
                            usd_value = float(tx_data.get("token0_amount_usd", 0))
                        else:
                            # En otro caso, token1 es probablemente el token de interés
                            token = token1
                            actual_tx_type = "BUY"  # Comprando token con SOL/stablecoin
                            usd_value = float(tx_data.get("token1_amount_usd", 0))
                    
                # Si no pudimos determinar token o tipo, ignorar
                if not token or not actual_tx_type:
                    logger.warning("⚠️ No se pudo determinar token o tipo de transacción, ignorando")
                    return
                
                # Si no hay valor USD, ignorar la transacción
                min_tx_usd = float(Config.get("min_transaction_usd", Config.MIN_TRANSACTION_USD))
                if usd_value <= 0 or usd_value < min_tx_usd:
                    logger.info(f"⚠️ Transacción con valor insuficiente (${usd_value}), ignorando")
                    return
                
                # Incrementar contador
                transaction_counter += 1
                
                logger.info(f"💵 Transacción relevante: {actual_tx_type} {usd_value}$ en {token} por {wallet}")
                
                # Guardar en la BD
                tx_for_db = {
                    "wallet": wallet,
                    "token": token,
                    "type": actual_tx_type,
                    "amount_usd": usd_value
                }
                
                try:
                    db.save_transaction(tx_for_db)
                    logger.info(f"✅ Transacción guardada en BD")
                except Exception as e:
                    logger.error(f"🚨 Error guardando transacción en BD: {e}")
                
                # Añadir a la lógica de señales
                try:
                    signal_logic.add_transaction(wallet, token, usd_value, actual_tx_type)
                    logger.info(f"✅ Transacción añadida a lógica de señales")
                except Exception as e:
                    logger.error(f"🚨 Error añadiendo transacción a señales: {e}")
                
            except Exception as tx_e:
                logger.error(f"🚨 Error procesando transacción individual: {tx_e}", exc_info=True)
    
    except json.JSONDecodeError as e:
        logger.warning(f"⚠️ Error al decodificar JSON: {e}")
    except Exception as e:
        logger.error(f"🚨 Error en on_cielo_message: {e}", exc_info=True)

async def log_raw_message(message):
    """
    Registra el mensaje completo recibido para debugging.
    """
    try:
        # Intentar parsear como JSON para un formato más legible
        data = json.loads(message)
        logger.info(f"\n-------- MENSAJE CIELO RECIBIDO --------")
        logger.info(f"Tipo de mensaje: {data.get('type', 'No type')}")
        
        # Si contiene datos de transacción en el nuevo formato
        if data.get("type") == "tx" and "data" in data:
            tx_data = data["data"]
            logger.info(f"Contiene datos de transacción")
            
            # Mostrar detalles relevantes
            logger.info(f"  Wallet: {tx_data.get('wallet', 'N/A')}")
            
            # Mostrar token según el tipo de transacción
            if tx_data.get("tx_type") == "transfer":
                logger.info(f"  Token: {tx_data.get('contract_address', 'N/A')}")
            elif tx_data.get("tx_type") == "swap":
                logger.info(f"  Token0: {tx_data.get('token0_address', 'N/A')}")
                logger.info(f"  Token1: {tx_data.get('token1_address', 'N/A')}")
            
            logger.info(f"  Tipo: {tx_data.get('tx_type', 'N/A')}")
            
            # Mostrar detalles de valor
            if "amount_usd" in tx_data:
                logger.info(f"  Valor USD: {tx_data.get('amount_usd')} USD")
            if "token0_amount_usd" in tx_data:
                logger.info(f"  Valor token0: {tx_data.get('token0_amount_usd')} USD")
            if "token1_amount_usd" in tx_data:
                logger.info(f"  Valor token1: {tx_data.get('token1_amount_usd')} USD")
                
        logger.info("----------------------------------------\n")
    except Exception as e:
        logger.error(f"Error al loguear mensaje: {e}")
        logger.info(f"Mensaje original: {message[:200]}...")

async def manage_websocket_connection(cielo_api, wallets_list, handle_message, filter_params):
    """
    Función que maneja la conexión WebSocket de Cielo basada en el estado del bot.
    
    Args:
        cielo_api: Instancia de CieloAPI
        wallets_list: Lista de wallets a monitorear
        handle_message: Callback para mensajes recibidos
        filter_params: Parámetros de filtro para la API
    """
    global cielo_ws_connection, is_bot_active
    
    while running:
        # Verificar si el bot está activo
        bot_active = is_bot_active and is_bot_active()
        
        if bot_active and cielo_ws_connection is None:
            # Si el bot está activo y no hay conexión, iniciarla
            logger.info("🔄 Iniciando conexión WebSocket con Cielo API...")
            cielo_ws_connection = asyncio.create_task(
                cielo_api.run_forever_wallets(wallets_list, handle_message, filter_params)
            )
            send_telegram_message("📡 *Conexión a Cielo API iniciada*\nEl bot está comenzando a procesar transacciones.")
        
        elif not bot_active and cielo_ws_connection is not None:
            # Si el bot está inactivo y hay conexión, cancelarla
            logger.info("🛑 Cancelando conexión WebSocket con Cielo API...")
            cielo_ws_connection.cancel()
            cielo_ws_connection = None
            send_telegram_message("🔌 *Conexión a Cielo API detenida*\nBot en modo inactivo para ahorrar créditos API.")
        
        # Verificar estado cada 30 segundos
        await asyncio.sleep(30)

def handle_shutdown(sig, frame):
    """
    Maneja el cierre del programa.
    """
    global running, cielo_ws_connection
    print("\n🛑 Señal de cierre recibida, terminando tareas...")
    running = False
    
    # Cancelar conexión WebSocket si existe
    if cielo_ws_connection is not None:
        cielo_ws_connection.cancel()
        cielo_ws_connection = None
    
    # Intentar guardar estados antes de salir
    try:
        if ml_data_preparation:
            ml_data_preparation.save_features_to_csv()
            ml_data_preparation.save_outcomes_to_csv()
        print("✅ Estados guardados correctamente")
    except Exception as e:
        print(f"⚠️ Error guardando estados: {e}")
    
    # Enviar mensaje de apagado
    try:
        send_telegram_message("🛑 *ChipaTrading Bot se está apagando*\nEl servicio está siendo detenido o reiniciado.")
    except:
        pass
    
    print("👋 ChipaTrading Bot finalizado")
    sys.exit(0)

async def main():
    """
    Función principal que inicializa y ejecuta el bot.
    """
    global wallets_list, signal_predictor, ml_data_preparation, last_heartbeat, is_bot_active
    
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
        
        # 5. Configurar RugCheck a través de la clase RugCheckAPI
        rugcheck_api = None
        if Config.RUGCHECK_PRIVATE_KEY and Config.RUGCHECK_WALLET_PUBKEY:
            try:
                logger.info("🔐 Inicializando RugCheck API...")
                rugcheck_api = RugCheckAPI()
                # Intentamos autenticar para verificar que funciona
                jwt_token = rugcheck_api.authenticate()
                
                if not jwt_token:
                    logger.warning("⚠️ No se pudo obtener token JWT de RugCheck")
                    send_telegram_message("⚠️ *Advertencia*: No se pudo conectar con RugCheck para validar tokens")
                else:
                    logger.info("✅ RugCheck API inicializada correctamente")
            except Exception as e:
                logger.error(f"⚠️ Error al configurar RugCheck: {e}")
                rugcheck_api = None
        
        # 6. Inicializar componentes principales
        logger.info("⚙️ Inicializando componentes...")
        
        dex_client = DexScreenerClient()
        scoring_system = ScoringSystem()
        
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
        
        # 9. Inicializar SignalLogic con todos los componentes
        signal_logic = SignalLogic(
            scoring_system=scoring_system, 
            dex_client=dex_client, 
            rugcheck_api=rugcheck_api,
            ml_predictor=signal_predictor
        )
        
        # 10. Iniciar bot de comandos de Telegram
        logger.info("🤖 Iniciando bot de comandos de Telegram...")
        is_bot_active = await process_telegram_commands(
            Config.TELEGRAM_BOT_TOKEN,
            Config.TELEGRAM_CHAT_ID,
            signal_logic
        )
        
        # 11. Iniciar tareas periódicas en segundo plano
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
        
        # Tarea de verificación de mantenimiento
        asyncio.create_task(maintenance_check_task())
        
        # 12. Configurar callback para mensajes de Cielo
        async def handle_message(msg):
            await log_raw_message(msg)  # Primero hacer log del mensaje completo
            await on_cielo_message(msg, signal_logic, ml_data_preparation, dex_client, scoring_system, signal_predictor)
        
        # 13. Configurar parámetros de filtro para Cielo API
        filter_params = {
            "chains": ["solana"],
            "tx_types": ["swap", "transfer"],  # Incluir swaps y transferencias
            # Ya no filtramos por valor mínimo en la suscripción
        }
        
        # 14. Enviar mensaje de inicio exitoso
        send_telegram_message(
            "🚀 *ChipaTrading Bot Iniciado*\n\n"
            f"• Wallets monitoreadas: `{len(wallets_list)}`\n"
            f"• Validación RugCheck: `{'Activa' if rugcheck_api else 'Inactiva'}`\n"
            f"• Modelo ML: `{'Cargado' if model_loaded else 'Pendiente'}`\n"
            f"• Comando bot: `/status` para ver estado actual\n\n"
            "Sistema listo para detectar señales Daily Runner. ¡Buenas operaciones! 📊"
        )
        
        # 15. Iniciar manejo de conexión WebSocket con Cielo (maneja conexión según estado del bot)
        logger.info("📡 Iniciando gestión de conexión con Cielo API...")
        cielo = CieloAPI(Config.CIELO_API_KEY)
        
        # 16. Iniciar tarea de gestión de conexión WebSocket que se activa/desactiva según estado del bot
        await manage_websocket_connection(cielo, wallets_list, handle_message, filter_params)
        
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
