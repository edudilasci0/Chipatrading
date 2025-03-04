import asyncio
import json
import os
import sys
import time
import signal
import logging
import traceback
import threading
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

# Configurar logging más avanzado
def setup_logging():
    """
    Configura el sistema de logging con niveles y rotación de archivos.
    """
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    
    # Archivo de log principal
    log_file = os.path.join(log_dir, "chipatrading.log")
    
    # Configurar nivel de log base
    log_level = getattr(logging, Config.get("LOG_LEVEL", "INFO"))
    
    # Configurar logger principal
    logger = logging.getLogger("chipatrading")
    logger.setLevel(log_level)
    
    # Handler para consola
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_format)
    
    # Handler para archivo
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(log_level)
    file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_format)
    
    # Aplicar handlers
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    # Configurar también loggers de componentes individuales
    for component in ["database", "dexscreener", "ml_preparation", "signal_predictor"]:
        component_logger = logging.getLogger(component)
        component_logger.setLevel(log_level)
        component_logger.addHandler(console_handler)
        component_logger.addHandler(file_handler)
    
    return logger

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
# NUEVO: Variables para estadísticas y rendimiento
performance_stats = {
    "start_time": time.time(),
    "signals_emitted": 0,
    "transactions_processed": 0,
    "errors": 0,
    "last_error": None,
    "memory_usage": 0
}

# NUEVO: Lock para actualización segura de estadísticas
stats_lock = threading.Lock()

# NUEVO: Cache de wallets
wallet_cache = {
    "last_update": 0,
    "wallets": []
}

# Inicializar logger
logger = setup_logging()

def load_wallets():
    """
    Carga las wallets desde traders_data.json con cache
    
    Returns:
        list: Lista de direcciones de wallets
    """
    global wallet_cache
    
    # Comprobar si tenemos cache reciente (menos de 10 minutos)
    cache_age = time.time() - wallet_cache["last_update"]
    if wallet_cache["wallets"] and cache_age < 600:
        logger.debug(f"Usando caché de wallets ({len(wallet_cache['wallets'])} wallets, edad: {cache_age:.1f}s)")
        return wallet_cache["wallets"]
    
    try:
        # Verificar tiempo de última modificación del archivo
        file_path = "traders_data.json"
        file_mod_time = os.path.getmtime(file_path)
        
        # Si el archivo no ha cambiado y tenemos wallets en cache, usar cache
        if wallet_cache["wallets"] and file_mod_time < wallet_cache["last_update"]:
            return wallet_cache["wallets"]
            
        with open(file_path, "r") as f:
            data = json.load(f)
            wallets = [entry["Wallet"] for entry in data]
            logger.info(f"📋 Se cargaron {len(wallets)} wallets desde traders_data.json")
            
            # Actualizar cache
            wallet_cache["wallets"] = wallets
            wallet_cache["last_update"] = time.time()
            
            return wallets
    except FileNotFoundError:
        logger.warning("⚠️ No se encontró el archivo traders_data.json")
        return wallet_cache["wallets"] if wallet_cache["wallets"] else []
    except json.JSONDecodeError:
        logger.error("🚨 Error decodificando JSON en traders_data.json")
        return wallet_cache["wallets"] if wallet_cache["wallets"] else []
    except Exception as e:
        logger.error(f"🚨 Error al cargar traders_data.json: {e}")
        return wallet_cache["wallets"] if wallet_cache["wallets"] else []

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

async def daily_summary_task(signal_logic, signal_predictor=None, dex_client=None):
    """
    Envía un resumen diario de actividad mejorado con más estadísticas.
    
    Args:
        signal_logic: Instancia de SignalLogic
        signal_predictor: Instancia de SignalPredictor o None
        dex_client: Instancia de DexScreenerClient o None
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
            
            # NUEVO: Calcular uso de memoria
            import psutil
            process = psutil.Process(os.getpid())
            memory_usage = process.memory_info().rss / 1024 / 1024  # MB
            
            # Obtener estadísticas de DB
            db_stats = db.get_db_stats()
            
            # Obtener datos para el resumen
            signals_today = db.count_signals_today()
            active_tokens = signal_logic.get_active_candidates_count()
            tx_today = db.count_transactions_today()
            recent_signals = signal_logic.get_recent_signals(hours=24)
            
            # NUEVO: Obtener stats de caché
            cache_stats = {}
            if dex_client:
                cache_stats["dexscreener"] = dex_client.get_cache_stats()
            
            # NUEVO: Estadísticas generales de rendimiento
            uptime_hours = (time.time() - performance_stats["start_time"]) / 3600
            
            # Preparar mensaje base
            summary_msg = (
                "*📈 Resumen Diario ChipaTrading*\n\n"
                f"• Señales emitidas hoy: `{signals_today}`\n"
                f"• Tokens monitoreados: `{active_tokens}`\n"
                f"• Transacciones procesadas: `{tx_today}`\n"
                f"• Wallets monitoreadas: `{len(wallets_list)}`\n\n"
                
                f"*🖥️ Estado del Sistema:*\n"
                f"• Uptime: `{uptime_hours:.1f}h`\n"
                f"• Memoria: `{memory_usage:.1f} MB`\n"
                f"• Transacciones totales: `{db_stats.get('transactions_count', 'N/A')}`\n"
                f"• Estado: `{'Activo' if is_bot_active and is_bot_active() else 'Inactivo'}`\n\n"
            )
            
            # Obtener estadísticas de rendimiento
            try:
                stats = db.get_signals_performance_stats()
                if stats:
                    summary_msg += "*📊 Rendimiento de Señales*\n"
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
                    f"• Precision: `{model_info.get('precision', 'N/A')}`\n"
                    f"• Recall: `{model_info.get('recall', 'N/A')}`\n"
                    f"• Ejemplos: `{model_info['sample_count']}`\n"
                )
                
                # NUEVO: Top features
                if model_info.get('top_features'):
                    summary_msg += "• Top Features:\n"
                    for feature, importance in model_info['top_features']:
                        summary_msg += f"  - `{feature}`: `{importance:.3f}`\n"
            
            # Añadir resumen de señales recientes si hay alguna
            if recent_signals:
                summary_msg += "\n*🔍 Últimas Señales (24h)*\n"
                for token, ts, conf, sig_id in recent_signals[:5]:
                    ts_str = datetime.fromtimestamp(ts).strftime('%H:%M')
                    summary_msg += f"• {sig_id} `{token[:10]}...` - Confianza: {conf:.2f} ({ts_str})\n"
                
                if len(recent_signals) > 5:
                    summary_msg += f"...y {len(recent_signals) - 5} más\n"
            
            summary_msg += "\nEl sistema continúa monitoreando wallets. ¡Buenas operaciones! 📊"
            
            # Enviar mensaje
            send_telegram_message(summary_msg)
            logger.info("📊 Resumen diario enviado")
            
        except Exception as e:
            logger.error(f"⚠️ Error en resumen diario: {e}", exc_info=True)
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
            new_wallets = load_wallets()
            if not new_wallets:
                logger.warning("⚠️ No se pudieron cargar wallets, manteniendo lista actual")
                await asyncio.sleep(interval)
                continue
                
            if wallets_list:
                original_count = len(wallets_list)
                new_count = len(new_wallets)
                if abs(new_count - original_count) > 10 or new_count == 0:
                    logger.warning(f"Cambio significativo en wallets: {original_count} → {new_count}")
                    if new_count < original_count * 0.5 and original_count > 20:
                        logger.error(f"⚠️ Reducción drástica en wallets! (más del 50%) Manteniendo lista anterior")
                        send_telegram_message(
                            "⚠️ *Alerta de Seguridad*\n\n"
                            f"Detectada reducción drástica en la lista de wallets: {original_count} → {new_count}.\n"
                            "Por precaución, se mantiene la lista anterior."
                        )
                        await asyncio.sleep(interval)
                        continue
            
            added_count = 0
            for wallet in new_wallets:
                if wallet not in wallets_list:
                    wallets_list.append(wallet)
                    added_count += 1
            
            if added_count > 0:
                logger.info(f"📋 Se añadieron {added_count} nuevas wallets, total: {len(wallets_list)}")
                if added_count >= 5:
                    send_telegram_message(
                        f"*📋 Actualización de Wallets*\nSe han añadido {added_count} nuevas wallets para monitorear.\nTotal: {len(wallets_list)} wallets"
                    )
            
            await asyncio.sleep(interval)
            
        except Exception as e:
            logger.error(f"⚠️ Error actualizando wallets: {e}", exc_info=True)
            await asyncio.sleep(interval)

async def monitoring_task():
    """
    Tarea periódica para monitorizar el estado del bot y enviar alertas.
    Versión mejorada con más métricas.
    """
    global message_counter, transaction_counter, last_counter_log, last_heartbeat, running
    
    while running:
        try:
            await asyncio.sleep(600)
            
            if is_bot_active and not is_bot_active():
                logger.info("⏸️ Bot en modo inactivo, omitiendo mensaje de estado")
                continue
            
            now = time.time()
            elapsed_since_last_log = now - last_counter_log
            
            import psutil
            process = psutil.Process(os.getpid())
            memory_usage = process.memory_info().rss / 1024 / 1024  # MB
            cpu_percent = process.cpu_percent(interval=1.0)
            
            with stats_lock:
                performance_stats["memory_usage"] = memory_usage
            
            msg = (
                "*📊 Estado del Bot*\n\n"
                f"• Mensajes recibidos (últimos {elapsed_since_last_log/60:.1f} min): `{message_counter}`\n"
                f"• Transacciones procesadas: `{transaction_counter}`\n"
                f"• Wallets monitoreadas: `{len(wallets_list)}`\n"
                f"• Tiempo desde inicio: `{(now - last_heartbeat)/3600:.1f}h`\n"
                f"• Estado: `{'Activo' if is_bot_active and is_bot_active() else 'Inactivo'}`\n\n"
                f"*🖥️ Recursos:*\n"
                f"• Memoria: `{memory_usage:.1f} MB`\n"
                f"• CPU: `{cpu_percent:.1f}%`\n"
            )
            
            with stats_lock:
                if performance_stats["last_error"]:
                    error_time, error_msg = performance_stats["last_error"]
                    time_since_error = now - error_time
                    if time_since_error < 3600:
                        msg += f"\n⚠️ *Último error* ({time_since_error/60:.0f}m atrás):\n`{error_msg[:100]}`\n"
            
            send_telegram_message(msg)
            logger.info(f"📊 Mensajes: {message_counter}, Transacciones: {transaction_counter} en los últimos {elapsed_since_last_log/60:.1f} minutos")
            
            if message_counter < 5 and elapsed_since_last_log > 300:
                send_telegram_message(
                    "⚠️ *Advertencia*: Pocos mensajes recibidos en los últimos minutos. "
                    "Posible problema de conexión con Cielo API."
                )
            
            message_counter = 0
            transaction_counter = 0
            last_counter_log = now
            
        except Exception as e:
            logger.error(f"⚠️ Error en monitoring_task: {e}", exc_info=True)
            with stats_lock:
                performance_stats["errors"] += 1
                performance_stats["last_error"] = (time.time(), str(e))
            await asyncio.sleep(60)

async def ml_periodic_tasks(ml_data_preparation, dex_client, signal_predictor, interval=86400):
    """
    Ejecuta tareas periódicas relacionadas con Machine Learning
    Versión mejorada con tracking y notificaciones más detalladas.
    
    Args:
        ml_data_preparation: Instancia de MLDataPreparation
        dex_client: Instancia de DexScreenerClient
        signal_predictor: Instancia de SignalPredictor
        interval: Intervalo en segundos entre ejecuciones
    """
    while running:
        try:
            logger.info("🧠 Ejecutando tareas periódicas de ML...")
            send_telegram_message(
                "🧠 *Iniciando actualización del modelo ML*\n"
                "Recolectando outcomes y preparando datos de entrenamiento..."
            )
            
            start_time = time.time()
            outcomes_count = ml_data_preparation.collect_signal_outcomes(dex_client)
            outcome_time = time.time() - start_time
            
            old_accuracy = signal_predictor.accuracy if signal_predictor and signal_predictor.model else None
            model_improvement = "N/A"
            
            if outcomes_count >= 3:
                logger.info(f"🧠 Intentando entrenar modelo con {outcomes_count} nuevos outcomes...")
                ml_data_preparation.analyze_feature_correlations()
                start_time = time.time()
                training_df = ml_data_preparation.prepare_training_data()
                prepare_time = time.time() - start_time
                
                if training_df is not None and len(training_df) >= 20:
                    start_time = time.time()
                    trained = signal_predictor.train_model(force=(outcomes_count >= 5))
                    train_time = time.time() - start_time
                    
                    if trained:
                        new_accuracy = signal_predictor.accuracy
                        if old_accuracy:
                            improvement = (new_accuracy - old_accuracy) * 100
                            model_improvement = f"{improvement:+.2f}%"
                        
                        logger.info(f"🧠 Modelo entrenado exitosamente en {train_time:.1f}s")
                        feature_analysis = signal_predictor.feature_analysis()
                        model_info = signal_predictor.get_model_info()
                        msg = (
                            f"*🧠 Modelo ML Actualizado*\n\n"
                            f"• Exactitud: `{model_info['accuracy']:.2f}` ({model_improvement})\n"
                            f"• Precision: `{model_info.get('precision', 'N/A'):.2f}`\n"
                            f"• Recall: `{model_info.get('recall', 'N/A'):.2f}`\n"
                            f"• F1-Score: `{model_info.get('f1_score', 'N/A'):.2f}`\n"
                            f"• Ejemplos: `{model_info['sample_count']}`\n\n"
                        )
                        
                        if feature_analysis and 'top_features' in feature_analysis:
                            msg += "*Features más importantes:*\n"
                            for i, (feature, importance) in enumerate(feature_analysis['top_features'][:5]):
                                msg += f"{i+1}. `{feature}`: `{importance:.3f}`\n"
                        if feature_analysis and 'insights' in feature_analysis:
                            msg += "\n*Insights del modelo:*\n"
                            for insight in feature_analysis['insights']:
                                msg += f"• {insight}\n"
                        
                        msg += f"\nEl modelo se usará para evaluar la calidad de nuevas señales."
                        send_telegram_message(msg)
                    else:
                        logger.info("🧠 No fue necesario reentrenar el modelo")
                        send_telegram_message(
                            "🧠 *Actualización ML completada*\n"
                            f"Se recolectaron {outcomes_count} nuevos outcomes, pero el modelo actual sigue siendo óptimo."
                        )
            
            ml_data_preparation.clean_old_data(days=90)
            logger.info(f"🧠 Próxima ejecución de tareas ML en {interval/3600:.1f} horas")
            await asyncio.sleep(interval)
            
        except Exception as e:
            logger.error(f"⚠️ Error en tareas ML: {e}", exc_info=True)
            with stats_lock:
                performance_stats["errors"] += 1
                performance_stats["last_error"] = (time.time(), f"Error ML: {str(e)}")
            send_telegram_message(
                "⚠️ *Error en proceso ML*\n"
                f"Ocurrió un error durante la actualización del modelo: `{str(e)[:100]}`\n"
                "Se reintentará en la próxima ejecución programada."
            )
            await asyncio.sleep(interval)

async def maintenance_check_task():
    """
    Monitoriza el estado del sistema y envía alertas si hay problemas.
    Versión mejorada con verificaciones adicionales de recursos.
    """
    global message_counter, last_counter_log
    
    last_processed_msg_time = time.time()
    
    while running:
        try:
            await asyncio.sleep(900)
            
            if is_bot_active and not is_bot_active():
                logger.info("⏸️ Bot en modo inactivo, omitiendo verificación de mantenimiento")
                continue
            
            now = time.time()
            
            try:
                import psutil
                cpu_percent = psutil.cpu_percent(interval=1)
                if cpu_percent > 80:
                    send_telegram_message(
                        "⚠️ *Alerta de Recursos*\n"
                        f"Uso elevado de CPU: {cpu_percent:.1f}%\n"
                        "Esto puede afectar el rendimiento del bot."
                    )
                memory = psutil.virtual_memory()
                if memory.percent > 85:
                    send_telegram_message(
                        "⚠️ *Alerta de Recursos*\n"
                        f"Uso elevado de memoria: {memory.percent:.1f}%\n"
                        "Esto puede afectar la estabilidad del sistema."
                    )
                disk = psutil.disk_usage('/')
                if disk.percent > 90:
                    send_telegram_message(
                        "⚠️ *Alerta de Recursos*\n"
                        f"Espacio en disco bajo: {disk.percent:.1f}%\n"
                        "Es recomendable liberar espacio."
                    )
            except:
                pass
            
            if now - last_processed_msg_time > 1800 and message_counter < 5:
                send_telegram_message(
                    "⚠️ *Alerta de Mantenimiento*\n"
                    "No se han recibido suficientes mensajes en los últimos 30 minutos.\n"
                    "Posible problema de conexión con Cielo API."
                )
            
            try:
                signal_count = db.count_signals_today()
                db_stats = db.get_db_stats()
                if 'db_size_bytes' in db_stats and db_stats['db_size_bytes'] > 1_000_000_000:
                    send_telegram_message(
                        "⚠️ *Alerta de Base de Datos*\n"
                        f"La base de datos ha crecido considerablemente: {db_stats['db_size_pretty']}\n"
                        "Considere ejecutar tareas de limpieza."
                    )
                if message_counter > 0:
                    last_processed_msg_time = now
            except Exception as e:
                send_telegram_message(
                    f"⚠️ *Alerta de Base de Datos*\n"
                    f"Error al conectar con la base de datos: {e}"
                )
                with stats_lock:
                    performance_stats["errors"] += 1
                    performance_stats["last_error"] = (time.time(), f"Error DB: {str(e)}")
                
        except Exception as e:
            logger.error(f"Error en tarea de mantenimiento: {e}", exc_info=True)
            with stats_lock:
                performance_stats["errors"] += 1
                performance_stats["last_error"] = (time.time(), str(e))

async def on_cielo_message(message, signal_logic, ml_data_preparation, dex_client, scoring_system, signal_predictor=None):
    """
    Procesa los mensajes recibidos de Cielo.
    Versión optimizada con mejor manejo de errores y rastreo de rendimiento.
    """
    global message_counter, transaction_counter, wallets_list, is_bot_active
    
    if is_bot_active and not is_bot_active():
        logger.debug("⏸️ Bot en modo inactivo, ignorando mensaje")
        return
    
    message_counter += 1
    
    try:
        logger.debug(f"Mensaje recibido (primeros 200 caracteres): {message[:200]}...")
        
        data = json.loads(message)
        
        if "type" in data:
            logger.debug(f"📡 Mensaje tipo: {data['type']}")
        
        if data.get("type") == "tx" and "data" in data:
            tx_data = data["data"]
            logger.debug(f"📦 Mensaje de transacción recibido: {json.dumps(tx_data)[:200]}...")
            
            try:
                if "wallet" not in tx_data:
                    logger.warning("⚠️ Transacción sin wallet, ignorando")
                    return
                
                wallet = tx_data.get("wallet", "unknown_wallet")
                
                wallet_set = set(wallets_list)
                if wallet not in wallet_set:
                    logger.debug(f"⚠️ Wallet {wallet[:8]}... no está en la lista de seguimiento, ignorando")
                    return
                
                tx_type = tx_data.get("tx_type", "unknown_type")
                token = None
                actual_tx_type = None
                usd_value = 0.0
                
                if tx_type == "transfer":
                    token = tx_data.get("contract_address")
                    
                    if wallet == tx_data.get("to"):
                        actual_tx_type = "BUY"
                    elif wallet == tx_data.get("from"):
                        actual_tx_type = "SELL"
                    
                    usd_value = float(tx_data.get("amount_usd", 0))
                    
                elif tx_type == "swap":
                    token0 = tx_data.get("token0_address")
                    token1 = tx_data.get("token1_address")
                    
                    if token0 and token1:
                        sol_token = "So11111111111111111111111111111111111111112"
                        usdc_token = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
                        usdt_token = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
                        common_tokens = [sol_token, usdc_token, usdt_token]
                        
                        if token1 in common_tokens:
                            token = token0
                            actual_tx_type = "SELL"
                            usd_value = float(tx_data.get("token0_amount_usd", 0))
                        else:
                            token = token1
                            actual_tx_type = "BUY"
                            usd_value = float(tx_data.get("token1_amount_usd", 0))
                    
                if not token or not actual_tx_type:
                    logger.warning("⚠️ No se pudo determinar token o tipo de transacción, ignorando")
                    return
                
                min_tx_usd = float(Config.get("min_transaction_usd", Config.MIN_TRANSACTION_USD))
                if usd_value <= 0 or usd_value < min_tx_usd:
                    logger.debug(f"⚠️ Transacción con valor insuficiente (${usd_value}), ignorando")
                    return
                
                start_time = time.time()
                
                transaction_counter += 1
                with stats_lock:
                    performance_stats["transactions_processed"] += 1
                
                logger.info(f"💵 Transacción relevante: {actual_tx_type} {usd_value}$ en {token} por {wallet}")
                
                tx_for_db = {
                    "wallet": wallet,
                    "token": token,
                    "type": actual_tx_type,
                    "amount_usd": usd_value
                }
                
                try:
                    db.save_transaction(tx_for_db)
                    logger.debug(f"✅ Transacción guardada en BD")
                except Exception as e:
                    logger.error(f"🚨 Error guardando transacción en BD: {e}")
                    with stats_lock:
                        performance_stats["errors"] += 1
                        performance_stats["last_error"] = (time.time(), f"Error DB: {str(e)}")
                
                if actual_tx_type == "SELL" and scoring_system.wallet_token_buys:
                    wallet_token_key = f"{wallet}:{token}"
                    if wallet_token_key in scoring_system.wallet_token_buys:
                        try:
                            buy_data = scoring_system.wallet_token_buys[wallet_token_key]
                            buy_amount = buy_data['amount_usd']
                            buy_time = buy_data['timestamp']
                            hold_time_hours = (time.time() - buy_time) / 3600
                            
                            if usd_value > buy_amount:
                                profit_percent = (usd_value - buy_amount) / buy_amount
                                db.save_wallet_profit(
                                    wallet, token, buy_amount, usd_value, 
                                    profit_percent, hold_time_hours, buy_time
                                )
                                logger.info(f"💰 Profit registrado: {wallet} {profit_percent:.2%} en {hold_time_hours:.1f}h")
                        except Exception as e:
                            logger.error(f"Error registrando profit: {e}")
                
                try:
                    scoring_system.update_score_on_trade(wallet, {
                        "type": actual_tx_type,
                        "token": token,
                        "amount_usd": usd_value
                    })
                except Exception as e:
                    logger.error(f"Error actualizando score: {e}")
                
                try:
                    price = await dex_client.get_token_price(token)
                    if price > 0:
                        db.update_token_metadata(token, max_price=price, max_volume=usd_value)
                except Exception as e:
                    logger.error(f"Error actualizando metadatos del token: {e}")
                
                try:
                    signal_logic.add_transaction(wallet, token, usd_value, actual_tx_type)
                    logger.debug(f"✅ Transacción añadida a lógica de señales")
                except Exception as e:
                    logger.error(f"🚨 Error añadiendo transacción a señales: {e}")
                    with stats_lock:
                        performance_stats["errors"] += 1
                        performance_stats["last_error"] = (time.time(), f"Error Signal Logic: {str(e)}")
                
                processing_time = time.time() - start_time
                logger.debug(f"⏱️ Procesamiento completado en {processing_time*1000:.2f}ms")
                
            except Exception as tx_e:
                logger.error(f"🚨 Error procesando transacción individual: {tx_e}", exc_info=True)
                with stats_lock:
                    performance_stats["errors"] += 1
                    performance_stats["last_error"] = (time.time(), f"Error TX: {str(tx_e)}")
    
    except json.JSONDecodeError as e:
        logger.warning(f"⚠️ Error al decodificar JSON: {e}")
    except Exception as e:
        logger.error(f"🚨 Error en on_cielo_message: {e}", exc_info=True)
        with stats_lock:
            performance_stats["errors"] += 1
            performance_stats["last_error"] = (time.time(), str(e))
            if performance_stats["errors"] >= 5:
                send_telegram_message(
                    "🚨 *Error crítico de conexión*\n"
                    f"Múltiples fallos al gestionar la conexión: {str(e)}\n"
                    "Verificar logs para más detalles."
                )
                performance_stats["errors"] = 0
            await asyncio.sleep(60)

def handle_shutdown(sig, frame):
    """
    Maneja el cierre del programa.
    Versión mejorada con cleanup de recursos adicionales.
    """
    global running, cielo_ws_connection
    print("\n🛑 Señal de cierre recibida, terminando tareas...")
    running = False
    
    if cielo_ws_connection is not None:
        cielo_ws_connection.cancel()
        cielo_ws_connection = None
    
    try:
        if ml_data_preparation:
            ml_data_preparation.save_features_to_csv()
            ml_data_preparation.save_outcomes_to_csv()
        print("✅ Estados de ML guardados correctamente")
        
        try:
            dex_client = getattr(sys.modules[__name__], 'dex_client', None)
            if dex_client:
                dex_client.shutdown()
                print("✅ Recursos de DexScreener liberados")
        except:
            print("⚠️ No se pudieron liberar recursos de DexScreener")
            
    except Exception as e:
        print(f"⚠️ Error guardando estados: {e}")
    
    try:
        send_telegram_message("🛑 *ChipaTrading Bot se está apagando*\nEl servicio está siendo detenido o reiniciado.")
    except:
        pass
    
    print("👋 ChipaTrading Bot finalizado")
    sys.exit(0)

async def log_raw_message(message):
    """Registra los mensajes crudos recibidos del WebSocket."""
    logging.debug(f"Mensaje recibido: {message}")

import websockets

async def manage_websocket_connection(cielo, wallets_list, handle_message, filter_params):
    """Gestiona la conexión WebSocket con Cielo API."""
    uri = cielo.get_websocket_uri()
    while True:
        try:
            async with websockets.connect(uri) as websocket:
                logging.info("✅ Conectado a Cielo WebSocket")
                while True:
                    message = await websocket.recv()
                    await log_raw_message(message)
                    await handle_message(message, wallets_list, filter_params)
        except Exception as e:
            logging.error(f"⚠️ Error en la conexión WebSocket: {e}")
            await asyncio.sleep(5)

# -------------------- FUNCIÓN PRINCIPAL --------------------
async def main():
    """
    Función principal que inicializa y ejecuta el bot.
    Versión optimizada con mejor gestión de errores y recursos.
    """
    global wallets_list, signal_predictor, ml_data_preparation, last_heartbeat, is_bot_active, dex_client
    
    try:
        last_heartbeat = time.time()
        
        signal.signal(signal.SIGINT, handle_shutdown)
        signal.signal(signal.SIGTERM, handle_shutdown)
        
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
                    send_telegram_message("⚠️ *Advertencia*: No se pudo conectar con RugCheck para validar tokens")
                else:
                    logger.info("✅ RugCheck API inicializada correctamente")
            except Exception as e:
                logger.error(f"⚠️ Error al configurar RugCheck: {e}")
                rugcheck_api = None
        
        logger.info("⚙️ Inicializando componentes...")
        dex_client = DexScreenerClient()
        scoring_system = ScoringSystem()
        
        logger.info("🧠 Inicializando componentes de Machine Learning...")
        ml_data_preparation = MLDataPreparation()
        signal_predictor = SignalPredictor()
        model_loaded = signal_predictor.load_model()
        
        if model_loaded:
            logger.info("✅ Modelo ML cargado correctamente")
        else:
            logger.info("ℹ️ No se encontró modelo ML previo, se entrenará automáticamente cuando haya suficientes datos")
        
        wallets_list = load_wallets()
        logger.info(f"📋 Se cargaron {len(wallets_list)} wallets para monitorear")
        
        if not wallets_list:
            logger.warning("⚠️ No se cargaron wallets, verificar traders_data.json")
            send_telegram_message("⚠️ *Advertencia*: No se cargaron wallets para monitorear. Verificar `traders_data.json`")
        
        signal_logic = SignalLogic(
            scoring_system=scoring_system, 
            dex_client=dex_client, 
            rugcheck_api=rugcheck_api,
            ml_predictor=signal_predictor
        )
        
        logger.info("🤖 Iniciando bot de comandos de Telegram...")
        is_bot_active = await process_telegram_commands(
            Config.TELEGRAM_BOT_TOKEN,
            Config.TELEGRAM_CHAT_ID,
            signal_logic
        )
        
        logger.info("🔄 Iniciando tareas periódicas...")
        asyncio.create_task(signal_logic.check_signals_periodically())
        asyncio.create_task(daily_summary_task(signal_logic, signal_predictor, dex_client))
        asyncio.create_task(refresh_wallets_task())
        asyncio.create_task(ml_periodic_tasks(ml_data_preparation, dex_client, signal_predictor))
        asyncio.create_task(monitoring_task())
        asyncio.create_task(maintenance_check_task())
        
        async def cleanup_old_data_task():
            while running:
                try:
                    await asyncio.sleep(7 * 24 * 3600)
                    logger.info("🧹 Iniciando limpieza de datos antiguos...")
                    deleted = db.cleanup_old_data(days=90)
                    logger.info(f"🧹 Limpieza completada: {sum(deleted.values())} registros eliminados")
                except Exception as e:
                    logger.error(f"Error en tarea de limpieza: {e}")
                    await asyncio.sleep(86400)
                    
        asyncio.create_task(cleanup_old_data_task())
        
        async def handle_message(msg):
            await log_raw_message(msg)
            await on_cielo_message(msg, signal_logic, ml_data_preparation, dex_client, scoring_system, signal_predictor)
        
        filter_params = {
            "chains": ["solana"],
            "tx_types": ["swap", "transfer"],
        }
        
        send_telegram_message(
            "🚀 *ChipaTrading Bot Iniciado*\n\n"
            f"• Wallets monitoreadas: `{len(wallets_list)}`\n"
            f"• Validación RugCheck: `{'Activa' if rugcheck_api else 'Inactiva'}`\n"
            f"• Modelo ML: `{'Cargado' if model_loaded else 'Pendiente'}`\n"
            f"• Comando bot: `/status` para ver estado actual\n\n"
            "Sistema listo para detectar señales Daily Runner. ¡Buenas operaciones! 📊"
        )
        
        logger.info("📡 Iniciando gestión de conexión con Cielo API...")
        cielo = CieloAPI(Config.CIELO_API_KEY)
        
        await manage_websocket_connection(cielo, wallets_list, handle_message, filter_params)
        
    except Exception as e:
        logger.critical(f"🚨 Error crítico en la función principal: {e}", exc_info=True)
        with stats_lock:
            performance_stats["errors"] += 1
            performance_stats["last_error"] = (time.time(), str(e))
        send_telegram_message(f"🚨 *Error Crítico*\nEl bot ha encontrado un error grave y necesita reiniciarse:\n`{str(e)}`")
        await asyncio.sleep(5)
        sys.exit(1)

if __name__ == "__main__":
    try:
        version = "2.0.0"
        print(f"🚀 Iniciando ChipaTrading Bot v{version}")
        try:
            import platform
            import psutil
            print(f"Sistema: {platform.system()} {platform.version()}")
            cpu_count = psutil.cpu_count()
            memory_info = psutil.virtual_memory()
            print(f"CPU: {cpu_count} núcleos, RAM: {memory_info.total/1024/1024/1024:.1f} GB")
        except:
            pass
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\n👋 Terminando ChipaTrading Bot por interrupción del usuario...")
    except Exception as e:
        logger.critical(f"🚨 Error crítico no capturado: {e}", exc_info=True)
        print(f"🚨 Error crítico: {e}")
        try:
            send_telegram_message(
                "🚨 *Error Crítico*\n"
                f"Error no capturado: {str(e)}\n"
                "El bot se ha detenido inesperadamente."
            )
        except:
            pass
        sys.exit(1)
