import logging
import requests
from config import Config

# Set up logger
logger = logging.getLogger("chipatrading")

def send_telegram_message(message):
    """
    Envía un mensaje a través del bot de Telegram.
    
    Args:
        message: Mensaje a enviar (soporta formato Markdown)
    """
    try:
        bot_token = Config.TELEGRAM_BOT_TOKEN
        chat_id = Config.TELEGRAM_CHAT_ID
        
        if not bot_token or not chat_id:
            logger.warning("⚠️ No se puede enviar mensaje a Telegram: faltan credenciales")
            return False
        
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        data = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "Markdown"
        }
        
        response = requests.post(url, data=data, timeout=10)
        
        if response.status_code == 200:
            logger.debug(f"✅ Mensaje enviado a Telegram: {message[:50]}...")
            return True
        else:
            logger.warning(f"⚠️ Error enviando mensaje a Telegram: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        logger.error(f"🚨 Error en send_telegram_message: {e}")
        return False

async def process_telegram_commands(bot_token, chat_id, signal_logic):
    """
    Procesa comandos recibidos por Telegram.
    """
    # Import db inside the function to avoid circular imports
    import db
    
    try:
        # Para usar python-telegram-bot
        from telegram import ParseMode
        from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext
    except ImportError:
        logger.error("❌ No se pudo importar python-telegram-bot. Instalarlo con pip install python-telegram-bot==13.15")
        # Devolver una función dummy que siempre retorna True
        return lambda: True
    
    # Estado global del bot
    bot_status = {"active": True}
    
    # Comandos de control
    def start_command(update, context):
        if str(update.effective_chat.id) != str(chat_id):
            update.message.reply_text("⛔️ No estás autorizado para este comando.")
            return
            
        bot_status["active"] = True
        update.message.reply_text("✅ Bot activado. Procesando transacciones y emitiendo señales.")
    
    def stop_command(update, context):
        if str(update.effective_chat.id) != str(chat_id):
            update.message.reply_text("⛔️ No estás autorizado para este comando.")
            return
            
        bot_status["active"] = False
        update.message.reply_text("🛑 Bot desactivado. No se procesarán nuevas transacciones ni señales.")
    
    def status_command(update, context):
        if str(update.effective_chat.id) != str(chat_id):
            update.message.reply_text("⛔️ No estás autorizado para este comando.")
            return
            
        status = "✅ Activo" if bot_status["active"] else "🛑 Inactivo"
        active_tokens = signal_logic.get_active_candidates_count()
        signals_today = db.count_signals_today()
        signals_hour = db.count_signals_last_hour() 
        
        try:
            stats_text = get_performance_stats_text()
        except:
            stats_text = "No hay datos de rendimiento disponibles."
        
        update.message.reply_text(
            f"*Estado del Bot:* {status}\n\n"
            f"*Monitoreo:*\n"
            f"• Tokens actualmente monitoreados: `{active_tokens}`\n"
            f"• Señales emitidas hoy: `{signals_today}`\n"
            f"• Señales en la última hora: `{signals_hour}`\n\n"
            f"*Rendimiento:*\n{stats_text}",
            parse_mode=ParseMode.MARKDOWN
        )
    
    def config_command(update, context):
        # Obtener y mostrar configuración actual
        if str(update.effective_chat.id) != str(chat_id):
            update.message.reply_text("⛔️ No estás autorizado para este comando.")
            return
            
        settings = db.get_all_settings()
        config_text = "*Configuración Actual:*\n\n"
        
        for key, value in settings.items():
            config_text += f"• `{key}`: `{value}`\n"
        
        update.message.reply_text(config_text, parse_mode=ParseMode.MARKDOWN)
    
    def set_command(update, context):
        # Actualizar un valor de configuración
        if str(update.effective_chat.id) != str(chat_id):
            update.message.reply_text("⛔️ No estás autorizado para este comando.")
            return
            
        if len(context.args) != 2:
            update.message.reply_text(
                "⚠️ Uso incorrecto. Formato: /set clave valor\n"
                "Ejemplo: `/set min_traders_for_signal 3`",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        key = context.args[0]
        value = context.args[1]
        
        try:
            db.update_setting(key, value)
            update.message.reply_text(f"✅ Configuración actualizada: `{key}` = `{value}`", parse_mode=ParseMode.MARKDOWN)
        except Exception as e:
            update.message.reply_text(f"❌ Error al actualizar configuración: {e}")
    
    def stats_command(update, context):
        if str(update.effective_chat.id) != str(chat_id):
            update.message.reply_text("⛔️ No estás autorizado para este comando.")
            return
            
        stats_text = get_performance_stats_text()
        update.message.reply_text(
            f"*Estadísticas de Rendimiento:*\n\n{stats_text}",
            parse_mode=ParseMode.MARKDOWN
        )
    
    def get_performance_stats_text():
        try:
            stats = db.get_signals_performance_stats()
            if not stats:
                return "No hay datos de rendimiento disponibles."
            
            stats_text = ""
            for stat in stats:
                timeframe = stat["timeframe"]
                success_rate = stat["success_rate"]
                avg_percent = stat["avg_percent_change"]
                total = stat["total_signals"]
                
                emoji = "🟢" if success_rate >= 60 else "🟡" if success_rate >= 50 else "🔴"
                stats_text += f"{emoji} *{timeframe}*: {success_rate}% éxito, {avg_percent}% promedio ({total} señales)\n"
            
            return stats_text
        except Exception as e:
            logger.error(f"Error obteniendo estadísticas: {e}")
            return "Error al obtener estadísticas."
    
    # Iniciar el bot con implementación compatible con Render
    try:
        updater = Updater(bot_token)
        dispatcher = updater.dispatcher
        
        # Registrar comandos
        dispatcher.add_handler(CommandHandler("start", start_command))
        dispatcher.add_handler(CommandHandler("stop", stop_command))
        dispatcher.add_handler(CommandHandler("status", status_command))
        dispatcher.add_handler(CommandHandler("config", config_command))
        dispatcher.add_handler(CommandHandler("set", set_command))
        dispatcher.add_handler(CommandHandler("stats", stats_command))
        
        # Iniciar el bot en modo no-bloqueante
        updater.start_polling()
        logger.info("✅ Bot de Telegram iniciado - Comandos habilitados")
        
        # Devolver función para verificar estado
        def is_bot_active():
            return bot_status["active"]
        
        return is_bot_active
    except Exception as e:
        logger.error(f"❌ Error iniciando bot de Telegram: {e}")
        # En caso de error, devolver una función que siempre retorna True
        return lambda: True
