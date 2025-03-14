import logging
import requests
import time
from config import Config

logger = logging.getLogger("chipatrading")

def send_telegram_message(message):
    if len(message) > 4096:
        message = message[:4090] + "...\n[Mensaje truncado]"
        logger.warning("Mensaje truncado por longitud.")
    
    bot_token = Config.TELEGRAM_BOT_TOKEN
    chat_id = Config.TELEGRAM_CHAT_ID
    
    if not bot_token or not chat_id:
        logger.warning("⚠️ Credenciales de Telegram faltantes.")
        return False
    
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    data = {"chat_id": chat_id, "text": message, "parse_mode": "Markdown"}
    
    retries = 3
    delay = 2
    for i in range(retries):
        try:
            response = requests.post(url, data=data, timeout=10)
            if response.status_code == 200:
                logger.debug(f"Mensaje enviado: {message[:50]}...")
                return True
            else:
                logger.warning(f"Error {response.status_code}: {response.text}")
        except Exception as e:
            logger.error(f"Error en envío (intento {i+1}): {e}")
        time.sleep(delay)
        delay *= 2
    return False

def format_signal_message(signal_data, alert_type="signal"):
    token = signal_data.get("token", "N/A")
    confidence = signal_data.get("confidence", 0)
    tx_velocity = signal_data.get("tx_velocity", "N/A")
    buy_ratio = signal_data.get("buy_ratio", "N/A")
    
    solscan_link = f"https://solscan.io/token/{token}"
    birdeye_link = f"https://birdeye.so/token/{token}?chain=solana"
    neobullx_link = f"https://neo.bullx.io/terminal?chainId=1399811149&address={token}"
    solanabeach_link = f"https://solanabeach.io/token/{token}"
    
    header = "🔥 *SEÑAL DE TRADING*" if alert_type=="signal" else (
             "🚨 *Early Alpha Alert*" if alert_type=="early_alpha" else "🔥 *Daily Runner Alert*")
    
    message = (
        f"{header}\n\n"
        f"Token: `{token}`\n"
        f"Confianza: `{confidence:.2f}`\n"
        f"TX Velocity: `{tx_velocity}` tx/min\n"
        f"Buy Ratio: `{buy_ratio}`\n\n"
        f"🔗 *Exploradores:*\n"
        f"• [Solscan]({solscan_link})\n"
        f"• [Birdeye]({birdeye_link})\n"
        f"• [NeoBullX]({neobullx_link})\n"
        f"• [Solana Beach]({solanabeach_link})\n"
    )
    return message

def fix_telegram_commands():
    """
    Función helper para preparar los comandos de Telegram.
    Simplemente devuelve la función process_telegram_commands sin modificaciones.
    """
    return process_telegram_commands

def fix_on_cielo_message():
    """
    Función helper para preparar el manejador de mensajes de Cielo.
    Devuelve una función que recibe un mensaje y lo procesa usando los componentes necesarios.
    """
    async def on_cielo_message(message, wallet_tracker, scoring_system, signal_logic, scalper_monitor):
        try:
            import json, time
            data = json.loads(message)
            msg_type = data.get("type", "desconocido")
            
            if msg_type == "tx" and "data" in data:
                tx_data = data["data"]
                # Normalización de datos:
                normalized_tx = {}
                normalized_tx["wallet"] = tx_data.get("wallet")
                if tx_data.get("tx_type") == "swap":
                    if "token1_address" in tx_data and tx_data.get("token1_address") not in ["native", "So11111111111111111111111111111111111111112"]:
                        normalized_tx["token"] = tx_data.get("token1_address")
                        normalized_tx["type"] = "BUY"
                        normalized_tx["token_name"] = tx_data.get("token1_name", "Unknown")
                        normalized_tx["token_symbol"] = tx_data.get("token1_symbol", "???")
                        normalized_tx["amount_usd"] = float(tx_data.get("token1_amount_usd", 0))
                    elif "token0_address" in tx_data and tx_data.get("token0_address") not in ["native", "So11111111111111111111111111111111111111112"]:
                        normalized_tx["token"] = tx_data.get("token0_address")
                        normalized_tx["type"] = "SELL"
                        normalized_tx["token_name"] = tx_data.get("token0_name", "Unknown")
                        normalized_tx["token_symbol"] = tx_data.get("token0_symbol", "???")
                        normalized_tx["amount_usd"] = float(tx_data.get("token0_amount_usd", 0))
                elif tx_data.get("tx_type") == "transfer":
                    normalized_tx["token"] = tx_data.get("contract_address")
                    normalized_tx["type"] = "TRANSFER"
                    normalized_tx["token_name"] = tx_data.get("name", "Unknown")
                    normalized_tx["token_symbol"] = tx_data.get("symbol", "???")
                    normalized_tx["amount_usd"] = float(tx_data.get("amount_usd", 0))
                else:
                    logger.warning("Tipo de transacción no reconocido")
                    return

                normalized_tx["timestamp"] = tx_data.get("timestamp", int(time.time()))
                min_tx_usd = float(Config.get("MIN_TRANSACTION_USD", 200))
                if normalized_tx["amount_usd"] < min_tx_usd:
                    return

                logger.info(f"Transacción normalizada: {normalized_tx['token']} | {normalized_tx['type']} | ${normalized_tx['amount_usd']:.2f}")
                signal_logic.process_transaction(normalized_tx)
                scalper_monitor.process_transaction(normalized_tx)
            elif msg_type not in ["wallet_subscribed", "pong"]:
                logger.debug(f"Mensaje de tipo {msg_type} no procesado")
        except Exception as e:
            logger.error(f"Error en on_cielo_message: {e}", exc_info=True)
    return on_cielo_message

async def process_telegram_commands(bot_token, chat_id, signal_logic):
    import db
    try:
        from telegram import ParseMode
        from telegram.ext import Updater, CommandHandler
    except ImportError:
        logger.error("Instalar python-telegram-bot: pip install python-telegram-bot==13.15")
        return lambda: True

    bot_status = {"active": True, "verbosity": logging.INFO}

    def start_command(update, context):
        if str(update.effective_chat.id) != str(chat_id):
            update.message.reply_text("⛔️ No autorizado.")
            return
        bot_status["active"] = True
        update.message.reply_text("✅ Bot activado.")

    def stop_command(update, context):
        if str(update.effective_chat.id) != str(chat_id):
            update.message.reply_text("⛔️ No autorizado.")
            return
        bot_status["active"] = False
        update.message.reply_text("🛑 Bot desactivado.")

    def status_command(update, context):
        if str(update.effective_chat.id) != str(chat_id):
            update.message.reply_text("⛔️ No autorizado.")
            return
        active_tokens = signal_logic.get_active_candidates_count()
        emerging = db.get_emerging_tokens() if hasattr(db, "get_emerging_tokens") else []
        update.message.reply_text(
            f"*Estado del Bot:*\n"
            f"Activo: {'✅' if bot_status['active'] else '🛑'}\n"
            f"Tokens monitoreados: `{active_tokens}`\n"
            f"Tokens emergentes: `{len(emerging)}`\n",
            parse_mode=ParseMode.MARKDOWN
        )

    def emerging_command(update, context):
        if str(update.effective_chat.id) != str(chat_id):
            update.message.reply_text("⛔️ No autorizado.")
            return
        emerging = db.get_emerging_tokens() if hasattr(db, "get_emerging_tokens") else []
        if not emerging:
            update.message.reply_text("No se detectaron tokens emergentes.")
        else:
            msg = "*Tokens Emergentes:*\n"
            for token in emerging:
                msg += f"• `{token['token']}` - Confianza: `{token['confidence']:.2f}`\n"
            update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

    def top_command(update, context):
        if str(update.effective_chat.id) != str(chat_id):
            update.message.reply_text("⛔️ No autorizado.")
            return
        try:
            top_traders = db.get_top_traders()
            if not top_traders:
                update.message.reply_text("No hay top traders disponibles.")
                return
            msg = "*Top Traders:*\n\n"
            for trader in top_traders:
                msg += f"• {trader['wallet']} - Profit: {trader['avg_profit']:.2%} - Trades: {trader['trade_count']}\n"
            update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
        except Exception as e:
            update.message.reply_text(f"❌ Error: {e}")

    def debug_command(update, context):
        if str(update.effective_chat.id) != str(chat_id):
            update.message.reply_text("⛔️ No autorizado.")
            return
        debug_info = (
            f"*Estado del Sistema:*\n"
            f"Bot activo: {bot_status['active']}\n"
            f"Verbosity: {logging.getLevelName(bot_status['verbosity'])}\n"
        )
        update.message.reply_text(debug_info, parse_mode=ParseMode.MARKDOWN)

    def verbosity_command(update, context):
        if str(update.effective_chat.id) != str(chat_id):
            update.message.reply_text("⛔️ No autorizado.")
            return
        if not context.args:
            update.message.reply_text("Uso: /verbosity <nivel>")
            return
        level_str = context.args[0].upper()
        if level_str not in ["DEBUG", "INFO", "WARNING", "ERROR"]:
            update.message.reply_text("Nivel no válido.")
            return
        level = getattr(logging, level_str, logging.INFO)
        logger.setLevel(level)
        bot_status["verbosity"] = level
        update.message.reply_text(f"✅ Nivel ajustado a {level_str}")

    def chart_command(update, context):
        if str(update.effective_chat.id) != str(chat_id):
            update.message.reply_text("⛔️ No autorizado.")
            return
        if not context.args:
            update.message.reply_text("Uso: /chart <token_address>")
            return
        token = context.args[0]
        try:
            performances = db.get_signal_performance(token=token)
            if not performances:
                update.message.reply_text(f"No hay datos de rendimiento para el token {token}")
                return
            data_points = [(p["timeframe"], p["percent_change"]) for p in performances]
            order = {"3m": 1, "5m": 2, "10m": 3, "30m": 4, "1h": 5, "2h": 6, "4h": 7, "24h": 8}
            data_points.sort(key=lambda x: order.get(x[0], 9))
            
            result = "*Rendimiento de token*\n"
            result += f"`{token}`\n\n"
            for timeframe, percent in data_points:
                emoji = "🟢" if percent >= 0 else "🔴"
                bar_length = 20
                filled_length = int(round(bar_length * abs(percent) / 100))
                bar = "█" * filled_length + "-" * (bar_length - filled_length)
                result += f"{emoji} *{timeframe}*: {percent:.2f}% [{bar}]\n"
            update.message.reply_text(result, parse_mode=ParseMode.MARKDOWN)
        except Exception as e:
            update.message.reply_text(f"❌ Error generando gráfico: {e}")

    def config_command(update, context):
        if str(update.effective_chat.id) != str(chat_id):
            update.message.reply_text("⛔️ No autorizado.")
            return
        if not context.args or len(context.args) < 2:
            update.message.reply_text("Uso: /config <key> <value>")
            return
        key = context.args[0]
        value = context.args[1]
        try:
            db.update_setting(key, value)
            update.message.reply_text(f"✅ Configuración actualizada: {key} = {value}")
        except Exception as e:
            update.message.reply_text(f"❌ Error: {e}")

    try:
        updater = Updater(bot_token)
        dispatcher = updater.dispatcher

        dispatcher.add_handler(CommandHandler("start", start_command))
        dispatcher.add_handler(CommandHandler("stop", stop_command))
        dispatcher.add_handler(CommandHandler("status", status_command))
        dispatcher.add_handler(CommandHandler("top", top_command))
        dispatcher.add_handler(CommandHandler("emerging", emerging_command))
        dispatcher.add_handler(CommandHandler("debug", debug_command))
        dispatcher.add_handler(CommandHandler("verbosity", verbosity_command))
        dispatcher.add_handler(CommandHandler("chart", chart_command))
        dispatcher.add_handler(CommandHandler("config", config_command))

        updater.start_polling()
        logger.info("✅ Bot de Telegram iniciado - Comandos habilitados")
        return lambda: bot_status["active"]
    except Exception as e:
        logger.error(f"❌ Error iniciando bot: {e}")
        return lambda: True
