import logging
import requests
import time
from config import Config

logger = logging.getLogger("telegram_utils")

def send_telegram_message(message):
    """
    Envía un mensaje a Telegram, con manejo de mensajes muy largos.
    """
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

def fix_telegram_commands():
    """
    Inicializa y configura el bot de Telegram para comandos.
    """
    try:
        from telegram import ParseMode
        from telegram.ext import Updater, CommandHandler
    except ImportError:
        logger.error("Instalar python-telegram-bot: pip install python-telegram-bot==13.15")
        return lambda: True

    bot_status = {"active": True, "verbosity": logging.INFO}

    def start_command(update, context):
        if str(update.effective_chat.id) != str(Config.TELEGRAM_CHAT_ID):
            update.message.reply_text("⛔️ No autorizado.")
            return
        bot_status["active"] = True
        update.message.reply_text("✅ Bot activado.")

    def stop_command(update, context):
        if str(update.effective_chat.id) != str(Config.TELEGRAM_CHAT_ID):
            update.message.reply_text("⛔️ No autorizado.")
            return
        bot_status["active"] = False
        update.message.reply_text("🛑 Bot desactivado.")

    def status_command(update, context):
        if str(update.effective_chat.id) != str(Config.TELEGRAM_CHAT_ID):
            update.message.reply_text("⛔️ No autorizado.")
            return
        active_tokens = "N/A"  # Aquí puedes agregar lógica para obtener datos
        update.message.reply_text(
            f"*Estado del Bot:*\nActivo: {'✅' if bot_status['active'] else '🛑'}\nTokens monitoreados: {active_tokens}\n",
            parse_mode=ParseMode.MARKDOWN
        )

    def chart_command(update, context):
        from telegram import ParseMode
        import db
        if str(update.effective_chat.id) != str(Config.TELEGRAM_CHAT_ID):
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
            order = {"3m": 1, "5m": 2, "10m": 3, "30m": 4, "1h": 5, "2h": 6, "4h": 7, "24h": 8}
            data_points = [(p["timeframe"], p["percent_change"]) for p in performances]
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
        import db
        if str(update.effective_chat.id) != str(Config.TELEGRAM_CHAT_ID):
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
        updater = Updater(Config.TELEGRAM_BOT_TOKEN)
        dispatcher = updater.dispatcher

        dispatcher.add_handler(CommandHandler("start", start_command))
        dispatcher.add_handler(CommandHandler("stop", stop_command))
        dispatcher.add_handler(CommandHandler("status", status_command))
        dispatcher.add_handler(CommandHandler("chart", chart_command))
        dispatcher.add_handler(CommandHandler("config", config_command))

        updater.start_polling()
        logger.info("✅ Bot de Telegram iniciado - Comandos habilitados")
        return lambda: bot_status["active"]
    except Exception as e:
        logger.error(f"❌ Error iniciando bot: {e}")
        return lambda: True

def fix_on_cielo_message():
    """
    Devuelve una función que procesa los mensajes recibidos de Cielo.
    """
    async def on_cielo_message(message, wallet_tracker, scoring_system, signal_logic, scalper_monitor):
        try:
            import json
            import time
            data = json.loads(message)
            msg_type = data.get("type", "desconocido")
            
            if msg_type == "tx" and "data" in data:
                tx_data = data["data"]
                normalized_tx = {}
                normalized_tx["wallet"] = tx_data.get("wallet")
                if not normalized_tx["wallet"]:
                    logger.debug("Transacción ignorada: Falta wallet")
                    return
                
                is_tracked = normalized_tx["wallet"] in wallet_tracker.get_wallets()
                
                if tx_data.get("tx_type") == "swap":
                    token0_is_native = tx_data.get("token0_address") in ["native", "So11111111111111111111111111111111111111112"]
                    token1_is_native = tx_data.get("token1_address") in ["native", "So11111111111111111111111111111111111111112"]
                    if token1_is_native and not token0_is_native:
                        normalized_tx["token"] = tx_data.get("token0_address")
                        normalized_tx["type"] = "SELL"
                        normalized_tx["token_name"] = tx_data.get("token0_name", "Unknown")
                        normalized_tx["token_symbol"] = tx_data.get("token0_symbol", "???")
                        normalized_tx["amount_usd"] = float(tx_data.get("token0_amount_usd", 0))
                    elif token0_is_native and not token1_is_native:
                        normalized_tx["token"] = tx_data.get("token1_address")
                        normalized_tx["type"] = "BUY"
                        normalized_tx["token_name"] = tx_data.get("token1_name", "Unknown")
                        normalized_tx["token_symbol"] = tx_data.get("token1_symbol", "???")
                        normalized_tx["amount_usd"] = float(tx_data.get("token1_amount_usd", 0))
                    else:
                        normalized_tx["token"] = tx_data.get("token1_address")
                        normalized_tx["type"] = "BUY"
                        normalized_tx["token_name"] = tx_data.get("token1_name", "Unknown")
                        normalized_tx["token_symbol"] = tx_data.get("token1_symbol", "???")
                        normalized_tx["amount_usd"] = float(tx_data.get("token1_amount_usd", 0))
                elif tx_data.get("tx_type") == "transfer":
                    normalized_tx["token"] = tx_data.get("contract_address")
                    normalized_tx["type"] = "TRANSFER"
                    normalized_tx["token_name"] = tx_data.get("name", "Unknown")
                    normalized_tx["token_symbol"] = tx_data.get("symbol", "???")
                    normalized_tx["amount_usd"] = float(tx_data.get("amount_usd", 0))
                else:
                    logger.debug(f"Tipo de transacción no procesada: {tx_data.get('tx_type')}")
                    return

                normalized_tx["timestamp"] = tx_data.get("timestamp", int(time.time()))
                min_tx_usd = float(Config.get("MIN_TRANSACTION_USD", 200))
                if normalized_tx["amount_usd"] < min_tx_usd:
                    return
                if not normalized_tx.get("token") or normalized_tx["token"] in ["native", "So11111111111111111111111111111111111111112"]:
                    logger.debug("Transacción ignorada: Token es nativo o falta")
                    return

                logger.info(f"Transacción normalizada: {normalized_tx['wallet']} | {normalized_tx['token']} | {normalized_tx['type']} | ${normalized_tx['amount_usd']:.2f}")
                signal_logic.process_transaction(normalized_tx)
                scalper_monitor.process_transaction(normalized_tx)
            elif msg_type not in ["wallet_subscribed", "pong"]:
                logger.debug(f"Mensaje de tipo {msg_type} no procesado")
        except Exception as e:
            logger.error(f"Error en on_cielo_message: {e}", exc_info=True)
    return on_cielo_message
