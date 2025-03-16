import logging
import requests
import time
from config import Config

logger = logging.getLogger("telegram_utils")

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

def fix_telegram_commands():
    try:
        from telegram import ParseMode
        from telegram.ext import Updater, CommandHandler
    except ImportError:
        logger.error("Instalar python-telegram-bot: pip install python-telegram-bot==13.15")
        return lambda bot_token, chat_id, signal_logic: None

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
        active_tokens = signal_logic.get_active_candidates_count() if hasattr(signal_logic, 'get_active_candidates_count') else len(signal_logic.token_candidates)
        update.message.reply_text(f"*Estado del Bot:*\nActivo: {'✅' if bot_status['active'] else '🛑'}\nTokens monitoreados: `{active_tokens}`", parse_mode=ParseMode.MARKDOWN)

    # Agrega otros comandos según necesites...
    # Por ejemplo, comando /config, /top, etc.

    def _dummy_command(update, context):
        update.message.reply_text("Comando dummy ejecutado.")

    try:
        updater = Updater(Config.TELEGRAM_BOT_TOKEN)
        dispatcher = updater.dispatcher

        dispatcher.add_handler(CommandHandler("start", start_command))
        dispatcher.add_handler(CommandHandler("stop", stop_command))
        dispatcher.add_handler(CommandHandler("status", status_command))
        dispatcher.add_handler(CommandHandler("dummy", _dummy_command))

        updater.start_polling()
        logger.info("✅ Bot de Telegram iniciado - Comandos habilitados")
    except Exception as e:
        logger.error(f"❌ Error iniciando bot: {e}")

    # La función de comandos no necesita retornar nada, pero para compatibilidad devolvemos un dummy
    return lambda bot_token, chat_id, signal_logic: bot_status["active"]

def fix_on_cielo_message():
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
                    token0_native = tx_data.get("token0_address") in ["native", "So11111111111111111111111111111111111111112"]
                    token1_native = tx_data.get("token1_address") in ["native", "So11111111111111111111111111111111111111112"]
                    if token1_native and not token0_native:
                        normalized_tx["token"] = tx_data.get("token0_address")
                        normalized_tx["type"] = "SELL"
                        normalized_tx["token_name"] = tx_data.get("token0_name", "Unknown")
                        normalized_tx["token_symbol"] = tx_data.get("token0_symbol", "???")
                        normalized_tx["amount_usd"] = float(tx_data.get("token0_amount_usd", 0))
                    elif token0_native and not token1_native:
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
                if not normalized_tx.get("token") or normalized_tx.get("token") in ["native", "So11111111111111111111111111111111111111112"]:
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
