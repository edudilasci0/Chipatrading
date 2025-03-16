import logging
import requests
import time
from config import Config

logger = logging.getLogger("telegram_utils")

def send_telegram_message(message):
    if len(message) > 4096:
        message = message[:4090] + "...\n[Message truncated]"
        logger.warning("Message truncated due to length.")
    
    bot_token = Config.TELEGRAM_BOT_TOKEN
    chat_id = Config.TELEGRAM_CHAT_ID
    
    if not bot_token or not chat_id:
        logger.warning("Telegram credentials missing.")
        return False
    
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    data = {"chat_id": chat_id, "text": message, "parse_mode": "Markdown"}
    
    retries = 3
    delay = 2
    for i in range(retries):
        try:
            response = requests.post(url, data=data, timeout=10)
            if response.status_code == 200:
                logger.debug(f"Message sent: {message[:50]}...")
                return True
            else:
                logger.warning(f"Error {response.status_code}: {response.text}")
        except Exception as e:
            logger.error(f"Error sending message (attempt {i+1}): {e}")
        time.sleep(delay)
        delay *= 2
    return False

def fix_on_cielo_message():
    def on_cielo_message(message, wallet_tracker, scoring_system, signal_logic, scalper_monitor):
        import json, time
        try:
            data = json.loads(message)
            msg_type = data.get("type", "unknown")
            if msg_type == "tx" and "data" in data:
                tx_data = data["data"]
                normalized_tx = {}
                normalized_tx["wallet"] = tx_data.get("wallet")
                if not normalized_tx["wallet"]:
                    logger.debug("Transaction ignored: missing wallet")
                    return
                is_tracked_trader = normalized_tx["wallet"] in wallet_tracker.get_wallets()
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
                    logger.debug(f"Transaction type not processed: {tx_data.get('tx_type')}")
                    return
                normalized_tx["timestamp"] = tx_data.get("timestamp", int(time.time()))
                min_tx_usd = float(Config.get("MIN_TRANSACTION_USD", 200))
                if normalized_tx["amount_usd"] < min_tx_usd:
                    return
                if not normalized_tx.get("token") or normalized_tx["token"] in ["native", "So11111111111111111111111111111111111111112"]:
                    logger.debug("Transaction ignored: token is native or missing")
                    return
                is_pump_token = normalized_tx["token"].endswith("pump")
                if is_pump_token:
                    normalized_tx["is_pump_token"] = True
                trader_tag = "[FOLLOWED]" if is_tracked_trader else ""
                pump_tag = "[PUMP]" if is_pump_token else ""
                logger.info(f"Normalized transaction: {normalized_tx['wallet']} {trader_tag} | {normalized_tx['token']} {pump_tag} | {normalized_tx['type']} | ${normalized_tx['amount_usd']:.2f}")
                signal_logic.process_transaction(normalized_tx)
                scalper_monitor.process_transaction(normalized_tx)
            elif msg_type not in ["wallet_subscribed", "pong"]:
                logger.debug(f"Message type {msg_type} not processed")
        except Exception as e:
            logger.error(f"Error in on_cielo_message: {e}", exc_info=True)
    return on_cielo_message

def fix_telegram_commands():
    def process_telegram_commands(bot_token, chat_id, signal_logic):
        try:
            from telegram.ext import Updater, CommandHandler
            from telegram import ParseMode
        except ImportError:
            logger.error("Install python-telegram-bot: pip install python-telegram-bot==13.15")
            return lambda bot_token, chat_id, signal_logic: True

        bot_status = {"active": True, "verbosity": logging.INFO}

        def start_command(update, context):
            if str(update.effective_chat.id) != str(chat_id):
                update.message.reply_text("⛔️ Unauthorized.")
                return
            bot_status["active"] = True
            update.message.reply_text("✅ Bot activated.")

        def stop_command(update, context):
            if str(update.effective_chat.id) != str(chat_id):
                update.message.reply_text("⛔️ Unauthorized.")
                return
            bot_status["active"] = False
            update.message.reply_text("🛑 Bot deactivated.")

        def status_command(update, context):
            if str(update.effective_chat.id) != str(chat_id):
                update.message.reply_text("⛔️ Unauthorized.")
                return
            active_tokens = signal_logic.get_active_candidates_count() if hasattr(signal_logic, "get_active_candidates_count") else len(signal_logic.token_candidates)
            update.message.reply_text(
                f"*Bot Status:*\nActive: {'✅' if bot_status['active'] else '🛑'}\nTokens monitored: `{active_tokens}`",
                parse_mode=ParseMode.MARKDOWN
            )

        def emerging_command(update, context):
            if str(update.effective_chat.id) != str(chat_id):
                update.message.reply_text("⛔️ Unauthorized.")
                return
            try:
                emerging = db.get_emerging_tokens() if hasattr(db, "get_emerging_tokens") else []
                if not emerging:
                    update.message.reply_text("No emerging tokens detected.")
                else:
                    msg = "*Emerging Tokens:*\n"
                    for token in emerging:
                        msg += f"• `{token['token']}` - Confidence: `{token['confidence']:.2f}`\n"
                    update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
            except Exception as e:
                update.message.reply_text(f"❌ Error: {e}")

        def top_command(update, context):
            if str(update.effective_chat.id) != str(chat_id):
                update.message.reply_text("⛔️ Unauthorized.")
                return
            try:
                top_traders = db.get_top_traders()
                if not top_traders:
                    update.message.reply_text("No top traders available.")
                    return
                msg = "*Top Traders:*\n\n"
                for trader in top_traders:
                    msg += f"• {trader['wallet']} - Profit: {trader['avg_profit']:.2%} - Trades: {trader['trade_count']}\n"
                update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
            except Exception as e:
                update.message.reply_text(f"❌ Error: {e}")

        def debug_command(update, context):
            if str(update.effective_chat.id) != str(chat_id):
                update.message.reply_text("⛔️ Unauthorized.")
                return
            debug_info = (
                f"*System Status:*\n"
                f"Bot active: {bot_status['active']}\n"
                f"Verbosity: {logging.getLevelName(bot_status['verbosity'])}\n"
            )
            update.message.reply_text(debug_info, parse_mode=ParseMode.MARKDOWN)

        def verbosity_command(update, context):
            if str(update.effective_chat.id) != str(chat_id):
                update.message.reply_text("⛔️ Unauthorized.")
                return
            if not context.args:
                update.message.reply_text("Usage: /verbosity <level>")
                return
            level_str = context.args[0].upper()
            if level_str not in ["DEBUG", "INFO", "WARNING", "ERROR"]:
                update.message.reply_text("Invalid level.")
                return
            level = getattr(logging, level_str, logging.INFO)
            logger.setLevel(level)
            bot_status["verbosity"] = level
            update.message.reply_text(f"✅ Level set to {level_str}")

        def config_command(update, context):
            if str(update.effective_chat.id) != str(chat_id):
                update.message.reply_text("⛔️ Unauthorized.")
                return
            if not context.args or len(context.args) < 2:
                update.message.reply_text("Usage: /config <key> <value>")
                return
            key = context.args[0]
            value = context.args[1]
            try:
                db.update_setting(key, value)
                update.message.reply_text(f"✅ Setting updated: {key} = {value}")
            except Exception as e:
                update.message.reply_text(f"❌ Error: {e}")

        def chart_command(update, context):
            if str(update.effective_chat.id) != str(chat_id):
                update.message.reply_text("⛔️ Unauthorized.")
                return
            if not context.args:
                update.message.reply_text("Usage: /chart <token_address>")
                return
            token = context.args[0]
            try:
                performances = db.get_signal_performance(token=token)
                if not performances:
                    update.message.reply_text(f"No performance data for token {token}")
                    return
                data_points = [(p["timeframe"], p["percent_change"]) for p in performances]
                order = {"3m": 1, "5m": 2, "10m": 3, "30m": 4, "1h": 5, "2h": 6, "4h": 7, "24h": 8}
                data_points.sort(key=lambda x: order.get(x[0], 9))
                result = "*Token Performance*\n"
                result += f"`{token}`\n\n"
                for timeframe, percent in data_points:
                    emoji = "🟢" if percent >= 0 else "🔴"
                    bar_length = 20
                    filled_length = int(round(bar_length * abs(percent) / 100))
                    bar = "█" * filled_length + "-" * (bar_length - filled_length)
                    result += f"{emoji} *{timeframe}*: {percent:.2f}% [{bar}]\n"
                update.message.reply_text(result, parse_mode="Markdown")
            except Exception as e:
                update.message.reply_text(f"❌ Error generating chart: {e}")

        try:
            from telegram.ext import Updater, CommandHandler
            from telegram import ParseMode
        except ImportError:
            logger.error("Install python-telegram-bot: pip install python-telegram-bot==13.15")
            return lambda bot_token, chat_id, signal_logic: True

        updater = Updater(bot_token)
        dispatcher = updater.dispatcher

        dispatcher.add_handler(CommandHandler("start", start_command))
        dispatcher.add_handler(CommandHandler("stop", stop_command))
        dispatcher.add_handler(CommandHandler("status", status_command))
        dispatcher.add_handler(CommandHandler("config", config_command))
        dispatcher.add_handler(CommandHandler("top", top_command))
        dispatcher.add_handler(CommandHandler("emerging", emerging_command))
        dispatcher.add_handler(CommandHandler("debug", debug_command))
        dispatcher.add_handler(CommandHandler("verbosity", verbosity_command))
        dispatcher.add_handler(CommandHandler("chart", chart_command))

        updater.start_polling()
        logger.info("✅ Telegram Bot started - Commands enabled")
        return lambda bot_token, chat_id, signal_logic: bot_status["active"]
    return process_telegram_commands
