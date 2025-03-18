# telegram_utils.py
import logging
import requests
import time
from config import Config
import db

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
                logger.debug("Message sent")
                return True
            else:
                logger.warning(f"Error {response.status_code}: {response.text}")
        except Exception as e:
            logger.error(f"Error sending telegram message (attempt {i+1}): {e}")
        time.sleep(delay)
        delay *= 2
    return False

def send_enhanced_signal(token, confidence, tx_velocity, traders, token_type="", token_name=None, market_cap=None, initial_price=None):
    """
    Envía una señal con formato mejorado y más información útil
    """
    # Obtener nombre del token si está disponible
    token_name_display = f"{token_name} " if token_name else ""
    
    # Formatear números grandes para mejor lectura
    market_cap_display = ""
    if market_cap:
        if market_cap >= 1000000:
            market_cap_display = f"💰 Market Cap: `${market_cap/1000000:.2f}M`\n"
        else:
            market_cap_display = f"💰 Market Cap: `${market_cap/1000:.2f}K`\n"
    
    # Formatear traders con nombres si están disponibles
    trader_names = []
    for wallet in traders[:5]:
        name = db.get_trader_name_from_wallet(wallet)
        if name and name != wallet:  # Si hay un nombre asociado
            trader_names.append(f"{name} ({wallet[:4]}...{wallet[-4:]})")
        else:
            trader_names.append(f"{wallet[:6]}...{wallet[-4:]}")
    
    traders_info = ", ".join(trader_names)
    if len(traders) > 5:
        traders_info += f" y {len(traders) - 5} más"
    
    # Clasificación del token basada en la confianza
    confidence_rating = "⭐⭐⭐" if confidence > 0.8 else "⭐⭐" if confidence > 0.5 else "⭐"
    
    price_display = ""
    if initial_price and initial_price > 0:
        # Determinar formato para mejor visualización dependiendo del rango de precios
        if initial_price < 0.000001:
            price_display = f"💲 Precio inicial: `${initial_price:.10f}`\n"
        elif initial_price < 0.001:
            price_display = f"💲 Precio inicial: `${initial_price:.8f}`\n"
        elif initial_price < 1:
            price_display = f"💲 Precio inicial: `${initial_price:.6f}`\n"
        else:
            price_display = f"💲 Precio inicial: `${initial_price:.4f}`\n"
    
    msg = (
        f"🚨 *SEÑAL DETECTADA*\n\n"
        f"Token: {token_name_display}`{token}`\n"
        f"Confianza: `{confidence:.2f}` {confidence_rating}\n"
        f"Velocidad TX: `{tx_velocity:.2f}` tx/min\n"
        f"{market_cap_display}"
        f"{price_display}"
        f"Traders: {traders_info}\n"
        f"{token_type}\n\n"
        f"🔗 *Exploradores:*\n"
        f"• [Solscan](https://solscan.io/token/{token})\n"
        f"• [Birdeye](https://birdeye.so/token/{token}?chain=solana)\n"
        f"• [DexScreener](https://dexscreener.com/solana/{token})\n"
    )
    
    return send_telegram_message(msg)

async def process_telegram_commands(bot_token, chat_id, signal_logic):
    try:
        from telegram import ParseMode
        from telegram.ext import Updater, CommandHandler
    except ImportError:
        logger.error("Install python-telegram-bot: pip install python-telegram-bot==13.15")
        return True

    bot_status = {"active": True, "verbosity": logging.INFO}

    def start_command(update, context):
        if str(update.effective_chat.id) != str(chat_id):
            update.message.reply_text("⛔️ Not authorized.")
            return
        bot_status["active"] = True
        update.message.reply_text("✅ Bot activated.")

    def stop_command(update, context):
        if str(update.effective_chat.id) != str(chat_id):
            update.message.reply_text("⛔️ Not authorized.")
            return
        bot_status["active"] = False
        update.message.reply_text("🛑 Bot deactivated.")

    def status_command(update, context):
        if str(update.effective_chat.id) != str(chat_id):
            update.message.reply_text("⛔️ Not authorized.")
            return
        active_tokens = signal_logic.get_active_candidates_count() if hasattr(signal_logic, "get_active_candidates_count") else 0
        update.message.reply_text(f"*Bot Status:*\nActive: {'✅' if bot_status['active'] else '🛑'}\nTokens monitored: `{active_tokens}`", parse_mode=ParseMode.MARKDOWN)

    updater = Updater(bot_token)
    dispatcher = updater.dispatcher
    dispatcher.add_handler(CommandHandler("start", start_command))
    dispatcher.add_handler(CommandHandler("stop", stop_command))
    dispatcher.add_handler(CommandHandler("status", status_command))
    updater.start_polling()
    logger.info("✅ Telegram Bot started - Commands enabled")
    return bot_status["active"]

def fix_telegram_commands():
    return process_telegram_commands

def fix_on_cielo_message():
    async def on_cielo_message(message, wallet_tracker, scoring_system, signal_logic, scalper_monitor):
        try:
            import json
            import time
            data = json.loads(message)
            msg_type = data.get("type", "unknown")
            if msg_type == "tx" and "data" in data:
                tx_data = data["data"]
                normalized_tx = {}
                normalized_tx["wallet"] = tx_data.get("wallet")
                if not normalized_tx["wallet"]:
                    logger.debug("Transaction ignored: Missing wallet")
                    return
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
                    logger.debug("Transaction ignored: Token is native or missing")
                    return
                logger.info(f"Normalized transaction: {normalized_tx['wallet']} | {normalized_tx['token']} | {normalized_tx['type']} | ${normalized_tx['amount_usd']:.2f}")
                signal_logic.process_transaction(normalized_tx)
                scalper_monitor.process_transaction(normalized_tx)
            elif msg_type not in ["wallet_subscribed", "pong"]:
                logger.debug(f"Message type {msg_type} not processed")
        except Exception as e:
            logger.error(f"Error in on_cielo_message: {e}", exc_info=True)
    return on_cielo_message
