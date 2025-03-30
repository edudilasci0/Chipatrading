import logging
import requests
import time
import asyncio
import json
from typing import Dict, List, Callable, Any, Optional
from config import Config
import db

logger = logging.getLogger("telegram_utils")

def send_telegram_message(message: str) -> bool:
    """
    Envía un mensaje a Telegram usando la API.
    """
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

def send_enhanced_signal(token: str, confidence: float, tx_velocity: float, traders: List[str],
                           token_type: str = "", token_name: Optional[str] = None, 
                           market_cap: Optional[float] = None, initial_price: Optional[float] = None,
                           extended_analysis: Optional[Dict[str, Any]] = None, signal_level: Optional[str] = None) -> bool:
    """
    Envía una señal con formato mejorado y más información útil, destacando market cap y volumen.
    """
    token_name_display = f"{token_name} " if token_name else ""
    
    market_cap_display = ""
    if market_cap:
        if market_cap >= 1_000_000:
            market_cap_display = f"💰 Market Cap: `${market_cap/1_000_000:.2f}M`\n"
        else:
            market_cap_display = f"💰 Market Cap: `${market_cap/1000:.2f}K`\n"
    
    volume_display = ""
    if extended_analysis and "market" in extended_analysis:
        volume = extended_analysis["market"].get("volume", 0)
        if volume > 0:
            if volume >= 1_000_000:
                volume_display = f"📊 Volumen: `${volume/1_000_000:.2f}M`\n"
            else:
                volume_display = f"📊 Volumen: `${volume/1000:.2f}K`\n"
    
    trader_names = []
    for wallet in traders[:5]:
        name = db.get_trader_name_from_wallet(wallet)
        if name and name != wallet:
            trader_names.append(f"{name} ({wallet[:4]}...{wallet[-4:]})")
        else:
            trader_names.append(f"{wallet[:6]}...{wallet[-4:]}")
    traders_info = ", ".join(trader_names)
    if len(traders) > 5:
        traders_info += f" y {len(traders) - 5} más"
    
    confidence_rating = "⭐⭐⭐" if confidence > 0.8 else "⭐⭐" if confidence > 0.5 else "⭐"
    signal_level_display = f"Nivel {signal_level} " if signal_level else ""
    
    price_display = ""
    if initial_price and initial_price > 0:
        if initial_price < 0.000001:
            price_display = f"💲 Precio inicial: `${initial_price:.10f}`\n"
        elif initial_price < 0.001:
            price_display = f"💲 Precio inicial: `${initial_price:.8f}`\n"
        elif initial_price < 1:
            price_display = f"💲 Precio inicial: `${initial_price:.6f}`\n"
        else:
            price_display = f"💲 Precio inicial: `${initial_price:.4f}`\n"
    
    additional_info = ""
    if extended_analysis:
        whale_data = extended_analysis.get("whale", {})
        market_data = extended_analysis.get("market", {})
        if market_data.get("trending_platforms", []):
            trending_platforms = ", ".join(market_data.get("trending_platforms", []))
            additional_info += f"🔥 *TRENDING* en {trending_platforms}\n"
        if whale_data.get("has_whale_activity", False):
            whale_count = whale_data.get("known_whales_count", 0)
            additional_info += f"🐋 *Actividad de ballenas detectada* ({whale_count} whales)\n"
        holder_growth = market_data.get("holder_growth_rate_1h", 0)
        if holder_growth > 5:
            additional_info += f"👥 *Holders creciendo* +{holder_growth:.1f}% en 1h\n"
        if market_data.get("healthy_liquidity", False):
            additional_info += f"💧 *Liquidez saludable*\n"
        token_data = extended_analysis.get("token", {})
        if token_data.get("price_action_quality", 0) > 0.7:
            additional_info += f"📈 *Patrón técnico fuerte*\n"
    
    msg = (
        f"🚨 *SEÑAL DETECTADA* {signal_level_display}\n\n"
        f"Token: {token_name_display}`{token}`\n"
        f"{market_cap_display}"
        f"{volume_display}"
        f"Confianza: `{confidence:.2f}` {confidence_rating}\n"
        f"Velocidad TX: `{tx_velocity:.2f}` tx/min\n"
        f"{price_display}"
        f"Traders: {traders_info}\n"
        f"{token_type}\n"
        f"{additional_info}\n"
        f"🔗 *Explorer:*\n"
        f"• [NeoBullX](https://solana.neobullx.app/asset/{token})\n"
    )
    
    return send_telegram_message(msg)

def send_performance_report(token: str, signal_id: str, timeframe: str, percent_change: float, 
                            volatility: Optional[float] = None, trend: Optional[str] = None, 
                            volume_display: Optional[str] = None, traders_count: Optional[int] = None, 
                            whale_activity: Optional[bool] = None, liquidity_change: Optional[float] = None) -> bool:
    if percent_change > 50:
        emoji = "🚀"
    elif percent_change > 20:
        emoji = "🔥"
    elif percent_change > 0:
        emoji = "✅"
    elif percent_change > -20:
        emoji = "⚠️"
    else:
        emoji = "❌"
    
    volatility_display = f"Volatilidad: *{volatility:.2f}%*\n" if volatility is not None else ""
    trend_display = f"Tendencia: *{trend}*\n" if trend else ""
    volume_info = f"Volumen: `{volume_display}`\n" if volume_display else ""
    traders_info = f"Traders activos: `{traders_count}`\n" if traders_count else ""
    
    additional_info = ""
    if whale_activity:
        additional_info += f"🐋 *Actividad de ballenas detectada*\n"
    if liquidity_change and liquidity_change > 10:
        additional_info += f"💧 *Liquidez aumentó* +{liquidity_change:.1f}%\n"
    elif liquidity_change and liquidity_change < -10:
        additional_info += f"⚠️ *Liquidez disminuyó* {liquidity_change:.1f}%\n"
    
    neobullx_link = f"https://solana.neobullx.app/asset/{token}"
    
    message = (
        f"*🔍 Seguimiento {timeframe} #{signal_id}*\n\n"
        f"Token: `{token}`\n"
        f"Cambio: *{percent_change:.2f}%* {emoji}\n"
        f"{volatility_display}"
        f"{trend_display}"
        f"{volume_info}"
        f"Traders activos: `{traders_count}`\n"
        f"{additional_info}\n"
        f"🔗 *Explorer:*\n"
        f"• [NeoBullX]({neobullx_link})\n"
    )
    
    return send_telegram_message(message)

def fix_on_cielo_message() -> Callable[[str, Any, Any, Any, Optional[Any]], Any]:
    """
    Devuelve una función asíncrona que procesa mensajes de Cielo.
    La función se asegura de convertir el mensaje a JSON y luego
    llama a la lógica de señales, scoring y (opcionalmente) al monitoreo de scalpers.
    """
    async def on_cielo_message(message, wallet_tracker, scoring_system, signal_logic, scalper_monitor=None):
        try:
            data = json.loads(message)
            if "type" not in data or data["type"] != "transaction":
                return
            if "data" not in data:
                return
            tx_data = data["data"]
            if "token" not in tx_data or "amountUsd" not in tx_data:
                logger.debug("Transacción sin datos de token o monto ignorada")
                return
            normalized_tx = {
                "wallet": tx_data.get("wallet", ""),
                "token": tx_data.get("token", ""),
                "type": tx_data.get("txType", "").upper(),
                "amount_usd": float(tx_data.get("amountUsd", 0)),
                "timestamp": time.time()
            }
            min_usd = float(Config.get("MIN_TRANSACTION_USD", 200))
            if normalized_tx["amount_usd"] < min_usd:
                return
            if signal_logic:
                signal_logic.process_transaction(normalized_tx)
            if scoring_system:
                scoring_system.update_score_on_trade(normalized_tx["wallet"], normalized_tx)
            if scalper_monitor:
                scalper_monitor.process_transaction(normalized_tx)
        except json.JSONDecodeError:
            logger.warning("Error decodificando mensaje JSON de Cielo")
        except Exception as e:
            logger.error(f"Error procesando mensaje de Cielo: {e}", exc_info=True)
    return on_cielo_message

def fix_telegram_commands() -> Callable:
    async def process_telegram_commands(bot_token: str, chat_id: str, signal_logic, wallet_manager=None):
        try:
            from telegram import Update, ParseMode
            from telegram.ext import Updater, CommandHandler, CallbackContext
        except ImportError:
            logger.error("Install python-telegram-bot: pip install python-telegram-bot==13.15")
            return True

        bot_status = {"active": True, "verbosity": logging.INFO}

        def start_command(update: Update, context: CallbackContext):
            if str(update.effective_chat.id) != str(chat_id):
                update.message.reply_text("⛔️ Not authorized.")
                return
            bot_status["active"] = True
            update.message.reply_text("✅ Bot activated.")

        def stop_command(update: Update, context: CallbackContext):
            if str(update.effective_chat.id) != str(chat_id):
                update.message.reply_text("⛔️ Not authorized.")
                return
            bot_status["active"] = False
            update.message.reply_text("🛑 Bot deactivated.")

        def status_command(update: Update, context: CallbackContext):
            if str(update.effective_chat.id) != str(chat_id):
                update.message.reply_text("⛔️ Not authorized.")
                return
            active_tokens = signal_logic.get_active_candidates_count() if hasattr(signal_logic, "get_active_candidates_count") else 0
            signals_today = db.count_signals_today()
            txs_today = db.count_transactions_today()
            update.message.reply_text(
                f"*Bot Status:*\n"
                f"Active: {'✅' if bot_status['active'] else '🛑'}\n"
                f"Tokens monitoreados: `{active_tokens}`\n"
                f"Signals today: `{signals_today}`\n"
                f"Transactions processed: `{txs_today}`", 
                parse_mode=ParseMode.MARKDOWN
            )

        # Comandos para la gestión de wallets, si wallet_manager está disponible
        def add_wallet_command(update: Update, context: CallbackContext):
            if str(update.effective_chat.id) != str(chat_id):
                update.message.reply_text("⛔️ Not authorized.")
                return
            if not wallet_manager:
                update.message.reply_text("Wallet manager no disponible.")
                return
            args = context.args
            if len(args) < 1:
                update.message.reply_text("Uso: /addwallet <address> [name] [category] [score]")
                return
            address = args[0]
            name = args[1] if len(args) > 1 else address[:8]
            category = args[2] if len(args) > 2 else "Default"
            try:
                score = float(args[3]) if len(args) > 3 else float(Config.get("DEFAULT_SCORE", 5.0))
            except ValueError:
                update.message.reply_text("Score debe ser un número.")
                return
            if wallet_manager.add_wallet(address, name, category, score):
                update.message.reply_text(f"✅ Wallet añadida: {name} ({address[:6]}...{address[-4:]})")
            else:
                update.message.reply_text("❌ Error al añadir wallet.")

        # Agregar más comandos según sea necesario…

        updater = Updater(bot_token, use_context=True)
        dispatcher = updater.dispatcher
        dispatcher.add_handler(CommandHandler("start", start_command))
        dispatcher.add_handler(CommandHandler("stop", stop_command))
        dispatcher.add_handler(CommandHandler("status", status_command))
        if wallet_manager:
            dispatcher.add_handler(CommandHandler("addwallet", add_wallet_command))
        updater.start_polling()
        logger.info(f"🤖 Bot de Telegram iniciado. ID del chat: {chat_id}")

        while True:
            await asyncio.sleep(60)
    return process_telegram_commands
