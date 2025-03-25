# telegram_utils.py
import logging
import requests
import time
import asyncio  # Añadido el import de asyncio
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

def send_enhanced_signal(token, confidence, tx_velocity, traders, token_type="", token_name=None, 
                         market_cap=None, initial_price=None, extended_analysis=None, signal_level=None):
    """
    Envía una señal con formato mejorado y más información útil,
    destacando market cap y volumen
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
    
    # Formatear volumen si está disponible
    volume_display = ""
    if extended_analysis and "market" in extended_analysis:
        volume = extended_analysis["market"].get("volume", 0)
        if volume > 0:
            if volume >= 1000000:
                volume_display = f"📊 Volumen: `${volume/1000000:.2f}M`\n"
            else:
                volume_display = f"📊 Volumen: `${volume/1000:.2f}K`\n"
    
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
    
    # Señal con nivel S/A/B/C si está disponible
    signal_level_display = f"Nivel {signal_level} " if signal_level else ""
    
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
    
    # Información adicional del análisis extendido
    additional_info = ""
    if extended_analysis:
        whale_data = extended_analysis.get("whale", {})
        market_data = extended_analysis.get("market", {})
        
        # Mostrar si está en trending
        if market_data.get("trending_platforms", []):
            trending_platforms = ", ".join(market_data.get("trending_platforms", []))
            additional_info += f"🔥 *TRENDING* en {trending_platforms}\n"
        
        # Mostrar actividad de ballenas
        if whale_data.get("has_whale_activity", False):
            whale_count = whale_data.get("known_whales_count", 0)
            additional_info += f"🐋 *Actividad de ballenas detectada* ({whale_count} whales)\n"
        
        # Mostrar crecimiento de holders
        holder_growth = market_data.get("holder_growth_rate_1h", 0)
        if holder_growth > 5:
            additional_info += f"👥 *Holders creciendo* +{holder_growth:.1f}% en 1h\n"
        
        # Mostrar liquidez
        if market_data.get("healthy_liquidity", False):
            additional_info += f"💧 *Liquidez saludable*\n"
        
        # Mostrar calidad de patrones de precio
        token_data = extended_analysis.get("token", {})
        if token_data.get("price_action_quality", 0) > 0.7:
            additional_info += f"📈 *Patrón técnico fuerte*\n"
    
    # Construir mensaje final - Solo con el enlace a NeoBullX
    msg = (
        f"🚨 *SEÑAL DETECTADA* {signal_level_display}\n\n"
        f"Token: {token_name_display}`{token}`\n"
        f"{market_cap_display}"  # Destacando market cap
        f"{volume_display}"     # Destacando volumen
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

def send_performance_report(token, signal_id, timeframe, percent_change, volatility=None, trend=None, 
                          volume_display=None, traders_count=None, whale_activity=None, liquidity_change=None):
    """
    Envía un reporte de rendimiento enriquecido para seguimiento de señales
    """
    # Selección de emoji según el desempeño
    if percent_change > 50:
        emoji = "🚀"  # Excelente
    elif percent_change > 20:
        emoji = "🔥"  # Muy bueno
    elif percent_change > 0:
        emoji = "✅"  # Positivo
    elif percent_change > -20:
        emoji = "⚠️"  # Moderado
    else:
        emoji = "❌"  # Muy negativo
    
    # Añadir volatilidad y tendencia
    volatility_display = f"Volatilidad: *{volatility:.2f}%*\n" if volatility is not None else ""
    trend_display = f"Tendencia: *{trend}*\n" if trend else ""
    volume_info = f"Volumen: `{volume_display}`\n" if volume_display else ""
    traders_info = f"Traders activos: `{traders_count}`\n" if traders_count else ""
    
    # Información adicional de actividad de whales y liquidez
    additional_info = ""
    if whale_activity:
        additional_info += f"🐋 *Actividad de ballenas detectada*\n"
    if liquidity_change and liquidity_change > 10:
        additional_info += f"💧 *Liquidez aumentó* +{liquidity_change:.1f}%\n"
    elif liquidity_change and liquidity_change < -10:
        additional_info += f"⚠️ *Liquidez disminuyó* {liquidity_change:.1f}%\n"
    
    # Enlace solo a NeoBullX
    neobullx_link = f"https://solana.neobullx.app/asset/{token}"
    
    message = (
        f"*🔍 Seguimiento {timeframe} #{signal_id}*\n\n"
        f"Token: `{token}`\n"
        f"Cambio: *{percent_change:.2f}%* {emoji}\n"
        f"{volatility_display}"
        f"{trend_display}"
        f"{volume_info}"
        f"{traders_info}"
        f"{additional_info}\n"
        f"🔗 *Explorer:*\n"
        f"• [NeoBullX]({neobullx_link})\n"
    )
    
    return send_telegram_message(message)

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
        signals_today = db.count_signals_today()
        txs_today = db.count_transactions_today()
        
        update.message.reply_text(
            f"*Bot Status:*\n"
            f"Active: {'✅' if bot_status['active'] else '🛑'}\n"
            f"Tokens monitored: `{active_tokens}`\n"
            f"Signals today: `{signals_today}`\n"
            f"Transactions processed: `{txs_today}`", 
            parse_mode=ParseMode.MARKDOWN
        )

    def stats_command(update, context):
        if str(update.effective_chat.id) != str(chat_id):
            update.message.reply_text("⛔️ Not authorized.")
            return
        
        try:
            performance_stats = db.get_signals_performance_stats()
            if not performance_stats:
                update.message.reply_text("No hay estadísticas de rendimiento disponibles.")
                return
            
            stats_text = "*Estadísticas de Rendimiento:*\n\n"
            for stat in performance_stats:
                stats_text += f"*{stat['timeframe']}*: "
                stats_text += f"`{stat['avg_percent_change']}%` promedio, "
                stats_text += f"`{stat['success_rate']}%` de éxito "
                stats_text += f"({stat['total_signals']} señales)\n"
            
            update.message.reply_text(stats_text, parse_mode=ParseMode.MARKDOWN)
        except Exception as e:
            logger.error(f"Error en stats_command: {e}")
            update.message.reply_text(f"Error al obtener estadísticas: {e}")

    updater = Updater(bot_token)
    dispatcher = updater.dispatcher
    dispatcher.add_handler(CommandHandler("start", start_command))
    dispatcher.add_handler(CommandHandler("stop", stop_command))
    dispatcher.add_handler(Co
