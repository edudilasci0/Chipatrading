# main.py
import asyncio
import requests
from cielo_api import CieloAPI
from dexscreener_api import DexScreenerClient
from scoring import ScoringSystem
from db import init_db_connection
# from signal_logic import SignalLogic   # si lo usas

TELEGRAM_BOT_TOKEN = "123456789:ABCXYZ"
TELEGRAM_CHAT_ID = "-1001234567890"
CIELO_API_KEY = "bb4dbdac-9ac7-4c42-97d3-f6435d0674da"

def send_telegram_message(text: str, parse_mode="Markdown"):
    """
    Envía un mensaje a Telegram usando requests.
    """
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": parse_mode
    }
    try:
        resp = requests.post(url, data=payload)
        if resp.status_code == 200:
            print("✉️ Mensaje enviado a Telegram con éxito.")
        else:
            print(f"⚠️ Error al enviar mensaje a Telegram: {resp.text}")
    except Exception as e:
        print(f"🚨 Excepción al enviar mensaje a Telegram: {e}")

async def daily_summary_loop():
    """
    Envía un resumen diario de las actividades (opcional).
    """
    while True:
        try:
            await asyncio.sleep(86400)  # 24 horas
            # Recolectar datos del día (ejemplo).
            signals_today = 5
            runners_detected = 2
            summary_msg = (
                "*Resumen Diario Chipatrading*\n\n"
                f"• Señales emitidas hoy: `{signals_today}`\n"
                f"• Daily runners detectados: `{runners_detected}`\n\n"
                "¡Gracias por usar Chipatrading!"
            )
            send_telegram_message(summary_msg)
        except Exception as e:
            print(f"🚨 Error en daily_summary_loop: {e}")

async def on_cielo_message(message: str, scoring_system: ScoringSystem, dexscreener_client: DexScreenerClient):
    """
    Función que maneja cada mensaje proveniente de Cielo (WebSocket).
    Procesa transacciones, actualiza scoring, etc.
    """
    import json
    try:
        data = json.loads(message)
        if "transactions" in data:
            for tx in data["transactions"]:
                tx_type = tx.get("type")
                usd_value = float(tx.get("amount_usd", 0))
                wallet = tx.get("wallet")
                token = tx.get("token", "Unknown")

                # Filtro: solo BUY/SELL, >300 USD, etc. Ajusta según quieras.
                if tx_type in ["BUY", "SELL"] and usd_value >= 300:
                    print(f"🔎 Transacción Relevante => {tx}")

                    # Actualizar score
                    scoring_system.update_score_on_trade(wallet, tx)

                    # Podrías obtener volumen asc y market cap
                    # (placeholder de DexScreener)
                    vol_data = dexscreener_client.get_volume_growth(token)
                    market_cap = dexscreener_client.get_market_cap(token)

                    # Calcular confianza, etc.
                    # Ej: confidence = scoring_system.compute_signal_confidence([...], { ... })

                    # Mandar alerta Telegram de la transacción
                    msg = (
                        f"Trader {wallet} hizo {tx_type} en {token} "
                        f"({usd_value}$). MarketCap aprox: {market_cap}$"
                    )
                    send_telegram_message(msg)

    except json.JSONDecodeError as e:
        print(f"⚠️ Error decodificando JSON => {e}")
    except Exception as e:
        print(f"🚨 Error en on_cielo_message => {e}")

async def main():
    # 1) Conexión a la BD (placeholder)
    init_db_connection()  # en db.py define la lógica

    # 2) Mensaje de bienvenida
    welcome_msg = (
        "*Bienvenido(a) a Chipatrading*\n\n"
        "Hola, soy **ChipatradingBot**.\n\n"
        "Estoy aquí para:\n"
        "• Detectar tokens con volumen ascendente\n"
        "• Analizar transacciones de whales y scalpers\n"
        "• Emitir señales si encuentro daily runners\n\n"
        "¡Estaré enviando actualizaciones cuando detecte movimientos interesantes!"
    )
    send_telegram_message(welcome_msg)

    # 3) Instanciar DexScreenerClient
    dexscreener_client = DexScreenerClient()

    # 4) Instanciar el sistema de scoring
    scoring_system = ScoringSystem()

    # 5) Instanciar CieloAPI y filtrar swaps solana > 300
    from cielo_api import CieloAPI
    cielo = CieloAPI(api_key=CIELO_API_KEY)

    filter_params = {
        "chains": ["solana"],
        "tx_types": ["swap"],
        "min_usd_value": 300
    }

    # 6) Tarea para resumen diario (opcional)
    asyncio.create_task(daily_summary_loop())

    # 7) Callback de Cielo
    async def handle_cielo_msg(msg):
        await on_cielo_message(msg, scoring_system, dexscreener_client)

    # 8) Ejecutar el WebSocket
    await cielo.run_forever(on_message_callback=handle_cielo_msg, filter_params=filter_params)

if __name__ == "__main__":
    asyncio.run(main())
