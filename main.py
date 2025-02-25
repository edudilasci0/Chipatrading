# main.py

import time
import logging
import os
import json
import requests
from wallet_tracker import WalletTracker
from cielo_api import CieloAPI, CieloWebSocketClient
from rugcheck import login_rugcheck_solana, validar_seguridad_contrato

# Configurar logging para registrar errores y eventos
logging.basicConfig(
    filename='app.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s'
)

# Variables de entorno (se deben configurar en Render)
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "TU_TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "TU_TELEGRAM_CHAT_ID")
CIELO_API_KEY = os.getenv("CIELO_API_KEY", None)
# Nota: Para RugCheck, la autenticación se hace mediante firma, por lo que no se requiere API_KEY
DATABASE_URL = os.getenv("DATABASE_URL", "postgres://usuario:clave@host:puerto/dbname")

def send_telegram_alert(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}
    try:
        response = requests.post(url, data=payload)
        if response.status_code == 200:
            logging.info("Alerta enviada a Telegram")
        else:
            logging.error("Error al enviar alerta a Telegram: %s", response.text)
    except Exception as e:
        logging.exception("Excepción al enviar alerta a Telegram: %s", e)

def process_feed_data():
    try:
        cielo = CieloAPI(api_key=CIELO_API_KEY)
        params = {"limit": 50, "minUSD": 100, "includeMarketCap": True}
        data = cielo.get_feed(params)
        if data:
            transactions = data.get("transactions", [])
            for tx in transactions:
                token = tx.get("token_data", {})
                token_mint = token.get("mint")
                if token_mint:
                    # Validar seguridad del token usando RugCheck
                    jwt = login_rugcheck_solana(
                        private_key=os.getenv("SOLANA_PRIVATE_KEY").encode("utf-8"),
                        wallet_public_key=os.getenv("SOLANA_PUBLIC_KEY")
                    )
                    if jwt and validar_seguridad_contrato(jwt, token_mint):
                        message = (
                            f"Transacción relevante:\nToken: {token.get('symbol', 'N/D')}\n"
                            f"USD Value: {tx.get('usd_value', 'N/D')}\nTipo: {tx.get('type', 'N/D')}"
                        )
                        send_telegram_alert(message)
                    else:
                        logging.info("Token %s no pasó validación de seguridad.", token.get("symbol", "N/D"))
        else:
            logging.error("No se obtuvieron datos del feed de Cielo.")
    except Exception as e:
        logging.exception("Error en process_feed_data: %s", e)

def main():
    tracker = WalletTracker()
    # Inicia el WebSocket para actualizaciones en tiempo real (opcional)
    ws_client = CieloWebSocketClient(api_key=CIELO_API_KEY, on_message_callback=lambda msg: logging.info("WS: %s", msg))
    ws_client.run()
    
    while True:
        process_feed_data()
        time.sleep(60)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception("Error en main: %s", e)
