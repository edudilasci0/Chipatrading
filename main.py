# main.py

import time
import logging
import os
import requests
from datetime import datetime

from wallet_tracker import WalletTracker
from cielo_api import CieloAPI, CieloWebSocketClient
from rugcheck import login_rugcheck_solana, validar_seguridad_contrato
from db import init_db, save_transaction

# Configuración de logging: se registra en app.log y en la salida estándar.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)

# Variables de entorno (configúralas en Render)
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "YOUR_TELEGRAM_CHAT_ID")
CIELO_API_KEY = os.getenv("CIELO_API_KEY", None)
DATABASE_PATH = os.getenv("DATABASE_PATH", "postgres://user:pass@host:port/dbname")
SOLANA_PRIVATE_KEY = os.getenv("SOLANA_PRIVATE_KEY", "YOUR_SOLANA_PRIVATE_KEY")
SOLANA_PUBLIC_KEY = os.getenv("SOLANA_PUBLIC_KEY", "YOUR_SOLANA_PUBLIC_KEY")

# Contadores globales para el resumen de actividad
processed_transactions = 0
inserted_transactions = 0

def send_telegram_alert(message):
    """Envía una alerta a Telegram y registra el resultado."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}
    try:
        response = requests.post(url, data=payload)
        if response.status_code == 200:
            logging.info("Alerta enviada a Telegram: %s", message)
        else:
            logging.error("Error al enviar alerta a Telegram: %s", response.text)
    except Exception as e:
        logging.exception("Excepción al enviar alerta a Telegram: %s", e)

def send_summary_alert(processed, inserted):
    """
    Envía un resumen de actividad a Telegram con la cantidad de transacciones procesadas
    e insertadas en la base de datos.
    """
    message = (
        f"Resumen de actividad:\n"
        f"- Transacciones procesadas: {processed}\n"
        f"- Transacciones insertadas: {inserted}"
    )
    send_telegram_alert(message)

def process_feed_data():
    """
    Consulta el feed de Cielo, analiza cada token y, si pasa la validación de seguridad,
    envía alertas a Telegram y guarda la transacción en la base de datos.
    """
    global processed_transactions, inserted_transactions
    try:
        cielo = CieloAPI(api_key=CIELO_API_KEY)
        params = {"limit": 50, "minUSD": 100, "includeMarketCap": True}
        data = cielo.get_feed(params)
        if data:
            transactions = data.get("transactions", [])
            for tx in transactions:
                processed_transactions += 1
                token = tx.get("token_data", {})
                token_mint = token.get("mint")
                if token_mint:
                    # Autenticarse en RugCheck y validar el token
                    jwt = login_rugcheck_solana(
                        private_key=SOLANA_PRIVATE_KEY.encode("utf-8"),
                        wallet_public_key=SOLANA_PUBLIC_KEY
                    )
                    if jwt and validar_seguridad_contrato(jwt, token_mint):
                        alert_message = (
                            f"Transacción relevante:\nToken: {token.get('symbol', 'N/D')}\n"
                            f"USD Value: {tx.get('usd_value', 'N/D')}\nTipo: {tx.get('type', 'N/D')}"
                        )
                        send_telegram_alert(alert_message)
                        
                        tx_data = {
                            "id": tx.get("id", "N/D"),
                            "symbol": token.get("symbol", "N/D"),
                            "mint": token_mint,
                            "usd_value": tx.get("usd_value", 0),
                            "type": tx.get("type", "N/D"),
                            "timestamp": tx.get("timestamp", datetime.now().isoformat())
                        }
                        save_transaction(tx_data, send_telegram_alert)
                        inserted_transactions += 1
                    else:
                        logging.info("Token %s rechazado en validación.", token.get("symbol", "N/D"))
                        send_telegram_alert(f"Token {token.get('symbol', 'N/D')} rechazado en validación.")
        else:
            logging.error("No se obtuvieron datos del feed de Cielo.")
    except Exception as e:
        logging.exception("Error en process_feed_data: %s", e)
        send_telegram_alert("Error en process_feed_data.")

def main():
    logging.info("Inicio del bot de trading...")
    print("Inicio del bot de trading...")
    
    # Inicializa la base de datos (crea tablas si no existen)
    init_db()

    # Inicializa el tracker (si lo utilizas para gestionar wallets)
    tracker = WalletTracker()

    # Enviar mensaje de prueba a Telegram para confirmar integración
    send_telegram_alert("Mensaje de prueba: El bot de trading se inició correctamente.")
    
    # Inicia el WebSocket para actualizaciones en tiempo real (opcional)
    ws_client = CieloWebSocketClient(
        api_key=CIELO_API_KEY, 
        on_message_callback=lambda msg: logging.info("WS: %s", msg)
    )
    ws_client.run()
    
    # Bucle principal: ejecuta el proceso y envía resumen cada 60 segundos
    while True:
        logging.info("Ejecutando process_feed_data()")
        process_feed_data()
        send_summary_alert(processed_transactions, inserted_transactions)
        time.sleep(60)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception("Error en main: %s", e)
        send_telegram_alert("Error en main.")
