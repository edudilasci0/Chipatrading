# main.py (fragmento modificado)

import time
import logging
import os
import requests
import json
from datetime import datetime

from wallet_tracker import WalletTracker
from cielo_api import CieloAPI, CieloWebSocketClient
from rugcheck import login_rugcheck_solana, validar_seguridad_contrato
from db import init_db, save_transaction

# Configuración de logging (ya lo tienes configurado)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)

# Variables de entorno configuradas en Render
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "YOUR_TELEGRAM_CHAT_ID")
CIELO_API_KEY = os.getenv("CIELO_API_KEY", None)
DATABASE_PATH = os.getenv("DATABASE_URL", "postgres://user:pass@host:port/dbname")
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

def load_wallets():
    """
    Carga la lista de wallets desde el archivo traders_data.json.
    Se espera que cada entrada tenga una clave "Wallet" con la dirección.
    """
    try:
        with open("traders_data.json", "r") as f:
            data = json.load(f)
        wallets = [entry["Wallet"] for entry in data if "Wallet" in entry]
        logging.info("Wallets cargadas: %s", wallets)
        return wallets
    except Exception as e:
        logging.exception("Error al cargar wallets: %s", e)
        return []

def process_feed_data():
    """
    Recorre la lista de wallets, consulta el feed de Cielo filtrando por cada wallet,
    e intenta procesar las transacciones.
    """
    global processed_transactions, inserted_transactions
    try:
        cielo = CieloAPI(api_key=CIELO_API_KEY)
        wallets = load_wallets()
        if not wallets:
            logging.error("No se cargaron wallets para filtrar el feed.")
            return

        for wallet in wallets:
            params = {
                "wallet": wallet,
                "limit": 50,
                "minUSD": 100,
                "includeMarketCap": True
            }
            data = cielo.get_feed(params)
            logging.info("Wallet %s - Respuesta completa del feed: %s", wallet, data)
            if data:
                transactions = data.get("transactions", [])
                if not transactions:
                    logging.info("Wallet %s - No se encontraron transacciones.", wallet)
                else:
                    for tx in transactions:
                        processed_transactions += 1
                        token = tx.get("token_data", {})
                        token_mint = token.get("mint")
                        if token_mint:
                            jwt = login_rugcheck_solana(
                                private_key=SOLANA_PRIVATE_KEY.encode("utf-8"),
                                wallet_public_key=SOLANA_PUBLIC_KEY
                            )
                            if jwt and validar_seguridad_contrato(jwt, token_mint):
                                alert_message = (
                                    f"Transacción relevante (Wallet {wallet}):\n"
                                    f"Token: {token.get('symbol', 'N/D')}\n"
                                    f"USD Value: {tx.get('usd_value', 'N/D')}\n"
                                    f"Tipo: {tx.get('type', 'N/D')}"
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
                                logging.info("Wallet %s - Token %s rechazado en validación.", wallet, token.get("symbol", "N/D"))
                                send_telegram_alert(f"Wallet {wallet} - Token {token.get('symbol', 'N/D')} rechazado en validación.")
            else:
                logging.error("Wallet %s - No se obtuvieron datos del feed de Cielo.", wallet)
    except Exception as e:
        logging.exception("Error en process_feed_data: %s", e)
        send_telegram_alert("Error en process_feed_data.")

def main():
    logging.info("Inicio del bot de trading...")
    print("Inicio del bot de trading...")
    
    # Inicializa la base de datos (crea tablas si no existen)
    init_db()

    # Inicializa el tracker (si se utiliza)
    tracker = WalletTracker()

    # Envía un mensaje de prueba a Telegram para confirmar la integración
    send_telegram_alert("Mensaje de prueba: El bot de trading se inició correctamente.")
    
    # Inicia el WebSocket para actualizaciones en tiempo real desde Cielo (opcional)
    ws_client = CieloWebSocketClient(
        api_key=CIELO_API_KEY, 
        on_message_callback=lambda msg: logging.info("WS: %s", msg)
    )
    ws_client.run()
    
    # Bucle principal: ejecuta el proceso y envía un resumen cada 60 segundos
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
