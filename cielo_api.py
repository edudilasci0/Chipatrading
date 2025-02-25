# cielo_api.py

import requests
import json
import threading
import websocket

class CieloAPI:
    BASE_URL = "https://feed-api.cielo.finance/api/v1"

    def __init__(self, api_key=None):
        self.api_key = api_key

    def get_feed(self, params={}):
        url = f"{self.BASE_URL}/feed"
        try:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                return response.json()
            else:
                print("Error en get_feed:", response.status_code, response.text)
                return None
        except Exception as e:
            print("Excepción en get_feed:", e)
            return None

    def get_wallet_by_address(self, wallet_address):
        url = f"{self.BASE_URL}/tracked-wallets/address/{wallet_address}"
        try:
            response = requests.get(url)
            if response.status_code == 200:
                return response.json()
            else:
                print("Error en get_wallet_by_address:", response.status_code, response.text)
                return None
        except Exception as e:
            print("Excepción en get_wallet_by_address:", e)
            return None

class CieloWebSocketClient:
    WS_URL = "wss://feed-api.cielo.finance/api/v1/ws"

    def __init__(self, api_key=None, on_message_callback=None):
        self.api_key = api_key
        self.on_message_callback = on_message_callback
        self.ws = None

    def on_message(self, ws, message):
        print("Mensaje recibido:", message)
        if self.on_message_callback:
            self.on_message_callback(message)

    def on_error(self, ws, error):
        print("Error en WebSocket:", error)

    def on_close(self, ws, close_status_code, close_msg):
        print("WebSocket cerrado:", close_status_code, close_msg)

    def on_open(self, ws):
        print("WebSocket conectado")
        subscribe_message = {
            "type": "subscribe_feed",
            "filter": {
                "tx_types": ["transfer", "swap"],
                "chains": ["solana"],
                "min_usd_value": 100
            }
        }
        ws.send(json.dumps(subscribe_message))
        print("Mensaje de suscripción enviado:", subscribe_message)

    def run(self):
        headers = {}
        if self.api_key:
            headers["X-API-KEY"] = self.api_key

        self.ws = websocket.WebSocketApp(
            self.WS_URL,
            header=headers,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open
        )
        wst = threading.Thread(target=self.ws.run_forever)
        wst.daemon = True
        wst.start()
        return self.ws

    def stop(self):
        if self.ws:
            self.ws.close()
