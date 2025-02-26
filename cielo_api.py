# cielo_api.py
import asyncio
import websockets
import json

class CieloAPI:
    """
    Maneja la conexión WebSocket a la API de Cielo y la suscripción al feed.
    """

    def __init__(self, api_key: str):
        """
        :param api_key: API Key de Cielo
        """
        self.api_key = api_key
        self.ws_url = "wss://feed-api.cielo.finance/api/v1/ws"
        self.websocket = None

    async def connect(self, on_message_callback, filter_params=None):
        """
        Establece la conexión WebSocket, suscribe el feed y procesa mensajes entrantes.
        :param on_message_callback: Función async que maneja cada mensaje del WS.
        :param filter_params: dict de filtros para 'subscribe_feed'.
        """
        headers = {
            "X-API-KEY": self.api_key  # Se incluye la API Key directamente
        }
        print(f"🔗 Conectando a Cielo con API Key: {self.api_key}...")

        async with websockets.connect(self.ws_url, extra_headers=headers) as ws:
            self.websocket = ws
            print("✅ WebSocket conectado a Cielo")

            # Enviar comando subscribe_feed con los filtros
            if filter_params is None:
                filter_params = {}

            subscribe_message = {
                "type": "subscribe_feed",
                "filter": filter_params
            }
            await ws.send(json.dumps(subscribe_message))
            print(f"📡 Suscrito con filtros => {filter_params}")

            # Escuchar mensajes indefinidamente
            async for message in ws:
                await on_message_callback(message)

    async def run_forever(self, on_message_callback, filter_params=None):
        """
        Ejecuta la conexión en un bucle infinito, reintentando si se cierra.
        """
        while True:
            try:
                await self.connect(on_message_callback, filter_params)
            except (websockets.ConnectionClosed, OSError) as e:
                print(f"🔴 Conexión cerrada o error de red: {e}. Reintentando en 5s...")
                await asyncio.sleep(5)
            except Exception as e:
                print(f"🚨 Error inesperado: {e}, reintentando en 5s...")
                await asyncio.sleep(5)
