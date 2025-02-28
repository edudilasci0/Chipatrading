# cielo_api.py
import asyncio
import websockets
import json
import os

class CieloAPI:
    """
    Maneja la conexión WebSocket a la API de Cielo.
    """

    def __init__(self, api_key=None):
        # Si no se pasa una api_key, se busca en ENV o se usa la real por defecto.
        if api_key is None:
            api_key = os.environ.get("CIELO_API_KEY", "bb4dbdac-9ac7-4c42-97d3-f6435d0674da")
        self.api_key = api_key

        self.ws_url = "wss://feed-api.cielo.finance/api/v1/ws"

    async def connect(self, on_message_callback, filter_params=None):
        headers = {
            "X-API-KEY": self.api_key
        }
        print(f"🔗 Conectando a la API de Cielo con key: {self.api_key}")

        async with websockets.connect(self.ws_url, extra_headers=headers) as ws:
            print("✅ WebSocket conectado a Cielo")

            if filter_params is None:
                filter_params = {}

            subscribe_message = {
                "type": "subscribe_feed",
                "filter": filter_params
            }
            await ws.send(json.dumps(subscribe_message))
            print(f"📡 Suscrito con filtros => {filter_params}")

            async for message in ws:
                await on_message_callback(message)

    async def run_forever(self, on_message_callback, filter_params=None):
        while True:
            try:
                await self.connect(on_message_callback, filter_params)
            except (websockets.ConnectionClosed, OSError) as e:
                print(f"🔴 Conexión cerrada o error de red: {e}. Reintentando en 5s...")
                await asyncio.sleep(5)
            except Exception as e:
                print(f"🚨 Error inesperado: {e}, reintentando en 5s...")
                await asyncio.sleep(5)
