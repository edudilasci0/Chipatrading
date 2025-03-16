import asyncio
import websockets
import json
import time
import logging
from config import Config

logger = logging.getLogger("cielo_api")

class CieloAPI:
    def __init__(self, api_key):
        """
        Inicializa la conexión con la API de Cielo mediante WebSocket.
        """
        self.api_key = api_key
        self.ws_url = "wss://feed-api.cielo.finance/api/v1/ws"
        self.connection_failures = 0

    async def subscribe_to_wallets(self, ws, wallets, filter_params=None):
        """
        Suscribe a múltiples wallets en la conexión WebSocket.
        Utiliza bloques (chunks) para evitar saturar la API.
        """
        subscription_params = {
            "chains": ["solana"],
            "tx_types": ["swap", "transfer"],  # Se filtran transacciones relevantes
        }
        chunk_size = 50  # Número de wallets por bloque
        for i in range(0, len(wallets), chunk_size):
            chunk = wallets[i:i + chunk_size]
            for wallet in chunk:
                msg = {
                    "type": "subscribe_wallet",
                    "wallet": wallet,
                    "filter": subscription_params
                }
                await ws.send(json.dumps(msg))
                # Pequeña pausa entre envíos para evitar saturar la conexión
                await asyncio.sleep(0.02)
            if i + chunk_size < len(wallets):
                logger.info(f"Progreso: {i + chunk_size}/{len(wallets)} wallets suscritas...")
                await asyncio.sleep(0.5)
        logger.info("✅ Todas las wallets han sido suscritas")

    async def _ping_periodically(self, ws):
        """
        Envía mensajes de ping periódicamente para mantener viva la conexión.
        """
        while True:
            try:
                await asyncio.sleep(300)  # Cada 5 minutos
                ping_message = {"type": "ping"}
                await ws.send(json.dumps(ping_message))
                logger.info("📤 Ping enviado a Cielo WebSocket")
            except Exception as e:
                logger.error(f"Error enviando ping: {e}")
                break

    async def run_forever_wallets(self, wallets, on_message_callback, filter_params=None):
        """
        Mantiene la conexión WebSocket abierta y reintenta en caso de fallo.
        Se suscribe a las wallets y procesa los mensajes mediante on_message_callback.
        """
        retry_delay = 1
        max_retry_delay = 60
        attempt = 0

        while True:
            try:
                async with websockets.connect(
                    self.ws_url, 
                    extra_headers={"X-API-KEY": self.api_key}, 
                    ping_interval=30
                ) as ws:
                    logger.info("📡 WebSocket conectado a Cielo (modo multi-wallet)")
                    self.connection_failures = 0
                    attempt = 0
                    await self.subscribe_to_wallets(ws, wallets, filter_params)
                    # Iniciar tarea para enviar pings periódicos
                    ping_task = asyncio.create_task(self._ping_periodically(ws))
                    try:
                        async for message in ws:
                            try:
                                await on_message_callback(message)
                            except Exception as e:
                                logger.error(f"Error procesando mensaje: {e}", exc_info=True)
                                continue
                    finally:
                        ping_task.cancel()
                        try:
                            await ping_task
                        except asyncio.CancelledError:
                            pass
            except (websockets.ConnectionClosed, OSError) as e:
                self.connection_failures += 1
                attempt += 1
                retry_delay = min(2 ** attempt, max_retry_delay)
                logger.warning(f"Conexión cerrada, reintentando en {retry_delay}s: {e}")
                await asyncio.sleep(retry_delay)
            except Exception as e:
                self.connection_failures += 1
                attempt += 1
                retry_delay = min(2 ** attempt, max_retry_delay)
                logger.error(f"Error inesperado: {e}", exc_info=True)
                await asyncio.sleep(retry_delay)
