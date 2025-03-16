import os
import sys
import time
import asyncio
import json
import logging
import websockets

from config import Config

logger = logging.getLogger("cielo_api")

class CieloAPI:
    """
    Clase para manejar la conexión WebSocket a la API de Cielo.
    Se encarga de suscribirse a las wallets y distribuir mensajes a un callback.
    """

    def __init__(self, api_key=None):
        self.api_key = api_key if api_key else Config.CIELO_API_KEY
        self.ws_url = "wss://feed-api.cielo.finance/api/v1/ws"
        self.last_connection_attempt = 0
        self.connection_failures = 0

    async def subscribe_to_wallets(self, ws, wallets, filter_params=None):
        """
        Suscribe múltiples wallets en la conexión WebSocket.
        """
        subscription_params = {
            "chains": ["solana"],
            "tx_types": ["swap", "transfer"],
        }
        
        chunk_size = 50  # Procesamos en bloques pequeños
        for i in range(0, len(wallets), chunk_size):
            chunk = wallets[i:i+chunk_size]
            for wallet in chunk:
                msg = {
                    "type": "subscribe_wallet",
                    "wallet": wallet,
                    "filter": subscription_params
                }
                await ws.send(json.dumps(msg))
                # Pausa corta para evitar saturar la API
                await asyncio.sleep(0.02)
            if i + chunk_size < len(wallets):
                logger.info(f"Progreso: {i+chunk_size}/{len(wallets)} wallets suscritas...")
                await asyncio.sleep(0.5)
        logger.info("✅ Todas las wallets han sido suscritas")

    async def _ping_periodically(self, ws):
        """
        Envía pings periódicos para mantener viva la conexión.
        """
        while True:
            try:
                await asyncio.sleep(300)  # cada 5 minutos
                ping_message = {"type": "ping"}
                await ws.send(json.dumps(ping_message))
                logger.debug("📤 Ping enviado a Cielo WebSocket")
            except Exception as e:
                logger.warning(f"Error enviando ping: {e}")
                break

    async def _reconnect_with_backoff(self, attempt=0, max_attempts=10):
        """
        Manejo mejorado de reconexiones con backoff exponencial.
        """
        if attempt >= max_attempts:
            logger.critical("Número máximo de intentos alcanzado. Reiniciando conexión...")
            from telegram_utils import send_telegram_message
            send_telegram_message("❌ *Error Crítico*: Conexión a Cielo perdida tras múltiples intentos. Reiniciando...")
            return False
        import random
        delay = min(30, (2 ** attempt) * (0.8 + 0.4 * random.random()))
        logger.warning(f"Reconectando a Cielo en {delay:.2f}s (intento {attempt+1}/{max_attempts})")
        await asyncio.sleep(delay)
        return True

    async def run_forever_wallets(self, wallets, on_message_callback, filter_params=None):
        """
        Mantiene una conexión WebSocket abierta y se suscribe a las wallets.
        Reintenta la conexión en caso de error usando backoff exponencial.
        """
        attempt = 0
        max_retry_delay = 60
        while True:
            try:
                self.last_connection_attempt = time.time()
                headers = {"X-API-KEY": self.api_key}
                async with websockets.connect(self.ws_url, extra_headers=headers, ping_interval=30) as ws:
                    logger.info("📡 WebSocket conectado a Cielo (modo multi-wallet)")
                    self.connection_failures = 0
                    attempt = 0
                    await self.subscribe_to_wallets(ws, wallets, filter_params)
                    ping_task = asyncio.create_task(self._ping_periodically(ws))
                    try:
                        async for message in ws:
                            try:
                                # Log de depuración
                                await self.log_raw_message(message)
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
                logger.error(f"🚨 Error inesperado ({self.connection_failures}): {e}", exc_info=True)
                await asyncio.sleep(retry_delay)

    async def log_raw_message(self, message):
        """
        Registra el mensaje recibido para depuración.
        """
        try:
            data = json.loads(message)
            logger.debug(f"Mensaje recibido: {data.get('type', 'No type')}")
        except Exception as e:
            logger.error(f"Error al loguear mensaje: {e}")
