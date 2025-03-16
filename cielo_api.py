import time
import json
import asyncio
import requests
import logging
from config import Config

logger = logging.getLogger("cielo_api")

class HeliusClient:
    def __init__(self, api_key):
        self.api_key = api_key
        self.ws_url = "wss://feed-api.cielo.finance/api/v1/ws"
        self.last_connection_attempt = 0
        self.connection_failures = 0

    def _request(self, endpoint, params, version="v1"):
        url = f"https://api.helius.xyz/{version}/{endpoint}"
        if version == "v1":
            params["apiKey"] = self.api_key
        else:  # v0
            params["api-key"] = self.api_key
        try:
            logger.debug(f"Solicitando Helius {version}: {url}")
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error en HeliusClient._request ({version}): {e}")
            return None

    def get_token_data(self, token):
        now = time.time()
        if token in getattr(self, "cache", {}) and now - self.cache[token]["timestamp"] < Config.HELIUS_CACHE_DURATION:
            return self.cache[token]["data"]
        data = None
        endpoint = f"tokens/{token}"
        data = self._request(endpoint, {}, version="v1")
        if not data:
            data = self._request(endpoint, {}, version="v0")
        if not data:
            endpoint = f"addresses/{token}/tokens"
            data = self._request(endpoint, {}, version="v0")
        if data:
            if isinstance(data, list) and data:
                data = data[0]
            normalized_data = {
                "price": self._extract_value(data, ["price", "priceUsd"]),
                "market_cap": self._extract_value(data, ["marketCap", "market_cap"]),
                "volume": self._extract_value(data, ["volume24h", "volume", "volumeUsd"]),
                "volume_growth": {
                    "growth_5m": self._normalize_percentage(self._extract_value(data, ["volumeChange5m", "volume_change_5m"])),
                    "growth_1h": self._normalize_percentage(self._extract_value(data, ["volumeChange1h", "volume_change_1h"]))
                },
                "source": "helius"
            }
            if not hasattr(self, "cache"):
                self.cache = {}
            self.cache[token] = {"data": normalized_data, "timestamp": now}
            return normalized_data
        logger.warning(f"No se pudieron obtener datos para el token {token} desde Helius")
        return None

    def _extract_value(self, data, possible_keys):
        for key in possible_keys:
            if key in data:
                return data[key]
        return 0

    def _normalize_percentage(self, value):
        if value is None:
            return 0
        if value > 1 or value < -1:
            return value / 100
        return value

    async def subscribe_to_wallets(self, ws, wallets, filter_params=None):
        subscription_params = {
            "chains": ["solana"],
            "tx_types": ["swap", "transfer"],
        }
        chunk_size = 50
        for i in range(0, len(wallets), chunk_size):
            chunk = wallets[i:i+chunk_size]
            for wallet in chunk:
                msg = {
                    "type": "subscribe_wallet",
                    "wallet": wallet,
                    "filter": subscription_params
                }
                await ws.send(json.dumps(msg))
                await asyncio.sleep(0.02)
            if i + chunk_size < len(wallets):
                print(f"  Progreso: {i+chunk_size}/{len(wallets)} wallets suscritas...")
                await asyncio.sleep(0.5)
        print("✅ Todas las wallets han sido suscritas")

    async def _ping_periodically(self, ws):
        while True:
            try:
                await asyncio.sleep(300)
                ping_message = {"type": "ping"}
                await ws.send(json.dumps(ping_message))
                logger.info("Ping enviado a Cielo WebSocket")
            except Exception as e:
                logger.error(f"Error enviando ping: {e}")
                break

    async def _reconnect_with_backoff(self, attempt=0, max_attempts=10):
        if attempt >= max_attempts:
            logger.critical("Número máximo de intentos alcanzado. Reiniciando completamente...")
            from telegram_utils import send_telegram_message
            send_telegram_message("❌ *Error Crítico*: Conexión a Cielo perdida tras múltiples intentos. Reiniciando...")
            await asyncio.sleep(60)
            return False
        import random
        delay = min(30, (2 ** attempt) * (0.8 + 0.4 * random.random()))
        logger.warning(f"Reconectando a Cielo en {delay:.2f}s (intento {attempt+1}/{max_attempts})")
        await asyncio.sleep(delay)
        return True

    async def run_forever_wallets(self, wallets, on_message_callback, filter_params=None):
        attempt = 0
        max_retry_delay = 60
        import websockets
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
            except (asyncio.TimeoutError, websockets.ConnectionClosed, OSError) as e:
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

# También se exporta HeliusClient
