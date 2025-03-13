import asyncio
import websockets
import json
import time
from config import Config
import logging

logger = logging.getLogger("cielo_api")

class CieloAPI:
    """
    Maneja la conexión WebSocket a la API de Cielo.
    """
    def __init__(self, api_key=None):
        self.api_key = api_key if api_key else Config.CIELO_API_KEY
        self.ws_url = "wss://feed-api.cielo.finance/api/v1/ws"
        self.last_connection_attempt = 0
        self.connection_failures = 0

    async def log_raw_message(self, message):
        try:
            data = json.loads(message)
            print(f"\n-------- MENSAJE CIELO RECIBIDO --------")
            print(f"Tipo de mensaje: {data.get('type', 'No type')}")
            print(f"MENSAJE COMPLETO: {json.dumps(data)[:1000]}...")
            if "transactions" in data:
                txs = data["transactions"]
                print(f"Contiene {len(txs)} transacciones")
                if len(txs) > 0:
                    print(f"\nTransacción completa #1:")
                    print(json.dumps(txs[0], indent=2))
                for i, tx in enumerate(txs[:3]):
                    print(f"\nTransacción #{i+1}:")
                    print(f"  Tipo: {tx.get('type', 'N/A')}")
                    print(f"  Wallet: {tx.get('wallet', 'N/A')}")
                    print(f"  Token: {tx.get('token', 'N/A')}")
                    value_fields = [k for k in tx.keys() if 'amount' in k.lower() or 'value' in k.lower() or 'usd' in k.lower()]
                    for field in value_fields:
                        print(f"  {field}: {tx.get(field, 'N/A')}")
                    other_fields = [k for k in tx.keys() if k not in ['type', 'wallet', 'token'] and k not in value_fields]
                    for field in other_fields:
                        print(f"  {field}: {tx.get(field, 'N/A')}")
                if len(txs) > 3:
                    print(f"... y {len(txs) - 3} transacciones más")
            print("----------------------------------------\n")
        except Exception as e:
            print(f"Error al loguear mensaje: {e}")
            print(f"Mensaje original: {message[:500]}...")

    async def subscribe_to_wallets(self, ws, wallets, filter_params=None):
        subscription_params = {
            "chains": ["solana"],
            "tx_types": ["swap", "transfer"],
        }
        print(f"🔄 Suscribiendo a {len(wallets)} wallets con filtros: {filter_params}")
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
            if i + chunk_size < len(wallets):
                print(f"  Progress: {i+chunk_size}/{len(wallets)} wallets suscritas...")
                await asyncio.sleep(0.5)
        print("✅ Todas las wallets han sido suscritas")

    async def _ping_periodically(self, ws):
        while True:
            try:
                await asyncio.sleep(300)
                ping_message = {"type": "ping"}
                await ws.send(json.dumps(ping_message))
                print("📤 Ping enviado a Cielo WebSocket")
            except Exception as e:
                print(f"⚠️ Error enviando ping: {e}")
                break

    async def _reconnect_with_backoff(self, attempt=0, max_attempts=10):
        if attempt >= max_attempts:
            logger.critical("Número máximo de intentos alcanzado. Deteniendo bot.")
            from telegram_utils import send_telegram_message
            send_telegram_message("❌ *Error Crítico*: Conexión a Cielo perdida permanentemente. Revisa los logs.")
            return False
        delay = min(30, 2 ** attempt)
        logger.warning(f"Reconectando a Cielo en {delay}s (intento {attempt+1}/{max_attempts})")
        await asyncio.sleep(delay)
        return True

    async def run_forever_wallets(self, wallets, on_message_callback, filter_params=None):
        attempt = 0
        max_retry_delay = 60
        while True:
            try:
                self.last_connection_attempt = time.time()
                headers = {"X-API-KEY": self.api_key}
                async with websockets.connect(self.ws_url, extra_headers=headers, ping_interval=30) as ws:
                    print("📡 WebSocket conectado a Cielo (modo multi-wallet)")
                    self.connection_failures = 0
                    attempt = 0
                    await self.subscribe_to_wallets(ws, wallets, filter_params)
                    ping_task = asyncio.create_task(self._ping_periodically(ws))
                    try:
                        async for message in ws:
                            await self.log_raw_message(message)
                            await on_message_callback(message)
                    finally:
                        ping_task.cancel()
            except (websockets.ConnectionClosed, OSError) as e:
                self.connection_failures += 1
                attempt += 1
                if not await self._reconnect_with_backoff(attempt, max_retry_delay):
                    break
                continue
            except Exception as e:
                self.connection_failures += 1
                attempt += 1
                logger.error(f"🚨 Error inesperado ({self.connection_failures}): {e}, reintentando...", exc_info=True)
                if not await self._reconnect_with_backoff(attempt, max_retry_delay):
                    break

    async def run_forever(self, on_message_callback, filter_params=None):
        attempt = 0
        max_retry_delay = 60
        while True:
            try:
                headers = {"X-API-KEY": self.api_key}
                async with websockets.connect(self.ws_url, extra_headers=headers, ping_interval=30) as ws:
                    print("📡 WebSocket conectado a Cielo (modo feed)")
                    self.connection_failures = 0
                    attempt = 0
                    subscribe_message = {
                        "type": "subscribe_feed",
                        "filter": filter_params or {}
                    }
                    await ws.send(json.dumps(subscribe_message))
                    print(f"📡 Suscrito con filtros => {filter_params}")
                    ping_task = asyncio.create_task(self._ping_periodically(ws))
                    try:
                        async for message in ws:
                            await self.log_raw_message(message)
                            await on_message_callback(message)
                    finally:
                        ping_task.cancel()
            except (websockets.ConnectionClosed, OSError) as e:
                self.connection_failures += 1
                attempt += 1
                print(f"🔴 Conexión cerrada o error de red ({self.connection_failures}): {e}")
                print(f"Reintentando en {2 ** attempt}s...")
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                self.connection_failures += 1
                attempt += 1
                print(f"🚨 Error inesperado ({self.connection_failures}): {e}, reintentando en {2 ** attempt}s...")
                await asyncio.sleep(2 ** attempt)
