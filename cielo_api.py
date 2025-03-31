#!/usr/bin/env python3
# cielo_api.py
import asyncio
import websockets
import json
import time
import logging
from config import Config

logger = logging.getLogger("cielo_api")

class CieloAPI:
    def __init__(self, api_key=None):
        self.api_key = api_key if api_key else Config.CIELO_API_KEY
        self.ws_url = "wss://feed-api.cielo.finance/api/v1/ws"
        self.connection_failures = 0
        self.ws = None
        self.is_running = False
        self.ping_task = None
        self.message_callback = None
        self.last_message_time = 0

    async def subscribe_to_wallets(self, ws, wallets, filter_params=None):
        # Nueva versión mejorada
        subscription_params = {
            "chains": ["solana"],
            "tx_types": ["swap", "transfer"],
            "min_amount_usd": float(Config.get("MIN_TRANSACTION_USD", "200"))
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
                logger.debug(f"Suscripción enviada para wallet: {wallet}")
                await asyncio.sleep(0.02)
            if i + chunk_size < len(wallets):
                logger.info(f"Progreso: {i+chunk_size}/{len(wallets)} wallets suscritas...")
                await asyncio.sleep(0.5)
        # Enviar ping para confirmar suscripción
        await ws.send(json.dumps({"type": "ping"}))
        logger.info(f"✅ Todas las {len(wallets)} wallets han sido suscritas")
    
    async def connect(self, wallets):
        """
        Inicia conexión de forma independiente al polling.
        
        Args:
            wallets: Lista de direcciones de wallets a monitorear
        """
        try:
            if self.ws is not None and not self.ws.closed:
                await self.disconnect()
            self.is_running = True
            headers = {"X-API-KEY": self.api_key}
            self.ws = await websockets.connect(self.ws_url, extra_headers=headers, ping_interval=30)
            logger.info("WebSocket conectado a Cielo")
            await self.subscribe_to_wallets(self.ws, wallets)
            self.ping_task = asyncio.create_task(self._ping_periodically(self.ws))
            asyncio.create_task(self._listen_messages(self.ws))
            return True
        except Exception as e:
            logger.error(f"Error en connect: {e}")
            self.connection_failures += 1
            return False

    async def disconnect(self):
        """Cierra conexión ordenadamente"""
        self.is_running = False
        if self.ping_task:
            self.ping_task.cancel()
            try:
                await self.ping_task
            except asyncio.CancelledError:
                pass
        if self.ws:
            try:
                await self.ws.close()
                self.ws = None
                logger.info("WebSocket desconectado de Cielo")
            except Exception as e:
                logger.error(f"Error en disconnect: {e}")

    async def check_availability(self):
        """Verifica la disponibilidad de Cielo"""
        try:
            test_ws = await websockets.connect(self.ws_url, extra_headers={"X-API-KEY": self.api_key})
            await test_ws.send(json.dumps({"type": "ping"}))
            await asyncio.wait_for(test_ws.recv(), timeout=5)
            await test_ws.close()
            return True
        except Exception as e:
            logger.warning(f"Cielo no disponible: {e}")
            return False

    def is_connected(self):
        """Verifica si hay una conexión activa"""
        return self.ws is not None and not self.ws.closed

    def set_message_callback(self, callback):
        """Configura callback externo para mensajes"""
        self.message_callback = callback
        logger.info("Callback de mensajes configurado para Cielo")

    async def _listen_messages(self, ws):
        """Escucha mensajes del WebSocket y los procesa"""
        while self.is_running and not ws.closed:
            try:
                message = await ws.recv()
                self.last_message_time = time.time()
                if self.message_callback:
                    await self.message_callback(message)
            except websockets.ConnectionClosed:
                logger.warning("Conexión a Cielo cerrada")
                break
            except Exception as e:
                logger.error(f"Error recibiendo mensaje: {e}")
        if self.is_running:
            logger.info("Intentando reconectar automáticamente...")
            self.ws = None

    async def run_forever_wallets(self, wallets, on_message_callback, filter_params=None):
        retry_delay = 1
        max_retry_delay = 60
        while True:
            try:
                headers = {"X-API-KEY": self.api_key}
                async with websockets.connect(self.ws_url, extra_headers=headers, ping_interval=30) as ws:
                    print("📡 WebSocket conectado a Cielo (modo multi-wallet)")
                    self.connection_failures = 0
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
            except (websockets.ConnectionClosed, OSError) as e:
                self.connection_failures += 1
                retry_delay = min(retry_delay * 2, max_retry_delay)
                logger.warning(f"Conexión cerrada, reintentando en {retry_delay}s: {e}")
                await asyncio.sleep(retry_delay)
            except Exception as e:
                self.connection_failures += 1
                retry_delay = min(retry_delay * 2, max_retry_delay)
                logger.error(f"Error inesperado ({self.connection_failures}): {e}", exc_info=True)
                await asyncio.sleep(retry_delay)

    async def _ping_periodically(self, ws):
        while True:
            try:
                await asyncio.sleep(300)
                await ws.send(json.dumps({"type": "ping"}))
                print("📤 Ping enviado a Cielo WebSocket")
            except Exception as e:
                logger.error(f"Error enviando ping: {e}")
                break
