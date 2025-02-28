import asyncio
import websockets
import json
import time
from config import Config

class CieloAPI:
    """
    Maneja la conexión WebSocket a la API de Cielo.
    """

    def __init__(self, api_key=None):
        """
        Inicializa la API de Cielo.
        
        Args:
            api_key: API key para Cielo, si no se proporciona usa el valor de Config
        """
        self.api_key = api_key if api_key else Config.CIELO_API_KEY
        self.ws_url = "wss://feed-api.cielo.finance/api/v1/ws"
        self.last_connection_attempt = 0
        self.connection_failures = 0

    async def subscribe_to_wallets(self, ws, wallets, filter_params=None):
        """
        Suscribe a múltiples wallets en la misma conexión WebSocket.
        
        Args:
            ws: WebSocket abierto
            wallets: Lista de direcciones de wallets
            filter_params: Diccionario con parámetros de filtro
        """
        if filter_params is None:
            filter_params = {}
        
        # Registrar la suscripción
        print(f"🔄 Suscribiendo a {len(wallets)} wallets...")
        
        # Suscribir wallets en bloques para no saturar la API
        chunk_size = 50  # Suscribir de 50 en 50
        for i in range(0, len(wallets), chunk_size):
            chunk = wallets[i:i+chunk_size]
            for wallet in chunk:
                msg = {
                    "type": "subscribe_wallet",
                    "wallet": wallet,
                    "filter": filter_params
                }
                await ws.send(json.dumps(msg))
            
            # Pequeña pausa entre bloques
            if i + chunk_size < len(wallets):
                print(f"  Progress: {i+chunk_size}/{len(wallets)} wallets suscritas...")
                await asyncio.sleep(0.5)
        
        print("✅ Todas las wallets han sido suscritas")

    async def run_forever_wallets(self, wallets, on_message_callback, filter_params=None):
        """
        Mantiene una conexión WebSocket abierta y se suscribe a múltiples wallets.
        Reintenta la conexión en caso de errores con backoff exponencial.
        
        Args:
            wallets: Lista de direcciones de wallets
            on_message_callback: Función a llamar con cada mensaje recibido
            filter_params: Diccionario con parámetros de filtro
        """
        retry_delay = 1  # Comenzar con 1 segundo
        max_retry_delay = 60  # Máximo 1 minuto entre reintentos
        
        while True:
            try:
                # Registrar intento de conexión
                self.last_connection_attempt = time.time()
                
                # Conectar a WebSocket
                headers = {"X-API-KEY": self.api_key}
                async with websockets.connect(self.ws_url, extra_headers=headers, ping_interval=30) as ws:
                    print("📡 WebSocket conectado a Cielo (modo multi-wallet)")
                    # Restablecer contador de fallos al conectar exitosamente
                    self.connection_failures = 0
                    retry_delay = 1
                    
                    # Suscribir todas las wallets
                    await self.subscribe_to_wallets(ws, wallets, filter_params)

                    # Procesar mensajes entrantes
                    async for message in ws:
                        await on_message_callback(message)
                        
            except (websockets.ConnectionClosed, OSError) as e:
                self.connection_failures += 1
                retry_delay = min(retry_delay * 2, max_retry_delay)  # Backoff exponencial
                
                print(f"🔴 Conexión cerrada o error de red ({self.connection_failures}): {e}")
                print(f"Reintentando en {retry_delay}s...")
                
                # Notificar a Telegram si han pasado más de 5 minutos desde último intento
                # y han habido múltiples fallos
                if self.connection_failures > 3:
                    from telegram_utils import send_telegram_message
                    send_telegram_message(
                        f"⚠️ *Alerta de Conexión*\nError de conexión a Cielo: {e}\n"
                        f"Reintentando en {retry_delay}s..."
                    )
                
                await asyncio.sleep(retry_delay)
                
            except Exception as e:
                self.connection_failures += 1
                print(f"🚨 Error inesperado ({self.connection_failures}): {e}, reintentando en {retry_delay}s...")
                await asyncio.sleep(retry_delay)
                
    async def run_forever(self, on_message_callback, filter_params=None):
        """
        Mantiene una conexión WebSocket abierta con la API de Cielo en modo feed.
        
        Args:
            on_message_callback: Función a llamar con cada mensaje recibido
            filter_params: Diccionario con parámetros de filtro
        """
        retry_delay = 1  # Comenzar con 1 segundo
        max_retry_delay = 60  # Máximo 1 minuto entre reintentos
        
        while True:
            try:
                headers = {"X-API-KEY": self.api_key}
                async with websockets.connect(self.ws_url, extra_headers=headers, ping_interval=30) as ws:
                    print("📡 WebSocket conectado a Cielo (modo feed)")
                    
                    # Restablecer contador de fallos
                    self.connection_failures = 0
                    retry_delay = 1
                    
                    # Suscribirse al feed
                    subscribe_message = {
                        "type": "subscribe_feed",
                        "filter": filter_params or {}
                    }
                    await ws.send(json.dumps(subscribe_message))
                    print(f"📡 Suscrito con filtros => {filter_params}")

                    # Procesar mensajes entrantes
                    async for message in ws:
                        await on_message_callback(message)
                        
            except (websockets.ConnectionClosed, OSError) as e:
                self.connection_failures += 1
                retry_delay = min(retry_delay * 2, max_retry_delay)  # Backoff exponencial
                
                print(f"🔴 Conexión cerrada o error de red ({self.connection_failures}): {e}")
                print(f"Reintentando en {retry_delay}s...")
                
                await asyncio.sleep(retry_delay)
                
            except Exception as e:
                self.connection_failures += 1
                print(f"🚨 Error inesperado ({self.connection_failures}): {e}, reintentando en {retry_delay}s...")
                await asyncio.sleep(retry_delay)
