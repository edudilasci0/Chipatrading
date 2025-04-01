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
        self.message_samples = []
        self.max_samples = 5  # Guardar solo los primeros 5 mensajes para análisis
        self.message_counter = 0
        self.subscription_confirmed = False

    async def subscribe_to_wallets(self, ws, wallets, filter_params=None):
        """
        Suscribe a múltiples wallets en chunks para evitar sobrecargar la conexión.
        
        Args:
            ws: WebSocket conectado
            wallets: Lista de direcciones a suscribir
            filter_params: Parámetros adicionales de filtrado (opcional)
        """
        # Preparar parámetros de suscripción
        subscription_params = {
            "chains": ["solana"],
            "tx_types": ["swap", "transfer"],
            "min_amount_usd": float(Config.get("MIN_TRANSACTION_USD", "50"))  # Reducido para aumentar volumen
        }
        
        # Si hay parámetros específicos, combinarlos con los predeterminados
        if filter_params:
            subscription_params.update(filter_params)
            
        logger.info(f"Iniciando suscripción de {len(wallets)} wallets con parámetros: {subscription_params}")
        
        # Enviar suscripciones en chunks para evitar sobrecarga
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
                await asyncio.sleep(0.02)  # Pequeña pausa entre suscripciones
                
            if i + chunk_size < len(wallets):
                progress = min(i+chunk_size, len(wallets))
                logger.info(f"Progreso: {progress}/{len(wallets)} wallets suscritas...")
                await asyncio.sleep(0.5)  # Pausa entre chunks
                
        # Enviar ping para confirmar suscripción
        await ws.send(json.dumps({"type": "ping"}))
        logger.info(f"✅ Todas las {len(wallets)} wallets han sido suscritas")
        self.subscription_confirmed = True
    
    async def connect(self, wallets):
        """
        Inicia conexión de forma independiente al polling.
        
        Args:
            wallets: Lista de direcciones de wallets a monitorear
            
        Returns:
            bool: True si la conexión fue exitosa
        """
        try:
            if self.ws is not None and not self.ws.closed:
                logger.info("Cerrando conexión WebSocket existente antes de reconectar")
                await self.disconnect()
                
            self.is_running = True
            self.subscription_confirmed = False
            headers = {"X-API-KEY": self.api_key}
            
            logger.info(f"Conectando a {self.ws_url}...")
            self.ws = await websockets.connect(
                self.ws_url, 
                extra_headers=headers, 
                ping_interval=30,
                close_timeout=10
            )
            logger.info("WebSocket conectado a Cielo")
            
            # Iniciar tareas de ping y escucha de mensajes
            self.ping_task = asyncio.create_task(self._ping_periodically(self.ws))
            listen_task = asyncio.create_task(self._listen_messages(self.ws))
            
            # Suscribir wallets
            await self.subscribe_to_wallets(self.ws, wallets)
            
            return True
        except Exception as e:
            logger.error(f"Error en connect: {e}", exc_info=True)
            self.connection_failures += 1
            return False

    async def disconnect(self):
        """Cierra conexión ordenadamente"""
        self.is_running = False
        
        # Cancelar tarea de ping si existe
        if self.ping_task:
            self.ping_task.cancel()
            try:
                await self.ping_task
            except asyncio.CancelledError:
                pass
                
        # Cerrar WebSocket si existe
        if self.ws:
            try:
                await self.ws.close()
                self.ws = None
                logger.info("WebSocket desconectado de Cielo")
            except Exception as e:
                logger.error(f"Error en disconnect: {e}")

    async def check_availability(self):
        """
        Verifica la disponibilidad de Cielo con una conexión de prueba.
        
        Returns:
            bool: True si Cielo está disponible
        """
        try:
            logger.info("Verificando disponibilidad de Cielo...")
            test_ws = await websockets.connect(
                self.ws_url, 
                extra_headers={"X-API-KEY": self.api_key},
                close_timeout=5
            )
            await test_ws.send(json.dumps({"type": "ping"}))
            response = await asyncio.wait_for(test_ws.recv(), timeout=5)
            await test_ws.close()
            
            if response:
                try:
                    data = json.loads(response)
                    if data.get("type") == "pong":
                        logger.info("✅ Cielo disponible y respondiendo")
                        return True
                except Exception:
                    pass
                    
            logger.info("✅ Cielo disponible pero respuesta inesperada")
            return True
        except Exception as e:
            logger.warning(f"❌ Cielo no disponible: {e}")
            return False

    def is_connected(self):
        """
        Verifica si hay una conexión activa.
        
        Returns:
            bool: True si hay conexión activa
        """
        return self.ws is not None and not self.ws.closed and self.is_running

    def set_message_callback(self, callback):
        """
        Configura callback externo para mensajes.
        
        Args:
            callback: Función a llamar cuando se recibe un mensaje
        """
        self.message_callback = callback
        logger.info("Callback de mensajes configurado para Cielo")

    async def _listen_messages(self, ws):
        """
        Escucha mensajes del WebSocket y los procesa.
        
        Args:
            ws: WebSocket conectado
        """
        logger.info("Iniciando escucha de mensajes de Cielo")
        while self.is_running and not ws.closed:
            try:
                message = await ws.recv()
                self.last_message_time = time.time()
                self.message_counter += 1
                
                # Log detallado para depuración
                logger.debug(f"[MSG #{self.message_counter}] RAW mensaje recibido: {message[:200]}...")
                
                # Guardar muestra para análisis (solo las primeras)
                if len(self.message_samples) < self.max_samples:
                    self.message_samples.append(message)
                    logger.info(f"Muestra #{len(self.message_samples)} guardada")
                    
                    # Si hemos recolectado suficientes muestras, guardarlas en archivo
                    if len(self.message_samples) == self.max_samples:
                        try:
                            with open("cielo_message_samples.json", "w") as f:
                                json.dump(self.message_samples, f, indent=2)
                            logger.info("Muestras de mensajes guardadas en cielo_message_samples.json")
                        except Exception as e:
                            logger.error(f"Error guardando muestras: {e}")
                
                # Procesar mensaje
                if self.message_callback:
                    try:
                        await self.message_callback(message)
                    except Exception as e:
                        logger.error(f"Error en callback de mensaje: {e}", exc_info=True)
                        
            except websockets.ConnectionClosed as e:
                logger.warning(f"Conexión a Cielo cerrada: {e}")
                break
                
            except Exception as e:
                logger.error(f"Error recibiendo mensaje: {e}", exc_info=True)
                
        # Si salimos del bucle pero el bot sigue corriendo, intentar reconectar
        if self.is_running:
            logger.info("Bucle de escucha finalizado pero bot activo. Preparando para reconexión...")
            self.ws = None

    async def run_forever_wallets(self, wallets, on_message_callback, filter_params=None):
        """
        Método legacy para mantener compatibilidad.
        Mantiene una conexión WebSocket activa, reconectando si es necesario.
        
        Args:
            wallets: Lista de wallets a monitorear
            on_message_callback: Función callback para mensajes
            filter_params: Parámetros de filtrado (opcional)
        """
        self.message_callback = on_message_callback
        retry_delay = 1
        max_retry_delay = 60
        
        while True:
            try:
                logger.info("Iniciando conexión en modo run_forever...")
                headers = {"X-API-KEY": self.api_key}
                
                async with websockets.connect(
                    self.ws_url, 
                    extra_headers=headers, 
                    ping_interval=30,
                    close_timeout=10
                ) as ws:
                    self.ws = ws
                    logger.info("📡 WebSocket conectado a Cielo (modo multi-wallet)")
                    self.connection_failures = 0
                    retry_delay = 1  # Resetear delay tras conexión exitosa
                    
                    # Suscribir wallets
                    await self.subscribe_to_wallets(ws, wallets, filter_params)
                    
                    # Iniciar tarea de ping
                    ping_task = asyncio.create_task(self._ping_periodically(ws))
                    
                    try:
                        # Bucle de recepción de mensajes
                        async for message in ws:
                            self.last_message_time = time.time()
                            self.message_counter += 1
                            
                            # Log detallado
                            logger.debug(f"[MSG #{self.message_counter}] Recibido: {message[:150]}...")
                            
                            # Guardar muestra
                            if len(self.message_samples) < self.max_samples:
                                self.message_samples.append(message)
                                logger.info(f"Muestra #{len(self.message_samples)} guardada")
                                
                                if len(self.message_samples) == self.max_samples:
                                    try:
                                        with open("cielo_message_samples.json", "w") as f:
                                            json.dump(self.message_samples, f, indent=2)
                                        logger.info("Muestras de mensajes guardadas en cielo_message_samples.json")
                                    except Exception as e:
                                        logger.error(f"Error guardando muestras: {e}")
                            
                            # Procesar mensaje
                            try:
                                await on_message_callback(message)
                            except Exception as e:
                                logger.error(f"Error procesando mensaje: {e}", exc_info=True)
                                
                    finally:
                        # Limpiar tarea de ping al salir
                        ping_task.cancel()
                        try:
                            await ping_task
                        except asyncio.CancelledError:
                            pass
                            
            except (websockets.ConnectionClosed, OSError) as e:
                self.connection_failures += 1
                logger.warning(f"Conexión cerrada (intento #{self.connection_failures}), reintentando en {retry_delay}s: {e}")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, max_retry_delay)
                
            except Exception as e:
                self.connection_failures += 1
                logger.error(f"Error inesperado ({self.connection_failures}): {e}", exc_info=True)
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, max_retry_delay)

    async def _ping_periodically(self, ws):
        """
        Envía pings periódicos para mantener la conexión activa.
        
        Args:
            ws: WebSocket conectado
        """
        ping_interval = 300  # 5 minutos
        ping_counter = 0
        
        logger.info(f"Iniciando pings periódicos cada {ping_interval} segundos")
        while True:
            try:
                await asyncio.sleep(ping_interval)
                ping_counter += 1
                
                if ws.closed:
                    logger.warning("WebSocket cerrado, cancelando pings")
                    break
                    
                await ws.send(json.dumps({"type": "ping", "id": str(ping_counter)}))
                logger.info(f"📤 Ping #{ping_counter} enviado a Cielo WebSocket")
                
            except asyncio.CancelledError:
                logger.info("Tarea de ping cancelada")
                break
                
            except Exception as e:
                logger.error(f"Error enviando ping #{ping_counter}: {e}")
                break
