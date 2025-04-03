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
        # Seguimiento de suscripciones: Cambiado de booleano a conjuntos
        self.subscription_requests = set()  # Wallets para las que se envió solicitud
        self.subscription_confirmed = set()  # Wallets confirmadas
        self.subscription_failed = set()  # Wallets fallidas

    async def subscribe_to_wallets(self, ws, wallets, filter_params=None):
        """
        Suscribe a múltiples wallets en chunks para evitar sobrecargar la conexión.
        
        Args:
            ws: WebSocket conectado.
            wallets: Lista de direcciones a suscribir.
            filter_params: Parámetros adicionales de filtrado (opcional).
        """
        # Preparar parámetros de suscripción
        subscription_params = {
            "chains": ["solana"],
            "tx_types": ["swap", "transfer"],
            "min_amount_usd": float(Config.get("MIN_TRANSACTION_USD", "50"))
        }
        
        # Si hay parámetros específicos, combinarlos con los predeterminados
        if filter_params:
            subscription_params.update(filter_params)
            
        logger.info(f"Iniciando suscripción de {len(wallets)} wallets con parámetros: {subscription_params}")
        
        # Limpiar seguimiento de suscripciones
        self.subscription_requests.clear()
        self.subscription_confirmed.clear()
        self.subscription_failed.clear()
        
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
                self.subscription_requests.add(wallet)  # Registrar solicitud
                logger.debug(f"Suscripción enviada para wallet: {wallet}")
                await asyncio.sleep(0.02)
            if i + chunk_size < len(wallets):
                logger.info(f"Progreso: {min(i+chunk_size, len(wallets))}/{len(wallets)} wallets suscritas...")
                await asyncio.sleep(0.5)
        
        # Enviar ping para confirmar suscripción
        await ws.send(json.dumps({"type": "ping"}))
        logger.info(f"Enviadas solicitudes de suscripción para {len(self.subscription_requests)} wallets")
        
        # Esperar un tiempo razonable para recibir confirmaciones
        await asyncio.sleep(2)
        
        missing = self.subscription_requests - self.subscription_confirmed - self.subscription_failed
        if missing:
            logger.warning(f"⚠️ Hay {len(missing)} wallets sin confirmación después de la suscripción inicial. Ejemplos: {list(missing)[:5]}")
        else:
            logger.info(f"✅ Todas las {len(self.subscription_confirmed)} wallets confirmadas correctamente")

    async def connect(self, wallets):
        """
        Inicia conexión de forma independiente al polling.
        
        Args:
            wallets: Lista de direcciones de wallets a monitorear.
            
        Returns:
            bool: True si la conexión fue exitosa.
        """
        try:
            if self.ws is not None and not self.ws.closed:
                logger.info("Cerrando conexión WebSocket existente antes de reconectar")
                await self.disconnect()
                
            self.is_running = True
            self.subscription_confirmed = set()  # Reiniciar el conjunto
            headers = {"X-API-KEY": self.api_key}
            
            logger.info(f"Conectando a {self.ws_url}...")
            self.ws = await websockets.connect(
                self.ws_url, 
                extra_headers=headers, 
                ping_interval=30,
                close_timeout=10
            )
            logger.info("WebSocket conectado a Cielo")
            
            self.ping_task = asyncio.create_task(self._ping_periodically(self.ws))
            listen_task = asyncio.create_task(self._listen_messages(self.ws))
            
            await self.subscribe_to_wallets(self.ws, wallets)
            
            check_task = asyncio.create_task(self._periodic_subscription_check())
            
            return True
        except Exception as e:
            logger.error(f"Error en connect: {e}", exc_info=True)
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
        """
        Verifica la disponibilidad de Cielo con una conexión de prueba.
        
        Returns:
            bool: True si Cielo está disponible.
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
            bool: True si hay conexión activa.
        """
        return self.ws is not None and not self.ws.closed and self.is_running

    def set_message_callback(self, callback):
        """
        Configura callback externo para mensajes.
        
        Args:
            callback: Función a llamar cuando se recibe un mensaje.
        """
        self.message_callback = callback
        logger.info("Callback de mensajes configurado para Cielo")

    async def _listen_messages(self, ws):
        """
        Escucha mensajes del WebSocket y los procesa.
        
        Args:
            ws: WebSocket conectado.
        """
        logger.info("Iniciando escucha de mensajes de Cielo")
        while self.is_running and not ws.closed:
            try:
                message = await ws.recv()
                self.last_message_time = time.time()
                self.message_counter += 1
                logger.debug(f"[MSG #{self.message_counter}] RAW mensaje recibido: {message[:200]}...")
                if len(self.message_samples) < self.max_samples:
                    self.message_samples.append(message)
                    logger.info(f"Muestra #{len(self.message_samples)} guardada")
                    if len(self.message_samples) == self.max_samples:
                        logger.info("===== INICIO DE MUESTRAS DE MENSAJES =====")
                        for i, sample in enumerate(self.message_samples):
                            logger.info(f"MUESTRA #{i+1}: {sample[:500]}...")
                        logger.info("===== FIN DE MUESTRAS DE MENSAJES =====")
                if self.message_callback:
                    try:
                        logger.debug(f"Llamando callback para mensaje #{self.message_counter}")
                        await self.message_callback(message)
                        logger.debug(f"Callback completado para mensaje #{self.message_counter}")
                    except Exception as e:
                        logger.error(f"Error en callback de mensaje: {e}", exc_info=True)
            except websockets.ConnectionClosed as e:
                logger.warning(f"Conexión a Cielo cerrada: {e}")
                break
            except Exception as e:
                logger.error(f"Error recibiendo mensaje: {e}", exc_info=True)
        if self.is_running:
            logger.info("Bucle de escucha finalizado pero bot activo. Preparando para reconexión...")
            self.ws = None

    def check_subscription_status(self):
        """Verifica el estado de las suscripciones de wallets."""
        missing = self.subscription_requests - self.subscription_confirmed
        if missing:
            logger.warning(f"⚠️ {len(missing)} wallets sin confirmación de suscripción. Ejemplos: {list(missing)[:5]}")
        else:
            logger.info(f"✅ Todas las {len(self.subscription_confirmed)} wallets confirmadas")
        return {
            "total_requested": len(self.subscription_requests),
            "confirmed": len(self.subscription_confirmed),
            "pending": len(missing),
            "missing_wallets": list(missing)[:10] if missing else []
        }

    async def _periodic_subscription_check(self):
        """Verifica periódicamente el estado de las suscripciones."""
        while self.is_running:
            await asyncio.sleep(300)  # Cada 5 minutos
            if self.is_running:
                self.check_subscription_status()

    async def run_forever_wallets(self, wallets, on_message_callback, filter_params=None):
        """
        Método legacy para mantener compatibilidad.
        Mantiene una conexión WebSocket activa, reconectando si es necesario.
        
        Args:
            wallets: Lista de wallets a monitorear.
            on_message_callback: Función callback para mensajes.
            filter_params: Parámetros de filtrado (opcional).
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
                    retry_delay = 1
                    await self.subscribe_to_wallets(ws, wallets, filter_params)
                    await asyncio.sleep(5)
                    self.check_subscription_status()
                    check_task = asyncio.create_task(self._periodic_subscription_check())
                    ping_task = asyncio.create_task(self._ping_periodically(ws))
                    try:
                        async for message in ws:
                            self.last_message_time = time.time()
                            self.message_counter += 1
                            logger.debug(f"[MSG #{self.message_counter}] Recibido: {message[:150]}...")
                            if isinstance(message, str):
                                try:
                                    data = json.loads(message)
                                    if data.get("type") == "wallet_subscribed":
                                        if "data" in data and "wallet" in data["data"]:
                                            wallet = data["data"]["wallet"]
                                            self.subscription_confirmed.add(wallet)
                                            pending = len(self.subscription_requests) - len(self.subscription_confirmed)
                                            if len(self.subscription_confirmed) % 10 == 0 or len(self.subscription_confirmed) == len(self.subscription_requests):
                                                logger.info(f"Progreso: {len(self.subscription_confirmed)}/{len(self.subscription_requests)} wallets confirmadas, {pending} pendientes")
                                    elif data.get("type") != "pong":
                                        logger.info(f"MENSAJE RECIBIDO TIPO '{data.get('type')}': {message[:300]}...")
                                except:
                                    pass
                            if len(self.message_samples) < self.max_samples:
                                self.message_samples.append(message)
                                logger.info(f"Muestra #{len(self.message_samples)} guardada")
                                if len(self.message_samples) == self.max_samples:
                                    logger.info("===== INICIO DE MUESTRAS DE MENSAJES =====")
                                    for i, sample in enumerate(self.message_samples):
                                        logger.info(f"MUESTRA #{i+1}: {sample[:500]}...")
                                    logger.info("===== FIN DE MUESTRAS DE MENSAJES =====")
                            try:
                                logger.debug(f"Llamando callback para mensaje #{self.message_counter}")
                                await on_message_callback(message)
                                logger.debug(f"Callback completado para mensaje #{self.message_counter}")
                            except Exception as e:
                                logger.error(f"Error procesando mensaje: {e}", exc_info=True)
                    finally:
                        if 'check_task' in locals():
                            check_task.cancel()
                            try:
                                await check_task
                            except asyncio.CancelledError:
                                pass
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
            ws: WebSocket conectado.
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

    async def diagnose_connectivity(self):
        """
        Ejecuta diagnóstico completo de conectividad.
        
        Returns:
            dict: Resultados del diagnóstico.
        """
        logger.info("Iniciando diagnóstico de conectividad...")
        results = {
            "timestamp": datetime.now().isoformat(),
            "active_source": "cielo",
            "connected": False,
            "ping_success": False,
            "seconds_since_last_message": 0,
            "wallets_count": 0,
            "failures": 0,
            "transaction_counts": dict(self.tx_counts)
        }
        if not self.ws:
            logger.error("No hay adaptador Cielo configurado")
            results["error"] = "No hay adaptador Cielo configurado"
            return results
        connected = self.is_connected()
        results["connected"] = connected
        logger.info(f"Estado de conexión Cielo: {'Conectado' if connected else 'Desconectado'}")
        last_msg_time = self.last_message_time
        seconds_since_last = time.time() - last_msg_time
        results["seconds_since_last_message"] = seconds_since_last
        logger.info(f"Tiempo desde último mensaje: {seconds_since_last:.1f} segundos")
        failures = self.connection_failures
        results["failures"] = failures
        logger.info(f"Fallos acumulados: {failures}")
        if connected and self.ws:
            try:
                await self.ws.send(json.dumps({"type": "ping", "id": "diagnostic"}))
                logger.info("Ping enviado a Cielo")
                results["ping_success"] = True
            except Exception as e:
                logger.error(f"Error enviando ping: {e}")
                results["ping_error"] = str(e)
        wallets = self.subscription_requests  # O usar _get_wallets_to_track()
        results["wallets_count"] = len(wallets)
        logger.info(f"Número de wallets en seguimiento: {len(wallets)}")
        logger.info("===== DIAGNÓSTICO DE CONECTIVIDAD =====")
        for key, value in results.items():
            if key != "transaction_counts":
                logger.info(f"{key}: {value}")
        logger.info("Estadísticas de transacciones:")
        for key, value in results["transaction_counts"].items():
            if key not in ["by_type", "by_source"]:
                logger.info(f"  {key}: {value}")
        logger.info("===== FIN DE DIAGNÓSTICO =====")
        return results

    async def start_test_mode(self, interval=30, sample_size=2):
        """
        Genera transacciones de prueba para verificar el funcionamiento.
        
        Args:
            interval: Intervalo entre transacciones (segundos).
            sample_size: Número de wallets a usar para pruebas.
        """
        logger.info("🧪 Iniciando modo de prueba - generando transacciones simuladas")
        wallets = self._get_wallets_to_track()
        if not wallets or len(wallets) < sample_size:
            logger.error("No hay suficientes wallets para el modo de prueba")
            return
        import random
        sample_wallets = random.sample(wallets, sample_size)
        test_tokens = [
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            "So11111111111111111111111111111111111111112",
            "7ABz8qEFZTHPkovMDsmQkm64DZWN5wRtU7LEtD2ShkQ6"
        ]
        test_counter = 0
        while self.is_running:
            wallet = random.choice(sample_wallets)
            token = random.choice(test_tokens)
            amount = random.uniform(200, 1000)
            tx_type = random.choice(["BUY", "SELL"])
            test_counter += 1
            test_tx = {
                "wallet": wallet,
                "token": token,
                "type": tx_type,
                "amount_usd": amount,
                "timestamp": time.time(),
                "source": "test_mode"
            }
            logger.info(f"📊 TEST #{test_counter}: {wallet[:8]}... {tx_type} {token[:8]}... ${amount:.2f}")
            await self.process_transaction(test_tx)
            await asyncio.sleep(interval)

    def enable_diagnostic_mode(self, enable=True, max_samples=10):
        """
        Activa o desactiva el modo de diagnóstico.
        
        Args:
            enable: True para activar, False para desactivar.
            max_samples: Número máximo de muestras a recolectar.
        """
        self._diagnostic_mode = enable
        self._max_diagnostic_samples = max_samples
        self._diagnostic_samples = []
        if enable:
            logger.info(f"🔍 Modo diagnóstico activado (max {max_samples} muestras)")
        else:
            logger.info("🔍 Modo diagnóstico desactivado")
            
    def get_status_report(self):
        """
        Genera un informe completo del estado del TransactionManager.
        
        Returns:
            dict: Informe de estado.
        """
        now = time.time()
        last_message_time = self.source_health["cielo"]["last_message"]
        time_since_last = now - last_message_time
        return {
            "timestamp": datetime.now().isoformat(),
            "active_source": "cielo",
            "is_running": self.is_running,
            "is_connected": self.is_connected(),
            "health": {
                "cielo": self.source_health["cielo"],
                "seconds_since_last_message": time_since_last
            },
            "transactions": {
                "total": self.tx_counts["total"],
                "processed": self.tx_counts["processed"],
                "filtered": self.tx_counts["filtered_out"],
                "duplicates": self.tx_counts["duplicates"],
                "errors": self.tx_counts["errors"],
                "by_type": self.tx_counts["by_type"],
                "by_source": self.tx_counts["by_source"],
                "last_minute": self.tx_counts["last_minute"]
            }
        }
