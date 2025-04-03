import asyncio
import websockets
import json
import time
import logging
from datetime import datetime
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
        
        # Sistema mejorado de diagnóstico y muestreo
        self.message_samples = []
        self.max_samples = 5  # Guardar solo los primeros 5 mensajes para análisis
        self.message_counter = 0
        self.transaction_counter = 0
        self.last_transactions = []  # Guarda las últimas 10 transacciones para diagnóstico
        self.max_transactions = 10
        
        # Seguimiento de suscripciones: Cambiado de booleano a conjuntos
        self.subscription_requests = set()  # Wallets para las que se envió solicitud
        self.subscription_confirmed = set()  # Wallets confirmadas
        self.subscription_failed = set()  # Wallets fallidas
        
        # Estado de salud y estadísticas
        self.source_health = {"healthy": False, "last_check": 0, "failures": 0, "last_message": time.time()}
        
        # Contadores para diagnóstico
        self.tx_counts = {
            "total": 0,            # Total recibidas desde inicio
            "processed": 0,        # Procesadas correctamente
            "filtered_out": 0,     # Filtradas por criterios
            "errors": 0,           # Errores en procesamiento
            "by_type": {},         # Contador por tipo de mensaje
        }
        
        # Para diagnósticos avanzados
        self._diagnostic_mode = False
        self._diagnostic_samples = []
        self._max_diagnostic_samples = 10

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
                try:
                    await ws.send(json.dumps(msg))
                    self.subscription_requests.add(wallet)  # Registrar solicitud
                    logger.debug(f"Suscripción enviada para wallet: {wallet}")
                except Exception as e:
                    logger.error(f"Error enviando suscripción para wallet {wallet}: {e}")
                    self.subscription_failed.add(wallet)
                await asyncio.sleep(0.02)
            if i + chunk_size < len(wallets):
                logger.info(f"Progreso: {min(i+chunk_size, len(wallets))}/{len(wallets)} wallets suscritas...")
                await asyncio.sleep(0.5)
        
        # Enviar ping para confirmar suscripción
        try:
            await ws.send(json.dumps({"type": "ping", "id": "post-subscription"}))
            logger.info(f"Enviadas solicitudes de suscripción para {len(self.subscription_requests)} wallets")
        except Exception as e:
            logger.error(f"Error enviando ping post-suscripción: {e}")
        
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
            
            self.source_health["healthy"] = True
            self.source_health["last_check"] = time.time()
            
            return True
        except Exception as e:
            logger.error(f"Error en connect: {e}", exc_info=True)
            self.connection_failures += 1
            self.source_health["healthy"] = False
            self.source_health["failures"] += 1
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

    async def _process_cielo_message(self, message):
        """
        Procesa mensajes de Cielo e identifica transacciones.
        
        Args:
            message: Mensaje recibido (string JSON o diccionario)
            
        Returns:
            bool: True si se procesó una transacción correctamente
        """
        try:
            # Convertir de string a JSON si es necesario
            if isinstance(message, str):
                try:
                    data = json.loads(message)
                except json.JSONDecodeError as e:
                    logger.warning(f"Mensaje inválido de Cielo (no es JSON): {str(message)[:100]} - Error: {e}")
                    return False
            else:
                data = message
            
            # Verificar si el mensaje es una transacción
            msg_type = data.get("type", "unknown")
            
            # Actualizar contadores por tipo de mensaje
            self.tx_counts["by_type"].setdefault(msg_type, 0)
            self.tx_counts["by_type"][msg_type] += 1
            
            # Si el mensaje es un pong, solo actualizar el estado
            if msg_type == "pong":
                logger.debug(f"Recibido pong de Cielo (ID: {data.get('id', 'desconocido')})")
                return False
                
            # Verificar si el mensaje es una transacción
            if msg_type != "transaction":
                return False
                
            if "data" not in data:
                logger.debug("Mensaje de transacción sin datos")
                return False
                
            tx_data = data["data"]
            
            # Validaciones específicas para transacciones
            if "token" not in tx_data or "amountUsd" not in tx_data:
                logger.debug(f"Transacción sin token o monto ignorada: {tx_data}")
                return False
                
            # Registrar transacción para diagnóstico
            self.transaction_counter += 1
            logger.info(f"Transacción #{self.transaction_counter} detectada: {tx_data.get('txType', 'unknown')} para token {tx_data.get('token', 'unknown')}")
            
            # Guardar las últimas transacciones para diagnóstico
            transaction_summary = {
                "wallet": tx_data.get("wallet", ""),
                "token": tx_data.get("token", ""),
                "type": tx_data.get("txType", ""),
                "amount": tx_data.get("amountUsd", 0),
                "timestamp": time.time()
            }
            self.last_transactions.append(transaction_summary)
            if len(self.last_transactions) > self.max_transactions:
                self.last_transactions = self.last_transactions[-self.max_transactions:]
            
            self.tx_counts["total"] += 1
            return True
        except Exception as e:
            logger.error(f"Error procesando mensaje de Cielo: {e}", exc_info=True)
            self.tx_counts["errors"] += 1
            return False

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
                self.source_health["last_message"] = time.time()
                self.message_counter += 1
                
                # Diagnóstico para los primeros mensajes
                if len(self.message_samples) < self.max_samples:
                    self.message_samples.append(message)
                    logger.info(f"Muestra #{len(self.message_samples)} guardada")
                    if len(self.message_samples) == self.max_samples:
                        logger.info("===== INICIO DE MUESTRAS DE MENSAJES =====")
                        for i, sample in enumerate(self.message_samples):
                            logger.info(f"MUESTRA #{i+1}: {sample[:500]}...")
                        logger.info("===== FIN DE MUESTRAS DE MENSAJES =====")
                
                # Procesar el mensaje y verificar si es una transacción
                is_transaction = await self._process_cielo_message(message)
                
                # Verificar suscripciones
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
                    except Exception as e:
                        logger.debug(f"Error procesando confirmación de suscripción: {e}")
                
                # Enviar al callback si está configurado
                if self.message_callback:
                    try:
                        logger.debug(f"Llamando callback para mensaje #{self.message_counter}")
                        await self.message_callback(message)
                        logger.debug(f"Callback completado para mensaje #{self.message_counter}")
                        if is_transaction:
                            self.tx_counts["processed"] += 1
                    except Exception as e:
                        logger.error(f"Error en callback de mensaje: {e}", exc_info=True)
                        self.tx_counts["errors"] += 1
            except websockets.ConnectionClosed as e:
                logger.warning(f"Conexión a Cielo cerrada: {e}")
                break
            except Exception as e:
                logger.error(f"Error recibiendo mensaje: {e}", exc_info=True)
        if self.is_running:
            logger.info("Bucle de escucha finalizado pero bot activo. Preparando para reconexión...")
            self.ws = None
            self.source_health["healthy"] = False

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
                    self.source_health["healthy"] = True
                    self.source_health["last_check"] = time.time()
                    retry_delay = 1
                    await self.subscribe_to_wallets(ws, wallets, filter_params)
                    await asyncio.sleep(5)
                    self.check_subscription_status()
                    check_task = asyncio.create_task(self._periodic_subscription_check())
                    ping_task = asyncio.create_task(self._ping_periodically(ws))
                    try:
                        async for message in ws:
                            self.last_message_time = time.time()
                            self.source_health["last_message"] = time.time()
                            self.message_counter += 1
                            
                            # Diagnóstico
                            if len(self.message_samples) < self.max_samples:
                                self.message_samples.append(message)
                                logger.info(f"Muestra #{len(self.message_samples)} guardada")
                                if len(self.message_samples) == self.max_samples:
                                    logger.info("===== INICIO DE MUESTRAS DE MENSAJES =====")
                                    for i, sample in enumerate(self.message_samples):
                                        logger.info(f"MUESTRA #{i+1}: {sample[:500]}...")
                                    logger.info("===== FIN DE MUESTRAS DE MENSAJES =====")
                            
                            # Procesar mensaje
                            is_transaction = await self._process_cielo_message(message)
                            
                            # Enviar al callback
                            try:
                                await on_message_callback(message)
                                if is_transaction:
                                    self.tx_counts["processed"] += 1
                            except Exception as e:
                                logger.error(f"Error procesando mensaje: {e}", exc_info=True)
                                self.tx_counts["errors"] += 1
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
                self.source_health["healthy"] = False
                self.source_health["failures"] += 1
                logger.warning(f"Conexión cerrada (intento #{self.connection_failures}), reintentando en {retry_delay}s: {e}")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, max_retry_delay)
            except Exception as e:
                self.connection_failures += 1
                self.source_health["healthy"] = False
                self.source_health["failures"] += 1
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

    async def simulate_transaction(self):
        """
        Simula una transacción para verificar el flujo de procesamiento.
        
        Returns:
            bool: True si la transacción simulada fue procesada correctamente
        """
        logger.info("🧪 Generando transacción de prueba")
        sample_tx = {
            "type": "transaction",
            "data": {
                "wallet": "DfMxre4cKmvogbLrPigxmibVTTQDuzjdXojWzjCXXhzj",  # Una de tus wallets
                "token": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
                "txType": "BUY",
                "amountUsd": 500.0
            }
        }
        
        # Procesar directamente como si fuera un mensaje recibido
        if self.message_callback:
            logger.info("Enviando transacción simulada al callback")
            try:
                await self.message_callback(json.dumps(sample_tx))
                logger.info("✅ Transacción de prueba procesada correctamente")
                return True
            except Exception as e:
                logger.error(f"❌ Error procesando transacción de prueba: {e}", exc_info=True)
                return False
        else:
            logger.warning("❌ No hay callback configurado para procesar la transacción de prueba")
            return False

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
    
    def get_diagnostic_data(self):
        """
        Obtiene datos de diagnóstico completos.
        
        Returns:
            dict: Datos de diagnóstico
        """
        return {
            "connection": {
                "is_connected": self.is_connected(),
                "failures": self.connection_failures,
                "last_message_time": self.last_message_time,
                "seconds_since_last": time.time() - self.last_message_time
            },
            "transactions": {
                "total": self.tx_counts["total"],
                "processed": self.tx_counts["processed"],
                "errors": self.tx_counts["errors"],
                "by_type": self.tx_counts["by_type"],
                "last_transactions": self.last_transactions
            },
            "subscriptions": {
                "requested": len(self.subscription_requests),
                "confirmed": len(self.subscription_confirmed),
                "failed": len(self.subscription_failed),
                "pending": len(self.subscription_requests) - len(self.subscription_confirmed) - len(self.subscription_failed)
            },
            "health": {
                "healthy": self.source_health["healthy"],
                "failures": self.source_health["failures"],
                "last_check": self.source_health["last_check"]
            }
        }
    
    async def run_diagnostics(self):
        """
        Ejecuta diagnósticos completos de la conexión.
        
        Returns:
            dict: Resultados del diagnóstico
        """
        logger.info("🔍 Iniciando diagnósticos completos de Cielo...")
        results = self.get_diagnostic_data()
        
        # Verificar si podemos enviar un ping
        if self.is_connected() and self.ws:
            try:
                await self.ws.send(json.dumps({"type": "ping", "id": "diagnostic"}))
                results["ping_sent"] = True
                logger.info("✅ Ping diagnóstico enviado correctamente")
            except Exception as e:
                results["ping_sent"] = False
                results["ping_error"] = str(e)
                logger.error(f"❌ Error enviando ping diagnóstico: {e}")
        
        # Intentar suscribirse a una wallet de prueba
        if self.is_connected() and self.ws:
            try:
                test_wallet = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
                msg = {
                    "type": "subscribe_wallet",
                    "wallet": test_wallet,
                    "filter": {
                        "chains": ["solana"],
                        "tx_types": ["swap", "transfer"],
                        "min_amount_usd": 50
                    }
                }
                await self.ws.send(json.dumps(msg))
                results["test_subscription_sent"] = True
                logger.info("✅ Suscripción de prueba enviada correctamente")
            except Exception as e:
                results["test_subscription_sent"] = False
                results["test_subscription_error"] = str(e)
                logger.error(f"❌ Error enviando suscripción de prueba: {e}")
        
        # Probar transacción simulada
        try:
            simulation_result = await self.simulate_transaction()
            results["transaction_simulation"] = simulation_result
        except Exception as e:
            results["transaction_simulation"] = False
            results["simulation_error"] = str(e)
            logger.error(f"❌ Error en simulación de transacción: {e}")
        
        logger.info(f"🔍 Diagnósticos completados: {json.dumps(results, default=str)}")
        return results
        
    def get_status_report(self):
        """
        Genera un informe completo del estado del cliente Cielo.
        
        Returns:
            dict: Informe de estado.
        """
        now = time.time()
        time_since_last = now - self.last_message_time
        return {
            "timestamp": datetime.now().isoformat(),
            "is_running": self.is_running,
            "is_connected": self.is_connected(),
            "health": {
                "healthy": self.source_health["healthy"],
                "failures": self.source_health["failures"],
                "seconds_since_last_message": time_since_last
            },
            "transactions": {
                "total": self.tx_counts["total"],
                "processed": self.tx_counts["processed"],
                "errors": self.tx_counts["errors"],
                "by_type": self.tx_counts["by_type"]
            },
            "subscriptions": {
                "requested": len(self.subscription_requests),
                "confirmed": len(self.subscription_confirmed),
                "pending": len(self.subscription_requests) - len(self.subscription_confirmed)
            }
        }
