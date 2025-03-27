# transaction_manager.py
import asyncio
import time
import logging
import json
from typing import Dict, List, Set, Any, Optional, Callable, Tuple
from enum import Enum
from datetime import datetime, timedelta
import threading
from config import Config

logger = logging.getLogger("transaction_manager")

class DataSource(Enum):
    """Enum para las fuentes de datos disponibles"""
    CIELO = "cielo"
    HELIUS = "helius"
    NONE = "none"

class TransactionManager:
    """
    Componente central para la gestión de fuentes de datos redundantes.
    Permite conmutar automáticamente entre Cielo y Helius cuando sea necesario.
    """
    
    def __init__(self, signal_logic=None, wallet_tracker=None, 
                 scoring_system=None, scalper_monitor=None,
                 wallet_manager=None):
        """
        Inicializa el gestor de transacciones.
        
        Args:
            signal_logic: Módulo de lógica de señales
            wallet_tracker: Tracker de wallets (legacy)
            scoring_system: Sistema de scoring
            scalper_monitor: Monitor de actividad de scalpers
            wallet_manager: Gestor de wallets (nuevo)
        """
        # Componentes externos
        self.signal_logic = signal_logic
        self.wallet_tracker = wallet_tracker
        self.scoring_system = scoring_system
        self.scalper_monitor = scalper_monitor
        self.wallet_manager = wallet_manager
        
        # Adaptadores para fuentes de datos
        self.cielo_adapter = None
        self.helius_adapter = None
        
        # Estado de las fuentes
        self.active_source = DataSource.NONE
        self.preferred_source = DataSource.CIELO
        self.source_health = {
            DataSource.CIELO: {"healthy": False, "last_check": 0, "failures": 0, "last_message": 0},
            DataSource.HELIUS: {"healthy": False, "last_check": 0, "failures": 0, "last_message": 0}
        }
        
        # Configuración
        self.health_check_interval = int(Config.get("SOURCE_HEALTH_CHECK_INTERVAL", 60))  # 60 segundos
        self.max_failures = int(Config.get("MAX_SOURCE_FAILURES", 3))
        self.source_timeout = int(Config.get("SOURCE_TIMEOUT", 300))  # 5 minutos sin mensajes = fuente inactiva
        self.helius_polling_interval = int(Config.get("HELIUS_POLLING_INTERVAL", 15))  # 15 segundos
        
        # Control de tareas
        self.running = False
        self.tasks = []
        self.health_check_task = None
        self.helius_polling_task = None
        
        # Cache para evitar duplicados
        self.processed_tx_cache = {}
        self.cache_cleanup_time = 0
        self.cache_ttl = 3600  # 1 hora
        self.processed_tx_lock = threading.Lock()
        
        logger.info("TransactionManager inicializado")

    async def start(self):
        """
        Inicia el gestor de transacciones y conecta a las fuentes disponibles.
        """
        if self.running:
            logger.warning("TransactionManager ya está en ejecución")
            return
        
        self.running = True
        logger.info("Iniciando TransactionManager...")
        
        # Validar configuración
        if not self.cielo_adapter and not self.helius_adapter:
            logger.error("No hay adaptadores de fuentes configurados")
            self.running = False
            return
        
        # Preparar callback de Cielo
        if self.cielo_adapter:
            # Configurar callback para mensajes de Cielo
            if hasattr(self.cielo_adapter, 'set_message_callback'):
                self.cielo_adapter.set_message_callback(self.handle_cielo_message)
            logger.info("Adaptador Cielo configurado")
        
        # Preparar callback de Helius
        if self.helius_adapter:
            # Configurar callback para transacciones de Helius
            if hasattr(self.helius_adapter, 'set_transaction_callback'):
                self.helius_adapter.set_transaction_callback(self.handle_helius_transaction)
            logger.info("Adaptador Helius configurado")
        
        # Iniciar tarea de verificación de salud
        self.health_check_task = asyncio.create_task(self.run_health_checks())
        
        # Obtener lista de wallets a monitorear
        wallets_to_track = self._get_wallets_to_track()
        if not wallets_to_track:
            logger.error("No hay wallets para monitorear")
            self.running = False
            await self.stop()
            return
        
        logger.info(f"Monitoreando {len(wallets_to_track)} wallets")
        
        # Intentar conectar a Cielo primero (fuente preferida)
        if self.cielo_adapter:
            try:
                # Nueva forma (adaptador compatible)
                if hasattr(self.cielo_adapter, 'connect'):
                    self.tasks.append(asyncio.create_task(
                        self.cielo_adapter.connect(wallets_to_track)
                    ))
                    logger.info("Iniciada conexión a Cielo")
                # Forma legacy
                else:
                    self.tasks.append(asyncio.create_task(
                        self.cielo_adapter.run_forever_wallets(
                            wallets=wallets_to_track,
                            on_message_callback=self.handle_cielo_message
                        )
                    ))
                    logger.info("Iniciada conexión a Cielo (modo legacy)")
                
                self.active_source = DataSource.CIELO
                self.source_health[DataSource.CIELO]["healthy"] = True
                self.source_health[DataSource.CIELO]["last_check"] = time.time()
            except Exception as e:
                logger.error(f"Error conectando a Cielo: {e}")
                # Intentar con Helius como fallback
                await self._try_switch_to_source(DataSource.HELIUS)
        else:
            # Si no hay adaptador de Cielo, usar Helius directamente
            await self._try_switch_to_source(DataSource.HELIUS)
        
        logger.info(f"TransactionManager iniciado con fuente activa: {self.active_source.value}")
    
    async def stop(self):
        """
        Detiene el gestor de transacciones y cierra todas las conexiones.
        """
        if not self.running:
            return
        
        self.running = False
        logger.info("Deteniendo TransactionManager...")
        
        # Cancelar tareas de verificación de salud
        if self.health_check_task:
            self.health_check_task.cancel()
            try:
                await self.health_check_task
            except asyncio.CancelledError:
                pass
        
        # Cancelar tarea de polling de Helius
        if self.helius_polling_task:
            self.helius_polling_task.cancel()
            try:
                await self.helius_polling_task
            except asyncio.CancelledError:
                pass
        
        # Desconectar Cielo
        if self.cielo_adapter:
            try:
                if hasattr(self.cielo_adapter, 'disconnect'):
                    await self.cielo_adapter.disconnect()
                elif hasattr(self.cielo_adapter, 'close_session'):
                    await self.cielo_adapter.close_session()
                logger.info("Conexión a Cielo cerrada")
            except Exception as e:
                logger.error(f"Error cerrando conexión a Cielo: {e}")
        
        # Detener Helius
        if self.helius_adapter:
            try:
                if hasattr(self.helius_adapter, 'stop_polling'):
                    await self.helius_adapter.stop_polling()
                elif hasattr(self.helius_adapter, 'close_session'):
                    await self.helius_adapter.close_session()
                logger.info("Conexión a Helius cerrada")
            except Exception as e:
                logger.error(f"Error cerrando conexión a Helius: {e}")
        
        # Cancelar todas las tareas pendientes
        for task in self.tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        self.tasks = []
        logger.info("TransactionManager detenido")

    async def run_health_checks(self):
        """
        Ejecuta verificaciones periódicas de salud para las fuentes de datos.
        """
        try:
            while self.running:
                await self._check_sources_health()
                await asyncio.sleep(self.health_check_interval)
        except asyncio.CancelledError:
            logger.info("Tarea de verificación de salud cancelada")
        except Exception as e:
            logger.error(f"Error en verificación de salud: {e}")
    
    async def _check_sources_health(self):
        """
        Verifica el estado de salud de las fuentes de datos y
        conmuta si es necesario.
        """
        now = time.time()
        
        # Verificar fuente activa
        active_health = self.source_health[self.active_source]
        active_timeout = now - active_health["last_message"] > self.source_timeout
        
        if self.active_source != DataSource.NONE and active_timeout:
            logger.warning(f"Fuente activa {self.active_source.value} sin mensajes por {self.source_timeout}s")
            active_health["healthy"] = False
            active_health["failures"] += 1
        
        # Si la fuente activa no está saludable, intentar conmutar
        if (self.active_source != DataSource.NONE and 
            (not active_health["healthy"] or active_health["failures"] >= self.max_failures)):
            # Determinar a qué fuente conmutar
            target_source = DataSource.HELIUS if self.active_source == DataSource.CIELO else DataSource.CIELO
            logger.warning(f"Fuente {self.active_source.value} no saludable. Intentando conmutar a {target_source.value}")
            await self._try_switch_to_source(target_source)
        
        # Verificar salud de Cielo si está disponible
        if self.cielo_adapter and self.active_source != DataSource.CIELO:
            cielo_health = self.source_health[DataSource.CIELO]
            
            # Solo verificar si ha pasado suficiente tiempo desde la última verificación
            if now - cielo_health["last_check"] > self.health_check_interval * 2:
                try:
                    # Verificar disponibilidad de Cielo
                    is_available = False
                    if hasattr(self.cielo_adapter, 'check_availability'):
                        is_available = await self.cielo_adapter.check_availability()
                    else:
                        # Asumir disponible si no hay método de verificación
                        is_available = True
                    
                    cielo_health["healthy"] = is_available
                    if is_available:
                        cielo_health["failures"] = 0
                        
                        # Si la fuente preferida está disponible, conmutar de vuelta
                        if (self.preferred_source == DataSource.CIELO and 
                            self.active_source != DataSource.CIELO):
                            logger.info("Cielo disponible nuevamente. Conmutando de vuelta.")
                            await self._try_switch_to_source(DataSource.CIELO)
                    
                except Exception as e:
                    logger.warning(f"Error verificando disponibilidad de Cielo: {e}")
                    cielo_health["healthy"] = False
                    cielo_health["failures"] += 1
                
                cielo_health["last_check"] = now
        
        # Verificar salud de Helius si está disponible
        if self.helius_adapter and self.active_source != DataSource.HELIUS:
            helius_health = self.source_health[DataSource.HELIUS]
            
            # Solo verificar si ha pasado suficiente tiempo desde la última verificación
            if now - helius_health["last_check"] > self.health_check_interval * 2:
                try:
                    # Verificar disponibilidad de Helius
                    is_available = False
                    if hasattr(self.helius_adapter, 'check_availability'):
                        is_available = await self.helius_adapter.check_availability()
                    else:
                        # Intentar obtener datos para verificar disponibilidad
                        sample_wallet = self._get_sample_wallet()
                        if sample_wallet:
                            token_data = await self.helius_adapter.get_token_data_async("So11111111111111111111111111111111111111112")
                            is_available = token_data is not None
                        else:
                            is_available = True  # Asumir disponible si no podemos verificar
                    
                    helius_health["healthy"] = is_available
                    if is_available:
                        helius_health["failures"] = 0
                        
                        # Solo conmutar a Helius si Cielo no está disponible y no es la fuente activa
                        if (self.active_source == DataSource.NONE and 
                            not self.source_health[DataSource.CIELO]["healthy"]):
                            logger.info("Helius disponible. Conmutando.")
                            await self._try_switch_to_source(DataSource.HELIUS)
                    
                except Exception as e:
                    logger.warning(f"Error verificando disponibilidad de Helius: {e}")
                    helius_health["healthy"] = False
                    helius_health["failures"] += 1
                
                helius_health["last_check"] = now

    async def _try_switch_to_source(self, target_source: DataSource):
        """
        Intenta conmutar a la fuente de datos especificada.
        
        Args:
            target_source: Fuente de datos objetivo
        """
        if target_source == self.active_source:
            return
        
        logger.info(f"Intentando conmutar de {self.active_source.value} a {target_source.value}")
        
        # Detener fuente actual si está activa
        if self.active_source != DataSource.NONE:
            current_source = self.active_source
            self.active_source = DataSource.NONE
            
            if current_source == DataSource.CIELO and self.cielo_adapter:
                try:
                    if hasattr(self.cielo_adapter, 'disconnect'):
                        await self.cielo_adapter.disconnect()
                    logger.info("Conexión a Cielo cerrada")
                except Exception as e:
                    logger.error(f"Error cerrando conexión a Cielo: {e}")
            
            elif current_source == DataSource.HELIUS and self.helius_adapter:
                try:
                    if hasattr(self.helius_adapter, 'stop_polling'):
                        await self.helius_adapter.stop_polling()
                    logger.info("Polling de Helius detenido")
                except Exception as e:
                    logger.error(f"Error deteniendo polling de Helius: {e}")
                
                # Cancelar tarea de polling si existe
                if self.helius_polling_task:
                    self.helius_polling_task.cancel()
                    try:
                        await self.helius_polling_task
                    except asyncio.CancelledError:
                        pass
                    self.helius_polling_task = None
        
        # Iniciar nueva fuente
        wallets_to_track = self._get_wallets_to_track()
        
        if target_source == DataSource.CIELO and self.cielo_adapter:
            try:
                # Nueva forma (adaptador compatible)
                if hasattr(self.cielo_adapter, 'connect'):
                    self.tasks.append(asyncio.create_task(
                        self.cielo_adapter.connect(wallets_to_track)
                    ))
                    logger.info("Iniciada conexión a Cielo")
                # Forma legacy
                else:
                    self.tasks.append(asyncio.create_task(
                        self.cielo_adapter.run_forever_wallets(
                            wallets=wallets_to_track,
                            on_message_callback=self.handle_cielo_message
                        )
                    ))
                    logger.info("Iniciada conexión a Cielo (modo legacy)")
                
                self.active_source = DataSource.CIELO
                self.source_health[DataSource.CIELO]["healthy"] = True
                self.source_health[DataSource.CIELO]["last_check"] = time.time()
                self.source_health[DataSource.CIELO]["failures"] = 0
            except Exception as e:
                logger.error(f"Error conectando a Cielo: {e}")
                self.source_health[DataSource.CIELO]["healthy"] = False
                self.source_health[DataSource.CIELO]["failures"] += 1
                return False
        
        elif target_source == DataSource.HELIUS and self.helius_adapter:
            try:
                # Iniciar polling de Helius
                if hasattr(self.helius_adapter, 'start_polling'):
                    self.helius_polling_task = asyncio.create_task(
                        self.helius_adapter.start_polling(
                            wallets_to_track, 
                            interval=self.helius_polling_interval
                        )
                    )
                    logger.info(f"Iniciado polling de Helius cada {self.helius_polling_interval}s")
                # Forma alternativa: implementar polling aquí
                else:
                    self.helius_polling_task = asyncio.create_task(
                        self._run_helius_polling(wallets_to_track)
                    )
                    logger.info(f"Iniciado polling interno de Helius cada {self.helius_polling_interval}s")
                
                self.active_source = DataSource.HELIUS
                self.source_health[DataSource.HELIUS]["healthy"] = True
                self.source_health[DataSource.HELIUS]["last_check"] = time.time()
                self.source_health[DataSource.HELIUS]["failures"] = 0
            except Exception as e:
                logger.error(f"Error iniciando polling de Helius: {e}")
                self.source_health[DataSource.HELIUS]["healthy"] = False
                self.source_health[DataSource.HELIUS]["failures"] += 1
                return False
        
        else:
            logger.warning(f"No se puede conmutar a {target_source.value}: adaptador no disponible")
            return False
        
        logger.info(f"Conmutación exitosa a {target_source.value}")
        return True

    async def handle_cielo_message(self, message):
        """
        Procesa un mensaje de Cielo WebSocket.
        
        Args:
            message: Mensaje de Cielo WebSocket
        """
        # Actualizar timestamp de último mensaje recibido
        self.source_health[DataSource.CIELO]["last_message"] = time.time()
        
        try:
            # Si el mensaje es string, parsearlo como JSON
            if isinstance(message, str):
                data = json.loads(message)
            else:
                data = message
            
            # Verificar si es un mensaje de heartbeat/ping
            if "type" in data and data["type"] == "pong":
                logger.debug("Recibido pong de Cielo")
                return
            
            # Extraer y normalizar datos de la transacción
            tx_data = self._extract_cielo_transaction(data)
            if not tx_data:
                return
            
            # Procesar la transacción normalizada
            await self.process_transaction(tx_data, DataSource.CIELO)
            
        except json.JSONDecodeError:
            logger.warning(f"Mensaje de Cielo no es JSON válido: {message[:100]}...")
        except Exception as e:
            logger.error(f"Error procesando mensaje de Cielo: {e}, mensaje: {message[:100]}...")
    
    async def handle_helius_transaction(self, transaction):
        """
        Procesa una transacción de Helius.
        
        Args:
            transaction: Datos de transacción de Helius
        """
        # Actualizar timestamp de último mensaje recibido
        self.source_health[DataSource.HELIUS]["last_message"] = time.time()
        
        try:
            # Extraer y normalizar datos de la transacción
            tx_data = self._extract_helius_transaction(transaction)
            if not tx_data:
                return
            
            # Procesar la transacción normalizada
            await self.process_transaction(tx_data, DataSource.HELIUS)
            
        except Exception as e:
            logger.error(f"Error procesando transacción de Helius: {e}")
    
    async def _run_helius_polling(self, wallets: List[str]):
        """
        Implementa el polling periódico para Helius.
        
        Args:
            wallets: Lista de wallets a monitorear
        """
        if not self.helius_adapter:
            logger.error("No hay adaptador de Helius configurado para polling")
            return
        
        try:
            logger.info(f"Iniciando polling de Helius para {len(wallets)} wallets")
            while self.running and self.active_source == DataSource.HELIUS:
                start_time = time.time()
                
                # Procesar wallets en chunks para evitar sobrecarga
                chunk_size = 10
                for i in range(0, len(wallets), chunk_size):
                    chunk = wallets[i:i+chunk_size]
                    await self._poll_helius_wallets(chunk)
                
                # Calcular tiempo de espera para mantener el intervalo constante
                elapsed = time.time() - start_time
                wait_time = max(0.1, self.helius_polling_interval - elapsed)
                
                await asyncio.sleep(wait_time)
        except asyncio.CancelledError:
            logger.info("Polling de Helius cancelado")
        except Exception as e:
            logger.error(f"Error en polling de Helius: {e}")
            self.source_health[DataSource.HELIUS]["healthy"] = False
            self.source_health[DataSource.HELIUS]["failures"] += 1
    
    async def _poll_helius_wallets(self, wallets: List[str]):
        """
        Consulta las transacciones recientes para un grupo de wallets.
        
        Args:
            wallets: Lista de wallets a consultar
        """
        if not wallets:
            return
        
        try:
            # Verificar si el adaptador tiene el método específico
            if hasattr(self.helius_adapter, 'fetch_recent_transactions'):
                transactions = await self.helius_adapter.fetch_recent_transactions(wallets)
                for tx in transactions:
                    await self.handle_helius_transaction(tx)
            
            # Forma alternativa: consulta directa a la API
            elif hasattr(self.helius_adapter, 'get_wallet_transactions'):
                for wallet in wallets:
                    try:
                        transactions = await self.helius_adapter.get_wallet_transactions(wallet)
                        for tx in transactions:
                            await self.handle_helius_transaction(tx)
                    except Exception as e:
                        logger.debug(f"Error obteniendo transacciones para {wallet}: {e}")
            
            else:
                logger.warning("Adaptador de Helius no tiene métodos para obtener transacciones")
        
        except Exception as e:
            logger.error(f"Error en _poll_helius_wallets: {e}")
    
    async def process_transaction(self, tx_data: Dict[str, Any], source: DataSource):
        """
        Procesa una transacción normalizada, evitando duplicados.
        
        Args:
            tx_data: Datos normalizados de la transacción
            source: Fuente de datos que originó la transacción
        """
        if not tx_data or "wallet" not in tx_data or "token" not in tx_data:
            return
        
        # Verificar si es un duplicado
        tx_hash = self._generate_tx_hash(tx_data)
        if self._is_duplicate_transaction(tx_hash):
            return
        
        # Registrar transacción en caché
        self._add_to_tx_cache(tx_hash)
        
        # Limpiar caché periódicamente
        self._cleanup_tx_cache()
        
        # Logging
        direction = "compra" if tx_data.get("type", "").upper() == "BUY" else "venta"
        logger.debug(f"Procesando {direction} de {tx_data.get('token')} por {tx_data.get('wallet')} "
                     f"(${tx_data.get('amount_usd', 0):.2f}) - {source.value}")
        
        # Actualizar puntuación de wallet si está disponible el scoring system
        if self.scoring_system:
            try:
                self.scoring_system.update_score_on_trade(tx_data["wallet"], tx_data)
            except Exception as e:
                logger.error(f"Error actualizando score: {e}")
        
        # Registrar en wallet_manager si está disponible
        if self.wallet_manager:
            try:
                self.wallet_manager.register_transaction(
                    tx_data["wallet"], 
                    tx_data["token"], 
                    tx_data["type"], 
                    tx_data.get("amount_usd", 0)
                )
            except Exception as e:
                logger.error(f"Error registrando transacción en wallet_manager: {e}")
        
        # Procesar en signal_logic
        if self.signal_logic:
            try:
                self.signal_logic.process_transaction(tx_data)
            except Exception as e:
                logger.error(f"Error procesando transacción en signal_logic: {e}")
        
        # Procesar en scalper_monitor si está disponible
        if self.scalper_monitor:
            try:
                self.scalper_monitor.process_transaction(tx_data)
            except Exception as e:
                logger.error(f"Error procesando transacción en scalper_monitor: {e}")

    def _extract_cielo_transaction(self, message: Dict) -> Optional[Dict]:
        """
        Extrae y normaliza datos de transacción desde un mensaje de Cielo.
        
        Args:
            message: Mensaje de Cielo WebSocket
            
        Returns:
            Dict: Datos normalizados de la transacción o None si no es válida
        """
        try:
            if (not message or not isinstance(message, dict) or 
                "data" not in message or 
                "actions" not in message["data"]):
                return None
            
            actions = message["data"]["actions"]
            if not actions or not isinstance(actions, list):
                return None
            
            # Solo procesar acciones de swap o transfer
            valid_actions = [a for a in actions if a.get("type") in ["swap", "transfer"]]
            if not valid_actions:
                return None
            
            # Tomar la primera acción válida
            action = valid_actions[0]
            
            wallet = message["data"].get("wallet", "")
            token = action.get("mint", "")  # Token involucrado
            tx_type = action.get("type", "").upper()
            
            # Determinar si es BUY o SELL
            # En Cielo, el type puede ser "swap" o "transfer", necesitamos inferir la dirección
            if tx_type == "SWAP":
                # En swaps, determinar dirección según IN y OUT
                if "in" in action and "out" in action:
                    # Si el token está en "in", es una compra
                    if token == action["in"].get("mint"):
                        direction = "BUY"
                    # Si el token está en "out", es una venta
                    elif token == action["out"].get("mint"):
                        direction = "SELL"
                    else:
                        return None  # Token no involucrado directamente
                else:
                    return None  # Datos incompletos
            elif tx_type == "TRANSFER":
                # En transfers, determinar dirección según source y destination
                if "source" in action and "destination" in action:
                    if wallet == action["destination"]:
                        direction = "BUY"
                    elif wallet == action["source"]:
                        direction = "SELL"
                    else:
                        return None  # Wallet no involucrada directamente
                else:
                    return None  # Datos incompletos
            else:
                return None  # Tipo no soportado
            
            # Extraer monto en USD
            amount_usd = 0
            if "usdValue" in action:
                amount_usd = float(action["usdValue"])
            
            # Validar monto mínimo
            min_usd = float(Config.get("MIN_TRANSACTION_USD", 200))
            if amount_usd < min_usd:
                return None
            
            # Construir datos normalizados
            tx_data = {
                "wallet": wallet,
                "token": token,
                "type": direction,
                "amount_usd": amount_usd,
                "timestamp": time.time(),
                "source": "cielo"
            }
            
            return tx_data
            
        except Exception as e:
            logger.error(f"Error extrayendo datos de Cielo: {e}")
            return None
    
    def _extract_helius_transaction(self, transaction: Dict) -> Optional[Dict]:
        """
        Extrae y normaliza datos de transacción desde Helius.
        
        Args:
            transaction: Datos de transacción de Helius
            
        Returns:
            Dict: Datos normalizados de la transacción o None si no es válida
        """
        try:
            if not transaction or not isinstance(transaction, dict):
                return None
            
            # Extraer datos básicos
            wallet = transaction.get("wallet", "")
            token = transaction.get("token", "")
            tx_type = transaction.get("type", "").upper()
            
            # Validar campos requeridos
            if not wallet or not token:
                return None
            
            # Normalizar tipo de transacción
            if tx_type not in ["BUY", "SELL"]:
                # Intentar inferir tipo si no está especificado
                if "direction" in transaction:
                    direction = transaction["direction"].upper()
                    if direction == "IN":
                        tx_type = "BUY"
                    elif direction == "OUT":
                        tx_type = "SELL"
                    else:
                        return None  # Dirección desconocida
                else:
                    return None  # No se puede determinar tipo
            
            # Extraer monto en USD
            amount_usd = 0
            if "amount_usd" in transaction:
                amount_usd = float(transaction["amount_usd"])
            elif "value_usd" in transaction:
                amount_usd = float(transaction["value_usd"])
            
            # Validar monto mínimo
            min_usd = float(Config.get("MIN_TRANSACTION_USD", 200))
            if amount_usd < min_usd:
                return None
            
            # Construir datos normalizados
            tx_data = {
                "wallet": wallet,
                "token": token,
                "type": tx_type,
                "amount_usd": amount_usd,
                "timestamp": transaction.get("timestamp", time.time()),
                "source": "helius"
            }
            
            return tx_data
            
        except Exception as e:
            logger.error(f"Error extrayendo datos de Helius: {e}")
            return None
    
    def _generate_tx_hash(self, tx_data: Dict) -> str:
        """
        Genera un hash único para identificar transacciones duplicadas.
        
        Args:
            tx_data: Datos de la transacción
            
        Returns:
            str: Hash único para la transacción
        """
        # Componentes clave para identificar una transacción única
        components = [
            tx_data.get("wallet", ""),
            tx_data.get("token", ""),
            tx_data.get("type", ""),
            str(tx_data.get("amount_usd", 0)),
            # Redondeamos timestamp a minutos para evitar falsos negativos
            str(int(tx_data.get("timestamp", 0) / 60))
        ]
        
        # Unir componentes para formar el hash
        return ":".join(components)
    
    def _is_duplicate_transaction(self, tx_hash: str) -> bool:
        """
        Verifica si una transacción ya fue procesada.
        
        Args:
            tx_hash: Hash de la transacción
            
        Returns:
            bool: True si es duplicada
        """
        with self.processed_tx_lock:
            return tx_hash in self.processed_tx_cache
    
    def _add_to_tx_cache(self, tx_hash: str) -> None:
        """
        Agrega una transacción a la caché de procesados.
        
        Args:
            tx_hash: Hash de la transacción
        """
        with self.processed_tx_lock:
            self.processed_tx_cache[tx_hash] = time.time()
    
    def _cleanup_tx_cache(self) -> None:
        """
        Limpia transacciones antiguas de la caché para evitar consumo excesivo de memoria.
        """
        now = time.time()
        # Limitar frecuencia de limpieza (cada 5 minutos)
        if now - self.cache_cleanup_time < 300:
            return
            
        self.cache_cleanup_time = now
        
        with self.processed_tx_lock:
            # Remover entradas más antiguas que cache_ttl
            to_remove = []
            for tx_hash, timestamp in self.processed_tx_cache.items():
                if now - timestamp > self.cache_ttl:
                    to_remove.append(tx_hash)
            
            for tx_hash in to_remove:
                del self.processed_tx_cache[tx_hash]
                
            if to_remove:
                logger.debug(f"Limpiados {len(to_remove)} hashes de transacciones de la caché")
    
    def _get_wallets_to_track(self) -> List[str]:
        """
        Obtiene la lista de wallets a monitorear, priorizando wallet_manager.
        
        Returns:
            List[str]: Lista de direcciones de wallet
        """
        # Usar wallet_manager si está disponible (nueva implementación)
        if self.wallet_manager:
            return self.wallet_manager.get_wallets()
        
        # Fallback a wallet_tracker (implementación legacy)
        if self.wallet_tracker:
            return self.wallet_tracker.get_wallets()
        
        # Sin gestor de wallets, retornar lista vacía
        logger.warning("No hay gestor de wallets configurado")
        return []
    
    def _get_sample_wallet(self) -> Optional[str]:
        """
        Obtiene una wallet de muestra para verificar disponibilidad de APIs.
        
        Returns:
            Optional[str]: Dirección de wallet o None si no hay wallets
        """
        wallets = self._get_wallets_to_track()
        if wallets:
            return wallets[0]
        return None
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Obtiene estadísticas del gestor de transacciones.
        
        Returns:
            Dict[str, Any]: Estadísticas
        """
        return {
            "active_source": self.active_source.value,
            "preferred_source": self.preferred_source.value,
            "sources_health": {
                "cielo": {
                    "healthy": self.source_health[DataSource.CIELO]["healthy"],
                    "failures": self.source_health[DataSource.CIELO]["failures"],
                    "last_message_ago": time.time() - self.source_health[DataSource.CIELO]["last_message"] 
                    if self.source_health[DataSource.CIELO]["last_message"] > 0 else -1
                },
                "helius": {
                    "healthy": self.source_health[DataSource.HELIUS]["healthy"],
                    "failures": self.source_health[DataSource.HELIUS]["failures"],
                    "last_message_ago": time.time() - self.source_health[DataSource.HELIUS]["last_message"]
                    if self.source_health[DataSource.HELIUS]["last_message"] > 0 else -1
                }
            },
            "wallets_count": len(self._get_wallets_to_track()),
            "cache_size": len(self.processed_tx_cache),
            "running": self.running
        }
