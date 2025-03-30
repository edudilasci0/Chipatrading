import asyncio
import time
import logging
import json
from typing import Dict, List, Any, Optional
from enum import Enum
from config import Config
import threading
import db

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
    
    def __init__(self, signal_logic=None, wallet_tracker=None, scoring_system=None, wallet_manager=None):
        """
        Inicializa el gestor de transacciones.
        
        Args:
            signal_logic: Módulo de lógica de señales.
            wallet_tracker: Tracker de wallets (legacy).
            scoring_system: Sistema de scoring.
            wallet_manager: Gestor de wallets (nuevo).
        """
        self.signal_logic = signal_logic
        self.wallet_tracker = wallet_tracker
        self.scoring_system = scoring_system
        self.wallet_manager = wallet_manager
        
        self.cielo_adapter = None
        self.helius_adapter = None
        
        self.active_source = DataSource.NONE
        self.preferred_source = DataSource.CIELO
        self.source_health = {
            DataSource.CIELO: {"healthy": False, "last_check": 0, "failures": 0, "last_message": 0},
            DataSource.HELIUS: {"healthy": False, "last_check": 0, "failures": 0, "last_message": 0}
        }
        
        self.health_check_interval = int(Config.get("SOURCE_HEALTH_CHECK_INTERVAL", 60))
        self.max_failures = int(Config.get("MAX_SOURCE_FAILURES", 3))
        self.source_timeout = int(Config.get("SOURCE_TIMEOUT", 300))
        self.helius_polling_interval = int(Config.get("HELIUS_POLLING_INTERVAL", 15))
        
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
        if self.running:
            logger.warning("TransactionManager ya está en ejecución")
            return
        self.running = True
        logger.info("Iniciando TransactionManager...")
        
        if not self.cielo_adapter and not self.helius_adapter:
            logger.error("No hay adaptadores de fuentes configurados")
            self.running = False
            return
        
        if self.cielo_adapter and hasattr(self.cielo_adapter, 'set_message_callback'):
            self.cielo_adapter.set_message_callback(self.handle_cielo_message)
            logger.info("Adaptador Cielo configurado")
        
        if self.helius_adapter and hasattr(self.helius_adapter, 'set_transaction_callback'):
            self.helius_adapter.set_transaction_callback(self.handle_helius_transaction)
            logger.info("Adaptador Helius configurado")
        
        self.health_check_task = asyncio.create_task(self.run_health_checks())
        
        wallets_to_track = self._get_wallets_to_track()
        if not wallets_to_track:
            logger.error("No hay wallets para monitorear")
            self.running = False
            await self.stop()
            return
        
        logger.info(f"Monitoreando {len(wallets_to_track)} wallets")
        
        if self.cielo_adapter:
            try:
                if hasattr(self.cielo_adapter, 'connect'):
                    self.tasks.append(asyncio.create_task(self.cielo_adapter.connect(wallets_to_track)))
                    logger.info("Iniciada conexión a Cielo")
                else:
                    self.tasks.append(asyncio.create_task(
                        self.cielo_adapter.run_forever_wallets(wallets=wallets_to_track, on_message_callback=self.handle_cielo_message)
                    ))
                    logger.info("Iniciada conexión a Cielo (modo legacy)")
                self.active_source = DataSource.CIELO
                self.source_health[DataSource.CIELO]["healthy"] = True
                self.source_health[DataSource.CIELO]["last_check"] = time.time()
            except Exception as e:
                logger.error(f"Error conectando a Cielo: {e}")
                await self._try_switch_to_source(DataSource.HELIUS)
        else:
            await self._try_switch_to_source(DataSource.HELIUS)
        
        logger.info(f"TransactionManager iniciado con fuente activa: {self.active_source.value}")
    
    async def stop(self):
        if not self.running:
            return
        self.running = False
        logger.info("Deteniendo TransactionManager...")
        if self.health_check_task:
            self.health_check_task.cancel()
            try:
                await self.health_check_task
            except asyncio.CancelledError:
                pass
        if self.helius_polling_task:
            self.helius_polling_task.cancel()
            try:
                await self.helius_polling_task
            except asyncio.CancelledError:
                pass
        if self.cielo_adapter and hasattr(self.cielo_adapter, 'disconnect'):
            try:
                await self.cielo_adapter.disconnect()
                logger.info("Conexión a Cielo cerrada")
            except Exception as e:
                logger.error(f"Error cerrando conexión a Cielo: {e}")
        if self.helius_adapter and hasattr(self.helius_adapter, 'stop_polling'):
            try:
                await self.helius_adapter.stop_polling()
                logger.info("Conexión a Helius cerrada")
            except Exception as e:
                logger.error(f"Error cerrando conexión a Helius: {e}")
        for task in self.tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self.tasks = []
        logger.info("TransactionManager detenido")
    
    async def run_health_checks(self):
        try:
            while self.running:
                await self._check_sources_health()
                await asyncio.sleep(self.health_check_interval)
        except asyncio.CancelledError:
            logger.info("Tarea de verificación de salud cancelada")
        except Exception as e:
            logger.error(f"Error en verificación de salud: {e}")
    
    async def _check_sources_health(self):
        now = time.time()
        active_health = self.source_health[self.active_source]
        active_timeout = now - active_health["last_message"] > self.source_timeout
        if self.active_source != DataSource.NONE and active_timeout:
            logger.warning(f"Fuente activa {self.active_source.value} sin mensajes por {self.source_timeout}s")
            active_health["healthy"] = False
            active_health["failures"] += 1
        if (self.active_source != DataSource.NONE and 
            (not active_health["healthy"] or active_health["failures"] >= self.max_failures)):
            target_source = DataSource.HELIUS if self.active_source == DataSource.CIELO else DataSource.CIELO
            logger.warning(f"Fuente {self.active_source.value} no saludable. Intentando conmutar a {target_source.value}")
            await self._try_switch_to_source(target_source)
        if self.cielo_adapter and self.active_source != DataSource.CIELO:
            cielo_health = self.source_health[DataSource.CIELO]
            if now - cielo_health["last_check"] > self.health_check_interval * 2:
                try:
                    is_available = False
                    if hasattr(self.cielo_adapter, 'check_availability'):
                        is_available = await self.cielo_adapter.check_availability()
                    else:
                        is_available = True
                    cielo_health["healthy"] = is_available
                    if is_available:
                        cielo_health["failures"] = 0
                        if self.preferred_source == DataSource.CIELO and self.active_source != DataSource.CIELO:
                            logger.info("Cielo disponible nuevamente. Conmutando de vuelta.")
                            await self._try_switch_to_source(DataSource.CIELO)
                    else:
                        cielo_health["healthy"] = False
                        cielo_health["failures"] += 1
                except Exception as e:
                    logger.warning(f"Error verificando disponibilidad de Cielo: {e}")
                    cielo_health["healthy"] = False
                    cielo_health["failures"] += 1
                cielo_health["last_check"] = now
        if self.helius_adapter and self.active_source != DataSource.HELIUS:
            helius_health = self.source_health[DataSource.HELIUS]
            if now - helius_health["last_check"] > self.health_check_interval * 2:
                try:
                    is_available = False
                    if hasattr(self.helius_adapter, 'check_availability'):
                        is_available = await self.helius_adapter.check_availability()
                    else:
                        sample_wallet = self._get_sample_wallet()
                        if sample_wallet:
                            token_data = await self.helius_adapter.get_token_data_async("So11111111111111111111111111111111111111112")
                            is_available = token_data is not None
                        else:
                            is_available = True
                    helius_health["healthy"] = is_available
                    if is_available:
                        helius_health["failures"] = 0
                        if self.active_source == DataSource.NONE and not self.source_health[DataSource.CIELO]["healthy"]:
                            logger.info("Helius disponible. Conmutando.")
                            await self._try_switch_to_source(DataSource.HELIUS)
                    else:
                        helius_health["healthy"] = False
                        helius_health["failures"] += 1
                except Exception as e:
                    logger.warning(f"Error verificando disponibilidad de Helius: {e}")
                    helius_health["healthy"] = False
                    helius_health["failures"] += 1
                helius_health["last_check"] = now
    
    async def _try_switch_to_source(self, target_source):
        if target_source == self.active_source:
            return
        logger.info(f"Intentando conmutar de {self.active_source.value} a {target_source.value}")
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
                if self.helius_polling_task:
                    self.helius_polling_task.cancel()
                    try:
                        await self.helius_polling_task
                    except asyncio.CancelledError:
                        pass
                    self.helius_polling_task = None
        wallets_to_track = self._get_wallets_to_track()
        if target_source == DataSource.CIELO and self.cielo_adapter:
            try:
                if hasattr(self.cielo_adapter, 'connect'):
                    self.tasks.append(asyncio.create_task(self.cielo_adapter.connect(wallets_to_track)))
                    logger.info("Iniciada conexión a Cielo")
                else:
                    self.tasks.append(asyncio.create_task(
                        self.cielo_adapter.run_forever_wallets(wallets=wallets_to_track, on_message_callback=self.handle_cielo_message)
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
                if hasattr(self.helius_adapter, 'start_polling'):
                    self.helius_polling_task = asyncio.create_task(
                        self.helius_adapter.start_polling(wallets_to_track, interval=self.helius_polling_interval)
                    )
                    logger.info(f"Iniciado polling de Helius cada {self.helius_polling_interval}s")
                else:
                    self.helius_polling_task = asyncio.create_task(self._run_helius_polling(wallets_to_track))
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
        Procesa mensajes recibidos desde el adaptador de Cielo.
        
        Args:
            message: Mensaje en formato string (generalmente JSON)
        """
        try:
            self.source_health[DataSource.CIELO]["last_message"] = time.time()
            if isinstance(message, str):
                try:
                    data = json.loads(message)
                except json.JSONDecodeError:
                    logger.warning(f"Mensaje inválido de Cielo: {message[:100]}")
                    return
            else:
                data = message
            if "type" not in data or data["type"] != "transaction":
                return
            if "data" not in data:
                return
            tx_data = data["data"]
            if "token" not in tx_data or "amountUsd" not in tx_data:
                logger.debug("Transacción sin datos de token o monto ignorada")
                return
            normalized_tx = {
                "wallet": tx_data.get("wallet", ""),
                "token": tx_data.get("token", ""),
                "type": tx_data.get("txType", "").upper(),
                "amount_usd": float(tx_data.get("amountUsd", 0)),
                "timestamp": time.time(),
                "source": "cielo"
            }
            await self.process_transaction(normalized_tx)
        except Exception as e:
            logger.error(f"Error en handle_cielo_message: {e}")

    async def handle_helius_transaction(self, tx_data):
        """
        Procesa transacciones recibidas desde el adaptador de Helius.
        
        Args:
            tx_data: Datos de la transacción ya estructurados.
        """
        try:
            self.source_health[DataSource.HELIUS]["last_message"] = time.time()
            if not isinstance(tx_data, dict) or "wallet" not in tx_data or "token" not in tx_data:
                logger.debug("Datos de transacción Helius incompletos")
                return
            if "type" not in tx_data or "amount_usd" not in tx_data:
                logger.debug("Transacción de Helius sin tipo o monto")
                return
            if "source" not in tx_data:
                tx_data["source"] = "helius"
            await self.process_transaction(tx_data)
        except Exception as e:
            logger.error(f"Error en handle_helius_transaction: {e}")

    async def process_transaction(self, tx_data):
        """
        Procesa una transacción normalizada y la distribuye a los componentes apropiados.
        
        Args:
            tx_data: Datos normalizados de la transacción.
        """
        try:
            min_usd = float(Config.get("MIN_TRANSACTION_USD", 200))
            if tx_data.get("amount_usd", 0) < min_usd:
                return
            if self.is_duplicate_transaction(tx_data):
                return
            try:
                db.save_transaction(tx_data)
            except Exception as e:
                logger.error(f"Error guardando transacción en BD: {e}")
            if self.scoring_system:
                self.scoring_system.update_score_on_trade(tx_data["wallet"], tx_data)
            if self.wallet_manager:
                self.wallet_manager.register_transaction(
                    tx_data["wallet"],
                    tx_data["token"],
                    tx_data["type"],
                    tx_data["amount_usd"]
                )
            if self.signal_logic:
                self.signal_logic.process_transaction(tx_data)
            logger.debug(f"Transacción procesada: {tx_data['wallet']} - {tx_data['token']} - ${tx_data['amount_usd']:.2f}")
        except Exception as e:
            logger.error(f"Error en process_transaction: {e}")

    def is_duplicate_transaction(self, tx_data):
        """
        Verifica si una transacción ya ha sido procesada recientemente.
        
        Args:
            tx_data: Datos de la transacción.
            
        Returns:
            bool: True si la transacción es duplicada.
        """
        try:
            tx_id = f"{tx_data['wallet']}:{tx_data['token']}:{tx_data['type']}:{tx_data['amount_usd']}"
            now = time.time()
            if now - self.cache_cleanup_time > 3600:
                with self.processed_tx_lock:
                    self.processed_tx_cache = {k: v for k, v in self.processed_tx_cache.items() if now - v < self.cache_ttl}
                    self.cache_cleanup_time = now
            with self.processed_tx_lock:
                if tx_id in self.processed_tx_cache:
                    if now - self.processed_tx_cache[tx_id] < 300:
                        return True
                self.processed_tx_cache[tx_id] = now
            return False
        except Exception as e:
            logger.error(f"Error verificando duplicado: {e}")
            return False

    async def _run_helius_polling(self, wallets):
        """
        Implementa polling periódico a Helius cuando no está disponible el método nativo.
        
        Args:
            wallets: Lista de direcciones de wallets a monitorear.
        """
        while self.running and self.active_source == DataSource.HELIUS:
            try:
                logger.debug(f"Iniciando polling interno de Helius para {len(wallets)} wallets")
                sample_size = min(20, len(wallets))
                sample_wallets = wallets[:sample_size]
                for wallet in sample_wallets:
                    await self._poll_wallet_transactions(wallet)
                await asyncio.sleep(self.helius_polling_interval)
            except asyncio.CancelledError:
                logger.info("Polling interno de Helius cancelado")
                break
            except Exception as e:
                logger.error(f"Error en polling interno de Helius: {e}")
                await asyncio.sleep(self.helius_polling_interval * 2)

    async def _poll_wallet_transactions(self, wallet):
        """
        Consulta transacciones recientes para una wallet mediante Helius.
        
        Args:
            wallet: Dirección de la wallet.
        """
        try:
            last_tx_time = 0
            with self.processed_tx_lock:
                if wallet in self.processed_tx_cache:
                    last_tx_time = self.processed_tx_cache[wallet]
            if hasattr(self.helius_adapter, 'get_wallet_transactions'):
                transactions = await self.helius_adapter.get_wallet_transactions(wallet, limit=5)
            else:
                transactions = await self._get_wallet_recent_transactions(wallet, limit=5)
            if not transactions:
                return
            for tx in transactions:
                tx_time = tx.get("timestamp", 0)
                if tx_time > last_tx_time:
                    if "wallet" not in tx:
                        tx["wallet"] = wallet
                    if "source" not in tx:
                        tx["source"] = "helius_polling"
                    await self.process_transaction(tx)
                    with self.processed_tx_lock:
                        self.processed_tx_cache[wallet] = max(last_tx_time, tx_time)
        except Exception as e:
            logger.error(f"Error consultando transacciones para {wallet}: {e}")

    async def _get_wallet_recent_transactions(self, wallet, limit=5):
        """
        Obtiene transacciones recientes de una wallet desde Helius.
        Método de fallback en caso de que el adaptador no disponga de get_wallet_transactions.
        
        Args:
            wallet: Dirección de la wallet.
            limit: Número máximo de transacciones a obtener.
            
        Returns:
            list: Lista de transacciones normalizadas.
        """
        if not self.helius_adapter:
            return []
        try:
            db_txs = db.get_wallet_recent_transactions(wallet, hours=1)
            if db_txs and len(db_txs) > 0:
                return db_txs
            if hasattr(self.helius_adapter, 'get_wallet_transactions'):
                raw_txs = await self.helius_adapter.get_wallet_transactions(wallet, limit)
            else:
                logger.warning("Método get_wallet_transactions no disponible en Helius adapter")
                return []
            normalized_txs = []
            for tx in raw_txs:
                normalized_tx = {
                    "wallet": wallet,
                    "token": tx.get("token", ""),
                    "type": tx.get("type", "UNKNOWN").upper(),
                    "amount_usd": float(tx.get("amount_usd", 0)),
                    "timestamp": tx.get("timestamp", time.time()),
                    "source": "helius_api"
                }
                normalized_txs.append(normalized_tx)
            return normalized_txs
        except Exception as e:
            logger.error(f"Error obteniendo transacciones de {wallet} desde Helius: {e}")
            return []

    def _get_wallets_to_track(self):
        """
        Obtiene la lista de wallets a monitorear, priorizando el nuevo WalletManager
        sobre el WalletTracker legacy.
        
        Returns:
            list: Lista de direcciones de wallets para monitorear.
        """
        wallets_to_track = []
        if self.wallet_manager and hasattr(self.wallet_manager, 'get_wallets'):
            wallets_to_track = self.wallet_manager.get_wallets()
            if wallets_to_track:
                logger.info(f"Obtenidas {len(wallets_to_track)} wallets desde WalletManager")
                return wallets_to_track
        if self.wallet_tracker and hasattr(self.wallet_tracker, 'get_wallets'):
            wallets_to_track = self.wallet_tracker.get_wallets()
            if wallets_to_track:
                logger.info(f"Obtenidas {len(wallets_to_track)} wallets desde WalletTracker")
                return wallets_to_track
        try:
            import json
            with open('traders_data.json', 'r') as f:
                data = json.load(f)
                wallets_to_track = [entry["Wallet"] for entry in data if "Wallet" in entry]
                logger.info(f"Obtenidas {len(wallets_to_track)} wallets directamente desde traders_data.json")
        except Exception as e:
            logger.error(f"Error leyendo wallets desde traders_data.json: {e}")
        return wallets_to_track

    def _get_sample_wallet(self):
        """
        Obtiene una wallet de muestra para hacer pruebas con las APIs.
        
        Returns:
            str: Dirección de wallet o None si no hay ninguna.
        """
        wallets = self._get_wallets_to_track()
        if wallets and len(wallets) > 0:
            return wallets[0]
        return None

    def get_stats(self) -> Dict[str, Any]:
        return {
            "active_source": self.active_source.value,
            "preferred_source": self.preferred_source.value,
            "sources_health": {
                "cielo": {
                    "healthy": self.source_health[DataSource.CIELO]["healthy"],
                    "failures": self.source_health[DataSource.CIELO]["failures"],
                    "last_message_ago": time.time() - self.source_health[DataSource.CIELO]["last_message"] if self.source_health[DataSource.CIELO]["last_message"] > 0 else -1
                },
                "helius": {
                    "healthy": self.source_health[DataSource.HELIUS]["healthy"],
                    "failures": self.source_health[DataSource.HELIUS]["failures"],
                    "last_message_ago": time.time() - self.source_health[DataSource.HELIUS]["last_message"] if self.source_health[DataSource.HELIUS]["last_message"] > 0 else -1
                }
            },
            "wallets_count": len(self._get_wallets_to_track()),
            "cache_size": len(self.processed_tx_cache),
            "running": self.running
        }
