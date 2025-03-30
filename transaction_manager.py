import asyncio
import time
import logging
import json
from config import Config
import db

logger = logging.getLogger("transaction_manager")

class DataSource:
    CIELO = "cielo"
    HELIUS = "helius"
    NONE = "none"

class TransactionManager:
    def __init__(self, signal_logic=None, wallet_tracker=None, scoring_system=None, wallet_manager=None):
        self.signal_logic = signal_logic
        self.wallet_tracker = wallet_tracker
        self.scoring_system = scoring_system
        self.wallet_manager = wallet_manager

        self.cielo_adapter = None
        self.helius_adapter = None  # No se usará

        self.active_source = DataSource.CIELO  # Usamos siempre Cielo
        self.preferred_source = DataSource.CIELO
        self.source_health = {
            DataSource.CIELO: {"healthy": False, "last_check": 0, "failures": 0, "last_message": time.time()},
            DataSource.HELIUS: {"healthy": False, "last_check": 0, "failures": 0, "last_message": time.time()}
        }

        # Valores por defecto en caso de que Config.get falle
        self.health_check_interval = int(Config.SOURCE_HEALTH_CHECK_INTERVAL) 
        self.max_failures = int(Config.MAX_SOURCE_FAILURES)
        self.source_timeout = int(Config.SOURCE_TIMEOUT)
        self.running = False
        self.tasks = []
        self.health_check_task = None

        self.processed_tx_cache = {}
        self.cache_cleanup_time = 0
        self.cache_ttl = 3600
        self.processed_tx_lock = asyncio.Lock()

        logger.info("TransactionManager inicializado")

    async def start(self):
        if self.running:
            logger.warning("TransactionManager ya está en ejecución")
            return
        self.running = True
        logger.info("Iniciando TransactionManager...")

        if not self.cielo_adapter:
            logger.error("Adaptador Cielo no configurado")
            self.running = False
            return

        if hasattr(self.cielo_adapter, 'set_message_callback'):
            self.cielo_adapter.set_message_callback(self.handle_cielo_message)
        logger.info("Adaptador Cielo configurado")
        
        self.health_check_task = asyncio.create_task(self.run_health_checks())

        wallets_to_track = self._get_wallets_to_track()
        if not wallets_to_track:
            logger.error("No hay wallets para monitorear")
            self.running = False
            return

        logger.info(f"Monitoreando {len(wallets_to_track)} wallets")
        try:
            if hasattr(self.cielo_adapter, 'connect'):
                connected = await self.cielo_adapter.connect(wallets_to_track)
                if connected:
                    logger.info("Conexión a Cielo establecida correctamente")
                else:
                    logger.warning("No se pudo establecer conexión a Cielo, intentando método alternativo")
                    callback = lambda message: asyncio.ensure_future(self.handle_cielo_message(message))
                    self.tasks.append(asyncio.create_task(
                        self.cielo_adapter.run_forever_wallets(
                            wallets=wallets_to_track,
                            on_message_callback=callback
                        )
                    ))
            else:
                callback = lambda message: asyncio.ensure_future(self.handle_cielo_message(message))
                self.tasks.append(asyncio.create_task(
                    self.cielo_adapter.run_forever_wallets(
                        wallets=wallets_to_track,
                        on_message_callback=callback
                    )
                ))
                logger.info("Iniciada conexión a Cielo (modo legacy)")
            self.active_source = DataSource.CIELO
            self.source_health[DataSource.CIELO]["healthy"] = True
            self.source_health[DataSource.CIELO]["last_check"] = time.time()
        except Exception as e:
            logger.error(f"Error conectando a Cielo: {e}")
        logger.info(f"TransactionManager iniciado con fuente activa: {self.active_source}")

    async def stop(self):
        """Detiene el TransactionManager y sus tareas asociadas"""
        if not self.running:
            return
        
        self.running = False
        logger.info("Deteniendo TransactionManager...")
        
        # Cancelar tareas
        if self.health_check_task:
            self.health_check_task.cancel()
            try:
                await self.health_check_task
            except asyncio.CancelledError:
                pass
            
        for task in self.tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        # Cerrar conexiones
        if self.cielo_adapter and hasattr(self.cielo_adapter, 'disconnect'):
            await self.cielo_adapter.disconnect()
            
        logger.info("TransactionManager detenido")

    async def handle_cielo_message(self, message):
        try:
            self.source_health[DataSource.CIELO]["last_message"] = time.time()
            self.source_health[DataSource.CIELO]["healthy"] = True
            if isinstance(message, str):
                try:
                    data = json.loads(message)
                except json.JSONDecodeError:
                    logger.warning(f"Mensaje inválido de Cielo: {message[:100]}")
                    return
            else:
                data = message
            
            # Para mensajes de ping, simplemente actualizar el estado
            if isinstance(data, dict) and data.get("type") == "pong":
                logger.debug("Recibido pong de Cielo")
                return
                
            if not isinstance(data, dict) or data.get("type") != "transaction":
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
            logger.error(f"Error en handle_cielo_message: {e}", exc_info=True)

    async def process_transaction(self, tx_data):
        try:
            min_usd = float(Config.MIN_TRANSACTION_USD)
            if tx_data.get("amount_usd", 0) < min_usd:
                return
            if await self.is_duplicate_transaction(tx_data):
                return
            try:
                db.save_transaction(tx_data)
            except Exception as e:
                logger.error(f"Error guardando transacción en BD: {e}")
            if self.signal_logic:
                self.signal_logic.process_transaction(tx_data)
            if self.scoring_system:
                self.scoring_system.update_score_on_trade(tx_data["wallet"], tx_data)
            if self.wallet_manager:
                self.wallet_manager.register_transaction(
                    tx_data["wallet"],
                    tx_data["token"],
                    tx_data["type"],
                    tx_data["amount_usd"]
                )
        except Exception as e:
            logger.error(f"Error en process_transaction: {e}", exc_info=True)

    async def is_duplicate_transaction(self, tx_data):
        now = time.time()
        if now - self.cache_cleanup_time > 300:
            async with self.processed_tx_lock:
                keys_to_remove = [key for key, t in self.processed_tx_cache.items() if now - t > self.cache_ttl]
                for key in keys_to_remove:
                    del self.processed_tx_cache[key]
                self.cache_cleanup_time = now
        wallet = tx_data.get("wallet", "")
        token = tx_data.get("token", "")
        amount = str(tx_data.get("amount_usd", 0))
        tx_type = tx_data.get("type", "")
        cache_key = f"{wallet}:{token}:{amount}:{tx_type}"
        async with self.processed_tx_lock:
            if cache_key in self.processed_tx_cache:
                return True
            else:
                self.processed_tx_cache[cache_key] = now
                return False

    def _get_wallets_to_track(self):
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
        if not wallets_to_track:
            try:
                import json
                with open('traders_data.json', 'r') as f:
                    data = json.load(f)
                    wallets_to_track = [entry["Wallet"] for entry in data if "Wallet" in entry]
                    logger.info(f"Obtenidas {len(wallets_to_track)} wallets directamente desde traders_data.json")
            except Exception as e:
                logger.error(f"Error leyendo wallets desde traders_data.json: {e}")
        return wallets_to_track

    async def run_health_checks(self):
        try:
            while self.running:
                await asyncio.sleep(self.health_check_interval)
                await self._check_sources_health()
        except asyncio.CancelledError:
            logger.info("Tarea de verificación de salud cancelada")
        except Exception as e:
            logger.error(f"Error en verificación de salud: {e}")

    async def _check_sources_health(self):
        now = time.time()
        active_health = self.source_health[self.active_source]
        if now - active_health["last_message"] > self.source_timeout:
            logger.warning(f"Fuente activa {self.active_source} sin mensajes por {self.source_timeout}s")
            active_health["healthy"] = False
            active_health["failures"] += 1
            
            # Intentar enviar ping para verificar conexión
            if self.cielo_adapter and hasattr(self.cielo_adapter, 'ws') and self.cielo_adapter.ws:
                try:
                    await self.cielo_adapter.ws.send(json.dumps({"type": "ping"}))
                    logger.info("Ping enviado a Cielo para verificar conexión")
                except Exception as e:
                    logger.error(f"Error enviando ping a Cielo: {e}")
                    
        if not active_health["healthy"] or active_health["failures"] >= self.max_failures:
            logger.warning(f"Fuente {self.active_source} no saludable, intentando reconectar")
            if self.cielo_adapter:
                try:
                    # Intentar reconectar
                    if hasattr(self.cielo_adapter, 'disconnect'):
                        await self.cielo_adapter.disconnect()
                    
                    wallets = self._get_wallets_to_track()
                    if hasattr(self.cielo_adapter, 'connect'):
                        connected = await self.cielo_adapter.connect(wallets)
                        if connected:
                            logger.info("Reconexión a Cielo exitosa")
                            active_health["healthy"] = True
                            active_health["failures"] = 0
                    else:
                        # Intentar reiniciar mediante otras tareas
                        logger.warning("Reconexión a Cielo no implementada, necesita reinicio manual")
                except Exception as e:
                    logger.error(f"Error reconectando a Cielo: {e}")
            
        active_health["last_check"] = now

    async def _try_switch_to_source(self, target_source):
        # En esta versión, usamos solo Cielo, por lo que este método puede dejarse como stub
        logger.info(f"Conmutación solicitada a {target_source} (no implementada, usando Cielo)")
        return True
