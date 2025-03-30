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
        self.helius_adapter = None

        self.active_source = DataSource.NONE
        self.preferred_source = DataSource.CIELO
        self.source_health = {
            DataSource.CIELO: {"healthy": False, "last_check": 0, "failures": 0, "last_message": time.time()},
            DataSource.HELIUS: {"healthy": False, "last_check": 0, "failures": 0, "last_message": time.time()}
        }

        self.health_check_interval = int(Config.get("SOURCE_HEALTH_CHECK_INTERVAL", 60))
        self.max_failures = int(Config.get("MAX_SOURCE_FAILURES", 3))
        self.source_timeout = int(Config.get("SOURCE_TIMEOUT", 300))
        self.helius_polling_interval = int(Config.get("HELIUS_POLLING_INTERVAL", 15))
        self.running = False
        self.tasks = []
        self.health_check_task = None
        self.helius_polling_task = None

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

        if not self.cielo_adapter and not self.helius_adapter:
            logger.error("No hay adaptadores de fuentes configurados")
            self.running = False
            return

        if self.cielo_adapter:
            if hasattr(self.cielo_adapter, 'set_message_callback'):
                self.cielo_adapter.set_message_callback(self.handle_cielo_message)
            logger.info("Adaptador Cielo configurado")
        if self.helius_adapter:
            if hasattr(self.helius_adapter, 'set_transaction_callback'):
                self.helius_adapter.set_transaction_callback(self.handle_helius_transaction)
            logger.info("Adaptador Helius configurado")

        self.health_check_task = asyncio.create_task(self.run_health_checks())

        wallets_to_track = self._get_wallets_to_track()
        if not wallets_to_track:
            logger.error("No hay wallets para monitorear")
            self.running = False
            return

        logger.info(f"Monitoreando {len(wallets_to_track)} wallets")

        if self.cielo_adapter:
            try:
                if hasattr(self.cielo_adapter, 'connect'):
                    self.tasks.append(asyncio.create_task(
                        self.cielo_adapter.connect(wallets_to_track)
                    ))
                    logger.info("Iniciada conexión a Cielo")
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
                await self._try_switch_to_source(DataSource.HELIUS)
        else:
            await self._try_switch_to_source(DataSource.HELIUS)

        logger.info(f"TransactionManager iniciado con fuente activa: {self.active_source}")

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

    async def handle_helius_transaction(self, transaction):
        try:
            self.source_health[DataSource.HELIUS]["last_message"] = time.time()
            self.source_health[DataSource.HELIUS]["healthy"] = True
            if not transaction or "token" not in transaction:
                return
            if "source" not in transaction:
                transaction["source"] = "helius"
            await self.process_transaction(transaction)
        except Exception as e:
            logger.error(f"Error en handle_helius_transaction: {e}", exc_info=True)

    async def process_transaction(self, tx_data):
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

    def is_duplicate_transaction(self, tx_data):
        now = time.time()
        if now - self.cache_cleanup_time > 300:
            async def cleanup():
                async with self.processed_tx_lock:
                    keys_to_remove = [key for key, t in self.processed_tx_cache.items() if now - t > self.cache_ttl]
                    for key in keys_to_remove:
                        del self.processed_tx_cache[key]
                    self.cache_cleanup_time = now
            asyncio.create_task(cleanup())
        wallet = tx_data.get("wallet", "")
        token = tx_data.get("token", "")
        amount = str(tx_data.get("amount_usd", 0))
        tx_type = tx_data.get("type", "")
        cache_key = f"{wallet}:{token}:{amount}:{tx_type}"
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
                await asyncio.sleep(int(Config.get("SOURCE_HEALTH_CHECK_INTERVAL", 60)))
                await self._check_sources_health()
        except asyncio.CancelledError:
            logger.info("Tarea de verificación de salud cancelada")
        except Exception as e:
            logger.error(f"Error en verificación de salud: {e}")

    async def _check_sources_health(self):
        now = time.time()
        active_health = self.source_health[self.active_source]
        active_timeout = now - active_health["last_message"] > self.source_timeout
        if self.active_source != DataSource.NONE and active_timeout:
            logger.warning(f"Fuente activa {self.active_source} sin mensajes por {self.source_timeout}s")
            active_health["healthy"] = False
            active_health["failures"] += 1
        if self.active_source != DataSource.NONE and (not active_health["healthy"] or active_health["failures"] >= self.max_failures):
            target_source = DataSource.HELIUS if self.active_source == DataSource.CIELO else DataSource.CIELO
            logger.warning(f"Fuente {self.active_source} no saludable. Intentando conmutar a {target_source}")
            await self._try_switch_to_source(target_source)
        if self.cielo_adapter and self.active_source != DataSource.CIELO:
            cielo_health = self.source_health[DataSource.CIELO]
            if now - cielo_health["last_check"] > int(Config.get("SOURCE_HEALTH_CHECK_INTERVAL", 60)) * 2:
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
                        cielo_health["failures"] += 1
                except Exception as e:
                    logger.warning(f"Error verificando disponibilidad de Cielo: {e}")
                    cielo_health["healthy"] = False
                    cielo_health["failures"] += 1
                cielo_health["last_check"] = now
        if self.helius_adapter and self.active_source != DataSource.HELIUS:
            helius_health = self.source_health[DataSource.HELIUS]
            if now - helius_health["last_check"] > int(Config.get("SOURCE_HEALTH_CHECK_INTERVAL", 60)) * 2:
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
                        helius_health["failures"] += 1
                except Exception as e:
                    logger.warning(f"Error verificando disponibilidad de Helius: {e}")
                    helius_health["healthy"] = False
                    helius_health["failures"] += 1
                helius_health["last_check"] = now

    async def _try_switch_to_source(self, target_source):
        if target_source == self.active_source:
            return
        logger.info(f"Intentando conmutar de {self.active_source} a {target_source}")
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
                    self.tasks.append(asyncio.create_task(
                        self.cielo_adapter.connect(wallets_to_track)
                    ))
                    logger.info("Iniciada conexión a Cielo")
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
                if hasattr(self.helius_adapter, 'start_polling'):
                    self.helius_polling_task = asyncio.create_task(
                        self.helius_adapter.start_polling(
                            wallets_to_track, 
                            interval=self.helius_polling_interval
                        )
                    )
                    logger.info(f"Iniciado polling de Helius cada {self.helius_polling_interval}s")
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
            logger.warning(f"No se puede conmutar a {target_source}: adaptador no disponible")
            return False
        logger.info(f"Conmutación exitosa a {target_source}")
        return True

    async def _run_helius_polling(self, wallets):
        try:
            logger.info(f"Iniciando polling de Helius para {len(wallets)} wallets")
            while self.running and self.active_source == DataSource.HELIUS:
                start_time = time.time()
                chunk_size = 10
                for i in range(0, len(wallets), chunk_size):
                    chunk = wallets[i:i+chunk_size]
                    for wallet in chunk:
                        try:
                            transactions = await self.helius_adapter.get_wallet_transactions(wallet)
                            for tx in transactions:
                                if self.is_duplicate_transaction(tx):
                                    continue
                                await self.handle_helius_transaction(tx)
                        except Exception as e:
                            logger.debug(f"Error obteniendo transacciones para {wallet}: {e}")
                elapsed = time.time() - start_time
                wait_time = max(0.1, self.helius_polling_interval - elapsed)
                await asyncio.sleep(wait_time)
        except asyncio.CancelledError:
            logger.info("Polling de Helius cancelado")
        except Exception as e:
            logger.error(f"Error en _run_helius_polling: {e}")
            self.source_health[DataSource.HELIUS]["healthy"] = False
            self.source_health[DataSource.HELIUS]["failures"] += 1

    def _get_sample_wallet(self):
        wallets = self._get_wallets_to_track()
        if wallets:
            return wallets[0]
        return None
