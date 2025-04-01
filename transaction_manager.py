import asyncio
import time
import logging
import json
import os
from datetime import datetime
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
        self.health_check_interval = int(Config.get("SOURCE_HEALTH_CHECK_INTERVAL", "60"))
        self.max_failures = int(Config.get("MAX_SOURCE_FAILURES", "3"))
        self.source_timeout = int(Config.get("SOURCE_TIMEOUT", "300"))
        self.running = False
        self.tasks = []
        self.health_check_task = None

        self.processed_tx_cache = {}
        self.cache_cleanup_time = 0
        self.cache_ttl = 3600
        self.processed_tx_lock = asyncio.Lock()
        
        # Contadores y estadísticas
        self.tx_counts = {
            "total": 0,            # Total recibidas desde inicio
            "processed": 0,        # Procesadas correctamente
            "filtered_out": 0,     # Filtradas por criterios
            "duplicates": 0,       # Duplicadas detectadas
            "errors": 0,           # Errores en procesamiento
            "last_minute": 0,      # Contador últimos 60s
            "last_minute_timestamp": time.time(),
            "by_type": {},         # Contador por tipo
            "by_source": {}        # Contador por fuente
        }
        
        # Diagnóstico
        self._diagnostic_mode = False
        self._diagnostic_samples = []
        self._max_diagnostic_samples = 10
        
        logger.info("TransactionManager inicializado")

    async def start(self):
        """Inicia el TransactionManager y establece conexiones"""
        if self.running:
            logger.warning("TransactionManager ya está en ejecución")
            return
            
        self.running = True
        logger.info("Iniciando TransactionManager...")

        if not self.cielo_adapter:
            logger.error("Adaptador Cielo no configurado")
            self.running = False
            return

        # Configurar callback en Cielo
        if hasattr(self.cielo_adapter, 'set_message_callback'):
            self.cielo_adapter.set_message_callback(self.handle_cielo_message)
            logger.info("Adaptador Cielo configurado con callback")
        else:
            logger.warning("Adaptador Cielo no soporta callback directo")
        
        # Iniciar tarea de verificación de salud
        self.health_check_task = asyncio.create_task(self.run_health_checks())
        logger.info("Tarea de verificación de salud iniciada")

        # Obtener wallets para monitorear
        wallets_to_track = self._get_wallets_to_track()
        if not wallets_to_track:
            logger.error("No hay wallets para monitorear")
            self.running = False
            return

        logger.info(f"Monitoreando {len(wallets_to_track)} wallets")
        
        # Intentar conectar a Cielo
        try:
            # Primero intentar método connect si está disponible
            if hasattr(self.cielo_adapter, 'connect'):
                connected = await self.cielo_adapter.connect(wallets_to_track)
                if connected:
                    logger.info("Conexión a Cielo establecida correctamente")
                else:
                    logger.warning("No se pudo establecer conexión a Cielo, intentando método alternativo")
                    # Método alternativo (legacy)
                    callback = lambda message: asyncio.ensure_future(self.handle_cielo_message(message))
                    self.tasks.append(asyncio.create_task(
                        self.cielo_adapter.run_forever_wallets(
                            wallets=wallets_to_track,
                            on_message_callback=callback
                        )
                    ))
                    logger.info("Tarea legacy de Cielo iniciada")
            else:
                # Método legacy directamente
                callback = lambda message: asyncio.ensure_future(self.handle_cielo_message(message))
                self.tasks.append(asyncio.create_task(
                    self.cielo_adapter.run_forever_wallets(
                        wallets=wallets_to_track,
                        on_message_callback=callback
                    )
                ))
                logger.info("Iniciada conexión a Cielo (modo legacy)")
                
            # Actualizar estado
            self.active_source = DataSource.CIELO
            self.source_health[DataSource.CIELO]["healthy"] = True
            self.source_health[DataSource.CIELO]["last_check"] = time.time()
            
        except Exception as e:
            logger.error(f"Error conectando a Cielo: {e}", exc_info=True)
            
        # Iniciar modo de diagnóstico si es necesario
        logger.info(f"TransactionManager iniciado con fuente activa: {self.active_source}")
        if self._diagnostic_mode:
            logger.info("🔍 Modo diagnóstico activado")

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
        """
        Procesa mensajes recibidos de Cielo.
        
        Args:
            message: Mensaje recibido (string JSON o diccionario)
        """
        try:
            # Actualizar estado de salud
            self.source_health[DataSource.CIELO]["last_message"] = time.time()
            self.source_health[DataSource.CIELO]["healthy"] = True
            
            # Inicializar contadores si no existen
            if not hasattr(self, "rx_counter"):
                self.rx_counter = 0
            self.rx_counter += 1
            
            # Log inicial para confirmar entrada
            logger.debug(f"[MSG #{self.rx_counter}] Recibido: {message[:50]}...")
            
            # Guardar muestra para diagnóstico si está habilitado
            if self._diagnostic_mode and len(self._diagnostic_samples) < self._max_diagnostic_samples:
                self._diagnostic_samples.append({
                    "timestamp": time.time(),
                    "message": message if isinstance(message, str) else json.dumps(message)
                })
            
            # Convertir de string a JSON si es necesario
            if isinstance(message, str):
                try:
                    data = json.loads(message)
                except json.JSONDecodeError as e:
                    logger.warning(f"Mensaje inválido de Cielo (no es JSON): {message[:100]} - Error: {e}")
                    return
            else:
                data = message
            
            # Para mensajes de ping, simplemente actualizar el estado
            if isinstance(data, dict) and data.get("type") == "pong":
                logger.debug(f"Recibido pong de Cielo (ID: {data.get('id', 'desconocido')})")
                return
                
            # Verificar si el mensaje es una transacción
            if not isinstance(data, dict) or data.get("type") != "transaction":
                logger.debug(f"Mensaje ignorado - tipo: {data.get('type', 'desconocido')}")
                return
                
            # Validar estructura del mensaje
            if "data" not in data:
                logger.debug("Mensaje sin datos de transacción")
                return
                
            tx_data = data["data"]
            
            # Validar campos requeridos
            if "token" not in tx_data or "amountUsd" not in tx_data:
                logger.debug(f"Transacción sin token o monto ignorada: {tx_data}")
                return
                
            # Normalizar datos de transacción
            try:
                normalized_tx = {
                    "wallet": tx_data.get("wallet", ""),
                    "token": tx_data.get("token", ""),
                    "type": tx_data.get("txType", "").upper(),
                    "amount_usd": float(tx_data.get("amountUsd", 0)),
                    "timestamp": time.time(),
                    "source": "cielo"
                }
                
                # Actualizar contador por tipo
                tx_type = normalized_tx["type"]
                if tx_type not in self.tx_counts["by_type"]:
                    self.tx_counts["by_type"][tx_type] = 0
                self.tx_counts["by_type"][tx_type] += 1
                
                # Actualizar contador por fuente
                source = normalized_tx["source"]
                if source not in self.tx_counts["by_source"]:
                    self.tx_counts["by_source"][source] = 0
                self.tx_counts["by_source"][source] += 1
                
                # Actualizar contador total
                self.tx_counts["total"] += 1
                
                # Actualizar contador por minuto
                now = time.time()
                if now - self.tx_counts["last_minute_timestamp"] > 60:
                    self.tx_counts["last_minute"] = 1
                    self.tx_counts["last_minute_timestamp"] = now
                else:
                    self.tx_counts["last_minute"] += 1
                
                # Procesar la transacción
                await self.process_transaction(normalized_tx)
                
            except Exception as e:
                logger.error(f"Error normalizando datos de transacción: {e}", exc_info=True)
                self.tx_counts["errors"] += 1
                
        except Exception as e:
            logger.error(f"Error en handle_cielo_message: {e}", exc_info=True)
            self.tx_counts["errors"] += 1

    async def process_transaction(self, tx_data):
        """
        Procesa una transacción normalizada.
        
        Args:
            tx_data: Datos normalizados de la transacción
        """
        try:
            # Log detallado de entrada
            logger.debug(f"Procesando tx: {tx_data}")
            
            # Verificar monto mínimo
            min_usd = float(Config.get("MIN_TRANSACTION_USD", "200"))
            if tx_data.get("amount_usd", 0) < min_usd:
                logger.debug(f"Transacción ignorada: monto ${tx_data.get('amount_usd', 0):.2f} < ${min_usd}")
                self.tx_counts["filtered_out"] += 1
                return
                
            # Verificar si es duplicada
            is_duplicate = await self.is_duplicate_transaction(tx_data)
            if is_duplicate:
                logger.debug(f"Transacción duplicada ignorada: {tx_data['wallet']} - {tx_data['token']}")
                self.tx_counts["duplicates"] += 1
                return
                
            # Guardar en la BD
            try:
                db.save_transaction(tx_data)
                logger.info(f"Transacción guardada en BD: {tx_data['wallet']} {tx_data['type']} {tx_data['token']} ${tx_data['amount_usd']:.2f}")
            except Exception as e:
                logger.error(f"Error guardando transacción en BD: {e}", exc_info=True)
                
            # Procesar con signal_logic
            if self.signal_logic:
                try:
                    self.signal_logic.process_transaction(tx_data)
                except Exception as e:
                    logger.error(f"Error en signal_logic.process_transaction: {e}", exc_info=True)
                    
            # Actualizar scoring
            if self.scoring_system:
                try:
                    self.scoring_system.update_score_on_trade(tx_data["wallet"], tx_data)
                except Exception as e:
                    logger.error(f"Error en scoring_system.update_score_on_trade: {e}", exc_info=True)
                    
            # Registrar en wallet_manager
            if self.wallet_manager:
                try:
                    self.wallet_manager.register_transaction(
                        tx_data["wallet"],
                        tx_data["token"],
                        tx_data["type"],
                        tx_data["amount_usd"]
                    )
                except Exception as e:
                    logger.error(f"Error en wallet_manager.register_transaction: {e}", exc_info=True)
            
            # Actualizar contadores
            self.tx_counts["processed"] += 1
                
        except Exception as e:
            logger.error(f"Error en process_transaction: {e}", exc_info=True)
            self.tx_counts["errors"] += 1

    async def is_duplicate_transaction(self, tx_data):
        """
        Verifica si una transacción ya ha sido procesada para evitar duplicados.
        
        Args:
            tx_data: Datos de la transacción
            
        Returns:
            bool: True si la transacción es un duplicado
        """
        now = time.time()
        
        # Limpiar caché periódicamente
        if now - self.cache_cleanup_time > 300:  # Cada 5 minutos
            async with self.processed_tx_lock:
                keys_to_remove = [key for key, t in self.processed_tx_cache.items() if now - t > self.cache_ttl]
                for key in keys_to_remove:
                    del self.processed_tx_cache[key]
                self.cache_cleanup_time = now
                logger.debug(f"Limpieza de caché: eliminadas {len(keys_to_remove)} entradas")
        
        # Generar clave única para la transacción
        wallet = tx_data.get("wallet", "")
        token = tx_data.get("token", "")
        amount = str(tx_data.get("amount_usd", 0))
        tx_type = tx_data.get("type", "")
        cache_key = f"{wallet}:{token}:{amount}:{tx_type}"
        
        # Verificar si ya existe en la caché
        async with self.processed_tx_lock:
            if cache_key in self.processed_tx_cache:
                return True
            else:
                self.processed_tx_cache[cache_key] = now
                return False

    def _get_wallets_to_track(self):
        """
        Obtiene la lista de wallets a monitorear desde diferentes fuentes.
        
        Returns:
            list: Lista de direcciones de wallets
        """
        wallets_to_track = []
        
        # Intentar desde wallet_manager (prioridad)
        if self.wallet_manager and hasattr(self.wallet_manager, 'get_wallets'):
            wallets_to_track = self.wallet_manager.get_wallets()
            if wallets_to_track:
                logger.info(f"Obtenidas {len(wallets_to_track)} wallets desde WalletManager")
                return wallets_to_track
        
        # Intentar desde wallet_tracker (segunda opción)
        if self.wallet_tracker and hasattr(self.wallet_tracker, 'get_wallets'):
            wallets_to_track = self.wallet_tracker.get_wallets()
            if wallets_to_track:
                logger.info(f"Obtenidas {len(wallets_to_track)} wallets desde WalletTracker")
                return wallets_to_track
        
        # Si no se encontraron wallets, intentar directamente desde el archivo JSON
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
        """Ejecuta verificaciones periódicas del estado de las fuentes de datos"""
        try:
            while self.running:
                await asyncio.sleep(self.health_check_interval)
                if not self.running:
                    break
                await self._check_sources_health()
        except asyncio.CancelledError:
            logger.info("Tarea de verificación de salud cancelada")
        except Exception as e:
            logger.error(f"Error en verificación de salud: {e}", exc_info=True)

    async def _check_sources_health(self):
        """Verifica el estado de las fuentes y actúa en consecuencia"""
        now = time.time()
        active_health = self.source_health[self.active_source]
        
        # Verificar tiempo desde último mensaje
        if now - active_health["last_message"] > self.source_timeout:
            logger.warning(f"Fuente activa {self.active_source} sin mensajes por {self.source_timeout}s")
            active_health["healthy"] = False
            active_health["failures"] += 1
            
            # Intentar enviar ping para verificar conexión
            if self.cielo_adapter and hasattr(self.cielo_adapter, 'ws') and self.cielo_adapter.ws:
                try:
                    await self.cielo_adapter.ws.send(json.dumps({"type": "ping", "id": str(int(now))}))
                    logger.info("Ping enviado a Cielo para verificar conexión")
                except Exception as e:
                    logger.error(f"Error enviando ping a Cielo: {e}")
                    
        # Si la fuente no está saludable o ha fallado demasiadas veces, intentar reconectar
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
                    logger.error(f"Error reconectando a Cielo: {e}", exc_info=True)
            
        active_health["last_check"] = now

    async def _try_switch_to_source(self, target_source):
        """
        Intenta cambiar a una fuente alternativa.
        
        Args:
            target_source: Fuente a la que cambiar
            
        Returns:
            bool: True si el cambio fue exitoso
        """
        # En esta versión, usamos solo Cielo
        logger.info(f"Conmutación solicitada a {target_source} (no implementada, usando Cielo)")
        return True

    async def diagnose_connectivity(self):
        """
        Ejecuta diagnóstico completo de conectividad.
        
        Returns:
            dict: Resultados del diagnóstico
        """
        logger.info("Iniciando diagnóstico de conectividad...")
        results = {
            "timestamp": datetime.now().isoformat(),
            "active_source": self.active_source,
            "connected": False,
            "ping_success": False,
            "seconds_since_last_message": 0,
            "wallets_count": 0,
            "failures": 0,
            "transaction_counts": dict(self.tx_counts)
        }
        
        # 1. Verificar estado de conexión con Cielo
        if not self.cielo_adapter:
            logger.error("No hay adaptador Cielo configurado")
            results["error"] = "No hay adaptador Cielo configurado"
            return results
            
        connected = self.cielo_adapter.is_connected() if hasattr(self.cielo_adapter, 'is_connected') else False
        results["connected"] = connected
        logger.info(f"Estado de conexión Cielo: {'Conectado' if connected else 'Desconectado'}")
        
        # 2. Verificar tiempo desde último mensaje
        last_msg_time = self.source_health[DataSource.CIELO]["last_message"]
        seconds_since_last = time.time() - last_msg_time
        results["seconds_since_last_message"] = seconds_since_last
        logger.info(f"Tiempo desde último mensaje: {seconds_since_last:.1f} segundos")
        
        # 3. Verificar fallos
        failures = self.source_health[DataSource.CIELO]["failures"]
        results["failures"] = failures
        logger.info(f"Fallos acumulados: {failures}")
        
        # 4. Intentar enviar ping para verificar conexión
        if connected and hasattr(self.cielo_adapter, 'ws') and self.cielo_adapter.ws:
            try:
                await self.cielo_adapter.ws.send(json.dumps({"type": "ping", "id": "diagnostic"}))
                logger.info("Ping enviado a Cielo")
                results["ping_success"] = True
            except Exception as e:
                logger.error(f"Error enviando ping: {e}")
                results["ping_error"] = str(e)
        
        # 5. Verificar wallets suscritas
        wallets = self._get_wallets_to_track()
        results["wallets_count"] = len(wallets)
        logger.info(f"Número de wallets en seguimiento: {len(wallets)}")
        
        # 6. Guardar diagnóstico en archivo
        try:
            diagnostic_dir = "diagnostics"
            os.makedirs(diagnostic_dir, exist_ok=True)
            
            filename = f"{diagnostic_dir}/connectivity_diagnostic_{int(time.time())}.json"
            with open(filename, "w") as f:
                json.dump(results, f, indent=2)
            
            # Si hay muestras de diagnóstico, guardarlas también
            if self._diagnostic_samples:
                samples_filename = f"{diagnostic_dir}/message_samples_{int(time.time())}.json"
                with open(samples_filename, "w") as f:
                    json.dump(self._diagnostic_samples, f, indent=2)
                    
            logger.info(f"Diagnóstico guardado en {filename}")
            results["diagnostic_file"] = filename
        except Exception as e:
            logger.error(f"Error guardando diagnóstico: {e}")
            results["save_error"] = str(e)
        
        return results
        
    async def start_test_mode(self, interval=30, sample_size=2):
        """
        Genera transacciones de prueba para verificar el funcionamiento.
        
        Args:
            interval: Intervalo entre transacciones (segundos)
            sample_size: Número de wallets a usar para pruebas
        """
        logger.info("🧪 Iniciando modo de prueba - generando transacciones simuladas")
        
        wallets = self._get_wallets_to_track()
        if not wallets or len(wallets) < sample_size:
            logger.error("No hay suficientes wallets para el modo de prueba")
            return
            
        # Seleccionar algunas wallets aleatoriamente
        import random
        sample_wallets = random.sample(wallets, sample_size)
        test_tokens = [
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
            "So11111111111111111111111111111111111111112",   # wSOL
            "7ABz8qEFZTHPkovMDsmQkm64DZWN5wRtU7LEtD2ShkQ6"  # Un ejemplo de los datos
        ]
        
        test_counter = 0
        while self.running:
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
            enable: True para activar, False para desactivar
            max_samples: Número máximo de muestras a recolectar
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
            dict: Informe de estado
        """
        now = time.time()
        
        # Calcular tiempo desde último mensaje
        last_message_time = self.source_health[self.active_source]["last_message"]
        time_since_last = now - last_message_time
        
        return {
            "timestamp": datetime.now().isoformat(),
            "active_source": self.active_source,
            "is_running": self.running,
            "is_connected": self.cielo_adapter.is_connected() if hasattr(self.cielo_adapter, 'is_connected') else None,
            "health": {
                "cielo": self.source_health[DataSource.CIELO],
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
