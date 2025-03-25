# signal_logic.py
import time
import asyncio
import logging
import math
from config import Config
import db

# Importar módulos de análisis avanzado
from whale_detector import WhaleDetector
from market_metrics import MarketMetricsAnalyzer
from token_analyzer import TokenAnalyzer
from trader_profiler import TraderProfiler
from telegram_utils import send_enhanced_signal

logger = logging.getLogger("signal_logic")

class SignalLogic:
    def __init__(self, scoring_system=None, helius_client=None, gmgn_client=None,
                 rugcheck_api=None, ml_predictor=None, pattern_detector=None,
                 wallet_tracker=None):
        """
        Inicializa la clase con los parámetros actuales e instancia los nuevos módulos.
        """
        self.scoring_system = scoring_system
        self.helius_client = helius_client
        self.gmgn_client = gmgn_client
        self.rugcheck_api = rugcheck_api
        self.ml_predictor = ml_predictor
        self.pattern_detector = pattern_detector
        self.wallet_tracker = wallet_tracker
        self.token_candidates = {}
        self.recent_signals = []
        self.last_signal_check = time.time()
        self.watched_tokens = set()
        self.token_type_scores = {}
        self._init_token_type_scores()
        self.monitored_tokens = {}  # Para tokens que necesitan monitoreo continuo
        self.last_monitoring_time = time.time()
        
        # Instanciar módulos de análisis avanzado
        self.whale_detector = WhaleDetector(helius_client=self.helius_client)
        self.market_metrics = MarketMetricsAnalyzer(helius_client=self.helius_client)
        self.token_analyzer = TokenAnalyzer(token_data_service=self.helius_client)
        self.trader_profiler = TraderProfiler()
        
        self.performance_tracker = None  # Se asigna externamente
        
        # Iniciar tarea de monitoreo periódico
        asyncio.create_task(self.periodic_monitoring())

    def _init_token_type_scores(self):
        self.token_type_scores = {
            "meme": 1.2,
            "standard": 1.0
        }
    
    def process_transaction(self, tx_data):
        """
        Procesa una transacción entrante con verificación inmediata de umbrales.
        """
        try:
            if not tx_data or "token" not in tx_data:
                return
            token = tx_data.get("token")
            wallet = tx_data.get("wallet")
            amount_usd = float(tx_data.get("amount_usd", 0))
            tx_type = tx_data.get("type", "").upper()
            timestamp = tx_data.get("timestamp", time.time())
            
            # Ignorar token nativo
            if token in ["native", "So11111111111111111111111111111111111111112"]:
                logger.debug(f"Ignorando token nativo: {token}")
                return
                
            # Verificar monto mínimo
            min_usd = float(Config.get("MIN_TRANSACTION_USD", 200))
            if amount_usd < min_usd:
                return
                
            # Verificar calidad del trader
            wallet_score = self.scoring_system.get_score(wallet) if self.scoring_system else 5.0
            is_elite_trader = wallet_score >= 8.0
            
            # Actualizar estadísticas del token (independientemente del trader)
            self._update_token_stats(token, wallet, tx_data, wallet_score)
            
            # Guardar la transacción para análisis
            try:
                db.save_transaction({
                    "wallet": wallet,
                    "token": token,
                    "type": tx_type,
                    "amount_usd": amount_usd
                })
            except Exception as e:
                logger.error(f"Error guardando transacción en BD: {e}")
            
            # Para traders elite con compras, verificar umbrales inmediatamente
            if is_elite_trader and tx_type == "BUY":
                logger.info(f"Trader elite {wallet} (score: {wallet_score:.1f}) detectado comprando {token}")
                asyncio.create_task(self._verify_and_signal(token, wallet, tx_data, wallet_score))
                
        except Exception as e:
            logger.error(f"Error procesando transacción en SignalLogic: {e}", exc_info=True)
    
    def _update_token_stats(self, token, wallet, tx_data, wallet_score):
        """
        Actualiza las estadísticas del token independientemente de si genera señal o no.
        """
        timestamp = tx_data.get("timestamp", time.time())
        tx_type = tx_data.get("type", "").upper()
        amount_usd = float(tx_data.get("amount_usd", 0))
        
        if token not in self.token_candidates:
            self.token_candidates[token] = {
                "wallets": set(),
                "transactions": [],
                "first_seen": timestamp,
                "last_update": timestamp,
                "whale_activity": wallet_score > 8.5,
                "volume_usd": 0,
                "buy_count": 0,
                "sell_count": 0,
                "high_quality_traders": set()
            }
        
        candidate = self.token_candidates[token]
        candidate["wallets"].add(wallet)
        
        if wallet_score >= 8.0:
            candidate["high_quality_traders"].add(wallet)
            
        if wallet_score > 8.5:
            candidate["whale_activity"] = True
            
        tx_data_enhanced = tx_data.copy()
        tx_data_enhanced["wallet_score"] = wallet_score
        tx_data_enhanced["timestamp"] = timestamp
        
        if tx_type == "BUY":
            candidate["buy_count"] += 1
        elif tx_type == "SELL":
            candidate["sell_count"] += 1
            
        candidate["volume_usd"] += amount_usd
        candidate["transactions"].append(tx_data_enhanced)
        candidate["last_update"] = timestamp
            
    async def _verify_and_signal(self, token, wallet, tx_data, wallet_score):
        """
        Verifica umbrales y genera señal inmediata si cumple criterios.
        """
        try:
            # Para evitar duplicación, verificar si ya está en seguimiento
            if token in self.watched_tokens:
                logger.debug(f"Token {token} ya está en seguimiento")
                return
                
            # Obtener datos de mercado
            market_data = await self.get_token_market_data(token)
            
            # Extraer métricas clave
            market_cap = market_data.get("market_cap", 0)
            volume = market_data.get("volume", 0)
            
            # Verificar umbrales
            mcap_threshold = 100000  # $100K market cap
            volume_threshold = 200000  # $200K volumen
            
            meets_mcap = market_cap >= mcap_threshold
            meets_volume = volume >= volume_threshold
            
            # Generar señal inmediata si cumple ambos umbrales
            if meets_mcap and meets_volume:
                logger.info(f"⚡ Señal inmediata: {token} cumple umbrales - MC: ${market_cap/1000:.1f}K, Vol: ${volume/1000:.1f}K")
                await self._generate_token_signal(token, wallet, wallet_score, market_data)
            else:
                # Añadir a monitoreo para verificación periódica
                missing_criteria = []
                if not meets_mcap:
                    missing_criteria.append(f"Market Cap (${market_cap/1000:.1f}K < $100K)")
                if not meets_volume:
                    missing_criteria.append(f"Volumen (${volume/1000:.1f}K < $200K)")
                
                logger.info(f"👁️ Token {token} añadido a monitoreo - No cumple: {', '.join(missing_criteria)}")
                
                self.monitored_tokens[token] = {
                    "wallet": wallet,
                    "first_seen": tx_data.get("timestamp", time.time()),
                    "wallet_score": wallet_score,
                    "last_check": time.time(),
                    "tx_data": tx_data
                }
                
        except Exception as e:
            logger.error(f"Error verificando token {token} para señal: {e}", exc_info=True)
    
    async def periodic_monitoring(self):
        """
        Monitorea periódicamente tokens que no cumplieron los umbrales inicialmente.
        """
        monitor_interval = 60  # Verificar cada 60 segundos
        max_monitoring_time = 3600 * 4  # Monitorear por hasta 4 horas
        
        while True:
            try:
                now = time.time()
                
                # Evitar verificaciones demasiado frecuentes
                if now - self.last_monitoring_time < monitor_interval:
                    await asyncio.sleep(1)
                    continue
                    
                self.last_monitoring_time = now
                
                tokens_to_remove = []
                for token, data in self.monitored_tokens.items():
                    # Verificar si ya es hora de revisar este token
                    if now - data["last_check"] < monitor_interval:
                        continue
                        
                    # Verificar si ha pasado demasiado tiempo desde que vimos el token
                    if now - data["first_seen"] > max_monitoring_time:
                        tokens_to_remove.append(token)
                        logger.debug(f"Token {token} eliminado del monitoreo: tiempo máximo excedido")
                        continue
                    
                    # Actualizar timestamp de última verificación
                    self.monitored_tokens[token]["last_check"] = now
                    
                    # Obtener datos actuales
                    market_data = await self.get_token_market_data(token)
                    market_cap = market_data.get("market_cap", 0)
                    volume = market_data.get("volume", 0)
                    
                    mcap_threshold = 100000  # $100K
                    volume_threshold = 200000  # $200K
                    
                    meets_mcap = market_cap >= mcap_threshold
                    meets_volume = volume >= volume_threshold
                    
                    # Si ahora cumple ambos umbrales, generar señal
                    if meets_mcap and meets_volume:
                        logger.info(f"✅ Token monitoreado {token} ahora cumple umbrales - MC: ${market_cap/1000:.1f}K, Vol: ${volume/1000:.1f}K")
                        await self._generate_token_signal(
                            token, 
                            data["wallet"], 
                            data["wallet_score"], 
                            market_data
                        )
                        tokens_to_remove.append(token)
                    else:
                        # Actualizar progreso
                        missing = []
                        if not meets_mcap:
                            missing.append(f"MC: ${market_cap/1000:.1f}K/$100K")
                        if not meets_volume:
                            missing.append(f"Vol: ${volume/1000:.1f}K/$200K")
                        logger.debug(f"Token {token} continúa en monitoreo - No cumple: {', '.join(missing)}")
                
                # Eliminar tokens procesados o expirados
                for token in tokens_to_remove:
                    if token in self.monitored_tokens:
                        del self.monitored_tokens[token]
                
                # Dormir brevemente para permitir otras tareas
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"Error en monitoreo periódico: {e}", exc_info=True)
                await asyncio.sleep(5)  # Esperar un poco más si hay error
    
    async def _generate_token_signal(self, token, wallet, wallet_score, market_data):
        """
        Genera una señal para un token que ha cumplido los criterios.
        """
        try:
            if token in self.watched_tokens:
                logger.debug(f"Ignorando señal duplicada para {token}")
                return
                
            now = time.time()
            
            # Obtener análisis extendido para la señal
            extended_analysis = await self._extend_token_analysis(token, market_data)
            
            # Obtener datos del candidato si existen
            candidate = self.token_candidates.get(token, {
                "wallets": {wallet},
                "high_quality_traders": {wallet} if wallet_score >= 8.0 else set(),
                "buy_count": 1,
                "volume_usd": market_data.get("volume", 0)
            })
            
            # Preparar datos para la señal
            initial_price = market_data.get("price", 0)
            market_cap = market_data.get("market_cap", 0)
            token_name = market_data.get("name", "")
            
            # Calcular confianza basada en traders de alta calidad
            high_quality_count = len(candidate.get("high_quality_traders", {wallet}))
            confidence = min(0.95, 0.7 + (high_quality_count * 0.05))
            
            # Clasificar la señal según la confianza
            if confidence >= 0.9:
                signal_level = "S"
            elif confidence >= 0.8:
                signal_level = "A"
            elif confidence >= 0.6:
                signal_level = "B"
            else:
                signal_level = "C"
            
            # Guardar la señal en la base de datos
            trader_count = len(candidate.get("wallets", {wallet}))
            signal_id = db.save_signal(token, trader_count, confidence, initial_price)
            
            # Preparar características para el predictor ML
            features = {
                "token": token,
                "token_name": token_name,
                "trader_count": trader_count,
                "num_transactions": len(candidate.get("transactions", [])),
                "total_volume_usd": candidate.get("volume_usd", market_data.get("volume", 0)),
                "avg_volume_per_trader": candidate.get("volume_usd", market_data.get("volume", 0)) / max(1, trader_count),
                "buy_ratio": candidate.get("buy_count", 1) / max(1, candidate.get("buy_count", 1) + candidate.get("sell_count", 0)),
                "tx_velocity": self._calculate_tx_velocity(candidate.get("transactions", [])),
                "market_cap": market_cap,
                "volume_1h": market_data.get("volume", 0),
                "volume_growth_5m": market_data.get("volume_growth", {}).get("growth_5m", 0),
                "volume_growth_1h": market_data.get("volume_growth", {}).get("growth_1h", 0),
                "whale_flag": 1 if candidate.get("whale_activity", False) else 0,
                "is_meme": 1 if token.endswith('pump') else 0,
                "signal_level": signal_level,
                "known_traders": list(candidate.get("wallets", {wallet})),
                "extended_analysis": extended_analysis
            }
            
            # Guardar características para análisis ML
            db.save_signal_features(signal_id, token, features)
            
            # Iniciar seguimiento de rendimiento si está disponible
            if self.performance_tracker:
                signal_info = {
                    "confidence": confidence,
                    "traders_count": trader_count,
                    "total_volume": candidate.get("volume_usd", market_data.get("volume", 0)),
                    "signal_id": signal_id,
                    "token_name": token_name,
                    "known_traders": list(candidate.get("wallets", {wallet}))
                }
                self.performance_tracker.add_signal(token, signal_info)
                
            # Registrar la señal como procesada
            self.recent_signals.append({
                "token": token,
                "confidence": confidence,
                "timestamp": now,
                "signal_id": signal_id,
                "signal_level": signal_level
            })
            
            # Limitar tamaño de historial de señales
            if len(self.recent_signals) > 20:
                self.recent_signals = self.recent_signals[-20:]
                
            # Marcar token como en seguimiento
            self.watched_tokens.add(token)
            
            # Enviar señal a Telegram con información enriquecida
            token_type = "🔴 TOKEN PUMP" if token.endswith("pump") else ""
            tx_velocity = self._calculate_tx_velocity(candidate.get("transactions", []))
            
            # Determinar traders a mostrar
            known_traders = list(candidate.get("wallets", {wallet}))
            
            send_enhanced_signal(
                token=token,
                confidence=confidence,
                tx_velocity=tx_velocity,
                traders=known_traders,
                token_type=token_type,
                token_name=token_name,
                market_cap=market_cap,
                initial_price=initial_price,
                extended_analysis=extended_analysis,
                signal_level=signal_level
            )
            
            logger.info(f"Señal generada para {token} con confianza {confidence:.2f} (Nivel {signal_level})")
            
        except Exception as e:
            logger.error(f"Error generando señal para {token}: {e}", exc_info=True)
    
    def _calculate_tx_velocity(self, transactions):
        """
        Calcula la velocidad de transacciones (tx/min) basado en el historial.
        """
        if not transactions or len(transactions) < 2:
            return 0
            
        timestamps = [tx.get("timestamp", 0) for tx in transactions]
        min_time = min(timestamps)
        max_time = max(timestamps)
        time_span = (max_time - min_time) / 60  # Convertir a minutos
        
        if time_span <= 0:
            return len(transactions)  # Todas en el mismo segundo
            
        return len(transactions) / time_span
    
    async def _process_candidates(self):
        """
        Evalúa los tokens candidatos utilizando un análisis compuesto y
        calcula la puntuación de confianza de la señal.
        
        Nota: Este método se mantiene para compatibilidad, pero el procesamiento
        principal ahora ocurre en _verify_and_signal y periodic_monitoring.
        """
        try:
            now = time.time()
            window = float(Config.get("SIGNAL_WINDOW_SECONDS", 540))
            cutoff = now - window
            candidates = []

            logger.info(f"🔍 Inicio procesamiento de candidatos. Tokens en análisis: {len(self.token_candidates)}")
            
            for token, data in list(self.token_candidates.items()):
                recent_txs = [tx for tx in data["transactions"] if tx["timestamp"] > cutoff]
                
                if not recent_txs:
                    logger.debug(f"Token {token} ignorado: Sin transacciones recientes en la ventana de {window}s")
                    continue
                if token in self.watched_tokens:
                    logger.debug(f"Token {token} ignorado: Ya está en seguimiento")
                    continue
                
                tracked_wallets = set(self.wallet_tracker.get_wallets()) if self.wallet_tracker else set()
                traders_with_buys = {}
                for tx in recent_txs:
                    wallet = tx.get("wallet")
                    if wallet in tracked_wallets and tx.get("type") == "BUY":
                        traders_with_buys.setdefault(wallet, []).append(tx)
                
                if len(traders_with_buys) < int(Config.get("MIN_TRADERS_FOR_SIGNAL", "2")):
                    logger.info(f"Token {token} ignorado: Traders insuficientes ({len(traders_with_buys)}/{int(Config.get('MIN_TRADERS_FOR_SIGNAL', '2'))})")
                    continue
                
                trader_count = len(data["wallets"])
                volume_usd = sum(tx["amount_usd"] for tx in recent_txs)
                buy_txs = [tx for tx in recent_txs if tx.get("type") == "BUY"]
                buy_ratio = len(buy_txs) / max(1, len(recent_txs))
                timestamps = [tx["timestamp"] for tx in recent_txs]
                tx_velocity = len(recent_txs) / max(1, (max(timestamps) - min(timestamps)) / 60) if len(timestamps) >= 2 else 0
                wallet_scores = [self.scoring_system.get_score(wallet) for wallet in traders_with_buys.keys()] if self.scoring_system else [5.0]*len(traders_with_buys)
                is_pump_token = token.endswith('pump')
                token_type = "meme" if is_pump_token else "standard"
                
                # Verificar datos de mercado asíncronamente
                market_data = await self.get_token_market_data(token)
                
                # Actualizar estado de monitoreo
                extended_analysis = await self._extend_token_analysis(token, market_data)
                volume_acceleration = extended_analysis.get("token", {}).get("volume_acceleration", 0)
                holder_growth = extended_analysis.get("market", {}).get("holder_growth_rate", 0)
                
                # Calcular confianza usando método existente
                confidence = self.compute_confidence(
                    wallet_scores=wallet_scores,
                    volume_1h=volume_usd,
                    market_cap=market_data.get("market_cap", 0),
                    recent_volume_growth=market_data.get("volume_growth", {}).get("growth_5m", 0),
                    token_type=token_type,
                    whale_activity=data.get("whale_activity", False),
                    volume_acceleration=volume_acceleration,
                    holder_growth=holder_growth
                )
                
                elite_traders = sum(1 for score in wallet_scores if score > 9.0)
                if elite_traders > 0:
                    confidence = min(0.95, confidence * 1.1)
                
                logger.info(f"✅ Token {token} calificado como candidato. Confianza: {confidence:.2f}, Traders: {trader_count}, Volumen: ${volume_usd:.2f}")
                
                candidate = {
                    "token": token,
                    "confidence": confidence,
                    "trader_count": trader_count,
                    "volume_usd": volume_usd,
                    "recent_transactions": recent_txs,
                    "tx_velocity": tx_velocity,
                    "buy_ratio": buy_ratio,
                    "token_type": token_type,
                    "market_data": market_data,
                    "extended_analysis": extended_analysis,
                    "elite_trader_count": elite_traders,
                    "high_quality_count": sum(1 for score in wallet_scores if score > 7.5),
                    "known_traders": list(traders_with_buys.keys())
                }
                candidates.append(candidate)
            
            candidates.sort(key=lambda x: x["confidence"], reverse=True)
            
            # Línea corregida para evitar el error de sintaxis en f-strings anidados
            candidate_info = []
            for c in candidates[:3]:
                candidate_info.append(f"{c['token']}:{c['confidence']:.2f}")
            logger.info(f"🔄 Candidatos calificados: {len(candidates)}. Ordenados por confianza: {candidate_info}")
            
            await self._generate_signals(candidates)
        except Exception as e:
            logger.error(f"Error in _process_candidates: {e}", exc_info=True)
    
    async def _generate_signals(self, candidates):
        """
        Genera señales clasificadas en niveles S, A, B, C y envía una notificación enriquecida.
        """
        try:
            now = time.time()
            signal_throttling = int(Config.get("SIGNAL_THROTTLING", 10))
            recent_signals_count = db.count_signals_last_hour()
            
            if recent_signals_count >= signal_throttling:
                logger.info(f"⚠️ Procesamiento de señales limitado: {recent_signals_count}/{signal_throttling} en la última hora")
                return
            
            if self.recent_signals and now - self.recent_signals[-1]["timestamp"] < 180:
                logger.info(f"⏱️ Esperando tiempo mínimo entre señales (última: hace {now - self.recent_signals[-1]['timestamp']:.0f}s)")
                return
            
            min_confidence = float(Config.get("MIN_CONFIDENCE_THRESHOLD", 0.3))
            qualifying_candidates = [c for c in candidates if c["confidence"] >= min_confidence]
            
            logger.info(f"📊 Evaluando {len(qualifying_candidates)} candidatos con confianza >= {min_confidence}")
            if not qualifying_candidates:
                logger.info(f"⚠️ No hay candidatos que cumplan el umbral mínimo de confianza ({min_confidence})")
                return
            
            best_candidate = qualifying_candidates[0]
            token = best_candidate["token"]
            
            for sig in self.recent_signals:
                if sig["token"] == token and now - sig["timestamp"] < 3600:
                    logger.info(f"⏱️ Token {token} ignorado: Ya tuvo una señal hace {now - sig['timestamp']:.0f}s")
                    return
            
            confidence = best_candidate["confidence"]
            trader_count = len(best_candidate.get("known_traders", []))
            tx_velocity = best_candidate["tx_velocity"]
            buy_ratio = best_candidate["buy_ratio"]
            token_opportunity = best_candidate["token_type"]
            market_data = best_candidate.get("market_data", {})
            initial_price = market_data.get("price", 0)
            market_cap = market_data.get("market_cap", 0)
            token_name = market_data.get("name", "")
            
            # Verificar umbrales de mercado
            mcap_threshold = 100000  # $100K
            volume_threshold = 200000  # $200K
            
            if market_cap < mcap_threshold or market_data.get("volume", 0) < volume_threshold:
                logger.info(f"⚠️ Token {token} ignorado: No cumple umbrales de mercado. MC: ${market_cap/1000:.1f}K, Vol: ${market_data.get('volume', 0)/1000:.1f}K")
                
                # Añadir a monitoreo en lugar de generar señal inmediata
                high_quality_wallet = next((wallet for wallet in best_candidate.get("known_traders", []) 
                                          if self.scoring_system and self.scoring_system.get_score(wallet) >= 8.0), 
                                         best_candidate.get("known_traders", [""])[0])
                
                self.monitored_tokens[token] = {
                    "wallet": high_quality_wallet,
                    "first_seen": now,
                    "wallet_score": self.scoring_system.get_score(high_quality_wallet) if self.scoring_system else 5.0,
                    "last_check": now,
                    "tx_data": {"type": "BUY", "amount_usd": market_data.get("volume", 0), "timestamp": now}
                }
                return
            
            # Clasificar la señal según la confianza
            if confidence >= 0.9:
                signal_level = "S"
            elif confidence >= 0.8:
                signal_level = "A"
            elif confidence >= 0.6:
                signal_level = "B"
            else:
                signal_level = "C"
            
            signal_id = db.save_signal(token, trader_count, confidence, initial_price)
            
            features = {
                "token": token,
                "token_name": token_name,
                "trader_count": trader_count,
                "num_transactions": len(best_candidate["recent_transactions"]),
                "total_volume_usd": best_candidate["volume_usd"],
                "avg_volume_per_trader": best_candidate["volume_usd"] / max(1, trader_count),
                "buy_ratio": buy_ratio,
                "tx_velocity": tx_velocity,
                "market_cap": market_data.get("market_cap", 0),
                "volume_1h": market_data.get("volume", 0),
                "volume_growth_5m": market_data.get("volume_growth", {}).get("growth_5m", 0),
                "volume_growth_1h": market_data.get("volume_growth", {}).get("growth_1h", 0),
                "whale_flag": 1 if best_candidate.get("whale_activity") else 0,
                "is_meme": 1 if token_opportunity == "meme" else 0,
                "signal_level": signal_level,
                "known_traders": best_candidate.get("known_traders", []),
                "extended_analysis": best_candidate.get("extended_analysis", {})
            }
            
            db.save_signal_features(signal_id, token, features)
            
            if self.performance_tracker:
                signal_info = {
                    "confidence": confidence,
                    "traders_count": trader_count,
                    "total_volume": best_candidate["volume_usd"],
                    "signal_id": signal_id,
                    "token_name": token_name,
                    "known_traders": best_candidate.get("known_traders", [])
                }
                self.performance_tracker.add_signal(token, signal_info)
                
            self.recent_signals.append({
                "token": token,
                "confidence": confidence,
                "timestamp": now,
                "signal_id": signal_id,
                "signal_level": signal_level
            })
            if len(self.recent_signals) > 20:
                self.recent_signals = self.recent_signals[-20:]
            self.watched_tokens.add(token)
            
            # Enviar señal enriquecida a Telegram
            send_enhanced_signal(
                token=token,
                confidence=confidence,
                tx_velocity=tx_velocity,
                traders=best_candidate.get("known_traders", []),
                token_type="🔴 TOKEN PUMP" if token.endswith("pump") else
