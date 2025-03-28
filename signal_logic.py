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
            
            db.save_signal_features(signal_id, token, features)
            
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
                
            self.recent_signals.append({
                "token": token,
                "confidence": confidence,
                "timestamp": now,
                "signal_id": signal_id,
                "signal_level": signal_level
            })
            
            # Limitar historial de señales
            if len(self.recent_signals) > 20:
                self.recent_signals = self.recent_signals[-20:]
            self.watched_tokens.add(token)
            
            # Enviar señal enriquecida a Telegram
            token_type = "🔴 TOKEN PUMP" if token.endswith("pump") else ""
            tx_velocity = self._calculate_tx_velocity(candidate.get("transactions", []))
            
            send_enhanced_signal(
                token=token,
                confidence=confidence,
                tx_velocity=tx_velocity,
                traders=best_candidate.get("known_traders", []) if (best_candidate := candidate) else [],
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
            return len(transactions)
            
        return len(transactions) / time_span
    
    async def _extend_token_analysis(self, token, market_data=None):
        """
        Extiende el análisis del token integrando resultados de:
         - WhaleDetector
         - MarketMetricsAnalyzer
         - TokenAnalyzer
         - TraderProfiler
        Retorna un diccionario con todos los indicadores relevantes.
        """
        analysis = {}
        try:
            recent_transactions = db.get_token_transactions(token, hours=1)
            whale_result = await self.whale_detector.detect_large_transactions(
                token, recent_transactions, market_cap=market_data.get("market_cap", 0) if market_data else 0
            )
        except Exception as e:
            logger.error(f"Error in whale analysis for {token}: {e}", exc_info=True)
            whale_result = {}
            
        try:
            market_result = await self.market_metrics.calculate_market_health(token)
        except Exception as e:
            logger.error(f"Error in market metrics for {token}: {e}", exc_info=True)
            market_result = {}
            
        try:
            token_result = await self.token_analyzer.analyze_volume_patterns(
                token, 
                current_volume=market_data.get("volume", 0) if market_data else 0,
                market_data=market_data
            )
            price_patterns = await self.token_analyzer.detect_price_patterns(token, None, market_data)
            if price_patterns:
                token_result.update(price_patterns)
        except Exception as e:
            logger.error(f"Error in token analysis for {token}: {e}", exc_info=True)
            token_result = {}
            
        try:
            active_traders = set()
            for tx in recent_transactions:
                active_traders.add(tx.get("wallet", ""))
            trader_result = {}
            for wallet in list(active_traders)[:5]:
                try:
                    profile = await self.trader_profiler.get_trader_profile(wallet)
                    if profile:
                        trader_result[wallet] = profile
                except Exception as inner_e:
                    logger.warning(f"Error getting profile for trader {wallet}: {inner_e}")
        except Exception as e:
            logger.error(f"Error in trader profiling for {token}: {e}", exc_info=True)
            trader_result = {}
        
        analysis["whale"] = whale_result
        analysis["market"] = market_result
        analysis["token"] = token_result
        analysis["trader"] = trader_result
        
        return analysis
    
    async def get_token_market_data(self, token):
        """
        Obtiene datos de mercado para el token, utilizando diversas fuentes.
        Prioriza obtener market cap y volumen precisos.
        """
        data = None
        source = "none"
        
        if token.endswith('pump') or token.endswith('ai') or token.endswith('erc') or token.endswith('inu'):
            return {
                "price": 0.00001,
                "market_cap": 500000,
                "volume": 20000,
                "volume_growth": {"growth_5m": 0.5, "growth_1h": 0.2},
                "estimated": True,
                "source": "meme_default"
            }
            
        if self.helius_client:
            try:
                data = self.helius_client.get_token_data(token)
                if data and data.get("market_cap", 0) > 0:
                    source = "helius"
                    logger.debug(f"Market data for {token} obtained from Helius")
            except Exception as e:
                logger.warning(f"Helius API error for {token}: {str(e)[:100]}")
        
        if not data or data.get("market_cap", 0) == 0:
            if self.gmgn_client:
                try:
                    gmgn_data = self.gmgn_client.get_market_data(token)
                    if gmgn_data and gmgn_data.get("market_cap", 0) > 0:
                        data = gmgn_data
                        source = "gmgn"
                        logger.debug(f"Market data for {token} obtained from GMGN")
                except Exception as e:
                    logger.warning(f"GMGN API error for {token}: {str(e)[:100]}")
        
        if not data or data.get("market_cap", 0) == 0 or data.get("volume", 0) == 0:
            if hasattr(self, 'dex_monitor') and self.dex_monitor:
                try:
                    dex_data = await self.dex_monitor.get_combined_liquidity_data(token)
                    if dex_data:
                        if not data:
                            data = {}
                        if data.get("market_cap", 0) == 0 and dex_data.get("market_cap", 0) > 0:
                            data["market_cap"] = dex_data.get("market_cap")
                        if data.get("volume", 0) == 0 and dex_data.get("volume_24h", 0) > 0:
                            data["volume"] = dex_data.get("volume_24h")
                        if data.get("price", 0) == 0 and dex_data.get("price", 0) > 0:
                            data["price"] = dex_data.get("price")
                        source = f"{source}_dex" if source != "none" else "dex"
                        logger.debug(f"Market data for {token} enhanced with DEX data")
                except Exception as e:
                    logger.warning(f"DEX data error for {token}: {str(e)[:100]}")
                    
        if not data:
            data = {}
            
        if data.get("market_cap", 0) == 0:
            data["market_cap"] = 1000000
            logger.debug(f"Using default market cap for {token}")
            
        if data.get("volume", 0) == 0:
            data["volume"] = 10000
            logger.debug(f"Using default volume for {token}")
            
        if data.get("price", 0) == 0:
            data["price"] = 0.00001
            logger.debug(f"Using default price for {token}")
            
        if "volume_growth" not in data:
            data["volume_growth"] = {"growth_5m": 0.1, "growth_1h": 0.05}
            
        if source == "none":
            source = "default"
            data["estimated"] = True
            logger.info(f"Using default market data for {token} - APIs failed")
            
        data["source"] = source
        return data

    def get_active_candidates_count(self):
        """
        Retorna el número de candidatos activos.
        """
        return len(self.token_candidates)

    def compute_confidence(self, wallet_scores, volume_1h, market_cap, recent_volume_growth=0,
                           token_type=None, whale_activity=False, volume_acceleration=0, holder_growth=0):
        """
        Calcula la puntuación de confianza para una señal con múltiples factores.
        """
        if not wallet_scores:
            return 0.0
        
        normalized_scores = [min(score / 10, 1.0) for score in wallet_scores]
        exp_scores = [score ** 1.5 for score in normalized_scores]
        trader_quality = sum(exp_scores) / len(exp_scores)
        
        whale_factor = 1.0 if whale_activity else 0.0
        holder_factor = min(max(holder_growth / 100.0, 0), 1)
        
        if market_cap <= 0:
            liquidity_factor = 0.5
        else:
            max_mcap = 100_000_000
            liquidity_factor = max(0, min(1, 1 - (market_cap / max_mcap)))
            if volume_1h > 0:
                vol_mcap_ratio = volume_1h / max(market_cap, 1)
                if vol_mcap_ratio > 0.1:
                    liquidity_factor *= 0.8
        
        volume_growth_normalized = min(max(recent_volume_growth, 0), 3) / 3
        volume_accel_normalized = min(volume_acceleration / 10.0, 1)
        technical_factor = (volume_growth_normalized * 0.7) + (volume_accel_normalized * 0.3)
        
        trader_weight = float(Config.get("TRADER_QUALITY_WEIGHT", 0.35))
        whale_weight = float(Config.get("WHALE_ACTIVITY_WEIGHT", 0.20))
        holder_weight = float(Config.get("HOLDER_GROWTH_WEIGHT", 0.15))
        liquidity_weight = float(Config.get("LIQUIDITY_HEALTH_WEIGHT", 0.15))
        technical_weight = float(Config.get("TECHNICAL_FACTORS_WEIGHT", 0.15))
        
        composite_score = (
            trader_quality * trader_weight +
            whale_factor * whale_weight +
            holder_factor * holder_weight +
            liquidity_factor * liquidity_weight +
            technical_factor * technical_weight
        )
        
        token_multiplier = 1.0
        if token_type and token_type.lower() in self.token_type_scores:
            token_multiplier = self.token_type_scores[token_type.lower()]
        composite_score *= token_multiplier
        
        def sigmoid_normalize(x, center=0.5, steepness=8):
            return 1 / (1 + math.exp(-steepness * (x - center)))
        
        normalized_score = sigmoid_normalize(composite_score, center=0.5, steepness=8)
        final_score = max(0.1, min(0.95, normalized_score))
        
        return round(final_score, 3)
    
    async def _extend_token_analysis(self, token, market_data=None):
        """
        Extiende el análisis del token integrando resultados de varios módulos.
        """
        analysis = {}
        try:
            recent_transactions = db.get_token_transactions(token, hours=1)
            whale_result = await self.whale_detector.detect_large_transactions(
                token, recent_transactions, market_cap=market_data.get("market_cap", 0) if market_data else 0
            )
        except Exception as e:
            logger.error(f"Error in whale analysis for {token}: {e}", exc_info=True)
            whale_result = {}
        try:
            market_result = await self.market_metrics.calculate_market_health(token)
        except Exception as e:
            logger.error(f"Error in market metrics for {token}: {e}", exc_info=True)
            market_result = {}
        try:
            token_result = await self.token_analyzer.analyze_volume_patterns(
                token, 
                current_volume=market_data.get("volume", 0) if market_data else 0,
                market_data=market_data
            )
            price_patterns = await self.token_analyzer.detect_price_patterns(token, None, market_data)
            if price_patterns:
                token_result.update(price_patterns)
        except Exception as e:
            logger.error(f"Error in token analysis for {token}: {e}", exc_info=True)
            token_result = {}
        try:
            active_traders = set()
            for tx in recent_transactions:
                active_traders.add(tx.get("wallet", ""))
            trader_result = {}
            for wallet in list(active_traders)[:5]:
                try:
                    profile = await self.trader_profiler.get_trader_profile(wallet)
                    if profile:
                        trader_result[wallet] = profile
                except Exception as inner_e:
                    logger.warning(f"Error getting profile for trader {wallet}: {inner_e}")
        except Exception as e:
            logger.error(f"Error in trader profiling for {token}: {e}", exc_info=True)
            trader_result = {}
        
        analysis["whale"] = whale_result
        analysis["market"] = market_result
        analysis["token"] = token_result
        analysis["trader"] = trader_result
        
        return analysis
    
    def get_active_candidates_count(self):
        """
        Retorna el número de candidatos activos.
        """
        return len(self.token_candidates)
    
    def get_signal_performance_summary(self, token):
        """
        Obtiene un resumen del rendimiento de una señal específica.
        """
        if token not in self.signal_performance:
            return None
        
        data = self.signal_performance[token]
        initial_price = data.get("initial_price", 0)
        max_price = data.get("max_price", 0)
        max_gain = data.get("max_gain", 0)
        
        last_price = self.last_prices.get(token, 0)
        current_gain = ((last_price - initial_price) / initial_price) * 100 if initial_price > 0 and last_price > 0 else 0
        
        trend = "neutral"
        if "performances" in data:
            recent_timeframes = ["1h", "2h", "4h"]
            for tf in recent_timeframes:
                if tf in data["performances"]:
                    perf = data["performances"][tf]
                    if "trend" in perf:
                        trend = perf["trend"]
                        break
        
        return {
            "token": token,
            "signal_id": data.get("signal_id", ""),
            "initial_price": initial_price,
            "current_price": last_price,
            "max_price": max_price,
            "current_gain": current_gain,
            "max_gain": max_gain,
            "is_dead": data.get("is_dead", False),
            "death_reason": data.get("death_reason"),
            "elapsed_time": int(time.time() - data.get("timestamp", 0)),
            "trend": trend
        }
    
    def cleanup_old_data(self):
        """
        Limpia datos antiguos de señales muertas para liberar memoria.
        """
        now = time.time()
        tokens_to_remove = []
        
        for token, data in self.signal_performance.items():
            if data.get("is_dead", False) and "death_time" in data:
                death_time = data["death_time"]
                if now - death_time > 86400:
                    tokens_to_remove.append(token)
        
        for token in tokens_to_remove:
            try:
                del self.signal_performance[token]
                if token in self.last_prices:
                    del self.last_prices[token]
                if token in self.signal_updates:
                    del self.signal_updates[token]
                if token in self.early_stage_monitoring:
                    del self.early_stage_monitoring[token]
                logger.info(f"Datos de señal muerta eliminados para {token}")
            except Exception as e:
                logger.error(f"Error limpiando datos de señal para {token}: {e}")
        
        return len(tokens_to_remove)
    
    async def _async_get_token_price(self, token):
        """
        Versión asíncrona para obtener el precio del token.
        """
        if self.token_data_service:
            try:
                if hasattr(self.token_data_service, 'get_token_price'):
                    price = await self.token_data_service.get_token_price(token)
                    if price and price > 0:
                        self.last_prices[token] = price
                        return price
                elif hasattr(self.token_data_service, 'get_token_data_async'):
                    token_data = await self.token_data_service.get_token_data_async(token)
                    if token_data and 'price' in token_data and token_data['price'] > 0:
                        price = token_data['price']
                        self.last_prices[token] = price
                        return price
            except Exception as e:
                logger.error(f"Error en token_data_service.get_token_price para {token}: {e}")
        
        if self.dex_monitor:
            try:
                liquidity_data = await self.dex_monitor.get_combined_liquidity_data(token)
                if liquidity_data and liquidity_data.get("price", 0) > 0:
                    price = liquidity_data["price"]
                    self.last_prices[token] = price
                    return price
            except Exception as e:
                logger.warning(f"Error obteniendo precio desde DEX Monitor para {token}: {e}")
        
        return self.last_prices.get(token, 0)
    
    def _get_token_price(self, token):
        """
        Método sincrónico para obtener el precio del token.
        """
        try:
            if token in self.last_prices:
                return self.last_prices.get(token, 0)
            
            if self.token_data_service and hasattr(self.token_data_service, 'get_token_data'):
                token_data = self.token_data_service.get_token_data(token)
                if token_data and 'price' in token_data and token_data['price'] > 0:
                    self.last_prices[token] = token_data['price']
                    return token_data['price']
        except Exception as e:
            logger.error(f"Error obteniendo precio para {token}: {e}")
        
        return 0
    
    async def _periodic_dead_signal_detection(self):
        """
        Tarea periódica para detectar señales muertas o inactivas.
        """
        interval = int(Config.get("DEAD_SIGNAL_CHECK_INTERVAL", 300))
        
        while not self.shutdown_flag:
            try:
                await asyncio.sleep(interval)
                if self.shutdown_flag:
                    break
                await self.detect_dead_signals()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error en detección periódica de señales muertas: {e}")
                await asyncio.sleep(interval * 2)
    
    async def detect_dead_signals(self):
        """
        Detecta señales inactivas o en reversión para marcarlas como muertas.
        """
        now = time.time()
        
        for token, data in self.signal_performance.items():
            if data.get("is_dead", False):
                continue
            
            last_update = data.get("last_update", 0)
            if now - last_update > 86400:
                logger.info(f"Señal para {token} marcada como muerta por inactividad")
                self.mark_signal_as_dead(token, "Inactividad (>24h)")
                continue
            
            recent_timeframes = ["4h", "2h", "1h"]
            recent_perf = None
            if "performances" in data:
                for tf in recent_timeframes:
                    if tf in data["performances"]:
                        recent_perf = data["performances"][tf]
                        break
            if recent_perf and recent_perf.get("percent_change", 0) < -30:
                logger.info(f"Señal para {token} marcada como muerta por reversión ({recent_perf.get('percent_change', 0):.2f}%)")
                self.mark_signal_as_dead(token, f"Reversión fuerte ({recent_perf.get('percent_change', 0):.2f}%)")
                continue
            
            if "performances" in data:
                recent_perf = None
                for tf in recent_timeframes:
                    if tf in data["performances"] and "liquidity" in data["performances"][tf]:
                        recent_perf = data["performances"][tf]
                        break
                if recent_perf and recent_perf.get("liquidity", 0) < 1000:
                    initial_liquidity = data.get("liquidity_initial", 0)
                    current_liquidity = recent_perf.get("liquidity", 0)
                    if initial_liquidity > 5000 and current_liquidity < initial_liquidity * 0.2:
                        logger.info(f"Señal para {token} marcada como muerta por caída de liquidez ({current_liquidity:.2f})")
                        self.mark_signal_as_dead(token, "Liquidez crítica")
                        continue
    
    def mark_signal_as_dead(self, token, reason="No especificado"):
        """
        Marca una señal como "muerta" para detener su seguimiento.
        """
        if token not in self.signal_performance:
            return
        
        self.signal_performance[token]["is_dead"] = True
        self.signal_performance[token]["death_reason"] = reason
        self.signal_performance[token]["death_time"] = time.time()
        
        signal_id = self.signal_performance[token].get("signal_id", "")
        
        try:
            if hasattr(db, "update_signal_status"):
                db.update_signal_status(signal_id, "dead", reason)
        except Exception as e:
            logger.error(f"Error actualizando estado de señal en BD: {e}")
        
        try:
            message = (
                f"⚰️ *Señal Finalizada* #{signal_id}\n\n"
                f"Token: `{token}`\n"
                f"Razón: {reason}\n"
                f"Rendimiento final: {self._get_final_performance(token)}\n"
                f"Máximo alcanzado: {self.signal_performance[token].get('max_gain', 0):.2f}%\n"
            )
            from telegram_utils import send_telegram_message
            send_telegram_message(message)
        except Exception as e:
            logger.error(f"Error enviando notificación de señal muerta: {e}")
        
        logger.info(f"Señal {signal_id} marcada como muerta. Razón: {reason}")
    
    def _get_final_performance(self, token):
        """
        Obtiene el rendimiento final de una señal para su reporte.
        """
        if token not in self.signal_performance:
            return "N/A"
        
        data = self.signal_performance[token]
        initial_price = data.get("initial_price", 0)
        
        last_price = self.last_prices.get(token, 0)
        if last_price == 0:
            performances = data.get("performances", {})
            if performances:
                recent_timeframes = ["24h", "4h", "2h", "1h", "30m", "10m", "5m", "3m"]
                for tf in recent_timeframes:
                    if tf in performances:
                        last_price = performances[tf].get("price", 0)
                        if last_price > 0:
                            break
        
        if initial_price <= 0 or last_price <= 0:
            return "N/A"
        
        percent_change = ((last_price - initial_price) / initial_price) * 100
        
        if percent_change > 50:
            emoji = "🚀"
        elif percent_change > 20:
            emoji = "🔥"
        elif percent_change > 0:
            emoji = "✅"
        elif percent_change > -20:
            emoji = "⚠️"
        else:
            emoji = "❌"
        
        return f"{emoji} *{percent_change:.2f}%*"
    
    async def _calculate_tx_velocity(self, transactions):
        """
        Calcula la velocidad de transacciones (tx/min) basado en el historial.
        """
        if not transactions or len(transactions) < 2:
            return 0
        timestamps = [tx.get("timestamp", 0) for tx in transactions]
        min_time = min(timestamps)
        max_time = max(timestamps)
        time_span = (max_time - min_time) / 60
        if time_span <= 0:
            return len(transactions)
        return len(transactions) / time_span
