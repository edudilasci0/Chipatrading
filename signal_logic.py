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
        
        # Instanciar módulos de análisis avanzado
        self.whale_detector = WhaleDetector(helius_client=self.helius_client)
        self.market_metrics = MarketMetricsAnalyzer(helius_client=self.helius_client)
        self.token_analyzer = TokenAnalyzer(token_data_service=self.helius_client)
        self.trader_profiler = TraderProfiler()
        
        self.performance_tracker = None  # Se asigna externamente

    def _init_token_type_scores(self):
        self.token_type_scores = {
            "meme": 1.2,
            "standard": 1.0
        }
    
    def process_transaction(self, tx_data):
        """
        Procesa una transacción entrante:
         - Valida los datos
         - Actualiza el registro de candidatos
         - Guarda la transacción en DB
         - Llama al procesamiento de candidatos en caso de transacción de alto volumen
        """
        try:
            if not tx_data or "token" not in tx_data:
                return
            token = tx_data.get("token")
            wallet = tx_data.get("wallet")
            amount_usd = float(tx_data.get("amount_usd", 0))
            tx_type = tx_data.get("type", "").upper()
            timestamp = tx_data.get("timestamp", time.time())
            if token in ["native", "So11111111111111111111111111111111111111112"]:
                logger.debug(f"Ignoring native token: {token}")
                return
            min_usd = float(Config.get("MIN_TRANSACTION_USD", 200))
            if amount_usd < min_usd:
                return
            if token not in self.token_candidates:
                self.token_candidates[token] = {
                    "wallets": set(),
                    "transactions": [],
                    "first_seen": timestamp,
                    "last_update": timestamp,
                    "whale_activity": False,
                    "volume_usd": 0,
                    "buy_count": 0,
                    "sell_count": 0
                }
            candidate = self.token_candidates[token]
            candidate["wallets"].add(wallet)
            wallet_score = self.scoring_system.get_score(wallet) if self.scoring_system else 5.0
            if wallet_score > 8.5:
                candidate["whale_activity"] = True
                logger.info(f"Elite trader activity detected for token {token}")
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
            try:
                db.save_transaction({
                    "wallet": wallet,
                    "token": token,
                    "type": tx_type,
                    "amount_usd": amount_usd
                })
            except Exception as e:
                logger.error(f"Error saving transaction in DB: {e}")
            # Si es una transacción de alto volumen y de trader elite, forzar el procesamiento
            if amount_usd > float(Config.get("HIGH_VOLUME_THRESHOLD", 5000)) and candidate["whale_activity"]:
                logger.info(f"High volume transaction detected: {token} | ${amount_usd:.2f}")
                asyncio.create_task(self._process_candidates())
            logger.debug(f"Transaction processed: {token} | {wallet} | ${amount_usd:.2f} | {tx_type}")
        except Exception as e:
            logger.error(f"Error processing transaction in SignalLogic: {e}", exc_info=True)
    
    def compute_confidence(self, wallet_scores, volume_1h, market_cap, recent_volume_growth=0,
                           token_type=None, whale_activity=False, volume_acceleration=0, holder_growth=0):
        """
        Calcula la puntuación de confianza integrando múltiples factores:
         - Calidad de traders (35%)
         - Actividad de ballenas (20%)
         - Crecimiento de holders (15%)
         - Análisis de liquidez (15%)
         - Aceleración de volumen y patrones técnicos (15%)
        Se utiliza una función sigmoidea para normalizar el resultado entre 0.1 y 0.95.
        """
        if not wallet_scores:
            return 0.0
        # Calidad de traders: promedio de wallet_scores / 10
        trader_quality = sum(wallet_scores) / (10 * len(wallet_scores))
        # Factor ballenas: 1 si hay actividad, 0 en caso contrario
        whale_factor = 1.0 if whale_activity else 0.0
        # Crecimiento de holders: se espera un porcentaje; normalizamos dividiendo por 100
        holder_factor = min(max(holder_growth / 100.0, 0), 1)
        # Factor liquidez: se calcula como inversamente proporcional al market cap (umbral 100M)
        liquidity_factor = max(0, min(1, 1 - (market_cap / 100_000_000))) if market_cap > 0 else 0.5
        # Factor de volumen técnico: basado en la aceleración del volumen
        volume_factor = min(volume_acceleration / 10.0, 1)
        
        composite_score = (0.20 * whale_factor +
                           0.15 * holder_factor +
                           0.15 * liquidity_factor +
                           0.15 * volume_factor +
                           0.35 * trader_quality)
        
        # Función sigmoidea para normalizar
        def sigmoid(x, center=0.5, steepness=8):
            return 1 / (1 + math.exp(-steepness * (x - center)))
        
        raw_conf = sigmoid(composite_score, center=0.5, steepness=8)
        normalized_conf = max(0.1, min(0.95, raw_conf))
        return round(normalized_conf, 3)
    
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
            whale_result = await self.whale_detector.detect_large_transactions(
                token, recent_transactions=[], market_cap=market_data.get("market_cap", 0) if market_data else 0
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
                token, volume_1h=market_data.get("volume", 0) if market_data else 0, market_data=market_data
            )
        except Exception as e:
            logger.error(f"Error in token analysis for {token}: {e}", exc_info=True)
            token_result = {}
        try:
            trader_result = await self.trader_profiler.get_trader_profile(token)
        except Exception as e:
            logger.error(f"Error in trader profiling for {token}: {e}", exc_info=True)
            trader_result = {}
        
        analysis["whale"] = whale_result
        analysis["market"] = market_result
        analysis["token"] = token_result
        analysis["trader"] = trader_result
        return analysis
    
    async def _process_candidates(self):
        """
        Evalúa los tokens candidatos utilizando un análisis compuesto y
        calcula la puntuación de confianza de la señal.
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
                market_data = await self.get_token_market_data(token)
                extended_analysis = await self._extend_token_analysis(token, market_data)
                volume_acceleration = extended_analysis.get("token", {}).get("volume_acceleration", 0)
                holder_growth = extended_analysis.get("market", {}).get("holder_growth_rate", 0)
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
            
            logger.info(f"🔄 Candidatos calificados: {len(candidates)}. Ordenados por confianza: {[f'{c['token']}:{c['confidence']:.2f}' for c in candidates[:3]]}")
            
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
                token_type="🔴 TOKEN PUMP" if token.endswith("pump") else "",
                token_name=token_name,
                market_cap=market_cap,
                initial_price=initial_price,
                extended_analysis=best_candidate.get("extended_analysis", {}),
                signal_level=signal_level
            )
            
            logger.info(f"Signal generated for {token} with confidence {confidence:.2f} (Level {signal_level})")
        except Exception as e:
            logger.error(f"Error in _generate_signals: {e}", exc_info=True)
    
    async def get_token_market_data(self, token):
        """
        Obtiene datos de mercado para el token, utilizando diversas fuentes.
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
        if not data or data.get("market_cap", 0) == 0:
            if hasattr(self, 'dexscreener_client') and self.dexscreener_client:
                try:
                    dex_data = await self.dexscreener_client.fetch_token_data(token)
                    if dex_data and dex_data.get("market_cap", 0) > 0:
                        data = dex_data
                        source = "dexscreener"
                        logger.debug(f"Market data for {token} obtained from DexScreener")
                except Exception as e:
                    logger.warning(f"DexScreener API error for {token}: {str(e)[:100]}")
        if not data or data.get("market_cap", 0) == 0:
            data = {
                "price": 0.00001,
                "market_cap": 1000000,
                "volume": 10000,
                "volume_growth": {"growth_5m": 0.1, "growth_1h": 0.05},
                "estimated": True
            }
            source = "default"
            logger.info(f"Using default market data for {token} - APIs failed")
        data["source"] = source
        return data
    
    def get_active_candidates_count(self):
        return len(self.token_candidates)
