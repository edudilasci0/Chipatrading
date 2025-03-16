import time
import asyncio
import logging
from config import Config
import db

logger = logging.getLogger("signal_logic")

class SignalLogic:
    def __init__(self, scoring_system=None, helius_client=None, gmgn_client=None, rugcheck_api=None, ml_predictor=None, pattern_detector=None):
        self.scoring_system = scoring_system
        self.helius_client = helius_client
        self.gmgn_client = gmgn_client
        self.rugcheck_api = rugcheck_api
        self.ml_predictor = ml_predictor
        self.pattern_detector = pattern_detector
        
        # Inicializar DexScreener como fuente alternativa
        from dexscreener_client import DexScreenerClient
        self.dexscreener_client = DexScreenerClient()
        
        self.performance_tracker = None
        self.token_candidates = {}
        self.recent_signals = []
        self.last_signal_check = time.time()
        self.watched_tokens = set()
        self._init_token_type_scores()
    
    def _init_token_type_scores(self):
        """
        Inicializa los multiplicadores para cada tipo de token.
        """
        self.token_type_scores = {
            "meme": 1.2,
            "daily_runner": 1.15,
            "standard": 1.0
        }
    
    def process_transaction(self, tx_data):
        """
        Procesa una transacción recibida y actualiza candidatos para señales.
        """
        try:
            if not tx_data or "token" not in tx_data:
                return
            
            token = tx_data.get("token")
            wallet = tx_data.get("wallet")
            amount_usd = float(tx_data.get("amount_usd", 0))
            tx_type = tx_data.get("type", "").upper()
            timestamp = tx_data.get("timestamp", time.time())
            
            # Ignorar tokens nativos
            if token in ["native", "So11111111111111111111111111111111111111112"]:
                logger.debug(f"Ignorando token nativo: {token}")
                return
            
            # Verificar monto mínimo
            min_usd = float(Config.get("MIN_TRANSACTION_USD", 200))
            if amount_usd < min_usd:
                return
            
            # Se asume que si la wallet es conocida, es de mayor calidad
            wallet_score = self.scoring_system.get_score(wallet) if self.scoring_system else Config.DEFAULT_SCORE
            
            # Inicializar candidato si no existe
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
            candidate["volume_usd"] += amount_usd
            if tx_type == "BUY":
                candidate["buy_count"] += 1
            elif tx_type == "SELL":
                candidate["sell_count"] += 1
            
            # Marcar actividad de whale si el score es alto
            if wallet_score > 8.5:
                candidate["whale_activity"] = True
            
            tx_data_enhanced = tx_data.copy()
            tx_data_enhanced["wallet_score"] = wallet_score
            tx_data_enhanced["timestamp"] = timestamp
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
                logger.error(f"Error guardando transacción en BD: {e}")
            
            # Si se detecta una transacción de alto volumen de trader elite, forzar procesamiento
            high_volume_threshold = float(Config.get("HIGH_VOLUME_THRESHOLD", 5000))
            if amount_usd > high_volume_threshold and candidate["whale_activity"]:
                logger.info(f"🚨 Transacción de alto volumen en {token}: ${amount_usd:.2f}")
                asyncio.create_task(self._process_candidates())
            
            logger.debug(f"Transacción procesada: {token} | {wallet} | ${amount_usd:.2f} | {tx_type}")
        except Exception as e:
            logger.error(f"Error procesando transacción en SignalLogic: {e}", exc_info=True)
    
    async def _process_candidates(self):
        """
        Procesa candidatos para generar señales.
        Aplica filtros, calcula métricas y llama a _generate_signals.
        """
        try:
            now = time.time()
            window = float(Config.get("SIGNAL_WINDOW_SECONDS", 540))
            cutoff = now - window
            candidates = []
            
            for token, data in list(self.token_candidates.items()):
                recent_txs = [tx for tx in data["transactions"] if tx["timestamp"] > cutoff]
                if not recent_txs or token in self.watched_tokens:
                    continue
                trader_count = len(data["wallets"])
                volume_usd = sum(tx["amount_usd"] for tx in recent_txs)
                buy_txs = [tx for tx in recent_txs if tx.get("type") == "BUY"]
                buy_ratio = len(buy_txs) / max(1, len(recent_txs))
                
                timestamps = [tx["timestamp"] for tx in recent_txs]
                tx_velocity = len(recent_txs) / max(1, (max(timestamps) - min(timestamps)) / 60)
                
                # Filtrar por market cap mínimo (mínimo $100,000)
                market_data = await self.get_token_market_data(token)
                if market_data.get("market_cap", 0) < 100000:
                    continue
                
                # Clasificar tipo de token (meme, daily_runner o standard)
                token_opportunity = self._classify_token_opportunity(token, recent_txs, market_data)
                
                # Detectar cambios rápidos de volumen para señales inmediatas
                is_immediate = False
                if len(recent_txs) >= 2:
                    vol_diff = recent_txs[-1]["amount_usd"] - recent_txs[-2]["amount_usd"]
                    if vol_diff > 1000:
                        is_immediate = True
                        logger.info(f"Señal inmediata: {token} por cambio de volumen (${vol_diff:.2f})")
                
                wallet_scores = [self.scoring_system.get_score(w) for w in data["wallets"]]
                volume_growth = market_data.get("volume_growth", {}).get("growth_5m", 0)
                confidence = self.scoring_system.compute_confidence(
                    wallet_scores,
                    volume_usd,
                    market_data.get("market_cap", 0),
                    recent_volume_growth=volume_growth,
                    token_type=token_opportunity,
                    whale_activity=data.get("whale_activity", False),
                    tx_velocity=tx_velocity
                )
                if is_immediate:
                    confidence = min(1.0, confidence * 1.3)
                
                candidate = {
                    "token": token,
                    "confidence": confidence,
                    "trader_count": trader_count,
                    "volume_usd": volume_usd,
                    "recent_transactions": recent_txs,
                    "tx_velocity": tx_velocity,
                    "buy_ratio": buy_ratio,
                    "token_type": token_opportunity,
                    "market_data": market_data
                }
                candidates.append(candidate)
            
            candidates.sort(key=lambda x: x["confidence"], reverse=True)
            await self._generate_signals(candidates)
        except Exception as e:
            logger.error(f"Error en _process_candidates: {e}", exc_info=True)
    
    async def _generate_signals(self, candidates):
        """
        Genera señales a partir de la lista de candidatos.
        """
        try:
            now = time.time()
            signal_throttling = int(Config.get("SIGNAL_THROTTLING", 10))
            recent_signals_count = db.count_signals_last_hour()
            if recent_signals_count >= signal_throttling:
                logger.info(f"Límite de señales alcanzado: {recent_signals_count}/{signal_throttling}")
                return
            
            if self.recent_signals and now - self.recent_signals[-1]["timestamp"] < 180:
                return
            
            min_confidence = float(Config.get("MIN_CONFIDENCE_THRESHOLD", 0.3))
            qualifying_candidates = [c for c in candidates if c["confidence"] >= min_confidence]
            if not qualifying_candidates:
                return
            
            best_candidate = qualifying_candidates[0]
            token = best_candidate["token"]
            for sig in self.recent_signals:
                if sig["token"] == token and now - sig["timestamp"] < 3600:
                    return
            
            # Guardar señal en la base de datos
            initial_price = best_candidate["market_data"].get("price", 0)
            signal_id = db.save_signal(token, best_candidate["trader_count"], best_candidate["confidence"], initial_price)
            features = {
                "token": token,
                "trader_count": best_candidate["trader_count"],
                "num_transactions": len(best_candidate["recent_transactions"]),
                "total_volume_usd": best_candidate["volume_usd"],
                "avg_volume_per_trader": best_candidate["volume_usd"] / max(1, best_candidate["trader_count"]),
                "buy_ratio": best_candidate["buy_ratio"],
                "tx_velocity": best_candidate["tx_velocity"],
                "market_cap": best_candidate["market_data"].get("market_cap", 0),
                "volume_1h": best_candidate["market_data"].get("volume", 0),
                "volume_growth_5m": best_candidate["market_data"].get("volume_growth", {}).get("growth_5m", 0),
                "whale_flag": 1 if best_candidate.get("whale_activity") else 0,
                "is_meme": 1 if best_candidate["token_type"] == "meme" else 0,
                "window_seconds": float(Config.get("SIGNAL_WINDOW_SECONDS", 540))
            }
            db.save_signal_features(signal_id, token, features)
            if self.performance_tracker:
                signal_info = {
                    "confidence": best_candidate["confidence"],
                    "traders_count": best_candidate["trader_count"],
                    "total_volume": best_candidate["volume_usd"],
                    "signal_id": signal_id
                }
                self.performance_tracker.add_signal(token, signal_info)
            signal_data = {
                "token": token,
                "confidence": best_candidate["confidence"],
                "timestamp": now,
                "signal_id": signal_id,
                "token_type": best_candidate["token_type"],
                "tx_velocity": best_candidate["tx_velocity"],
                "buy_ratio": best_candidate["buy_ratio"]
            }
            self.recent_signals.append(signal_data)
            if len(self.recent_signals) > 20:
                self.recent_signals = self.recent_signals[-20:]
            self.watched_tokens.add(token)
            from telegram_utils import format_signal_message, send_telegram_message
            if best_candidate["token_type"] == "daily_runner":
                message = format_signal_message(signal_data, "daily_runner")
            elif best_candidate["token_type"] == "meme":
                message = format_signal_message(signal_data, "early_alpha")
            else:
                message = format_signal_message(signal_data, "signal")
            send_telegram_message(message)
            logger.info(f"Señal generada para {token} con confianza {best_candidate['confidence']:.2f}")
        except Exception as e:
            logger.error(f"Error en _generate_signals: {e}", exc_info=True)
    
    def _classify_token_opportunity(self, token, recent_txs, market_data):
        """
        Clasifica oportunidades con énfasis en memecoins y daily runners.
        Devuelve "daily_runner", "meme" o "standard".
        """
        token_type = "standard"
        if market_data.get("market_cap", 0) < 5_000_000 and market_data.get("volume_growth", {}).get("growth_5m", 0) > 0.3:
            token_type = "daily_runner"
        meme_keywords = ["pepe", "doge", "shib", "inu", "moon", "elon", "wojak", "cat"]
        for tx in recent_txs:
            token_name = tx.get("token_name", "").lower()
            token_symbol = tx.get("token_symbol", "").lower()
            if any(kw in token_name for kw in meme_keywords) or any(kw in token_symbol for kw in meme_keywords):
                token_type = "meme"
                break
        return token_type
    
    async def get_token_market_data(self, token):
        """
        Obtiene datos de mercado utilizando múltiples APIs.
        """
        data = None
        source = "none"
        if self.helius_client:
            try:
                data = self.helius_client.get_token_data(token)
                if data and data.get("market_cap", 0) > 0:
                    source = "helius"
                    logger.debug(f"Datos de {token} obtenidos de Helius")
            except Exception as e:
                logger.warning(f"Error API Helius para {token}: {str(e)[:100]}")
        if (not data or data.get("market_cap", 0) == 0) and self.gmgn_client:
            try:
                gmgn_data = self.gmgn_client.get_market_data(token)
                if gmgn_data and gmgn_data.get("market_cap", 0) > 0:
                    data = gmgn_data
                    source = "gmgn"
                    logger.debug(f"Datos de {token} obtenidos de GMGN")
            except Exception as e:
                logger.warning(f"Error API GMGN para {token}: {str(e)[:100]}")
        if (not data or data.get("market_cap", 0) == 0) and hasattr(self, 'dexscreener_client') and self.dexscreener_client:
            try:
                dex_data = await self.dexscreener_client.fetch_token_data(token)
                if dex_data and dex_data.get("market_cap", 0) > 0:
                    data = dex_data
                    source = "dexscreener"
                    logger.debug(f"Datos de {token} obtenidos de DexScreener")
            except Exception as e:
                logger.warning(f"Error API DexScreener para {token}: {str(e)[:100]}")
        if not data or data.get("market_cap", 0) == 0:
            data = {
                "price": 0.00001,
                "market_cap": 1000000,
                "volume": 10000,
                "volume_growth": {"growth_5m": 0.1, "growth_1h": 0.05},
                "estimated": True
            }
            source = "default"
            logger.warning(f"Usando datos predeterminados para {token} - todas las APIs fallaron")
        data["source"] = source
        return data

    # Método check_signals_periodically se espera que exista en tu sistema
    async def check_signals_periodically(self):
        while True:
            try:
                if time.time() - self.last_signal_check > 60:
                    await self._process_candidates()
                    self.last_signal_check = time.time()
            except Exception as e:
                logger.error(f"Error en check_signals_periodically: {e}", exc_info=True)
            await asyncio.sleep(10)
