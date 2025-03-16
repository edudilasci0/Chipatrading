import time
import asyncio
import logging
from config import Config
import db

logger = logging.getLogger("signal_logic")

# Función para optimizar el cálculo de confianza
def optimize_signal_confidence():
    def compute_optimized_confidence(self, wallet_scores, volume_1h, market_cap, 
                                     recent_volume_growth=0, token_type=None, 
                                     whale_activity=False, tx_velocity=0):
        if not wallet_scores:
            return 0.0
        exp_scores = [min(score ** 1.5, 12.0) for score in wallet_scores]
        weighted_avg = sum(exp_scores) / (len(exp_scores) * (Config.MAX_SCORE ** 1.5)) * Config.MAX_SCORE
        score_factor = weighted_avg / Config.MAX_SCORE

        unique_wallets = len(wallet_scores)
        wallet_diversity = min(unique_wallets / 10.0, 1.0)
        high_quality_traders = sum(1 for score in wallet_scores if score > 8.0)
        elite_traders = sum(1 for score in wallet_scores if score > 9.0)
        quality_ratio = (high_quality_traders + (elite_traders * 2)) / max(1, len(wallet_scores))
        quality_factor = min(quality_ratio * 1.5, 1.0)
        elite_bonus = min(elite_traders * 0.1, 0.3)
        
        tx_velocity_normalized = min(tx_velocity / 20.0, 1.0)
        pump_dump_risk = 0
        if tx_velocity > 15 and elite_traders == 0 and high_quality_traders / max(1, len(wallet_scores)) < 0.2:
            pump_dump_risk = 0.3
            
        wallet_factor = (score_factor * 0.4) + (wallet_diversity * 0.3) + (quality_factor * 0.2) + elite_bonus - pump_dump_risk

        if token_type == "meme":
            growth_factor = min(recent_volume_growth * 3.0, 1.0)
            market_cap_threshold = 10_000_000
        else:
            growth_factor = min(recent_volume_growth * 1.5, 1.0)
            market_cap_threshold = 5_000_000
            
        market_factor = 0.8
        if market_cap > 0:
            if market_cap < market_cap_threshold:
                market_factor = 0.9
            elif market_cap > 100_000_000:
                market_factor = 0.6
                
        tx_velocity_factor = 0
        if token_type == "meme" and tx_velocity > 5:
            tx_velocity_factor = min(0.2, tx_velocity / 25.0)
        elif tx_velocity > 10:
            tx_velocity_factor = min(0.15, tx_velocity / 30.0)

        weighted_score = (wallet_factor * 0.65) + (market_factor * 0.35) + tx_velocity_factor
        
        if whale_activity:
            weighted_score *= 1.1
            
        if token_type and token_type.lower() in self.token_type_scores:
            multiplier = self.token_type_scores[token_type.lower()]
            weighted_score *= multiplier

        import math
        def sigmoid_normalize(x, center=0.5, steepness=8):
            return 1 / (1 + math.exp(-steepness * (x - center)))
            
        normalized = max(0.1, min(1.0, sigmoid_normalize(weighted_score, 0.5, 8)))
        return round(normalized, 3)
    
    return compute_optimized_confidence

# Función para mejorar la detección de tokens emergentes (alpha)
def enhance_alpha_detection():
    async def detect_emerging_alpha_tokens(self):
        try:
            now = time.time()
            cutoff = now - 3600
            alpha_candidates = []
            for token, data in self.token_candidates.items():
                if data["first_seen"] < cutoff:
                    continue
                if len(data["wallets"]) < 2:
                    continue
                has_elite_trader = False
                trader_scores = []
                for wallet in data["wallets"]:
                    score = self.scoring_system.get_score(wallet)
                    trader_scores.append(score)
                    if score > 9.0:
                        has_elite_trader = True
                if not has_elite_trader and len(data["wallets"]) < 3:
                    continue
                market_data = await self.get_token_market_data(token)
                if market_data.get("market_cap", 0) > 20_000_000:
                    continue
                volume_1h = market_data.get("volume", 0)
                if volume_1h < 1000:
                    continue
                avg_score = sum(trader_scores) / len(trader_scores) if trader_scores else 0
                alpha_score = ((avg_score / 10.0) * 0.4 +
                               (min(len(data["wallets"]) / 5.0, 1.0) * 0.2) +
                               (min(volume_1h / 5000.0, 1.0) * 0.2) +
                               (0.2 if has_elite_trader else 0))
                alpha_candidates.append({
                    "token": token,
                    "alpha_score": alpha_score,
                    "traders_count": len(data["wallets"]),
                    "elite_traders": has_elite_trader,
                    "first_seen": data["first_seen"],
                    "volume_1h": volume_1h,
                    "market_cap": market_data.get("market_cap", 0)
                })
            alpha_candidates.sort(key=lambda x: x["alpha_score"], reverse=True)
            return [c for c in alpha_candidates if c["alpha_score"] > 0.7]
        except Exception as e:
            logger.error(f"Error detectando tokens alfa: {e}", exc_info=True)
            return []
    return detect_emerging_alpha_tokens

class SignalLogic:
    def __init__(self, scoring_system=None, helius_client=None, gmgn_client=None, rugcheck_api=None, ml_predictor=None, pattern_detector=None):
        self.scoring_system = scoring_system
        self.helius_client = helius_client
        self.gmgn_client = gmgn_client
        self.rugcheck_api = rugcheck_api
        self.ml_predictor = ml_predictor
        self.pattern_detector = pattern_detector
        from dexscreener_client import DexScreenerClient
        self.dexscreener_client = DexScreenerClient()
        self.performance_tracker = None
        self.token_candidates = {}
        self.recent_signals = []
        self.last_signal_check = time.time()
        self.watched_tokens = set()
        self._init_token_type_scores()
    
    def _init_token_type_scores(self):
        self.token_type_scores = {
            "meme": 1.1,
            "daily_runner": 1.2,
            "defi": 1.0,
            "standard": 1.0
        }
    
    def process_transaction(self, tx_data):
        try:
            if not tx_data or "token" not in tx_data:
                return
            token = tx_data.get("token")
            wallet = tx_data.get("wallet")
            amount_usd = float(tx_data.get("amount_usd", 0))
            tx_type = tx_data.get("type", "").upper()
            timestamp = tx_data.get("timestamp", time.time())
            if token in ["native", "So11111111111111111111111111111111111111112"]:
                logger.debug(f"Ignorando token nativo: {token}")
                return
            min_usd = float(Config.get("MIN_TRANSACTION_USD", 200))
            if amount_usd < min_usd:
                return
            wallets_known = []  # Aquí podrías integrar una lista de wallets conocidas
            wallet_score = self.scoring_system.get_score(wallet) if self.scoring_system else 5.0
            is_known_trader = wallet in wallets_known
            if is_known_trader:
                logger.info(f"Transacción de trader conocido: {wallet} | {token} | ${amount_usd:.2f}")
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
            if wallet_score > 8.5 or is_known_trader:
                candidate["whale_activity"] = True
                logger.info(f"Actividad elite detectada en {token}")
            tx_data_enhanced = tx_data.copy()
            tx_data_enhanced["wallet_score"] = wallet_score
            tx_data_enhanced["timestamp"] = timestamp
            tx_data_enhanced["is_known_trader"] = is_known_trader
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
                logger.error(f"Error guardando transacción en BD: {e}")
            high_volume_threshold = float(Config.get("HIGH_VOLUME_THRESHOLD", 5000))
            if amount_usd > high_volume_threshold and candidate["whale_activity"]:
                logger.info(f"Transacción de alto volumen detectada: {token} | ${amount_usd:.2f}")
                asyncio.create_task(self._process_candidates())
            logger.debug(f"Transacción procesada: {token} | {wallet} | ${amount_usd:.2f} | {tx_type}")
        except Exception as e:
            logger.error(f"Error procesando transacción en SignalLogic: {e}", exc_info=True)
    
    async def get_token_market_data(self, token):
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
        if not data or data.get("market_cap", 0) == 0:
            if self.gmgn_client:
                try:
                    gmgn_data = self.gmgn_client.get_market_data(token)
                    if gmgn_data and gmgn_data.get("market_cap", 0) > 0:
                        data = gmgn_data
                        source = "gmgn"
                        logger.debug(f"Datos de {token} obtenidos de GMGN")
                except Exception as e:
                    logger.warning(f"Error API GMGN para {token}: {str(e)[:100]}")
        if not data or data.get("market_cap", 0) == 0:
            if hasattr(self, 'dexscreener_client') and self.dexscreener_client:
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
    
    async def _process_candidates(self):
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
                tx_velocity = len(recent_txs) / max(1, (max(timestamps) - min(timestamps))/60) if len(timestamps) >= 2 else 0
                token_name = recent_txs[0].get("token_name", "").lower() if recent_txs else ""
                token_symbol = recent_txs[0].get("token_symbol", "").lower() if recent_txs else ""
                meme_keywords = ["pepe", "doge", "shib", "inu", "moon", "elon", "wojak", "cat", "rip", "justice", "killed", "dead", "murder"]
                is_meme = any(kw in token_name for kw in meme_keywords) or any(kw in token_symbol for kw in meme_keywords)
                if is_meme:
                    token_opportunity = "meme"
                elif (await self.get_token_market_data(token)).get("market_cap", 0) < 5000000 and tx_velocity > 2.0:
                    token_opportunity = "daily_runner"
                else:
                    token_opportunity = "standard"
                is_immediate = False
                if len(recent_txs) >= 2:
                    vol_diff = recent_txs[-1]["amount_usd"] - recent_txs[-2]["amount_usd"]
                    elite_trader_count = sum(1 for wallet in data["wallets"] if self.scoring_system.get_score(wallet) > 9.0)
                    if vol_diff > 1000 and elite_trader_count > 0:
                        is_immediate = True
                        logger.info(f"Señal inmediata detectada: {token} por cambio rápido de volumen (${vol_diff:.2f})")
                min_traders = int(Config.get("MIN_TRADERS_FOR_SIGNAL", 2))
                min_vol_usd = float(Config.get("MIN_VOLUME_USD", 2000))
                if token_opportunity == "meme":
                    min_vol_usd *= 0.75
                if trader_count < min_traders and not is_immediate:
                    continue
                if volume_usd < min_vol_usd and not is_immediate:
                    continue
                volume_growth = (await self.get_token_market_data(token)).get("volume_growth", {}).get("growth_5m", 0)
                wallet_scores = [self.scoring_system.get_score(w) for w in data["wallets"]]
                confidence = self.scoring_system.compute_confidence(
                    wallet_scores=wallet_scores,
                    volume_1h=volume_usd,
                    market_cap=(await self.get_token_market_data(token)).get("market_cap", 0),
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
                    "market_data": await self.get_token_market_data(token),
                    "elite_trader_count": sum(1 for wallet in data["wallets"] if self.scoring_system.get_score(wallet) > 9.0),
                    "high_quality_count": sum(1 for wallet in data["wallets"] if self.scoring_system.get_score(wallet) > 7.5),
                    "token_name": token_name,
                    "token_symbol": token_symbol
                }
                candidates.append(candidate)
            candidates.sort(key=lambda x: x["confidence"], reverse=True)
            await self._generate_signals(candidates)
        except Exception as e:
            logger.error(f"Error en _process_candidates: {e}", exc_info=True)
    
    async def _generate_signals(self, candidates):
        try:
            now = time.time()
            signal_throttling = int(Config.get("SIGNAL_THROTTLING", 10))
            recent_signals_count = db.count_signals_last_hour()
            if recent_signals_count >= signal_throttling:
                logger.info(f"Límite de señales por hora alcanzado ({recent_signals_count}/{signal_throttling})")
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
            confidence = best_candidate["confidence"]
            trader_count = best_candidate["trader_count"]
            tx_velocity = best_candidate["tx_velocity"]
            buy_ratio = best_candidate["buy_ratio"]
            token_opportunity = best_candidate["token_type"]
            market_data = best_candidate.get("market_data", {})
            initial_price = market_data.get("price", 0)
            signal_id = db.save_signal(token, trader_count, confidence, initial_price)
            features = {
                "token": token,
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
                "window_seconds": float(Config.get("SIGNAL_WINDOW_SECONDS", 540))
            }
            db.save_signal_features(signal_id, token, features)
            if self.performance_tracker:
                signal_info = {
                    "confidence": confidence,
                    "traders_count": trader_count,
                    "total_volume": best_candidate["volume_usd"],
                    "signal_id": signal_id
                }
                self.performance_tracker.add_signal(token, signal_info)
            signal_data = {
                "token": token,
                "confidence": confidence,
                "timestamp": now,
                "signal_id": signal_id,
                "token_type": token_opportunity,
                "tx_velocity": tx_velocity,
                "buy_ratio": buy_ratio
            }
            self.recent_signals.append(signal_data)
            if len(self.recent_signals) > 20:
                self.recent_signals = self.recent_signals[-20:]
            self.watched_tokens.add(token)
            from telegram_utils import format_signal_message, send_telegram_message
            if token_opportunity == "daily_runner":
                msg = format_signal_message(signal_data, "daily_runner")
            elif token_opportunity == "meme":
                msg = format_signal_message(signal_data, "meme")
            else:
                msg = format_signal_message(signal_data, "signal")
            send_telegram_message(msg)
            logger.info(f"Señal generada para {token} con confianza {confidence:.2f}")
        except Exception as e:
            logger.error(f"Error en _generate_signals: {e}", exc_info=True)
    
    async def check_signals_periodically(self):
        while True:
            try:
                if time.time() - self.last_signal_check > 60:
                    await self._process_candidates()
                    self.last_signal_check = time.time()
            except Exception as e:
                logger.error(f"Error en check_signals_periodically: {e}", exc_info=True)
            await asyncio.sleep(10)
