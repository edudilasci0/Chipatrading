import time
import asyncio
import logging
from config import Config
import db

logger = logging.getLogger("signal_logic")

class SignalLogic:
    def __init__(self, scoring_system=None, helius_client=None, gmgn_client=None, ml_predictor=None, pattern_detector=None):
        # Inicialización de componentes (RugCheck eliminado)
        self.scoring_system = scoring_system
        self.helius_client = helius_client
        self.gmgn_client = gmgn_client
        self.ml_predictor = ml_predictor
        self.pattern_detector = pattern_detector
        self.performance_tracker = None
        self.token_candidates = {}  # Diccionario de tokens candidatos
        self.recent_signals = []    # Lista de señales generadas
        self.last_signal_check = time.time()
        self.last_cleanup = time.time()
        self.watched_tokens = set()  # Tokens ya en seguimiento

    def process_transaction(self, tx_data):
        """
        Procesa una transacción recibida de Cielo y actualiza los candidatos a señales.
        """
        try:
            if not tx_data or "token" not in tx_data:
                return
            
            token = tx_data.get("token")
            wallet = tx_data.get("wallet")
            amount_usd = float(tx_data.get("amount_usd", 0))
            tx_type = tx_data.get("type", "").upper()
            timestamp = tx_data.get("timestamp", time.time())
            
            min_usd = float(Config.get("MIN_TRANSACTION_USD", 200))
            if amount_usd < min_usd:
                return
            
            if token not in self.token_candidates:
                self.token_candidates[token] = {
                    "wallets": set(),
                    "transactions": [],
                    "first_seen": timestamp,
                    "last_update": timestamp,
                    "whale_activity": False
                }
            
            candidate = self.token_candidates[token]
            candidate["wallets"].add(wallet)
            
            # Evaluar si la wallet es de alta calidad ("whale")
            wallet_score = self.scoring_system.get_score(wallet) if self.scoring_system else 5.0
            if wallet_score > 8.5:
                candidate["whale_activity"] = True
            
            # Agregar transacción con información adicional
            tx_data_enhanced = tx_data.copy()
            tx_data_enhanced["wallet_score"] = wallet_score
            tx_data_enhanced["timestamp"] = timestamp
            candidate["transactions"].append(tx_data_enhanced)
            candidate["last_update"] = timestamp
            
            # Guardar en BD para análisis posterior
            try:
                db.save_transaction({
                    "wallet": wallet,
                    "token": token,
                    "type": tx_type,
                    "amount_usd": amount_usd
                })
            except Exception as e:
                logger.error(f"Error guardando transacción en BD: {e}")
            
            logger.debug(f"Transacción procesada: {token} | {wallet} | ${amount_usd:.2f} | {tx_type}")
        except Exception as e:
            logger.error(f"Error procesando transacción en SignalLogic: {e}", exc_info=True)

    def _classify_token_type(self, token_name, token_symbol):
        """
        Clasifica automáticamente el tipo de token basado en su nombre y símbolo.
        Retorna 'meme', 'defi', 'gaming' o 'unknown'.
        """
        name = token_name.lower() if token_name else ""
        symbol = token_symbol.lower() if token_symbol else ""
        
        meme_keywords = ["doge", "shib", "pepe", "wojak", "moon", "elon", "cat", "inu"]
        defi_keywords = ["swap", "yield", "farm", "stake", "dao", "defi", "finance", "lend", "borrow"]
        gaming_keywords = ["game", "nft", "play", "meta", "verse", "land", "world"]
        
        if any(kw in name for kw in meme_keywords) or any(kw in symbol for kw in meme_keywords):
            return "meme"
        elif any(kw in name for kw in defi_keywords) or any(kw in symbol for kw in defi_keywords):
            return "defi"
        elif any(kw in name for kw in gaming_keywords) or any(kw in symbol for kw in gaming_keywords):
            return "gaming"
        return "unknown"

    def _classify_token_opportunity(self, token, recent_txs, market_data):
        """
        Clasifica oportunidades con énfasis en memecoins y daily runners.
        Devuelve 'daily_runner', 'meme' o None según criterios.
        """
        token_type = None
        if market_data.get("market_cap", 0) < 5_000_000 and market_data.get("volume_growth", {}).get("growth_5m", 0) > 0.3:
            token_type = "daily_runner"
        meme_keywords = ["pepe", "doge", "shib", "inu", "moon", "elon", "wojak"]
        for tx in recent_txs:
            token_name = tx.get("token_name", "").lower()
            if any(keyword in token_name for keyword in meme_keywords):
                token_type = "meme"
                break
        return token_type

    def _monitor_rapid_volume_changes(self, token, candidate):
        """
        Monitorea cambios rápidos en el volumen. Retorna True si la diferencia entre
        las dos últimas transacciones supera un umbral.
        """
        txs = candidate.get("transactions", [])
        if len(txs) < 2:
            return False
        vol_diff = abs(txs[-1]["amount_usd"] - txs[-2]["amount_usd"])
        threshold = float(Config.get("RAPID_VOLUME_THRESHOLD", 5000))
        if vol_diff > threshold:
            logger.info(f"Rápido cambio de volumen detectado en {token}: Δ${vol_diff:.2f}")
            return True
        return False

    async def get_token_market_data(self, token):
        """
        Obtiene datos de mercado con integración con Helius y manejo de fallbacks.
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
                logger.error(f"Error API Helius para {token}: {e}", exc_info=True)
        if not data or data.get("market_cap", 0) == 0:
            if self.gmgn_client:
                try:
                    gmgn_data = self.gmgn_client.get_market_data(token)
                    if gmgn_data and gmgn_data.get("market_cap", 0) > 0:
                        data = gmgn_data
                        source = "gmgn"
                        logger.debug(f"Datos de {token} obtenidos de GMGN")
                except Exception as e:
                    logger.error(f"Error API GMGN para {token}: {e}", exc_info=True)
        if not data:
            data = {"price": 0, "market_cap": 0, "volume": 0,
                    "volume_growth": {"growth_5m": 0, "growth_1h": 0}, "estimated": True}
            source = "none"
            logger.debug(f"Datos por defecto usados para {token}")
        data["source"] = source
        return data

    async def _process_candidates(self):
        """
        Procesa candidatos para generar señales, priorizando daily runners y aplicando lógica para señales inmediatas.
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
                tx_velocity = len(recent_txs) / max(1, (max(timestamps) - min(timestamps))/60)
                
                market_data = await self.get_token_market_data(token)
                token_opportunity = self._classify_token_opportunity(token, recent_txs, market_data)
                
                # Señal inmediata si es daily_runner, meme o hay cambio rápido de volumen
                is_immediate = self._monitor_rapid_volume_changes(token, data)
                if token_opportunity in ["daily_runner", "meme"] or is_immediate:
                    logger.info(f"Señal inmediata detectada para {token} ({token_opportunity})")
                
                wallet_scores = [self.scoring_system.get_score(w) for w in data["wallets"]]
                volume_growth = market_data.get("volume_growth", {}).get("growth_5m", 0)
                
                confidence = self.scoring_system.compute_confidence(
                    wallet_scores,
                    volume_usd,
                    market_data.get("market_cap", 0),
                    recent_volume_growth=volume_growth,
                    token_type=token_opportunity,
                    whale_activity=data.get("whale_activity", False)
                )
                
                if is_immediate:
                    confidence = min(1.0, confidence * 1.2)
                
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
        Genera señales a partir de candidatos procesados.
        """
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
            
            # Se omite la validación con RugCheck (no se incluye)
            
            confidence = best_candidate["confidence"]
            trader_count = best_candidate["trader_count"]
            tx_velocity = best_candidate["tx_velocity"]
            buy_ratio = best_candidate["buy_ratio"]
            token_type = best_candidate["token_type"]
            market_data = best_candidate.get("market_data", {})
            initial_price = market_data.get("price", 0)
            
            try:
                signal_id = db.save_signal(token, trader_count, confidence, initial_price)
                features = {
                    "token": token,
                    "trader_count": trader_count,
                    "num_transactions": len(best_candidate["recent_transactions"]) if "recent_transactions" in best_candidate else len(best_candidate["transactions"]),
                    "total_volume_usd": best_candidate["volume_usd"],
                    "avg_volume_per_trader": best_candidate["volume_usd"] / max(1, trader_count),
                    "buy_ratio": buy_ratio,
                    "tx_velocity": tx_velocity,
                    "market_cap": market_data.get("market_cap", 0),
                    "volume_1h": market_data.get("volume", 0),
                    "volume_growth_5m": market_data.get("volume_growth", {}).get("growth_5m", 0),
                    "volume_growth_1h": market_data.get("volume_growth", {}).get("growth_1h", 0),
                    "whale_flag": 1 if best_candidate.get("whale_activity") else 0,
                    "is_meme": 1 if token_type == "meme" else 0,
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
                    "token_type": token_type,
                    "tx_velocity": tx_velocity,
                    "buy_ratio": buy_ratio
                }
                
                self.recent_signals.append(signal_data)
                if len(self.recent_signals) > 20:
                    self.recent_signals = self.recent_signals[-20:]
                self.watched_tokens.add(token)
                
                from telegram_utils import format_signal_message, send_telegram_message
                if token_type == "daily_runner":
                    alert_message = format_signal_message(signal_data, "daily_runner")
                elif token_type == "meme":
                    alert_message = format_signal_message(signal_data, "meme")
                else:
                    alert_message = format_signal_message(signal_data, "signal")
                send_telegram_message(alert_message)
                
                logger.info(f"Señal generada para {token} con confianza {confidence:.2f}")
                
            except Exception as e:
                logger.error(f"Error guardando señal: {e}")
            
        except Exception as e:
            logger.error(f"Error en _generate_signals: {e}", exc_info=True)

    async def check_signals_periodically(self):
        """
        Ejecuta periódicamente la verificación de señales.
        """
        while True:
            try:
                if time.time() - self.last_signal_check > 60:
                    await self._process_candidates()
                    self.last_signal_check = time.time()
            except Exception as e:
                logger.error(f"Error en check_signals_periodically: {e}", exc_info=True)
            await asyncio.sleep(10)
