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
        self.performance_tracker = None
        self.token_candidates = {}  # {token: {wallets, transactions, first_seen, last_update, whale_activity, volume_usd, buy_count, sell_count}}
        self.recent_signals = []    # Lista de señales emitidas
        self.last_signal_check = time.time()
        self.watched_tokens = set()
        # Inicializar scores para tipos de tokens
        self._init_token_type_scores()

    def _init_token_type_scores(self):
        self.token_type_scores = {
            "meme": 1.2,
            "daily_runner": 1.1,
            "standard": 1.0
        }

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
            # Evitar procesar tokens nativos
            if token in ["native", "So11111111111111111111111111111111111111112"]:
                logger.debug(f"Ignorando token nativo: {token}")
                return
            min_usd = float(Config.get("MIN_TRANSACTION_USD", 200))
            if amount_usd < min_usd:
                return
            # Obtener score de la wallet
            wallet_score = self.scoring_system.get_score(wallet) if self.scoring_system else 5.0
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
            if wallet_score > 8.5:
                candidate["whale_activity"] = True
                logger.info(f"⚡ Actividad de trader elite detectada en {token}")
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
                logger.error(f"Error guardando transacción en BD: {e}")
            high_volume_threshold = float(Config.get("HIGH_VOLUME_THRESHOLD", 5000))
            if amount_usd > high_volume_threshold and candidate["whale_activity"]:
                logger.info(f"🚨 Transacción de alto volumen detectada: {token} | ${amount_usd:.2f}")
                asyncio.create_task(self._process_candidates())
            logger.debug(f"Transacción procesada: {token} | {wallet} | ${amount_usd:.2f} | {tx_type}")
        except Exception as e:
            logger.error(f"Error procesando transacción en SignalLogic: {e}", exc_info=True)

    def _classify_token_opportunity(self, token, recent_txs, market_data):
        """
        Clasifica oportunidades con énfasis en memecoins y daily runners.
        Devuelve 'daily_runner', 'meme' o 'standard' según criterios.
        """
        token_type = "standard"
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
        Monitorea cambios rápidos en el volumen de transacciones.
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
        Obtiene datos de mercado utilizando Helius y GMGN como fallback.
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
            data = {
                "price": 0,
                "market_cap": 0,
                "volume": 0,
                "volume_growth": {"growth_5m": 0, "growth_1h": 0},
                "estimated": True
            }
            source = "none"
            logger.debug(f"Datos por defecto usados para {token}")
        data["source"] = source
        return data

    async def _process_candidates(self):
        """
        Procesa candidatos para señales, filtrando tokens con market cap menor a MIN_MARKETCAP.
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
                tx_velocity = len(recent_txs) / max(1, ((max(timestamps) - min(timestamps)) / 60))
                market_data = await self.get_token_market_data(token)
                if market_data.get("market_cap", 0) < Config.MIN_MARKETCAP:
                    continue
                token_opportunity = self._classify_token_opportunity(token, recent_txs, market_data)
                is_immediate = self._monitor_rapid_volume_changes(token, data)
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
            if self.rugcheck_api:
                is_safe = self.rugcheck_api.validate_token_safety(token, 50)
                if not is_safe:
                    logger.info(f"Token {token} rechazado por RugCheck")
                    db.save_failed_token(token, "Failed RugCheck validation")
                    return
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
                    msg = format_signal_message(signal_data, "daily_runner")
                elif token_type == "meme":
                    msg = format_signal_message(signal_data, "meme")
                else:
                    msg = format_signal_message(signal_data, "signal")
                send_telegram_message(msg)
                logger.info(f"Señal generada para {token} con confianza {confidence:.2f}")
            except Exception as e:
                logger.error(f"Error guardando señal: {e}")
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
