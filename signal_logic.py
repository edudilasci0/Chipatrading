import time
import asyncio
import logging
import math
from typing import Dict, Any
from config import Config
import db
from market_metrics import MarketMetricsAnalyzer
from token_analyzer import TokenAnalyzer
from trader_profiler import TraderProfiler
from telegram_utils import send_enhanced_signal

logger = logging.getLogger("signal_logic")

class SignalLogic:
    def __init__(self, scoring_system=None, rugcheck_api=None, ml_predictor=None, pattern_detector=None, wallet_tracker=None):
        self.scoring_system = scoring_system
        self.rugcheck_api = rugcheck_api  # Se mantiene pero puede ser None
        self.ml_predictor = ml_predictor
        self.pattern_detector = pattern_detector
        self.wallet_tracker = wallet_tracker
        self.token_candidates = {}
        self.recent_signals = []
        self.last_signal_check = time.time()
        self.watched_tokens = set()
        self.token_type_scores = {}
        self._init_token_type_scores()
        self.monitored_tokens = {}
        self.last_monitoring_time = time.time()
        
        # Usamos solo DexScreener para datos de mercado
        self.market_metrics = MarketMetricsAnalyzer()
        self.token_analyzer = TokenAnalyzer()
        self.trader_profiler = TraderProfiler()
        
        self.performance_tracker = None  # Se asigna externamente
        
        asyncio.create_task(self.periodic_monitoring())

    def _init_token_type_scores(self):
        self.token_type_scores = {
            "meme": 1.2,
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
                
            wallet_score = self.scoring_system.get_score(wallet) if self.scoring_system else 5.0
            
            self._update_token_stats(token, wallet, tx_data, wallet_score)
            
            try:
                db.save_transaction(tx_data)
            except Exception as e:
                logger.error(f"Error guardando transacción en BD: {e}")
            
            if self.scoring_system:
                self.scoring_system.update_score_on_trade(tx_data["wallet"], tx_data)
            if self.wallet_tracker and hasattr(self.wallet_tracker, 'register_transaction'):
                self.wallet_tracker.register_transaction(
                    tx_data["wallet"],
                    tx_data["token"],
                    tx_data["type"],
                    tx_data["amount_usd"]
                )
            logger.debug(f"Transacción procesada: {tx_data['wallet']} - {tx_data['token']} - ${tx_data['amount_usd']:.2f}")
            
            asyncio.create_task(self._verify_and_signal(token, wallet, tx_data, wallet_score))
        except Exception as e:
            logger.error(f"Error en process_transaction: {e}", exc_info=True)
    
    def _update_token_stats(self, token, wallet, tx_data, wallet_score):
        timestamp = tx_data.get("timestamp", time.time())
        tx_type = tx_data.get("type", "").upper()
        amount_usd = float(tx_data.get("amount_usd", 0))
        if token not in self.token_candidates:
            self.token_candidates[token] = {
                "wallets": set(),
                "transactions": [],
                "first_seen": timestamp,
                "last_update": timestamp,
                "volume_usd": 0,
                "buy_count": 0,
                "sell_count": 0,
                "high_quality_traders": set()
            }
        candidate = self.token_candidates[token]
        candidate["wallets"].add(wallet)
        if wallet_score >= 8.0:
            candidate["high_quality_traders"].add(wallet)
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

    def _calculate_transaction_confidence(self, transactions):
        if not transactions:
            return 0.0
        total = sum(tx.get("amount_usd", 0) for tx in transactions)
        avg = total / len(transactions)
        if avg >= 10000:
            return 1.0
        elif avg >= 5000:
            return 0.9
        elif avg >= 3000:
            return 0.8
        elif avg >= 2000:
            return 0.7
        elif avg >= 1000:
            return 0.6
        else:
            return 0.5

    async def _verify_and_signal(self, token, wallet, tx_data, wallet_score):
        try:
            if token in self.watched_tokens:
                logger.debug(f"Token {token} ya está en seguimiento")
                return
            market_data = await self.get_token_market_data(token)
            market_cap = market_data.get("market_cap", 0)
            volume = market_data.get("volume", 0)
            mcap_threshold = 100000
            volume_threshold = 200000
            if market_cap >= mcap_threshold and volume >= volume_threshold:
                logger.info(f"⚡ Señal inmediata: {token} cumple umbrales - MC: ${market_cap/1000:.1f}K, Vol: ${volume/1000:.1f}K")
                await self._generate_token_signal(token, wallet, wallet_score, market_data)
            else:
                missing_criteria = []
                if market_cap < mcap_threshold:
                    missing_criteria.append(f"Market Cap (${market_cap/1000:.1f}K < $100K)")
                if volume < volume_threshold:
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
    
    async def get_token_market_data(self, token):
        result = {"market_cap": 0, "volume": 0, "price": 0}
        try:
            # Usar DexScreener para obtener datos de mercado
            if hasattr(self, 'market_metrics') and self.market_metrics:
                token_data = await self.market_metrics.fetch_token_data(token)
                if token_data:
                    result = token_data
        except Exception as e:
            logger.error(f"Error obteniendo datos de mercado para {token}: {e}")
        return result

    async def periodic_monitoring(self):
        monitor_interval = 60
        max_monitoring_time = 3600 * 4
        while True:
            try:
                now = time.time()
                if now - self.last_monitoring_time < monitor_interval:
                    await asyncio.sleep(1)
                    continue
                self.last_monitoring_time = now
                tokens_to_remove = []
                for token, data in self.monitored_tokens.items():
                    if now - data["last_check"] < monitor_interval:
                        continue
                    if now - data["first_seen"] > max_monitoring_time:
                        tokens_to_remove.append(token)
                        logger.debug(f"Token {token} eliminado del monitoreo: tiempo máximo excedido")
                        continue
                    self.monitored_tokens[token]["last_check"] = now
                    market_data = await self.get_token_market_data(token)
                    market_cap = market_data.get("market_cap", 0)
                    volume = market_data.get("volume", 0)
                    if market_cap >= 100000 and volume >= 200000:
                        logger.info(f"✅ Token monitoreado {token} ahora cumple umbrales - MC: ${market_cap/1000:.1f}K, Vol: ${volume/1000:.1f}K")
                        await self._generate_token_signal(token, data["wallet"], data["wallet_score"], market_data)
                        tokens_to_remove.append(token)
                    else:
                        missing = []
                        if market_cap < 100000:
                            missing.append(f"MC: ${market_cap/1000:.1f}K/$100K")
                        if volume < 200000:
                            missing.append(f"Vol: ${volume/1000:.1f}K/$200K")
                        logger.debug(f"Token {token} continúa en monitoreo - No cumple: {', '.join(missing)}")
                for token in tokens_to_remove:
                    if token in self.monitored_tokens:
                        del self.monitored_tokens[token]
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Error en monitoreo periódico: {e}", exc_info=True)
                await asyncio.sleep(5)

    async def _generate_token_signal(self, token, wallet, wallet_score, market_data):
        try:
            if token in self.watched_tokens:
                logger.debug(f"Ignorando señal duplicada para {token}")
                return
            now = time.time()
            extended_analysis = {}  # Puedes extender este análisis si se desea
            candidate = self.token_candidates.get(token, {
                "wallets": {wallet},
                "high_quality_traders": {wallet} if wallet_score >= 8.0 else set(),
                "buy_count": 1,
                "volume_usd": market_data.get("volume", 0),
                "transactions": []
            })
            initial_price = market_data.get("price", 0)
            market_cap = market_data.get("market_cap", 0)
            token_name = market_data.get("name", "")
            num_wallets = len(candidate.get("wallets", []))
            tx_amount_factor = self._calculate_transaction_confidence(candidate.get("transactions", []))
            base_confidence = 0.5 + (num_wallets * 0.02)
            high_quality_factor = 0.05 * len(candidate.get("high_quality_traders", []))
            confidence = min(0.95, base_confidence + tx_amount_factor + high_quality_factor)
            if confidence >= 0.9:
                signal_level = "S"
            elif confidence >= 0.8:
                signal_level = "A"
            elif confidence >= 0.6:
                signal_level = "B"
            else:
                signal_level = "C"
            trader_count = num_wallets
            signal_id = db.save_signal(token, trader_count, confidence, initial_price, market_cap, market_data.get("volume", 0))
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
            if len(self.recent_signals) > 20:
                self.recent_signals = self.recent_signals[-20:]
            self.watched_tokens.add(token)
            token_type = "🔴 TOKEN PUMP" if token.endswith("pump") else ""
            tx_velocity = len(candidate.get("transactions", [])) / (now - candidate.get("first_seen", now) + 1) * 60  # tx/min
            send_enhanced_signal(
                token=token,
                confidence=confidence,
                tx_velocity=tx_velocity,
                traders=list(candidate.get("wallets", {wallet})),
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

    def get_active_candidates_count(self):
        return len(self.token_candidates)

    def get_stats(self) -> dict:
        now = time.time()
        return {
            "active_tokens": len(self.token_candidates),
            "watched_tokens": len(self.watched_tokens),
            "monitored_tokens": len(self.monitored_tokens),
            "signals_today": len([s for s in self.recent_signals if now - s["timestamp"] < 86400]),
            "high_confidence_signals": len([s for s in self.recent_signals if s["confidence"] >= 0.8]),
        }
