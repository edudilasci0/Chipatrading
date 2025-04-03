#!/usr/bin/env python3
# signal_logic.py - Lógica central para detección de señales en el bot de trading

import time
import asyncio
import logging
import math
from typing import Dict, Any, Optional
from config import Config
import db

# Importar módulos de análisis avanzado (ahora usando solo DexScreener)
from market_metrics import MarketMetricsAnalyzer
from token_analyzer import TokenAnalyzer
from trader_profiler import TraderProfiler
from telegram_utils import send_enhanced_signal

logger = logging.getLogger("signal_logic")

class SignalLogic:
    def __init__(self, scoring_system=None, rugcheck_api=None, ml_predictor=None,
                 pattern_detector=None, wallet_tracker=None, dexscreener_client=None):
        """
        Inicializa la clase con los parámetros actuales e instancia los módulos necesarios.
        Nota: Se eliminó 'helius_client' y WhaleDetector, ya que la fuente de mercado será DexScreener.
        """
        self.scoring_system = scoring_system
        self.rugcheck_api = rugcheck_api  # Puede ser None
        self.ml_predictor = ml_predictor
        self.pattern_detector = pattern_detector
        self.wallet_tracker = wallet_tracker
        self.dexscreener_client = dexscreener_client  # Nueva fuente de mercado
        self.token_candidates = {}
        self.recent_signals = []
        self.last_signal_check = time.time()
        self.watched_tokens = set()
        self.token_type_scores = {}
        self._init_token_type_scores()
        self.monitored_tokens = {}  # Para tokens que necesitan monitoreo continuo
        self.last_monitoring_time = time.time()

        # Instanciar módulos de análisis avanzado utilizando DexScreener
        self.market_metrics = MarketMetricsAnalyzer(dexscreener_client=self.dexscreener_client)
        self.token_analyzer = TokenAnalyzer(dexscreener_client=self.dexscreener_client)
        self.trader_profiler = TraderProfiler()
        
        self.performance_tracker = None  # Se asigna externamente
        
        asyncio.create_task(self.periodic_monitoring())

    def _init_token_type_scores(self):
        self.token_type_scores = {
            "meme": 1.2,
            "standard": 1.0
        }
    
    def process_transaction(self, tx_data):
        """
        Procesa una transacción entrante y actualiza las estadísticas del token.
        Se procesan todas las transacciones sin filtrar por traders elite.
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
            min_usd = float(Config.MIN_TRANSACTION_USD)
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
            
            # Verifica si esta transacción podría ser parte de una señal
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
        """
        Calcula un factor de confianza basado en los montos individuales de las transacciones.
        Prioriza transacciones mayores: $1K, $2K, $3K, $5K, $10K+.
        """
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
        """
        Verifica si una transacción cumple ciertos umbrales y, de ser así, genera una señal.
        """
        try:
            if token in self.watched_tokens:
                logger.debug(f"Token {token} ya está en seguimiento")
                return
            market_data = await self.get_token_market_data(token)
            market_cap = market_data.get("market_cap", 0)
            volume = market_data.get("volume", 0)
            mcap_threshold = float(Config.get("MCAP_THRESHOLD", "100000"))
            volume_threshold = float(Config.get("VOLUME_THRESHOLD", "200000"))
            
            # Logs detallados
            logger.info(f"Verificando token {token} - MC: ${market_cap/1000:.1f}K (umbral: ${mcap_threshold/1000:.1f}K), Vol: ${volume/1000:.1f}K (umbral: ${volume_threshold/1000:.1f}K)")
            
            meets_mcap = market_cap >= mcap_threshold
            meets_volume = volume >= volume_threshold
            if meets_mcap and meets_volume:
                logger.info(f"⚡ Señal inmediata: {token} cumple umbrales - MC: ${market_cap/1000:.1f}K, Vol: ${volume/1000:.1f}K")
                await self._generate_token_signal(token, wallet, wallet_score, market_data)
            else:
                missing_criteria = []
                if not meets_mcap:
                    missing_criteria.append(f"Market Cap (${market_cap/1000:.1f}K < ${mcap_threshold/1000:.1f}K)")
                if not meets_volume:
                    missing_criteria.append(f"Volumen (${volume/1000:.1f}K < ${volume_threshold/1000:.1f}K)")
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
        """
        Obtiene datos de mercado para un token usando DexScreener.
        """
        result = {"market_cap": 0, "volume": 0, "price": 0}
        try:
            if self.dexscreener_client:
                token_data = await self.dexscreener_client.fetch_token_data(token)
                if token_data:
                    result = token_data
        except Exception as e:
            logger.error(f"Error obteniendo datos de mercado para {token}: {e}")
        return result

    async def periodic_monitoring(self):
        monitor_interval = 60  # cada 60 segundos
        max_monitoring_time = 3600 * 4  # hasta 4 horas
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
                    mcap_threshold = 100000
                    volume_threshold = 200000
                    meets_mcap = market_cap >= mcap_threshold
                    meets_volume = volume >= volume_threshold
                    if meets_mcap and meets_volume:
                        logger.info(f"✅ Token monitoreado {token} ahora cumple umbrales - MC: ${market_cap/1000:.1f}K, Vol: ${volume/1000:.1f}K")
                        await self._generate_token_signal(token, data["wallet"], data["wallet_score"], market_data)
                        tokens_to_remove.append(token)
                    else:
                        missing = []
                        if not meets_mcap:
                            missing.append(f"MC: ${market_cap/1000:.1f}K/$100K")
                        if not meets_volume:
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
            extended_analysis = {}  # Se puede extender el análisis si es necesario
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
        """
        Retorna el número de tokens candidatos activos en seguimiento.
        """
        return len(self.token_candidates)

    def get_stats(self) -> dict:
        """
        Retorna estadísticas sobre el estado actual del sistema.
        """
        now = time.time()
        return {
            "active_tokens": len(self.token_candidates),
            "watched_tokens": len(self.watched_tokens),
            "monitored_tokens": len(self.monitored_tokens),
            "signals_today": len([s for s in self.recent_signals if now - s["timestamp"] < 86400]),
            "high_confidence_signals": len([s for s in self.recent_signals if s["confidence"] >= 0.8]),
        }
