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

        # Inicializar DexScreener como fuente alternativa si está configurado
        if hasattr(self, 'dexscreener_client'):
            pass  # Se supone que se inicializa externamente si se necesita
        else:
            self.dexscreener_client = None

        self.performance_tracker = None
        self.token_candidates = {}
        self.recent_signals = []
        self.last_signal_check = time.time()
        self.watched_tokens = set()
        self.wallet_tracker = None  # Se asigna desde main.py si es necesario

        # Inicializar puntajes por tipo de token
        self._init_token_type_scores()

    def _init_token_type_scores(self):
        # Define multiplicadores según tipo de token (puedes ajustar estos valores)
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
            
            # Si el token no existe en candidates, lo inicializamos
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
            
            if amount_usd > float(Config.get("HIGH_VOLUME_THRESHOLD", 5000)) and candidate["whale_activity"]:
                logger.info(f"Alto volumen detectado en {token}: ${amount_usd:.2f}")
                asyncio.create_task(self._process_candidates())
                
            logger.debug(f"Transacción procesada: {token} | {wallet} | ${amount_usd:.2f} | {tx_type}")
        except Exception as e:
            logger.error(f"Error procesando transacción en SignalLogic: {e}", exc_info=True)

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
                
                tracked_wallets = set(self.wallet_tracker.get_wallets()) if self.wallet_tracker else set()
                known_traders = set()
                for tx in recent_txs:
                    if tx.get("wallet") in tracked_wallets and tx.get("type") == "BUY":
                        known_traders.add(tx.get("wallet"))
                if len(known_traders) < 2:
                    continue
                logger.info(f"Token {token} tiene {len(known_traders)} traders conocidos")
                
                trader_count = len(data["wallets"])
                volume_usd = sum(tx["amount_usd"] for tx in recent_txs)
                buy_txs = [tx for tx in recent_txs if tx.get("type") == "BUY"]
                buy_ratio = len(buy_txs) / max(1, len(recent_txs))
                timestamps = [tx["timestamp"] for tx in recent_txs]
                tx_velocity = len(recent_txs) / max(1, (max(timestamps) - min(timestamps)) / 60) if len(timestamps) >= 2 else 0
                
                wallet_scores = [self.scoring_system.get_score(w) for w in known_traders]
                
                is_pump_token = token.endswith('pump')
                token_type = "meme" if is_pump_token else "standard"
                
                market_data = None
                if is_pump_token and self.dexscreener_client:
                    market_data = await self.dexscreener_client.fetch_token_data(token)
                if not market_data:
                    market_data = await self.get_token_market_data(token)
                
                volume_growth = market_data.get("volume_growth", {}).get("growth_5m", 0)
                confidence = self.scoring_system.compute_confidence(
                    wallet_scores=wallet_scores,
                    volume_1h=volume_usd,
                    market_cap=market_data.get("market_cap", 0),
                    recent_volume_growth=volume_growth,
                    token_type=token_type,
                    whale_activity=data.get("whale_activity", False),
                    tx_velocity=tx_velocity
                )
                
                elite_traders = sum(1 for score in wallet_scores if score > 9.0)
                if elite_traders > 0:
                    confidence = min(1.0, confidence * 1.2)
                
                candidate = {
                    "token": token,
                    "confidence": confidence,
                    "trader_count": len(known_traders),
                    "volume_usd": volume_usd,
                    "recent_transactions": recent_txs,
                    "tx_velocity": tx_velocity,
                    "buy_ratio": buy_ratio,
                    "token_type": token_type,
                    "market_data": market_data,
                    "elite_trader_count": elite_traders,
                    "high_quality_count": sum(1 for score in wallet_scores if score > 7.5),
                    "known_traders": list(known_traders)
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
                logger.info(f"Límite de señales alcanzado: {recent_signals_count}/{signal_throttling}")
                return
            if self.recent_signals and now - self.recent_signals[-1]["timestamp"] < 180:
                return
            min_confidence = float(Config.get("MIN_CONFIDENCE_THRESHOLD", 0.3))
            qualifying = [c for c in candidates if c["confidence"] >= min_confidence]
            if not qualifying:
                return
            best_candidate = qualifying[0]
            token = best_candidate["token"]
            for sig in self.recent_signals:
                if sig["token"] == token and now - sig["timestamp"] < 3600:
                    return
            confidence = best_candidate["confidence"]
            trader_count = best_candidate["trader_count"]
            trader_names = [self.scoring_system.get_trader_name_from_wallet(w) for w in best_candidate.get("known_traders", [])]
            tx_velocity = best_candidate["tx_velocity"]
            buy_ratio = best_candidate["buy_ratio"]
            token_type = best_candidate["token_type"]
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
                "is_meme": 1 if token_type == "meme" else 0,
                "window_seconds": float(Config.get("SIGNAL_WINDOW_SECONDS", 540)),
                "known_traders": trader_names
            }
            
            db.save_signal_features(signal_id, token, features)
            if self.performance_tracker:
                signal_info = {
                    "confidence": confidence,
                    "traders_count": trader_count,
                    "total_volume": best_candidate["volume_usd"],
                    "signal_id": signal_id,
                    "known_traders": trader_names
                }
                self.performance_tracker.add_signal(token, signal_info)
            signal_data = {
                "token": token,
                "confidence": confidence,
                "timestamp": now,
                "signal_id": signal_id,
                "token_type": token_type,
                "tx_velocity": tx_velocity,
                "buy_ratio": buy_ratio,
                "known_traders": trader_names
            }
            self.recent_signals.append(signal_data)
            if len(self.recent_signals) > 20:
                self.recent_signals = self.recent_signals[-20:]
            self.watched_tokens.add(token)
            from telegram_utils import send_telegram_message
            traders_info = ", ".join(trader_names[:5])
            if len(trader_names) > 5:
                traders_info += f" y {len(trader_names)-5} más"
            token_type_tag = "🔴 PUMP TOKEN" if token.endswith("pump") else ""
            msg = (
                f"🚨 *SEÑAL DETECTADA*\n\n"
                f"Token: `{token}`\n"
                f"Confianza: `{confidence:.2f}`\n"
                f"Velocidad TX: `{tx_velocity:.2f}` tx/min\n"
                f"Traders: {traders_info}\n"
                f"{token_type_tag}\n\n"
                f"🔗 *Exploradores:*\n"
                f"• [Solscan](https://solscan.io/token/{token})\n"
                f"• [Birdeye](https://birdeye.so/token/{token}?chain=solana)\n"
            )
            send_telegram_message(msg)
            logger.info(f"Señal generada para {token} con confianza {confidence:.2f}")
        except Exception as e:
            logger.error(f"Error en _generate_signals: {e}", exc_info=True)

    async def get_token_market_data(self, token):
        """
        Obtiene datos de mercado utilizando múltiples APIs hasta obtener datos válidos.
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
        data = None
        # Intentar con Helius
        if self.helius_client:
            try:
                data = self.helius_client.get_token_data(token)
                if data and data.get("market_cap", 0) > 0:
                    source = "helius"
                    logger.debug(f"Datos de {token} obtenidos de Helius")
            except Exception as e:
                logger.warning(f"Error API Helius para {token}: {str(e)[:100]}")
        # Intentar con GMGN
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
        # Intentar con DexScreener
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
                "estimated": True,
                "source": "default"
            }
            source = "default"
            logger.info(f"Usando datos predeterminados para {token} - APIs fallaron")
        data["source"] = source
        return data
