import time
import asyncio
import logging
from config import Config
import db

logger = logging.getLogger("signal_logic")

class SignalLogic:
    def __init__(self, scoring_system=None, helius_client=None, gmgn_client=None, rugcheck_api=None, ml_predictor=None, pattern_detector=None):
        """
        Inicializa la lógica de señales.
        Se inyectan clientes para Helius y GMGN para obtener datos de mercado.
        """
        self.scoring_system = scoring_system
        self.helius_client = helius_client
        self.gmgn_client = gmgn_client
        self.rugcheck_api = rugcheck_api
        self.ml_predictor = ml_predictor
        self.pattern_detector = pattern_detector
        self.performance_tracker = None
        self.token_candidates = {}  # Diccionario: token -> datos (wallets, transactions, etc.)
        self.recent_signals = []    # Lista de señales generadas: (token, timestamp, confidence, signal_id)
        self.last_signal_check = time.time()
        self.last_cleanup = time.time()
        self.watched_tokens = set()  # Tokens que ya tienen señal en seguimiento

    def process_transaction(self, tx_data):
        """
        Procesa una transacción para actualizar los candidatos de señales.
        """
        try:
            if not tx_data:
                logger.warning("Ignorando tx_data vacío en process_transaction")
                return

            token = tx_data.get("token")
            wallet = tx_data.get("wallet")
            amount_usd = tx_data.get("amount_usd", 0)
            tx_type = tx_data.get("type")
            
            if not token or not wallet:
                logger.warning(f"Transacción sin token o wallet, ignorando: {tx_data}")
                return

            # Mantener umbral mínimo actual
            min_tx_usd = float(Config.get("MIN_TRANSACTION_USD", 100))
            if amount_usd < min_tx_usd:
                logger.debug(f"Transacción ignorada por monto bajo: ${amount_usd:.2f} < ${min_tx_usd}")
                return

            timestamp = tx_data.get("timestamp", int(time.time()))
            tx_data["timestamp"] = timestamp

            logger.info(f"Procesando transacción: wallet {wallet[:8]}... -> token {token[:8]}... [{tx_type}] ${amount_usd:.2f}")

            # Inicializar datos del token si no existe
            if token not in self.token_candidates:
                self.token_candidates[token] = {
                    "wallets": set(),
                    "transactions": [],
                    "last_update": timestamp,
                    "volume_usd": 0,
                    "first_seen": timestamp,
                    "whale_activity": False,
                    "transaction_rate": 0.0,
                    "consecutive_buys": 0,   # Seguimiento de compras consecutivas
                    "buy_volume": 0.0,       # Volumen de compras
                    "sell_volume": 0.0       # Volumen de ventas
                }
                logger.info(f"Nuevo token detectado: {token}")

            candidate = self.token_candidates[token]
            candidate["wallets"].add(wallet)
            candidate["transactions"].append(tx_data)
            candidate["last_update"] = timestamp
            candidate["volume_usd"] += amount_usd

            # Actualizar volúmenes y contador de compras consecutivas
            if tx_type == "BUY":
                candidate["buy_volume"] += amount_usd
                # Contar compras consecutivas
                if len(candidate["transactions"]) > 1:
                    prev_tx = candidate["transactions"][-2]
                    if prev_tx.get("type") == "BUY":
                        candidate["consecutive_buys"] += 1
                    else:
                        candidate["consecutive_buys"] = 1
                else:
                    candidate["consecutive_buys"] = 1
            elif tx_type == "SELL":
                candidate["sell_volume"] += amount_usd
                candidate["consecutive_buys"] = 0

            # Detección refinada de actividad de ballenas: si el monto supera un umbral, se marca como actividad de ballena
            if amount_usd > 5000:
                candidate["whale_activity"] = True
                logger.info(f"🐋 Actividad de ballena detectada en {token[:8]}... monto: ${amount_usd:.2f}")

            # Actualizar tasa de transacciones (tx/min)
            txs = candidate["transactions"]
            if len(txs) > 1:
                time_span = max(txs[-1]["timestamp"] - txs[0]["timestamp"], 1)
                tx_rate = len(txs) / (time_span / 60)
                candidate["transaction_rate"] = tx_rate
                if tx_rate > 20:
                    logger.info(f"⚡ Alta velocidad de transacciones en {token[:8]}... {tx_rate:.1f} tx/min")

            logger.info(f"Actualización de candidato: token={token[:8]}..., traders={len(candidate['wallets'])}, txs={len(candidate['transactions'])}, vol=${candidate['volume_usd']:.2f}, tx_rate={candidate.get('transaction_rate', 0):.1f}, consecutive_buys={candidate['consecutive_buys']}")

            # Limpieza periódica de transacciones antiguas
            now = time.time()
            if now - self.last_cleanup > 3600:
                self._cleanup_old_data()
                self.last_cleanup = now

        except Exception as e:
            logger.error(f"Error en process_transaction: {e}", exc_info=True)

    def _cleanup_old_data(self):
        """
        Elimina transacciones de más de 24 horas de antigüedad.
        """
        now = time.time()
        cutoff = now - 86400  # 24 horas
        for token in list(self.token_candidates.keys()):
            original_count = len(self.token_candidates[token]["transactions"])
            self.token_candidates[token]["transactions"] = [tx for tx in self.token_candidates[token]["transactions"] if tx["timestamp"] > cutoff]
            cleaned = original_count - len(self.token_candidates[token]["transactions"])
            if cleaned > 0:
                logger.debug(f"Limpieza: {cleaned} transacciones eliminadas para token {token[:8]}...")

    def _classify_token_type(self, token, recent_txs, market_data):
        """
        Clasifica el token de manera más sofisticada utilizando:
          - Palabras clave para memecoins.
          - Datos de clientes externos (GMGN, Helius).
        """
        token_type = None
        # Lista de keywords para memecoins
        meme_keywords = ["pepe", "doge", "shib", "inu", "moon", "elon", "wojak"]
        
        # Utilizar GMGN para detectar memecoins
        if self.gmgn_client and self.gmgn_client.is_memecoin(token):
            token_type = "meme"
        else:
            # Buscar keywords en los nombres de token en transacciones recientes
            for tx in recent_txs:
                token_name = tx.get("token_name", "").lower()
                if any(keyword in token_name for keyword in meme_keywords):
                    token_type = "meme"
                    break
        return token_type

    def _should_generate_immediate_signal(self, token, candidate):
        """
        Determina si se debe generar una señal inmediata para casos excepcionales:
         - Actividad de ballenas combinada con alta velocidad de transacciones.
         - Compras consecutivas superiores al umbral.
        """
        if candidate.get("whale_activity") and candidate.get("transaction_rate", 0) > 15:
            return True
        if candidate.get("consecutive_buys", 0) >= int(Config.get("CONSECUTIVE_BUY_THRESHOLD", 3)):
            return True
        return False

    def _check_specific_token(self, token, candidate):
        """
        Verifica condiciones específicas para ciertos tokens de alta prioridad.
        Ejemplo: si el token contiene ciertas palabras clave o cumple condiciones especiales de mercado.
        """
        specific_keywords = Config.get("SPECIFIC_TOKEN_KEYWORDS", "special,priority").split(',')
        token_lower = token.lower()
        if any(keyword.strip() in token_lower for keyword in specific_keywords):
            logger.info(f"Token {token[:8]}... coincide con palabra clave específica para alta prioridad")
            return True
        return False

    async def _process_candidates(self):
        """
        Procesa los tokens candidatos para generar señales.
        """
        try:
            now = time.time()
            window_seconds = float(Config.get("SIGNAL_WINDOW_SECONDS", 540))
            cutoff = now - window_seconds
            candidates = []

            logger.info(f"Procesando {len(self.token_candidates)} candidatos para señales...")

            for token, data in list(self.token_candidates.items()):
                try:
                    recent_txs = [tx for tx in data["transactions"] if tx["timestamp"] > cutoff]
                    if not recent_txs:
                        logger.debug(f"Token {token}: sin transacciones recientes")
                        continue

                    if token in self.watched_tokens:
                        logger.debug(f"Token {token} ya en seguimiento")
                        continue

                    trader_count = len(data["wallets"])
                    volume_usd = sum(tx["amount_usd"] for tx in recent_txs)
                    buy_txs = [tx for tx in recent_txs if tx.get("type") == "BUY"]
                    buy_percentage = len(buy_txs) / max(1, len(recent_txs))

                    timestamps = [tx["timestamp"] for tx in recent_txs]
                    if len(timestamps) > 1:
                        tx_timespan = max(timestamps) - min(timestamps)
                        tx_timespan = max(1, tx_timespan)
                        tx_velocity = len(recent_txs) / (tx_timespan / 60)
                    else:
                        tx_velocity = 1.0

                    logger.info(f"Token {token[:8]}...: traders={trader_count}, vol=${volume_usd:.2f}, buy_ratio={buy_percentage:.2f}, tx_velocity={tx_velocity:.2f}, consecutive_buys={data.get('consecutive_buys', 0)}")

                    # Verificar condiciones mínimas, manteniendo umbrales actuales
                    min_traders = int(Config.get("MIN_TRADERS_FOR_SIGNAL", 1))
                    min_volume = float(Config.get("MIN_VOLUME_USD", 1000))
                    min_buy_percentage = float(Config.get("MIN_BUY_PERCENTAGE", 0.5))
                    min_velocity = float(Config.get("MIN_TX_VELOCITY", 0.3))

                    # Caso especial si hay actividad de ballenas, alta velocidad o muchas compras consecutivas
                    is_special_case = data.get("whale_activity", False) or tx_velocity > 15 or data.get("consecutive_buys", 0) >= int(Config.get("CONSECUTIVE_BUY_THRESHOLD", 3))
                    if not is_special_case:
                        if trader_count < min_traders:
                            logger.debug(f"Token {token} descartado: traders insuficientes ({trader_count} < {min_traders})")
                            continue
                        if volume_usd < min_volume:
                            logger.debug(f"Token {token} descartado: volumen insuficiente (${volume_usd:.2f} < ${min_volume})")
                            continue
                        if buy_percentage < min_buy_percentage:
                            logger.debug(f"Token {token} descartado: bajo buy ratio ({buy_percentage:.2f} < {min_buy_percentage})")
                            continue
                        if tx_velocity < min_velocity:
                            logger.debug(f"Token {token} descartado: baja velocidad de txs ({tx_velocity:.2f} < {min_velocity})")
                            continue
                    else:
                        logger.info(f"Caso especial detectado para {token[:8]}... (whale={data.get('whale_activity')}, tx_velocity={tx_velocity:.1f}, consecutive_buys={data.get('consecutive_buys')})")

                    # Obtener datos de mercado
                    market_data = await self.get_token_market_data(token)
                    market_cap = market_data.get("market_cap", 0)
                    vol_growth = market_data.get("volume_growth", {})

                    # Clasificar token de forma sofisticada
                    token_type = self._classify_token_type(token, recent_txs, market_data)

                    # Calcular puntuaciones de traders
                    trader_scores = [self.scoring_system.get_score(w) for w in data["wallets"]]

                    # Calcular confianza base
                    confidence = self.scoring_system.compute_confidence(
                        wallet_scores=trader_scores,
                        volume_1h=market_data.get("volume", 0),
                        market_cap=market_cap,
                        recent_volume_growth=vol_growth.get("growth_5m", 0),
                        token_type=token_type
                    )

                    # Optimización de la fórmula de confianza:
                    # Boosting por actividad de ballenas (1.2x)
                    if data.get("whale_activity"):
                        confidence *= 1.2
                        logger.debug(f"Boost de ballenas aplicado: confidence={confidence:.2f}")
                    # Boosting para alta velocidad de transacciones (1.15x)
                    if tx_velocity > 20:
                        confidence *= 1.15
                        logger.debug(f"Boost de tx_velocity aplicado: confidence={confidence:.2f}")
                    # Boosting para compras consecutivas (1.1x)
                    if data.get("consecutive_buys", 0) >= int(Config.get("CONSECUTIVE_BUY_THRESHOLD", 3)):
                        confidence *= 1.1
                        logger.debug(f"Boost de compras consecutivas aplicado: confidence={confidence:.2f}")

                    # Verificar si se debe generar señal inmediata (casos excepcionales)
                    if self._should_generate_immediate_signal(token, data) or self._check_specific_token(token, data):
                        logger.info(f"Generando señal inmediata para {token[:8]}... por condiciones excepcionales")

                    # Usar ML si está disponible
                    ml_prediction = 0.5
                    if self.ml_predictor and self.ml_predictor.model is not None:
                        features = {
                            "num_traders": trader_count,
                            "num_transactions": len(recent_txs),
                            "total_volume_usd": volume_usd,
                            "avg_volume_per_trader": volume_usd / max(1, trader_count),
                            "buy_ratio": buy_percentage,
                            "tx_velocity": tx_velocity,
                            "avg_trader_score": sum(trader_scores) / max(1, len(trader_scores)),
                            "max_trader_score": max(trader_scores) if trader_scores else 0,
                            "market_cap": market_cap,
                            "volume_1h": market_data.get("volume", 0),
                            "volume_growth_5m": vol_growth.get("growth_5m", 0),
                            "volume_growth_1h": vol_growth.get("growth_1h", 0),
                            "tx_rate": tx_velocity,
                            "whale_flag": 1 if data.get("whale_activity", False) else 0,
                            "is_meme": 1 if token_type == "meme" else 0
                        }
                        ml_prediction = self.ml_predictor.predict_success(features)
                        confidence = confidence * 0.7 + ml_prediction * 0.3
                        logger.info(f"ML prediction para {token[:8]}...: {ml_prediction:.2f}")

                    candidates.append({
                        "token": token,
                        "confidence": confidence,
                        "ml_prediction": ml_prediction,
                        "trader_count": trader_count,
                        "volume_usd": volume_usd,
                        "recent_transactions": recent_txs,
                        "market_cap": market_cap,
                        "volume_1h": market_data.get("volume", 0),
                        "volume_growth": vol_growth,
                        "buy_percentage": buy_percentage,
                        "trader_scores": trader_scores,
                        "initial_price": market_data.get("price", 0),
                        "data_source": market_data.get("source", "unknown"),
                        "tx_velocity": tx_velocity,
                        "token_type": token_type
                    })
                except Exception as e:
                    logger.error(f"Error procesando candidato {token}: {e}", exc_info=True)
            
            candidates.sort(key=lambda x: x["confidence"], reverse=True)
            signal_throttling = int(Config.get("SIGNAL_THROTTLING", 5))
            if len(candidates) > signal_throttling:
                candidates = candidates[:signal_throttling]
                logger.info(f"Throttling: seleccionados {signal_throttling} de {len(candidates)} candidatos")
            
            await self._generate_signals(candidates)
        except Exception as e:
            logger.error(f"Error en _process_candidates: {e}", exc_info=True)

    async def get_token_market_data(self, token):
        """
        Obtiene datos de mercado utilizando múltiples fuentes con fallback:
          1. Prioriza datos de Helius.
          2. Fallback a GMGN si Helius falla.
          3. Maneja errores específicos de la API y loggea detalles.
          4. Asegura que siempre se retornen valores predeterminados útiles.
        """
        data = None
        source = "none"
        # Intentar con Helius
        if self.helius_client:
            try:
                data = self.helius_client.get_token_data(token)
                if data and data.get("market_cap", 0) > 0:
                    source = "helius"
                    logger.debug(f"Datos de {token[:8]} obtenidos de Helius")
                else:
                    logger.debug(f"Datos de Helius para {token[:8]} insuficientes")
            except Exception as e:
                logger.error(f"Error API Helius para {token[:8]}: {e}", exc_info=True)
        # Fallback a GMGN
        if not data or data.get("market_cap", 0) == 0:
            if self.gmgn_client:
                try:
                    gmgn_data = self.gmgn_client.get_market_data(token)
                    if gmgn_data and gmgn_data.get("market_cap", 0) > 0:
                        data = gmgn_data
                        source = "gmgn"
                        logger.debug(f"Datos de {token[:8]} obtenidos de GMGN")
                    else:
                        logger.debug(f"Datos de GMGN para {token[:8]} insuficientes")
                except Exception as e:
                    logger.error(f"Error API GMGN para {token[:8]}: {e}", exc_info=True)
        # Estimar datos a partir de transacciones si no se obtuvieron datos
        if not data or data.get("market_cap", 0) == 0:
            if token in self.token_candidates:
                recent_txs = self.token_candidates[token].get("transactions", [])
                now = time.time()
                window_seconds = float(Config.get("SIGNAL_WINDOW_SECONDS", 540))
                cutoff = now - window_seconds
                recent_txs = [tx for tx in recent_txs if tx["timestamp"] > cutoff]
                if recent_txs:
                    volume_est = sum(tx["amount_usd"] for tx in recent_txs)
                    data = {
                        "price": 0,
                        "market_cap": 0,
                        "volume": volume_est,
                        "volume_growth": {"growth_5m": 0, "growth_1h": 0},
                        "estimated": True
                    }
                    source = "estimated"
                    logger.debug(f"Datos de {token[:8]} estimados a partir de transacciones")
        # Valores predeterminados en caso de no obtener datos
        if not data:
            data = {
                "price": 0,
                "market_cap": 0,
                "volume": 0,
                "volume_growth": {"growth_5m": 0, "growth_1h": 0},
                "estimated": True
            }
            source = "none"
            logger.debug(f"Datos por defecto usados para {token[:8]}")
        data["source"] = source
        return data

    async def _generate_signals(self, candidates):
        """
        Genera señales a partir de la lista de candidatos.
        """
        if not candidates:
            logger.info("No hay candidatos para generar señales")
            return
            
        signals_generated = 0
        min_confidence = float(Config.get("MIN_CONFIDENCE_THRESHOLD", 0.3))
        
        for candidate in candidates:
            if candidate["confidence"] < min_confidence:
                logger.debug(f"Candidato {candidate['token'][:8]} descartado por baja confianza: {candidate['confidence']:.2f} < {min_confidence}")
                continue
                
            try:
                signal_id = db.save_signal(
                    candidate["token"],
                    candidate["trader_count"],
                    candidate["confidence"],
                    candidate.get("initial_price", 0)
                )
                
                if "ml_prediction" in candidate:
                    ml_features = {
                        "num_traders": candidate["trader_count"],
                        "volume_usd": candidate["volume_usd"],
                        "buy_ratio": candidate["buy_percentage"],
                        "tx_velocity": candidate.get("tx_velocity", 0),
                        "market_cap": candidate["market_cap"],
                        "volume_1h": candidate["volume_1h"],
                        "volume_growth_5m": candidate["volume_growth"].get("growth_5m", 0),
                        "is_meme": 1 if candidate.get("token_type") == "meme" else 0,
                        "window_seconds": float(Config.get("SIGNAL_WINDOW_SECONDS", 540))
                    }
                    db.save_signal_features(signal_id, candidate["token"], ml_features)
                
                self.recent_signals.append((candidate["token"], time.time(), candidate["confidence"], signal_id))
                self.watched_tokens.add(candidate["token"])
                
                if self.performance_tracker:
                    signal_info = {
                        "confidence": candidate["confidence"],
                        "traders_count": candidate["trader_count"],
                        "total_volume": candidate["volume_usd"],
                        "signal_id": signal_id
                    }
                    self.performance_tracker.add_signal(candidate["token"], signal_info)
                
                self._send_signal_notification(candidate, signal_id)
                
                signals_generated += 1
                logger.info(f"✅ Señal generada para {candidate['token'][:8]} con confianza {candidate['confidence']:.2f}")
            except Exception as e:
                logger.error(f"Error al generar señal para {candidate['token'][:8]}: {e}", exc_info=True)

        if len(self.recent_signals) > 100:
            self.recent_signals = self.recent_signals[-100:]
            
        if signals_generated > 0:
            logger.info(f"Se generaron {signals_generated} señales en total")
        else:
            logger.info("No se generaron señales en este ciclo")

    def _send_signal_notification(self, signal_data, signal_id):
        """
        Envía notificación de la señal a Telegram.
        """
        from telegram_utils import send_telegram_message
        
        token = signal_data['token']
        emoji = "🔥"
        if signal_data.get("token_type") == "meme":
            emoji = "🚀"
        elif signal_data.get("confidence", 0) > 0.7:
            emoji = "💎"
        
        solscan_link = f"https://solscan.io/token/{token}"
        birdeye_link = f"https://birdeye.so/token/{token}?chain=solana"
        neobullx_link = f"https://neo.bullx.io/terminal?chainId=1399811149&address={token}"
        
        volume_growth_5m = signal_data.get("volume_growth", {}).get("growth_5m", 0) * 100
        tx_velocity = signal_data.get("tx_velocity", 0)
        market_cap = signal_data.get("market_cap", 0)
        
        if market_cap >= 1_000_000:
            mcap_str = f"${market_cap/1_000_000:.2f}M"
        elif market_cap > 0:
            mcap_str = f"${market_cap/1_000:.1f}K"
        else:
            mcap_str = "Desconocido"
        
        trader_scores = signal_data.get("trader_scores", [])
        num_traders = signal_data.get("trader_count", 0)
        high_score_traders = sum(1 for score in trader_scores if score > 7.0)
        
        message = (
            f"{emoji} *SEÑAL #{signal_id}* {emoji}\n\n"
            f"Token: `{token}`\n"
            f"Confianza: `{signal_data['confidence']:.2f}`\n"
            f"Traders: `{num_traders}` (high score: `{high_score_traders}`)\n"
            f"Market Cap: `{mcap_str}`\n"
            f"Vol 5m Growth: `{volume_growth_5m:.1f}%`\n"
            f"TX Velocity: `{tx_velocity:.1f}` tx/min\n\n"
            f"🔗 *Enlaces:*\n"
            f"• [Solscan]({solscan_link})\n"
            f"• [Birdeye]({birdeye_link})\n"
            f"• [NeoBullX]({neobullx_link})\n\n"
            f"_Se realizará seguimiento en intervalos definidos._"
        )
        send_telegram_message(message)

    async def check_signals_periodically(self):
        """
        Ejecuta la comprobación de señales de forma periódica.
        """
        while True:
            try:
                now = time.time()
                if now - self.last_signal_check > 60:
                    logger.info("Ejecutando verificación de señales...")
                    await self._process_candidates()
                    self.last_signal_check = now
            except Exception as e:
                logger.error(f"Error en check_signals_periodically: {e}", exc_info=True)
            await asyncio.sleep(10)

    def get_active_candidates_count(self):
        """
        Retorna el número de tokens candidatos activos.
        """
        now = time.time()
        window_seconds = float(Config.get("SIGNAL_WINDOW_SECONDS", 540))
        cutoff = now - window_seconds
        active_count = sum(1 for token, data in self.token_candidates.items() if data["last_update"] > cutoff)
        return active_count
