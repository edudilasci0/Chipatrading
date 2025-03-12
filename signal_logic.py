import time
import asyncio
import logging
from config import Config
import db
from telegram_utils import send_telegram_message

logger = logging.getLogger("signal_logic")

class SignalLogic:
    def __init__(self, scoring_system=None, helius_client=None, gmgn_client=None, rugcheck_api=None, ml_predictor=None):
        """
        Inicializa la lógica de señales.
        Se inyecta helius_client y gmgn_client para obtener datos de mercado.
        """
        self.scoring_system = scoring_system
        self.helius_client = helius_client  
        self.gmgn_client = gmgn_client
        self.rugcheck_api = rugcheck_api
        self.ml_predictor = ml_predictor
        self.performance_tracker = None
        self.token_candidates = {}  # Diccionario con información de tokens: {token: {wallets, transactions, last_update, volume_usd, first_seen}}
        self.recent_signals = []    # Lista de tuplas: (token, timestamp, confidence, signal_id)
        self.last_signal_check = time.time()
        self.last_cleanup = time.time()

    async def check_signals_periodically(self, interval=30):
        """
        Ejecuta periódicamente el proceso de candidatos para generar señales.
        """
        while True:
            try:
                await self._process_candidates()
                await asyncio.sleep(interval)
            except Exception as e:
                logger.error(f"Error en check_signals_periodically: {e}", exc_info=True)
                await asyncio.sleep(interval)

    def process_transaction(self, tx_data):
        """
        Procesa una transacción para actualizar los candidatos de señales.
        
        Args:
            tx_data: Datos de la transacción
        """
        try:
            if not tx_data:
                logger.warning("Ignorando tx_data vacío en process_transaction")
                return

            token = tx_data.get("token")
            wallet = tx_data.get("wallet")
            amount_usd = tx_data.get("amount_usd", 0)

            if not token or not wallet:
                logger.warning(f"Transacción sin token o wallet, ignorando: {tx_data}")
                return

            # Validar monto mínimo
            min_tx_usd = float(Config.get("MIN_TRANSACTION_USD", 200))
            if amount_usd < min_tx_usd:
                logger.debug(f"Transacción ignorada por monto bajo: ${amount_usd:.2f} < ${min_tx_usd}")
                return

            timestamp = tx_data.get("timestamp", int(time.time()))
            tx_data["timestamp"] = timestamp  # Asegurar que hay timestamp

            # Log para seguimiento
            logger.info(f"SignalLogic.process_transaction: {wallet[:8]}... -> {token[:8]}... [{tx_data.get('type')}] ${amount_usd:.2f}")

            # Inicializar estructura para token si no existe
            if token not in self.token_candidates:
                self.token_candidates[token] = {
                    "wallets": set(),
                    "transactions": [],
                    "last_update": timestamp,
                    "volume_usd": 0,
                    "first_seen": timestamp
                }
                logger.info(f"Nuevo token detectado: {token}")

            # Actualizar datos
            self.token_candidates[token]["wallets"].add(wallet)
            self.token_candidates[token]["transactions"].append(tx_data)
            self.token_candidates[token]["last_update"] = timestamp
            self.token_candidates[token]["volume_usd"] += amount_usd

            # Log de candidato actualizado
            candidate = self.token_candidates[token]
            logger.info(f"Candidato actualizado: token={token[:8]}..., wallets={len(candidate['wallets'])}, " +
                        f"transactions={len(candidate['transactions'])}, volume_usd=${candidate['volume_usd']:.2f}")

            # Limpieza periódica de transacciones antiguas (más de 24h)
            now = time.time()
            if now - self.last_cleanup > 3600:  # Cada hora
                self._cleanup_old_data()
                self.last_cleanup = now

        except Exception as e:
            logger.error(f"Error procesando transacción en SignalLogic: {e}", exc_info=True)

    def _cleanup_old_data(self):
        """
        Limpia las transacciones de los candidatos que sean más antiguas de 24 horas.
        """
        cutoff = time.time() - 86400  # 24 horas
        for token, data in self.token_candidates.items():
            original_count = len(data["transactions"])
            data["transactions"] = [tx for tx in data["transactions"] if tx["timestamp"] > cutoff]
            if len(data["transactions"]) < original_count:
                logger.info(f"Limpieza: {original_count - len(data['transactions'])} transacciones eliminadas para token {token[:8]}...")

    async def _process_candidates(self):
        """
        Procesa los tokens candidatos para generar señales con criterios mejorados.
        """
        now = time.time()
        window_seconds = float(Config.get("SIGNAL_WINDOW_SECONDS", 540))
        cutoff = now - window_seconds
        candidates = []
        
        logger.info(f"Procesando {len(self.token_candidates)} candidatos para señales...")

        for token, data in list(self.token_candidates.items()):
            try:
                # Obtener transacciones recientes
                recent_txs = [tx for tx in data["transactions"] if tx["timestamp"] > cutoff]
                if not recent_txs:
                    logger.debug(f"Token {token}: sin transacciones recientes, omitiendo")
                    continue

                # Verificar si este token ya tuvo señal recientemente
                token_recently_signaled = any(
                    t == token and ts > (now - 3600) for t, ts, _, _ in self.recent_signals
                )
                if token_recently_signaled:
                    logger.debug(f"Token {token} ya generó señal recientemente, omitiendo")
                    continue

                # Calcular métricas básicas
                trader_count = len(data["wallets"])
                volume_usd = sum(tx["amount_usd"] for tx in recent_txs)

                # Calcular métricas avanzadas
                buy_txs = [tx for tx in recent_txs if tx["type"] == "BUY"]
                sell_txs = [tx for tx in recent_txs if tx["type"] == "SELL"]
                buy_percentage = len(buy_txs) / max(1, len(recent_txs))
                
                # Calcular velocidad de transacciones (txs por minuto)
                timestamps = [tx["timestamp"] for tx in recent_txs]
                if len(timestamps) > 1:
                    tx_timespan = max(timestamps) - min(timestamps)
                    tx_timespan = max(1, tx_timespan)
                    tx_velocity = len(recent_txs) / (tx_timespan / 60)
                else:
                    tx_velocity = 1.0

                logger.info(f"Token {token[:8]}...: {trader_count} traders, ${volume_usd:.2f} vol, " +
                            f"{buy_percentage:.2f} buy ratio, {tx_velocity:.2f} tx/min")

                # Verificar condiciones mínimas
                min_traders = int(Config.get("MIN_TRADERS_FOR_SIGNAL", 2))
                min_volume = float(Config.get("MIN_VOLUME_USD", 2000))
                min_buy_percentage = float(Config.get("MIN_BUY_PERCENTAGE", 0.7))
                min_velocity = float(Config.get("MIN_TX_VELOCITY", 0.5))
                if trader_count < min_traders:
                    logger.debug(f"Token {token} descartado: pocos traders ({trader_count} < {min_traders})")
                    continue
                if volume_usd < min_volume:
                    logger.debug(f"Token {token} descartado: poco volumen (${volume_usd:.2f} < ${min_volume})")
                    continue
                if buy_percentage < min_buy_percentage:
                    logger.debug(f"Token {token} descartado: bajo ratio de compras ({buy_percentage:.2f} < {min_buy_percentage})")
                    continue
                if tx_velocity < min_velocity:
                    logger.debug(f"Token {token} descartado: baja velocidad de txs ({tx_velocity:.2f} < {min_velocity})")
                    continue

                # Obtener datos de mercado mediante múltiples fuentes
                market_data = await self.get_token_market_data(token)
                market_cap = market_data.get("market_cap", 0)
                vol_growth = market_data.get("volume_growth", {})
                tx_rate = len(recent_txs) / window_seconds

                token_type = None
                if self.gmgn_client and self.gmgn_client.is_memecoin(token):
                    token_type = "meme"
                elif vol_growth.get("growth_5m", 0) > 0.2 and market_cap < 5_000_000:
                    token_type = "meme"

                trader_scores = [self.scoring_system.get_score(w) for w in data["wallets"]]
                confidence = self.scoring_system.compute_confidence(
                    wallet_scores=trader_scores,
                    volume_1h=market_data.get("volume", 0),
                    market_cap=market_cap,
                    recent_volume_growth=vol_growth.get("growth_5m", 0),
                    token_type=token_type
                )
                
                # Ajustar para detectar daily runners o actividad de ballenas
                config = Config.MEMECOIN_CONFIG
                is_memecoin = (tx_rate > config["TX_RATE_THRESHOLD"] and 
                               vol_growth.get("growth_5m", 0) > config["VOLUME_GROWTH_THRESHOLD"] and 
                               market_cap < 10_000_000)
                whale_threshold = 10000
                if recent_txs and max(tx["amount_usd"] for tx in recent_txs) > whale_threshold:
                    is_memecoin = True

                if is_memecoin:
                    confidence *= 1.5

                candidate_data = {
                    "token": token,
                    "confidence": confidence,
                    "ml_prediction": 0.5,  # Valor base; se ajusta con ML si está disponible
                    "trader_count": trader_count,
                    "volume_usd": volume_usd,
                    "recent_transactions": recent_txs,
                    "market_cap": market_cap,
                    "volume_1h": market_data.get("volume", 0),
                    "volume_growth": vol_growth,
                    "buy_percentage": buy_percentage,
                    "trader_scores": trader_scores,
                    "initial_price": market_data.get("price", 0),
                    "data_source": market_data.get("source", "unknown")
                }
                candidates.append(candidate_data)
            except Exception as e:
                logger.error(f"Error procesando candidato {token}: {e}")

        candidates.sort(key=lambda x: x["confidence"], reverse=True)
        await self._generate_signals(candidates)

    async def _generate_signals(self, candidates):
        """
        Genera señales para los candidatos que cumplen los criterios.
        """
        now = time.time()
        min_confidence = float(Config.get("MIN_CONFIDENCE_THRESHOLD", 0.3))
        throttling = int(Config.get("SIGNAL_THROTTLING", 10))
        
        signals_last_hour = db.count_signals_last_hour()
        if signals_last_hour >= throttling:
            logger.warning(f"Límite de señales alcanzado ({signals_last_hour}/{throttling}), no generando más señales")
            return
        
        signals_generated = 0
        for candidate in candidates:
            token = candidate["token"]
            confidence = candidate["confidence"]
            trader_count = candidate["trader_count"]
            initial_price = candidate.get("initial_price", 0)
            
            already_signaled = any(
                t == token for t, ts, _, _ in self.recent_signals if now - ts < 3600
            )
            if already_signaled:
                logger.debug(f"Token {token} ya ha generado señal recientemente, omitiendo")
                continue
            
            if confidence < min_confidence:
                logger.debug(f"Token {token} con baja confianza ({confidence:.2f}), omitiendo")
                continue
            
            try:
                signal_id = db.save_signal(token, trader_count, confidence, initial_price)
                self.recent_signals.append((token, now, confidence, signal_id))
                if len(self.recent_signals) > 100:
                    self.recent_signals = self.recent_signals[-100:]
                message = self._format_signal_message(candidate, signal_id)
                send_telegram_message(message)
                if self.performance_tracker:
                    signal_info = {
                        "confidence": confidence,
                        "traders_count": trader_count,
                        "total_volume": candidate.get("volume_usd", 0),
                        "signal_id": signal_id
                    }
                    self.performance_tracker.add_signal(token, signal_info)
                signals_generated += 1
                logger.info(f"✅ Señal generada para {token} con confianza {confidence:.2f}")
                if signals_generated >= (throttling - signals_last_hour):
                    break
            except Exception as e:
                logger.error(f"Error generando señal para {token}: {e}")

    async def get_token_market_data(self, token):
        """
        Obtiene datos de mercado utilizando múltiples fuentes con fallback.
        Prioridad: Helius -> GMGN -> Estimado.
        """
        data = None
        source = "none"
        if self.helius_client:
            data = self.helius_client.get_token_data(token)
            if data and data.get("market_cap", 0) > 0:
                source = "helius"
        if (not data or data.get("market_cap", 0) == 0) and self.gmgn_client:
            gmgn_data = self.gmgn_client.get_market_data(token)
            if gmgn_data and gmgn_data.get("market_cap", 0) > 0:
                data = gmgn_data
                source = "gmgn"
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
        if not data:
            data = {
                "price": 0,
                "market_cap": 0,
                "volume": 0,
                "volume_growth": {"growth_5m": 0, "growth_1h": 0},
                "estimated": True
            }
            source = "none"
        data["source"] = source
        return data

    def _format_signal_message(self, candidate, signal_id):
        """
        Formatea el mensaje de señal para enviar a Telegram.
        """
        token = candidate["token"]
        confidence = candidate["confidence"]
        trader_count = candidate["trader_count"]
        volume_usd = candidate.get("volume_usd", 0)
        market_cap = candidate.get("market_cap", 0)
        buy_percentage = candidate.get("buy_percentage", 0)
        ml_prediction = candidate.get("ml_prediction", 0.5)
        
        message = (
            f"*🚀 Nueva Señal #{signal_id}*\n\n"
            f"Token: `{token}`\n"
            f"Confianza: *{confidence:.2f}*\n"
            f"Predicción ML: *{ml_prediction:.2f}*\n\n"
            f"Traders: `{trader_count}`\n"
            f"Volumen: `${volume_usd:,.2f}`\n"
            f"Market Cap: `${market_cap:,.2f}`\n"
            f"Buy Ratio: `{buy_percentage*100:.1f}%`\n\n"
            f"_Señal generada por ChipaTrading_"
        )
        return message
