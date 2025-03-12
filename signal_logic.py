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
        self.token_candidates = {}  # {token: {wallets, transactions, last_update, volume_usd, first_seen}}
        self.recent_signals = []    # Lista de (token, timestamp, confidence, signal_id)
        self.last_signal_check = time.time()
        self.last_cleanup = time.time()  # Para el control de limpieza de transacciones antiguas
        self.watched_tokens = set()  # Set para seguimiento rápido de tokens que ya tienen señales

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

            # Validar monto mínimo - REDUCIDO para capturar más transacciones
            min_tx_usd = float(Config.get("MIN_TRANSACTION_USD", 100))  # Bajado de 200 a 100
            if amount_usd < min_tx_usd:
                logger.debug(f"Transacción ignorada por monto bajo: ${amount_usd:.2f} < ${min_tx_usd}")
                return

            timestamp = tx_data.get("timestamp", int(time.time()))
            tx_data["timestamp"] = timestamp  # Asegurar que hay timestamp

            logger.info(f"SignalLogic.process_transaction: {wallet[:8]}...{wallet[-5:]} -> {token[:8]}...{token[-5:]} " +
                        f"[{tx_data.get('type')}] ${amount_usd:.2f}")

            # Inicializar estructura para token si no existe
            if token not in self.token_candidates:
                self.token_candidates[token] = {
                    "wallets": set(),
                    "transactions": [],
                    "last_update": timestamp,
                    "volume_usd": 0,
                    "first_seen": timestamp,
                    "whale_activity": False,
                    "transaction_rate": 0.0
                }
                logger.info(f"Nuevo token detectado: {token}")

            # Actualizar datos
            self.token_candidates[token]["wallets"].add(wallet)
            self.token_candidates[token]["transactions"].append(tx_data)
            self.token_candidates[token]["last_update"] = timestamp
            self.token_candidates[token]["volume_usd"] += amount_usd
            
            # Detectar actividad de ballenas (transacciones grandes)
            if amount_usd > 5000:
                self.token_candidates[token]["whale_activity"] = True
                logger.info(f"🐋 Actividad de ballena detectada en {token[:8]}... (${amount_usd:.2f})")

            # Actualizar transaction_rate (transacciones por minuto)
            txs = self.token_candidates[token]["transactions"]
            if len(txs) > 1:
                time_span = max(txs[-1]["timestamp"] - txs[0]["timestamp"], 1)  # Evitar división por cero
                tx_rate = len(txs) / (time_span / 60)  # tx por minuto
                self.token_candidates[token]["transaction_rate"] = tx_rate
                
                # Detección de actividad inusual (muchas tx rápidas)
                if tx_rate > 20:  # Más de 20 tx por minuto
                    logger.info(f"⚡ Alta velocidad de transacciones en {token[:8]}... ({tx_rate:.1f} tx/min)")

            candidate = self.token_candidates[token]
            logger.info(f"Candidato actualizado: token={token[:8]}..., "
                        f"wallets={len(candidate['wallets'])}, "
                        f"transactions={len(candidate['transactions'])}, "
                        f"volume_usd=${candidate['volume_usd']:.2f}, "
                        f"tx_rate={candidate.get('transaction_rate', 0):.1f}")

            # Limpieza periódica de transacciones antiguas (más de 24h)
            now = time.time()
            if now - self.last_cleanup > 3600:  # Cada hora
                self._cleanup_old_data()
                self.last_cleanup = now

        except Exception as e:
            logger.error(f"Error procesando transacción en SignalLogic: {e}", exc_info=True)

    def _cleanup_old_data(self):
        """
        Elimina transacciones de más de 24 horas de antigüedad para cada token.
        """
        now = time.time()
        cutoff = now - 86400  # 24 horas
        for token in list(self.token_candidates.keys()):
            original_count = len(self.token_candidates[token]["transactions"])
            self.token_candidates[token]["transactions"] = [
                tx for tx in self.token_candidates[token]["transactions"] if tx["timestamp"] > cutoff
            ]
            cleaned = original_count - len(self.token_candidates[token]["transactions"])
            if cleaned > 0:
                logger.debug(f"Limpieza de {cleaned} transacciones antiguas para token {token[:8]}...")

    async def _process_candidates(self):
        """
        Procesa los tokens candidatos para generar señales con criterios mejorados.
        """
        try:
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

                    # Verificar si este token ya tuvo señal recientemente (en self.watched_tokens)
                    if token in self.watched_tokens:
                        logger.debug(f"Token {token} ya en seguimiento, omitiendo")
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
                        tx_timespan = max(1, tx_timespan)  # Evitar división por cero
                        tx_velocity = len(recent_txs) / (tx_timespan / 60)
                    else:
                        tx_velocity = 1.0

                    logger.info(f"Token {token[:8]}...: {trader_count} traders, ${volume_usd:.2f} vol, " +
                                f"{buy_percentage:.2f} buy ratio, {tx_velocity:.2f} tx/min")

                    # Verificar condiciones mínimas - AJUSTADAS para capturar más señales
                    min_traders = int(Config.get("MIN_TRADERS_FOR_SIGNAL", 1))  # Bajado a 1
                    min_volume = float(Config.get("MIN_VOLUME_USD", 1000))  # Bajado a 1000
                    min_buy_percentage = float(Config.get("MIN_BUY_PERCENTAGE", 0.5))  # Bajado a 0.5
                    min_velocity = float(Config.get("MIN_TX_VELOCITY", 0.3))  # Bajado a 0.3

                    # NUEVO: Excepción para tokens con actividad de ballenas o alta velocidad
                    is_special_case = data.get("whale_activity", False) or tx_velocity > 15

                    if not is_special_case:
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
                    else:
                        logger.info(f"⭐ Caso especial detectado para {token[:8]}... (whale_activity={data.get('whale_activity')}, tx_velocity={tx_velocity:.1f})")

                    # Obtener datos de mercado usando función de fallback
                    market_data = await self.get_token_market_data(token)
                    market_cap = market_data.get("market_cap", 0)
                    vol_growth = market_data.get("volume_growth", {})

                    # Clasificar token como "meme" si aplica
                    token_type = None
                    if self.gmgn_client and self.gmgn_client.is_memecoin(token):
                        token_type = "meme"
                    elif vol_growth.get("growth_5m", 0) > 0.2 and (market_cap < 5_000_000 or market_cap == 0):
                        token_type = "meme"
                    # NUEVO: Detección por nombre/símbolo en mensajes de Cielo
                    elif any(tx.get("token_name", "").lower() in ["pepe", "doge", "shib", "inu", "moon", "elon", "wojak"] 
                           for tx in recent_txs if "token_name" in tx):
                        token_type = "meme"

                    # Calcular todas las puntuaciones de traders y promediar
                    trader_scores = [self.scoring_system.get_score(w) for w in data["wallets"]]
                    
                    # Calcular confianza mejorada basada en métricas
                    confidence = self.scoring_system.compute_confidence(
                        wallet_scores=trader_scores,
                        volume_1h=market_data.get("volume", 0),
                        market_cap=market_cap,
                        recent_volume_growth=vol_growth.get("growth_5m", 0),
                        token_type=token_type
                    )

                    # Ajustar para detectar daily runners o actividad de ballenas
                    config = Config.MEMECOIN_CONFIG
                    is_memecoin = (tx_velocity > config["TX_RATE_THRESHOLD"] and 
                                vol_growth.get("growth_5m", 0) > config["VOLUME_GROWTH_THRESHOLD"] and 
                                market_cap < 10_000_000)
                    whale_threshold = 5000  # Bajado de 10000 a 5000
                    if recent_txs and max(tx["amount_usd"] for tx in recent_txs) > whale_threshold:
                        is_memecoin = True
                        logger.info(f"🐋 Actividad de ballena detectada en {token[:8]}...")

                    if is_memecoin:
                        confidence *= 1.5
                        logger.info(f"🚀 Memecoin potencial detectado: {token[:8]}... (confianza ajustada: {confidence:.2f})")

                    # NUEVO: Usar ML si está disponible
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
                        
                        # Ajustar confianza con la predicción ML
                        confidence = confidence * 0.7 + ml_prediction * 0.3
                        logger.info(f"ML predicción para {token[:8]}...: {ml_prediction:.2f}")

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
                    logger.error(f"Error procesando candidato {token}: {e}")
            
            # Ordenar por confianza y aplicar throttling
            candidates.sort(key=lambda x: x["confidence"], reverse=True)
            
            # NUEVO: Seleccionar los mejores candidatos si hay muchos
            signal_throttling = int(Config.get("SIGNAL_THROTTLING", 5))
            if len(candidates) > signal_throttling:
                top_candidates = candidates[:signal_throttling]
                logger.info(f"Aplicando throttling: seleccionando los {signal_throttling} mejores de {len(candidates)} candidatos")
                candidates = top_candidates
            
            # Generar señales de los candidatos finales
            await self._generate_signals(candidates)
        except Exception as e:
            logger.error(f"Error en _process_candidates: {e}", exc_info=True)

    async def get_token_market_data(self, token):
        """
        Obtiene datos de mercado utilizando múltiples fuentes con fallback.
        Prioridad: Helius -> GMGN -> datos estimados.
        """
        data = None
        source = "none"

        # Intentar con Helius
        if self.helius_client:
            try:
                data = self.helius_client.get_token_data(token)
                if data and data.get("market_cap", 0) > 0:
                    source = "helius"
                    logger.debug(f"Datos de mercado para {token[:8]}... obtenidos de Helius")
            except Exception as e:
                logger.warning(f"Error obteniendo datos de Helius para {token[:8]}...: {e}")

        # Fallback a GMGN
        if not data or data.get("market_cap", 0) == 0:
            if self.gmgn_client:
                try:
                    gmgn_data = self.gmgn_client.get_market_data(token)
                    if gmgn_data and gmgn_data.get("market_cap", 0) > 0:
                        data = gmgn_data
                        source = "gmgn"
                        logger.debug(f"Datos de mercado para {token[:8]}... obtenidos de GMGN")
                except Exception as e:
                    logger.warning(f"Error obteniendo datos de GMGN para {token[:8]}...: {e}")

        # Estimación a partir de transacciones
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
                    logger.debug(f"Datos de mercado para {token[:8]}... estimados desde transacciones")

        # Si aún no hay datos, usar placeholder
        if not data:
            data = {
                "price": 0,
                "market_cap": 0,
                "volume": 0,
                "volume_growth": {"growth_5m": 0, "growth_1h": 0},
                "estimated": True
            }
            source = "none"
            logger.debug(f"Sin datos de mercado para {token[:8]}... usando valores por defecto")

        data["source"] = source
        return data

    async def _generate_signals(self, candidates):
        """
        Genera señales a partir de la lista de candidatos.
        """
        # NUEVO: Verificar si hay candidatos
        if not candidates:
            logger.info("No hay candidatos que cumplan los criterios para generar señales")
            return
            
        signals_generated = 0
        min_confidence = float(Config.get("MIN_CONFIDENCE_THRESHOLD", 0.3))
        
        for candidate in candidates:
            # Verificar confianza mínima
            if candidate["confidence"] < min_confidence:
                logger.debug(f"Candidato {candidate['token'][:8]}... descartado por baja confianza: {candidate['confidence']:.2f} < {min_confidence}")
                continue
                
            # Registrar señal en BD
            try:
                signal_id = db.save_signal(
                    candidate["token"],
                    candidate["trader_count"],
                    candidate["confidence"],
                    candidate.get("initial_price", 0)
                )
                
                # Guardar features para ML
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
                
                # Actualizar lista de señales recientes
                self.recent_signals.append((candidate["token"], time.time(), candidate["confidence"], signal_id))
                self.watched_tokens.add(candidate["token"])  # Agregar a set de tokens en seguimiento
                
                # Iniciar seguimiento de rendimiento
                if self.performance_tracker:
                    signal_info = {
                        "confidence": candidate["confidence"],
                        "traders_count": candidate["trader_count"],
                        "total_volume": candidate["volume_usd"],
                        "signal_id": signal_id
                    }
                    self.performance_tracker.add_signal(candidate["token"], signal_info)
                
                # Enviar señal a Telegram
                self._send_signal_notification(candidate, signal_id)
                
                signals_generated += 1
                logger.info(f"✅ Señal generada para {candidate['token'][:8]}... con confianza {candidate['confidence']:.2f}")
            except Exception as e:
                logger.error(f"Error al generar señal para {candidate['token'][:8]}...: {e}")

        # Limitar el tamaño de recent_signals a 100
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
        
        # Construir enlaces
        solscan_link = f"https://solscan.io/token/{token}"
        birdeye_link = f"https://birdeye.so/token/{token}?chain=solana"
        neobullx_link = f"https://neo.bullx.io/terminal?chainId=1399811149&address={token}"
        
        # Recolectar métricas adicionales
        volume_growth_5m = signal_data.get("volume_growth", {}).get("growth_5m", 0) * 100
        tx_velocity = signal_data.get("tx_velocity", 0)
        market_cap = signal_data.get("market_cap", 0)
        
        # Formatear market cap
        if market_cap >= 1_000_000:
            mcap_str = f"${market_cap/1_000_000:.2f}M"
        elif market_cap > 0:
            mcap_str = f"${market_cap/1_000:.1f}K"
        else:
            mcap_str = "Desconocido"
        
        # Formatear nombres de traders
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
            f"_Se realizará seguimiento cada 3m, 5m, 10m, 30m, 1h, 2h, 4h y 24h._"
        )
        
        send_telegram_message(message)
            
    async def check_signals_periodically(self):
        """
        Ejecuta la comprobación de señales periódicamente.
        """
        while True:
            try:
                now = time.time()
                if now - self.last_signal_check > 60:  # Cada minuto
                    logger.info("Ejecutando comprobación de señales...")
                    await self._process_candidates()
                    self.last_signal_check = now
            except Exception as e:
                logger.error(f"Error en check_signals_periodically: {e}", exc_info=True)
            await asyncio.sleep(10)
            
    def get_active_candidates_count(self):
        """
        Retorna el número de tokens candidatos activos
        """
        now = time.time()
        window_seconds = float(Config.get("SIGNAL_WINDOW_SECONDS", 540))
        cutoff = now - window_seconds
        active_count = 0
        
        for token, data in self.token_candidates.items():
            if data["last_update"] > cutoff:
                active_count += 1
                
        return active_count
