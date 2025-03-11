import time
import asyncio
import logging
from datetime import datetime
from config import Config
import json
import db
from telegram_utils import send_telegram_message

logger = logging.getLogger("signal_logic")

class SignalLogic:
    def __init__(self, scoring_system=None, helius_client=None, gmgn_client=None, rugcheck_api=None, ml_predictor=None):
        """
        Inicializa la lógica de señales.
        Se inyectan clientes para obtener datos de mercado y otros servicios.
        """
        self.scoring_system = scoring_system
        self.helius_client = helius_client
        self.gmgn_client = gmgn_client  # Cliente GMGN como respaldo
        self.rugcheck_api = rugcheck_api  # Se mantendrá en init, pero no se usará
        self.ml_predictor = ml_predictor
        self.performance_tracker = None
        self.token_candidates = {}  # Estructura: {token: {wallets, transactions, last_update, volume_usd}}
        self.recent_signals = []    # Lista de (token, timestamp, confidence, signal_id)
        self.last_signal_check = time.time()
        self.last_cleanup = time.time()

    def get_active_candidates_count(self):
        """
        Retorna el número de tokens candidatos que están siendo monitoreados actualmente.
        
        Returns:
            int: Número de tokens candidatos activos
        """
        return len(self.token_candidates)

    def process_transaction(self, tx_data):
        """
        Procesa una transacción para actualizar los candidatos de señales.
        
        Args:
            tx_data: Datos de la transacción
        """
        try:
            if not tx_data:
                return
                
            token = tx_data.get("token")
            wallet = tx_data.get("wallet")
            amount_usd = tx_data.get("amount_usd", 0)
            
            if not token or not wallet:
                return
                
            # Validar monto mínimo
            min_tx_usd = float(Config.get("MIN_TRANSACTION_USD", 200))
            if amount_usd < min_tx_usd:
                return
                
            timestamp = tx_data.get("timestamp", int(time.time()))
            tx_data["timestamp"] = timestamp  # Asegurar que hay timestamp
            
            # Log para seguimiento
            logger.info(f"Procesando tx: {wallet[:10]}...{wallet[-5:]} -> {token[:10]}...{token[-5:]} " +
                      f"[{tx_data.get('type')}] ${amount_usd:.2f}")
            
            # Inicializar estructura para token si no existe
            if token not in self.token_candidates:
                self.token_candidates[token] = {
                    "wallets": set(),
                    "transactions": [],
                    "last_update": timestamp,
                    "volume_usd": 0,
                    "first_seen": timestamp  # Añadir timestamp de primera vez visto
                }
                logger.info(f"Nuevo token detectado: {token}")
            
            # Actualizar datos
            self.token_candidates[token]["wallets"].add(wallet)
            self.token_candidates[token]["transactions"].append(tx_data)
            self.token_candidates[token]["last_update"] = timestamp
            self.token_candidates[token]["volume_usd"] += amount_usd
            
            # Limpieza periódica de transacciones antiguas (mayor a 24h)
            now = time.time()
            if now - self.last_cleanup > 3600:  # Cada hora
                self._cleanup_old_data()
                self.last_cleanup = now
                
        except Exception as e:
            logger.error(f"Error procesando transacción: {e}")
    
    def _cleanup_old_data(self):
        """
        Elimina datos antiguos (transacciones de más de 24h) para no acumular memoria.
        """
        now = time.time()
        cutoff = now - 86400  # 24 horas
        
        tokens_to_remove = []
        for token, data in self.token_candidates.items():
            # Filtrar transacciones recientes
            recent_txs = [tx for tx in data["transactions"] if tx["timestamp"] > cutoff]
            
            if not recent_txs:
                tokens_to_remove.append(token)
            else:
                # Actualizar con solo transacciones recientes
                self.token_candidates[token]["transactions"] = recent_txs
                
                # Recalcular wallets activas basadas en transacciones recientes
                active_wallets = {tx["wallet"] for tx in recent_txs}
                self.token_candidates[token]["wallets"] = active_wallets
                
                # Recalcular volumen
                volume_usd = sum(tx["amount_usd"] for tx in recent_txs)
                self.token_candidates[token]["volume_usd"] = volume_usd
        
        # Eliminar tokens sin actividad reciente
        for token in tokens_to_remove:
            del self.token_candidates[token]
            
        if tokens_to_remove:
            logger.info(f"Limpiados {len(tokens_to_remove)} tokens sin actividad reciente")

    async def check_signals_periodically(self, interval=30):
        """
        Verifica y procesa candidatos para generar señales de forma periódica.
        """
        while True:
            try:
                now = time.time()
                # Verificar si pasó suficiente tiempo desde la última verificación
                throttle_seconds = float(Config.get("SIGNAL_THROTTLING", 10))
                if now - self.last_signal_check < throttle_seconds:
                    await asyncio.sleep(1)
                    continue
                
                # Procesar candidatos
                await self._process_candidates()
                self.last_signal_check = now
                
            except Exception as e:
                logger.error(f"Error en check_signals_periodically: {e}")
            
            # Esperar para la próxima verificación
            await asyncio.sleep(interval)

    async def _process_candidates(self):
        """
        Procesa los tokens candidatos para generar señales con criterios mejorados.
        """
        now = time.time()
        window_seconds = float(Config.get("SIGNAL_WINDOW_SECONDS", 540))
        cutoff = now - window_seconds
        candidates = []

        for token, data in list(self.token_candidates.items()):
            try:
                # Obtener transacciones recientes
                recent_txs = [tx for tx in data["transactions"] if tx["timestamp"] > cutoff]
                if not recent_txs:
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
                buy_volume = sum(tx["amount_usd"] for tx in buy_txs)
                sell_volume = sum(tx["amount_usd"] for tx in sell_txs)
                
                # Calcular velocidad de transacciones (txs por minuto)
                timestamps = [tx["timestamp"] for tx in recent_txs]
                if len(timestamps) > 1:
                    tx_timespan = max(timestamps) - min(timestamps)
                    tx_timespan = max(1, tx_timespan)  # Evitar división por cero
                    tx_velocity = len(recent_txs) / (tx_timespan / 60)
                else:
                    tx_velocity = 1.0  # Valor predeterminado
                
                # Verificar condiciones mínimas
                min_traders = int(Config.get("MIN_TRADERS_FOR_SIGNAL", 2))
                min_volume = float(Config.get("MIN_VOLUME_USD", 2000))
                min_buy_percentage = float(Config.get("MIN_BUY_PERCENTAGE", 0.7))
                min_velocity = float(Config.get("MIN_TX_VELOCITY", 0.5))
                
                # Log de métricas para depuración
                logger.info(f"Token {token}: {trader_count} traders, ${volume_usd:.2f} vol, " +
                          f"{buy_percentage:.2f} buy ratio, {tx_velocity:.2f} tx/min")
                
                # Filtrar por criterios más estrictos
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
                    
                # Recopilar scores de wallets para evaluar la calidad de los traders
                if self.scoring_system:
                    wallet_scores = [self.scoring_system.get_score(wallet) for wallet in data["wallets"]]
                    avg_score = sum(wallet_scores) / len(wallet_scores) if wallet_scores else 0
                    high_quality_threshold = float(Config.get("HIGH_QUALITY_TRADER_SCORE", 7.0))
                    high_quality_traders = sum(1 for score in wallet_scores if score >= high_quality_threshold)
                    high_quality_ratio = high_quality_traders / len(wallet_scores) if wallet_scores else 0
                else:
                    wallet_scores = []
                    avg_score = 0
                    high_quality_traders = 0
                    high_quality_ratio = 0
                    
                # Obtener información de mercado y token si están disponibles los clientes
                token_type = None
                market_cap = 0
                volume_1h = 0
                volume_growth_5m = 0
                volume_growth_1h = 0
                
                # Intentar obtener info desde GMGN
                if self.gmgn_client:
                    try:
                        market_data = self.gmgn_client.get_market_data(token)
                        if market_data:
                            market_cap = market_data.get("market_cap", 0)
                            volume_1h = market_data.get("volume", 0) / 24  # Estimar volumen horario
                            volume_growth = market_data.get("volume_growth", {})
                            volume_growth_5m = volume_growth.get("growth_5m", 0)
                            volume_growth_1h = volume_growth.get("growth_1h", 0)
                            token_type = "meme" if self.gmgn_client.is_memecoin(token) else None
                            
                            logger.info(f"GMGN data para {token}: mcap=${market_cap:.2f}, " +
                                      f"vol_1h=${volume_1h:.2f}, growth_5m={volume_growth_5m:.2f}")
                    except Exception as e:
                        logger.warning(f"Error obteniendo datos de GMGN para {token}: {e}")
                
                # Calcular confianza basada en todos los factores
                # Esta es una fórmula mejorada comparada con la asignación estática
                if self.scoring_system and wallet_scores:
                    # Usar el método de la clase scoring para calcular la confianza
                    confidence = self.scoring_system.compute_confidence(
                        wallet_scores=wallet_scores, 
                        volume_1h=volume_1h, 
                        market_cap=market_cap,
                        recent_volume_growth=volume_growth_5m,
                        token_type=token_type
                    )
                else:
                    # Cálculo básico si no hay scoring_system
                    confidence_factors = [
                        min(1.0, trader_count / 10),  # Más traders = mejor
                        min(1.0, volume_usd / 10000),  # Más volumen = mejor
                        min(1.0, buy_percentage * 1.2),  # Más compras que ventas = mejor
                        min(1.0, tx_velocity / 5),  # Más actividad = mejor
                    ]
                    confidence = sum(confidence_factors) / len(confidence_factors)
                
                # Aplicar multiplicadores según tipo de token
                if token_type == "meme" and volume_growth_5m > 0.3:
                    confidence *= 1.2  # Bonus para memecoins con crecimiento rápido
                
                # Asegurarse de que la confianza esté entre 0 y 1
                confidence = max(0.0, min(1.0, confidence))
                
                # Si tenemos predictor ML, usarlo para refinar la predicción
                if self.ml_predictor:
                    try:
                        # Preparar features para el modelo ML
                        ml_features = {
                            'num_traders': trader_count,
                            'num_transactions': len(recent_txs),
                            'total_volume_usd': volume_usd,
                            'avg_volume_per_trader': volume_usd / trader_count if trader_count > 0 else 0,
                            'buy_ratio': buy_percentage,
                            'tx_velocity': tx_velocity,
                            'avg_trader_score': avg_score,
                            'max_trader_score': max(wallet_scores) if wallet_scores else 0,
                            'market_cap': market_cap,
                            'volume_1h': volume_1h,
                            'volume_growth_5m': volume_growth_5m,
                            'volume_growth_1h': volume_growth_1h,
                            'tx_rate': tx_velocity,
                            'whale_flag': 1 if high_quality_ratio > 0.5 else 0,
                            'is_meme': 1 if token_type == "meme" else 0,
                            'window_seconds': window_seconds
                        }
                        
                        # Obtener predicción ML
                        ml_confidence = self.ml_predictor.predict_success(ml_features)
                        
                        # Combinar confianza heurística con ML (promedio ponderado)
                        confidence = (confidence * 0.4) + (ml_confidence * 0.6)
                        logger.info(f"ML ajustó confianza para {token}: {confidence:.3f}")
                    except Exception as e:
                        logger.warning(f"Error al usar ML para {token}: {e}")
                
                # Verificar umbral mínimo de confianza
                min_confidence = float(Config.get("MIN_CONFIDENCE_THRESHOLD", 0.3))
                if confidence < min_confidence:
                    logger.debug(f"Token {token} descartado: baja confianza ({confidence:.3f} < {min_confidence})")
                    continue

                # Crear candidato
                candidate = {
                    "token": token,
                    "confidence": confidence,
                    "trader_count": trader_count,
                    "volume_usd": volume_usd,
                    "buy_percentage": buy_percentage,
                    "tx_velocity": tx_velocity,
                    "avg_trader_score": avg_score,
                    "high_quality_ratio": high_quality_ratio,
                    "market_cap": market_cap,
                    "volume_growth_5m": volume_growth_5m,
                    "recent_transactions": recent_txs,
                    "token_type": token_type,
                    "initial_price": 0
                }
                
                # Intentar obtener precio inicial si está disponible
                if self.gmgn_client:
                    try:
                        market_data = self.gmgn_client.get_market_data(token)
                        if market_data and "price" in market_data:
                            candidate["initial_price"] = market_data["price"]
                    except Exception:
                        pass
                elif self.helius_client:
                    try:
                        token_data = self.helius_client.get_token_data(token)
                        if token_data and "price" in token_data:
                            candidate["initial_price"] = token_data["price"]
                    except Exception:
                        pass

                candidates.append(candidate)
                logger.info(f"Candidato generado para {token} con confianza {confidence:.3f}, "
                            f"{trader_count} traders, ${volume_usd:.2f} USD")
                            
            except Exception as e:
                logger.error(f"Error procesando candidato {token}: {e}")

        # Ordenar candidatos por confianza (de mayor a menor)
        candidates.sort(key=lambda x: x["confidence"], reverse=True)
        
        # Limitar número de señales para evitar spam
        max_signals = int(Config.get("MAX_SIGNALS_PER_RUN", 3))
        await self._generate_signals(candidates[:max_signals])

    async def _generate_signals(self, candidates):
        """
        Genera señales basadas en los candidatos procesados.
        Guarda en DB y envía alertas a Telegram.
        """
        if not candidates:
            return
            
        for candidate in candidates:
            try:
                token = candidate["token"]
                confidence = candidate["confidence"]
                trader_count = candidate["trader_count"]
                volume_usd = candidate["volume_usd"]
                initial_price = candidate.get("initial_price", 0)
                
                # Guardar señal en base de datos
                signal_id = db.save_signal(
                    token=token,
                    trader_count=trader_count,
                    confidence=confidence,
                    initial_price=initial_price
                )
                
                # Registrar en recientes
                now = time.time()
                self.recent_signals.append((token, now, confidence, signal_id))
                
                # Limpiar señales antiguas (más de 6h)
                self.recent_signals = [
                    (t, ts, conf, sid) for t, ts, conf, sid in self.recent_signals 
                    if ts > (now - 21600)
                ]
                
                # Registrar detalles adicionales como features para ML
                signal_features = {
                    'num_traders': trader_count,
                    'num_transactions': len(candidate.get("recent_transactions", [])),
                    'total_volume_usd': volume_usd,
                    'buy_ratio': candidate.get("buy_percentage", 0),
                    'tx_velocity': candidate.get("tx_velocity", 0),
                    'avg_trader_score': candidate.get("avg_trader_score", 0),
                    'high_quality_ratio': candidate.get("high_quality_ratio", 0),
                    'market_cap': candidate.get("market_cap", 0),
                    'volume_1h': candidate.get("volume_1h", 0),
                    'volume_growth_5m': candidate.get("volume_growth_5m", 0),
                    'volume_growth_1h': candidate.get("volume_growth_1h", 0),
                    'token_type': candidate.get("token_type", "unknown"),
                    'window_seconds': float(Config.get("SIGNAL_WINDOW_SECONDS", 540))
                }
                
                db.save_signal_features(signal_id, token, signal_features)
                
                # Preparar información para performance tracker
                signal_info = {
                    "signal_id": signal_id,
                    "confidence": confidence,
                    "traders_count": trader_count,
                    "total_volume": volume_usd,
                    "token_type": candidate.get("token_type", "unknown")
                }
                
                # Iniciar seguimiento de rendimiento si está disponible
                if self.performance_tracker:
                    self.performance_tracker.add_signal(token, signal_info)
                
                # Enviar alerta a Telegram
                await self._send_signal_alert(token, signal_id, candidate)
                
                logger.info(f"✅ Señal generada para {token} (ID: {signal_id}) con confianza {confidence:.2f}")
            except Exception as e:
                logger.error(f"Error generando señal para {candidate['token']}: {e}")
    
    async def _send_signal_alert(self, token, signal_id, candidate):
        """
        Envía alerta de señal a Telegram.
        
        Args:
            token: Dirección del token
            signal_id: ID de la señal
            candidate: Datos del candidato
        """
        try:
            confidence = candidate["confidence"]
            trader_count = candidate["trader_count"]
            volume_usd = candidate["volume_usd"]
            token_type = candidate.get("token_type", "unknown")
            initial_price = candidate.get("initial_price", 0)
            market_cap = candidate.get("market_cap", 0)
            
            # Emoji según confianza
            if confidence >= 0.8:
                confidence_emoji = "🔥"
            elif confidence >= 0.6:
                confidence_emoji = "✅"
            elif confidence >= 0.4:
                confidence_emoji = "⚠️"
            else:
                confidence_emoji = "🔍"
                
            # Emoji según tipo de token
            if token_type == "meme":
                token_emoji = "🐸"
            elif token_type == "defi":
                token_emoji = "💰"
            elif token_type == "nft":
                token_emoji = "🖼️"
            elif token_type == "gaming":
                token_emoji = "🎮"
            else:
                token_emoji = "🪙"
            
            # URLs para explorar el token
            birdeye_url = f"https://birdeye.so/token/{token}?chain=solana"
            dexscreener_url = f"https://dexscreener.com/solana/{token}"
            neobullx_url = f"https://neo.bullx.io/terminal?chainId=1399811149&address={token}"
            
            # Crear mensaje de Telegram
            message = (
                f"{confidence_emoji} *Nueva Señal #{signal_id}* {token_emoji}\n\n"
                f"*Token:* `{token}`\n"
                f"*Confianza:* `{confidence:.2f}`\n"
                f"*Traders:* `{trader_count}`\n"
                f"*Volumen USD:* `${volume_usd:,.2f}`\n"
            )
            
            # Añadir datos de mercado si están disponibles
            if initial_price > 0:
                message += f"*Precio:* `${initial_price:.8f}`\n"
            
            if market_cap > 0:
                message += f"*Market Cap:* `${market_cap:,.2f}`\n"
            
            if token_type and token_type != "unknown":
                message += f"*Tipo:* `{token_type.upper()}`\n"
            
            # Añadir enlaces
            message += "\n*🔍 Explorar:*\n"
            message += f"• [Birdeye]({birdeye_url})\n"
            message += f"• [Dexscreener]({dexscreener_url})\n"
            message += f"• [Neo BullX]({neobullx_url})\n"
            
            # Añadir información adicional importante
            if candidate.get("high_quality_ratio", 0) > 0.5:
                message += "\n⭐ *Traders de alta calidad detectados*"
            
            if candidate.get("volume_growth_5m", 0) > 0.5:
                message += "\n📈 *Crecimiento rápido de volumen*"
            
            # Enviar mensaje
            send_telegram_message(message)
            
        except Exception as e:
            logger.error(f"Error enviando alerta para {token}: {e}")
