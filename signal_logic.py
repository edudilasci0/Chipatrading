import time
import asyncio
from config import Config
import db  # Asegúrate de que este módulo implemente count_signals_last_hour() y save_signal()
from telegram_utils import send_telegram_message  # Función para enviar mensajes a Telegram

class SignalLogic:
    def __init__(self, scoring_system=None, dex_client=None, rugcheck_api=None, ml_predictor=None):
        """
        Inicializa la lógica de señales.
        
        Args:
            scoring_system: Sistema de puntuación para evaluar traders.
            dex_client: Cliente para interactuar con DexScreener u otra fuente de datos.
            rugcheck_api: Instancia de la API para validación de tokens con RugCheck (opcional).
            ml_predictor: Predictor ML para evaluar señales.
        """
        self.scoring_system = scoring_system
        self.dex_client = dex_client
        self.rugcheck_api = rugcheck_api
        self.ml_predictor = ml_predictor
        self.performance_tracker = None  # Opcional, para seguimiento de rendimiento
        self.token_candidates = {}  # {token: {wallets: set(), transactions: [], last_update: timestamp, volume_usd: float}}
        self.recent_signals = []    # [(token, timestamp, confidence, signal_id)]
        self.last_signal_check = time.time()
        
    def add_transaction(self, wallet, token, amount_usd, tx_type):
        """
        Procesa una nueva transacción para un token.
        
        Args:
            wallet (str): Dirección de la wallet que realizó la transacción.
            token (str): Dirección o identificador del token.
            amount_usd (float): Valor en USD de la transacción.
            tx_type (str): Tipo de transacción (por ejemplo, "BUY" o "SELL").
        """
        now = int(time.time())
        
        # NUEVO: Ignorar tokens especiales o conocidos
        if token in Config.IGNORE_TOKENS:
            print(f"⚠️ Token {token} en lista de ignorados, no se procesará")
            return
        
        # Inicializar datos del token si no existen
        if token not in self.token_candidates:
            self.token_candidates[token] = {
                "wallets": set(),
                "transactions": [],
                "last_update": now,
                "volume_usd": 0
            }
        
        # Actualizar información del token
        self.token_candidates[token]["wallets"].add(wallet)
        self.token_candidates[token]["transactions"].append({
            "wallet": wallet,
            "amount_usd": amount_usd,
            "type": tx_type,
            "timestamp": now
        })
        self.token_candidates[token]["volume_usd"] += amount_usd
        self.token_candidates[token]["last_update"] = now
        
        print(f"Procesando transacción para token {token}: {tx_type} ${amount_usd} por {wallet}")
    
    async def check_signals_periodically(self, interval=30):
        """
        Ejecuta la verificación de señales periódicamente.
        
        Args:
            interval: Intervalo en segundos entre ejecuciones.
        """
        while True:
            try:
                await self._process_candidates()
                await asyncio.sleep(interval)
            except Exception as e:
                print(f"Error en check_signals_periodically: {e}")
                await asyncio.sleep(interval)
    
    def get_active_candidates_count(self):
        """
        Retorna el número de tokens candidatos activos.
        """
        now = time.time()
        window_seconds = float(Config.get("signal_window_seconds", Config.SIGNAL_WINDOW_SECONDS))
        cutoff = now - window_seconds
        
        active_count = 0
        for token, data in self.token_candidates.items():
            if data["last_update"] > cutoff:
                active_count += 1
        return active_count
    
    def get_recent_signals(self, hours=24):
        """
        Obtiene las señales recientes.
        
        Args:
            hours: Horas hacia atrás para considerar señales.
            
        Returns:
            list: Lista de señales recientes (token, timestamp, confidence, signal_id).
        """
        cutoff = time.time() - (hours * 3600)
        return [(token, ts, conf, sig_id) for token, ts, conf, sig_id in self.recent_signals if ts > cutoff]
    
    async def _process_candidates(self):
        """
        Procesa los tokens candidatos y genera señales si se cumplen los criterios.
        """
        now = time.time()
        window_seconds = float(Config.get("signal_window_seconds", Config.SIGNAL_WINDOW_SECONDS))
        cutoff = now - window_seconds
        
        candidates = []
        for token, data in list(self.token_candidates.items()):
            try:
                # Filtrar transacciones recientes
                recent_txs = [tx for tx in data["transactions"] if tx["timestamp"] > cutoff]
                if not recent_txs:
                    continue
                
                trader_count = len(data["wallets"])
                volume_usd = sum(tx["amount_usd"] for tx in recent_txs)
                buy_txs = [tx for tx in recent_txs if tx["type"] == "BUY"]
                buy_percentage = len(buy_txs) / max(1, len(recent_txs))
                
                # Verificar umbrales mínimos
                min_traders = int(Config.get("min_traders_for_signal", Config.MIN_TRADERS_FOR_SIGNAL))
                min_volume = float(Config.get("min_volume_usd", Config.MIN_VOLUME_USD))
                if trader_count < min_traders or volume_usd < min_volume:
                    continue
                
                # Validar seguridad del token de forma asíncrona
                is_safe = await self._validate_token_safety_async(token)
                if not is_safe:
                    continue
                
                # Obtener scores de traders
                trader_scores = []
                for wallet in data["wallets"]:
                    if self.scoring_system:
                        score = self.scoring_system.get_score(wallet)
                        trader_scores.append(score)
                
                high_quality_traders = sum(1 for score in trader_scores if score >= 7.0)
                elite_traders = sum(1 for score in trader_scores if score >= 9.0)
                
                # Obtener datos de volumen y market cap
                volume_1h, market_cap, price = 0, 0, 0
                vol_growth = None
                if self.dex_client:
                    try:
                        volume_1h, market_cap, price = await self.dex_client.fetch_token_data(token)
                        await self.dex_client.update_volume_history(token)
                        vol_growth = self.dex_client.get_volume_growth(token)
                    except Exception as e:
                        print(f"Error obteniendo datos de mercado para {token}: {e}")
                
                # Calcular confianza base
                confidence = 0.5
                if self.scoring_system and trader_scores:
                    confidence = self.scoring_system.compute_confidence(
                        wallet_scores=trader_scores,
                        volume_1h=volume_1h,
                        market_cap=market_cap,
                        recent_volume_growth=vol_growth.get("growth_5m", 0) if vol_growth else 0,
                        token_type=None
                    )
                
                # Ajustar la confianza usando ML si está disponible
                ml_prediction = 0.5
                if self.ml_predictor and self.ml_predictor.model:
                    try:
                        features = {
                            "num_traders": trader_count,
                            "num_transactions": len(recent_txs),
                            "total_volume_usd": volume_usd,
                            "avg_volume_per_trader": volume_usd / trader_count if trader_count > 0 else 0,
                            "buy_ratio": buy_percentage,
                            "tx_velocity": len(recent_txs) / window_seconds,
                            "avg_trader_score": sum(trader_scores) / len(trader_scores) if trader_scores else 0,
                            "max_trader_score": max(trader_scores) if trader_scores else 0,
                            "market_cap": market_cap,
                            "volume_1h": volume_1h,
                            "volume_growth_5m": vol_growth.get("growth_5m", 0) if vol_growth else 0,
                            "volume_growth_1h": vol_growth.get("growth_1h", 0) if vol_growth else 0,
                            "high_quality_ratio": high_quality_traders / trader_count if trader_count > 0 else 0,
                            "elite_trader_count": elite_traders
                        }
                        ml_prediction = self.ml_predictor.predict_success(features)
                        confidence = (confidence + ml_prediction) / 2
                    except Exception as e:
                        print(f"Error en predicción ML para {token}: {e}")
                
                candidates.append({
                    "token": token,
                    "confidence": confidence,
                    "ml_prediction": ml_prediction,
                    "trader_count": trader_count,
                    "volume_usd": volume_usd,
                    "recent_transactions": recent_txs,
                    "market_cap": market_cap,
                    "volume_1h": volume_1h,
                    "volume_growth": vol_growth if vol_growth else {},
                    "buy_percentage": buy_percentage,
                    "trader_scores": trader_scores,
                    "high_quality_traders": high_quality_traders,
                    "elite_traders": elite_traders,
                    "initial_price": price
                })
            except Exception as e:
                print(f"Error procesando candidato {token}: {e}")
        
        # Ordenar candidatos por confianza descendente
        candidates.sort(key=lambda x: x["confidence"], reverse=True)
        await self._generate_signals(candidates)
    
    async def _validate_token_safety_async(self, token):
        """
        Versión asíncrona para validar la seguridad del token.
        """
        if not Config.get("ENABLE_RUGCHECK_FILTERING", False):
            return True
        if not self.rugcheck_api:
            return True
        try:
            min_score = int(Config.get("rugcheck_min_score", 50))
            is_safe = self.rugcheck_api.validate_token_safety(token, min_score)
            if not is_safe:
                print(f"⚠️ Token {token} no pasó validación de seguridad")
            return is_safe
        except Exception as e:
            print(f"Error validando token {token}: {e}")
            return True
    
    async def _generate_signals(self, candidates):
        """
        Genera señales para los candidatos que cumplen los criterios.
        
        Args:
            candidates: Lista de candidatos ordenados por confianza.
        """
        now = time.time()
        min_confidence = float(Config.get("min_confidence_threshold", Config.MIN_CONFIDENCE_THRESHOLD))
        throttling = int(Config.get("signal_throttling", Config.SIGNAL_THROTTLING))
        
        signals_last_hour = db.count_signals_last_hour()
        if signals_last_hour >= throttling:
            print(f"Límite de señales alcanzado ({signals_last_hour}/{throttling}), no generando más señales")
            return
        
        signals_generated = 0
        for candidate in candidates:
            token = candidate["token"]
            confidence = candidate["confidence"]
            trader_count = candidate["trader_count"]
            initial_price = candidate.get("initial_price", 0)
            
            # Evitar señales duplicadas para el mismo token en la última hora
            already_signaled = any(t == token for t, ts, _, _ in self.recent_signals if now - ts < 3600)
            if already_signaled:
                continue
            
            if confidence < min_confidence:
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
                print(f"✅ Señal generada para {token} con confianza {confidence:.2f}")
                
                # Opcional: guardar features para ML
                if hasattr(self, "ml_data_preparation") and self.ml_data_preparation:
                    try:
                        self.ml_data_preparation.extract_signal_features(token, self.dex_client, self.scoring_system)
                    except Exception as e:
                        print(f"Error guardando features ML: {e}")
            except Exception as e:
                print(f"Error generando señal para {token}: {e}")
            
            if signals_generated >= (throttling - signals_last_hour):
                break
    
    def _format_signal_message(self, candidate, signal_id):
        """
        Formatea el mensaje de señal para Telegram.
        
        Args:
            candidate: Datos del candidato.
            signal_id: ID asignado a la señal.
            
        Returns:
            str: Mensaje formateado.
        """
        token = candidate["token"]
        confidence = candidate["confidence"]
        trader_count = candidate["trader_count"]
        volume_usd = candidate.get("volume_usd", 0)
        market_cap = candidate.get("market_cap", 0)
        buy_percentage = candidate.get("buy_percentage", 0)
        high_quality_traders = candidate.get("high_quality_traders", 0)
        elite_traders = candidate.get("elite_traders", 0)
        ml_prediction = candidate.get("ml_prediction", 0.5)
        
        # Enlaces a exploradores (se mantienen los tres para referencia)
        dexscreener_link = f"https://dexscreener.com/solana/{token}"
        birdeye_link = f"https://birdeye.so/token/{token}?chain=solana"
        neobullx_link = f"https://neo.bullx.io/terminal?chainId=1399811149&address={token}"
        
        if confidence > 0.8:
            confidence_emoji = "🚀"
        elif confidence > 0.6:
            confidence_emoji = "🔥"
        else:
            confidence_emoji = "✅"
        
        message = (
            f"*{confidence_emoji} Nueva Señal #{signal_id}*\n\n"
            f"Token: `{token}`\n"
            f"Confianza: *{confidence:.2f}*\n"
            f"Predicción ML: *{ml_prediction:.2f}*\n\n"
            
            f"*📊 Datos:*\n"
            f"• Traders: `{trader_count}`\n"
            f"• Volumen: `${volume_usd:,.2f}`\n"
            f"• Market Cap: `${market_cap:,.2f}`\n"
            f"• % Compras: `{buy_percentage*100:.1f}%`\n"
            f"• Traders de élite: `{elite_traders}`\n"
            f"• Traders calidad: `{high_quality_traders}`\n\n"
            
            f"*🔍 Enlaces:*\n"
            f"• [DexScreener]({dexscreener_link})\n"
            f"• [Birdeye]({birdeye_link})\n"
            f"• [Neo BullX]({neobullx_link})\n\n"
            
            f"_Señal generada por ChipaTrading_"
        )
        return message
