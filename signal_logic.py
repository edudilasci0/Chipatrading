import time
import json
import asyncio
from datetime import datetime
from telegram_utils import send_telegram_message
import db
from config import Config
from performance_tracker import PerformanceTracker

class SignalLogic:
    """
    Agrupa transacciones por token y emite señales cuando 
    se cumplen las condiciones configuradas.
    """
    def __init__(self, scoring_system, dex_client, rugcheck_api=None, ml_predictor=None):
        """
        Inicializa la lógica de señales.
        
        Args:
            scoring_system: Instancia de ScoringSystem
            dex_client: Instancia de DexScreenerClient
            rugcheck_api: Instancia de RugCheckAPI
            ml_predictor: Instancia de SignalPredictor o None
        """
        self.scoring_system = scoring_system
        self.dex_client = dex_client
        self.rugcheck_api = rugcheck_api
        self.ml_predictor = ml_predictor
        
        # Tokens actuales que estamos monitoreando
        self.candidates = {}
        
        # Para evitar emitir señales duplicadas
        self.signaled_tokens = set()
        
        # Para evitar señales repetidas en tokens fallidos
        self.failed_tokens = {}  # {token: timestamp}
        
        # Tiempo de limpieza
        self.last_cleanup = time.time()
        
        # Seguimiento de rendimiento con intervalos específicos
        self.performance_tracker = PerformanceTracker(dex_client)
        
        # Historial de señales
        self.signals_history = []  # [(token, timestamp, confidence), ...]
        
        # Límite de señales por hora (throttling)
        self.signal_throttling = int(Config.get("signal_throttling", 10))
        self.signals_last_hour = []  # [(token, timestamp), ...]
        
        # Control de actividad por wallet
        self.wallet_activity = {}  # {wallet: [(timestamp, token), ...]}

    def add_transaction(self, wallet, token, usd_value, tx_type):
        """
        Agrega una transacción al tracking y actualiza el scoring.
        
        Args:
            wallet: Dirección del wallet
            token: Dirección del token
            usd_value: Valor en USD de la transacción
            tx_type: Tipo de transacción ("BUY" o "SELL")
        """
        now = int(time.time())
        
        # Verificar si el token ha fallado recientemente (últimas 24h)
        if token in self.failed_tokens:
            if now - self.failed_tokens[token] < 86400:  # 24 horas
                # No procesar transacciones de tokens fallidos
                return
            else:
                # Ya pasaron 24h, eliminar del registro de fallidos
                del self.failed_tokens[token]
        
        # Verificar también en la base de datos
        db_failed = db.get_failed_token(token)
        if db_failed and now - datetime.fromisoformat(db_failed["timestamp"]).timestamp() < 86400:
            return
        
        # Registrar actividad de wallet para throttling
        if wallet not in self.wallet_activity:
            self.wallet_activity[wallet] = []
        
        # Añadir transacción actual y limpiar actividad antigua
        self.wallet_activity[wallet].append((now, token))
        self.wallet_activity[wallet] = [
            (ts, tk) for ts, tk in self.wallet_activity[wallet] 
            if now - ts < 3600  # Mantener solo la última hora
        ]
        
        # Si la wallet está haciendo muchas transacciones, reducir su impacto
        high_activity_threshold = 20  # Más de 20 tx por hora se considera sospechoso
        if len(self.wallet_activity[wallet]) > high_activity_threshold:
            print(f"⚠️ Wallet {wallet} con actividad inusualmente alta ({len(self.wallet_activity[wallet])} tx/h)")
            usd_value = usd_value * 0.5  # Reducir el impacto de esta transacción
        
        # Si es un token nuevo, inicializar su estructura
        if token not in self.candidates:
            self.candidates[token] = {
                "traders": set(),
                "first_ts": now,
                "last_ts": now,
                "usd_total": 0,
                "transactions": [],
                "buys": 0,
                "sells": 0
            }
        
        # Actualizar datos del token
        candidate = self.candidates[token]
        candidate["traders"].add(wallet)
        candidate["last_ts"] = now
        candidate["usd_total"] += usd_value
        
        # Actualizar contadores por tipo
        if tx_type == "BUY":
            candidate["buys"] += 1
        elif tx_type == "SELL":
            candidate["sells"] += 1
        
        # Guardar la transacción
        tx_data = {
            "wallet": wallet,
            "usd_value": usd_value,
            "type": tx_type,
            "timestamp": now
        }
        candidate["transactions"].append(tx_data)
        
        # Actualizar puntuación del trader
        tx_for_scoring = {
            "wallet": wallet, 
            "token": token, 
            "type": tx_type, 
            "amount_usd": usd_value
        }
        self.scoring_system.update_score_on_trade(wallet, tx_for_scoring)
        
        # Limpiar tokens antiguos cada 5 minutos
        if now - self.last_cleanup > 300:
            self._cleanup_old_candidates(now)
            self.last_cleanup = now

    async def check_signals_periodically(self, interval=30):
        """
        Verifica las señales cada cierto intervalo de tiempo.
        
        Args:
            interval: Tiempo en segundos entre verificaciones
        """
        while True:
            try:
                self.check_signals()
                await asyncio.sleep(interval)
            except Exception as e:
                print(f"🚨 Error al verificar señales: {e}")
                await asyncio.sleep(interval)

    def check_signals(self):
        """
        Verifica qué tokens cumplen con las condiciones para emitir señal.
        """
        now = int(time.time())
        tokens_to_remove = []
        
        # Limpiar señales de más de 1 hora para throttling
        self.signals_last_hour = [(token, ts) for token, ts in self.signals_last_hour if now - ts <= 3600]
        
        # Verificar si ya se enviaron demasiadas señales esta hora
        if len(self.signals_last_hour) >= self.signal_throttling:
            print(f"⚠️ Límite de señales por hora alcanzado ({self.signal_throttling}). Esperando...")
            return
        
        for token, info in self.candidates.items():
            # Skip tokens that have already been signaled
            if token in self.signaled_tokens:
                continue
                
            first_ts = info["first_ts"]
            trader_count = len(info["traders"])
            time_window = now - first_ts
            
            # Verificar si cumple las condiciones para emitir señal
            min_traders = int(Config.get("min_traders_for_signal", Config.MIN_TRADERS_FOR_SIGNAL))
            signal_window = int(Config.get("signal_window_seconds", Config.SIGNAL_WINDOW_SECONDS))
            
            # Verificar condiciones básicas
            if time_window < signal_window or trader_count < min_traders:
                continue
            
            # Verificar actividad reciente - necesitamos actividad en el último 20% del tiempo
            recent_window = int(signal_window * 0.2)
            recent_threshold = now - recent_window
            recent_transactions = [tx for tx in info["transactions"] if tx["timestamp"] > recent_threshold]
            
            if len(recent_transactions) < 2:
                print(f"⚠️ Token {token} filtrado por falta de actividad reciente")
                continue
            
            # Obtener volumen y market cap
            vol_1h, market_cap, current_price = self.dex_client.update_volume_history(token)
            
            # Verificar volumen mínimo
            min_volume = float(Config.get("min_volume_usd", Config.MIN_VOLUME_USD))
            if vol_1h < min_volume:
                print(f"⚠️ Token {token} filtrado por bajo volumen: ${vol_1h:.2f}")
                continue
            
            # Obtener crecimiento de volumen
            vol_growth = self.dex_client.get_volume_growth(token)
            if vol_growth["growth_5m"] < 0.05:  # Al menos 5% de crecimiento en 5 min
                print(f"⚠️ Token {token} filtrado por crecimiento insuficiente: {vol_growth['growth_5m']:.2f}")
                continue
            
            # Obtener scores de los traders involucrados
            wallet_scores = []
            high_quality_traders = 0
            for wallet in info["traders"]:
                score = self.scoring_system.get_score(wallet)
                wallet_scores.append(score)
                if score >= 7.0:  # Trader de alta calidad
                    high_quality_traders += 1
            
            # Valorar más la presencia de traders de alta calidad
            if high_quality_traders == 0 and trader_count < 5:
                print(f"⚠️ Token {token} filtrado por falta de traders de calidad")
                continue
            
            # Calcular confianza de la señal
            confidence = self.scoring_system.compute_confidence(
                wallet_scores, vol_1h, market_cap, vol_growth["growth_5m"]
            )
            
            # Aplicar predicción ML si está disponible
            if self.ml_predictor and self.ml_predictor.model:
                try:
                    features = self._extract_signal_features(token, info, vol_1h, market_cap)
                    success_probability = self.ml_predictor.predict_success(features)
                    
                    # Combinar confianza basada en reglas con predicción ML
                    combined_confidence = (confidence * 0.7) + (success_probability * 0.3)
                    
                    print(f"Token {token}: Confianza base {confidence:.2f}, ML {success_probability:.2f}, combinada {combined_confidence:.2f}")
                    confidence = combined_confidence
                except Exception as e:
                    print(f"⚠️ Error al aplicar predicción ML: {e}")
            
            # Filtrar señales con baja confianza
            min_confidence = float(Config.get("min_confidence_threshold", Config.MIN_CONFIDENCE_THRESHOLD))
            if confidence < min_confidence:
                print(f"⚠️ Token {token} filtrado por baja confianza: {confidence:.2f}")
                continue
            
            # Verificar seguridad con RugCheck
            if not self._validate_token_safety(token):
                # Registrar como fallido
                self.failed_tokens[token] = now
                db.save_failed_token(token, "Fallo en validación de seguridad")
                continue
            
            # Si pasa todas las validaciones, emitir señal
            self.emit_signal(token, info, confidence, vol_1h, market_cap, current_price)
            self.signaled_tokens.add(token)
            self.signals_history.append((token, now, confidence))
            self.signals_last_hour.append((token, now))
            
            # Iniciar seguimiento de rendimiento
            signal_info = {
                "confidence": confidence,
                "traders_count": trader_count,
                "total_volume": info["usd_total"]
            }
            self.performance_tracker.add_signal(token, signal_info)
            
            # Marcar para eliminar de candidatos
            tokens_to_remove.append(token)
                
        # Eliminar tokens que ya han sido procesados
        for token in tokens_to_remove:
            if token in self.candidates:
                del self.candidates[token]
    
    def _validate_token_safety(self, token):
        """
        Valida la seguridad del token usando RugCheck.
        
        Args:
            token: Dirección del token
            
        Returns:
            bool: True si el token es seguro
        """
        rugcheck_safe = True
        if self.rugcheck_api:
            try:
                min_rugcheck_score = int(Config.get("rugcheck_min_score", 50))
                rugcheck_safe = self.rugcheck_api.validate_token_safety(token, min_rugcheck_score)
                if not rugcheck_safe:
                    print(f"⚠️ Token {token} falló la validación de RugCheck")
                    return False
            except Exception as e:
                print(f"⚠️ Error al verificar RugCheck: {e}")
                # En caso de error, asumimos que el token es seguro
                # para no bloquear operaciones por un fallo técnico
        
        return True
        
    def _extract_signal_features(self, token, info, vol_1h, market_cap):
        """
        Extrae características para predicción ML.
        
        Args:
            token: Dirección del token
            info: Información del token
            vol_1h: Volumen de la última hora
            market_cap: Market cap del token
            
        Returns:
            dict: Características para el modelo ML
        """
        trader_count = len(info["traders"])
        transaction_count = len(info["transactions"])
        
        # Calcular ratio de compras/ventas
        total_txs = info.get("buys", 0) + info.get("sells", 0)
        buy_ratio = (info.get("buys", 0) / total_txs) if total_txs > 0 else 0
        
        # Calcular velocidad de transacciones
        time_window = info["last_ts"] - info["first_ts"]
        tx_velocity = transaction_count / max(time_window / 60, 1)  # transacciones por minuto
        
        # Obtener scores de traders
        wallet_scores = []
        for wallet in info["traders"]:
            score = self.scoring_system.get_score(wallet)
            wallet_scores.append(score)
        
        avg_trader_score = sum(wallet_scores) / len(wallet_scores) if wallet_scores else 0
        max_trader_score = max(wallet_scores) if wallet_scores else 0
        
        # Obtener crecimiento de volumen
        vol_growth = self.dex_client.get_volume_growth(token)
        
        # Crear diccionario de features
        features = {
            "num_traders": trader_count,
            "num_transactions": transaction_count,
            "total_volume_usd": info["usd_total"],
            "avg_volume_per_trader": info["usd_total"] / trader_count if trader_count > 0 else 0,
            "buy_ratio": buy_ratio,
            "tx_velocity": tx_velocity,
            "avg_trader_score": avg_trader_score,
            "max_trader_score": max_trader_score,
            "market_cap": market_cap,
            "volume_1h": vol_1h,
            "volume_growth_5m": vol_growth.get("growth_5m", 0),
            "volume_growth_1h": vol_growth.get("growth_1h", 0)
        }
        
        return features
                
    def emit_signal(self, token, info, confidence, vol_1h, market_cap, current_price):
        """
        Envía un mensaje a Telegram con la información de la señal.
        
        Args:
            token: Dirección del token
            info: Información del token candidato
            confidence: Nivel de confianza calculado
            vol_1h: Volumen de la última hora
            market_cap: Market cap del token
            current_price: Precio actual del token
        """
        traders_list = ", ".join([f"`{w[:8]}...{w[-6:]}`" for w in info["traders"]])
        usd_total = info["usd_total"]
        usd_per_trader = usd_total / len(info["traders"]) if info["traders"] else 0
        
        # Calcular ratio de compras/ventas
        total_txs = info.get("buys", 0) + info.get("sells", 0)
        buy_percent = (info.get("buys", 0) / total_txs * 100) if total_txs > 0 else 0
        
        # Formatear cantidades para mejor legibilidad
        formatted_vol = f"${vol_1h:,.2f}" if vol_1h else "Desconocido"
        formatted_mcap = f"${market_cap:,.2f}" if market_cap else "Desconocido"
        formatted_usd = f"${usd_total:,.2f}"
        formatted_avg = f"${usd_per_trader:,.2f}"
        
        # Determinar emoji según confianza
        if confidence > 0.8:
            confidence_emoji = "🔥"  # Muy alta
        elif confidence > 0.6:
            confidence_emoji = "✅"  # Alta
        elif confidence > 0.4:
            confidence_emoji = "⚠️"  # Media
        else:
            confidence_emoji = "❓"  # Baja
        
        # Obtener traders de alta calidad
        high_quality_traders = [w for w in info["traders"] if self.scoring_system.get_score(w) >= 7.0]
        high_quality_count = len(high_quality_traders)
        
        msg = (
            "*🚀 Señal Daily Runner Detectada*\n\n"
            f"Token: `{token}`\n\n"
            f"👥 *Traders Involucrados ({len(info['traders'])})*:\n{traders_list}\n"
            f"🎯 Traders de calidad: {high_quality_count}\n\n"
            f"💰 Volumen Total: {formatted_usd}\n"
            f"📊 Promedio por Trader: {formatted_avg}\n"
            f"📈 Ratio Compras/Ventas: {buy_percent:.1f}%\n\n"
            f"🔍 *Métricas de Mercado*:\n"
            f"• Vol. 1h: {formatted_vol}\n"
            f"• Market Cap: {formatted_mcap}\n"
            f"• Precio: ${current_price:.10f}\n"
            f"• Confianza: *{confidence:.2f}* {confidence_emoji}\n\n"
            f"⏰ Detectado en ventana de {Config.get('signal_window_seconds', Config.SIGNAL_WINDOW_SECONDS)/60:.1f} minutos\n\n"
            f"📊 *Seguimiento*: 10m, 30m, 1h, 2h, 4h y 24h"
        )
        
        # Enviar mensaje a Telegram
        send_telegram_message(msg)
        
        # Guardar señal en la base de datos
        signal_id = db.save_signal(token, len(info["traders"]), confidence, current_price)
        
        print(f"📢 Señal emitida para el token {token} con confianza {confidence:.2f}")
        
        # Retornar el ID de la señal para futuras referencias
        return signal_id
    
    def _cleanup_old_candidates(self, current_time, max_age=3600):
        """
        Limpia tokens viejos para evitar acumular demasiados datos.
        
        Args:
            current_time: Timestamp actual
            max_age: Edad máxima en segundos antes de eliminar un candidato
        """
        tokens_to_remove = []
        for token, info in self.candidates.items():
            if current_time - info["last_ts"] > max_age:
                tokens_to_remove.append(token)
                
        for token in tokens_to_remove:
            del self.candidates[token]
            
        if tokens_to_remove:
            print(f"🧹 Se eliminaron {len(tokens_to_remove)} tokens antiguos del tracking")
            
    def get_active_candidates_count(self):
        """
        Retorna el número de tokens que están siendo monitoreados actualmente.
        """
        return len(self.candidates)
        
    def get_recent_signals(self, hours=24):
        """
        Retorna las señales emitidas en las últimas X horas.
        
        Args:
            hours: Horas hacia atrás para buscar señales
            
        Returns:
            list: Lista de tuplas (token, timestamp, confidence)
        """
        now = time.time()
        cutoff = now - (hours * 3600)
        
        return [(token, ts, conf) for token, ts, conf in self.signals_history 
                if ts > cutoff]
