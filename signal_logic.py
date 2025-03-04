import time
import asyncio
import db
from config import Config
from telegram_utils import send_telegram_message

class SignalLogic:
    """
    Implementa la lógica para detectar y emitir señales de trading.
    """

    def __init__(self, scoring_system=None, dex_client=None, rugcheck_api=None, ml_predictor=None):
        """
        Inicializa la lógica de señales.
        """
        self.scoring_system = scoring_system
        self.dex_client = dex_client
        self.rugcheck_api = rugcheck_api
        self.ml_predictor = ml_predictor
        self.candidates = {}  # {token: {traders: set, transactions: list, usd_total: float, first_ts: int, last_ts: int}}
        self.signaled_tokens = set()  # Tokens que ya han generado señal
        self.signals_last_hour = []  # [(token, timestamp), ...] para throttling
        self.signals_history = []  # [(token, timestamp, confidence, signal_id), ...]
        self.failed_tokens = {}  # {token: timestamp} para tokens que no pasaron validación
        self.token_types = {}  # {token: type} para cachear tipos de token
        self.performance_tracker = None  # Se asignará desde fuera si es necesario
        self.signal_throttling = int(Config.get("signal_throttling", 10))  # Máximo de señales por hora
    
    def add_transaction(self, wallet, token, amount_usd, tx_type):
        """
        Añade una transacción a la lógica de señales.
        
        Args:
            wallet: Dirección del wallet
            token: Dirección del token
            amount_usd: Valor en USD
            tx_type: Tipo de transacción (BUY, SELL)
        """
        now = int(time.time())
        
        # Filtrar tokens ya señalados
        if token in self.signaled_tokens:
            return
            
        # Filtrar tokens con errores conocidos
        if token in self.failed_tokens:
            last_fail = self.failed_tokens[token]
            if now - last_fail < 3600:  # Ignorar por 1 hora
                return
        
        # Crear entrada para el token si no existe
        if token not in self.candidates:
            self.candidates[token] = {
                "traders": set(),
                "transactions": [],
                "usd_total": 0.0,
                "first_ts": now,
                "last_ts": now,
                "buys": 0,
                "sells": 0
            }
        
        # Actualizar información del token
        self.candidates[token]["traders"].add(wallet)
        self.candidates[token]["last_ts"] = now
        self.candidates[token]["usd_total"] += amount_usd
        
        # Registrar tipo de transacción
        if tx_type == "BUY":
            self.candidates[token]["buys"] += 1
        elif tx_type == "SELL":
            self.candidates[token]["sells"] += 1
        
        # Añadir transacción a la lista
        self.candidates[token]["transactions"].append({
            "wallet": wallet,
            "amount_usd": amount_usd,
            "timestamp": now,
            "type": tx_type
        })
    
    def _generate_signal_id(self, token):
        """
        Genera un ID único para una señal.
        
        Args:
            token: Dirección del token
            
        Returns:
            str: ID de la señal (ej: DR-123)
        """
        import random
        random_num = random.randint(100, 999)
        return f"DR-{random_num}"
    
    def _validate_token_safety(self, token):
        """
        Valida la seguridad de un token usando RugCheck.
        
        Args:
            token: Dirección del token
            
        Returns:
            bool: True si es seguro, False si no
        """
        # Si no hay API de RugCheck, asumir que es seguro
        if not self.rugcheck_api:
            return True
            
        try:
            # Obtener umbral mínimo de score
            min_score = int(Config.get("rugcheck_min_score", 50))
            
            # Validar con RugCheck
            is_safe = self.rugcheck_api.validate_token_safety(token, min_score)
            
            # Si no es seguro, registrar
            if not is_safe:
                print(f"🚫 Token {token} no pasó validación de seguridad")
                
            return is_safe
        except Exception as e:
            print(f"⚠️ Error validando seguridad de {token}: {e}")
            return False
    
    def _get_token_type(self, market_cap, vol_1h, vol_growth):
        """
        Determina el tipo de token basado en sus métricas.
        
        Args:
            market_cap: Market cap en USD
            vol_1h: Volumen en la última hora
            vol_growth: Crecimiento de volumen
            
        Returns:
            tuple: (tipo, emoji)
        """
        # Si market cap es muy bajo, probablemente sea nuevo
        if market_cap < 500000:
            return "New Launch", "🆕"
            
        # Si crecimiento de volumen es alto, puede ser meme o hype
        if vol_growth > 0.5:  # +50%
            return "Momentum Token", "🔥"
            
        # Si volumen es alto pero market cap bajo/medio
        if vol_1h > 50000 and market_cap < 5000000:
            return "Daily Runner", "🏃"
            
        # Por defecto
        return "Standard Token", "🪙"
    
    def _get_risk_level(self, market_cap, vol_1h):
        """
        Determina el nivel de riesgo de un token.
        
        Args:
            market_cap: Market cap en USD
            vol_1h: Volumen en la última hora
            
        Returns:
            tuple: (nivel, emoji, descripción)
        """
        # Riesgo muy alto: market cap muy bajo
        if market_cap < 200000 or vol_1h < 5000:
            return "Muy Alto", "⚠️", "Alto potencial de pérdida"
            
        # Riesgo alto: market cap bajo
        if market_cap < 1000000 or vol_1h < 20000:
            return "Alto", "🔴", "Alta volatilidad"
            
        # Riesgo medio: market cap medio
        if market_cap < 5000000 or vol_1h < 50000:
            return "Medio", "🟠", "Volatilidad moderada"
            
        # Riesgo bajo
        return "Bajo", "🟢", "Token establecido"
    
    def get_active_candidates_count(self):
        """
        Retorna el número de tokens candidatos activos.
        
        Returns:
            int: Número de candidatos
        """
        return len(self.candidates)
    
    def get_recent_signals(self, hours=24):
        """
        Retorna las señales emitidas en las últimas X horas.
        
        Args:
            hours: Número de horas a considerar
            
        Returns:
            list: Lista de tuplas (token, timestamp, confidence, signal_id)
        """
        now = int(time.time())
        cutoff = now - (hours * 3600)
        
        return [(token, ts, confidence, signal_id) 
                for token, ts, confidence, signal_id in self.signals_history 
                if ts > cutoff]
    
    async def check_signals_periodically(self, interval=60):
        """
        Ejecuta check_signals periódicamente.
        
        Args:
            interval: Intervalo en segundos entre verificaciones
        """
        while True:
            try:
                self.check_signals()
            except Exception as e:
                print(f"⚠️ Error en check_signals: {e}")
                
            await asyncio.sleep(interval)

    def check_signals(self):
        """
        Verifica qué tokens cumplen con las condiciones para emitir señal.
        Versión optimizada con evaluación en cascada y factores adicionales.
        """
        now = int(time.time())
        tokens_to_remove = []
        
        # Limpiar señales de más de 1 hora para throttling
        self.signals_last_hour = [(token, ts) for token, ts in self.signals_last_hour if now - ts <= 3600]
        
        # Verificar si ya se enviaron demasiadas señales esta hora
        if len(self.signals_last_hour) >= self.signal_throttling:
            print(f"⚠️ Límite de señales por hora alcanzado ({self.signal_throttling}). Esperando...")
            return
        
        # OPTIMIZACIÓN: Ordenar tokens candidatos por potencial (número de traders * volumen total)
        # Esto asegura que procesemos primero los tokens más prometedores
        sorted_candidates = sorted(
            self.candidates.items(),
            key=lambda x: len(x[1]["traders"]) * x[1].get("usd_total", 0),
            reverse=True
        )
        
        # Variables para contadores y logging
        tokens_evaluated = 0
        tokens_filtered_early = 0
        tokens_filtered_volume = 0
        tokens_filtered_recent = 0
        tokens_filtered_rugcheck = 0
        tokens_filtered_confidence = 0
        tokens_signaled = 0
        
        # Obtener umbrales de configuración
        min_traders = int(Config.get("min_traders_for_signal", Config.MIN_TRADERS_FOR_SIGNAL))
        signal_window = int(Config.get("signal_window_seconds", Config.SIGNAL_WINDOW_SECONDS))
        min_volume = float(Config.get("min_volume_usd", Config.MIN_VOLUME_USD))
        min_confidence = float(Config.get("min_confidence_threshold", Config.MIN_CONFIDENCE_THRESHOLD))
        
        for token, info in sorted_candidates:
            tokens_evaluated += 1
            
            # Skip tokens que ya han sido señalados
            if token in self.signaled_tokens:
                continue
                
            # OPTIMIZACIÓN: Filtros básicos rápidos primero
            first_ts = info["first_ts"]
            trader_count = len(info["traders"])
            time_window = now - first_ts
            
            # Condiciones básicas
            if time_window < signal_window or trader_count < min_traders:
                tokens_filtered_early += 1
                continue
            
            # OPTIMIZACIÓN: Verificar actividad reciente temprano
            recent_window = int(signal_window * 0.2)
            recent_threshold = now - recent_window
            recent_transactions = [tx for tx in info["transactions"] if tx["timestamp"] > recent_threshold]
            
            if len(recent_transactions) < 2:
                print(f"⚠️ Token {token} filtrado por falta de actividad reciente")
                tokens_filtered_recent += 1
                continue
            
            # OPTIMIZACIÓN: Verificar volumen antes de cálculos más costosos
            vol_1h, market_cap, current_price = self.dex_client.update_volume_history(token)
            
            if vol_1h < min_volume:
                print(f"⚠️ Token {token} filtrado por bajo volumen: ${vol_1h:.2f}")
                tokens_filtered_volume += 1
                continue
            
            # Calcular crecimiento de volumen
            vol_growth = self.dex_client.get_volume_growth(token)
            min_growth = 0.05  # Mínimo 5% crecimiento en 5min
            
            if vol_growth["growth_5m"] < min_growth:
                print(f"⚠️ Token {token} filtrado por crecimiento insuficiente: {vol_growth['growth_5m']:.2f}")
                tokens_filtered_volume += 1
                continue
            
            # OPTIMIZACIÓN: Verificar seguridad antes del cálculo costoso de confianza
            if not self._validate_token_safety(token):
                # Registrar como fallido
                self.failed_tokens[token] = now
                db.save_failed_token(token, "Fallo en validación de seguridad")
                tokens_filtered_rugcheck += 1
                continue
            
            # OPTIMIZACIÓN: Análisis mejorado de calidad de traders
            wallet_scores = []
            high_quality_traders = 0
            elite_traders = 0  # NUEVO: Traders de élite (score 9+)
            
            for wallet in info["traders"]:
                score = self.scoring_system.get_score(wallet)
                wallet_scores.append(score)
                if score >= 7.0:  # Trader de alta calidad
                    high_quality_traders += 1
                if score >= 9.0:  # NUEVO: Trader de élite
                    elite_traders += 1
            
            # OPTIMIZACIÓN: Filtro basado en calidad, no solo cantidad
            if high_quality_traders == 0 and trader_count < 5:
                print(f"⚠️ Token {token} filtrado por falta de traders de calidad")
                tokens_filtered_confidence += 1
                continue
            
            # OPTIMIZACIÓN: Calcular factores adicionales para la confianza
            
            # 1. Velocidad de transacciones (tx por minuto)
            tx_velocity = len(info["transactions"]) / max(1, (now - first_ts) / 60)
            
            # 2. Proporción compras/ventas
            total_txs = info.get("buys", 0) + info.get("sells", 0)
            buy_sell_ratio = info.get("buys", 0) / max(1, total_txs)
            
            # Calcular confianza base
            confidence = self.scoring_system.compute_confidence(
                wallet_scores, vol_1h, market_cap, vol_growth["growth_5m"]
            )
            
            # OPTIMIZACIÓN: Ajustar confianza con factores adicionales
            
            # Factor de velocidad - mayor velocidad indica más momentum
            velocity_factor = min(tx_velocity / 3.0, 1.0)  # Normalizado a máximo 1.0
            
            # Factor de compra/venta - preferimos mayoría de compras
            buy_factor = min(buy_sell_ratio * 1.5, 1.2)  # Máximo 20% boost
            
            # Factor de traders de élite
            elite_factor = 1.0
            if elite_traders > 0:
                elite_factor = 1.0 + (elite_traders * 0.05)  # 5% boost por cada trader de élite
            
            # Aplicar ajustes
            adjusted_confidence = confidence * velocity_factor * buy_factor * elite_factor
            
            # OPTIMIZACIÓN: Aplicar predicción ML si disponible
            ml_confidence = None
            if self.ml_predictor and self.ml_predictor.model:
                try:
                    # Preparar features incluyendo nuevos factores
                    features = self._extract_signal_features(token, info, vol_1h, market_cap)
                    # Añadir nuevos features
                    features["tx_velocity"] = tx_velocity
                    features["buy_sell_ratio"] = buy_sell_ratio
                    features["high_quality_ratio"] = high_quality_traders / max(1, trader_count)
                    features["elite_trader_count"] = elite_traders
                    
                    success_probability = self.ml_predictor.predict_success(features)
                    ml_confidence = success_probability
                    
                    # OPTIMIZACIÓN: Dar más peso al ML si tiene buena precisión
                    ml_weight = 0.3
                    if self.ml_predictor.accuracy and self.ml_predictor.accuracy > 0.7:
                        ml_weight = 0.5  # Dar más peso si el modelo es preciso
                    
                    final_confidence = (adjusted_confidence * (1 - ml_weight)) + (success_probability * ml_weight)
                    
                    print(f"Token {token}: Confianza base {confidence:.2f}, ajustada {adjusted_confidence:.2f}, "
                          f"ML {success_probability:.2f}, final {final_confidence:.2f}")
                    
                    confidence = final_confidence
                except Exception as e:
                    print(f"⚠️ Error al aplicar predicción ML: {e}")
                    confidence = adjusted_confidence
            else:
                confidence = adjusted_confidence
            
            # Filtrar señales con baja confianza
            if confidence < min_confidence:
                print(f"⚠️ Token {token} filtrado por baja confianza: {confidence:.2f}")
                tokens_filtered_confidence += 1
                continue
            
            # OPTIMIZACIÓN: Añadir métricas de calidad a la información de señal
            enhanced_info = info.copy()
            enhanced_info["tx_velocity"] = tx_velocity
            enhanced_info["buy_sell_ratio"] = buy_sell_ratio
            enhanced_info["high_quality_traders"] = high_quality_traders
            enhanced_info["elite_traders"] = elite_traders
            
            # Si pasa todas las validaciones, emitir señal
            signal_id = self.emit_signal(token, enhanced_info, confidence, vol_1h, market_cap, current_price, ml_confidence)
            self.signaled_tokens.add(token)
            
            # Añadir a historial con ID de señal
            self.signals_history.append((token, now, confidence, signal_id))
            self.signals_last_hour.append((token, now))
            
            # Iniciar seguimiento de rendimiento con ID
            signal_info = {
                "signal_id": signal_id,
                "confidence": confidence,
                "traders_count": trader_count,
                "total_volume": info["usd_total"]
            }
            if self.performance_tracker:
                self.performance_tracker.add_signal(token, signal_info)
            
            # Marcar para eliminar de candidatos
            tokens_to_remove.append(token)
            tokens_signaled += 1
            
            # OPTIMIZACIÓN: Limitar el número de señales por ejecución para no saturar
            if tokens_signaled >= 3:  # Máximo 3 señales por ejecución
                break
                
        # Eliminar tokens que ya han sido procesados
        for token in tokens_to_remove:
            if token in self.candidates:
                del self.candidates[token]
        
        # OPTIMIZACIÓN: Log de estadísticas de ejecución para análisis
        if tokens_evaluated > 0:
            print(f"📊 Estadísticas de check_signals: evaluados={tokens_evaluated}, "
                  f"señales={tokens_signaled}, filtros_tempranos={tokens_filtered_early}, "
                  f"filtros_volumen={tokens_filtered_volume}, filtros_recientes={tokens_filtered_recent}, "
                  f"filtros_rugcheck={tokens_filtered_rugcheck}, filtros_confianza={tokens_filtered_confidence}")

    def _extract_signal_features(self, token, info, vol_1h, market_cap):
        """
        Extrae características para predicción ML con features adicionales.
        
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
        
        # Calcular velocidad de acumulación por traders
        tx_per_trader = transaction_count / max(1, trader_count)
        
        # Obtener scores de traders
        wallet_scores = []
        for wallet in info["traders"]:
            score = self.scoring_system.get_score(wallet)
            wallet_scores.append(score)
        
        # Calcular estadísticas de calidad de traders
        avg_trader_score = sum(wallet_scores) / len(wallet_scores) if wallet_scores else 0
        max_trader_score = max(wallet_scores) if wallet_scores else 0
        min_trader_score = min(wallet_scores) if wallet_scores else 0
        
        # Calcular distribución de scores
        high_quality_count = sum(1 for s in wallet_scores if s >= 7.0)
        elite_count = sum(1 for s in wallet_scores if s >= 9.0)
        high_quality_ratio = high_quality_count / max(1, len(wallet_scores))
        
        # Obtener crecimiento de volumen
        vol_growth = self.dex_client.get_volume_growth(token)
        
        # OPTIMIZACIÓN: Normalizar algunas features para mejor rendimiento del modelo
        normalized_mcap = min(market_cap / 10000000, 1.0)  # Normalizar a 10M
        normalized_volume = min(vol_1h / 50000, 1.0)  # Normalizar a 50K
        
        # Crear diccionario de features
        features = {
            "num_traders": trader_count,
            "num_transactions": transaction_count,
            "total_volume_usd": info["usd_total"],
            "avg_volume_per_trader": info["usd_total"] / trader_count if trader_count > 0 else 0,
            "buy_ratio": buy_ratio,
            "tx_velocity": tx_velocity,
            "tx_per_trader": tx_per_trader,
            "avg_trader_score": avg_trader_score,
            "max_trader_score": max_trader_score,
            "min_trader_score": min_trader_score,
            "high_quality_ratio": high_quality_ratio,
            "elite_trader_count": elite_count,
            "market_cap": market_cap,
            "normalized_mcap": normalized_mcap,
            "volume_1h": vol_1h,
            "normalized_volume": normalized_volume,
            "volume_growth_5m": vol_growth.get("growth_5m", 0),
            "volume_growth_1h": vol_growth.get("growth_1h", 0)
        }
        
        return features

    def emit_signal(self, token, info, confidence, vol_1h, market_cap, current_price, ml_confidence=None):
        """
        Versión mejorada que incluye más información de calidad en la señal.
        
        Args:
            token: Dirección del token
            info: Información del token candidato (versión mejorada)
            confidence: Nivel de confianza calculado
            vol_1h: Volumen de la última hora
            market_cap: Market cap del token
            current_price: Precio actual del token
            ml_confidence: Confianza calculada por ML (opcional)
            
        Returns:
            str: ID de la señal
        """
        # Generar ID único para la señal
        signal_id = self._generate_signal_id(token)
        
        # Obtener tipo de token
        vol_growth = self.dex_client.get_volume_growth(token)
        token_type, type_emoji = self._get_token_type(market_cap, vol_1h, vol_growth.get("growth_5m", 0))
        
        # Obtener nivel de riesgo
        risk_level, risk_emoji, risk_desc = self._get_risk_level(market_cap, vol_1h)
        
        # OPTIMIZACIÓN: Destacar traders de calidad
        traders_list = ", ".join([f"`{w[:8]}...{w[-6:]}`" for w in info["traders"]])
        usd_total = info["usd_total"]
        usd_per_trader = usd_total / len(info["traders"]) if info["traders"] else 0
        
        # Calcular ratio de compras/ventas
        total_txs = info.get("buys", 0) + info.get("sells", 0)
        buy_percent = (info.get("buys", 0) / total_txs * 100) if total_txs > 0 else 0
        
        # NUEVO: Extraer métricas de calidad
        high_quality_traders = info.get("high_quality_traders", 0)
        elite_traders = info.get("elite_traders", 0)
        tx_velocity = info.get("tx_velocity", 0)
        
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
        
        # Crear enlaces para exploradores
        dexscreener_link = f"https://dexscreener.com/solana/{token}"
        birdeye_link = f"https://birdeye.so/token/{token}?chain=solana"
        neobullx_link = f"https://neo.bullx.io/terminal?chainId=1399811149&address={token}"
        
        # OPTIMIZACIÓN: Mensaje mejorado con más datos de calidad
        msg = (
            f"*🚀 Señal Daily Runner {signal_id}*\n\n"
            f"Token: `{token}`\n"
            f"{type_emoji} Tipo: *{token_type}*\n"
            f"{risk_emoji} Riesgo: *{risk_level}* ({risk_desc})\n\n"
            f"👥 *Traders Involucrados ({len(info['traders'])})*:\n{traders_list}\n"
            f"🎯 Traders de calidad: {high_quality_traders}\n"
            f"🏆 Traders de élite: {elite_traders}\n\n"
            f"💰 Volumen Total: {formatted_usd}\n"
            f"📊 Promedio por Trader: {formatted_avg}\n"
            f"📈 Ratio Compras/Ventas: {buy_percent:.1f}%\n"
            f"⚡ Velocidad: {tx_velocity:.1f} tx/min\n\n"
            f"🔍 *Métricas de Mercado*:\n"
            f"• Vol. 1h: {formatted_vol}\n"
            f"• Market Cap: {formatted_mcap}\n"
            f"• Precio: ${current_price:.10f}\n"
        )
        
        # Añadir información de confianza
        if ml_confidence is not None:
            msg += (
                f"• Confianza Sistema: *{confidence:.2f}* {confidence_emoji}\n"
                f"• Confianza ML: *{ml_confidence:.2f}*\n\n"
            )
        else:
            msg += f"• Confianza: *{confidence:.2f}* {confidence_emoji}\n\n"
        
        # Añadir enlaces y otros detalles
        msg += (
            f"🔗 *Enlaces*:\n"
            f"• [DexScreener]({dexscreener_link})\n"
            f"• [Birdeye]({birdeye_link})\n"
            f"• [Neo BullX]({neobullx_link})\n\n"
            f"⏰ Detectado en ventana de {Config.get('signal_window_seconds', Config.SIGNAL_WINDOW_SECONDS)/60:.1f} minutos\n\n"
            f"📊 *Seguimiento* {signal_id}: 10m, 30m, 1h, 2h, 4h y 24h"
        )
        
        # Enviar mensaje a Telegram
        send_telegram_message(msg)
        
        # Guardar señal en la base de datos con datos adicionales
        db_signal_id = db.save_signal(token, len(info["traders"]), confidence, current_price)
        
        print(f"📢 Señal {signal_id} emitida para el token {token} con confianza {confidence:.2f}")
        
        # Retornar el ID de la señal para futuras referencias
        return signal_id
