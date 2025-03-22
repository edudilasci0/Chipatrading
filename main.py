# scoring.py
import time
import math
import logging
from config import Config
import db

logger = logging.getLogger("scoring_system")

class ScoringSystem:
    def __init__(self):
        self.local_cache = {}  # {wallet: score}
        self.last_cache_cleanup = time.time()
        self.wallet_tx_count = {}  # {wallet: count}
        self.boosters = {}  # {wallet: {multiplier, expires, active}}
        self.token_type_scores = {}  # {token_type: multiplier}
        self.wallet_token_buys = {}  # {wallet: {token: [timestamps]}}
        self.wallet_profits = {}  # {wallet: {token: profit}}
        self.trader_performance = {}  # {wallet: {win_rate, avg_profit}}
        
        # Inicializar multiplicadores para diferentes tipos de token
        self._init_token_type_boosters()
        
        # Cargar scores iniciales
        self._load_initial_scores()

    def _init_token_type_boosters(self):
        """Inicializa los multiplicadores por tipo de token"""
        self.token_type_scores = {
            "meme": 1.3,
            "defi": 1.1,
            "nft": 1.15,
            "gaming": 1.1,
            "ai": 1.2,
            "new": 1.25,
            "exchange": 1.0,
            "stable": 0.9
        }

    def _load_initial_scores(self):
        """Carga scores iniciales desde la base de datos"""
        try:
            wallet_scores = db.execute_cached_query(
                "SELECT wallet, score FROM wallet_scores",
                max_age=3600
            )
            for item in wallet_scores:
                self.local_cache[item['wallet']] = float(item['score'])
            logger.info(f"Loaded {len(wallet_scores)} initial wallet scores")
        except Exception as e:
            logger.error(f"Error loading initial scores: {e}")

    def get_score(self, wallet):
        """
        Obtiene el score actual de un wallet, considerando boosters activos
        
        Args:
            wallet: Dirección del wallet
            
        Returns:
            float: Score entre 0 y 10
        """
        if wallet not in self.local_cache:
            self.local_cache[wallet] = db.get_wallet_score(wallet)
        
        base_score = self.local_cache[wallet]
        
        # Aplicar booster si está activo
        if wallet in self.boosters and self.boosters[wallet]['active']:
            if time.time() < self.boosters[wallet]['expires']:
                return min(10.0, base_score * self.boosters[wallet]['multiplier'])
            else:
                self.boosters[wallet]['active'] = False
        
        return base_score

    def update_score_on_trade(self, wallet, tx_data):
        """
        Actualiza el score de un wallet basado en una transacción,
        considerando decay y otros factores de peso
        
        Args:
            wallet: Dirección del wallet
            tx_data: Datos de la transacción
        """
        if wallet not in self.local_cache:
            current_score = db.get_wallet_score(wallet)
            self.local_cache[wallet] = current_score
        else:
            current_score = self.local_cache[wallet]
        
        # Inicializar contadores de transacciones
        if wallet not in self.wallet_tx_count:
            self.wallet_tx_count[wallet] = 0
        
        # Obtener parámetros de la transacción
        token = tx_data.get("token", "")
        amount_usd = tx_data.get("amount_usd", 0)
        tx_type = tx_data.get("type", "").upper()
        timestamp = tx_data.get("timestamp", time.time())
        
        # Obtener factores de ajuste desde configuración
        decay_factor = float(Config.get("SCORE_DECAY_FACTOR", 0.995))
        min_tx_factor = float(Config.get("MIN_TX_SCORE_IMPACT", 0.01))
        max_tx_factor = float(Config.get("MAX_TX_SCORE_IMPACT", 0.2))
        
        # Aplicar decay temporal al score existente
        if wallet in self.local_cache:
            time_since_last_update = timestamp - self.last_cache_cleanup
            days_since_update = time_since_last_update / (24 * 3600)
            decay_multiplier = decay_factor ** days_since_update
            current_score = current_score * decay_multiplier
        
        # Calcular impacto de la transacción actual
        # Impacto base basado en monto (USD)
        base_impact = 0.01 + (amount_usd / 5000) * 0.1  # 0.01 a 0.11 para $0-$5000
        base_impact = min(base_impact, max_tx_factor)  # Limitar impacto máximo
        
        # Ajustar el impacto según el tipo de transacción y otras condiciones
        if tx_type == "BUY":
            impact_multiplier = 1.0
            
            # Registro de compras para análisis de patrón
            if wallet not in self.wallet_token_buys:
                self.wallet_token_buys[wallet] = {}
            if token not in self.wallet_token_buys[wallet]:
                self.wallet_token_buys[wallet][token] = []
            self.wallet_token_buys[wallet][token].append(timestamp)
            
            # ¿El trader tiende a comprar antes de otros? (indicador de calidad)
            if self._is_early_buyer(wallet, token, timestamp):
                impact_multiplier *= 1.2
                
        elif tx_type == "SELL":
            impact_multiplier = 0.8  # Impacto ligeramente menor para ventas
            
            # ¿El trader vendió con beneficio?
            profit_pct = self._calculate_trade_profit(wallet, token, tx_data)
            if profit_pct > 0:
                # Actualizar histórico de ganancias
                if wallet not in self.wallet_profits:
                    self.wallet_profits[wallet] = {}
                if token not in self.wallet_profits[wallet]:
                    self.wallet_profits[wallet][token] = []
                self.wallet_profits[wallet][token].append(profit_pct)
                
                # Ajustar impacto según ganancia
                if profit_pct > 20:
                    impact_multiplier = 1.5  # Ganancia superior al 20%
                elif profit_pct > 10:
                    impact_multiplier = 1.3  # Ganancia superior al 10%
                else:
                    impact_multiplier = 1.1  # Ganancia positiva
        else:
            impact_multiplier = 0.5  # Otros tipos de transacciones
        
        # Factores adicionales
        
        # 1. Consistencia - ¿El trader opera regularmente?
        self.wallet_tx_count[wallet] = self.wallet_tx_count.get(wallet, 0) + 1
        consistency_bonus = min(self.wallet_tx_count[wallet] / 100, 0.2)  # Hasta +0.2 por cada 100 tx
        
        # 2. Calidad de tokens - ¿El trader opera tokens de calidad?
        token_quality = self._get_token_quality(token)
        token_factor = token_quality * 0.1  # Hasta +0.1 por tokens de calidad
        
        # 3. Verificar si hay actividad de whales en el token
        whale_activity = self._check_whale_activity(token)
        if whale_activity and amount_usd > float(Config.get("WHALE_TRANSACTION_THRESHOLD", 10000)):
            impact_multiplier *= 1.3  # Bonus por actividad de ballena
        
        # 4. Verificar si el token está en trending
        is_trending = self._check_token_trending(token)
        if is_trending:
            impact_multiplier *= 1.1  # Bonus por token trending
        
        # Calcular el impacto final
        final_impact = base_impact * impact_multiplier
        final_impact = max(min_tx_factor, min(final_impact, max_tx_factor))
        
        # Añadir bonificaciones de consistencia y token
        final_impact += consistency_bonus + token_factor
        
        # Aplicar el impacto al score
        if tx_type == "BUY":
            new_score = min(10.0, current_score + final_impact)
        else:
            # Para transacciones que no son compras, el impacto es más neutro
            if final_impact > 0.02:  # Si el impacto es significativo
                new_score = min(10.0, current_score + (final_impact * 0.7))  # Impacto reducido
            else:
                new_score = current_score  # Sin cambio para impactos menores
        
        # Actualizar caché y base de datos
        self.local_cache[wallet] = new_score
        db.update_wallet_score(wallet, new_score)
        
        logger.debug(f"Score updated for {wallet}: {current_score:.2f} -> {new_score:.2f} (impact: {final_impact:.4f})")
        return new_score

    def _is_early_buyer(self, wallet, token, timestamp):
        """
        Verifica si un trader compra antes que otros (indicador de alfa)
        
        Args:
            wallet: Dirección del wallet
            token: Dirección del token
            timestamp: Timestamp de la transacción
            
        Returns:
            bool: True si es comprador temprano
        """
        try:
            # Consultar primera compra registrada para este token
            query = """
            SELECT MIN(created_at) as first_tx 
            FROM transactions 
            WHERE token = %s AND tx_type = 'BUY'
            """
            result = db.execute_cached_query(query, (token,), max_age=300)
            if result and result[0]['first_tx']:
                first_tx_time = result[0]['first_tx'].timestamp()
                # Considerar early buyer si está en el primer 10% del tiempo total
                token_age = time.time() - first_tx_time
                if token_age > 0 and (timestamp - first_tx_time) / token_age < 0.1:
                    return True
            return False
        except Exception as e:
            logger.warning(f"Error checking early buyer status: {e}")
            return False

    def _calculate_trade_profit(self, wallet, token, sell_tx):
        """
        Calcula la ganancia aproximada de una venta basándose en compras anteriores
        
        Args:
            wallet: Dirección del wallet
            token: Dirección del token
            sell_tx: Datos de la transacción de venta
            
        Returns:
            float: Porcentaje de ganancia estimado
        """
        if wallet not in self.wallet_token_buys or token not in self.wallet_token_buys[wallet]:
            return 0
        
        # Obtener el precio de venta
        sell_amount_usd = sell_tx.get("amount_usd", 0)
        if sell_amount_usd <= 0:
            return 0
            
        try:
            # Buscar la última compra registrada antes de esta venta
            buy_timestamps = [ts for ts in self.wallet_token_buys[wallet][token] if ts < sell_tx.get("timestamp", 0)]
            if not buy_timestamps:
                return 0
                
            latest_buy_time = max(buy_timestamps)
            
            # Obtener precio de compra
            query = """
            SELECT amount_usd 
            FROM transactions 
            WHERE wallet = %s AND token = %s AND tx_type = 'BUY' AND created_at <= to_timestamp(%s)
            ORDER BY created_at DESC
            LIMIT 1
            """
            result = db.execute_cached_query(query, (wallet, token, latest_buy_time), max_age=60)
            if not result:
                return 0
                
            buy_amount_usd = result[0]["amount_usd"]
            if buy_amount_usd <= 0:
                return 0
                
            # Calcular cambio porcentual
            percent_change = ((sell_amount_usd - buy_amount_usd) / buy_amount_usd) * 100
            
            # Registrar profit en DB para análisis
            hold_time_hours = (sell_tx.get("timestamp", 0) - latest_buy_time) / 3600
            db.save_wallet_profit(
                wallet=wallet,
                token=token,
                buy_price=buy_amount_usd,
                sell_price=sell_amount_usd,
                profit_percent=percent_change,
                hold_time_hours=hold_time_hours,
                buy_timestamp=datetime.fromtimestamp(latest_buy_time)
            )
            
            return percent_change
        except Exception as e:
            logger.warning(f"Error calculating trade profit: {e}")
            return 0

    def _get_token_quality(self, token):
        """
        Calcula un score de calidad para el token basado en historiales y métricas
        
        Args:
            token: Dirección del token
            
        Returns:
            float: Score de calidad entre 0 y 1
        """
        try:
            # 1. Verificar historial de rendimiento
            query = """
            SELECT AVG(percent_change) as avg_performance
            FROM signal_performance
            WHERE token = %s AND timeframe = '1h'
            """
            result = db.execute_cached_query(query, (token,), max_age=300)
            
            performance_score = 0.5  # Score neutral por defecto
            if result and result[0]["avg_performance"] is not None:
                avg_perf = result[0]["avg_performance"]
                if avg_perf > 20:
                    performance_score = 1.0
                elif avg_perf > 10:
                    performance_score = 0.8
                elif avg_perf > 5:
                    performance_score = 0.7
                elif avg_perf > 0:
                    performance_score = 0.6
                elif avg_perf < -10:
                    performance_score = 0.3
            
            # 2. Verificar liquidez
            liquidity_score = 0.5
            try:
                token_liquidity = db.get_tokens_with_high_liquidity(min_liquidity=0, limit=1)
                if token_liquidity and token_liquidity[0]["token"] == token:
                    liquidity = token_liquidity[0]["total_liquidity_usd"]
                    if liquidity > 100000:
                        liquidity_score = 1.0
                    elif liquidity > 50000:
                        liquidity_score = 0.8
                    elif liquidity > 20000:
                        liquidity_score = 0.7
                    elif liquidity > 5000:
                        liquidity_score = 0.6
            except Exception as e:
                logger.debug(f"Error checking liquidity for {token}: {e}")
            
            # Combinar factores con pesos
            final_quality = (performance_score * 0.6) + (liquidity_score * 0.4)
            return final_quality
            
        except Exception as e:
            logger.warning(f"Error getting token quality: {e}")
            return 0.5  # Score neutral en caso de error

    def _check_whale_activity(self, token):
        """
        Verifica si hay actividad reciente de ballenas en el token
        
        Args:
            token: Dirección del token
            
        Returns:
            bool: True si hay actividad de ballenas
        """
        try:
            query = """
            SELECT COUNT(*) as count
            FROM whale_activity
            WHERE token = %s AND created_at > NOW() - INTERVAL '1 HOUR'
            """
            result = db.execute_cached_query(query, (token,), max_age=60)
            return result and result[0]["count"] > 0
        except Exception as e:
            logger.debug(f"Error checking whale activity: {e}")
            return False

    def _check_token_trending(self, token):
        """
        Verifica si el token está en trending
        
        Args:
            token: Dirección del token
            
        Returns:
            bool: True si el token está en trending
        """
        try:
            query = """
            SELECT COUNT(*) as count
            FROM trending_tokens
            WHERE token = %s AND created_at > NOW() - INTERVAL '6 HOUR'
            """
            result = db.execute_cached_query(query, (token,), max_age=300)
            return result and result[0]["count"] > 0
        except Exception as e:
            logger.debug(f"Error checking trending status: {e}")
            return False

    def compute_confidence(self, wallet_scores, volume_1h, market_cap, recent_volume_growth=0, 
                          token_type=None, whale_activity=False, volume_acceleration=0, holder_growth=0):
        """
        Calcula la puntuación de confianza para una señal con múltiples factores
        
        Args:
            wallet_scores: Lista de scores de wallets involucrados
            volume_1h: Volumen en la última hora (USD)
            market_cap: Market Cap del token (USD)
            recent_volume_growth: Crecimiento de volumen reciente (fracción)
            token_type: Tipo de token (meme, defi, etc)
            whale_activity: Si hay actividad de ballenas
            volume_acceleration: Aceleración del volumen
            holder_growth: Crecimiento de holders (%)
            
        Returns:
            float: Score de confianza entre 0 y 1
        """
        if not wallet_scores:
            return 0.0
        
        # 1. Factor de calidad de traders (35%)
        # Normalizar score de wallets a 0-1
        normalized_scores = [min(score / 10, 1.0) for score in wallet_scores]
        # Aplicar mayor peso a scores altos usando exponente
        exp_scores = [score ** 1.5 for score in normalized_scores]
        # Calcular promedio ponderado
        trader_quality = sum(exp_scores) / len(exp_scores)
        
        # 2. Factor de actividad de ballenas (20%)
        whale_factor = 1.0 if whale_activity else 0.0
        
        # 3. Factor de crecimiento de holders (15%)
        # Normalizar crecimiento de holders (%)
        holder_factor = min(max(holder_growth / 100.0, 0), 1)
        
        # 4. Factor de liquidez y market cap (15%)
        if market_cap <= 0:
            liquidity_factor = 0.5  # Valor neutral si no hay datos
        else:
            # Relación inversa con market cap - favorece tokens más pequeños
            # pero con suficiente liquidez
            max_mcap = 100_000_000  # $100M como umbral
            liquidity_factor = max(0, min(1, 1 - (market_cap / max_mcap)))
            
            # Ajustar por slippage implícito (derivado del volumen)
            if volume_1h > 0:
                vol_mcap_ratio = volume_1h / max(market_cap, 1)
                if vol_mcap_ratio > 0.1:  # Volumen > 10% del market cap
                    liquidity_factor *= 0.8  # Penalizar por posible manipulación
        
        # 5. Factor técnico: volumen y patrones (15%)
        volume_growth_normalized = min(max(recent_volume_growth, 0), 3) / 3  # Normalizar a 0-1
        volume_accel_normalized = min(volume_acceleration / 10.0, 1)
        technical_factor = (volume_growth_normalized * 0.7) + (volume_accel_normalized * 0.3)
        
        # Obtener pesos desde configuración
        trader_weight = float(Config.get("TRADER_QUALITY_WEIGHT", 0.35))
        whale_weight = float(Config.get("WHALE_ACTIVITY_WEIGHT", 0.20))
        holder_weight = float(Config.get("HOLDER_GROWTH_WEIGHT", 0.15))
        liquidity_weight = float(Config.get("LIQUIDITY_HEALTH_WEIGHT", 0.15))
        technical_weight = float(Config.get("TECHNICAL_FACTORS_WEIGHT", 0.15))
        
        # Calcular score compuesto
        composite_score = (
            trader_quality * trader_weight +
            whale_factor * whale_weight +
            holder_factor * holder_weight +
            liquidity_factor * liquidity_weight +
            technical_factor * technical_weight
        )
        
        # Aplicar bono por tipo de token si corresponde
        token_multiplier = 1.0
        if token_type and token_type.lower() in self.token_type_scores:
            token_multiplier = self.token_type_scores[token_type.lower()]
        composite_score *= token_multiplier
        
        # Normalizar con función sigmoidea para suavizar los extremos
        def sigmoid_normalize(x, center=0.5, steepness=8):
            return 1 / (1 + math.exp(-steepness * (x - center)))
        
        normalized_score = sigmoid_normalize(composite_score, center=0.5, steepness=8)
        
        # Restringir al rango 0.1-0.95
        final_score = max(0.1, min(0.95, normalized_score))
        
        return round(final_score, 3)

    def add_score_booster(self, wallet, multiplier, duration_hours=24):
        """
        Añade un potenciador temporal al score de un wallet
        
        Args:
            wallet: Dirección del wallet
            multiplier: Multiplicador a aplicar (1.1 = +10%)
            duration_hours: Duración en horas
        """
        expiry = time.time() + (duration_hours * 3600)
        self.boosters[wallet] = {
            "multiplier": multiplier,
            "expires": expiry,
            "active": True
        }
        logger.info(f"Score booster added for {wallet}: x{multiplier} for {duration_hours}h")

    def get_all_scores(self):
        """
        Devuelve un diccionario con todos los scores conocidos
        """
        return self.local_cache

    def get_trader_name_from_wallet(self, wallet):
        """
        Retorna un nombre humano para la wallet, si se tiene
        """
        return db.get_trader_name_from_wallet(wallet)

    def get_top_traders(self, limit=10):
        """
        Retorna los mejores traders según su score
        
        Args:
            limit: Número máximo de traders a retornar
            
        Returns:
            list: Lista de diccionarios con wallet y score
        """
        scores = [(wallet, score) for wallet, score in self.local_cache.items()]
        scores.sort(key=lambda x: x[1], reverse=True)
        
        result = []
        for wallet, score in scores[:limit]:
            name = self.get_trader_name_from_wallet(wallet)
            result.append({
                "wallet": wallet,
                "name": name if name != wallet else None,
                "score": score
            })
        
        return result

    def analyze_trader_performance(self, wallet):
        """
        Analiza el rendimiento histórico de un trader
        
        Args:
            wallet: Dirección del wallet
            
        Returns:
            dict: Análisis de rendimiento
        """
        try:
            # Obtener estadísticas de profit desde DB
            profit_stats = db.get_wallet_profit_stats(wallet, days=30)
            if not profit_stats:
                return {
                    "wallet": wallet,
                    "score": self.get_score(wallet),
                    "name": self.get_trader_name_from_wallet(wallet),
                    "stats": {
                        "trade_count": 0,
                        "win_rate": 0,
                        "avg_profit": 0,
                        "max_profit": 0
                    }
                }
            
            # Obtener transacciones recientes
            recent_txs = db.get_wallet_recent_transactions(wallet, hours=24*7)  # 1 semana
            tokens_traded = set()
            for tx in recent_txs:
                tokens_traded.add(tx.get("token"))
            
            # Obtener perfil completo si está disponible
            trader_profile = None
            try:
                trader_profile = db.get_trader_profile_by_wallet(wallet)
            except:
                pass
            
            # Crear resultado
            result = {
                "wallet": wallet,
                "score": self.get_score(wallet),
                "name": self.get_trader_name_from_wallet(wallet),
                "stats": {
                    "trade_count": profit_stats.get("trade_count", 0),
                    "win_rate": profit_stats.get("win_rate", 0),
                    "avg_profit": profit_stats.get("avg_profit", 0),
                    "max_profit": profit_stats.get("max_profit", 0),
                    "avg_hold_time": profit_stats.get("avg_hold_time", 0),
                    "unique_tokens": len(tokens_traded)
                }
            }
            
            # Añadir datos de perfil si están disponibles
            if trader_profile and "profile_data" in trader_profile:
                profile_data = trader_profile["profile_data"]
                if isinstance(profile_data, str):
                    import json
                    try:
                        profile_data = json.loads(profile_data)
                    except:
                        profile_data = {}
                
                if "trading_style" in profile_data:
                    result["trading_style"] = profile_data["trading_style"]
                
                if "specialization" in profile_data:
                    result["specialization"] = profile_data["specialization"]
            
            return result
        except Exception as e:
            logger.error(f"Error analyzing trader performance: {e}")
            return {
                "wallet": wallet,
                "score": self.get_score(wallet),
                "name": self.get_trader_name_from_wallet(wallet),
                "error": str(e)
            }

    def cleanup_old_data(self):
        """Limpia datos antiguos para conservar memoria"""
        now = time.time()
        if now - self.last_cache_cleanup < 3600:  # Sólo limpiar una vez por hora
            return
            
        self.last_cache_cleanup = now
        
        # Limpiar boosters expirados
        boosters_to_remove = []
        for wallet, booster in self.boosters.items():
            if now > booster['expires']:
                boosters_to_remove.append(wallet)
                
        for wallet in boosters_to_remove:
            del self.boosters[wallet]
        
        # Limitar tamaño de historial de compras
        for wallet, tokens in self.wallet_token_buys.items():
            for token, timestamps in list(tokens.items()):
                # Mantener solo las compras de los últimos 30 días
                self.wallet_token_buys[wallet][token] = [
                    ts for ts in timestamps if now - ts < 30 * 24 * 3600
                ]
                # Eliminar tokens sin compras
                if not self.wallet_token_buys[wallet][token]:
                    del self.wallet_token_buys[wallet][token]
            
            # Eliminar wallets sin tokens
            if not self.wallet_token_buys[wallet]:
                del self.wallet_token_buys[wallet]
        
        logger.info(f"Cleaned {len(boosters_to_remove)} expired boosters")
