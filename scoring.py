import time
import math
import db
from config import Config

class ScoringSystem:
    """
    Sistema de scoring para evaluar la calidad de los traders
    y calcular la confianza de las señales.
    """

    def __init__(self):
        """
        Inicializa el sistema de scoring.
        """
        # Cache local para evitar consultas repetidas a la BD
        self.local_cache = {}
        # Tiempo de la última limpieza de cache
        self.last_cache_cleanup = time.time()
        # Contador de transacciones por wallet
        self.wallet_tx_count = {}
        # Cache de boosters
        self.boosters = {}  # {wallet: {'active': bool, 'multiplier': float, 'expires': timestamp, 'reason': str}}
        # Score de boosters por tipo de token
        self.token_type_scores = {}  # {token_type: multiplier}
        
        # NUEVO: Tracking para análisis de profit/loss
        self.wallet_token_buys = {}  # {wallet:token: {'timestamp': time, 'amount_usd': amount}}
        self.wallet_profits = {}     # {wallet: [{'token': token, 'profit_percent': percent, 'timestamp': time}]}
        
        # Inicializar boosters de tipos de token
        self._init_token_type_boosters()
        
    def _init_token_type_boosters(self):
        """
        Inicializa boosters por tipo de token.
        """
        # Tokens conocidos con historiales de éxito tienen mayor boost
        self.token_type_scores = {
            "meme": 1.2,  # Tokens meme suelen tener más pump inicial
            "defi": 1.1,  # DeFi tokens
            "nft": 1.15,  # NFT relacionados
            "gaming": 1.1,  # Gaming tokens
            "ai": 1.2,     # AI tokens (tendencia actual)
            "new": 1.25    # Tokens nuevos (<24h)
        }

    def get_score(self, wallet):
        """
        Obtiene el score de un wallet, usando cache si está disponible.
        
        Args:
            wallet: Dirección de la wallet
            
        Returns:
            float: Score actual de la wallet
        """
        if wallet not in self.local_cache:
            self.local_cache[wallet] = db.get_wallet_score(wallet)
        
        # Aplicar booster si existe y está activo
        base_score = self.local_cache[wallet]
        if wallet in self.boosters and self.boosters[wallet]['active']:
            if time.time() < self.boosters[wallet]['expires']:
                return base_score * self.boosters[wallet]['multiplier']
            else:
                # Booster expirado
                self.boosters[wallet]['active'] = False
        
        return base_score

    def update_score_on_trade(self, wallet, tx_data):
        """
        Actualiza el score de un wallet basado en una transacción.
        Versión optimizada con análisis de profit y factores adicionales.
        
        Args:
            wallet: Dirección de la wallet
            tx_data: Diccionario con datos de la transacción
        """
        tx_type = tx_data.get("type", "")
        token = tx_data.get("token", "")
        amount_usd = tx_data.get("amount_usd", 0)
        current_score = self.get_score(wallet)
        
        # Incrementar contador de transacciones
        if wallet not in self.wallet_tx_count:
            self.wallet_tx_count[wallet] = 0
        self.wallet_tx_count[wallet] += 1
        
        # NUEVO: Tracking de pares wallet-token para análisis de profit/loss
        wallet_token_key = f"{wallet}:{token}"
        
        # Ajustar score según el tipo de transacción
        if tx_type == "BUY":
            # NUEVO: Registrar compra para futuro cálculo de profit
            self.wallet_token_buys[wallet_token_key] = {
                'timestamp': time.time(),
                'amount_usd': amount_usd
            }
            
            # OPTIMIZACIÓN: Incrementos diferenciados por tamaño de transacción
            if amount_usd > 10000:  # Whale
                score_increment = Config.BUY_SCORE_INCREASE * 3
            elif amount_usd > 5000:  # Grande
                score_increment = Config.BUY_SCORE_INCREASE * 2
            elif amount_usd > 1000:  # Medio
                score_increment = Config.BUY_SCORE_INCREASE * 1.5
            else:
                score_increment = Config.BUY_SCORE_INCREASE
                
            new_score = current_score + score_increment
            
        elif tx_type == "SELL":
            # NUEVO: Verificar si es profit o loss comparando con compra anterior
            profit_factor = 1.0
            
            if wallet_token_key in self.wallet_token_buys:
                buy_data = self.wallet_token_buys[wallet_token_key]
                buy_amount = buy_data['amount_usd']
                buy_time = buy_data['timestamp']
                hold_time_hours = (time.time() - buy_time) / 3600
                
                # Si vendemos por más de lo que compramos, es profit
                if amount_usd > buy_amount:
                    profit_percent = (amount_usd - buy_amount) / buy_amount
                    
                    # NUEVO: Valorar más las ganancias rápidas (señal de buen trader)
                    if hold_time_hours < 1:  # Menos de 1 hora
                        profit_factor = 1.5 + (profit_percent * 2)  # Boost grande por profit rápido
                    elif hold_time_hours < 24:  # Menos de 1 día
                        profit_factor = 1.2 + profit_percent
                    else:
                        profit_factor = 1.1 + (profit_percent * 0.5)
                        
                    # NUEVO: Registrar profit para estadísticas y boosters
                    if wallet not in self.wallet_profits:
                        self.wallet_profits[wallet] = []
                    
                    self.wallet_profits[wallet].append({
                        'token': token,
                        'profit_percent': profit_percent,
                        'hold_time_hours': hold_time_hours,
                        'timestamp': time.time()
                    })
                    
                    print(f"📈 Trader {wallet} realizó profit de {profit_percent:.2%} en {hold_time_hours:.1f}h")
                else:
                    # Es una venta con pérdida
                    loss_percent = (buy_amount - amount_usd) / buy_amount
                    
                    # NUEVO: Penalización moderada por pérdida
                    profit_factor = 1.0 - (loss_percent * 0.5)
                    
                    # Pero no penalizar mucho si el hold fue largo (puede ser DCA)
                    if hold_time_hours > 48:  # Más de 2 días
                        profit_factor = max(profit_factor, 0.9)  # No penalizar tanto
                    
                    print(f"📉 Trader {wallet} vendió con pérdida de {loss_percent:.2%}")
            
            # Incrementos por tamaño y ajustados por profit
            if amount_usd > 5000:
                score_increment = Config.SELL_SCORE_INCREASE * 2 * profit_factor
            else:
                score_increment = Config.SELL_SCORE_INCREASE * profit_factor
                
            new_score = current_score + score_increment
        else:
            return  # No cambiar score para otros tipos

        # Limitar entre MIN_SCORE y MAX_SCORE
        new_score = max(Config.MIN_SCORE, min(Config.MAX_SCORE, new_score))
        
        # NUEVO: Deterioro de score con el tiempo (evitar scores inflados)
        # Si el score está por encima de la media, aplicar pequeña reducción
        mid_score = (Config.MAX_SCORE + Config.MIN_SCORE) / 2
        if new_score > mid_score:
            decay_factor = 0.995  # 0.5% de decay por transacción
            new_score = mid_score + (new_score - mid_score) * decay_factor
        
        # Actualizar cache y BD
        self.local_cache[wallet] = new_score
        db.update_wallet_score(wallet, new_score)
        
        # NUEVO: Agregar boosters basados en rendimiento
        self._apply_performance_boosters(wallet)
        
        # Cada 50 transacciones por wallet, añadir un pequeño booster
        if self.wallet_tx_count[wallet] % 50 == 0:
            self.add_score_booster(wallet, 1.2, 86400, "Actividad constante")  # 20% boost por 24 horas
            print(f"🔥 ¡Booster de actividad para {wallet}! +20% por 24h")
        
        print(f"📊 Score de {wallet} actualizado: {current_score:.1f} → {new_score:.1f}")
        
        # Limpiar cache periódicamente
        if time.time() - self.last_cache_cleanup > 3600:  # Cada hora
            self.cleanup_cache()

    def _apply_performance_boosters(self, wallet):
        """
        Aplica boosters basados en el rendimiento histórico del trader.
        
        Args:
            wallet: Dirección de la wallet
        """
        if wallet not in self.wallet_profits:
            return
            
        # Calcular tasa de éxito reciente (últimos 30 días)
        recent_profits = [p for p in self.wallet_profits[wallet] 
                         if time.time() - p['timestamp'] < 2592000]  # 30 días
        
        if len(recent_profits) >= 3:  # Al menos 3 operaciones recientes
            # Calcular profit promedio
            avg_profit = sum(p['profit_percent'] for p in recent_profits) / len(recent_profits)
            
            # Si el trader ha sido consistentemente rentable, darle un boost
            if avg_profit > 0.2:  # Más de 20% promedio
                boost_multiplier = 1.3
                boost_duration = 172800  # 48 horas
                boost_reason = f"Historial de profit consistente ({avg_profit:.1%})"
                self.add_score_booster(wallet, boost_multiplier, boost_duration, boost_reason)
            
            # Bonus especial para traders con ganancias rápidas
            quick_profits = [p for p in recent_profits if p['hold_time_hours'] < 2 and p['profit_percent'] > 0.3]
            if len(quick_profits) >= 2:
                boost_multiplier = 1.5
                boost_duration = 259200  # 72 horas
                boost_reason = "Trader de momentum (ganancias rápidas)"
                self.add_score_booster(wallet, boost_multiplier, boost_duration, boost_reason)

    def compute_confidence(self, wallet_scores, volume_1h, market_cap, recent_volume_growth=0, token_type=None):
        """
        Calcula un nivel de confianza mejorado basado en múltiples factores.
        
        Args:
            wallet_scores: Lista de scores de wallets
            volume_1h: Volumen en la última hora en USD
            market_cap: Market cap en USD
            recent_volume_growth: Crecimiento de volumen en porcentaje (ej: 0.05 = 5%)
            token_type: Tipo de token para aplicar boosters
            
        Returns:
            float: Nivel de confianza entre 0.0 y 1.0
        """
        if not wallet_scores:
            return 0.0

        # OPTIMIZACIÓN: Evaluar distribución de scores, no solo promedio
        # Dar más peso a altos scores usando una curva exponencial
        exp_scores = [score ** 1.5 for score in wallet_scores]
        weighted_avg = sum(exp_scores) / (len(exp_scores) * (Config.MAX_SCORE ** 1.5)) * Config.MAX_SCORE
        
        # Usar el promedio ponderado para el factor de score
        score_factor = weighted_avg / Config.MAX_SCORE
        
        # Factor de diversidad - valorar más carteras diversas
        unique_wallets = len(wallet_scores)
        wallet_diversity = min(unique_wallets / 10, 1.0)  # Máximo con 10 wallets
        
        # OPTIMIZACIÓN: Valorar distribución de scores
        high_quality_traders = sum(1 for score in wallet_scores if score > 7.0)
        elite_traders = sum(1 for score in wallet_scores if score > 9.0)
        
        # Calcular ratio de traders de calidad (al menos 30% deben ser buenos)
        quality_ratio = (high_quality_traders + (elite_traders * 2)) / max(1, len(wallet_scores))
        quality_factor = min(quality_ratio * 1.5, 1.0)
        
        # Bonificación por elite traders (máximo 30%)
        elite_bonus = min(elite_traders * 0.1, 0.3)
        
        # OPTIMIZACIÓN: Balance entre cantidad y calidad
        # Combinar factores de wallet con pesos optimizados
        wallet_factor = (score_factor * 0.4) + (wallet_diversity * 0.3) + (quality_factor * 0.2) + elite_bonus
        
        # OPTIMIZACIÓN: Función sigmoide para market cap óptimo
        # Preferimos market caps medianos (entre 500K y 10M)
        if market_cap <= 0:
            mc_factor = 0.3  # Token sin market cap conocido
        else:
            # Rangos de market cap
            low_optimal = 500000    # 500K USD
            high_optimal = 10000000  # 10M USD
            
            if market_cap < Config.MIN_MARKETCAP:
                mc_factor = 0.3  # Muy bajo = riesgoso
            elif market_cap < low_optimal:
                # Entre mínimo y óptimo bajo
                normalized = (market_cap - Config.MIN_MARKETCAP) / (low_optimal - Config.MIN_MARKETCAP)
                mc_factor = 0.3 + (normalized * 0.5)  # Escalar de 0.3 a 0.8
            elif market_cap <= high_optimal:
                # En el rango óptimo
                mc_factor = 0.8
            elif market_cap <= Config.MAX_MARKETCAP:
                # Entre óptimo alto y máximo
                normalized = (Config.MAX_MARKETCAP - market_cap) / (Config.MAX_MARKETCAP - high_optimal)
                mc_factor = 0.5 + (normalized * 0.3)  # Escalar de 0.5 a 0.8
            else:
                # Por encima del máximo
                mc_factor = 0.5  # Penalización moderada
        
        # Normalizar volumen, con máximo en Config.VOL_NORMALIZATION_FACTOR USD
        vol_factor = min(volume_1h / Config.VOL_NORMALIZATION_FACTOR, 1.0)
        
        # OPTIMIZACIÓN: Factor de crecimiento no lineal
        # Valorar crecimiento rápido exponencialmente hasta cierto punto
        if recent_volume_growth <= 0:
            growth_factor = 0.2  # Valor base
        elif recent_volume_growth < 0.05:  # Menos de 5%
            growth_factor = 0.3 + (recent_volume_growth * 4)  # Escala lineal baja
        elif recent_volume_growth < 0.2:  # Entre 5% y 20%
            growth_factor = 0.5 + (recent_volume_growth * 2)  # Escala lineal media
        else:  # Más de 20%
            # Limitar el factor para evitar sobrevalorar crecimientos extremos
            growth_factor = 0.9 + min((recent_volume_growth - 0.2) * 0.5, 0.1)
        
        # Combinar factores de mercado con más peso al volumen y crecimiento
        market_factor = (vol_factor * 0.4) + (mc_factor * 0.3) + (growth_factor * 0.3)
        
        # OPTIMIZACIÓN: Balancear factores con más peso a wallets de calidad
        weighted_score = (wallet_factor * 0.65) + (market_factor * 0.35)
        
        # Aplicar booster de tipo de token si existe
        if token_type and token_type.lower() in self.token_type_scores:
            multiplier = self.token_type_scores[token_type.lower()]
            weighted_score *= multiplier
            print(f"🏷️ Booster aplicado para tipo {token_type}: x{multiplier}")
        
        # OPTIMIZACIÓN: Aplicar curva sigmoidea para concentrar valores
        # Evita valores muy bajos que nunca serán usados y muy altos poco diferenciados
        def sigmoid_normalize(x, center=0.5, steepness=10):
            """Ajusta la distribución de valores usando función sigmoidea"""
            return 1 / (1 + math.exp(-steepness * (x - center)))
        
        # Normalizar entre 0 y 1 usando sigmoid
        normalized = max(0.1, min(1.0, sigmoid_normalize(weighted_score, 0.5, 8)))
        
        # Redondear a 3 decimales
        return round(normalized, 3)

    def cleanup_cache(self, max_size=1000):
        """
        Limpia la cache si crece demasiado.
        
        Args:
            max_size: Tamaño máximo permitido de la caché
        """
        if len(self.local_cache) > max_size:
            # Conservar solo las 500 wallets más recientes
            self.local_cache = {}
            print(f"🧹 Cache de scores limpiada (superó {max_size} entradas)")
        
        # Limpiar boosters expirados
        now = time.time()
        expired_wallets = [wallet for wallet, data in self.boosters.items() 
                          if now > data['expires']]
        
        for wallet in expired_wallets:
            del self.boosters[wallet]
        
        # OPTIMIZACIÓN: Limpiar datos de profit antiguos (más de 90 días)
        old_data_cutoff = now - 7776000  # 90 días
        
        for wallet in self.wallet_profits:
            if wallet in self.wallet_profits:
                self.wallet_profits[wallet] = [
                    p for p in self.wallet_profits[wallet] 
                    if p['timestamp'] > old_data_cutoff
                ]
        
        # Limpiar wallet_token_buys antiguos (más de 7 días)
        buy_cutoff = now - 604800  # 7 días
        keys_to_remove = [k for k, v in self.wallet_token_buys.items() 
                         if v['timestamp'] < buy_cutoff]
        
        for key in keys_to_remove:
            del self.wallet_token_buys[key]
        
        if expired_wallets or keys_to_remove:
            print(f"🧹 Limpiados {len(expired_wallets)} boosters expirados y {len(keys_to_remove)} registros de compras antiguos")
        
        self.last_cache_cleanup = time.time()

    def add_score_booster(self, wallet, multiplier, duration_seconds, reason=None):
        """
        Añade un multiplicador temporal al score de una wallet.
        
        Args:
            wallet: Dirección de la wallet
            multiplier: Multiplicador a aplicar (ej: 1.5 para +50%)
            duration_seconds: Duración del booster en segundos
            reason: Razón para el booster (para logging)
        """
        if wallet not in self.boosters:
            self.boosters[wallet] = {
                'active': True,
                'multiplier': multiplier,
                'expires': time.time() + duration_seconds,
                'reason': reason
            }
        else:
            # Si ya existe, extender duración y mantener el mayor multiplicador
            self.boosters[wallet]['active'] = True
            self.boosters[wallet]['multiplier'] = max(self.boosters[wallet]['multiplier'], multiplier)
            self.boosters[wallet]['expires'] = time.time() + duration_seconds
            
            # Actualizar razón solo si se proporciona
            if reason:
                self.boosters[wallet]['reason'] = reason
        
        # Loguear el booster
        reason_str = f" - {reason}" if reason else ""
        print(f"🔥 Booster para wallet {wallet}: x{multiplier} por {duration_seconds/3600:.1f}h{reason_str}")
    
    def get_wallet_profit_stats(self, wallet):
        """
        Obtiene estadísticas de profit para una wallet.
        
        Args:
            wallet: Dirección de la wallet
            
        Returns:
            dict: Estadísticas de profit o None si no hay datos
        """
        if wallet not in self.wallet_profits or not self.wallet_profits[wallet]:
            return None
            
        profits = self.wallet_profits[wallet]
        
        # Filtrar solo últimos 30 días
        recent_cutoff = time.time() - 2592000  # 30 días
        recent_profits = [p for p in profits if p['timestamp'] > recent_cutoff]
        
        if not recent_profits:
            return None
            
        # Calcular estadísticas
        avg_profit = sum(p['profit_percent'] for p in recent_profits) / len(recent_profits)
        max_profit = max(p['profit_percent'] for p in recent_profits)
        success_rate = len([p for p in recent_profits if p['profit_percent'] > 0]) / len(recent_profits)
        
        avg_hold_time = sum(p['hold_time_hours'] for p in recent_profits) / len(recent_profits)
        
        return {
            'avg_profit': avg_profit,
            'max_profit': max_profit,
            'success_rate': success_rate,
            'trade_count': len(recent_profits),
            'avg_hold_time': avg_hold_time
        }
    
    def get_top_traders(self, limit=10, min_trades=3):
        """
        Obtiene los mejores traders basados en profit promedio.
        
        Args:
            limit: Número máximo de traders a retornar
            min_trades: Número mínimo de operaciones recientes para calificar
            
        Returns:
            list: Lista de diccionarios con datos de los mejores traders
        """
        trader_stats = []
        
        for wallet, profits in self.wallet_profits.items():
            # Filtrar solo últimos 30 días
            recent_cutoff = time.time() - 2592000  # 30 días
            recent_profits = [p for p in profits if p['timestamp'] > recent_cutoff]
            
            if len(recent_profits) < min_trades:
                continue
                
            avg_profit = sum(p['profit_percent'] for p in recent_profits) / len(recent_profits)
            success_rate = len([p for p in recent_profits if p['profit_percent'] > 0]) / len(recent_profits)
            
            trader_stats.append({
                'wallet': wallet,
                'avg_profit': avg_profit,
                'success_rate': success_rate,
                'trade_count': len(recent_profits),
                'score': self.get_score(wallet)
            })
        
        # Ordenar por profit promedio
        sorted_traders = sorted(trader_stats, key=lambda x: x['avg_profit'], reverse=True)
        
        return sorted_traders[:limit]
