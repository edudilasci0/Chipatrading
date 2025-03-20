# trader_profiler.py
import logging
import time
import asyncio
from datetime import datetime, timedelta
import math
from collections import deque, defaultdict
import db
from config import Config

logger = logging.getLogger("trader_profiler")

class TraderProfiler:
    """
    Analizador de perfiles de traders para identificar patrones y calidad de operadores.
    
    Características:
    - Análisis del historial de transacciones de wallets
    - Detección de especialización y comportamiento de traders
    - Cálculo de compatibilidad trader-token
    - Identificación de patrones de actividad coordinada
    """
    
    def __init__(self, scoring_system=None):
        self.scoring_system = scoring_system
        self.trader_profiles = {}  # {wallet: profile_data}
        self.token_trader_compatibility = {}  # {token: {wallet: compatibility_score}}
        self.trader_groups = {}  # {group_id: {wallets: set, patterns: dict}}
        self.token_traders = defaultdict(set)  # {token: set(wallets)}
        self.trader_tokens = defaultdict(set)  # {wallet: set(tokens)}
        self.transaction_history = {}  # {wallet: {tokens: {token: [transactions]}}}
        
        # Configuración de caché
        self.cache_ttl = int(Config.get("TRADER_PROFILE_CACHE_TTL", 3600))  # 1 hora
        self.compatibility_ttl = int(Config.get("COMPATIBILITY_CACHE_TTL", 600))  # 10 minutos
        
        # Configuración de análisis
        self.min_transactions = int(Config.get("MIN_TRANSACTIONS_FOR_PROFILE", 3))
        self.min_profit_trades = int(Config.get("MIN_PROFIT_TRADES_FOR_QUALITY", 5))
        self.quality_threshold = float(Config.get("TRADER_QUALITY_THRESHOLD", 0.7))  # 0-1
        self.coordination_threshold = float(Config.get("COORDINATION_THRESHOLD", 0.8))  # 0-1 (similitud requerida)
        
        # Límites para perfiles
        self.max_traders_per_token = int(Config.get("MAX_TRADERS_PER_TOKEN", 1000))
        self.max_tokens_per_trader = int(Config.get("MAX_TOKENS_PER_TRADER", 100))
    
    def process_transaction(self, tx_data):
        """
        Procesa una transacción para actualizar perfiles de traders.
        
        Args:
            tx_data: Datos de la transacción
        """
        if not tx_data:
            return
        
        wallet = tx_data.get("wallet")
        token = tx_data.get("token")
        amount_usd = float(tx_data.get("amount_usd", 0))
        tx_type = tx_data.get("type", "").upper()
        timestamp = tx_data.get("timestamp", time.time())
        
        if not wallet or not token or amount_usd <= 0:
            return
        
        # Actualizar relaciones token-trader
        self.token_traders[token].add(wallet)
        if len(self.token_traders[token]) > self.max_traders_per_token:
            # Eliminar traders antiguos si se excede el límite
            excess = len(self.token_traders[token]) - self.max_traders_per_token
            traders_to_remove = list(self.token_traders[token])[:excess]
            for trader in traders_to_remove:
                self.token_traders[token].remove(trader)
        
        self.trader_tokens[wallet].add(token)
        if len(self.trader_tokens[wallet]) > self.max_tokens_per_trader:
            # Eliminar tokens antiguos si se excede el límite
            excess = len(self.trader_tokens[wallet]) - self.max_tokens_per_trader
            tokens_to_remove = list(self.trader_tokens[wallet])[:excess]
            for tkn in tokens_to_remove:
                self.trader_tokens[wallet].remove(tkn)
        
        # Inicializar estructuras de datos si es necesario
        if wallet not in self.transaction_history:
            self.transaction_history[wallet] = {"tokens": {}}
        
        if token not in self.transaction_history[wallet]["tokens"]:
            self.transaction_history[wallet]["tokens"][token] = []
        
        # Agregar transacción al historial
        self.transaction_history[wallet]["tokens"][token].append({
            "type": tx_type,
            "amount_usd": amount_usd,
            "timestamp": timestamp
        })
        
        # Limitar el número de transacciones almacenadas
        max_tx_history = int(Config.get("MAX_TX_HISTORY_PER_TOKEN", 10))
        if len(self.transaction_history[wallet]["tokens"][token]) > max_tx_history:
            self.transaction_history[wallet]["tokens"][token] = self.transaction_history[wallet]["tokens"][token][-max_tx_history:]
        
        # Invalidar caché de perfiles y compatibilidad
        if wallet in self.trader_profiles:
            self.trader_profiles[wallet]["last_update"] = 0  # Forzar actualización
        
        if token in self.token_trader_compatibility and wallet in self.token_trader_compatibility[token]:
            del self.token_trader_compatibility[token][wallet]
    
    async def get_trader_profile(self, wallet):
        """
        Analiza el perfil completo de un trader basado en su historial.
        
        Args:
            wallet: Dirección de la wallet del trader
            
        Returns:
            dict: Perfil completo del trader
        """
        # Verificar caché primero
        now = time.time()
        if wallet in self.trader_profiles:
            profile = self.trader_profiles[wallet]
            
            # Si los datos son recientes, usar directamente
            last_update = profile.get("last_update", 0)
            if now - last_update < self.cache_ttl:
                return profile
        
        # Obtener datos adicionales de la base de datos
        db_transactions = []
        try:
            db_transactions = db.get_wallet_recent_transactions(wallet, hours=24*7)  # 1 semana
        except Exception as e:
            logger.warning(f"Error obteniendo transacciones de BD para {wallet}: {e}")
        
        # Obtener estadísticas de profit
        profit_stats = None
        try:
            profit_stats = db.get_wallet_profit_stats(wallet, days=30)
        except Exception as e:
            logger.warning(f"Error obteniendo estadísticas de profit para {wallet}: {e}")
        
        # Inicializar el perfil
        profile = {
            "wallet": wallet,
            "score": 5.0,  # Score por defecto
            "transaction_count": 0,
            "tokens_traded": set(),
            "trade_frequency": 0,  # Transacciones por día
            "avg_position_size": 0,
            "specialization": {
                "token_types": {},  # {token_type: count}
                "preferred_dex": None,
                "specialty_score": 0  # 0-1, qué tan especializado está
            },
            "trading_style": {
                "avg_hold_time": 0,  # Horas
                "is_scalper": False,
                "is_swing_trader": False,
                "is_hodler": False,
                "is_flipper": False  # Compra/vende rápidamente
            },
            "performance": {
                "win_rate": 0,
                "avg_profit": 0,
                "max_profit": 0,
                "consistency": 0  # 0-1
            },
            "quality_score": 0.5,  # 0-1
            "activity_pattern": {
                "active_hours": [],  # Horas del día más activas
                "active_days": [],   # Días de la semana más activos
                "last_active": 0     # Timestamp
            },
            "last_update": now
        }
        
        # Usar score del sistema de scoring si está disponible
        if self.scoring_system:
            try:
                profile["score"] = self.scoring_system.get_score(wallet)
            except Exception as e:
                logger.warning(f"Error obteniendo score para {wallet}: {e}")
        
        # Combinar transacciones de memoria y BD
        all_transactions = []
        total_volume = 0
        tokens_traded = set()
        
        # Procesar transacciones almacenadas en memoria
        if wallet in self.transaction_history:
            for token, txs in self.transaction_history[wallet]["tokens"].items():
                for tx in txs:
                    all_transactions.append({
                        "token": token,
                        "type": tx["type"],
                        "amount_usd": tx["amount_usd"],
                        "timestamp": tx["timestamp"]
                    })
                    total_volume += tx["amount_usd"]
                    tokens_traded.add(token)
        
        # Procesar transacciones de BD
        for tx in db_transactions:
            all_transactions.append({
                "token": tx["token"],
                "type": tx["type"],
                "amount_usd": tx["amount_usd"],
                "timestamp": datetime.fromisoformat(tx["created_at"]).timestamp()
            })
            total_volume += tx["amount_usd"]
            tokens_traded.add(tx["token"])
        
        # Ordenar por timestamp
        all_transactions.sort(key=lambda x: x["timestamp"])
        
        # Actualizar datos básicos del perfil
        profile["transaction_count"] = len(all_transactions)
        profile["tokens_traded"] = tokens_traded
        
        if all_transactions:
            # Calcular frecuencia de trading
            if len(all_transactions) >= 2:
                first_tx = all_transactions[0]["timestamp"]
                last_tx = all_transactions[-1]["timestamp"]
                days_span = (last_tx - first_tx) / 86400
                
                if days_span > 0:
                    profile["trade_frequency"] = len(all_transactions) / days_span
                else:
                    profile["trade_frequency"] = len(all_transactions)  # Todas en el mismo día
            
            # Calcular tamaño promedio de posición
            profile["avg_position_size"] = total_volume / len(all_transactions)
            
            # Analizar patrón de actividad
            if len(all_transactions) >= 3:
                active_hours = defaultdict(int)
                active_days = defaultdict(int)
                
                for tx in all_transactions:
                    dt = datetime.fromtimestamp(tx["timestamp"])
                    active_hours[dt.hour] += 1
                    active_days[dt.weekday()] += 1
                
                # Determinar horas y días más activos
                profile["activity_pattern"]["active_hours"] = sorted(
                    active_hours.keys(), 
                    key=lambda h: active_hours[h], 
                    reverse=True
                )[:3]  # Top 3 horas
                
                profile["activity_pattern"]["active_days"] = sorted(
                    active_days.keys(), 
                    key=lambda d: active_days[d], 
                    reverse=True
                )[:3]  # Top 3 días
            
            profile["activity_pattern"]["last_active"] = all_transactions[-1]["timestamp"]
        
        # Analizar estilo de trading
        if profit_stats:
            profile["trading_style"]["avg_hold_time"] = profit_stats.get("avg_hold_time", 0)
            
            # Clasificar estilo según tiempo de retención
            avg_hold_hours = profit_stats.get("avg_hold_time", 0)
            if avg_hold_hours < 1:
                profile["trading_style"]["is_flipper"] = True
            elif avg_hold_hours < 24:
                profile["trading_style"]["is_scalper"] = True
            elif avg_hold_hours < 72:
                profile["trading_style"]["is_swing_trader"] = True
            else:
                profile["trading_style"]["is_hodler"] = True
        
        # Analizar rendimiento
        if profit_stats:
            profile["performance"]["win_rate"] = profit_stats.get("win_rate", 0)
            profile["performance"]["avg_profit"] = profit_stats.get("avg_profit", 0)
            profile["performance"]["max_profit"] = profit_stats.get("max_profit", 0)
            
            # Calcular consistencia basada en % de éxito
            win_rate = profit_stats.get("win_rate", 0)
            if win_rate > 0:
                # Consistencia: qué tan cerca está el win_rate de extremos
                # 0.5 es consistencia perfecta, 0 o 1 también es consistente
                if win_rate <= 0.5:
                    consistency = win_rate / 0.5  # 0-1 para win_rate 0-0.5
                else:
                    consistency = (1 - win_rate) / 0.5  # 0-1 para win_rate 0.5-1
                
                profile["performance"]["consistency"] = 1 - consistency  # Invertir para que 1 sea más consistente
        
        # Calcular quality score
        quality_factors = []
        
        # 1. Score base from scoring system (0-10 scaled to 0-1)
        base_score = min(profile["score"] / 10, 1.0)
        quality_factors.append(base_score)
        
        # 2. Performance factor
        if profit_stats and profit_stats.get("trade_count", 0) >= self.min_profit_trades:
            win_rate = profit_stats.get("win_rate", 0)
            avg_profit = profit_stats.get("avg_profit", 0)
            
            # Ponderar win_rate y avg_profit
            performance_factor = (win_rate * 0.6) + (min(avg_profit / 20, 1.0) * 0.4)
            quality_factors.append(performance_factor)
        
        # 3. Experience factor (basado en número de transacciones)
        if profile["transaction_count"] > 0:
            experience_factor = min(profile["transaction_count"] / 20, 1.0)
            quality_factors.append(experience_factor)
        
        # 4. Diversidad (cantidad de tokens operados)
        if profile["tokens_traded"]:
            diversity_factor = min(len(profile["tokens_traded"]) / 10, 1.0)
            quality_factors.append(diversity_factor)
        
        # Calcular score final (promedio de factores)
        if quality_factors:
            profile["quality_score"] = sum(quality_factors) / len(quality_factors)
        
        # Analizar especialización
        if profile["tokens_traded"]:
            # Clasificar tokens por tipo para detectar especialización
            token_types = defaultdict(int)
            for token in profile["tokens_traded"]:
                # Heurística simple para determinar tipo de token
                token_type = "unknown"
                if token.endswith("pump"):
                    token_type = "meme"
                elif token.endswith("ai"):
                    token_type = "ai"
                else:
                    token_type = "standard"
                
                token_types[token_type] += 1
            
            profile["specialization"]["token_types"] = dict(token_types)
            
            # Detectar tipo principal de token
            if token_types:
                main_type = max(token_types.items(), key=lambda x: x[1])[0]
                main_type_count = token_types[main_type]
                total_tokens = sum(token_types.values())
                
                # Calcular score de especialización
                if total_tokens > 0:
                    specialty_score = main_type_count / total_tokens
                    profile["specialization"]["specialty_score"] = specialty_score
        
        # Guardar perfil en caché
        self.trader_profiles[wallet] = profile
        
        return profile
    
    async def evaluate_trader_activity_pattern(self, token, traders):
        """
        Detecta patrones de actividad coordinada entre traders para un token.
        
        Args:
            token: Dirección del token
            traders: Lista de wallets activas en el token
            
        Returns:
            dict: Análisis de patrones de actividad
        """
        result = {
            "coordinated_activity": False,
            "coordination_score": 0,  # 0-1
            "similar_traders": [],
            "timing_pattern": None,  # "sequential", "simultaneous", None
            "confidence": 0  # 0-1
        }
        
        if not traders or len(traders) < 2:
            return result
        
        # Limitar número de traders para análisis (por rendimiento)
        max_traders = int(Config.get("MAX_TRADERS_FOR_PATTERN_ANALYSIS", 20))
        if len(traders) > max_traders:
            # Priorizar traders con mayor score
            traders_with_scores = []
            for wallet in traders:
                if wallet in self.trader_profiles:
                    score = self.trader_profiles[wallet].get("score", 0)
                else:
                    score = self.scoring_system.get_score(wallet) if self.scoring_system else 0
                
                traders_with_scores.append((wallet, score))
            
            # Ordenar por score y tomar los top traders
            traders_with_scores.sort(key=lambda x: x[1], reverse=True)
            traders = [t[0] for t in traders_with_scores[:max_traders]]
        
        # Obtener transacciones recientes del token para estos traders
        trader_txs = {}
        for wallet in traders:
            # Conseguir transacciones de memoria
            if (wallet in self.transaction_history and 
                "tokens" in self.transaction_history[wallet] and 
                token in self.transaction_history[wallet]["tokens"]):
                
                txs = self.transaction_history[wallet]["tokens"][token]
                trader_txs[wallet] = sorted(txs, key=lambda x: x["timestamp"])
        
        # Verificar si tenemos suficientes datos
        active_traders = list(trader_txs.keys())
        if len(active_traders) < 2:
            return result
        
        # Analizar similitud en patrones de actividad
        similar_trader_groups = []
        
        # 1. Detectar transacciones cercanas en el tiempo
        time_windows = {}  # {timestamp_window: [wallets]}
        window_size = int(Config.get("TIME_WINDOW_FOR_COORDINATION", 300))  # 5 minutos por defecto
        
        for wallet, txs in trader_txs.items():
            for tx in txs:
                window_start = tx["timestamp"] // window_size * window_size
                if window_start not in time_windows:
                    time_windows[window_start] = []
                time_windows[window_start].append(wallet)
        
        # Contar coincidencias de traders en ventanas de tiempo
        trader_coincidences = defaultdict(int)
        for window, wallets in time_windows.items():
            if len(wallets) >= 2:
                # Incrementar contador para cada par de traders
                wallets_set = set(wallets)
                for wallet1 in wallets_set:
                    for wallet2 in wallets_set:
                        if wallet1 != wallet2:
                            pair = tuple(sorted([wallet1, wallet2]))
                            trader_coincidences[pair] += 1
        
        # Crear grupos de traders con actividad similar
        if trader_coincidences:
            # Normalizar coincidencias
            max_coincidences = max(trader_coincidences.values())
            normalized_coincidences = {
                pair: count / max_coincidences 
                for pair, count in trader_coincidences.items()
            }
            
            # Encontrar pares con alta similitud
            high_similarity_pairs = [
                pair for pair, similarity in normalized_coincidences.items()
                if similarity >= self.coordination_threshold
            ]
            
            if high_similarity_pairs:
                # Construir grupos de traders similares
                # Algoritmo simple: si A es similar a B y B es similar a C, A, B y C forman un grupo
                trader_groups = []
                
                for pair in high_similarity_pairs:
                    wallet1, wallet2 = pair
                    
                    # Verificar si alguno ya está en un grupo
                    found_group = False
                    for group in trader_groups:
                        if wallet1 in group or wallet2 in group:
                            group.add(wallet1)
                            group.add(wallet2)
                            found_group = True
                            break
                    
                    # Si no está en ningún grupo, crear uno nuevo
                    if not found_group:
                        trader_groups.append({wallet1, wallet2})
                
                # Filtrar grupos con al menos 2 traders
                trader_groups = [g for g in trader_groups if len(g) >= 2]
                
                if trader_groups:
                    similar_trader_groups = trader_groups
        
        # Determinar patrón de timing
        timing_pattern = None
        if similar_trader_groups:
            # Analizar secuencia temporal de transacciones
            sequential_count = 0
            simultaneous_count = 0
            
            for group in similar_trader_groups:
                group_wallets = list(group)
                if len(group_wallets) < 2:
                    continue
                
                # Recopilar todas las transacciones del grupo
                group_txs = []
                for wallet in group_wallets:
                    if wallet in trader_txs:
                        for tx in trader_txs[wallet]:
                            group_txs.append((wallet, tx["timestamp"], tx["type"]))
                
                # Ordenar por timestamp
                group_txs.sort(key=lambda x: x[1])
                
                # Analizar secuencialidad vs simultaneidad
                if len(group_txs) >= 3:
                    time_diffs = []
                    for i in range(1, len(group_txs)):
                        time_diffs.append(group_txs[i][1] - group_txs[i-1][1])
                    
                    avg_diff = sum(time_diffs) / len(time_diffs)
                    
                    if avg_diff < 60:  # Menos de 1 minuto entre transacciones
                        simultaneous_count += 1
                    elif 60 <= avg_diff <= 300:  # 1-5 minutos entre transacciones
                        sequential_count += 1
            
            if sequential_count > simultaneous_count:
                timing_pattern = "sequential"
            elif simultaneous_count > 0:
                timing_pattern = "simultaneous"
        
        # Calcular score de coordinación
        coordination_score = 0
        
        if similar_trader_groups:
            # Contar traders únicos en todos los grupos
            all_similar_traders = set()
            for group in similar_trader_groups:
                all_similar_traders.update(group)
            
            # Proporción de traders coordinados respecto al total
            if traders:
                coordination_ratio = len(all_similar_traders) / len(traders)
                
                # Ajustar por tamaño de grupo más grande
                max_group_size = max(len(group) for group in similar_trader_groups)
                group_size_factor = min(max_group_size / 3, 1.0)  # Normalizar a max 1.0
                
                # Ponderar factores
                coordination_score = (coordination_ratio * 0.6) + (group_size_factor * 0.4)
        
        # Determinar si hay actividad coordinada
        coordinated_activity = coordination_score >= 0.5
        
        # Actualizar resultado
        result["coordinated_activity"] = coordinated_activity
        result["coordination_score"] = coordination_score
        result["similar_traders"] = list(all_similar_traders) if 'all_similar_traders' in locals() else []
        result["timing_pattern"] = timing_pattern
        result["confidence"] = min(coordination_score * 1.5, 1.0)  # Ajustar confianza basada en score
        
        return result
    
    async def trader_token_compatibility(self, token, trader_data):
        """
        Calcula la compatibilidad entre un trader y un token específico.
        
        Args:
            token: Dirección del token
            trader_data: Datos adicionales del trader (opcional)
            
        Returns:
            dict: Score de compatibilidad por trader
        """
        # Estructura para resultados
        result = {}
        
        # Obtener lista de traders activos para este token
        active_traders = list(self.token_traders.get(token, set()))
        
        # Si hay trader_data, añadir esos traders también
        if trader_data:
            for wallet in trader_data:
                if wallet not in active_traders:
                    active_traders.append(wallet)
        
        # Si no hay traders, retornar vacío
        if not active_traders:
            return result
        
        # Verificar caché
        now = time.time()
        if token in self.token_trader_compatibility:
            token_compat = self.token_trader_compatibility[token]
            
            # Filtrar traders cuya compatibilidad está en caché y es reciente
            cached_traders = []
            for wallet in active_traders:
                if wallet in token_compat and now - token_compat[wallet]["timestamp"] < self.compatibility_ttl:
                    result[wallet] = token_compat[wallet]["score"]
                    cached_traders.append(wallet)
            
            # Remover traders ya procesados
            for wallet in cached_traders:
                active_traders.remove(wallet)
            
            # Si todos están en caché, retornar resultado
            if not active_traders:
                return result
        else:
            self.token_trader_compatibility[token] = {}
        
        # Para cada trader, calcular compatibilidad
        for wallet in active_traders:
            compatibility_score = 0.5  # Valor neutral por defecto
            
            # Obtener perfil del trader
            profile = await self.get_trader_profile(wallet)
            
            if profile:
                # Factores para compatibilidad
                
                # 1. Especialización en tipo de token
                token_type = "standard"  # Por defecto
                if token.endswith("pump"):
                    token_type = "meme"
                elif token.endswith("ai"):
                    token_type = "ai"
                
                specialization_factor = 0.5  # Neutral por defecto
                
                if "specialization" in profile and "token_types" in profile["specialization"]:
                    token_types = profile["specialization"]["token_types"]
                    
                    if token_type in token_types:
                        # Mayor peso si hay especialización
                        specialization_factor = 0.7 + (0.3 * profile["specialization"].get("specialty_score", 0))
                    else:
                        # Menor peso si no opera este tipo
                        specialization_factor = 0.3
                
                # 2. Estilo de trading
                style_factor = 0.5  # Neutral por defecto
                
                if "trading_style" in profile:
                    trading_style = profile["trading_style"]
                    
                    # Diferentes tipos de tokens se benefician de diferentes estilos
                    if token_type == "meme":
                        # Memecoins: mejor para scalpers y flippers (movimientos rápidos)
                        if trading_style.get("is_scalper") or trading_style.get("is_flipper"):
                            style_factor = 0.8
                        elif trading_style.get("is_hodler"):
                            style_factor = 0.3  # No ideal para hodlers
                    elif token_type == "ai":
                        # AI tokens: mejor para swing traders (median plazo)
                        if trading_style.get("is_swing_trader"):
                            style_factor = 0.8
                        elif trading_style.get("is_flipper"):
                            style_factor = 0.4  # No ideal para flippers
                    else:
                        # Standard tokens: versatilidad
                        style_factor = 0.6  # Ligeramente favorable
                
                # 3. Calidad del trader
                quality_factor = profile.get("quality_score", 0.5)
                
                # Combinar factores con pesos
                compatibility_score = (
                    specialization_factor * 0.4 +
                    style_factor * 0.3 +
                    quality_factor * 0.3
                )
                
                # Bonus si ya ha operado este token antes
                if token in profile.get("tokens_traded", set()):
                    compatibility_score = min(compatibility_score * 1.2, 1.0)
            
            # Guardar en caché
            self.token_trader_compatibility[token][wallet] = {
                "score": compatibility_score,
                "timestamp": now
            }
            
            # Añadir al resultado
            result[wallet] = compatibility_score
        
        return result
    
    def cleanup_old_data(self):
        """Limpia datos antiguos para liberar memoria"""
        now = time.time()
        
        # Limpiar perfiles antiguos
        wallets_to_remove = []
        for wallet, profile in self.trader_profiles.items():
            if now - profile.get("last_update", 0) > 86400 * 7:  # >7 días
                wallets_to_remove.append(wallet)
        
        for wallet in wallets_to_remove:
            del self.trader_profiles[wallet]
        
        # Limpiar compatibilidad antigua
        tokens_to_remove = []
        for token, compatibility in self.token_trader_compatibility.items():
            if not compatibility or all(now - data.get("timestamp", 0) > 86400 for data in compatibility.values()):
                tokens_to_remove.append(token)
        
        for token in tokens_to_remove:
            del self.token_trader_compatibility[token]
        
        # Limpiar historiales antiguos (solo mantener activos recientes)
        wallets_to_remove = []
        for wallet, history in self.transaction_history.items():
            if "tokens" not in history:
                wallets_to_remove.append(wallet)
                continue
            
            has_recent = False
            for token, txs in history["tokens"].items():
                if txs and now - txs[-1].get("timestamp", 0) < 86400 * 3:  # <3 días
                    has_recent = True
                    break
            
            if not has_recent:
                wallets_to_remove.append(wallet)
        
        for wallet in wallets_to_remove:
            del self.transaction_history[wallet]
