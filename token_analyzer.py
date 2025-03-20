# token_analyzer.py
import logging
import time
import asyncio
from datetime import datetime, timedelta
import math
from collections import deque
import numpy as np
import db
from config import Config

logger = logging.getLogger("token_analyzer")

class TokenAnalyzer:
    """
    Analizador de tokens para detectar patrones técnicos y oportunidades.
    
    Características:
    - Análisis de patrones de volumen y crecimiento
    - Detección de rupturas de resistencia y soporte
    - Cálculo de momentum y volatilidad
    - Detección de patrones de acumulación y distribución
    """
    
    def __init__(self, token_data_service=None):
        self.token_data_service = token_data_service
        self.price_history = {}  # {token: deque de precios históricos}
        self.volume_history = {}  # {token: deque de volúmenes históricos}
        self.price_points = {}  # {token: {"timestamp": [], "price": []}}
        self.token_metrics = {}  # {token: {"volatility": float, "momentum": float, ...}}
        self.support_resistance = {}  # {token: {"supports": [], "resistances": []}}
        self.max_history_points = int(Config.get("MAX_PRICE_HISTORY_POINTS", 48))  # 48 puntos por defecto
        self.min_points_for_analysis = int(Config.get("MIN_POINTS_FOR_ANALYSIS", 5))
        self.analysis_cache = {}  # {token: {"analysis": result, "timestamp": time}}
        self.cache_ttl = int(Config.get("TOKEN_ANALYSIS_CACHE_TTL", 300))  # 5 minutos
    
    async def update_price_data(self, token, current_price=None, current_volume=None, market_data=None):
        """
        Actualiza los datos históricos de precio y volumen para un token.
        
        Args:
            token: Dirección del token
            current_price: Precio actual (opcional)
            current_volume: Volumen actual (opcional)
            market_data: Datos completos de mercado (opcional)
        """
        timestamp = time.time()
        
        # Obtener datos de mercado si no se proporcionaron
        if not market_data and not (current_price and current_volume) and self.token_data_service:
            try:
                if hasattr(self.token_data_service, 'get_token_data_async'):
                    market_data = await self.token_data_service.get_token_data_async(token)
                elif hasattr(self.token_data_service, 'get_token_data'):
                    market_data = self.token_data_service.get_token_data(token)
            except Exception as e:
                logger.error(f"Error obteniendo datos de mercado para {token}: {e}")
        
        # Extraer precio y volumen
        price = current_price
        volume = current_volume
        
        if market_data:
            if not price and 'price' in market_data:
                price = market_data['price']
            if not volume and 'volume' in market_data:
                volume = market_data['volume']
        
        # Verificar si tenemos datos válidos
        if not price or price <= 0:
            logger.debug(f"No se pudo obtener precio válido para {token}")
            return False
        
        # Inicializar estructuras si es necesario
        if token not in self.price_history:
            self.price_history[token] = deque(maxlen=self.max_history_points)
            self.volume_history[token] = deque(maxlen=self.max_history_points)
            self.price_points[token] = {"timestamp": [], "price": []}
        
        # Asegurarnos de que no guardamos datos duplicados
        # Solo añadir punto si ha pasado un tiempo mínimo o hay cambio significativo de precio
        add_point = True
        if self.price_points[token]["timestamp"]:
            last_timestamp = self.price_points[token]["timestamp"][-1]
            last_price = self.price_points[token]["price"][-1]
            min_time_diff = 300  # 5 minutos mínimo entre puntos
            min_price_change = 0.01  # 1% mínimo de cambio para registrar
            
            time_diff = timestamp - last_timestamp
            if last_price > 0:
                price_change_pct = abs(price - last_price) / last_price
            else:
                price_change_pct = 1  # Si no hay precio previo, consideramos cambio significativo
            
            add_point = time_diff >= min_time_diff or price_change_pct >= min_price_change
        
        if add_point:
            # Añadir datos a las colecciones
            self.price_history[token].append((timestamp, price))
            if volume:
                self.volume_history[token].append((timestamp, volume))
            
            # Mantener lista ordenada para análisis
            self.price_points[token]["timestamp"].append(timestamp)
            self.price_points[token]["price"].append(price)
            
            # Si la lista excede el máximo, eliminar el punto más antiguo
            if len(self.price_points[token]["timestamp"]) > self.max_history_points:
                self.price_points[token]["timestamp"].pop(0)
                self.price_points[token]["price"].pop(0)
            
            # Recalcular métricas si hay suficientes puntos
            if len(self.price_points[token]["price"]) >= self.min_points_for_analysis:
                self._calculate_token_metrics(token)
                
            # Invalidar caché de análisis
            if token in self.analysis_cache:
                del self.analysis_cache[token]
                
            return True
        
        return False
    
    def _calculate_token_metrics(self, token):
        """Calcula métricas técnicas para un token"""
        if token not in self.price_points or len(self.price_points[token]["price"]) < self.min_points_for_analysis:
            return
        
        # Obtener datos
        prices = self.price_points[token]["price"]
        timestamps = self.price_points[token]["timestamp"]
        
        # Inicializar métricas si es necesario
        if token not in self.token_metrics:
            self.token_metrics[token] = {}
        
        try:
            # Calcular volatilidad (desviación estándar normalizada)
            if len(prices) >= 3:
                volatility = np.std(prices) / np.mean(prices) if np.mean(prices) > 0 else 0
                self.token_metrics[token]["volatility"] = volatility
            
            # Calcular momentum actual (cambio porcentual reciente)
            if len(prices) >= 2:
                current_price = prices[-1]
                prev_price = prices[0]
                momentum = (current_price - prev_price) / prev_price if prev_price > 0 else 0
                self.token_metrics[token]["momentum"] = momentum
            
            # Calcular RSI simplificado si hay suficientes puntos
            if len(prices) >= 14:
                gains = []
                losses = []
                for i in range(1, len(prices)):
                    change = prices[i] - prices[i-1]
                    if change > 0:
                        gains.append(change)
                        losses.append(0)
                    else:
                        gains.append(0)
                        losses.append(abs(change))
                
                avg_gain = sum(gains) / len(gains) if gains else 0
                avg_loss = sum(losses) / len(losses) if losses else 1e-6  # Evitar división por cero
                
                rs = avg_gain / avg_loss if avg_loss > 0 else 0
                rsi = 100 - (100 / (1 + rs))
                self.token_metrics[token]["rsi"] = rsi
            
            # Identificar posibles soportes y resistencias
            self._identify_support_resistance(token, prices)
            
        except Exception as e:
            logger.error(f"Error calculando métricas para {token}: {e}")
    
    def _identify_support_resistance(self, token, prices):
        """Identifica niveles de soporte y resistencia para un token"""
        if len(prices) < 5:
            return
        
        # Inicializar si es necesario
        if token not in self.support_resistance:
            self.support_resistance[token] = {"supports": [], "resistances": []}
        
        try:
            # Limpiar niveles antiguos
            self.support_resistance[token] = {"supports": [], "resistances": []}
            
            # Detectar picos y valles locales
            peaks = []
            valleys = []
            
            for i in range(1, len(prices) - 1):
                # Pico local (resistencia potencial)
                if prices[i] > prices[i-1] and prices[i] > prices[i+1]:
                    peaks.append(prices[i])
                
                # Valle local (soporte potencial)
                if prices[i] < prices[i-1] and prices[i] < prices[i+1]:
                    valleys.append(prices[i])
            
            # Agrupar niveles similares (dentro de 2%)
            def group_levels(levels, tolerance=0.02):
                if not levels:
                    return []
                
                levels.sort()
                grouped = []
                current_group = [levels[0]]
                
                for level in levels[1:]:
                    avg_group = sum(current_group) / len(current_group)
                    if abs(level - avg_group) / avg_group <= tolerance:
                        current_group.append(level)
                    else:
                        grouped.append(sum(current_group) / len(current_group))
                        current_group = [level]
                
                if current_group:
                    grouped.append(sum(current_group) / len(current_group))
                
                return grouped
            
            # Agrupar niveles y guardar
            self.support_resistance[token]["supports"] = group_levels(valleys)
            self.support_resistance[token]["resistances"] = group_levels(peaks)
            
        except Exception as e:
            logger.error(f"Error identificando soporte/resistencia para {token}: {e}")
    
    async def analyze_volume_patterns(self, token, current_volume=None, market_data=None):
        """
        Analiza patrones de volumen para un token.
        
        Args:
            token: Dirección del token
            current_volume: Volumen actual (opcional)
            market_data: Datos completos de mercado (opcional)
            
        Returns:
            dict: Análisis de patrones de volumen
        """
        # Verificar caché primero
        cache_key = f"volume_patterns_{token}"
        now = time.time()
        
        if cache_key in self.analysis_cache:
            cache_entry = self.analysis_cache[cache_key]
            if now - cache_entry["timestamp"] < self.cache_ttl:
                return cache_entry["analysis"]
        
        # Inicializar resultado
        result = {
            "unusual_volume": False,
            "volume_surge": False,
            "volume_trend": "neutral",  # "increasing", "decreasing", "neutral"
            "volume_peak": False,
            "volume_growth_1h": 0,
            "volume_z_score": 0,  # Desviaciones estándar respecto a la media
        }
        
        # Asegurarnos de tener datos de volumen actualizados
        volume = current_volume
        if not volume and market_data and 'volume' in market_data:
            volume = market_data['volume']
        
        # Si no tenemos datos, actualizar primero
        if not volume or token not in self.volume_history or not self.volume_history[token]:
            if self.token_data_service:
                try:
                    if not market_data:
                        if hasattr(self.token_data_service, 'get_token_data_async'):
                            market_data = await self.token_data_service.get_token_data_async(token)
                        elif hasattr(self.token_data_service, 'get_token_data'):
                            market_data = self.token_data_service.get_token_data(token)
                    
                    if market_data and 'volume' in market_data:
                        volume = market_data['volume']
                        await self.update_price_data(token, None, volume, market_data)
                except Exception as e:
                    logger.error(f"Error obteniendo volumen para {token}: {e}")
                    
        # Si aún no tenemos datos suficientes, retornar el resultado por defecto
        if token not in self.volume_history or len(self.volume_history[token]) < self.min_points_for_analysis:
            logger.debug(f"Datos de volumen insuficientes para {token}")
            return result
        
        try:
            # Extraer volúmenes y timestamps
            volumes = [v for _, v in self.volume_history[token]]
            timestamps = [t for t, _ in self.volume_history[token]]
            
            if not volumes:
                return result
            
            # Análisis básico
            avg_volume = sum(volumes) / len(volumes)
            max_volume = max(volumes)
            current_vol = volumes[-1] if volumes else 0
            
            # Calcular Z-score (desviaciones estándar respecto a la media)
            if len(volumes) >= 3:
                std_volume = np.std(volumes)
                if std_volume > 0:
                    z_score = (current_vol - avg_volume) / std_volume
                    result["volume_z_score"] = z_score
                    result["unusual_volume"] = abs(z_score) > 2  # Más de 2 desviaciones estándar
            
            # Detectar picos de volumen
            if current_vol > avg_volume * 2:
                result["volume_peak"] = True
            
            # Analizar tendencia reciente del volumen
            if len(volumes) >= 3:
                recent_volumes = volumes[-3:]
                if recent_volumes[2] > recent_volumes[0] * 1.2:
                    result["volume_trend"] = "increasing"
                elif recent_volumes[2] < recent_volumes[0] * 0.8:
                    result["volume_trend"] = "decreasing"
            
            # Calcular crecimiento de volumen en 1 hora
            if len(volumes) >= 2 and len(timestamps) >= 2:
                # Encontrar volumen más cercano a hace 1 hora
                hour_ago = now - 3600
                closest_idx = min(range(len(timestamps)), key=lambda i: abs(timestamps[i] - hour_ago))
                if timestamps[closest_idx] < now - 300:  # Solo si tiene al menos 5 minutos de antigüedad
                    vol_1h_ago = volumes[closest_idx]
                    if vol_1h_ago > 0:
                        growth_1h = (current_vol - vol_1h_ago) / vol_1h_ago
                        result["volume_growth_1h"] = growth_1h
                        result["volume_surge"] = growth_1h > 0.5  # +50% en 1h
            
            # Guardar en caché
            self.analysis_cache[cache_key] = {
                "analysis": result,
                "timestamp": now
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Error en analyze_volume_patterns para {token}: {e}")
            return result
    
    async def detect_price_patterns(self, token, current_price=None, market_data=None):
        """
        Detecta patrones técnicos en el precio de un token.
        
        Args:
            token: Dirección del token
            current_price: Precio actual (opcional)
            market_data: Datos completos de mercado (opcional)
            
        Returns:
            dict: Análisis de patrones de precio
        """
        # Verificar caché primero
        cache_key = f"price_patterns_{token}"
        now = time.time()
        
        if cache_key in self.analysis_cache:
            cache_entry = self.analysis_cache[cache_key]
            if now - cache_entry["timestamp"] < self.cache_ttl:
                return cache_entry["analysis"]
        
        # Inicializar resultado
        result = {
            "breakout": False,
            "breakdown": False,
            "trend": "neutral",  # "bullish", "bearish", "neutral"
            "momentum": 0,
            "volatility": 0,
            "rsi": 50,
            "support_levels": [],
            "resistance_levels": [],
            "nearest_support": None,
            "nearest_resistance": None,
            "price_action_quality": 0  # 0-1 score de calidad del patrón
        }
        
        # Asegurarnos de tener datos de precio actualizados
        price = current_price
        if not price and market_data and 'price' in market_data:
            price = market_data['price']
        
        # Si no tenemos datos, actualizar primero
        if not price or token not in self.price_points or not self.price_points[token]["price"]:
            if self.token_data_service:
                try:
                    if not market_data:
                        if hasattr(self.token_data_service, 'get_token_data_async'):
                            market_data = await self.token_data_service.get_token_data_async(token)
                        elif hasattr(self.token_data_service, 'get_token_data'):
                            market_data = self.token_data_service.get_token_data(token)
                    
                    if market_data and 'price' in market_data:
                        price = market_data['price']
                        await self.update_price_data(token, price, None, market_data)
                except Exception as e:
                    logger.error(f"Error obteniendo precio para {token}: {e}")
                    
        # Si aún no tenemos datos suficientes, retornar el resultado por defecto
        if token not in self.price_points or len(self.price_points[token]["price"]) < self.min_points_for_analysis:
            logger.debug(f"Datos de precio insuficientes para {token}")
            return result
        
        try:
            # Obtener métricas calculadas
            if token in self.token_metrics:
                metrics = self.token_metrics[token]
                if "volatility" in metrics:
                    result["volatility"] = metrics["volatility"]
                if "momentum" in metrics:
                    result["momentum"] = metrics["momentum"]
                if "rsi" in metrics:
                    result["rsi"] = metrics["rsi"]
            
            # Obtener niveles de soporte y resistencia
            if token in self.support_resistance:
                sr_data = self.support_resistance[token]
                result["support_levels"] = sr_data["supports"]
                result["resistance_levels"] = sr_data["resistances"]
            
            # Determinar tendencia basada en momentum y RSI
            if result["momentum"] > 0.05:
                result["trend"] = "bullish"
            elif result["momentum"] < -0.05:
                result["trend"] = "bearish"
            
            # Refinar con RSI
            if "rsi" in result:
                if result["rsi"] > 70:
                    result["trend"] = "bullish"
                elif result["rsi"] < 30:
                    result["trend"] = "bearish"
            
            # Detectar breakouts y breakdowns
            current_price = self.price_points[token]["price"][-1]
            
            # Encontrar soporte/resistencia más cercanos
            if result["support_levels"]:
                supports_below = [s for s in result["support_levels"] if s < current_price]
                if supports_below:
                    result["nearest_support"] = max(supports_below)
            
            if result["resistance_levels"]:
                resistances_above = [r for r in result["resistance_levels"] if r > current_price]
                if resistances_above:
                    result["nearest_resistance"] = min(resistances_above)
            
            # Detectar breakout/breakdown
            prices = self.price_points[token]["price"]
            if len(prices) >= 3:
                # Comprobar si el precio ha superado resistencias recientes
                for r in result["resistance_levels"]:
                    # Verificar si el precio estaba por debajo y ahora por encima
                    if prices[-2] < r and current_price > r:
                        result["breakout"] = True
                        break
                
                # Comprobar si el precio ha roto soportes recientes
                for s in result["support_levels"]:
                    # Verificar si el precio estaba por encima y ahora por debajo
                    if prices[-2] > s and current_price < s:
                        result["breakdown"] = True
                        break
            
            # Calcular score de calidad del patrón
            # Factores: volatilidad, claridad de la tendencia, volumen correspondiente
            volatility_factor = min(result["volatility"] * 5, 1)  # Normalizar volatilidad (0-1)
            trend_clarity = abs(result["momentum"]) * 2  # Fuerza de la tendencia
            
            result["price_action_quality"] = (
                volatility_factor * 0.4 + 
                trend_clarity * 0.4 + 
                (0.2 if result["breakout"] or result["breakdown"] else 0)
            )
            
            # Guardar en caché
            self.analysis_cache[cache_key] = {
                "analysis": result,
                "timestamp": now
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Error en detect_price_patterns para {token}: {e}")
            return result
    
    async def calculate_token_quality_score(self, token, market_data=None):
        """
        Calcula un score de calidad para el token basado en patrones técnicos.
        
        Args:
            token: Dirección del token
            market_data: Datos de mercado (opcional)
            
        Returns:
            float: Score de calidad (0-1)
        """
        # Verificar caché primero
        cache_key = f"quality_score_{token}"
        now = time.time()
        
        if cache_key in self.analysis_cache:
            cache_entry = self.analysis_cache[cache_key]
            if now - cache_entry["timestamp"] < self.cache_ttl:
                return cache_entry["analysis"]
        
        try:
            # Obtener análisis de volumen y precio
            volume_analysis = await self.analyze_volume_patterns(token, None, market_data)
            price_analysis = await self.detect_price_patterns(token, None, market_data)
            
            # Extraer factores clave
            # 1. Calidad del patrón de precio
            price_quality = price_analysis["price_action_quality"]
            
            # 2. Factor de volumen
            volume_factor = 0.5  # Base
            if volume_analysis["unusual_volume"]:
                volume_factor += 0.2
            if volume_analysis["volume_surge"]:
                volume_factor += 0.2
            if volume_analysis["volume_trend"] == "increasing":
                volume_factor += 0.1
            
            # 3. Factor de tendencia
            trend_factor = 0.5  # Neutral
            if price_analysis["trend"] == "bullish":
                trend_factor = 0.8
            elif price_analysis["trend"] == "bearish":
                trend_factor = 0.3
            
            # 4. Factor de ruptura
            breakout_factor = 0
            if price_analysis["breakout"]:
                breakout_factor = 0.3
            elif price_analysis["breakdown"]:
                breakout_factor = -0.2
            
            # Combinar factores (peso diferente para cada uno)
            quality_score = (
                price_quality * 0.3 +
                volume_factor * 0.3 +
                trend_factor * 0.3 +
                (0.5 + breakout_factor) * 0.1  # Normalizar a 0-1
            )
            
            # Asegurar que está en el rango 0-1
            quality_score = max(0, min(1, quality_score))
            
            # Guardar en caché
            self.analysis_cache[cache_key] = {
                "analysis": quality_score,
                "timestamp": now
            }
            
            return quality_score
            
        except Exception as e:
            logger.error(f"Error en calculate_token_quality_score para {token}: {e}")
            return 0.5  # Valor neutral por defecto
    
    def get_token_metrics_snapshot(self, token):
        """
        Obtiene un snapshot de las métricas actuales del token.
        
        Args:
            token: Dirección del token
            
        Returns:
            dict: Métricas del token
        """
        if token not in self.token_metrics:
            return {}
        
        # Crear copia para evitar modificaciones
        return dict(self.token_metrics[token])
    
    def cleanup_old_data(self):
        """Limpia datos antiguos para liberar memoria"""
        # Limpiar caché de análisis
        now = time.time()
        for key in list(self.analysis_cache.keys()):
            if now - self.analysis_cache[key]["timestamp"] > self.cache_ttl * 2:
                del self.analysis_cache[key]
        
        # Identificar tokens sin actividad reciente
        tokens_to_remove = []
        for token, history in self.price_history.items():
            if not history:
                tokens_to_remove.append(token)
                continue
                
            last_timestamp = history[-1][0]
            if now - last_timestamp > 86400:  # Más de 24 horas sin actualizaciones
                tokens_to_remove.append(token)
        
        # Limpiar datos
        for token in tokens_to_remove:
            if token in self.price_history:
                del self.price_history[token]
            if token in self.volume_history:
                del self.volume_history[token]
            if token in self.price_points:
                del self.price_points[token]
            if token in self.token_metrics:
                del self.token_metrics[token]
            if token in self.support_resistance:
                del self.support_resistance[token]
