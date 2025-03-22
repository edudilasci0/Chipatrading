# performance_tracker.py
import asyncio
import time
import logging
from datetime import datetime, timedelta
import db

logger = logging.getLogger("performance_tracker")

class PerformanceTracker:
    """
    Realiza seguimiento del rendimiento de las señales emitidas
    con intervalos específicos de monitoreo.
    Ahora con seguimiento en tiempo real de la evolución de señales,
    incorporando datos sobre cambios en volumen, liquidez y comportamiento de traders.
    """
    
    TRACK_INTERVALS = [
        (3, "3m"),        # 3 minutos
        (5, "5m"),        # 5 minutos
        (10, "10m"),      # 10 minutos
        (30, "30m"),      # 30 minutos
        (60, "1h"),       # 1 hora
        (120, "2h"),      # 2 horas
        (240, "4h"),      # 4 horas
        (1440, "24h")     # 24 horas
    ]
    
    def __init__(self, token_data_service=None, dex_monitor=None, market_metrics=None, whale_detector=None):
        """
        Inicializa el tracker de rendimiento con servicios adicionales.
        
        Args:
            token_data_service: Servicio de datos de tokens (por ejemplo, HeliusClient)
            dex_monitor: Monitor de DEX para datos de liquidez en tiempo real
            market_metrics: Analizador de métricas de mercado 
            whale_detector: Detector de actividad de ballenas
        """
        self.token_data_service = token_data_service
        self.dex_monitor = dex_monitor
        self.market_metrics = market_metrics
        self.whale_detector = whale_detector
        self.signal_performance = {}  # {token: performance_data}
        self.last_prices = {}         # {token: price}
        self.signal_updates = {}      # {token: last_update_timestamp}
        self.early_stage_monitoring = {}  # {token: bool} - Monitoreo intensivo en etapa temprana
        logger.info(f"PerformanceTracker inicializado con servicios avanzados")
    
    def add_signal(self, token, signal_info):
        """
        Registra una nueva señal para hacer seguimiento.
        
        Args:
            token: Dirección del token
            signal_info: Diccionario con información de la señal
        """
        timestamp = int(time.time())
        # Obtener precio inicial usando token_data_service si está disponible
        initial_price = self._get_token_price(token)
        if initial_price == 0 and signal_info.get("initial_price"):
            initial_price = signal_info.get("initial_price")
        
        performance_data = {
            "timestamp": timestamp,
            "initial_price": initial_price,
            "min_price": initial_price if initial_price > 0 else 0,
            "initial_time": timestamp,
            "performances": {},  # Resultados por intervalo
            "max_price": initial_price if initial_price > 0 else 0,
            "max_gain": 0,
            "confidence": signal_info.get("confidence", 0),
            "traders_count": signal_info.get("traders_count", 0),
            "total_volume": signal_info.get("total_volume", 0),
            "signal_id": signal_info.get("signal_id", None),
            "token_name": signal_info.get("token_name", ""),
            "known_traders": signal_info.get("known_traders", []),
            "liquidity_initial": 0,
            "holder_count_initial": 0,
            "whale_activity": False,
            "liquidity_change": 0,
            "holder_growth": 0,
            "volume_change": 0
        }
        
        # Obtener liquidez inicial si está disponible
        if self.dex_monitor:
            asyncio.create_task(self._get_initial_liquidity(token, performance_data))
        
        # Obtener cantidad de holders inicial si está disponible
        if self.market_metrics:
            asyncio.create_task(self._get_initial_holders(token, performance_data))
        
        self.signal_performance[token] = performance_data
        self.last_prices[token] = initial_price
        self.signal_updates[token] = timestamp
        self.early_stage_monitoring[token] = True  # Activar monitoreo intensivo
        
        # Iniciar seguimiento asíncrono
        asyncio.create_task(self._track_performance(token))
        logger.info(f"Iniciado seguimiento para token {token} con precio inicial ${initial_price}")
    
    async def _get_initial_liquidity(self, token, performance_data):
        """Obtiene y almacena la liquidez inicial del token"""
        try:
            if self.dex_monitor:
                liquidity_data = await self.dex_monitor.get_combined_liquidity_data(token)
                performance_data["liquidity_initial"] = liquidity_data.get("total_liquidity_usd", 0)
                logger.debug(f"Liquidez inicial para {token}: ${performance_data['liquidity_initial']}")
        except Exception as e:
            logger.error(f"Error obteniendo liquidez inicial para {token}: {e}")
    
    async def _get_initial_holders(self, token, performance_data):
        """Obtiene y almacena el número inicial de holders del token"""
        try:
            if self.market_metrics:
                holder_data = await self.market_metrics.get_holder_growth(token)
                performance_data["holder_count_initial"] = holder_data.get("holder_count", 0)
                logger.debug(f"Holders iniciales para {token}: {performance_data['holder_count_initial']}")
        except Exception as e:
            logger.error(f"Error obteniendo holders iniciales para {token}: {e}")
    
    async def _track_performance(self, token):
        """
        Realiza seguimiento de rendimiento en múltiples intervalos con monitoreo en tiempo real.
        
        Args:
            token: Dirección del token a seguir
        """
        # Monitoreo intensivo durante la primera hora
        if self.early_stage_monitoring.get(token, False):
            await self._early_stage_monitoring(token)
        
        for minutes, label in self.TRACK_INTERVALS:
            try:
                await asyncio.sleep(minutes * 60)
                
                if token not in self.signal_performance:
                    logger.warning(f"Token {token} ya no está en seguimiento, cancelando monitor")
                    break
                
                # Obtener datos combinados para reporte enriquecido
                performance_data = await self._gather_performance_data(token)
                
                # Actualizar precio mínimo si es menor
                if performance_data["current_price"] < self.signal_performance[token]["min_price"]:
                    self.signal_performance[token]["min_price"] = performance_data["current_price"]
                
                initial_price = self.signal_performance[token]["initial_price"]
                percent_change = ((performance_data["current_price"] - initial_price) / initial_price) * 100 if initial_price > 0 else 0
                
                # Actualizar máximo y ganancia máxima
                if performance_data["current_price"] > self.signal_performance[token]["max_price"]:
                    self.signal_performance[token]["max_price"] = performance_data["current_price"]
                    max_gain = ((performance_data["current_price"] - initial_price) / initial_price) * 100 if initial_price > 0 else 0
                    self.signal_performance[token]["max_gain"] = max_gain
                
                # Guardar el resultado del intervalo
                performance_entry = {
                    "price": performance_data["current_price"],
                    "percent_change": percent_change,
                    "timestamp": int(time.time()),
                    "liquidity": performance_data.get("liquidity", 0),
                    "volume": performance_data.get("volume", 0),
                    "holder_count": performance_data.get("holder_count", 0),
                    "whale_activity": performance_data.get("whale_activity", False)
                }
                self.signal_performance[token]["performances"][label] = performance_entry
                
                # Enviar reporte con información ampliada
                from telegram_utils import send_performance_report
                
                # Formatear volumen para mejor lectura
                volume = performance_data.get("volume", 0)
                if volume > 1000000:
                    volume_display = f"${volume/1000000:.2f}M"
                elif volume > 1000:
                    volume_display = f"${volume/1000:.2f}K"
                else:
                    volume_display = f"${volume:.2f}"
                
                send_performance_report(
                    token=token,
                    signal_id=self.signal_performance[token].get("signal_id", ""),
                    timeframe=label,
                    percent_change=percent_change,
                    volatility=performance_data.get("volatility", 0),
                    trend=performance_data.get("trend", "Neutral"),
                    volume_display=volume_display,
                    traders_count=len(performance_data.get("active_traders", [])),
                    whale_activity=performance_data.get("whale_activity", False),
                    liquidity_change=performance_data.get("liquidity_change", 0)
                )
                
                # Guardar en base de datos
                self._save_performance_data(token, label, percent_change, performance_data)
                
                logger.info(f"Actualización para {token} ({label}): {percent_change:.2f}% | Liq: ${performance_data.get('liquidity', 0):.2f} | Vol: {volume_display}")
                
            except Exception as e:
                logger.error(f"🚨 Error en seguimiento de {token} a {label}: {e}")
    
    async def _early_stage_monitoring(self, token):
        """
        Monitoreo intensivo durante la primera hora después de la señal.
        Actualizaciones cada 3-5 minutos para capturar movimientos rápidos.
        """
        early_intervals = [3, 8, 15, 25]  # Minutos
        
        for minutes in early_intervals:
            try:
                await asyncio.sleep(minutes * 60)
                
                if token not in self.signal_performance:
                    return
                
                # Verificar si todavía estamos en la primera hora
                now = time.time()
                signal_time = self.signal_performance[token]["timestamp"]
                if now - signal_time > 3600:  # 1 hora
                    self.early_stage_monitoring[token] = False
                    return
                
                # Recopilar datos y actualizar
                performance_data = await self._gather_performance_data(token)
                
                # Sólo actualizamos datos, no enviamos reporte completo
                self.last_prices[token] = performance_data["current_price"]
                self.signal_updates[token] = now
                
                # Si hay un cambio significativo (>10%), enviar actualización rápida
                initial_price = self.signal_performance[token]["initial_price"]
                if initial_price > 0:
                    percent_change = ((performance_data["current_price"] - initial_price) / initial_price) * 100
                    if abs(percent_change) > 10:
                        # Enviar actualización rápida
                        from telegram_utils import send_telegram_message
                        
                        # Determinar emoji basado en el cambio
                        emoji = "🚀" if percent_change > 10 else "⚠️"
                        
                        message = (
                            f"*{emoji} Actualización Rápida #{self.signal_performance[token].get('signal_id', '')}*\n\n"
                            f"Token: `{token}`\n"
                            f"Cambio: *{percent_change:.2f}%*\n"
                            f"Tiempo desde señal: {int((now - signal_time) / 60)} min\n\n"
                            f"[Ver en Birdeye](https://birdeye.so/token/{token}?chain=solana)"
                        )
                        
                        send_telegram_message(message)
                
                logger.debug(f"Monitoreo temprano para {token}: {percent_change:.2f}% después de {int((now - signal_time) / 60)} min")
                
            except Exception as e:
                logger.error(f"Error en monitoreo temprano de {token}: {e}")
    
    async def _gather_performance_data(self, token):
        """
        Recopila datos de rendimiento de múltiples fuentes para un análisis enriquecido.
        
        Args:
            token: Dirección del token
            
        Returns:
            dict: Datos de rendimiento combinados
        """
        result = {
            "current_price": self._get_token_price(token),
            "timestamp": int(time.time())
        }
        
        # Tareas asíncronas para recopilar datos en paralelo
        tasks = []
        
        # 1. Datos de DEX (liquidez)
        if self.dex_monitor:
            tasks.append(self._get_liquidity_data(token, result))
        
        # 2. Datos de mercado (holders, trending)
        if self.market_metrics:
            tasks.append(self._get_market_data(token, result))
        
        # 3. Datos de actividad de ballenas
        if self.whale_detector:
            tasks.append(self._get_whale_activity(token, result))
        
        # 4. Datos de análisis técnico
        if hasattr(self, 'token_analyzer') and self.token_analyzer:
            tasks.append(self._get_technical_analysis(token, result))
        
        # Ejecutar todas las tareas en paralelo
        if tasks:
            await asyncio.gather(*tasks)
        
        # Calcular cambios respecto a valores iniciales
        if token in self.signal_performance:
            # Cambio en liquidez
            initial_liquidity = self.signal_performance[token].get("liquidity_initial", 0)
            current_liquidity = result.get("liquidity", 0)
            if initial_liquidity > 0 and current_liquidity > 0:
                result["liquidity_change"] = ((current_liquidity - initial_liquidity) / initial_liquidity) * 100
            
            # Cambio en holders
            initial_holders = self.signal_performance[token].get("holder_count_initial", 0)
            current_holders = result.get("holder_count", 0)
            if initial_holders > 0 and current_holders > 0:
                result["holder_growth"] = ((current_holders - initial_holders) / initial_holders) * 100
        
        return result
    
    async def _get_liquidity_data(self, token, result):
        """Obtiene datos de liquidez desde DEX Monitor"""
        try:
            liquidity_data = await self.dex_monitor.get_combined_liquidity_data(token)
            if liquidity_data:
                result["liquidity"] = liquidity_data.get("total_liquidity_usd", 0)
                result["volume"] = liquidity_data.get("volume_24h", 0)
                result["slippage_1k"] = liquidity_data.get("slippage_1k", 0)
                result["slippage_10k"] = liquidity_data.get("slippage_10k", 0)
        except Exception as e:
            logger.error(f"Error obteniendo datos de liquidez para {token}: {e}")
    
    async def _get_market_data(self, token, result):
        """Obtiene datos de mercado desde Market Metrics"""
        try:
            holder_data = await self.market_metrics.get_holder_growth(token)
            if holder_data:
                result["holder_count"] = holder_data.get("holder_count", 0)
                result["holder_growth_rate_1h"] = holder_data.get("growth_rate_1h", 0)
            
            trending_data = await self.market_metrics.check_trending_status(token)
            if trending_data:
                result["is_trending"] = trending_data.get("is_trending", False)
                result["trending_platforms"] = trending_data.get("trending_platforms", [])
        except Exception as e:
            logger.error(f"Error obteniendo datos de mercado para {token}: {e}")
    
    async def _get_whale_activity(self, token, result):
        """Obtiene datos de actividad de ballenas"""
        try:
            # Obtener transacciones recientes
            recent_transactions = db.get_token_transactions(token, hours=1)
            
            whale_report = await self.whale_detector.detect_large_transactions(
                token, recent_transactions, result.get("market_cap", 0)
            )
            
            result["whale_activity"] = whale_report.get("has_whale_activity", False)
            result["whale_transactions"] = whale_report.get("whale_transactions", [])
            result["whale_impact_score"] = whale_report.get("impact_score", 0)
        except Exception as e:
            logger.error(f"Error obteniendo actividad de ballenas para {token}: {e}")
    
    async def _get_technical_analysis(self, token, result):
        """Obtiene análisis técnico del token"""
        try:
            # Actualizar datos de precio si aún no lo hemos hecho
            if "current_price" in result:
                await self.token_analyzer.update_price_data(token, result["current_price"])
            
            # Analizar patrones de volumen
            volume_patterns = await self.token_analyzer.analyze_volume_patterns(token)
            if volume_patterns:
                result["volume_trend"] = volume_patterns.get("volume_trend", "neutral")
                result["volume_surge"] = volume_patterns.get("volume_surge", False)
            
            # Detectar patrones de precio
            price_patterns = await self.token_analyzer.detect_price_patterns(token)
            if price_patterns:
                result["trend"] = price_patterns.get("trend", "neutral")
                result["volatility"] = price_patterns.get("volatility", 0)
                result["rsi"] = price_patterns.get("rsi", 50)
                result["support_levels"] = price_patterns.get("support_levels", [])
                result["resistance_levels"] = price_patterns.get("resistance_levels", [])
        except Exception as e:
            logger.error(f"Error obteniendo análisis técnico para {token}: {e}")
    
    def _save_performance_data(self, token, timeframe, percent_change, performance_data):
        """
        Guarda los datos de rendimiento en la base de datos.
        
        Args:
            token: Dirección del token
            timeframe: Intervalo de tiempo
            percent_change: Porcentaje de cambio
            performance_data: Datos completos de rendimiento
        """
        try:
            signal_data = self.signal_performance.get(token, {})
            db.save_signal_performance(
                token=token,
                signal_id=signal_data.get("signal_id"),
                timeframe=timeframe,
                percent_change=percent_change,
                confidence=signal_data.get('confidence', 0),
                traders_count=signal_data.get('traders_count', 0),
                extra_data={
                    "liquidity": performance_data.get("liquidity", 0),
                    "volume": performance_data.get("volume", 0),
                    "whale_activity": performance_data.get("whale_activity", False),
                    "holder_count": performance_data.get("holder_count", 0),
                    "is_trending": performance_data.get("is_trending", False)
                }
            )
        except Exception as e:
            logger.error(f"🚨 Error guardando datos para {token}: {e}")
    
    async def _async_get_token_price(self, token):
        """
        Versión asíncrona para obtener el precio del token usando token_data_service.
        Implementa fallback y actualiza la caché.
        """
        if self.token_data_service:
            try:
                if hasattr(self.token_data_service, 'get_token_price'):
                    price = await self.token_data_service.get_token_price(token)
                    if price and price > 0:
                        self.last_prices[token] = price
                        return price
                elif hasattr(self.token_data_service, 'get_token_data_async'):
                    token_data = await self.token_data_service.get_token_data_async(token)
                    if token_data and 'price' in token_data and token_data['price'] > 0:
                        price = token_data['price']
                        self.last_prices[token] = price
                        return price
            except Exception as e:
                logger.error(f"Error en token_data_service.get_token_price para {token}: {e}")
        
        # Intento secundario a través de DEX Monitor
        if self.dex_monitor:
            try:
                liquidity_data = await self.dex_monitor.get_combined_liquidity_data(token)
                if liquidity_data and liquidity_data.get("price", 0) > 0:
                    price = liquidity_data["price"]
                    self.last_prices[token] = price
                    return price
            except Exception as e:
                logger.warning(f"Error obteniendo precio desde DEX Monitor para {token}: {e}")
        
        # Si fallaron todos los intentos, retornamos el último precio conocido
        return self.last_prices.get(token, 0)
    
    def _get_token_price(self, token):
        """
        Método sincrónico para obtener el precio del token.
        Retorna el último precio conocido.
        """
        try:
            # Si tenemos un precio reciente en caché, lo usamos
            if token in self.last_prices:
                return self.last_prices.get(token, 0)
            
            # Si no, intentamos obtenerlo de forma sincrónica si es posible
            if self.token_data_service and hasattr(self.token_data_service, 'get_token_data'):
                token_data = self.token_data_service.get_token_data(token)
                if token_data and 'price' in token_data and token_data['price'] > 0:
                    self.last_prices[token] = token_data['price']
                    return token_data['price']
        except Exception as e:
            logger.error(f"🚨 Error obteniendo precio para {token}: {e}")
        
        return 0  # En caso de no poder obtener el precio
