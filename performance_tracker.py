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
    Ahora utiliza el servicio de datos (token_data_service) en lugar de DexScreener.
    Se implementan fallbacks, interpolación de datos y análisis básico (volatilidad y tendencia).
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
    
    def __init__(self, token_data_service=None):
        """
        Inicializa el tracker de rendimiento.
        
        Args:
            token_data_service: Servicio de datos de tokens (por ejemplo, HeliusClient)
        """
        self.token_data_service = token_data_service
        self.signal_performance = {}  # {token: performance_data}
        self.last_prices = {}         # {token: price}
        logger.info(f"PerformanceTracker inicializado con servicio: {type(token_data_service).__name__ if token_data_service else 'Ninguno'}")
    
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
        
        performance_data = {
            "timestamp": timestamp,
            "initial_price": initial_price,
            "min_price": initial_price,   # Para calcular volatilidad
            "initial_time": timestamp,
            "performances": {},  # Resultados por intervalo
            "max_price": initial_price,
            "max_gain": 0,
            "confidence": signal_info.get("confidence", 0),
            "traders_count": signal_info.get("traders_count", 0),
            "total_volume": signal_info.get("total_volume", 0),
            "signal_id": signal_info.get("signal_id", None)
        }
        
        self.signal_performance[token] = performance_data
        self.last_prices[token] = initial_price
        
        # Iniciar seguimiento asíncrono
        asyncio.create_task(self._track_performance(token))
        logger.info(f"Iniciado seguimiento para token {token} con precio inicial ${initial_price}")
    
    async def _track_performance(self, token):
        """
        Realiza seguimiento de rendimiento en múltiples intervalos.
        
        Args:
            token: Dirección del token a seguir
        """
        for minutes, label in self.TRACK_INTERVALS:
            try:
                await asyncio.sleep(minutes * 60)
                
                if token not in self.signal_performance:
                    logger.warning(f"Token {token} ya no está en seguimiento, cancelando monitor")
                    break
                
                # Obtener precio de forma asíncrona
                current_price = await self._async_get_token_price(token)
                if not current_price or current_price == 0:
                    # Intentar interpolar si no se obtuvo precio
                    current_price = self._interpolate_price(token)
                    logger.info(f"Precio interpolado para {token}: ${current_price}")
                
                # Actualizar precio mínimo si es menor
                if current_price < self.signal_performance[token]["min_price"]:
                    self.signal_performance[token]["min_price"] = current_price
                
                initial_price = self.signal_performance[token]["initial_price"]
                percent_change = ((current_price - initial_price) / initial_price) * 100 if initial_price > 0 else 0
                
                # Actualizar máximo y ganancia máxima
                if current_price > self.signal_performance[token]["max_price"]:
                    self.signal_performance[token]["max_price"] = current_price
                    max_gain = ((current_price - initial_price) / initial_price) * 100 if initial_price > 0 else 0
                    self.signal_performance[token]["max_gain"] = max_gain
                
                # Guardar el resultado del intervalo
                performance_entry = {
                    "price": current_price,
                    "percent_change": percent_change,
                    "timestamp": int(time.time())
                }
                self.signal_performance[token]["performances"][label] = performance_entry
                
                # Calcular volatilidad (diferencia entre max y min)
                volatility = ((self.signal_performance[token]["max_price"] - self.signal_performance[token]["min_price"]) / initial_price * 100) if initial_price > 0 else 0
                # Calcular tendencia básica
                trend = self._calculate_trend(token)
                
                # Enviar reporte con información ampliada
                self._send_performance_report(token, label, percent_change, volatility, trend)
                self._save_performance_data(token, label, percent_change)
                
                logger.info(f"Actualización para {token} ({label}): {percent_change:.2f}% | Volatilidad: {volatility:.2f}% | Tendencia: {trend}")
                
            except Exception as e:
                logger.error(f"🚨 Error en seguimiento de {token} a {label}: {e}")
    
    def _send_performance_report(self, token, timeframe, percent_change, volatility, trend):
        """
        Envía un reporte de rendimiento a Telegram con información ampliada.
        
        Args:
            token: Dirección del token
            timeframe: Intervalo de tiempo
            percent_change: Porcentaje de cambio
            volatility: Volatilidad calculada
            trend: Predicción de tendencia (Ascendente, Descendente, Estable)
        """
        # Importación dinámica para evitar la dependencia circular
        from telegram_utils import send_telegram_message
        
        # Selección de emoji según el desempeño
        if percent_change > 50:
            emoji = "🚀"  # Excelente
        elif percent_change > 20:
            emoji = "🔥"  # Muy bueno
        elif percent_change > 0:
            emoji = "✅"  # Positivo
        elif percent_change > -20:
            emoji = "⚠️"  # Moderado
        else:
            emoji = "❌"  # Muy negativo
        
        signal_id = self.signal_performance[token].get("signal_id", "")
        
        # Enlaces a múltiples exploradores
        neobullx_link = f"https://neo.bullx.io/terminal?chainId=1399811149&address={token}"
        solscan_link = f"https://solscan.io/token/{token}"
        birdeye_link = f"https://birdeye.so/token/{token}?chain=solana"
        
        # Se incluyen volumen y tendencia si están disponibles
        total_volume = self.signal_performance[token].get("total_volume", "N/A")
        traders = self.signal_performance[token].get("traders_count", "N/A")
        
        message = (
            f"*🔍 Seguimiento {timeframe} {signal_id}*\n\n"
            f"Token: `{token}`\n"
            f"Cambio: *{percent_change:.2f}%* {emoji}\n"
            f"Volatilidad: *{volatility:.2f}%*\n"
            f"Tendencia: *{trend}*\n"
            f"Volumen: `{total_volume}`\n"
            f"Traders activos: `{traders}`\n\n"
            f"🔗 *Exploradores:*\n"
            f"• [Neo BullX]({neobullx_link})\n"
            f"• [Solscan]({solscan_link})\n"
            f"• [Birdeye]({birdeye_link})\n"
        )
        
        # Si se dispone de una predicción de tendencia adicional, se puede incluir
        if "trend_prediction" in self.signal_performance[token]:
            message += f"\n📈 Predicción de tendencia: {self.signal_performance[token]['trend_prediction']}\n"
        
        send_telegram_message(message)
    
    def _save_performance_data(self, token, timeframe, percent_change):
        """
        Guarda los datos de rendimiento en la base de datos.
        
        Args:
            token: Dirección del token
            timeframe: Intervalo de tiempo
            percent_change: Porcentaje de cambio
        """
        try:
            signal_data = self.signal_performance[token]
            db.save_signal_performance(
                token=token,
                signal_id=signal_data.get("signal_id"),
                timeframe=timeframe,
                percent_change=percent_change,
                confidence=signal_data['confidence'],
                traders_count=signal_data['traders_count']
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
        
        # Si falla, se intenta obtener precio de respaldo
        fallback_price = self._get_token_price(token)
        if fallback_price == 0:
            fallback_price = self._interpolate_price(token)
        return fallback_price
    
    def _get_token_price(self, token):
        """
        Método de respaldo para obtener el precio del token.
        Retorna el último precio conocido.
        """
        try:
            return self.last_prices.get(token, 0)
        except Exception as e:
            logger.error(f"🚨 Error obteniendo precio para {token}: {e}")
            return self.last_prices.get(token, 0)
    
    def _interpolate_price(self, token):
        """
        Intenta interpolar el precio del token usando datos previos de performance.
        Si existen al menos dos registros, se promedia el último par.
        """
        perf = self.signal_performance.get(token, {}).get("performances", {})
        entries = list(perf.values())
        if len(entries) >= 2:
            entries.sort(key=lambda x: x["timestamp"])
            p1 = entries[-2]["price"]
            p2 = entries[-1]["price"]
            interpolated = (p1 + p2) / 2.0
            self.last_prices[token] = interpolated
            return interpolated
        return self.last_prices.get(token, 0)
    
    def _calculate_trend(self, token):
        """
        Calcula una tendencia básica comparando los últimos dos registros de performance.
        Retorna 'Ascendente', 'Descendente' o 'Estable'.
        """
        perf = self.signal_performance.get(token, {}).get("performances", {})
        entries = list(perf.values())
        if len(entries) >= 2:
            entries.sort(key=lambda x: x["timestamp"])
            prev = entries[-2]["percent_change"]
            last = entries[-1]["percent_change"]
            if last > prev:
                return "Ascendente"
            elif last < prev:
                return "Descendente"
            else:
                return "Estable"
        return "No determinado"
