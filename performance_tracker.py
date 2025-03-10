import asyncio
import time
import requests
import json
from datetime import datetime, timedelta
import db
from telegram_utils import send_telegram_message
import logging

logger = logging.getLogger("performance_tracker")

class PerformanceTracker:
    """
    Realiza seguimiento del rendimiento de las señales emitidas
    con intervalos específicos de monitoreo.
    Ahora utiliza el servicio de datos (token_data_service) en lugar de DexScreener.
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
            token_data_service: Servicio de datos de tokens (HeliusTokenDataService)
        """
        self.token_data_service = token_data_service
        self.signal_performance = {}  # {token: performance_data}
        self.last_prices = {}  # {token: price}
    
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
                    break
                
                current_price = await self._async_get_token_price(token)
                
                if not current_price:
                    continue
                
                initial_price = self.signal_performance[token]["initial_price"]
                percent_change = ((current_price - initial_price) / initial_price) * 100
                
                if current_price > self.signal_performance[token]["max_price"]:
                    self.signal_performance[token]["max_price"] = current_price
                    max_gain = ((current_price - initial_price) / initial_price) * 100
                    self.signal_performance[token]["max_gain"] = max_gain
                
                performance_entry = {
                    "price": current_price,
                    "percent_change": percent_change,
                    "timestamp": int(time.time())
                }
                
                self.signal_performance[token]["performances"][label] = performance_entry
                
                self._send_performance_report(token, label, percent_change)
                self._save_performance_data(token, label, percent_change)
                
            except Exception as e:
                logger.error(f"🚨 Error en seguimiento de {token} a {label}: {e}")
    
    def _send_performance_report(self, token, timeframe, percent_change):
        """
        Envía un reporte de rendimiento a Telegram.
        
        Args:
            token: Dirección del token
            timeframe: Intervalo de tiempo
            percent_change: Porcentaje de cambio
        """
        if percent_change > 50:
            emoji = "🚀"  # Excelente
        elif percent_change > 20:
            emoji = "🔥"  # Muy bueno
        elif percent_change > 0:
            emoji = "✅"  # Positivo
        elif percent_change > -20:
            emoji = "⚠️"  # Negativo pero moderado
        else:
            emoji = "❌"  # Muy negativo
        
        signal_id = self.signal_performance[token].get("signal_id", "")
        
        # En este ejemplo, solo se muestra el enlace de Neo BullX (se puede ajustar)
        neobullx_link = f"https://neo.bullx.io/terminal?chainId=1399811149&address={token}"
        
        message = (
            f"*🔍 Seguimiento {timeframe} {signal_id}*\n\n"
            f"Token: `{token}`\n"
            f"Cambio: *{percent_change:.2f}%* {emoji}\n"
            f"Confianza inicial: `{self.signal_performance[token]['confidence']:.2f}`\n"
            f"Traders involucrados: `{self.signal_performance[token]['traders_count']}`\n\n"
            f"🔗 *Enlace Neo BullX:*\n"
            f"• [Ver en Neo BullX]({neobullx_link})\n"
        )
        
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
            logger.error(f"🚨 Error guardando datos de rendimiento para {token}: {e}")
    
    async def _async_get_token_price(self, token):
        """
        Versión asíncrona para obtener el precio del token usando token_data_service.
        """
        if self.token_data_service:
            try:
                price = await self.token_data_service.get_token_price(token)
                if price:
                    self.last_prices[token] = price
                    return price
            except Exception as e:
                logger.error(f"Error en token_data_service.get_token_price para {token}: {e}")
        return self._get_token_price(token)
    
    def _get_token_price(self, token):
        """
        Método de respaldo para obtener el precio del token.
        Se utiliza si token_data_service no está disponible.
        """
        try:
            # Si por alguna razón token_data_service no está configurado, se intenta obtener precio
            # Aquí podrías integrar otra fuente o simplemente retornar el último precio conocido
            return self.last_prices.get(token, 0)
        except Exception as e:
            logger.error(f"🚨 Error obteniendo precio para {token}: {e}")
            return self.last_prices.get(token, 0)
