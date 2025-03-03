import asyncio
import time
import requests
import json
from datetime import datetime, timedelta
import db
from telegram_utils import send_telegram_message

class PerformanceTracker:
    """
    Realiza seguimiento del rendimiento de las señales emitidas
    con intervalos específicos de monitoreo.
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
    
    def __init__(self, dex_client=None):
        """
        Inicializa el tracker de rendimiento.
        
        Args:
            dex_client: Instancia de DexScreenerClient
        """
        self.dex_client = dex_client
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
        
        # Obtener precio inicial
        initial_price = self._get_token_price(token)
        
        # Preparar estructura de seguimiento
        performance_data = {
            "timestamp": timestamp,
            "initial_price": initial_price,
            "initial_time": timestamp,
            "performances": {},  # Almacenará resultados de cada intervalo
            "max_price": initial_price,
            "max_gain": 0,
            "confidence": signal_info.get("confidence", 0),
            "traders_count": signal_info.get("traders_count", 0),
            "total_volume": signal_info.get("total_volume", 0),
            "signal_id": signal_info.get("signal_id", None)  # Guardar el ID de la señal
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
                # Esperar el tiempo correspondiente
                await asyncio.sleep(minutes * 60)
                
                # Verificar si el token aún está siendo monitoreado
                if token not in self.signal_performance:
                    break
                
                # Obtener precio actual
                current_price = self._get_token_price(token)
                
                if not current_price:
                    continue
                
                # Calcular cambios
                initial_price = self.signal_performance[token]["initial_price"]
                percent_change = ((current_price - initial_price) / initial_price) * 100
                
                # Actualizar máximo precio y ganancia
                if current_price > self.signal_performance[token]["max_price"]:
                    self.signal_performance[token]["max_price"] = current_price
                    max_gain = ((current_price - initial_price) / initial_price) * 100
                    self.signal_performance[token]["max_gain"] = max_gain
                
                # Registrar performance
                performance_entry = {
                    "price": current_price,
                    "percent_change": percent_change,
                    "timestamp": int(time.time())
                }
                
                self.signal_performance[token]["performances"][label] = performance_entry
                
                # Enviar mensaje de seguimiento
                self._send_performance_report(token, label, percent_change)
                
                # Guardar en base de datos para análisis
                self._save_performance_data(token, label, percent_change)
                
            except Exception as e:
                print(f"🚨 Error en seguimiento de {token} a {label}: {e}")
    
    def _send_performance_report(self, token, timeframe, percent_change):
        """
        Envía un reporte de rendimiento a Telegram.
        
        Args:
            token: Dirección del token
            timeframe: Intervalo de tiempo
            percent_change: Porcentaje de cambio
        """
        # Determinar emoji según rendimiento
        if percent_change > 50:
            emoji = "🚀"  # Excelente
        elif percent_change > 20:
            emoji = "🔥"  # Muy bueno
        elif percent_change > 0:
            emoji = "✅"  # Positivo
        elif percent_change > -20:
            emoji = "⚠️"  # Negativo pero no terrible
        else:
            emoji = "❌"  # Muy negativo
        
        # Obtener el ID de la señal
        signal_id = self.signal_performance[token].get("signal_id", "")
        
        # Crear enlaces a exploradores
        dexscreener_link = f"https://dexscreener.com/solana/{token}"
        birdeye_link = f"https://birdeye.so/token/{token}?chain=solana"
        neobullx_link = f"https://neo.bullx.io/terminal?chainId=1399811149&address={token}"
        
        # Formatear mensaje
        message = (
            f"*🔍 Seguimiento {timeframe} {signal_id}*\n\n"
            f"Token: `{token}`\n"
            f"Cambio: *{percent_change:.2f}%* {emoji}\n"
            f"Confianza inicial: `{self.signal_performance[token]['confidence']:.2f}`\n"
            f"Traders involucrados: `{self.signal_performance[token]['traders_count']}`\n\n"
            f"🔗 *Enlaces*:\n"
            f"• [DexScreener]({dexscreener_link})\n"
            f"• [Birdeye]({birdeye_link})\n"
            f"• [Neo BullX]({neobullx_link})\n"
        )
        
        # Enviar mensaje a Telegram
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
            # Obtener datos de la señal
            signal_data = self.signal_performance[token]
            
            # Guardar en base de datos
            db.save_signal_performance(
                token=token,
                signal_id=None,  # Implementar recuperación de signal_id si es necesario
                timeframe=timeframe,
                percent_change=percent_change,
                confidence=signal_data['confidence'],
                traders_count=signal_data['traders_count']
            )
        except Exception as e:
            print(f"🚨 Error guardando datos de rendimiento: {e}")
    
    def _get_token_price(self, token):
        """
        Obtiene el precio actual del token.
        
        Args:
            token: Dirección del token
        
        Returns:
            float: Precio actual o None
        """
        try:
            # Intentar obtener precio usando DexScreener
            if self.dex_client:
                price = self.dex_client.get_token_price(token)
                if price:
                    self.last_prices[token] = price
                    return price
            
            # Método de respaldo
            return self._fetch_token_price(token)
        except Exception as e:
            print(f"🚨 Error obteniendo precio para {token}: {e}")
            return self.last_prices.get(token)
    
    def _fetch_token_price(self, token):
        """
        Método de respaldo para obtener precio del token.
        
        Args:
            token: Dirección del token
        
        Returns:
            float: Precio del token o None
        """
        try:
            url = f"https://api.dexscreener.com/latest/dex/tokens/{token}"
            response = requests.get(url, timeout=5)
            data = response.json()
            
            if "pairs" in data and data["pairs"]:
                price = float(data["pairs"][0]["priceUsd"])
                self.last_prices[token] = price
                return price
            return None
        except Exception as e:
            print(f"Error obteniendo precio: {e}")
            return None
