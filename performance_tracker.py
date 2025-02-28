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
    y actualiza estadísticas de efectividad.
    """
    
    def __init__(self, dex_client=None):
        """
        Inicializa el tracker de rendimiento.
        
        Args:
            dex_client: Instancia de DexScreenerClient o None para crear uno nuevo.
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
        
        self.signal_performance[token] = {
            "timestamp": timestamp,
            "initial_price": initial_price,
            "price_24h": None,
            "price_48h": None,
            "price_1w": None,
            "max_price": initial_price,
            "max_gain": 0,
            "confidence": signal_info.get("confidence", 0),
            "traders_count": signal_info.get("traders_count", 0),
            "total_volume": signal_info.get("total_volume", 0),
            "checked_24h": False,
            "checked_48h": False,
            "checked_1w": False
        }
        
        # Guardar precio inicial
        self.last_prices[token] = initial_price
        
        # Programar checkeos futuros
        asyncio.create_task(self._schedule_checkups(token))
    
    async def _schedule_checkups(self, token):
        """
        Programa verificaciones a las 24h, 48h y 1 semana.
        """
        # Verificar a las 24 horas
        await asyncio.sleep(24 * 60 * 60)  # 24 horas
        await self._check_performance(token, "24h")
        
        # Verificar a las 48 horas
        await asyncio.sleep(24 * 60 * 60)  # 24 horas más
        await self._check_performance(token, "48h")
        
        # Verificar a la semana
        await asyncio.sleep(5 * 24 * 60 * 60)  # 5 días más
        await self._check_performance(token, "1w")
    
    async def _check_performance(self, token, timeframe):
        """
        Verifica rendimiento en un momento específico.
        
        Args:
            token: Dirección del token
            timeframe: String indicando el marco temporal ("24h", "48h", "1w")
        """
        if token not in self.signal_performance:
            return
            
        # Obtener datos actuales
        current_price = self._get_token_price(token)
        if not current_price or not self.signal_performance[token]["initial_price"]:
            return
            
        # Calcular ganancia
        initial_price = self.signal_performance[token]["initial_price"]
        percent_change = ((current_price - initial_price) / initial_price) * 100
        
        # Actualizar información
        self.signal_performance[token][f"price_{timeframe}"] = current_price
        self.signal_performance[token][f"checked_{timeframe}"] = True
        
        # Actualizar precio máximo si corresponde
        if current_price > self.signal_performance[token]["max_price"]:
            self.signal_performance[token]["max_price"] = current_price
            max_gain = ((current_price - initial_price) / initial_price) * 100
            self.signal_performance[token]["max_gain"] = max_gain
        
        # Enviar mensaje con el resultado
        self._send_performance_report(token, timeframe, percent_change)
        
        # Guardar resultados en base de datos para ML
        self._save_performance_data(token, timeframe, percent_change)
    
    def _get_token_price(self, token):
        """
        Obtiene el precio actual del token desde DexScreener.
        """
        try:
            # Intentar obtener el precio del token usando el DexScreenerClient
            if self.dex_client:
                price = self.dex_client.get_token_price(token)
                if price:
                    # Actualizar cache
                    self.last_prices[token] = price
                    return price
            
            # Si no hay DexScreenerClient o falló, intentar directo
            return self._fetch_token_price(token)
        except Exception as e:
            print(f"🚨 Error al obtener precio para {token}: {e}")
            # Retornar el último precio conocido
            return self.last_prices.get(token)
    
    def _fetch_token_price(self, token):
        """
        Método específico para obtener el precio de un token.
        """
        try:
            url = f"https://api.dexscreener.com/latest/dex/tokens/{token}"
            response = requests.get(url, timeout=5)
            data = response.json()
            
            if "pairs" in data and data["pairs"]:
                # Tomar el primer par (generalmente el principal)
                price = float(data["pairs"][0]["priceUsd"])
                # Actualizar cache
                self.last_prices[token] = price
                return price
            return None
        except Exception as e:
            print(f"Error obteniendo precio: {e}")
            return None
    
    def _send_performance_report(self, token, timeframe, percent_change):
        """
        Envía un reporte de rendimiento a Telegram.
        """
        # Formatear el mensaje según el marco temporal
        if timeframe == "24h":
            title = "📊 Resultado a 24 Horas"
        elif timeframe == "48h":
            title = "📊 Resultado a 48 Horas"
        else:  # 1w
            title = "📊 Resultado a 1 Semana"
        
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
        
        # Formatear mensaje
        message = (
            f"*{title}* {emoji}\n\n"
            f"Token: `{token}`\n"
            f"Cambio: *{percent_change:.2f}%*\n"
            f"Confianza inicial: {self.signal_performance[token]['confidence']:.2f}\n"
            f"Traders involucrados: {self.signal_performance[token]['traders_count']}\n"
        )
        
        # Si hay ganancia máxima significativa, mostrarla
        max_gain = self.signal_performance[token]["max_gain"]
        if max_gain > percent_change + 10:  # Al menos 10% más que el actual
            message += f"Ganancia máxima alcanzada: *{max_gain:.2f}%* 📈\n"
        
        # Enviar mensaje
        send_telegram_message(message)
    
    def _save_performance_data(self, token, timeframe, percent_change):
        """
        Guarda datos de rendimiento en la base de datos para análisis ML.
        """
        try:
            # Guardar rendimiento en la base de datos
            signal_id = None  # Obtener el signal_id si está disponible
            
            # Obtener confianza y traders_count de los datos de la señal
            confidence = self.signal_performance[token]["confidence"]
            traders_count = self.signal_performance[token]["traders_count"]
            
            # Guardar en la base de datos
            db.save_signal_performance(
                token=token,
                signal_id=signal_id,
                timeframe=timeframe,
                percent_change=percent_change,
                confidence=confidence,
                traders_count=traders_count
            )
        except Exception as e:
            print(f"🚨 Error al guardar datos de rendimiento: {e}")
