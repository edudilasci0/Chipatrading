import time
import requests
import logging
from config import Config

logger = logging.getLogger("helius_client")

class HeliusClient:
    def __init__(self, api_key):
        self.api_key = api_key
        self.cache = {}
        self.cache_duration = int(Config.get("HELIUS_CACHE_DURATION", 300))
    
    def _request(self, endpoint, params):
        url = f"https://api.helius.xyz/{endpoint}"
        params["api-key"] = self.api_key
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error en HeliusClient._request: {e}", exc_info=True)
            return None
    
    def get_token_data(self, token):
        # Intentar obtener datos y almacenar en caché
        now = time.time()
        if token in self.cache and now - self.cache[token]["timestamp"] < self.cache_duration:
            return self.cache[token]["data"]
        data = self._request("token-data", {"token": token})
        if data:
            self.cache[token] = {"data": data, "timestamp": now}
        return data if data else {}
    
    def get_price_change(self, token, timeframe="1h"):
        # Obtiene el cambio porcentual del precio en el timeframe especificado
        data = self.get_token_data(token)
        if not data:
            return 0
        try:
            history = data.get("price_history", [])
            if not history:
                return 0
            # Supongamos que history es una lista de dicts con 'timestamp' y 'price'
            now = time.time()
            period = {"1h": 3600, "24h": 86400, "7d": 604800}.get(timeframe, 3600)
            past_prices = [entry["price"] for entry in history if now - entry["timestamp"] <= period]
            if past_prices:
                initial = past_prices[0]
                latest = past_prices[-1]
                return ((latest - initial) / initial) * 100 if initial > 0 else 0
        except Exception as e:
            logger.error(f"Error en get_price_change para {token}: {e}")
            return 0
    
    def get_price_history(self, token):
        # Obtiene el historial de precios para análisis
        data = self.get_token_data(token)
        return data.get("price_history", [])
