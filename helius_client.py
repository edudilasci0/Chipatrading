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
    
    def _request(self, endpoint, params, version="v1"):
        """
        Realiza una solicitud a la API de Helius
        """
        url = f"https://api.helius.xyz/{version}/{endpoint}"
        if version == "v1":
            params["apiKey"] = self.api_key
        else:  # v0
            params["api-key"] = self.api_key
        try:
            logger.debug(f"Solicitando Helius {version}: {url}")
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error en HeliusClient._request ({version}): {e}")
            return None
    
    def get_token_data(self, token):
        """
        Obtiene datos del token usando los endpoints correctos de Helius
        """
        now = time.time()
        if token in self.cache and now - self.cache[token]["timestamp"] < self.cache_duration:
            return self.cache[token]["data"]
        data = None
        endpoint = f"tokens/{token}"
        data = self._request(endpoint, {}, version="v1")
        if not data:
            data = self._request(endpoint, {}, version="v0")
        if not data:
            endpoint = f"addresses/{token}/tokens"
            data = self._request(endpoint, {}, version="v0")
        if data:
            if isinstance(data, list) and data:
                data = data[0]
            normalized_data = {
                "price": self._extract_value(data, ["price", "priceUsd"]),
                "market_cap": self._extract_value(data, ["marketCap", "market_cap"]),
                "volume": self._extract_value(data, ["volume24h", "volume", "volumeUsd"]),
                "volume_growth": {
                    "growth_5m": self._normalize_percentage(self._extract_value(data, ["volumeChange5m", "volume_change_5m"])),
                    "growth_1h": self._normalize_percentage(self._extract_value(data, ["volumeChange1h", "volume_change_1h"]))
                },
                "source": "helius"
            }
            self.cache[token] = {"data": normalized_data, "timestamp": now}
            return normalized_data
        logger.warning(f"No se pudieron obtener datos para el token {token} desde Helius")
        return None
    
    def _extract_value(self, data, possible_keys):
        for key in possible_keys:
            if key in data:
                return data[key]
        return 0
    
    def _normalize_percentage(self, value):
        if value is None:
            return 0
        if value > 1 or value < -1:
            return value / 100
        return value
    
    def get_price_change(self, token, timeframe="1h"):
        token_data = self.get_token_data(token)
        if not token_data:
            return 0
        try:
            if timeframe == "1h":
                return token_data.get("price_change_1h", 0)
            elif timeframe == "24h":
                return token_data.get("price_change_24h", 0)
            else:
                return 0
        except Exception as e:
            logger.error(f"Error en get_price_change para {token}: {e}")
            return 0
