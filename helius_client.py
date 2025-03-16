import time
import requests
import logging
from config import Config

logger = logging.getLogger("helius_client")

class HeliusClient:
    def __init__(self, api_key, token_cache=None):
        self.api_key = api_key
        self.cache = {}  # Usamos esta caché si no se pasa una externa
        self.cache_duration = int(Config.get("HELIUS_CACHE_DURATION", 300))
        # Si se pasa una instancia de caché externa, la usaremos
        self.token_cache = token_cache

    def _request(self, endpoint, params, version="v1"):
        url = f"https://api.helius.xyz/{version}/{endpoint}"
        # En v1 se usa "apiKey", en v0 "api-key"
        if version == "v1":
            params["apiKey"] = self.api_key
        else:
            params["api-key"] = self.api_key
        try:
            logger.debug(f"Solicitando Helius {version}: {url} con params {params}")
            response = requests.get(url, params=params, timeout=5)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error en HeliusClient._request ({version}): {e}")
            return None

    def get_token_data(self, token):
        now = time.time()
        # Si usamos caché externa, verificarla
        if self.token_cache:
            cached = self.token_cache.get(token)
            if cached:
                return cached
        else:
            if token in self.cache and now - self.cache[token]["timestamp"] < self.cache_duration:
                return self.cache[token]["data"]

        # Usar el endpoint DAS de Helius (v0 es el único endpoint actualmente disponible para tokens)
        endpoint = f"tokens/{token}"
        params = {}
        data = self._request(endpoint, params, version="v0")
        
        if data:
            # Si la respuesta es una lista, tomar el primer elemento
            if isinstance(data, list) and data:
                data = data[0]
            normalized_data = {
                "price": data.get("price", data.get("priceUsd", 0)),
                "market_cap": data.get("marketCap", data.get("market_cap", 0)),
                "volume": data.get("volume24h", data.get("volume", 0)),
                "volume_growth": {
                    "growth_5m": self._normalize_percentage(data.get("volumeChange5m", data.get("volume_change_5m", 0))),
                    "growth_1h": self._normalize_percentage(data.get("volumeChange1h", data.get("volume_change_1h", 0)))
                },
                "source": "helius"
            }
            # Guardar en caché
            if self.token_cache:
                self.token_cache.set(token, normalized_data)
            else:
                self.cache[token] = {"data": normalized_data, "timestamp": now}
            return normalized_data

        logger.warning(f"No se pudieron obtener datos para el token {token} desde Helius")
        return None

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
